package io.github.tmiya4ta.tcpchannel.testclient;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone TCP/TLS test client for the sample-app deployed in a staging
 * environment (kind/RTF/standalone runtime). Runs a fixed set of checks against
 * each framing the sample-app exposes plus the HTTP /push and /close
 * operations, and reports pass/fail.
 *
 * Usage:
 *   java -jar tcp-test-client.jar [tcpHost] [httpBaseUrl]
 *
 * Defaults:
 *   tcpHost      = 127.0.0.1
 *   httpBaseUrl  = http://127.0.0.1:8282
 *
 * Ports it expects on tcpHost (matches example/sample-app/.../config-local.yaml):
 *   5557 LINE, 5558 LENGTH_PREFIX, 5559 FIXED_LENGTH (16-byte payload, magic 0xAABB),
 *   5560 LINE+aggregator, 8282 HTTP /push /close.
 */
public final class TestClient {

    static final int LINE_PORT = 5557;
    static final int BIN_PORT = 5558;
    static final int FIXED_PORT = 5559;
    static final int AGG_PORT = 5560;

    private static final byte[] FIXED_MAGIC = new byte[]{(byte) 0xAA, (byte) 0xBB};
    private static final int FIXED_SIZE = 16;

    private final String host;
    private final String httpBase;
    private final List<Result> results = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        String httpBase = args.length > 1 ? args[1] : "http://127.0.0.1:8282";
        TestClient c = new TestClient(host, httpBase);
        c.runAll();
        c.printReport();
        if (c.results.stream().anyMatch(r -> !r.passed)) {
            System.exit(1);
        }
    }

    public TestClient(String host, String httpBase) {
        this.host = host;
        this.httpBase = httpBase;
    }

    void runAll() {
        run("line/echo", this::testLineEcho);
        run("line/multiple-frames", this::testLineMultipleFrames);
        run("length-prefix/echo", this::testLengthPrefixEcho);
        run("length-prefix/binary-payload", this::testLengthPrefixBinary);
        run("fixed-length/echo-with-magic", this::testFixedLengthEcho);
        run("fixed-length/resync-after-garbage", this::testFixedLengthResync);
        run("aggregator/four-lines-batched", this::testAggregator);
        run("http/push-then-receive-on-tcp", this::testHttpPush);
        run("http/close-disconnects-tcp", this::testHttpClose);
        run("concurrency/50-clients-each-10-frames", this::testConcurrent);
        run("liveness/server-still-up-after-cycles", this::testLivenessAfterCycles);
    }

    interface Check { void run() throws Exception; }

    private void run(String name, Check c) {
        long t0 = System.nanoTime();
        try {
            c.run();
            results.add(new Result(name, true, "ok", elapsedMs(t0)));
            System.out.println("PASS " + name + " (" + elapsedMs(t0) + " ms)");
        } catch (Throwable t) {
            results.add(new Result(name, false, t.toString(), elapsedMs(t0)));
            System.out.println("FAIL " + name + " (" + elapsedMs(t0) + " ms): " + t);
        }
    }

    private static long elapsedMs(long t0) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }

    void printReport() {
        System.out.println();
        System.out.println("=== Summary ===");
        long passed = results.stream().filter(r -> r.passed).count();
        long failed = results.size() - passed;
        System.out.println("Passed: " + passed + " / " + results.size());
        if (failed > 0) {
            System.out.println("Failed:");
            results.stream().filter(r -> !r.passed)
                    .forEach(r -> System.out.println("  " + r.name + " — " + r.detail));
        }
    }

    // ---------- LINE ----------

    void testLineEcho() throws Exception {
        try (Socket s = openWithTimeout(LINE_PORT, 5000)) {
            OutputStream out = s.getOutputStream();
            out.write("hello\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            String line = readLfLine(s);
            assertContains("ACK#1: hello", line);
        }
    }

    void testLineMultipleFrames() throws Exception {
        try (Socket s = openWithTimeout(LINE_PORT, 5000)) {
            OutputStream out = s.getOutputStream();
            for (int i = 0; i < 5; i++) {
                out.write(("msg-" + i + "\n").getBytes(StandardCharsets.UTF_8));
            }
            out.flush();
            for (int i = 0; i < 5; i++) {
                String line = readLfLine(s);
                assertContains("ACK#" + (i + 1) + ": msg-" + i, line);
            }
        }
    }

    // ---------- LENGTH_PREFIX ----------

    void testLengthPrefixEcho() throws Exception {
        try (Socket s = openWithTimeout(BIN_PORT, 5000)) {
            byte[] payload = "binary-frame-1".getBytes(StandardCharsets.UTF_8);
            writeLengthPrefixed(s.getOutputStream(), payload);
            byte[] received = readLengthPrefixed(s);
            assertArrayEquals(payload, received);
        }
    }

    void testLengthPrefixBinary() throws Exception {
        try (Socket s = openWithTimeout(BIN_PORT, 5000)) {
            byte[] payload = new byte[]{0, 1, 2, (byte) 0xff, (byte) 0xee, 'A', 'B', 0};
            writeLengthPrefixed(s.getOutputStream(), payload);
            byte[] received = readLengthPrefixed(s);
            assertArrayEquals(payload, received);
        }
    }

    // ---------- FIXED_LENGTH ----------

    void testFixedLengthEcho() throws Exception {
        try (Socket s = openWithTimeout(FIXED_PORT, 5000)) {
            byte[] payload = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8); // 16 bytes
            OutputStream out = s.getOutputStream();
            out.write(FIXED_MAGIC);
            out.write(payload);
            out.flush();
            byte[] received = readFixed(s);
            assertArrayEquals(payload, received);
        }
    }

    void testFixedLengthResync() throws Exception {
        try (Socket s = openWithTimeout(FIXED_PORT, 5000)) {
            byte[] payload = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8);
            OutputStream out = s.getOutputStream();
            // Send garbage then magic+payload — server must skip and resync.
            out.write(new byte[]{0x01, 0x02, 0x03, (byte) 0xAA, 0x04});
            out.write(FIXED_MAGIC);
            out.write(payload);
            out.flush();
            byte[] received = readFixed(s);
            assertArrayEquals(payload, received);
        }
    }

    // ---------- aggregator (size=4) ----------

    void testAggregator() throws Exception {
        try (Socket s = openWithTimeout(AGG_PORT, 5000)) {
            OutputStream out = s.getOutputStream();
            for (int i = 0; i < 4; i++) {
                out.write(("agg-" + i + "\n").getBytes(StandardCharsets.UTF_8));
            }
            out.flush();
            // The aggregator demo writes back 4 ACK lines plus an aggregation
            // confirmation. Read until we have four ACK#N: agg-* lines.
            int seen = 0;
            long deadline = System.currentTimeMillis() + 5_000;
            s.setSoTimeout(2000);
            while (seen < 4 && System.currentTimeMillis() < deadline) {
                String line = readLfLineQuiet(s);
                if (line == null) break;
                if (line.startsWith("ACK") && line.contains("agg-")) seen++;
            }
            if (seen < 4) {
                throw new AssertionError("expected 4 ACK lines, got " + seen);
            }
        }
    }

    // ---------- HTTP push / close ----------

    void testHttpPush() throws Exception {
        // Open a LINE connection so we have an active connId for /push to target.
        try (Socket s = openWithTimeout(LINE_PORT, 5000)) {
            // Drive at least one frame so the connection is registered.
            OutputStream out = s.getOutputStream();
            out.write("warmup\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            String warm = readLfLine(s);
            assertContains("ACK#1: warmup", warm);

            // Now push from HTTP and verify it lands on this socket.
            String pushed = "pushed-" + System.currentTimeMillis();
            int code = httpPost("/push", pushed.getBytes(StandardCharsets.UTF_8), "text/plain");
            if (code / 100 != 2) {
                throw new AssertionError("/push returned HTTP " + code);
            }
            s.setSoTimeout(3000);
            String line = readLfLine(s);
            assertContains(pushed, line);
        }
    }

    void testHttpClose() throws Exception {
        Socket s = openWithTimeout(LINE_PORT, 5000);
        try {
            OutputStream out = s.getOutputStream();
            out.write("warm-close\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            assertContains("ACK#1: warm-close", readLfLine(s));

            int code = httpPost("/close", new byte[0], "text/plain");
            if (code / 100 != 2) {
                throw new AssertionError("/close returned HTTP " + code);
            }
            // After /close the server should hang up — read should return -1
            // (or throw within timeout).
            s.setSoTimeout(3000);
            int r = s.getInputStream().read();
            if (r != -1) {
                throw new AssertionError("expected EOF after /close, got byte " + r);
            }
        } finally {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    // ---------- Concurrency ----------

    void testConcurrent() throws Exception {
        final int clients = 50;
        final int frames = 10;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch ready = new CountDownLatch(clients);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger err = new AtomicInteger();
        for (int i = 0; i < clients; i++) {
            final int idx = i;
            pool.submit(() -> {
                try (Socket s = openWithTimeout(LINE_PORT, 5000)) {
                    s.setSoTimeout(8000);
                    ready.countDown();
                    go.await();
                    OutputStream out = s.getOutputStream();
                    Set<String> seen = new HashSet<>();
                    for (int f = 0; f < frames; f++) {
                        String msg = "c" + idx + "-f" + f;
                        out.write((msg + "\n").getBytes(StandardCharsets.UTF_8));
                    }
                    out.flush();
                    for (int f = 0; f < frames; f++) {
                        String reply = readLfLine(s);
                        if (!reply.contains("c" + idx + "-f")) {
                            err.incrementAndGet();
                            return;
                        }
                        seen.add(reply);
                    }
                    if (seen.size() != frames) {
                        err.incrementAndGet();
                        return;
                    }
                    ok.incrementAndGet();
                } catch (Exception e) {
                    err.incrementAndGet();
                }
            });
        }
        ready.await(15, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new AssertionError("concurrency pool did not finish in time");
        }
        if (err.get() != 0) {
            throw new AssertionError("concurrent clients reported " + err.get() + " failure(s)");
        }
        if (ok.get() != clients) {
            throw new AssertionError("only " + ok.get() + "/" + clients + " concurrent clients passed");
        }
    }

    void testLivenessAfterCycles() throws Exception {
        // After all the previous tests, the server should still be up and able
        // to handle a fresh frame without delay.
        try (Socket s = openWithTimeout(LINE_PORT, 3000)) {
            OutputStream out = s.getOutputStream();
            out.write("liveness\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            String line = readLfLine(s);
            assertContains("ACK#1: liveness", line);
        }
    }

    // ---------- helpers ----------

    private Socket openWithTimeout(int port, int soTimeoutMs) throws IOException {
        Socket s = new Socket();
        s.connect(new java.net.InetSocketAddress(host, port), 5000);
        s.setSoTimeout(soTimeoutMs);
        return s;
    }

    private static String readLfLine(Socket s) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = s.getInputStream().read()) != -1) {
            if (b == '\n') return sb.toString();
            if (b != '\r') sb.append((char) b);
        }
        if (sb.length() == 0) throw new IOException("EOF before newline");
        return sb.toString();
    }

    private static String readLfLineQuiet(Socket s) throws IOException {
        try {
            return readLfLine(s);
        } catch (java.net.SocketTimeoutException ste) {
            return null;
        }
    }

    private static void writeLengthPrefixed(OutputStream out, byte[] payload) throws IOException {
        out.write((payload.length >>> 24) & 0xff);
        out.write((payload.length >>> 16) & 0xff);
        out.write((payload.length >>> 8) & 0xff);
        out.write(payload.length & 0xff);
        out.write(payload);
        out.flush();
    }

    private static byte[] readLengthPrefixed(Socket s) throws IOException {
        DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        int len = in.readInt();
        if (len < 0 || len > 1_000_000) throw new IOException("absurd length " + len);
        byte[] buf = new byte[len];
        in.readFully(buf);
        return buf;
    }

    private static byte[] readFixed(Socket s) throws IOException {
        // Server writes back MAGIC + payload of FIXED_SIZE bytes.
        BufferedInputStream in = new BufferedInputStream(s.getInputStream());
        // Find magic.
        int matched = 0;
        while (matched < FIXED_MAGIC.length) {
            int b = in.read();
            if (b == -1) throw new IOException("EOF before magic");
            if ((byte) b == FIXED_MAGIC[matched]) matched++;
            else if ((byte) b == FIXED_MAGIC[0]) matched = 1;
            else matched = 0;
        }
        byte[] buf = new byte[FIXED_SIZE];
        int read = 0;
        while (read < FIXED_SIZE) {
            int n = in.read(buf, read, FIXED_SIZE - read);
            if (n == -1) throw new IOException("EOF mid-frame");
            read += n;
        }
        return buf;
    }

    private int httpPost(String path, byte[] body, String contentType) throws IOException {
        URL u = URI.create(httpBase + path).toURL();
        HttpURLConnection c = (HttpURLConnection) u.openConnection();
        c.setRequestMethod("POST");
        c.setDoOutput(true);
        c.setConnectTimeout(5000);
        c.setReadTimeout(5000);
        c.setRequestProperty("Content-Type", contentType);
        try (OutputStream o = c.getOutputStream()) {
            o.write(body);
        }
        int code = c.getResponseCode();
        c.disconnect();
        return code;
    }

    private static void assertContains(String needle, String haystack) {
        if (haystack == null || !haystack.contains(needle)) {
            throw new AssertionError("expected to contain '" + needle + "' but got: '" + haystack + "'");
        }
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual) {
        if (expected.length != actual.length) {
            throw new AssertionError("length mismatch: expected " + expected.length + " got " + actual.length);
        }
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                throw new AssertionError("byte " + i + " mismatch");
            }
        }
    }

    static final class Result {
        final String name;
        final boolean passed;
        final String detail;
        final long elapsedMs;
        Result(String n, boolean p, String d, long e) {
            this.name = n; this.passed = p; this.detail = d; this.elapsedMs = e;
        }
    }
}
