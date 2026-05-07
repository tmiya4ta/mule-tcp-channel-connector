package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.connection.ConnectionEntry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress and race-condition tests. These exist because the connector is going
 * into a telco-grade mission-critical deployment where bugs are not negotiable.
 * We exercise paths a happy-path test would miss:
 * <ul>
 *   <li>many concurrent clients all sending many frames</li>
 *   <li>multiple writers to a single socket (write lock protection)</li>
 *   <li>sweeper firing while connections are doing real work</li>
 *   <li>disconnect operation racing with the read loop</li>
 * </ul>
 */
class ConcurrencyStressTest {

    @Test
    @DisplayName("100 concurrent clients × 50 frames each: all received intact, no dropped frames")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void hundredClientsManyFrames() throws Exception {
        final int clients = 100;
        final int framesPerClient = 50;
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().maxConnections(clients))) {
            CountDownLatch ready = new CountDownLatch(clients);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(clients);
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            AtomicInteger failures = new AtomicInteger();
            for (int i = 0; i < clients; i++) {
                final int clientIdx = i;
                pool.submit(() -> {
                    try (Socket s = new Socket("127.0.0.1", h.port())) {
                        ready.countDown();
                        go.await();
                        OutputStream out = s.getOutputStream();
                        for (int f = 0; f < framesPerClient; f++) {
                            String msg = "c" + clientIdx + "-f" + f + "\n";
                            out.write(msg.getBytes());
                        }
                        out.flush();
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(ready.await(15, TimeUnit.SECONDS), "all clients should connect");
            go.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "all clients should finish sending");
            pool.shutdown();
            assertEquals(0, failures.get(), "no client errors expected");

            // Drain the queue and verify every (clientIdx, frameIdx) pair arrived exactly once.
            int expected = clients * framesPerClient;
            Set<String> seen = new HashSet<>();
            long deadline = System.currentTimeMillis() + 25_000;
            while (seen.size() < expected && System.currentTimeMillis() < deadline) {
                byte[] frame = h.received.poll(500, TimeUnit.MILLISECONDS);
                if (frame == null) continue;
                seen.add(new String(frame));
            }
            assertEquals(expected, seen.size(),
                    "expected " + expected + " unique frames, got " + seen.size());
        }
    }

    @Test
    @DisplayName("Concurrent writers on one socket do not interleave bytes")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentWritersDoNotInterleave() throws Exception {
        // Echo mode: the harness writes back every frame it reads. We then
        // launch N threads all calling entry.writeFrame() concurrently with the
        // echo writer to provoke interleaving. Frames come back as
        // LENGTH_PREFIX, so any byte interleave is decoded as garbage and the
        // assertion below catches it.
        TestHarness.Builder b = new TestHarness.Builder();
        b.framing = Framing.LENGTH_PREFIX;
        try (TestHarness h = TestHarness.start(b)) {
            h.enableEcho();
            try (Socket client = new Socket("127.0.0.1", h.port())) {
                client.setSoTimeout(5000);
                OutputStream out = client.getOutputStream();
                DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));

                // Drive the connection so it gets registered, capture the connId.
                writeLengthPrefixed(out, "init".getBytes());
                byte[] echoed = readLengthPrefixed(in);
                assertArrayEquals("init".getBytes(), echoed);

                // The harness has registered exactly one ConnectionEntry; grab it.
                String connId = h.server.getLastConnectionId();
                ConnectionEntry entry = h.server.getEntry(connId);
                assertNotNull(entry);

                // Fire 8 threads that each write 50 distinct frames via the entry's
                // own writeFrame (the production path used by <tcpc:write>).
                final int writers = 8;
                final int perWriter = 50;
                CountDownLatch start = new CountDownLatch(1);
                ExecutorService pool = Executors.newFixedThreadPool(writers);
                for (int w = 0; w < writers; w++) {
                    final int idx = w;
                    pool.submit(() -> {
                        try {
                            start.await();
                            for (int f = 0; f < perWriter; f++) {
                                byte[] payload = ("w" + idx + "-f" + f).getBytes();
                                entry.writeFrame(Framing.LENGTH_PREFIX, LineDelimiter.LF, 0,
                                        new byte[0], payload);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                start.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

                // Read all writers + 0 echoes (since we wrote via entry.writeFrame
                // bypassing the readLoop, no echo is triggered for those frames).
                int expected = writers * perWriter;
                Set<String> seen = new HashSet<>();
                for (int i = 0; i < expected; i++) {
                    byte[] f = readLengthPrefixed(in);
                    seen.add(new String(f));
                }
                assertEquals(expected, seen.size(),
                        "all unique frames must arrive intact (interleaving would corrupt frames)");
            }
        }
    }

    @Test
    @DisplayName("Sweeper races with active traffic without losing frames")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void sweeperRunsAlongsideActiveTraffic() throws Exception {
        // idleTimeout=5 means sweeper period = 5s. We hold connections for ~12s
        // sending frames every 200ms (well under the timeout). No connection
        // should be marked idle and no frames should be lost.
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().idleTimeout(5).maxConnections(20))) {
            int clients = 10;
            int frames = 60; // 60 frames × 200ms ≈ 12s
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            CountDownLatch done = new CountDownLatch(clients);
            for (int i = 0; i < clients; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try (Socket s = new Socket("127.0.0.1", h.port())) {
                        OutputStream out = s.getOutputStream();
                        for (int f = 0; f < frames; f++) {
                            out.write(("c" + idx + "-f" + f + "\n").getBytes());
                            out.flush();
                            Thread.sleep(200);
                        }
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(25, TimeUnit.SECONDS));
            pool.shutdown();

            assertEquals(0, h.server.getMetrics().getIdleClosed(),
                    "active connections must not be reaped by the idle sweeper");
            assertEquals(clients * frames, h.server.getMetrics().getFramesReceived(),
                    "no frames should be lost while the sweeper runs in parallel");
        }
    }

    @Test
    @DisplayName("disconnect raced against a busy read loop is safe and idempotent")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void disconnectRacingReadLoop() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().maxConnections(50))) {
            int rounds = 50;
            for (int r = 0; r < rounds; r++) {
                Socket client = new Socket("127.0.0.1", h.port());
                OutputStream out = client.getOutputStream();
                // pump a few frames so the read loop is hot
                for (int i = 0; i < 3; i++) out.write(("frame-" + i + "\n").getBytes());
                out.flush();

                // Wait until the entry is registered, then close from the server side.
                String connId = waitForLatestConn(h, 1000);
                assertNotNull(connId);
                h.server.closeAndUnregister(connId);
                // Idempotent second call must not throw or produce noise.
                h.server.closeAndUnregister(connId);
                try { client.close(); } catch (IOException ignored) {}
            }
            // Settle before checking
            Thread.sleep(200);
            assertEquals(0, h.server.activeConnectionCount(),
                    "all connections should be cleaned up after disconnect rounds");
        }
    }

    private static String waitForLatestConn(TestHarness h, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            String id = h.server.getLastConnectionId();
            if (id != null && h.server.getEntry(id) != null) return id;
            Thread.sleep(10);
        }
        return null;
    }

    private static void writeLengthPrefixed(OutputStream out, byte[] payload) throws IOException {
        out.write((payload.length >>> 24) & 0xff);
        out.write((payload.length >>> 16) & 0xff);
        out.write((payload.length >>> 8) & 0xff);
        out.write(payload.length & 0xff);
        out.write(payload);
        out.flush();
    }

    private static byte[] readLengthPrefixed(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] buf = new byte[len];
        in.readFully(buf);
        return buf;
    }
}
