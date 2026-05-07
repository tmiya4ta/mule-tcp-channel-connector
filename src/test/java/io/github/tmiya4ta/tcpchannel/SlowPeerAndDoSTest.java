package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.api.Framing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Slow-peer, partial-frame, and oversized-frame DoS resistance tests.
 *
 * <p>The connector serves an external network. We need confidence that:
 * <ul>
 *   <li>A peer drip-feeding a frame byte by byte does not block another connection.</li>
 *   <li>readTimeout actually fires on a stuck socket.</li>
 *   <li>An oversized frame closes the offending connection without affecting siblings.</li>
 *   <li>Repeated oversized attempts do not exhaust memory.</li>
 *   <li>A truncated LENGTH_PREFIX header (4 bytes then silence) does not hang anything.</li>
 * </ul>
 */
class SlowPeerAndDoSTest {

    @Test
    @DisplayName("Slow drip-feed of a LINE frame eventually completes if under readTimeout")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void slowDripCompletesUnderReadTimeout() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().readTimeout(5))) {
            try (Socket client = new Socket("127.0.0.1", h.port())) {
                OutputStream out = client.getOutputStream();
                byte[] msg = "drip-feed\n".getBytes();
                for (byte b : msg) {
                    out.write(b);
                    out.flush();
                    Thread.sleep(50); // total ~500ms, well under 5s readTimeout
                }
                byte[] received = h.received.poll(2, TimeUnit.SECONDS);
                assertNotNull(received);
                assertArrayEquals("drip-feed".getBytes(), received);
            }
        }
    }

    @Test
    @DisplayName("readTimeout fires on a peer that opens a socket and goes silent")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void slowlorisCutByReadTimeout() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().readTimeout(2));
             Socket client = new Socket("127.0.0.1", h.port())) {
            // Send 1 byte then go silent. readTimeout=2s should cut it.
            client.getOutputStream().write('a');
            client.getOutputStream().flush();
            long deadline = System.currentTimeMillis() + 6_000;
            while (h.server.getMetrics().getReadTimeouts() < 1
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(h.server.getMetrics().getReadTimeouts() >= 1,
                    "slowloris peer must be cut by readTimeout");
        }
    }

    @Test
    @DisplayName("Truncated LENGTH_PREFIX header is cut by readTimeout, not hung forever")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void truncatedLengthPrefixHeader() throws Exception {
        TestHarness.Builder b = new TestHarness.Builder().readTimeout(2);
        b.framing = Framing.LENGTH_PREFIX;
        try (TestHarness h = TestHarness.start(b);
             Socket client = new Socket("127.0.0.1", h.port())) {
            // Only 2 of the 4 length-prefix bytes, then go silent.
            client.getOutputStream().write(new byte[]{0, 0});
            client.getOutputStream().flush();
            long deadline = System.currentTimeMillis() + 6_000;
            while (h.server.getMetrics().getReadTimeouts() < 1
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(h.server.getMetrics().getReadTimeouts() >= 1,
                    "stuck mid-header peer must be cut by readTimeout");
        }
    }

    @Test
    @DisplayName("Oversized LENGTH_PREFIX frame closes only the offender; sibling stays up")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void oversizedFrameDoesNotAffectSiblings() throws Exception {
        TestHarness.Builder b = new TestHarness.Builder();
        b.framing = Framing.LENGTH_PREFIX;
        b.maxFrameLength = 1024;
        try (TestHarness h = TestHarness.start(b)) {
            // Sibling connection that we want to stay alive.
            try (Socket goodClient = new Socket("127.0.0.1", h.port());
                 Socket badClient = new Socket("127.0.0.1", h.port())) {
                // Bad client claims a 10 MiB frame → server should reject and close.
                OutputStream bad = badClient.getOutputStream();
                writeIntBE(bad, 10_000_000);
                bad.write(new byte[]{1, 2, 3});
                bad.flush();

                // Good client sends a valid small frame.
                OutputStream good = goodClient.getOutputStream();
                writeIntBE(good, 5);
                good.write("hello".getBytes());
                good.flush();

                byte[] received = h.received.poll(3, TimeUnit.SECONDS);
                assertNotNull(received);
                assertArrayEquals("hello".getBytes(), received);

                // Bad client should be hung up on. Wait for read error metric to tick.
                long deadline = System.currentTimeMillis() + 3_000;
                while (h.server.getMetrics().getReadErrors() < 1
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                }
                assertTrue(h.server.getMetrics().getReadErrors() >= 1,
                        "oversized frame should produce a read error and close");

                // Good client should still be registered.
                long settle = System.currentTimeMillis() + 1_000;
                while (h.server.activeConnectionCount() > 1
                        && System.currentTimeMillis() < settle) {
                    Thread.sleep(20);
                }
                assertEquals(1, h.server.activeConnectionCount(),
                        "sibling connection must stay alive after offender is closed");
            }
        }
    }

    @Test
    @DisplayName("Repeated oversized attempts from sequential clients do not exhaust memory")
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void repeatedOversizedAttemptsAreBounded() throws Exception {
        TestHarness.Builder b = new TestHarness.Builder();
        b.framing = Framing.LENGTH_PREFIX;
        b.maxFrameLength = 1024;
        Runtime rt = Runtime.getRuntime();
        long startUsed = rt.totalMemory() - rt.freeMemory();
        try (TestHarness h = TestHarness.start(b)) {
            for (int i = 0; i < 100; i++) {
                try (Socket bad = new Socket("127.0.0.1", h.port())) {
                    OutputStream out = bad.getOutputStream();
                    // Single write so the bytes are guaranteed to enter the kernel
                    // buffer atomically (avoids RST-on-close races).
                    out.write(toBE(1025));
                    out.flush();
                    // Read once so we don't close before the server reads.
                    bad.setSoTimeout(200);
                    try { bad.getInputStream().read(); } catch (IOException ignored) {}
                } catch (IOException ignored) {
                }
            }
            // settle
            long deadline = System.currentTimeMillis() + 5_000;
            while (h.server.activeConnectionCount() > 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(0, h.server.activeConnectionCount(),
                    "all rejected connections must be cleaned up");
            // The exact split between readErrors (oversize rejection) and clean
            // EOF (peer closed before bytes arrived) varies per OS/scheduler;
            // either path is benign for production. The contract we DO require
            // is that no oversized frame surfaced as a "received frame".
            assertEquals(0, h.server.getMetrics().getFramesReceived(),
                    "no oversized frame should ever surface as a received frame");
            assertEquals(100, h.server.getMetrics().getConnectionsAccepted(),
                    "every attempt must have been accepted before being rejected internally");
        }
        // Force GC and check we didn't grow heap by orders of magnitude.
        System.gc();
        Thread.sleep(200);
        long endUsed = rt.totalMemory() - rt.freeMemory();
        long growthMb = Math.max(0, (endUsed - startUsed) / (1024 * 1024));
        assertTrue(growthMb < 50,
                "100 oversized attempts must not grow heap by >50MB (grew by " + growthMb + " MB)");
    }

    @Test
    @DisplayName("maxFrameLength is enforced even under concurrent attacks")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void maxFrameLengthEnforcedConcurrently() throws Exception {
        TestHarness.Builder b = new TestHarness.Builder().maxConnections(50);
        b.framing = Framing.LENGTH_PREFIX;
        b.maxFrameLength = 1024;
        try (TestHarness h = TestHarness.start(b)) {
            int attackers = 30;
            Thread[] ts = new Thread[attackers];
            for (int i = 0; i < attackers; i++) {
                ts[i] = new Thread(() -> {
                    try (Socket s = new Socket("127.0.0.1", h.port())) {
                        OutputStream out = s.getOutputStream();
                        writeIntBE(out, 99999); // larger than 1024
                        out.flush();
                    } catch (IOException ignored) {
                    }
                });
                ts[i].start();
            }
            for (Thread t : ts) t.join(5_000);
            // settle
            long deadline = System.currentTimeMillis() + 3_000;
            while (h.server.activeConnectionCount() > 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(0, h.server.activeConnectionCount());
            assertEquals(0, h.server.getMetrics().getFramesReceived(),
                    "no oversized frame should ever surface as a received frame");
        }
    }

    private static void writeIntBE(OutputStream out, int v) throws IOException {
        out.write(toBE(v));
    }

    private static byte[] toBE(int v) {
        return new byte[]{
                (byte) ((v >>> 24) & 0xff),
                (byte) ((v >>> 16) & 0xff),
                (byte) ((v >>> 8) & 0xff),
                (byte) (v & 0xff)
        };
    }
}
