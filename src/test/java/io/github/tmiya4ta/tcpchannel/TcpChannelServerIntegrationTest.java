package io.github.tmiya4ta.tcpchannel;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that exercise the new production-readiness features
 * (idle sweeper, read timeout, graceful drain, max connections, metrics)
 * end-to-end with real sockets via {@link TestHarness}.
 */
class TcpChannelServerIntegrationTest {

    @Test
    @DisplayName("LINE listener echoes a frame end-to-end")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void echoesFrame() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
            try (Socket client = new Socket("127.0.0.1", h.port())) {
                client.getOutputStream().write("hello\n".getBytes());
                client.getOutputStream().flush();
                byte[] received = h.received.poll(2, TimeUnit.SECONDS);
                assertNotNull(received);
                assertArrayEquals("hello".getBytes(), received);
            }
        }
    }

    @Test
    @DisplayName("maxConnections=2 rejects the third concurrent client")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void rejectsBeyondMaxConnections() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().maxConnections(2));
             Socket a = new Socket("127.0.0.1", h.port());
             Socket b = new Socket("127.0.0.1", h.port())) {
            // Wait until the harness has registered both.
            long deadline = System.currentTimeMillis() + 2000;
            while (h.server.activeConnectionCount() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(2, h.server.activeConnectionCount());

            try (Socket c = new Socket("127.0.0.1", h.port())) {
                // The server should accept the TCP-level connect (kernel queue) but
                // immediately close it. Detect via a write that blows up or read EOF.
                long endBy = System.currentTimeMillis() + 2000;
                while (h.server.getMetrics().getConnectionsRejected() < 1
                        && System.currentTimeMillis() < endBy) {
                    Thread.sleep(20);
                }
                assertTrue(h.server.getMetrics().getConnectionsRejected() >= 1,
                        "third client should have been rejected");
            }
        }
    }

    @Test
    @DisplayName("readTimeout closes a connection that sends no data")
    @Timeout(value = 8, unit = TimeUnit.SECONDS)
    void readTimeoutClosesIdleSocket() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().readTimeout(1));
             Socket client = new Socket("127.0.0.1", h.port())) {
            long endBy = System.currentTimeMillis() + 5000;
            while (h.server.getMetrics().getReadTimeouts() < 1
                    && System.currentTimeMillis() < endBy) {
                Thread.sleep(50);
            }
            assertTrue(h.server.getMetrics().getReadTimeouts() >= 1,
                    "read timeout should have fired");
            // Read loop's finally-block runs after the metric tick, so wait a
            // beat for closeAndUnregister to remove the entry.
            long unregBy = System.currentTimeMillis() + 1000;
            while (h.server.activeConnectionCount() != 0
                    && System.currentTimeMillis() < unregBy) {
                Thread.sleep(20);
            }
            assertEquals(0, h.server.activeConnectionCount(),
                    "connection should be unregistered after timeout");
        }
    }

    @Test
    @DisplayName("idle sweeper closes a silent connection after idleTimeout")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void idleSweeperClosesSilentConnection() throws Exception {
        // idleTimeout=5 → sweeper period clamped to max(5/4, 5) = 5s.
        // Open a socket, do nothing, expect it to disappear within ~12s.
        try (TestHarness h = TestHarness.start(new TestHarness.Builder().idleTimeout(5));
             Socket client = new Socket("127.0.0.1", h.port())) {
            long endBy = System.currentTimeMillis() + 20_000;
            while (h.server.getMetrics().getIdleClosed() < 1
                    && System.currentTimeMillis() < endBy) {
                Thread.sleep(200);
            }
            assertTrue(h.server.getMetrics().getIdleClosed() >= 1,
                    "idle sweeper should have closed the connection");
        }
    }

    @Test
    @DisplayName("graceful drain finishes within timeout when peer cooperates")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void gracefulDrain() throws Exception {
        TestHarness h = TestHarness.start(new TestHarness.Builder().gracefulShutdown(3));
        Socket client = new Socket("127.0.0.1", h.port());
        client.getOutputStream().write("hi\n".getBytes());
        client.getOutputStream().flush();
        assertNotNull(h.received.poll(2, TimeUnit.SECONDS));

        // Stop the server. Peer is silent → readLoop blocks on read until our
        // half-close arrives (peer reads EOF then closes its write side, we read
        // EOF on our side, readLoop exits). Should drain well under 3s.
        long t0 = System.nanoTime();
        h.close();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue(elapsedMs < 3_500,
                "graceful drain should finish quickly when the peer cooperates (elapsed=" + elapsedMs + "ms)");
        client.close();
    }

    @Test
    @DisplayName("graceful drain force-closes when peer keeps writing")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void drainForceClosesUncooperativePeer() throws Exception {
        TestHarness h = TestHarness.start(new TestHarness.Builder().gracefulShutdown(1));
        Socket client = new Socket("127.0.0.1", h.port());
        // Push frames in a tight loop in a background thread until the connection breaks.
        Thread spammer = new Thread(() -> {
            try (OutputStream out = client.getOutputStream()) {
                while (!Thread.currentThread().isInterrupted()) {
                    out.write("spam\n".getBytes());
                    out.flush();
                }
            } catch (IOException ignored) {
            }
        });
        spammer.setDaemon(true);
        spammer.start();
        // Wait until we've seen at least one frame so we know the loop is alive.
        assertNotNull(h.received.poll(3, TimeUnit.SECONDS));

        long t0 = System.nanoTime();
        h.close();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        // Drain timeout is 1s; total close should land within ~3s.
        assertTrue(elapsedMs < 4_000,
                "drain should force-close after timeout (elapsed=" + elapsedMs + "ms)");
        spammer.interrupt();
        try { client.close(); } catch (IOException ignored) {}
    }

    @Test
    @DisplayName("metrics counters increment on traffic")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void metricsCounters() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
            assertEquals(0, h.server.getMetrics().getConnectionsAccepted());
            try (Socket client = new Socket("127.0.0.1", h.port())) {
                client.getOutputStream().write("a\nb\nc\n".getBytes());
                client.getOutputStream().flush();
                for (int i = 0; i < 3; i++) {
                    assertNotNull(h.received.poll(2, TimeUnit.SECONDS));
                }
            }
            assertEquals(1, h.server.getMetrics().getConnectionsAccepted());
            assertEquals(3, h.server.getMetrics().getFramesReceived());
            assertEquals(3, h.server.getMetrics().getBytesReceived(),
                    "3 single-byte frames = 3 bytes received");
        }
    }
}
