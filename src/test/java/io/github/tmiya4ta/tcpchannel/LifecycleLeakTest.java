package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.internal.connection.TcpChannelServer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Repeated start/stop cycles must not leak threads, MBeans, or sockets.
 *
 * <p>Telco mission-critical systems redeploy in place; the connector's onStop /
 * onStart pair runs many times over a long-lived JVM. A small leak per cycle
 * compounds into ENOMEM / can't-create-thread within days. These tests catch
 * the obvious classes of leak before they reach production.
 */
class LifecycleLeakTest {

    @Test
    @DisplayName("Restart loop ×30 does not leak threads or MBeans")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void restartLoopDoesNotLeak() throws Exception {
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        // Drain JVM-internal warmups before sampling baselines.
        warmup();
        int baselineThreads = countServerSocketThreads(tmx);
        int baselineMBeans = listListenerMBeans(mbs).size();

        for (int i = 0; i < 30; i++) {
            try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
                // Touch a connection so the workers pool actually spawns a thread.
                try (Socket client = new Socket("127.0.0.1", h.port())) {
                    client.getOutputStream().write("ping\n".getBytes());
                    client.getOutputStream().flush();
                    h.received.poll(1, TimeUnit.SECONDS);
                }
            }
        }

        // Daemon threads can take a beat to reap; allow up to ~3s for cleanup.
        long deadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < deadline
                && countServerSocketThreads(tmx) > baselineThreads + 3) {
            Thread.sleep(100);
        }

        int liveThreads = countServerSocketThreads(tmx);
        int liveMBeans = listListenerMBeans(mbs).size();
        assertTrue(liveThreads <= baselineThreads + 3,
                "tcpc-* threads leaked across restart loop: baseline=" + baselineThreads
                        + " now=" + liveThreads);
        assertEquals(baselineMBeans, liveMBeans,
                "JMX MBeans must not leak across restart loop");
    }

    @Test
    @DisplayName("MBean re-registration is idempotent (does not throw on double register)")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void mbeanReRegistrationIdempotent() throws Exception {
        // Build a server with JMX enabled and re-register the bean explicitly.
        // This simulates a redeploy-without-clean-undeploy path that has hit
        // production in the wild.
        try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
            // Force a manual re-registration via the production code path.
            h.server.unregisterMBean();
            // Construct a JMX-enabled twin by hand and verify the registry path doesn't double-up.
            ServerSocket sock = new ServerSocket();
            sock.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
            TcpChannelServer twin = new TcpChannelServer(sock, "127.0.0.1", sock.getLocalPort(),
                    false,
                    h.server.getFraming(), h.server.getLineDelimiter(), h.server.getMaxFrameLength(),
                    h.server.isKeepAlive(), h.server.getMaxConnections(),
                    h.server.getFixedFrameSize(), h.server.getMagicBytes(),
                    0, 0, 0, 0, 0, 5, true);
            twin.registerMBean();
            twin.registerMBean(); // second call must not blow up
            twin.unregisterMBean();
            twin.unregisterMBean(); // idempotent on the way out too
            twin.close();
        }
    }

    @Test
    @DisplayName("closeAndUnregister is idempotent on a missing id")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    void closeAndUnregisterIdempotent() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
            // Calling on a non-existent id must be a no-op, not throw.
            h.server.closeAndUnregister("nope");
            h.server.closeAndUnregister(null);
            assertEquals(0, h.server.activeConnectionCount());
        }
    }

    @Test
    @DisplayName("Sustained connect/disconnect churn does not grow the registry")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void connectionChurnDoesNotGrowRegistry() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
            int attempted = 200;
            int connected = 0;
            for (int i = 0; i < attempted; i++) {
                try (Socket s = new Socket("127.0.0.1", h.port())) {
                    OutputStream out = s.getOutputStream();
                    out.write(("frame-" + i + "\n").getBytes());
                    out.flush();
                    connected++;
                } catch (IOException ignored) {
                    // OS-level backlog or transient connect failures don't
                    // change the contract under test (registry must drain).
                }
                if (i > 0 && i % 50 == 0) {
                    assertTrue(h.server.activeConnectionCount() <= 60,
                            "registry must drain between connections (was "
                                    + h.server.activeConnectionCount() + " after " + i + ")");
                }
            }
            long deadline = System.currentTimeMillis() + 5_000;
            while (h.server.activeConnectionCount() > 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(0, h.server.activeConnectionCount(),
                    "registry must end empty after churn");
            // The connector contract: every accepted connection produces a metric
            // tick, and every metric tick corresponds to a registry entry that
            // gets drained. We don't pin the OS-level connect success rate.
            assertTrue(connected >= attempted * 0.75,
                    "at least 75% of connect attempts should succeed under sequential churn");
            assertEquals(connected, h.server.getMetrics().getConnectionsAccepted(),
                    "metric must count exactly the connections that successfully connected");
        }
    }

    private static void warmup() throws Exception {
        try (TestHarness h = TestHarness.start(new TestHarness.Builder())) {
            try (Socket client = new Socket("127.0.0.1", h.port())) {
                client.getOutputStream().write("warm\n".getBytes());
                client.getOutputStream().flush();
                h.received.poll(1, TimeUnit.SECONDS);
            }
        }
        Thread.sleep(200);
    }

    private static int countServerSocketThreads(ThreadMXBean tmx) {
        long[] ids = tmx.getAllThreadIds();
        int n = 0;
        for (long id : ids) {
            java.lang.management.ThreadInfo info = tmx.getThreadInfo(id);
            if (info == null) continue;
            String name = info.getThreadName();
            if (name != null && (name.startsWith("tcpc-") || name.startsWith("harness-"))) {
                n++;
            }
        }
        return n;
    }

    private static Set<ObjectName> listListenerMBeans(MBeanServer mbs) throws Exception {
        return mbs.queryNames(new ObjectName("io.github.tmiya4ta.tcpchannel:type=Listener,*"), null);
    }
}
