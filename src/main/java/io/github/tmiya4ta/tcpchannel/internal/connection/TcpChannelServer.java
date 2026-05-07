package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.metrics.TcpChannelMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the bound {@link ServerSocket} and the registry of accepted connections.
 *
 * <p>Shared between the source (which accepts and registers connections) and
 * the operations (which look up sockets by id to write or disconnect) via
 * {@code @Connection} injection on a {@code CachedConnectionProvider}.
 *
 * <p>The server also runs an in-process idle sweeper (a single-threaded
 * scheduled executor) that closes connections whose {@code lastActivityNanos}
 * is older than {@code idleTimeoutSeconds}, and registers a JMX
 * {@link TcpChannelMetrics} bean for observability.
 */
public class TcpChannelServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpChannelServer.class);

    private final ServerSocket serverSocket;
    private final String host;
    private final int port;
    private final boolean tlsEnabled;

    private final Framing framing;
    private final LineDelimiter lineDelimiter;
    private final int maxFrameLength;
    private final boolean keepAlive;
    private final int maxConnections;
    private final int fixedFrameSize;
    private final byte[] magicBytes;

    private final int readTimeoutSeconds;
    private final int idleTimeoutSeconds;
    private final int tcpKeepIdleSeconds;
    private final int tcpKeepIntervalSeconds;
    private final int tcpKeepCount;
    private final int gracefulShutdownTimeoutSeconds;
    private final boolean jmxEnabled;

    private final ConcurrentHashMap<String, ConnectionEntry> connections = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastConnectionId = new AtomicReference<>();
    private final TcpChannelMetrics metrics;

    private ScheduledExecutorService sweeper;
    private ObjectName mbeanName;

    public TcpChannelServer(ServerSocket serverSocket, String host, int port, boolean tlsEnabled,
                            Framing framing, LineDelimiter lineDelimiter,
                            int maxFrameLength, boolean keepAlive, int maxConnections,
                            int fixedFrameSize, byte[] magicBytes,
                            int readTimeoutSeconds, int idleTimeoutSeconds,
                            int tcpKeepIdleSeconds, int tcpKeepIntervalSeconds, int tcpKeepCount,
                            int gracefulShutdownTimeoutSeconds, boolean jmxEnabled) {
        this.serverSocket = serverSocket;
        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.framing = framing;
        this.lineDelimiter = lineDelimiter;
        this.maxFrameLength = maxFrameLength;
        this.keepAlive = keepAlive;
        this.maxConnections = maxConnections;
        this.fixedFrameSize = fixedFrameSize;
        this.magicBytes = magicBytes == null ? new byte[0] : magicBytes;
        this.readTimeoutSeconds = readTimeoutSeconds;
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        this.tcpKeepIdleSeconds = tcpKeepIdleSeconds;
        this.tcpKeepIntervalSeconds = tcpKeepIntervalSeconds;
        this.tcpKeepCount = tcpKeepCount;
        this.gracefulShutdownTimeoutSeconds = gracefulShutdownTimeoutSeconds;
        this.jmxEnabled = jmxEnabled;
        this.metrics = new TcpChannelMetrics(this::activeConnectionCount);
    }

    public ServerSocket getServerSocket() { return serverSocket; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public boolean isTlsEnabled() { return tlsEnabled; }
    public Framing getFraming() { return framing; }
    public LineDelimiter getLineDelimiter() { return lineDelimiter; }
    public int getMaxFrameLength() { return maxFrameLength; }
    public boolean isKeepAlive() { return keepAlive; }
    public int getMaxConnections() { return maxConnections; }
    public int getFixedFrameSize() { return fixedFrameSize; }
    public byte[] getMagicBytes() { return magicBytes; }
    public int getReadTimeoutSeconds() { return readTimeoutSeconds; }
    public int getIdleTimeoutSeconds() { return idleTimeoutSeconds; }
    public int getTcpKeepIdleSeconds() { return tcpKeepIdleSeconds; }
    public int getTcpKeepIntervalSeconds() { return tcpKeepIntervalSeconds; }
    public int getTcpKeepCount() { return tcpKeepCount; }
    public int getGracefulShutdownTimeoutSeconds() { return gracefulShutdownTimeoutSeconds; }
    public boolean isJmxEnabled() { return jmxEnabled; }
    public TcpChannelMetrics getMetrics() { return metrics; }

    public void registerConnection(String id, Socket socket) {
        connections.put(id, new ConnectionEntry(socket));
        lastConnectionId.set(id);
    }

    public Socket unregisterConnection(String id) {
        if (id == null) return null;
        ConnectionEntry e = connections.remove(id);
        return e == null ? null : e.socket();
    }

    public Socket getConnection(String id) {
        if (id == null) return null;
        ConnectionEntry e = connections.get(id);
        return e == null ? null : e.socket();
    }

    public ConnectionEntry getEntry(String id) {
        if (id == null) return null;
        return connections.get(id);
    }

    public int activeConnectionCount() {
        return connections.size();
    }

    public String getLastConnectionId() {
        return lastConnectionId.get();
    }

    /**
     * Closes a single connection and removes it from the registry. Idempotent —
     * a missing id is a no-op. Used by the unified write-error path so write
     * failures from any of {@code onSuccess}, {@code <tcpc:write>}, or
     * {@code <tcpc:disconnect>} converge here.
     */
    public void closeAndUnregister(String id) {
        if (id == null) return;
        ConnectionEntry e = connections.remove(id);
        if (e == null) return;
        try {
            e.socket().close();
        } catch (IOException ignored) {
        }
    }

    public void closeAllConnections() {
        for (ConnectionEntry e : connections.values()) {
            try {
                e.socket().close();
            } catch (IOException ignored) {
            }
        }
        connections.clear();
    }

    /**
     * Half-closes the output side of every accepted socket. Sends FIN to the
     * peer so a well-behaved client knows we are shutting down and can finish
     * its in-flight reads. The peer's subsequent close drains us out via EOF
     * on the read loop. Idempotent and tolerant of already-closed sockets.
     */
    public void shutdownAllOutputs() {
        for (ConnectionEntry e : connections.values()) {
            Socket s = e.socket();
            try {
                if (!s.isClosed() && !s.isOutputShutdown()) {
                    s.shutdownOutput();
                }
            } catch (IOException ignored) {
            }
        }
    }

    public void close() {
        stopSweeper();
        unregisterMBean();
        closeAllConnections();
        try {
            if (!serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
    }

    public boolean isOpen() {
        return !serverSocket.isClosed();
    }

    public void startSweeper() {
        if (idleTimeoutSeconds <= 0) return;
        long periodSeconds = Math.max(idleTimeoutSeconds / 4, 5L);
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tcpc-sweeper-" + port);
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleAtFixedRate(this::sweepIdle, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        LOGGER.info("[tcpc] idle sweeper started (idleTimeout={}s, period={}s)",
                idleTimeoutSeconds, periodSeconds);
    }

    public void stopSweeper() {
        if (sweeper != null) {
            sweeper.shutdownNow();
            sweeper = null;
        }
    }

    void sweepIdle() {
        long thresholdNanos = TimeUnit.SECONDS.toNanos(idleTimeoutSeconds);
        long now = System.nanoTime();
        Iterator<Map.Entry<String, ConnectionEntry>> it = connections.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConnectionEntry> e = it.next();
            long idleNanos = now - e.getValue().lastActivityNanos();
            if (idleNanos > thresholdNanos) {
                LOGGER.info("[tcpc] sweeper closing idle connId={} (idle {}s)",
                        e.getKey(), TimeUnit.NANOSECONDS.toSeconds(idleNanos));
                try {
                    e.getValue().socket().close();
                } catch (IOException ignored) {
                }
                it.remove();
                metrics.incIdleClosed();
            }
        }
    }

    public void registerMBean() {
        if (!jmxEnabled) return;
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            mbeanName = new ObjectName(
                    "io.github.tmiya4ta.tcpchannel:type=Listener,port=" + port);
            if (server.isRegistered(mbeanName)) {
                server.unregisterMBean(mbeanName);
            }
            server.registerMBean(metrics, mbeanName);
            LOGGER.info("[tcpc] JMX MBean registered as {}", mbeanName);
        } catch (Exception e) {
            LOGGER.warn("[tcpc] failed to register JMX MBean: {}", e.getMessage());
            mbeanName = null;
        }
    }

    public void unregisterMBean() {
        if (mbeanName == null) return;
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbeanName);
        } catch (Exception ignored) {
        } finally {
            mbeanName = null;
        }
    }
}
