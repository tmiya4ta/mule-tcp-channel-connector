package io.github.tmiya4ta.tcpchannel.internal.source;

import io.github.tmiya4ta.tcpchannel.api.TcpChannelAttributes;
import io.github.tmiya4ta.tcpchannel.internal.connection.ConnectionEntry;
import io.github.tmiya4ta.tcpchannel.internal.connection.TcpChannelServer;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

import jdk.net.ExtendedSocketOptions;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.execution.OnError;
import org.mule.runtime.extension.api.annotation.execution.OnSuccess;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.mule.runtime.extension.api.runtime.source.SourceCallbackContext;
import org.mule.sdk.api.annotation.source.EmitsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Listens for TCP (or TLS) connections and dispatches each framed message to
 * the flow. Connections are kept open across messages until the client closes
 * the socket, an unrecoverable IO error occurs, the read timeout fires, or the
 * idle sweeper closes the connection.
 *
 * <p>Each accepted Socket is registered in the shared {@link TcpChannelServer}
 * so {@code <tcpc:write>} and {@code <tcpc:disconnect>} operations can address
 * the same socket by connectionId.
 */
@Alias("listener")
@EmitsResponse
@MediaType(value = MediaType.ANY, strict = false)
public class TcpChannelListener extends Source<byte[], TcpChannelAttributes> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpChannelListener.class);

    @Connection
    private ConnectionProvider<TcpChannelServer> connectionProvider;

    private TcpChannelServer server;
    private ExecutorService acceptor;
    private ExecutorService workers;
    private Semaphore connectionPermits;
    private volatile boolean running;
    private SourceCallback<byte[], TcpChannelAttributes> sourceCallback;

    @Override
    public void onStart(SourceCallback<byte[], TcpChannelAttributes> callback) throws ConnectionException {
        this.sourceCallback = callback;
        this.server = connectionProvider.connect();
        this.running = true;
        this.connectionPermits = new Semaphore(server.getMaxConnections());
        this.acceptor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "tcpc-acceptor-" + server.getPort());
            t.setDaemon(true);
            return t;
        });
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "tcpc-worker-" + server.getPort());
            t.setDaemon(true);
            return t;
        });
        acceptor.submit(this::acceptLoop);
        LOGGER.info("[tcpc] listener started on {}:{} tls={} framing={} delim={} maxFrame={} keepAlive={} maxConns={} readTimeout={}s idleTimeout={}s",
                server.getHost(), server.getPort(), server.isTlsEnabled(),
                server.getFraming(), server.getLineDelimiter(), server.getMaxFrameLength(),
                server.isKeepAlive(), server.getMaxConnections(),
                server.getReadTimeoutSeconds(), server.getIdleTimeoutSeconds());
    }

    @Override
    public void onStop() {
        this.running = false;
        if (server == null) {
            return;
        }
        // 1. Stop accepting new connections by closing the listening socket.
        try {
            server.getServerSocket().close();
        } catch (IOException ignored) {
        }
        if (acceptor != null) acceptor.shutdownNow();

        // 2. Stop the idle sweeper before we touch the connection map.
        server.stopSweeper();

        // 3. Half-close every accepted socket's output to signal peers, then
        //    let the read loops drain naturally on EOF.
        server.shutdownAllOutputs();

        // 4. Wait for read loops to exit with a hard cap.
        if (workers != null) {
            workers.shutdown();
            int waitSeconds = Math.max(server.getGracefulShutdownTimeoutSeconds(), 0);
            boolean drained = false;
            try {
                drained = workers.awaitTermination(waitSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (!drained) {
                LOGGER.warn("[tcpc] graceful drain timed out after {}s; force-closing {} connection(s)",
                        waitSeconds, server.activeConnectionCount());
                server.closeAllConnections();
                workers.shutdownNow();
            }
        }

        // 5. Tear down server (also unregisters MBean).
        connectionProvider.disconnect(server);
        LOGGER.info("[tcpc] listener stopped on {}:{}", server.getHost(), server.getPort());
        server = null;
    }

    private void acceptLoop() {
        while (running && server.isOpen()) {
            Socket socket;
            try {
                socket = server.getServerSocket().accept();
            } catch (SocketException e) {
                if (running) LOGGER.warn("[tcpc] accept failed: {}", e.getMessage());
                else break;
                continue;
            } catch (IOException e) {
                LOGGER.warn("[tcpc] accept IO error: {}", e.getMessage());
                continue;
            }

            // Reject when at the configured connection limit.
            if (!connectionPermits.tryAcquire()) {
                LOGGER.warn("[tcpc] connection limit ({}) reached; rejecting {} (active={})",
                        server.getMaxConnections(), socket.getRemoteSocketAddress(),
                        server.activeConnectionCount());
                server.getMetrics().incRejected();
                try { socket.close(); } catch (IOException ignored) {}
                continue;
            }

            applySocketOptions(socket);

            String connId = UUID.randomUUID().toString();
            server.registerConnection(connId, socket);
            server.getMetrics().incAccepted();
            LOGGER.info("[tcpc] accepted connId={} remote={} active={}/{}",
                    connId, socket.getRemoteSocketAddress(),
                    server.activeConnectionCount(), server.getMaxConnections());
            workers.submit(() -> {
                try {
                    if (socket instanceof SSLSocket) {
                        try {
                            ((SSLSocket) socket).startHandshake();
                        } catch (IOException e) {
                            LOGGER.warn("[tcpc] TLS handshake failed connId={}: {}", connId, e.getMessage());
                            server.closeAndUnregister(connId);
                            return;
                        }
                    }
                    readLoop(connId, socket);
                } finally {
                    connectionPermits.release();
                }
            });
        }
    }

    private void applySocketOptions(Socket socket) {
        try {
            if (server.getReadTimeoutSeconds() > 0) {
                socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(server.getReadTimeoutSeconds()));
            }
            if (server.isKeepAlive()) {
                socket.setKeepAlive(true);
                tryApplyKeepAliveTuning(socket);
            }
        } catch (SocketException e) {
            LOGGER.warn("[tcpc] failed to set socket options: {}", e.getMessage());
        }
    }

    /**
     * Best-effort TCP keepalive tuning via {@link ExtendedSocketOptions}. Only
     * available on platforms that expose them (Linux/macOS); silently skipped
     * otherwise. Each option is wrapped individually so a single unsupported
     * option does not block the others.
     */
    private void tryApplyKeepAliveTuning(Socket socket) {
        if (server.getTcpKeepIdleSeconds() > 0) {
            try {
                socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, server.getTcpKeepIdleSeconds());
            } catch (UnsupportedOperationException | IOException e) {
                LOGGER.debug("[tcpc] TCP_KEEPIDLE unsupported: {}", e.getMessage());
            }
        }
        if (server.getTcpKeepIntervalSeconds() > 0) {
            try {
                socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, server.getTcpKeepIntervalSeconds());
            } catch (UnsupportedOperationException | IOException e) {
                LOGGER.debug("[tcpc] TCP_KEEPINTERVAL unsupported: {}", e.getMessage());
            }
        }
        if (server.getTcpKeepCount() > 0) {
            try {
                socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, server.getTcpKeepCount());
            } catch (UnsupportedOperationException | IOException e) {
                LOGGER.debug("[tcpc] TCP_KEEPCOUNT unsupported: {}", e.getMessage());
            }
        }
    }

    private void readLoop(String connId, Socket socket) {
        AtomicLong msgIndex = new AtomicLong(0);
        String remote = String.valueOf(socket.getRemoteSocketAddress());
        ConnectionEntry entry = server.getEntry(connId);
        try (InputStream in = new BufferedInputStream(socket.getInputStream())) {
            while (running) {
                byte[] payload;
                try {
                    payload = FrameCodec.readFrame(in, server.getFraming(),
                            server.getLineDelimiter(), server.getMaxFrameLength(),
                            server.getFixedFrameSize(), server.getMagicBytes());
                } catch (SocketTimeoutException ste) {
                    LOGGER.warn("[tcpc] connId={} read timeout after {}s; closing",
                            connId, server.getReadTimeoutSeconds());
                    server.getMetrics().incReadTimeout();
                    return;
                }
                if (payload == null) {
                    return;
                }
                if (entry != null) entry.touch();
                long idx = msgIndex.incrementAndGet();
                server.getMetrics().incFramesReceived(payload.length);
                LOGGER.debug("[tcpc] connId={} msg#{} ({} bytes)", connId, idx, payload.length);
                SourceCallbackContext ctx = sourceCallback.createContext();
                ctx.addVariable("connectionId", connId);
                ctx.addVariable("messageIndex", idx);
                Result<byte[], TcpChannelAttributes> result = Result.<byte[], TcpChannelAttributes>builder()
                        .output(payload)
                        .attributes(new TcpChannelAttributes(connId, remote, idx))
                        .build();
                sourceCallback.handle(result, ctx);
            }
        } catch (IOException e) {
            if (running) {
                LOGGER.warn("[tcpc] connId={} read error: {}", connId, e.getMessage());
                server.getMetrics().incReadError();
            }
        } finally {
            server.closeAndUnregister(connId);
            LOGGER.info("[tcpc] connId={} closed", connId);
        }
    }

    @OnSuccess
    public void onSuccess(@Optional @Content InputStream response, SourceCallbackContext ctx) {
        String connId = ctx.<String>getVariable("connectionId").orElse(null);
        if (connId == null) return;
        ConnectionEntry entry = server.getEntry(connId);
        if (entry == null || entry.socket().isClosed()) {
            LOGGER.warn("[tcpc] onSuccess: socket already closed for connId={}", connId);
            return;
        }
        Socket socket = entry.socket();
        try {
            byte[] body = (response == null) ? new byte[0] : response.readAllBytes();
            OutputStream out = socket.getOutputStream();
            FrameCodec.writeFrame(out, server.getFraming(), server.getLineDelimiter(), body,
                    server.getFixedFrameSize(), server.getMagicBytes());
            out.flush();
            entry.touch();
            server.getMetrics().incFramesSent(body.length);
        } catch (IOException e) {
            LOGGER.warn("[tcpc] onSuccess write failed connId={}: {}", connId, e.getMessage());
            server.getMetrics().incWriteError();
            server.closeAndUnregister(connId);
        }
    }

    @OnError
    public void onError(SourceCallbackContext ctx) {
        String connId = ctx.<String>getVariable("connectionId").orElse(null);
        LOGGER.warn("[tcpc] flow error for connId={} — keeping connection open", connId);
    }
}
