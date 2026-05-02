package io.github.tmiya4ta.tcpchannel.internal.source;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.TcpChannelAttributes;
import io.github.tmiya4ta.tcpchannel.internal.connection.TcpChannelServer;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Listens for TCP connections and dispatches each framed message to the flow.
 * Connections are kept open across messages until the client closes the socket
 * or an unrecoverable IO error occurs. Framing is determined by the connection
 * provider (LINE / LENGTH_PREFIX).
 *
 * Each accepted Socket is registered in the shared {@link TcpChannelServer}
 * so that operations (write / disconnect) can address the same socket by id.
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
    private volatile boolean running;
    private SourceCallback<byte[], TcpChannelAttributes> sourceCallback;

    @Override
    public void onStart(SourceCallback<byte[], TcpChannelAttributes> callback) throws ConnectionException {
        this.sourceCallback = callback;
        this.server = connectionProvider.connect();
        this.running = true;
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
        LOGGER.info("[tcpc] listener started on {}:{} framing={}",
                server.getHost(), server.getPort(), server.getFraming());
    }

    @Override
    public void onStop() {
        this.running = false;
        if (server != null) {
            server.closeAllConnections();
        }
        if (acceptor != null) acceptor.shutdownNow();
        if (workers != null) workers.shutdownNow();
        if (server != null) {
            connectionProvider.disconnect(server);
            LOGGER.info("[tcpc] listener stopped on {}:{}", server.getHost(), server.getPort());
            server = null;
        }
    }

    private void acceptLoop() {
        while (running && server.isOpen()) {
            try {
                Socket socket = server.getServerSocket().accept();
                String connId = UUID.randomUUID().toString();
                server.registerConnection(connId, socket);
                LOGGER.info("[tcpc] accepted connId={} remote={}", connId, socket.getRemoteSocketAddress());
                workers.submit(() -> readLoop(connId, socket));
            } catch (SocketException e) {
                if (running) LOGGER.warn("[tcpc] accept failed: {}", e.getMessage());
                else break;
            } catch (IOException e) {
                LOGGER.warn("[tcpc] accept IO error: {}", e.getMessage());
            }
        }
    }

    private void readLoop(String connId, Socket socket) {
        AtomicLong msgIndex = new AtomicLong(0);
        String remote = String.valueOf(socket.getRemoteSocketAddress());
        Framing framing = server.getFraming();
        try (InputStream in = new BufferedInputStream(socket.getInputStream())) {
            while (running) {
                byte[] payload = FrameCodec.readFrame(in, framing);
                if (payload == null) {
                    break;
                }
                long idx = msgIndex.incrementAndGet();
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
            LOGGER.warn("[tcpc] connId={} read error: {}", connId, e.getMessage());
        } finally {
            server.unregisterConnection(connId);
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            LOGGER.info("[tcpc] connId={} closed", connId);
        }
    }

    @OnSuccess
    public void onSuccess(@Optional @Content InputStream response, SourceCallbackContext ctx) {
        String connId = ctx.<String>getVariable("connectionId").orElse(null);
        if (connId == null) return;
        Socket socket = server.getConnection(connId);
        if (socket == null || socket.isClosed()) {
            LOGGER.warn("[tcpc] onSuccess: socket already closed for connId={}", connId);
            return;
        }
        try {
            byte[] body = (response == null) ? new byte[0] : response.readAllBytes();
            OutputStream out = socket.getOutputStream();
            FrameCodec.writeFrame(out, server.getFraming(), body);
            out.flush();
        } catch (IOException e) {
            LOGGER.warn("[tcpc] onSuccess write failed connId={}: {}", connId, e.getMessage());
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            server.unregisterConnection(connId);
        }
    }

    @OnError
    public void onError(SourceCallbackContext ctx) {
        String connId = ctx.<String>getVariable("connectionId").orElse(null);
        LOGGER.warn("[tcpc] flow error for connId={} — keeping connection open", connId);
    }
}
