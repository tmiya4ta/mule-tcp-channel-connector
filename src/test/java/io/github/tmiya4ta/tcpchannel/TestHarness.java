package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.connection.ConnectionEntry;
import io.github.tmiya4ta.tcpchannel.internal.connection.TcpChannelServer;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Thin in-process harness that drives a {@link TcpChannelServer} without
 * involving Mule's Source SDK. Mirrors what {@code TcpChannelListener} does:
 * acceptor + per-connection readLoop, applying socket options and pushing
 * frames into a queue the test can pop from. Lets the integration tests run
 * as plain JUnit.
 */
final class TestHarness implements AutoCloseable {

    final TcpChannelServer server;
    final BlockingQueue<byte[]> received = new LinkedBlockingQueue<>();
    private final ExecutorService acceptor;
    private final ExecutorService workers;
    private final Semaphore permits;
    private volatile boolean running = true;

    static TestHarness start(Builder b) throws IOException {
        ServerSocket socket;
        if (b.serverSocketSupplier != null) {
            socket = b.serverSocketSupplier.get();
        } else {
            socket = new ServerSocket();
        }
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress("127.0.0.1", 0));
        TcpChannelServer s = new TcpChannelServer(socket, "127.0.0.1", socket.getLocalPort(),
                b.tlsEnabled,
                b.framing, b.lineDelimiter, b.maxFrameLength, b.keepAlive, b.maxConnections,
                b.fixedFrameSize, b.magicBytes,
                b.readTimeoutSeconds, b.idleTimeoutSeconds,
                0, 0, 0, b.gracefulShutdownTimeoutSeconds, false);
        s.startSweeper();
        TestHarness h = new TestHarness(s);
        h.acceptor.submit(h::acceptLoop);
        return h;
    }

    private TestHarness(TcpChannelServer s) {
        this.server = s;
        this.permits = new Semaphore(s.getMaxConnections());
        this.acceptor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "harness-acceptor-" + s.getPort());
            t.setDaemon(true);
            return t;
        });
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "harness-worker-" + s.getPort());
            t.setDaemon(true);
            return t;
        });
    }

    int port() {
        return server.getPort();
    }

    private void acceptLoop() {
        while (running && server.isOpen()) {
            Socket socket;
            try {
                socket = server.getServerSocket().accept();
            } catch (IOException e) {
                if (!running) return;
                continue;
            }
            if (!permits.tryAcquire()) {
                server.getMetrics().incRejected();
                try { socket.close(); } catch (IOException ignored) {}
                continue;
            }
            try {
                if (server.getReadTimeoutSeconds() > 0) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(server.getReadTimeoutSeconds()));
                }
                if (server.isKeepAlive()) {
                    socket.setKeepAlive(true);
                }
            } catch (IOException ignored) {
            }
            String connId = UUID.randomUUID().toString();
            server.registerConnection(connId, socket);
            server.getMetrics().incAccepted();
            workers.submit(() -> {
                try {
                    if (socket instanceof javax.net.ssl.SSLSocket) {
                        try {
                            ((javax.net.ssl.SSLSocket) socket).startHandshake();
                        } catch (IOException e) {
                            server.closeAndUnregister(connId);
                            return;
                        }
                    }
                    readLoop(connId, socket);
                } finally {
                    permits.release();
                }
            });
        }
    }

    private void readLoop(String connId, Socket socket) {
        ConnectionEntry entry = server.getEntry(connId);
        try (InputStream in = new BufferedInputStream(socket.getInputStream())) {
            while (running) {
                byte[] payload;
                try {
                    payload = FrameCodec.readFrame(in, server.getFraming(),
                            server.getLineDelimiter(), server.getMaxFrameLength(),
                            server.getFixedFrameSize(), server.getMagicBytes());
                } catch (SocketTimeoutException ste) {
                    server.getMetrics().incReadTimeout();
                    return;
                }
                if (payload == null) return;
                if (entry != null) entry.touch();
                server.getMetrics().incFramesReceived(payload.length);
                received.offer(payload);
                if (echoMode && entry != null) {
                    try {
                        entry.writeFrame(server.getFraming(), server.getLineDelimiter(),
                                server.getFixedFrameSize(), server.getMagicBytes(), payload);
                        server.getMetrics().incFramesSent(payload.length);
                    } catch (IOException e) {
                        server.getMetrics().incWriteError();
                        server.closeAndUnregister(connId);
                        return;
                    }
                }
            }
        } catch (IOException e) {
            if (running) server.getMetrics().incReadError();
        } finally {
            server.closeAndUnregister(connId);
        }
    }

    private boolean echoMode;
    void enableEcho() { this.echoMode = true; }

    @Override
    public void close() {
        running = false;
        try { server.getServerSocket().close(); } catch (IOException ignored) {}
        acceptor.shutdownNow();
        server.stopSweeper();
        server.shutdownAllOutputs();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(server.getGracefulShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                server.closeAllConnections();
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        server.close();
    }

    static final class Builder {
        Framing framing = Framing.LINE;
        LineDelimiter lineDelimiter = LineDelimiter.LF;
        int maxFrameLength = 65536;
        boolean keepAlive = false;
        int maxConnections = 200;
        int fixedFrameSize = 0;
        byte[] magicBytes = new byte[0];
        int readTimeoutSeconds = 0;
        int idleTimeoutSeconds = 0;
        int gracefulShutdownTimeoutSeconds = 2;
        boolean tlsEnabled = false;
        java.util.function.Supplier<ServerSocket> serverSocketSupplier;

        Builder maxConnections(int n) { this.maxConnections = n; return this; }
        Builder readTimeout(int s) { this.readTimeoutSeconds = s; return this; }
        Builder idleTimeout(int s) { this.idleTimeoutSeconds = s; return this; }
        Builder gracefulShutdown(int s) { this.gracefulShutdownTimeoutSeconds = s; return this; }
        Builder tls(java.util.function.Supplier<ServerSocket> sup) { this.tlsEnabled = true; this.serverSocketSupplier = sup; return this; }
    }
}
