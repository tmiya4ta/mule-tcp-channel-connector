package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the bound ServerSocket plus all the connection-level settings shared
 * between the source and the operations: framing, delimiter, max frame size,
 * SO_KEEPALIVE, and the connection-id registry.
 *
 * Both the Source (which accepts and registers) and Operations (which write /
 * disconnect) share the same instance via {@code @Connection} injection.
 */
public class TcpChannelServer {

    private final ServerSocket serverSocket;
    private final String host;
    private final int port;
    private final Framing framing;
    private final LineDelimiter lineDelimiter;
    private final int maxFrameLength;
    private final boolean keepAlive;
    private final int maxConnections;
    private final int fixedFrameSize;
    private final byte[] magicBytes;

    private final ConcurrentHashMap<String, Socket> connections = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastConnectionId = new AtomicReference<>();

    public TcpChannelServer(String host, int port, Framing framing, LineDelimiter lineDelimiter,
                            int maxFrameLength, boolean keepAlive, int maxConnections,
                            int fixedFrameSize, byte[] magicBytes) throws IOException {
        this.host = host;
        this.port = port;
        this.framing = framing;
        this.lineDelimiter = lineDelimiter;
        this.maxFrameLength = maxFrameLength;
        this.keepAlive = keepAlive;
        this.maxConnections = maxConnections;
        this.fixedFrameSize = fixedFrameSize;
        this.magicBytes = magicBytes == null ? new byte[0] : magicBytes;
        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new InetSocketAddress(host, port));
    }

    public ServerSocket getServerSocket() { return serverSocket; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public Framing getFraming() { return framing; }
    public LineDelimiter getLineDelimiter() { return lineDelimiter; }
    public int getMaxFrameLength() { return maxFrameLength; }
    public boolean isKeepAlive() { return keepAlive; }
    public int getMaxConnections() { return maxConnections; }
    public int getFixedFrameSize() { return fixedFrameSize; }
    public byte[] getMagicBytes() { return magicBytes; }

    public void registerConnection(String id, Socket socket) {
        connections.put(id, socket);
        lastConnectionId.set(id);
    }

    public Socket unregisterConnection(String id) {
        return connections.remove(id);
    }

    public Socket getConnection(String id) {
        return connections.get(id);
    }

    public int activeConnectionCount() {
        return connections.size();
    }

    public String getLastConnectionId() {
        return lastConnectionId.get();
    }

    public void closeAllConnections() {
        for (Socket s : connections.values()) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
        connections.clear();
    }

    public void close() {
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
}
