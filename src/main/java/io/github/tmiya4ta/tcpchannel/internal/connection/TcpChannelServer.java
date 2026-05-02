package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the bound ServerSocket, the active framing strategy, and a registry of
 * accepted client sockets keyed by connectionId. Both the Source (which accepts
 * and registers) and Operations (which write/disconnect) share the same instance
 * via @Connection injection.
 */
public class TcpChannelServer {

    private final ServerSocket serverSocket;
    private final String host;
    private final int port;
    private final Framing framing;

    private final ConcurrentHashMap<String, Socket> connections = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastConnectionId = new AtomicReference<>();

    public TcpChannelServer(String host, int port, Framing framing) throws IOException {
        this.host = host;
        this.port = port;
        this.framing = framing;
        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new InetSocketAddress(host, port));
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Framing getFraming() {
        return framing;
    }

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
