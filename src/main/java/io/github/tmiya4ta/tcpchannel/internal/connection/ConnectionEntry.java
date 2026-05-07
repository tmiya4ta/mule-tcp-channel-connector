package io.github.tmiya4ta.tcpchannel.internal.connection;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection state held in the registry. Wraps the live {@link Socket} with
 * a monotonically updated {@code lastActivityNanos} timestamp used by the idle
 * sweeper to detect dead/silent peers.
 *
 * <p>Both the read loop (on every successful frame) and the write paths
 * (listener's onSuccess and the {@code <tcpc:write>} operation) call
 * {@link #touch()} so any direction of traffic resets the idle timer.
 */
public final class ConnectionEntry {

    private final Socket socket;
    private final AtomicLong lastActivityNanos;

    public ConnectionEntry(Socket socket) {
        this.socket = socket;
        this.lastActivityNanos = new AtomicLong(System.nanoTime());
    }

    public Socket socket() {
        return socket;
    }

    public long lastActivityNanos() {
        return lastActivityNanos.get();
    }

    public void touch() {
        lastActivityNanos.set(System.nanoTime());
    }
}
