package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection state held in the registry. Wraps the live {@link Socket} with
 * {@code lastActivityNanos} (used by the idle sweeper) and a private write lock
 * that serialises all outbound frames for this connection.
 *
 * <p>Both the read loop (on every successful frame) and the write paths
 * (listener's onSuccess and the {@code <tcpc:write>} operation) call
 * {@link #touch()} so any direction of traffic resets the idle timer.
 *
 * <p>The write lock matters because multiple flow executions can target the
 * same connection concurrently — for example, the listener's {@code @OnSuccess}
 * writing a response on the read-loop thread while an HTTP-driven
 * {@code <tcpc:write>} pushes an unsolicited frame on a different thread.
 * Without serialisation, their bytes would interleave on the underlying
 * {@link OutputStream} and corrupt the wire format. The lock is private to the
 * entry so different connections do not contend.
 */
public final class ConnectionEntry {

    private final Socket socket;
    private final AtomicLong lastActivityNanos;
    private final Object writeLock = new Object();

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

    /**
     * Writes a single framed message to this connection. The frame is encoded,
     * flushed, and {@link #touch()}ed under the per-entry write lock so two
     * concurrent callers can never interleave bytes on the shared
     * {@link OutputStream}.
     */
    public void writeFrame(Framing framing, LineDelimiter lineDelimiter,
                           int fixedFrameSize, byte[] magicBytes, byte[] payload) throws IOException {
        synchronized (writeLock) {
            OutputStream out = socket.getOutputStream();
            FrameCodec.writeFrame(out, framing, lineDelimiter, payload, fixedFrameSize, magicBytes);
            out.flush();
            touch();
        }
    }
}
