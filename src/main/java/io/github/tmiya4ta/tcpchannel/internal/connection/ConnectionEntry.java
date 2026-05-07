package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection state held in the registry. Wraps the live {@link Socket} with
 * {@code lastActivityNanos} (used by the idle sweeper), a private write lock
 * that serialises all outbound frames for this connection, and an in-order
 * response queue that preserves request → response ordering even when the
 * Mule flow dispatches frames asynchronously.
 *
 * <p>Both the read loop (on every successful frame) and the write paths
 * (listener's onSuccess and the {@code <tcpc:write>} operation) call
 * {@link #touch()} so any direction of traffic resets the idle timer.
 *
 * <p>The write lock matters because multiple flow executions can target the
 * same connection concurrently — for example, the listener's {@code @OnSuccess}
 * writing a response on a Mule worker thread while an HTTP-driven
 * {@code <tcpc:write>} pushes an unsolicited frame on a different thread.
 * Without serialisation, their bytes would interleave on the underlying
 * {@link OutputStream} and corrupt the wire format. The lock is private to the
 * entry so different connections do not contend.
 *
 * <p><b>Response ordering.</b> Mule 4 EE runtime dispatches a source's
 * {@code sourceCallback.handle()} calls onto a worker pool, so when a peer
 * sends frames 1-5 quickly the {@code @OnSuccess} callbacks may complete in a
 * different order. {@link #writeOrderedFrame(long, ...)} stages each response
 * keyed by its incoming message index and flushes only contiguous entries
 * starting at {@code nextExpectedSeq}, guaranteeing the wire response order
 * matches the inbound request order.
 */
public final class ConnectionEntry {

    private final Socket socket;
    private final AtomicLong lastActivityNanos;
    private final Object writeLock = new Object();

    /** Next outbound response sequence (1-based, matches messageIndex). */
    private long nextExpectedSeq = 1;
    /** Responses completed out-of-order, awaiting their slot. */
    private final Map<Long, byte[]> pendingResponses = new HashMap<>();

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
     * Writes a single framed message immediately. Used by the
     * {@code <tcpc:write>} operation to push unsolicited frames — these have
     * no natural sequence number and should not be queued behind responses.
     * Bytes are still serialised under the write lock so they cannot
     * interleave with concurrent writers.
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

    /**
     * Stages a response frame under the write lock and flushes every
     * contiguously-numbered response that is now ready, starting at
     * {@code nextExpectedSeq}. Out-of-order completions wait silently in
     * {@code pendingResponses}; the next caller (or the same caller after
     * filling the gap) drains them.
     *
     * <p>This guarantees byte-for-byte that frame {@code N} is written before
     * frame {@code N+1} on the wire, regardless of which Mule worker thread
     * finished first.
     */
    public void writeOrderedFrame(long seq, Framing framing, LineDelimiter lineDelimiter,
                                  int fixedFrameSize, byte[] magicBytes, byte[] payload) throws IOException {
        synchronized (writeLock) {
            pendingResponses.put(seq, payload);
            OutputStream out = socket.getOutputStream();
            while (true) {
                byte[] ready = pendingResponses.remove(nextExpectedSeq);
                if (ready == null) return;
                FrameCodec.writeFrame(out, framing, lineDelimiter, ready, fixedFrameSize, magicBytes);
                out.flush();
                touch();
                nextExpectedSeq++;
            }
        }
    }

    /**
     * Visible for test diagnostics: how many responses are currently waiting
     * for their slot.
     */
    public int pendingResponseCount() {
        synchronized (writeLock) {
            return pendingResponses.size();
        }
    }
}
