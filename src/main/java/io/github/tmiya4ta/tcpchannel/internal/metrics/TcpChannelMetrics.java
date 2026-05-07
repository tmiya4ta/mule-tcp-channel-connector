package io.github.tmiya4ta.tcpchannel.internal.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Lock-free counter set for the TCP Channel listener. All increments are
 * thread-safe and the values are exposed read-only via {@link TcpChannelMetricsMBean}.
 *
 * The "active" gauge is a supplier so the registry's authoritative size is read
 * directly rather than mirrored.
 */
public final class TcpChannelMetrics implements TcpChannelMetricsMBean {

    private final AtomicLong connectionsAccepted = new AtomicLong();
    private final AtomicLong connectionsRejected = new AtomicLong();
    private final AtomicLong framesReceived = new AtomicLong();
    private final AtomicLong framesSent = new AtomicLong();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();
    private final AtomicLong readTimeouts = new AtomicLong();
    private final AtomicLong idleClosed = new AtomicLong();
    private final AtomicLong readErrors = new AtomicLong();
    private final AtomicLong writeErrors = new AtomicLong();

    private final LongSupplier activeConnectionsGauge;

    public TcpChannelMetrics(LongSupplier activeConnectionsGauge) {
        this.activeConnectionsGauge = activeConnectionsGauge;
    }

    public void incAccepted() { connectionsAccepted.incrementAndGet(); }
    public void incRejected() { connectionsRejected.incrementAndGet(); }
    public void incFramesReceived(long bytes) {
        framesReceived.incrementAndGet();
        bytesReceived.addAndGet(bytes);
    }
    public void incFramesSent(long bytes) {
        framesSent.incrementAndGet();
        bytesSent.addAndGet(bytes);
    }
    public void incReadTimeout() { readTimeouts.incrementAndGet(); }
    public void incIdleClosed() { idleClosed.incrementAndGet(); }
    public void incReadError() { readErrors.incrementAndGet(); }
    public void incWriteError() { writeErrors.incrementAndGet(); }

    @Override public long getConnectionsAccepted() { return connectionsAccepted.get(); }
    @Override public long getConnectionsRejected() { return connectionsRejected.get(); }
    @Override public long getConnectionsActive()   { return activeConnectionsGauge.getAsLong(); }
    @Override public long getFramesReceived()      { return framesReceived.get(); }
    @Override public long getFramesSent()          { return framesSent.get(); }
    @Override public long getBytesReceived()       { return bytesReceived.get(); }
    @Override public long getBytesSent()           { return bytesSent.get(); }
    @Override public long getReadTimeouts()        { return readTimeouts.get(); }
    @Override public long getIdleClosed()          { return idleClosed.get(); }
    @Override public long getReadErrors()          { return readErrors.get(); }
    @Override public long getWriteErrors()         { return writeErrors.get(); }
}
