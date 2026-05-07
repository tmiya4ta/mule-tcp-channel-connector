package io.github.tmiya4ta.tcpchannel.internal.metrics;

/**
 * JMX management interface for {@link TcpChannelMetrics}. Exposes connector-level
 * counters and gauges for monitoring under
 * {@code io.github.tmiya4ta.tcpchannel:type=Listener,port=&lt;port&gt;}.
 */
public interface TcpChannelMetricsMBean {

    long getConnectionsAccepted();

    long getConnectionsRejected();

    long getConnectionsActive();

    long getFramesReceived();

    long getFramesSent();

    long getBytesReceived();

    long getBytesSent();

    long getReadTimeouts();

    long getIdleClosed();

    long getReadErrors();

    long getWriteErrors();
}
