package io.github.tmiya4ta.tcpchannel.api;

import java.io.Serializable;

public class TcpChannelAttributes implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String connectionId;
    private final String remoteAddress;
    private final long messageIndex;

    public TcpChannelAttributes(String connectionId, String remoteAddress, long messageIndex) {
        this.connectionId = connectionId;
        this.remoteAddress = remoteAddress;
        this.messageIndex = messageIndex;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public long getMessageIndex() {
        return messageIndex;
    }
}
