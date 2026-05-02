package io.github.tmiya4ta.tcpchannel.internal.operations;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

public enum TcpChannelErrors implements ErrorTypeDefinition<TcpChannelErrors> {
    CONNECTION_NOT_FOUND,
    IO
}
