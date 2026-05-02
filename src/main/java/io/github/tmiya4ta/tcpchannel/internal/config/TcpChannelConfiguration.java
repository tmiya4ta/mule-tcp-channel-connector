package io.github.tmiya4ta.tcpchannel.internal.config;

import io.github.tmiya4ta.tcpchannel.internal.connection.TcpChannelConnectionProvider;
import io.github.tmiya4ta.tcpchannel.internal.operations.TcpChannelOperations;
import io.github.tmiya4ta.tcpchannel.internal.source.TcpChannelListener;
import org.mule.runtime.extension.api.annotation.Configuration;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.Sources;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;

@Configuration(name = "listener-config")
@ConnectionProviders(TcpChannelConnectionProvider.class)
@Sources(TcpChannelListener.class)
@Operations(TcpChannelOperations.class)
public class TcpChannelConfiguration {
}
