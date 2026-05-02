package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.Placement;

import java.io.IOException;

public class TcpChannelConnectionProvider implements CachedConnectionProvider<TcpChannelServer> {

    @Parameter
    @Optional(defaultValue = "0.0.0.0")
    @Placement(order = 1)
    private String host;

    @Parameter
    @Placement(order = 2)
    private int port;

    @Parameter
    @Optional(defaultValue = "LINE")
    @Placement(order = 3)
    private Framing framing;

    @Override
    public TcpChannelServer connect() throws ConnectionException {
        try {
            return new TcpChannelServer(host, port, framing);
        } catch (IOException e) {
            throw new ConnectionException("Failed to bind TCP server on " + host + ":" + port, e);
        }
    }

    @Override
    public void disconnect(TcpChannelServer server) {
        if (server != null) {
            server.close();
        }
    }

    @Override
    public ConnectionValidationResult validate(TcpChannelServer server) {
        if (server != null && server.isOpen()) {
            return ConnectionValidationResult.success();
        }
        return ConnectionValidationResult.failure("ServerSocket is closed", new IOException("closed"));
    }
}
