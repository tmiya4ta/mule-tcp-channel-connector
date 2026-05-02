package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.mule.runtime.extension.api.annotation.param.display.Summary;

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
    @Summary("Wire framing strategy. LINE for newline-delimited text, LENGTH_PREFIX for arbitrary binary.")
    private Framing framing;

    @Parameter
    @Optional(defaultValue = "LF")
    @Placement(order = 4)
    @Summary("Frame terminator for LINE framing. Ignored for LENGTH_PREFIX.")
    private LineDelimiter lineDelimiter;

    @Parameter
    @Optional(defaultValue = "67108864")
    @Placement(order = 5)
    @Summary("Maximum size of a single frame, in bytes. Frames larger than this raise IO error and close the offending connection. Default: 64 MiB.")
    private int maxFrameLength;

    @Parameter
    @Optional(defaultValue = "true")
    @Placement(order = 6)
    @Summary("If true, sets SO_KEEPALIVE on every accepted client socket so that the OS detects half-dead peers (~2h on Linux defaults).")
    private boolean keepAlive;

    @Parameter
    @Optional(defaultValue = "200")
    @Placement(order = 7)
    @Summary("Maximum number of concurrent client connections. New accepts beyond this are immediately closed.")
    private int maxConnections;

    @Override
    public TcpChannelServer connect() throws ConnectionException {
        try {
            return new TcpChannelServer(host, port, framing, lineDelimiter,
                    maxFrameLength, keepAlive, maxConnections);
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
