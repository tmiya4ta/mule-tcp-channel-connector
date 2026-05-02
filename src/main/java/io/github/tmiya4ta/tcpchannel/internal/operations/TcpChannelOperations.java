package io.github.tmiya4ta.tcpchannel.internal.operations;

import io.github.tmiya4ta.tcpchannel.internal.connection.TcpChannelServer;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static org.mule.runtime.extension.api.annotation.param.MediaType.TEXT_PLAIN;

public class TcpChannelOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpChannelOperations.class);

    /**
     * Writes a single framed message to the live socket identified by connectionId,
     * WITHOUT closing it. The framing is taken from the listener-config that owns
     * the connection (LINE or LENGTH_PREFIX).
     *
     * <p>The {@code data} parameter accepts any content the Mule transformer can
     * coerce to {@link InputStream} — String, byte[], InputStream, DataWeave
     * Binary, etc.
     */
    public void write(@Connection TcpChannelServer server,
                      String connectionId,
                      @Content InputStream data) {
        Socket socket = server.getConnection(connectionId);
        if (socket == null || socket.isClosed()) {
            throw new ModuleException("No live connection for id: " + connectionId,
                    TcpChannelErrors.CONNECTION_NOT_FOUND);
        }
        byte[] body;
        try {
            body = (data == null) ? new byte[0] : data.readAllBytes();
        } catch (IOException e) {
            throw new ModuleException("Failed to read input data", TcpChannelErrors.IO, e);
        }
        try {
            OutputStream out = socket.getOutputStream();
            FrameCodec.writeFrame(out, server.getFraming(), server.getLineDelimiter(), body);
            out.flush();
            LOGGER.info("[tcpc] write connId={} bytes={} framing={}",
                    connectionId, body.length, server.getFraming());
        } catch (IOException e) {
            LOGGER.warn("[tcpc] write failed connId={}: {}", connectionId, e.getMessage());
            throw new ModuleException("Write failed for connection " + connectionId,
                    TcpChannelErrors.IO, e);
        }
    }

    /**
     * Closes the socket identified by connectionId.
     */
    public void disconnect(@Connection TcpChannelServer server,
                           String connectionId) {
        Socket socket = server.unregisterConnection(connectionId);
        if (socket == null) {
            LOGGER.warn("[tcpc] disconnect: no live connection for id={}", connectionId);
            return;
        }
        try {
            socket.close();
            LOGGER.info("[tcpc] disconnect connId={} ok", connectionId);
        } catch (IOException e) {
            LOGGER.warn("[tcpc] disconnect connId={} error: {}", connectionId, e.getMessage());
        }
    }

    /**
     * Returns the most recently accepted connectionId (or null if none).
     * Convenience helper for demos and single-client tests; production code
     * should track ids via the listener attributes and an ObjectStore.
     */
    @MediaType(value = TEXT_PLAIN, strict = false)
    public String lastConnectionId(@Connection TcpChannelServer server) {
        return server.getLastConnectionId();
    }
}
