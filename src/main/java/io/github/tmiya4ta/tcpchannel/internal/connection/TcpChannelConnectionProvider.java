package io.github.tmiya4ta.tcpchannel.internal.connection;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.api.lifecycle.Initialisable;
import org.mule.runtime.api.lifecycle.InitialisationException;
import org.mule.runtime.api.tls.TlsContextFactory;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class TcpChannelConnectionProvider implements CachedConnectionProvider<TcpChannelServer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpChannelConnectionProvider.class);

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
    @Summary("Wire framing strategy. LINE for newline-delimited text, LENGTH_PREFIX for arbitrary binary, FIXED_LENGTH for fixed-size records.")
    private Framing framing;

    @Parameter
    @Optional(defaultValue = "LF")
    @Placement(order = 4)
    @Summary("Frame terminator for LINE framing. Ignored for LENGTH_PREFIX and FIXED_LENGTH.")
    private LineDelimiter lineDelimiter;

    @Parameter
    @Optional(defaultValue = "67108864")
    @Placement(order = 5)
    @Summary("Maximum size of a single frame, in bytes. Frames larger than this raise IO error and close the offending connection. Default: 64 MiB.")
    private int maxFrameLength;

    @Parameter
    @Optional(defaultValue = "true")
    @Placement(order = 6)
    @Summary("If true, sets SO_KEEPALIVE on every accepted client socket so the OS detects half-dead peers. Tune via tcpKeep* parameters.")
    private boolean keepAlive;

    @Parameter
    @Optional(defaultValue = "200")
    @Placement(order = 7)
    @Summary("Maximum number of concurrent client connections. New accepts beyond this are immediately closed.")
    private int maxConnections;

    @Parameter
    @Optional(defaultValue = "0")
    @Placement(order = 8)
    @Summary("Payload size in bytes for FIXED_LENGTH framing. Required when framing=FIXED_LENGTH; ignored otherwise.")
    private int fixedFrameSize;

    @Parameter
    @Optional(defaultValue = "")
    @Placement(order = 9)
    @Summary("Hex-encoded magic byte sequence prepended to every FIXED_LENGTH frame and used to resynchronise after garbage. Empty disables magic. Example: 'AABB'.")
    private String magicBytes;

    @Parameter
    @Optional(defaultValue = "0")
    @Placement(order = 10, tab = "Liveness")
    @Summary("SO_TIMEOUT on accepted sockets, in seconds. A blocking read that gets no data within this window throws SocketTimeoutException and the connection is closed. 0 disables (default).")
    private int readTimeoutSeconds;

    @Parameter
    @Optional(defaultValue = "0")
    @Placement(order = 11, tab = "Liveness")
    @Summary("Application-level idle timeout, in seconds. A background sweeper closes connections with no read or write activity for this long. 0 disables (default).")
    private int idleTimeoutSeconds;

    @Parameter
    @Optional(defaultValue = "0")
    @Placement(order = 12, tab = "Liveness")
    @Summary("TCP_KEEPIDLE: seconds of socket idleness before the OS sends the first keepalive probe. 0 = OS default (~2h on Linux). Effective only when keepAlive=true.")
    private int tcpKeepIdleSeconds;

    @Parameter
    @Optional(defaultValue = "0")
    @Placement(order = 13, tab = "Liveness")
    @Summary("TCP_KEEPINTVL: seconds between keepalive probes after the first. 0 = OS default. Effective only when keepAlive=true.")
    private int tcpKeepIntervalSeconds;

    @Parameter
    @Optional(defaultValue = "0")
    @Placement(order = 14, tab = "Liveness")
    @Summary("TCP_KEEPCNT: failed probe count before the OS declares the connection dead. 0 = OS default. Effective only when keepAlive=true.")
    private int tcpKeepCount;

    @Parameter
    @Optional(defaultValue = "5")
    @Placement(order = 15, tab = "Lifecycle")
    @Summary("Seconds to wait for in-flight reads to complete on stop/redeploy before forcing socket closure.")
    private int gracefulShutdownTimeoutSeconds;

    @Parameter
    @Optional(defaultValue = "true")
    @Placement(order = 16, tab = "Observability")
    @Summary("Register a JMX MBean exposing connector metrics under io.github.tmiya4ta.tcpchannel:type=Listener,port=<port>.")
    private boolean enableJmx;

    @Parameter
    @Optional
    @Placement(order = 17, tab = "TLS")
    @DisplayName("TLS Configuration")
    @Summary("If set, the listener binds an SSL ServerSocket using this TlsContextFactory. Omit for plain TCP.")
    private TlsContextFactory tlsContext;

    @Override
    public TcpChannelServer connect() throws ConnectionException {
        byte[] magic = parseHex(magicBytes);
        if (framing == Framing.FIXED_LENGTH) {
            if (fixedFrameSize <= 0) {
                throw new ConnectionException(
                        "framing=FIXED_LENGTH requires a positive fixedFrameSize");
            }
            if (fixedFrameSize > maxFrameLength) {
                throw new ConnectionException(
                        "fixedFrameSize (" + fixedFrameSize + ") exceeds maxFrameLength (" + maxFrameLength + ")");
            }
        }
        ServerSocket socket;
        boolean tls = tlsContext != null;
        try {
            if (tls) {
                socket = createSslServerSocket();
            } else {
                socket = new ServerSocket();
            }
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(host, port));
        } catch (IOException e) {
            throw new ConnectionException("Failed to bind TCP server on " + host + ":" + port, e);
        }
        TcpChannelServer server = new TcpChannelServer(socket, host, port, tls,
                framing, lineDelimiter, maxFrameLength, keepAlive, maxConnections,
                fixedFrameSize, magic,
                readTimeoutSeconds, idleTimeoutSeconds,
                tcpKeepIdleSeconds, tcpKeepIntervalSeconds, tcpKeepCount,
                gracefulShutdownTimeoutSeconds, enableJmx);
        server.startSweeper();
        server.registerMBean();
        return server;
    }

    private SSLServerSocket createSslServerSocket() throws ConnectionException, IOException {
        try {
            if (tlsContext instanceof Initialisable) {
                ((Initialisable) tlsContext).initialise();
            }
        } catch (InitialisationException e) {
            throw new ConnectionException("Failed to initialise TLS context", e);
        }
        SSLContext sslContext;
        try {
            sslContext = tlsContext.createSslContext();
        } catch (Exception e) {
            throw new ConnectionException("Failed to build SSLContext from tlsContext", e);
        }
        SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
        SSLServerSocket sslSocket = (SSLServerSocket) factory.createServerSocket();
        String[] enabledProtocols = tlsContext.getEnabledProtocols();
        if (enabledProtocols != null && enabledProtocols.length > 0) {
            sslSocket.setEnabledProtocols(enabledProtocols);
        }
        String[] enabledCiphers = tlsContext.getEnabledCipherSuites();
        if (enabledCiphers != null && enabledCiphers.length > 0) {
            sslSocket.setEnabledCipherSuites(enabledCiphers);
        }
        LOGGER.info("[tcpc] TLS enabled: protocols={} ciphers={}",
                enabledProtocols == null ? "default" : String.join(",", enabledProtocols),
                enabledCiphers == null ? "default" : enabledCiphers.length + " enabled");
        return sslSocket;
    }

    private static byte[] parseHex(String hex) throws ConnectionException {
        if (hex == null || hex.isEmpty()) return new byte[0];
        String s = hex.replace(" ", "").replace(":", "");
        if (s.length() % 2 != 0) {
            throw new ConnectionException("magicBytes must have an even number of hex digits: '" + hex + "'");
        }
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int hi = Character.digit(s.charAt(i * 2), 16);
            int lo = Character.digit(s.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new ConnectionException("magicBytes contains a non-hex character: '" + hex + "'");
            }
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
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
