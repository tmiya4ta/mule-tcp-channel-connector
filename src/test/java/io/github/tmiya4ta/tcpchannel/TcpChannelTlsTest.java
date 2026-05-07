package io.github.tmiya4ta.tcpchannel;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke-tests TLS wiring: build an SSL ServerSocket from a self-signed test
 * keystore, connect with a trust-all SSLSocket, send one LINE frame,
 * confirm it lands on the harness queue.
 */
class TcpChannelTlsTest {

    private static final String KEYSTORE = "/test-keystore.p12";
    private static final char[] PASS = "changeit".toCharArray();

    @Test
    @DisplayName("TLS listener accepts a TLS client and decodes one LINE frame")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void tlsHandshakeAndFrame() throws Exception {
        SSLContext serverCtx = serverContext();
        TestHarness.Builder b = new TestHarness.Builder();
        b.tls(() -> {
            try {
                SSLServerSocketFactory f = serverCtx.getServerSocketFactory();
                SSLServerSocket s = (SSLServerSocket) f.createServerSocket();
                return s;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        try (TestHarness h = TestHarness.start(b)) {
            SSLContext clientCtx = trustAllContext();
            try (SSLSocket client = (SSLSocket) clientCtx.getSocketFactory()
                    .createSocket("127.0.0.1", h.port())) {
                client.startHandshake();
                client.getOutputStream().write("encrypted-hello\n".getBytes());
                client.getOutputStream().flush();
                byte[] received = h.received.poll(3, TimeUnit.SECONDS);
                assertNotNull(received, "TLS frame should have been decoded");
                assertArrayEquals("encrypted-hello".getBytes(), received);
            }
        }
    }

    @Test
    @DisplayName("Plain client connecting to TLS port: handshake fails, server stays up")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void plainClientToTlsPortFailsHandshakeButLeavesServerAlive() throws Exception {
        SSLContext serverCtx = serverContext();
        TestHarness.Builder b = new TestHarness.Builder();
        b.tls(() -> {
            try {
                return (SSLServerSocket) serverCtx.getServerSocketFactory().createServerSocket();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        try (TestHarness h = TestHarness.start(b)) {
            // Plain TCP client speaking nonsense to a TLS port.
            try (Socket plain = new Socket("127.0.0.1", h.port())) {
                OutputStream out = plain.getOutputStream();
                out.write("not-a-tls-handshake\r\n".getBytes());
                out.flush();
            } catch (IOException ignored) {
            }

            // After the rogue connection, a legitimate TLS client should still
            // be able to handshake and exchange a frame.
            SSLContext clientCtx = trustAllContext();
            try (SSLSocket good = (SSLSocket) clientCtx.getSocketFactory()
                    .createSocket("127.0.0.1", h.port())) {
                good.startHandshake();
                good.getOutputStream().write("after-bogus\n".getBytes());
                good.getOutputStream().flush();
                byte[] received = h.received.poll(3, TimeUnit.SECONDS);
                assertNotNull(received, "good client must succeed after rogue");
                assertArrayEquals("after-bogus".getBytes(), received);
            }

            // Settle and verify the rogue did not leak a registry entry.
            long deadline = System.currentTimeMillis() + 2_000;
            while (h.server.activeConnectionCount() > 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(0, h.server.activeConnectionCount());
        }
    }

    @Test
    @DisplayName("Truncated TLS handshake (1 byte then close) does not hang the server")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void truncatedHandshakeRecovers() throws Exception {
        SSLContext serverCtx = serverContext();
        TestHarness.Builder b = new TestHarness.Builder();
        b.tls(() -> {
            try {
                return (SSLServerSocket) serverCtx.getServerSocketFactory().createServerSocket();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        try (TestHarness h = TestHarness.start(b)) {
            for (int i = 0; i < 10; i++) {
                try (Socket plain = new Socket("127.0.0.1", h.port())) {
                    plain.getOutputStream().write(0x16); // ClientHello record type byte
                    plain.getOutputStream().flush();
                } catch (IOException ignored) {
                }
            }
            // The server's accept-side handshakes must all fail and clean up.
            long deadline = System.currentTimeMillis() + 3_000;
            while (h.server.activeConnectionCount() > 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(0, h.server.activeConnectionCount(),
                    "truncated-handshake clients must be cleaned up");
        }
    }

    private static SSLContext serverContext() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = TcpChannelTlsTest.class.getResourceAsStream(KEYSTORE)) {
            assertNotNull(in, "test keystore missing on classpath: " + KEYSTORE);
            ks.load(in, PASS);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, PASS);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, new SecureRandom());
        return ctx;
    }

    private static SSLContext trustAllContext() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            public void checkClientTrusted(X509Certificate[] c, String t) { }
            public void checkServerTrusted(X509Certificate[] c, String t) { }
        }};
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAll, new SecureRandom());
        return ctx;
    }
}
