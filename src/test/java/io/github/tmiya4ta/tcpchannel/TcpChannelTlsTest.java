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
