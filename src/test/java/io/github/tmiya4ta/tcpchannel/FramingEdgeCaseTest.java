package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests on the codec and the listener that we'd lose sleep over in
 * a telco-grade deployment:
 * <ul>
 *   <li>FIXED_LENGTH payload that happens to contain the magic byte sequence</li>
 *   <li>fixedFrameSize=1 (smallest possible)</li>
 *   <li>LENGTH_PREFIX with length=0 in mid-stream</li>
 *   <li>Stream that arrives in tiny pieces (slow reader)</li>
 * </ul>
 */
class FramingEdgeCaseTest {

    private static final int MAX = 65536;

    @Test
    @DisplayName("FIXED_LENGTH: magic byte sequence appearing inside payload does NOT trigger mid-frame resync")
    void magicInPayloadIsNotResync() throws Exception {
        // Magic = 0xAA 0xBB. Payload of size 16 deliberately CONTAINS 0xAA 0xBB.
        // After the leading magic match, the codec must read the next 16 bytes
        // as opaque payload, not try to re-match magic.
        byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
        byte[] payload = new byte[]{
                'a', 'b', (byte) 0xAA, (byte) 0xBB, 'c', 'd', 'e', 'f',
                'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'
        };
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.write(magic);
        wire.write(payload);

        byte[] decoded = FrameCodec.readFrame(new ByteArrayInputStream(wire.toByteArray()),
                Framing.FIXED_LENGTH, LineDelimiter.LF, MAX, 16, magic);
        assertArrayEquals(payload, decoded);
    }

    @Test
    @DisplayName("FIXED_LENGTH with size=1 round-trips")
    void fixedLengthSizeOne() throws Exception {
        byte[] magic = new byte[]{(byte) 0xCA, (byte) 0xFE};
        byte[] payload = new byte[]{(byte) 0xAB};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeFrame(out, Framing.FIXED_LENGTH, LineDelimiter.LF, payload, 1, magic);
        byte[] decoded = FrameCodec.readFrame(new ByteArrayInputStream(out.toByteArray()),
                Framing.FIXED_LENGTH, LineDelimiter.LF, MAX, 1, magic);
        assertArrayEquals(payload, decoded);
    }

    @Test
    @DisplayName("LENGTH_PREFIX: length=0 in mid-stream is delivered as an empty frame, not silence")
    void lengthPrefixZeroInMidStream() throws Exception {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        FrameCodec.writeFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF, "first".getBytes(), 0, new byte[0]);
        FrameCodec.writeFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF, new byte[0], 0, new byte[0]);
        FrameCodec.writeFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF, "third".getBytes(), 0, new byte[0]);
        InputStream in = new ByteArrayInputStream(wire.toByteArray());
        assertArrayEquals("first".getBytes(),
                FrameCodec.readFrame(in, Framing.LENGTH_PREFIX, LineDelimiter.LF, MAX, 0, new byte[0]));
        assertArrayEquals(new byte[0],
                FrameCodec.readFrame(in, Framing.LENGTH_PREFIX, LineDelimiter.LF, MAX, 0, new byte[0]));
        assertArrayEquals("third".getBytes(),
                FrameCodec.readFrame(in, Framing.LENGTH_PREFIX, LineDelimiter.LF, MAX, 0, new byte[0]));
    }

    @Test
    @DisplayName("Codec handles a stream that delivers bytes one at a time with delays")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void slowByteStream() throws Exception {
        // Produce LENGTH_PREFIX frame "0123456789" through a piped stream
        // where the producer writes one byte every few ms. Reader must block
        // and assemble correctly.
        byte[] payload = "0123456789".getBytes();
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        FrameCodec.writeFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF, payload, 0, new byte[0]);
        byte[] full = wire.toByteArray();

        PipedOutputStream pout = new PipedOutputStream();
        PipedInputStream pin = new PipedInputStream(pout, 64);

        Thread producer = new Thread(() -> {
            try {
                for (byte b : full) {
                    pout.write(b & 0xff);
                    pout.flush();
                    Thread.sleep(2);
                }
                pout.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        producer.start();
        byte[] decoded = FrameCodec.readFrame(pin, Framing.LENGTH_PREFIX, LineDelimiter.LF, MAX, 0, new byte[0]);
        producer.join();
        assertArrayEquals(payload, decoded);
    }

    @Test
    @DisplayName("LINE LF: empty line (just '\\n') is delivered as zero-byte payload, not skipped")
    void lineEmptyFrameIsDelivered() throws Exception {
        InputStream in = new ByteArrayInputStream(new byte[]{'\n', 'a', '\n'});
        byte[] f1 = FrameCodec.readFrame(in, Framing.LINE, LineDelimiter.LF, MAX, 0, new byte[0]);
        byte[] f2 = FrameCodec.readFrame(in, Framing.LINE, LineDelimiter.LF, MAX, 0, new byte[0]);
        assertArrayEquals(new byte[0], f1, "first frame should be empty");
        assertArrayEquals("a".getBytes(), f2);
    }

    @Test
    @DisplayName("FIXED_LENGTH end-to-end: payload containing magic does not corrupt subsequent frames")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void fixedLengthMultiFrameWithMagicInsidePayload() throws Exception {
        TestHarness.Builder b = new TestHarness.Builder();
        b.framing = Framing.FIXED_LENGTH;
        b.fixedFrameSize = 8;
        b.magicBytes = new byte[]{(byte) 0xAA, (byte) 0xBB};
        try (TestHarness h = TestHarness.start(b);
             Socket client = new Socket("127.0.0.1", h.port())) {
            OutputStream out = client.getOutputStream();
            byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
            byte[] frame1 = new byte[]{1, 2, (byte) 0xAA, (byte) 0xBB, 5, 6, 7, 8};
            byte[] frame2 = new byte[]{(byte) 0xAA, (byte) 0xBB, 0, 0, 0, 0, 0, 0};
            byte[] frame3 = new byte[]{9, 10, 11, 12, 13, 14, 15, 16};
            out.write(magic); out.write(frame1);
            out.write(magic); out.write(frame2);
            out.write(magic); out.write(frame3);
            out.flush();
            assertArrayEquals(frame1, h.received.poll(2, TimeUnit.SECONDS));
            assertArrayEquals(frame2, h.received.poll(2, TimeUnit.SECONDS));
            assertArrayEquals(frame3, h.received.poll(2, TimeUnit.SECONDS));
        }
    }

    @Test
    @DisplayName("FrameCodec.writeFrame is null-payload safe via empty byte array")
    void writeNullPayloadGuard() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // The connector code converts null → byte[0] before calling. This test
        // pins that contract: empty payload encodes to a 4-byte length prefix
        // of 0 with no body.
        FrameCodec.writeFrame(out, Framing.LENGTH_PREFIX, LineDelimiter.LF, new byte[0], 0, new byte[0]);
        assertArrayEquals(new byte[]{0, 0, 0, 0}, out.toByteArray());
    }

    @Test
    @DisplayName("LENGTH_PREFIX: a frame of exactly maxFrameLength is allowed; one over rejects")
    void lengthPrefixBoundary() throws Exception {
        byte[] payload = new byte[100];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;

        // Allowed: == maxLen
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        FrameCodec.writeFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF, payload, 0, new byte[0]);
        byte[] decoded = FrameCodec.readFrame(new ByteArrayInputStream(wire.toByteArray()),
                Framing.LENGTH_PREFIX, LineDelimiter.LF, 100, 0, new byte[0]);
        assertArrayEquals(payload, decoded);

        // Rejected: one byte over
        byte[] over = new byte[101];
        ByteArrayOutputStream wire2 = new ByteArrayOutputStream();
        FrameCodec.writeFrame(wire2, Framing.LENGTH_PREFIX, LineDelimiter.LF, over, 0, new byte[0]);
        assertThrows(IOException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(wire2.toByteArray()),
                        Framing.LENGTH_PREFIX, LineDelimiter.LF, 100, 0, new byte[0]));
    }
}
