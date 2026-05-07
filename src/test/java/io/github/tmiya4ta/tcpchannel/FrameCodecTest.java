package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.framing.FrameCodec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {

    private static final int MAX = 4096;
    private static final byte[] NO_MAGIC = new byte[0];

    private static byte[] readFrame(byte[] in, Framing f, LineDelimiter d) throws IOException {
        return FrameCodec.readFrame(new ByteArrayInputStream(in), f, d, MAX, 0, NO_MAGIC);
    }

    private static byte[] readFixed(byte[] in, int size, byte[] magic) throws IOException {
        return FrameCodec.readFrame(new ByteArrayInputStream(in), Framing.FIXED_LENGTH,
                LineDelimiter.LF, MAX, size, magic);
    }

    private static byte[] writeFrame(byte[] payload, Framing f, LineDelimiter d) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeFrame(out, f, d, payload, 0, NO_MAGIC);
        return out.toByteArray();
    }

    @Nested
    @DisplayName("LINE framing")
    class LineFraming {

        @Test
        void readLfFrame() throws Exception {
            byte[] r = readFrame("hello\nworld\n".getBytes(), Framing.LINE, LineDelimiter.LF);
            assertArrayEquals("hello".getBytes(), r);
        }

        @Test
        void readLfStripsTrailingCr() throws Exception {
            byte[] r = readFrame("hello\r\n".getBytes(), Framing.LINE, LineDelimiter.LF);
            assertArrayEquals("hello".getBytes(), r);
        }

        @Test
        void readCrlfFrame() throws Exception {
            byte[] r = readFrame("hello\r\nworld\r\n".getBytes(), Framing.LINE, LineDelimiter.CRLF);
            assertArrayEquals("hello".getBytes(), r);
        }

        @Test
        void readCrlfDoesNotMatchBareLf() throws Exception {
            byte[] r = readFrame("hello\nworld\r\n".getBytes(), Framing.LINE, LineDelimiter.CRLF);
            assertArrayEquals("hello\nworld".getBytes(), r);
        }

        @Test
        void readNulFrame() throws Exception {
            byte[] r = readFrame(new byte[]{'a', 'b', 'c', 0, 'd'}, Framing.LINE, LineDelimiter.NUL);
            assertArrayEquals("abc".getBytes(), r);
        }

        @Test
        void emptyStreamReturnsNull() throws Exception {
            assertNull(readFrame(new byte[0], Framing.LINE, LineDelimiter.LF));
        }

        @Test
        void writeLfAppendsTerminator() throws Exception {
            assertArrayEquals("hello\n".getBytes(), writeFrame("hello".getBytes(), Framing.LINE, LineDelimiter.LF));
        }

        @Test
        void writeLfDoesNotDoubleTerminator() throws Exception {
            assertArrayEquals("hello\n".getBytes(), writeFrame("hello\n".getBytes(), Framing.LINE, LineDelimiter.LF));
        }

        @Test
        void writeCrlfAppendsTerminator() throws Exception {
            assertArrayEquals("hi\r\n".getBytes(), writeFrame("hi".getBytes(), Framing.LINE, LineDelimiter.CRLF));
        }

        @Test
        void writeNulAppendsZero() throws Exception {
            byte[] expected = new byte[]{'a', 0};
            assertArrayEquals(expected, writeFrame("a".getBytes(), Framing.LINE, LineDelimiter.NUL));
        }

        @Test
        void overlongLineRaisesIoException() {
            byte[] bigInput = new byte[MAX + 100];
            for (int i = 0; i < bigInput.length; i++) bigInput[i] = 'x';
            assertThrows(IOException.class,
                    () -> readFrame(bigInput, Framing.LINE, LineDelimiter.LF));
        }
    }

    @Nested
    @DisplayName("LENGTH_PREFIX framing")
    class LengthPrefixFraming {

        @Test
        void roundTrip() throws Exception {
            byte[] payload = "binary-payload".getBytes();
            byte[] wire = writeFrame(payload, Framing.LENGTH_PREFIX, LineDelimiter.LF);
            byte[] decoded = readFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF);
            assertArrayEquals(payload, decoded);
        }

        @Test
        void zeroLengthPayload() throws Exception {
            byte[] wire = writeFrame(new byte[0], Framing.LENGTH_PREFIX, LineDelimiter.LF);
            assertEquals(4, wire.length);
            byte[] decoded = readFrame(wire, Framing.LENGTH_PREFIX, LineDelimiter.LF);
            assertArrayEquals(new byte[0], decoded);
        }

        @Test
        void cleanEofReturnsNull() throws Exception {
            assertNull(readFrame(new byte[0], Framing.LENGTH_PREFIX, LineDelimiter.LF));
        }

        @Test
        void midFrameEofThrows() {
            byte[] truncated = new byte[]{0, 0, 0, 10, 'a', 'b'};
            assertThrows(IOException.class,
                    () -> readFrame(truncated, Framing.LENGTH_PREFIX, LineDelimiter.LF));
        }

        @Test
        void overlongFrameRejected() {
            byte[] huge = new byte[]{0, 1, 0, 0};
            assertThrows(IOException.class,
                    () -> readFrame(huge, Framing.LENGTH_PREFIX, LineDelimiter.LF));
        }
    }

    @Nested
    @DisplayName("FIXED_LENGTH framing")
    class FixedLengthFraming {

        @Test
        void roundTripWithMagic() throws Exception {
            byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
            byte[] payload = "0123456789ABCDEF".getBytes();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            FrameCodec.writeFrame(out, Framing.FIXED_LENGTH, LineDelimiter.LF, payload, 16, magic);
            byte[] wire = out.toByteArray();
            assertEquals(2 + 16, wire.length);
            byte[] decoded = readFixed(wire, 16, magic);
            assertArrayEquals(payload, decoded);
        }

        @Test
        void resyncSkipsGarbageBeforeMagic() throws Exception {
            byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
            byte[] garbage = new byte[]{1, 2, 3, (byte) 0xAA, 4};
            byte[] payload = "0123456789ABCDEF".getBytes();
            ByteArrayOutputStream wire = new ByteArrayOutputStream();
            wire.write(garbage);
            wire.write(magic);
            wire.write(payload);
            byte[] decoded = readFixed(wire.toByteArray(), 16, magic);
            assertArrayEquals(payload, decoded);
        }

        @Test
        void noMagicReadsExactSize() throws Exception {
            byte[] payload = "0123456789ABCDEF".getBytes();
            byte[] decoded = readFixed(payload, 16, NO_MAGIC);
            assertArrayEquals(payload, decoded);
        }

        @Test
        void writeRejectsWrongLength() {
            byte[] magic = new byte[]{(byte) 0xAA};
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            assertThrows(IOException.class,
                    () -> FrameCodec.writeFrame(out, Framing.FIXED_LENGTH, LineDelimiter.LF,
                            "short".getBytes(StandardCharsets.UTF_8), 16, magic));
        }

        @Test
        void midFrameEofAfterMagicThrows() {
            byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
            byte[] truncated = new byte[]{(byte) 0xAA, (byte) 0xBB, '0', '1'};
            assertThrows(IOException.class, () -> readFixed(truncated, 16, magic));
        }

        @Test
        void cleanEofBeforeMagicReturnsNull() throws Exception {
            byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
            assertNull(readFixed(new byte[0], 16, magic));
        }

        @Test
        void overlappingMagicMatch() throws Exception {
            // magic = 0xAA 0xBB; stream contains 0xAA 0xAA 0xBB <16 payload>
            // After reading 0xAA, expecting 0xBB, but get 0xAA again → reset matched=1.
            byte[] magic = new byte[]{(byte) 0xAA, (byte) 0xBB};
            byte[] payload = "0123456789ABCDEF".getBytes();
            ByteArrayOutputStream wire = new ByteArrayOutputStream();
            wire.write(0xAA);
            wire.write(magic);
            wire.write(payload);
            byte[] decoded = readFixed(wire.toByteArray(), 16, magic);
            assertArrayEquals(payload, decoded);
        }
    }
}
