package io.github.tmiya4ta.tcpchannel.internal.framing;

import io.github.tmiya4ta.tcpchannel.api.Framing;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Encodes and decodes wire frames for both LINE and LENGTH_PREFIX framings.
 *
 * <p>LINE: a frame is the bytes up to and including the next '\n'; the returned
 * payload has the trailing '\n' stripped (and '\r' if present, to be CRLF-tolerant).
 *
 * <p>LENGTH_PREFIX: a frame is a 4-byte big-endian unsigned-int length followed
 * by exactly that many payload bytes. Length 0 is a valid empty payload.
 *
 * <p>{@link #readFrame} returns {@code null} on clean EOF (peer closed write
 * side between frames). A partial frame (EOF mid-frame) raises an
 * {@link IOException}.
 */
public final class FrameCodec {

    private static final int MAX_LENGTH = 64 * 1024 * 1024; // 64 MiB safety cap

    private FrameCodec() {}

    public static byte[] readFrame(InputStream in, Framing framing) throws IOException {
        switch (framing) {
            case LINE:
                return readLine(in);
            case LENGTH_PREFIX:
                return readLengthPrefixed(in);
            default:
                throw new IllegalStateException("Unknown framing: " + framing);
        }
    }

    public static void writeFrame(OutputStream out, Framing framing, byte[] payload) throws IOException {
        switch (framing) {
            case LINE:
                out.write(payload);
                if (payload.length == 0 || payload[payload.length - 1] != (byte) '\n') {
                    out.write('\n');
                }
                return;
            case LENGTH_PREFIX:
                writeIntBE(out, payload.length);
                out.write(payload);
                return;
            default:
                throw new IllegalStateException("Unknown framing: " + framing);
        }
    }

    private static byte[] readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                int n = buf.size();
                byte[] arr = buf.toByteArray();
                if (n > 0 && arr[n - 1] == '\r') {
                    byte[] trimmed = new byte[n - 1];
                    System.arraycopy(arr, 0, trimmed, 0, n - 1);
                    return trimmed;
                }
                return arr;
            }
            buf.write(b);
        }
        // EOF
        if (buf.size() == 0) return null;
        // Treat trailing data without terminator as a final frame
        return buf.toByteArray();
    }

    private static byte[] readLengthPrefixed(InputStream in) throws IOException {
        DataInputStream din = new DataInputStream(in);
        int len;
        try {
            len = din.readInt();
        } catch (java.io.EOFException eof) {
            return null;
        }
        if (len < 0 || len > MAX_LENGTH) {
            throw new IOException("Frame length out of range: " + len);
        }
        byte[] payload = new byte[len];
        din.readFully(payload);
        return payload;
    }

    private static void writeIntBE(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xff);
        out.write((v >>> 16) & 0xff);
        out.write((v >>> 8) & 0xff);
        out.write(v & 0xff);
    }
}
