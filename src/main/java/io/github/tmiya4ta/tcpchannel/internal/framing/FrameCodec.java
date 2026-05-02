package io.github.tmiya4ta.tcpchannel.internal.framing;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Encodes and decodes wire frames for both LINE and LENGTH_PREFIX framings.
 *
 * <p>LINE: a frame is the bytes up to (and excluding) the configured
 * {@link LineDelimiter}. LF is CRLF-tolerant on input (a preceding '\r' is
 * stripped). CRLF requires the exact two-byte sequence on input. NUL uses a
 * single 0x00 byte. On output the terminator is appended if the payload does
 * not already end with it.
 *
 * <p>LENGTH_PREFIX: a frame is a 4-byte big-endian unsigned-int length followed
 * by exactly that many payload bytes. Length 0 is a valid empty payload.
 *
 * <p>{@link #readFrame} returns {@code null} on clean EOF (peer closed write
 * side between frames). A partial frame (EOF mid-frame) raises an
 * {@link IOException}. Frames larger than {@code maxFrameLength} also raise
 * {@link IOException}.
 */
public final class FrameCodec {

    private FrameCodec() {}

    public static byte[] readFrame(InputStream in, Framing framing,
                                   LineDelimiter lineDelimiter, int maxFrameLength,
                                   int fixedFrameSize, byte[] magicBytes)
            throws IOException {
        switch (framing) {
            case LINE:
                return readLine(in, lineDelimiter, maxFrameLength);
            case LENGTH_PREFIX:
                return readLengthPrefixed(in, maxFrameLength);
            case FIXED_LENGTH:
                return readFixed(in, fixedFrameSize, magicBytes);
            default:
                throw new IllegalStateException("Unknown framing: " + framing);
        }
    }

    public static void writeFrame(OutputStream out, Framing framing,
                                  LineDelimiter lineDelimiter, byte[] payload,
                                  int fixedFrameSize, byte[] magicBytes) throws IOException {
        switch (framing) {
            case LINE:
                writeLine(out, lineDelimiter, payload);
                return;
            case LENGTH_PREFIX:
                writeIntBE(out, payload.length);
                out.write(payload);
                return;
            case FIXED_LENGTH:
                writeFixed(out, fixedFrameSize, magicBytes, payload);
                return;
            default:
                throw new IllegalStateException("Unknown framing: " + framing);
        }
    }

    private static byte[] readLine(InputStream in, LineDelimiter delim, int maxLen) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        switch (delim) {
            case LF: {
                int b;
                while ((b = in.read()) != -1) {
                    if (b == '\n') {
                        return finalizeLfFrame(buf);
                    }
                    if (buf.size() >= maxLen) {
                        throw new IOException("Frame exceeds maxFrameLength=" + maxLen);
                    }
                    buf.write(b);
                }
                if (buf.size() == 0) return null;
                return buf.toByteArray();
            }
            case CRLF: {
                int prev = -1, b;
                while ((b = in.read()) != -1) {
                    if (prev == '\r' && b == '\n') {
                        byte[] arr = buf.toByteArray();
                        byte[] trimmed = new byte[arr.length - 1];
                        System.arraycopy(arr, 0, trimmed, 0, arr.length - 1);
                        return trimmed;
                    }
                    if (buf.size() >= maxLen) {
                        throw new IOException("Frame exceeds maxFrameLength=" + maxLen);
                    }
                    buf.write(b);
                    prev = b;
                }
                if (buf.size() == 0) return null;
                return buf.toByteArray();
            }
            case NUL: {
                int b;
                while ((b = in.read()) != -1) {
                    if (b == 0) {
                        return buf.toByteArray();
                    }
                    if (buf.size() >= maxLen) {
                        throw new IOException("Frame exceeds maxFrameLength=" + maxLen);
                    }
                    buf.write(b);
                }
                if (buf.size() == 0) return null;
                return buf.toByteArray();
            }
            default:
                throw new IllegalStateException("Unknown lineDelimiter: " + delim);
        }
    }

    private static byte[] finalizeLfFrame(ByteArrayOutputStream buf) {
        byte[] arr = buf.toByteArray();
        int n = arr.length;
        if (n > 0 && arr[n - 1] == '\r') {
            byte[] trimmed = new byte[n - 1];
            System.arraycopy(arr, 0, trimmed, 0, n - 1);
            return trimmed;
        }
        return arr;
    }

    private static void writeLine(OutputStream out, LineDelimiter delim, byte[] payload) throws IOException {
        out.write(payload);
        switch (delim) {
            case LF:
                if (payload.length == 0 || payload[payload.length - 1] != (byte) '\n') {
                    out.write('\n');
                }
                return;
            case CRLF:
                if (payload.length < 2
                        || payload[payload.length - 2] != (byte) '\r'
                        || payload[payload.length - 1] != (byte) '\n') {
                    out.write('\r');
                    out.write('\n');
                }
                return;
            case NUL:
                if (payload.length == 0 || payload[payload.length - 1] != 0) {
                    out.write(0);
                }
                return;
        }
    }

    /**
     * Reads exactly {@code size} bytes after locating the magic byte sequence.
     * If {@code magic} is empty, reads {@code size} bytes immediately.
     * Otherwise, scans forward one byte at a time, discarding non-matching
     * bytes (resync-on-garbage), until the magic prefix is found, then reads
     * the payload. Returns null only if EOF is hit before any magic byte (clean
     * shutdown). Mid-frame EOF raises {@link IOException}.
     */
    private static byte[] readFixed(InputStream in, int size, byte[] magic) throws IOException {
        if (size <= 0) {
            throw new IOException("FIXED_LENGTH framing requires a positive fixedFrameSize");
        }
        if (magic.length == 0) {
            byte[] payload = new byte[size];
            int read = in.readNBytes(payload, 0, size);
            if (read == 0) return null;
            if (read != size) {
                throw new IOException("EOF mid-frame: expected " + size + " bytes, got " + read);
            }
            return payload;
        }
        int matched = 0;
        boolean sawAnyByte = false;
        while (matched < magic.length) {
            int b = in.read();
            if (b == -1) {
                if (!sawAnyByte && matched == 0) return null;
                throw new IOException("EOF while searching for magicBytes (matched " + matched + "/" + magic.length + ")");
            }
            sawAnyByte = true;
            if (b == (magic[matched] & 0xff)) {
                matched++;
            } else if (b == (magic[0] & 0xff)) {
                matched = 1;
            } else {
                matched = 0;
            }
        }
        byte[] payload = new byte[size];
        int read = in.readNBytes(payload, 0, size);
        if (read != size) {
            throw new IOException("EOF mid-frame: expected " + size + " bytes after magic, got " + read);
        }
        return payload;
    }

    private static void writeFixed(OutputStream out, int size, byte[] magic, byte[] payload) throws IOException {
        if (size <= 0) {
            throw new IOException("FIXED_LENGTH framing requires a positive fixedFrameSize");
        }
        if (payload.length != size) {
            throw new IOException("FIXED_LENGTH payload size mismatch: expected " + size
                    + " bytes, got " + payload.length);
        }
        if (magic.length > 0) {
            out.write(magic);
        }
        out.write(payload);
    }

    private static byte[] readLengthPrefixed(InputStream in, int maxLen) throws IOException {
        DataInputStream din = new DataInputStream(in);
        int len;
        try {
            len = din.readInt();
        } catch (java.io.EOFException eof) {
            return null;
        }
        if (len < 0 || len > maxLen) {
            throw new IOException("Frame length out of range: " + len + " (max=" + maxLen + ")");
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
