package io.github.tmiya4ta.tcpchannel.api;

/**
 * Wire framing strategy for splitting the TCP byte stream into discrete messages.
 *
 * <p>LINE: messages are terminated by a {@link LineDelimiter}. The payload
 * delivered to the flow does NOT include the terminator. Suitable for
 * line-oriented text protocols (telnet-style, CSV-over-TCP, ...).
 *
 * <p>LENGTH_PREFIX: each message is prefixed with a 4-byte big-endian unsigned
 * integer giving the byte length of the payload, followed by exactly that
 * many payload bytes. Self-synchronising — the listener can recover from a
 * partially-read frame on the next length read. Suitable for arbitrary binary
 * protocols.
 *
 * <p>FIXED_LENGTH: messages are exactly {@code fixedFrameSize} bytes each.
 * Optionally preceded by a {@code magicBytes} marker that the listener uses
 * to re-synchronise after garbage / connection drift. Without a magic marker,
 * a single missed byte permanently corrupts the framing and resync is
 * impossible. Use this only when both peers are tightly co-designed.
 *
 * <p>Both peers MUST use the same framing.
 */
public enum Framing {
    LINE,
    LENGTH_PREFIX,
    FIXED_LENGTH
}
