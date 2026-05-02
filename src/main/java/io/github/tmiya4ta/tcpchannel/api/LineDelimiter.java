package io.github.tmiya4ta.tcpchannel.api;

/**
 * Frame terminator for {@link Framing#LINE}. Both peers must agree on the same
 * terminator. Outbound frames automatically have the terminator appended if
 * the payload does not already end with it.
 *
 * LF      : a single 0x0A byte ('\n'). Inbound is CRLF-tolerant (a preceding
 *           '\r' is stripped from the payload).
 * CRLF    : the two bytes 0x0D 0x0A ("\r\n"). Inbound requires the exact
 *           sequence; lone '\n' is treated as data.
 * NUL     : a single 0x00 byte. Useful for length-unsafe text protocols.
 */
public enum LineDelimiter {
    LF,
    CRLF,
    NUL
}
