package io.github.tmiya4ta.tcpchannel.api;

/**
 * Wire framing strategy for splitting the TCP byte stream into discrete messages.
 *
 * LINE: messages are terminated by '\n'. The payload delivered to the flow does NOT include
 * the terminator. Suitable for line-oriented text protocols (telnet-style, CSV-over-TCP, ...).
 *
 * LENGTH_PREFIX: each message is prefixed with a 4-byte big-endian unsigned integer
 * giving the byte length of the payload, followed by exactly that many payload bytes.
 * Suitable for arbitrary binary protocols. Both peers MUST use the same framing.
 */
public enum Framing {
    LINE,
    LENGTH_PREFIX
}
