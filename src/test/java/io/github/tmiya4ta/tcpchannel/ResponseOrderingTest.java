package io.github.tmiya4ta.tcpchannel;

import io.github.tmiya4ta.tcpchannel.api.Framing;
import io.github.tmiya4ta.tcpchannel.api.LineDelimiter;
import io.github.tmiya4ta.tcpchannel.internal.connection.ConnectionEntry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Mule 4 EE runtime dispatches sourceCallback.handle() onto worker pools,
 * so a peer that pipelines requests can see the connector's @OnSuccess
 * callbacks complete in arbitrary order. Without ordering protection, response
 * frames would land on the wire in completion order — breaking strict
 * request → response protocols (telco-grade is unforgiving here).
 *
 * These tests pin the contract: if @OnSuccess is called for messageIndex
 * 1..N in any permutation, the wire output must be 1..N in strict ascending
 * order.
 */
class ResponseOrderingTest {

    @Test
    @DisplayName("Sequential out-of-order writes (5,4,3,2,1) emerge as 1,2,3,4,5")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void reverseOrderCompletionStillEmitsInOrder() throws Exception {
        try (ServerSocket server = new ServerSocket(0);
             Socket peer = new Socket("127.0.0.1", server.getLocalPort());
             Socket accepted = server.accept()) {

            ConnectionEntry entry = new ConnectionEntry(accepted);
            // Complete in reverse order of arrival.
            for (long seq : new long[]{5, 4, 3, 2, 1}) {
                entry.writeOrderedFrame(seq, Framing.LINE, LineDelimiter.LF, 0, new byte[0],
                        ("frame-" + seq).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(0, entry.pendingResponseCount(),
                    "all stashed frames must be drained once seq=1 unlocks the queue");

            byte[] received = peer.getInputStream().readNBytes(40);
            assertArrayEquals(
                    "frame-1\nframe-2\nframe-3\nframe-4\nframe-5\n".getBytes(StandardCharsets.UTF_8),
                    received);
        }
    }

    @Test
    @DisplayName("Parallel out-of-order completion produces strictly ascending output")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void parallelOutOfOrderProducesAscendingOutput() throws Exception {
        try (ServerSocket server = new ServerSocket(0);
             Socket peer = new Socket("127.0.0.1", server.getLocalPort());
             Socket accepted = server.accept()) {

            ConnectionEntry entry = new ConnectionEntry(accepted);
            int n = 100;
            // Shuffle seq numbers 1..n to simulate scrambled flow completion.
            List<Long> order = new ArrayList<>();
            for (long i = 1; i <= n; i++) order.add(i);
            Collections.shuffle(order, new java.util.Random(42));

            ExecutorService pool = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(n);
            for (long seq : order) {
                final long s = seq;
                pool.submit(() -> {
                    try {
                        start.await();
                        entry.writeOrderedFrame(s, Framing.LINE, LineDelimiter.LF, 0, new byte[0],
                                ("f-" + s).getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(8, TimeUnit.SECONDS));
            pool.shutdown();
            assertEquals(0, entry.pendingResponseCount());

            // Read everything and verify strict 1..n order.
            StringBuilder allLines = new StringBuilder();
            int totalBytes = 0;
            // Worst-case bytes: "f-100\n" = 6 bytes, total ~600
            for (int i = 1; i <= n; i++) {
                StringBuilder line = new StringBuilder();
                int b;
                while ((b = peer.getInputStream().read()) != '\n') {
                    if (b < 0) fail("EOF before reading frame " + i);
                    line.append((char) b);
                    totalBytes++;
                }
                totalBytes++;
                String expected = "f-" + i;
                assertEquals(expected, line.toString(),
                        "frame " + i + " must arrive in ascending order");
                allLines.append(line).append('\n');
            }
        }
    }

    @Test
    @DisplayName("Gap in seq numbers stalls subsequent writes until the missing seq arrives")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void gapStallsThenFlushes() throws Exception {
        try (ServerSocket server = new ServerSocket(0);
             Socket peer = new Socket("127.0.0.1", server.getLocalPort());
             Socket accepted = server.accept()) {

            ConnectionEntry entry = new ConnectionEntry(accepted);
            // Send 2, 3, 4 — all three should be stashed because 1 hasn't arrived.
            entry.writeOrderedFrame(2, Framing.LINE, LineDelimiter.LF, 0, new byte[0], "f2".getBytes());
            entry.writeOrderedFrame(3, Framing.LINE, LineDelimiter.LF, 0, new byte[0], "f3".getBytes());
            entry.writeOrderedFrame(4, Framing.LINE, LineDelimiter.LF, 0, new byte[0], "f4".getBytes());
            assertEquals(3, entry.pendingResponseCount());
            // Peer should not have received anything yet.
            peer.setSoTimeout(200);
            assertThrows(java.net.SocketTimeoutException.class, () -> peer.getInputStream().read());

            // Now seq=1 arrives → drains the whole queue.
            entry.writeOrderedFrame(1, Framing.LINE, LineDelimiter.LF, 0, new byte[0], "f1".getBytes());
            assertEquals(0, entry.pendingResponseCount());

            peer.setSoTimeout(2000);
            byte[] received = peer.getInputStream().readNBytes(12);
            assertArrayEquals("f1\nf2\nf3\nf4\n".getBytes(StandardCharsets.UTF_8), received);
        }
    }

    @Test
    @DisplayName("Unsolicited writeFrame (push) is independent of ordered queue and writes immediately")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void unsolicitedWriteBypassesOrderedQueue() throws Exception {
        try (ServerSocket server = new ServerSocket(0);
             Socket peer = new Socket("127.0.0.1", server.getLocalPort());
             Socket accepted = server.accept()) {

            ConnectionEntry entry = new ConnectionEntry(accepted);
            // Stash response 2, queue is waiting for 1.
            entry.writeOrderedFrame(2, Framing.LINE, LineDelimiter.LF, 0, new byte[0], "ack-2".getBytes());
            assertEquals(1, entry.pendingResponseCount());

            // A push (unsolicited write) goes through immediately, even though
            // the ordered queue is stalled.
            entry.writeFrame(Framing.LINE, LineDelimiter.LF, 0, new byte[0], "push-now".getBytes());

            byte[] firstPayload = readLine(peer);
            assertArrayEquals("push-now".getBytes(), firstPayload,
                    "unsolicited push must reach the wire ahead of stalled responses");

            // Now seq=1 arrives, queue drains.
            entry.writeOrderedFrame(1, Framing.LINE, LineDelimiter.LF, 0, new byte[0], "ack-1".getBytes());
            assertArrayEquals("ack-1".getBytes(), readLine(peer));
            assertArrayEquals("ack-2".getBytes(), readLine(peer));
        }
    }

    private static byte[] readLine(Socket s) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = s.getInputStream().read()) != '\n') {
            if (b < 0) throw new IOException("EOF");
            sb.append((char) b);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
