# TCP Channel Connector

A Mule 4 connector that exposes a TCP `<tcpc:listener>` source which
**keeps each accepted connection open across multiple framed messages**, plus
operations to **write to** and **disconnect** any live connection from another
flow.

It exists to fill a gap in the standard `org.mule.connectors:mule-sockets-connector`:
its built-in `SocketWorker` follows a strict *read → flow → write → close* cycle,
so a TCP peer can never send a second message on the same socket. This connector
runs an explicit accept loop and a per-connection read loop, which means a single
TCP session can carry an arbitrary stream of request/response or push messages.

**v2.0.0** adds production-grade hardening:

* **TLS** via Mule's standard `TlsContextFactory`
* **Read timeout** (`SO_TIMEOUT`) for stuck-read detection
* **Idle sweeper** that closes connections silent for too long
* **TCP keepalive tuning** (`TCP_KEEPIDLE` / `TCP_KEEPINTERVAL` / `TCP_KEEPCOUNT`)
* **Graceful drain** on `onStop` (half-close + bounded wait)
* **JMX metrics** for accepted/rejected/idle-closed/timeout counters
* **JUnit 5 test suite** (31 tests) covering framing, drain, TLS, sweeper, and timeout paths

> Migrating from 1.x: see [v2.0.0 migration](#v200-migration) below.

---

## Compatibility

| Item                 | Version                              |
|----------------------|--------------------------------------|
| Mule Runtime         | 4.6+ (tested on **4.11.3 EE**)       |
| Java                 | **17** (declared via `@JavaVersionSupport(JAVA_17)`) |
| `mule-modules-parent`| `1.9.11`                             |
| Packaging            | `mule-extension`                     |

The `@JavaVersionSupport` annotation is set to Java 17 only; if you need to run
on Java 8 or 11 you must change it explicitly and rebuild.

---

## Build

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn clean install
```

Installs `com.example:mule-tcp-channel-connector:1.0.0` (classifier
`mule-plugin`) into the local `~/.m2`. Add the dependency to a Mule application:

```xml
<dependency>
    <groupId>io.github.tmiya4ta</groupId>
    <artifactId>mule-tcp-channel-connector</artifactId>
    <version>2.0.0</version>
    <classifier>mule-plugin</classifier>
</dependency>
```

XML namespace:

```xml
xmlns:tcpc="http://www.mulesoft.org/schema/mule/tcpc"
xsi:schemaLocation="
    http://www.mulesoft.org/schema/mule/tcpc
    http://www.mulesoft.org/schema/mule/tcpc/current/mule-tcpc.xsd"
```

---

## DSL summary

### Configuration

```xml
<tcpc:listener-config name="my-config">
    <tcpc:connection host="0.0.0.0" port="5557" framing="LINE"/>
</tcpc:listener-config>
```

**Wire / framing**

| Attribute        | Type    | Default     | Notes                                                             |
|------------------|---------|-------------|-------------------------------------------------------------------|
| `host`           | String  | `0.0.0.0`   | bind address                                                      |
| `port`           | int     | (required)  | TCP port                                                          |
| `framing`        | enum    | `LINE`      | `LINE`, `LENGTH_PREFIX`, or `FIXED_LENGTH` (see [Framing](#framing)) |
| `lineDelimiter`  | enum    | `LF`        | terminator for `LINE`: `LF`, `CRLF`, or `NUL`. Ignored otherwise. |
| `maxFrameLength` | int     | `67108864`  | hard cap (bytes) per inbound frame; oversize frames close the conn|
| `maxConnections` | int     | `200`       | concurrent connection cap; new accepts beyond it are immediately closed |
| `fixedFrameSize` | int     | `0`         | required (>0) when `framing=FIXED_LENGTH`; payload size in bytes  |
| `magicBytes`     | String  | `""`        | hex-encoded byte sequence prefixed to each FIXED_LENGTH frame; enables resync after garbage. e.g. `AABB`. |

**Liveness (Liveness tab in Studio)**

| Attribute                | Type    | Default | Notes                                                                                       |
|--------------------------|---------|---------|---------------------------------------------------------------------------------------------|
| `keepAlive`              | boolean | `true`  | enables `SO_KEEPALIVE`; the OS detects half-dead peers (slow on stock Linux defaults — tune with `tcpKeep*`) |
| `readTimeoutSeconds`     | int     | `0`     | `SO_TIMEOUT` on accepted sockets. A read with no data within this window closes the conn. `0` disables. |
| `idleTimeoutSeconds`     | int     | `0`     | application-level idle timeout. A background sweeper closes connections silent for this long. `0` disables. |
| `tcpKeepIdleSeconds`     | int     | `0`     | `TCP_KEEPIDLE` (Linux). `0` = OS default (~2h). Effective only when `keepAlive=true`.       |
| `tcpKeepIntervalSeconds` | int     | `0`     | `TCP_KEEPINTVL`. Seconds between probes after the first. `0` = OS default.                  |
| `tcpKeepCount`           | int     | `0`     | `TCP_KEEPCNT`. Failed probes before the OS declares the conn dead. `0` = OS default.        |

`TCP_KEEPIDLE` / `TCP_KEEPINTVL` / `TCP_KEEPCNT` are applied via Java 11+
`jdk.net.ExtendedSocketOptions`. Unsupported on platforms that don't expose
them (notably Windows pre-Java-17 patch levels) — failures are logged at DEBUG
and the option is silently skipped.

**Lifecycle / Observability / TLS**

| Attribute                          | Type                | Default | Notes                                                                              |
|------------------------------------|---------------------|---------|------------------------------------------------------------------------------------|
| `gracefulShutdownTimeoutSeconds`   | int                 | `5`     | on stop/redeploy, wait this long for in-flight reads before forcing close          |
| `enableJmx`                        | boolean             | `true`  | register the metrics MBean (see [Metrics](#metrics))                               |
| `tlsContext`                       | `TlsContextFactory` | (none)  | when set, the listener binds an SSL ServerSocket. See [TLS](#tls).                 |

### Source: `<tcpc:listener>`

Triggered for each framed message read from any accepted client. The flow's
final payload is sent back to the same socket as a response (the socket stays
open afterward).

```xml
<flow name="echo-flow">
    <tcpc:listener config-ref="my-config">
        <tcpc:response>#[payload]</tcpc:response>
    </tcpc:listener>
    <logger message="received #[sizeOf(payload)] bytes from #[attributes.connectionId]"/>
</flow>
```

The source's output type is `byte[]`; cast with `payload as String` if the
client speaks text.

`<tcpc:response>` is required only if you want a reply. Omit it for fire-and-forget.

#### Attributes (`attributes`)

| Field            | Type   | Meaning                                         |
|------------------|--------|-------------------------------------------------|
| `connectionId`   | String | UUID assigned to the live socket                |
| `remoteAddress`  | String | `Socket.getRemoteSocketAddress()` for the client|
| `messageIndex`   | long   | 1-based index of the message within this socket |

### Operations

```xml
<!-- Push an unsolicited message to a known live connection. -->
<tcpc:write config-ref="my-config" connectionId="#[vars.connId]">
    <tcpc:data>#[payload]</tcpc:data>
</tcpc:write>

<!-- Close a live connection from outside the listener flow. -->
<tcpc:disconnect config-ref="my-config" connectionId="#[vars.connId]"/>

<!-- Convenience for demos: returns the most recently accepted connectionId. -->
<tcpc:last-connection-id config-ref="my-config" target="connId"/>
```

The `<tcpc:data>` content accepts anything Mule can coerce to `InputStream`:
String, byte[], `java.io.InputStream`, DataWeave Binary, etc.

#### Errors

| Error                           | When                                 |
|---------------------------------|--------------------------------------|
| `TCPC:CONNECTION_NOT_FOUND`     | `connectionId` is not in the registry|
| `TCPC:IO`                       | underlying socket IO failed          |

---

## Framing

Both peers must agree on the same wire format. The server's framing is set on
`<tcpc:connection framing="..."/>` and is shared by the source and operations.

### `LINE`

Each message is terminated by the configured `lineDelimiter` (`LF`, `CRLF`,
or `NUL`). The payload delivered to the flow does NOT include the terminator.
For `LF` mode an optional preceding `\r` is stripped from the payload
(CRLF-tolerant). Outbound messages have the chosen terminator auto-appended
if missing. Suitable for telnet-style or newline-delimited text protocols.

### `LENGTH_PREFIX`

Each message is a 4-byte big-endian unsigned integer length followed by exactly
that many payload bytes. Length 0 is a valid empty message. Frame size is
capped by `maxFrameLength` (default 64 MiB). Self-synchronising: even a
malformed frame is followed by a fresh length read.
Suitable for arbitrary binary content (images, protobuf, custom packed structs).

### `FIXED_LENGTH`

Each message is exactly `fixedFrameSize` bytes long. Optionally preceded by
a `magicBytes` marker that the listener uses to **resynchronise after garbage
bytes on the stream**. Set `magicBytes=""` to disable the marker (in which
case any byte loss permanently corrupts framing — use only when both peers
are tightly co-designed).

The wire layout is `[magicBytes ...][payload of fixedFrameSize bytes]` per
message, repeated. On the read path, if `magicBytes` is non-empty the listener
discards bytes one by one until the magic prefix matches, then reads exactly
`fixedFrameSize` payload bytes and dispatches them. On the write path
(response and `<tcpc:write>`), the listener prepends `magicBytes` and
**enforces** that the outgoing payload is exactly `fixedFrameSize` bytes —
mismatches raise `TCPC:IO`.

Use this for fixed-record industrial / legacy protocols (sensor packets,
mainframe copybooks). Pick a `magicBytes` of at least 2 bytes that is
unlikely to occur inside payloads to maximise resync reliability.

Python encoder/decoder for `LENGTH_PREFIX`:

```python
import struct
def send_frame(sock, payload: bytes):
    sock.sendall(struct.pack(">I", len(payload)) + payload)
def recv_frame(sock) -> bytes:
    hdr = sock.recv(4, socket.MSG_WAITALL)
    (n,) = struct.unpack(">I", hdr)
    return sock.recv(n, socket.MSG_WAITALL)
```

Python encoder/decoder for `FIXED_LENGTH` with magic `0xAABB` and 16-byte payloads:

```python
MAGIC, SIZE = b"\xaa\xbb", 16
def send_frame(sock, payload: bytes):
    assert len(payload) == SIZE
    sock.sendall(MAGIC + payload)
def recv_frame(sock) -> bytes:
    return sock.recv(len(MAGIC) + SIZE, socket.MSG_WAITALL)[len(MAGIC):]
```

---

## Quick example

```xml
<tcpc:listener-config name="line">
    <tcpc:connection port="5557" framing="LINE"/>
</tcpc:listener-config>

<flow name="echo-line">
    <tcpc:listener config-ref="line">
        <tcpc:response>#[output text/plain --- "ACK: " ++ (payload as String)]</tcpc:response>
    </tcpc:listener>
</flow>
```

```bash
$ printf 'hello\n' | nc 127.0.0.1 5557
ACK: hello
```

For a multi-port (LINE + LENGTH_PREFIX + FIXED_LENGTH + aggregator) sample app
with HTTP-driven `push` and `disconnect` flows, see
[`example/sample-app`](example/sample-app/).

![Sample app flows in Anypoint Studio](docs/sample-app-flows.png)

### Sample app build

Build the connector first so it lands in your local Maven repo, then build
the sample app:

```bash
# 1. Connector → ~/.m2/repository
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn clean install

# 2. Sample app
cd example/sample-app
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  mvn clean package -DskipTests -DattachMuleSources

# 3. Drop into a Mule standalone runtime
cp target/tcp-channel-sample-app-1.0.0-mule-application.jar $MULE_HOME/apps/
```

The sample exposes:

| Port  | Framing       | Flow                          |
|-------|---------------|-------------------------------|
| 5557  | LINE          | `tcp-channel-line-flow` — echoes `ACK#N: <line>` |
| 5558  | LENGTH_PREFIX | `tcp-channel-binary-flow` — binary echo |
| 5559  | FIXED_LENGTH  | `tcp-channel-fixed-flow` — 16-byte payloads, magic `0xAABB` |
| 5560  | LINE          | `tcp-channel-aggregated-flow` — size-based aggregator demo |
| 8282  | HTTP          | `/push`, `/close` against the latest LINE client |

See [`example/sample-app/README.md`](example/sample-app/README.md) for client
snippets and configuration overrides.

---

## Liveness

The default 1.x behaviour ("trust SO_KEEPALIVE") catches OS-detectable failures
in roughly *hours* — fine for occasional cleanup, useless for sub-minute SLAs.
v2.0.0 adds three independent layers; pick the ones that match your peer's
behaviour:

| Layer                    | What it catches                                       | Latency            |
|--------------------------|-------------------------------------------------------|--------------------|
| `readTimeoutSeconds`     | A peer that opens a socket and never speaks again     | exact, per-read    |
| `idleTimeoutSeconds`     | A connection with no traffic in either direction      | sweeper period (≥5s)|
| `tcpKeepIdle/Interval/Count` | Pulled cable, dead host, hard-killed peer       | OS-level, on-network|

Recommended starting points for an interactive request/response protocol:

```xml
<tcpc:listener-config name="prod">
    <tcpc:connection host="0.0.0.0" port="5557" framing="LINE"
                     readTimeoutSeconds="60"
                     idleTimeoutSeconds="300"
                     keepAlive="true"
                     tcpKeepIdleSeconds="30"
                     tcpKeepIntervalSeconds="10"
                     tcpKeepCount="3"/>
</tcpc:listener-config>
```

`readTimeoutSeconds` is the most aggressive — it closes the connection on any
60-second silence, so make sure your peer either keeps a stream of frames going
or you set this to your protocol's idle expectation. `idleTimeoutSeconds`
covers the common case (connection sat idle in a pool past usefulness).

---

## TLS

Use Mule's standard `<tls:context>` element. Both keystore (server identity)
and trust-store (client cert verification) are supported.

```xml
<tcpc:listener-config name="secure">
    <tcpc:connection host="0.0.0.0" port="5557" framing="LINE">
        <tls:context>
            <tls:key-store path="keystore.p12" type="pkcs12"
                           password="secret" keyPassword="secret"/>
            <!-- For mTLS, add a trust-store: -->
            <!-- <tls:trust-store path="ca.p12" type="pkcs12" password="secret"/> -->
        </tls:context>
    </tcpc:connection>
</tcpc:listener-config>
```

The connection provider builds an `SSLServerSocket` from
`TlsContextFactory.createSslContext()` and applies the configured enabled
protocols and cipher suites. The server thread runs `SSLSocket.startHandshake()`
on every accepted client *before* entering the read loop, so handshake failures
are surfaced as a closed connection (logged at WARN) rather than spilling into
frame parsing.

---

## Metrics

When `enableJmx=true` (the default), the listener registers a JMX MBean at
`io.github.tmiya4ta.tcpchannel:type=Listener,port=<port>` exposing read-only
counters:

| Counter                | Increments on                                              |
|------------------------|------------------------------------------------------------|
| `ConnectionsAccepted`  | every successful `accept()`                                 |
| `ConnectionsRejected`  | accepts that hit `maxConnections`                           |
| `ConnectionsActive`    | live registry size (gauge, not a delta)                     |
| `FramesReceived`       | every frame decoded by the read loop                        |
| `FramesSent`           | every successful response or `<tcpc:write>`                 |
| `BytesReceived`        | sum of received frame sizes                                 |
| `BytesSent`            | sum of sent frame sizes                                     |
| `ReadTimeouts`         | `SocketTimeoutException` from the read loop                 |
| `IdleClosed`           | connections evicted by the idle sweeper                     |
| `ReadErrors`           | other read-side IOExceptions                                |
| `WriteErrors`          | write-side IOExceptions across both source and operations   |

Connect with `jconsole` / `VisualVM` to a local JVM, or expose remotely via the
Mule runtime's standard JMX configuration.

---

## v2.0.0 migration

* **`<dependency>` version**: bump to `2.0.0`.
* **Behaviour change**: `onStop` now drains in-flight reads instead of
  immediately closing. If your peer is uncooperative (keeps writing forever),
  drain falls back to force-close after `gracefulShutdownTimeoutSeconds`
  (default 5).
* **New defaults**: `readTimeoutSeconds=0` and `idleTimeoutSeconds=0` keep
  the old "never time out" behaviour. Opt in explicitly when you want them.
* **Errors raised by the `write` op now also close the connection** rather
  than leaving it in a half-broken state. If you relied on the old "log and
  ignore" behaviour, wrap the call in `<try>` and capture `TCPC:IO`.
* **TLS**: drop in a `<tls:context>` to enable; previously this required
  fronting the listener with a TLS-terminating proxy.

---

## Architecture

```
┌──────────────────┐  accept    ┌─────────────────────┐
│  ServerSocket    ├──────────► │  TcpChannelServer │
│  (one per cfg)   │            │  - connections Map  │
└──────────────────┘            │  - lastConnectionId │
                                └────────┬────────────┘
                                         │ shared via @Connection
                  ┌──────────────────────┼──────────────────────┐
                  ▼                      ▼                      ▼
            ┌──────────┐          ┌─────────────┐         ┌──────────────┐
            │ Source   │          │  write op   │         │ disconnect op│
            │ readLoop │          │  by connId  │         │  by connId   │
            └──────────┘          └─────────────┘         └──────────────┘
```

* `TcpChannelConnectionProvider` is a `CachedConnectionProvider`, so the
  source and every operation referencing the same `<tcpc:listener-config>`
  receive the **same** `TcpChannelServer` instance. The connection-id
  registry lives on that instance.
* The acceptor runs on a dedicated single-thread executor; each accepted
  socket gets its own thread from a cached pool that runs `readLoop`.
* `readLoop` calls `FrameCodec.readFrame(in, framing)` in a tight loop and
  hands each frame to `sourceCallback.handle(result, ctx)` with the
  `connectionId` stored in the callback context.
* The Source's `@OnSuccess` looks up the socket by `connectionId` and writes
  the response frame back. **The socket is not closed afterward** — the read
  loop simply waits for the next frame.

---

## Caveats & Limitations

### Functional

* **No backpressure.** Each accepted socket gets a thread, and each frame is
  handed off to the Mule flow with `sourceCallback.handle()`. If the flow
  cannot keep up, the socket's TCP receive buffer fills and the client stalls;
  there is no shed-load policy. Use `maxConnections` to cap the blast radius.
* **No automatic reconnection / retry on bind failure.** If `bind()` fails the
  deployment fails. Use the runtime's reconnection strategy if you need retries.
* **`lastConnectionId` is single-client convenience only.** With multiple
  concurrent clients it returns whichever connected most recently and is a
  race. For multi-client routing, capture `attributes.connectionId` in the
  listener flow and persist it (ObjectStore, DB, broker) keyed by whatever
  identifies the client at the application layer.
* **No application-level heartbeat.** v2.0.0 adds `readTimeoutSeconds` and an
  idle sweeper, but neither emits a ping; they only *detect* silence. If your
  protocol needs keep-alive frames, send them from the flow side.

### Lifecycle

* **Graceful drain has a hard cap.** `onStop` waits up to
  `gracefulShutdownTimeoutSeconds` (default 5) for in-flight reads to finish,
  then force-closes. Long-running flow executions can be cut off if the wait
  is exceeded.
* **Flow errors keep the connection open.** `@OnError` logs only; it does not
  send anything to the client and does not close the socket. Add explicit error
  handling in the flow if you need to surface failures over the wire.

### API design

* **`@Content InputStream` is read fully into memory.** Both the source's
  response handler and the `write` operation call `readAllBytes()` on the
  incoming stream. Streaming a large payload directly to the socket without
  buffering would require extending `FrameCodec` (and is incompatible with
  `LENGTH_PREFIX`, which needs the length up front).
* **Single framing per config.** A given `<tcpc:listener-config>` runs one
  framing strategy for both directions and for all operations. To mix LINE
  and LENGTH_PREFIX in the same app, declare two configs on different ports.
* **LINE delimiter is one of `LF` / `CRLF` / `NUL`.** Custom byte sequences
  (e.g. `0x1E`-record-separator) require extending `LineDelimiter` and
  `FrameCodec`. Both peers must agree on the same delimiter.

### TLS

* **No SNI multiplexing.** A given listener serves one `TlsContextFactory`,
  and therefore one server certificate. To serve multiple hostnames with
  distinct certs, run multiple listener-configs on distinct ports.
* **mTLS validation runs in the SSL stack.** When you provide a trust-store,
  Java's default trust-manager performs the chain check. Custom revocation /
  pinning policies require building the `TlsContextFactory` programmatically.

### Testing / observability

* Logging is via SLF4J under `io.github.tmiya4ta.tcpchannel.*` at INFO and DEBUG.
* JMX metrics cover counters and the active-connection gauge. For richer
  histograms (frame-size distribution, write latency) layer Micrometer or a
  `mule-jmx-module` adapter on top.
* MUnit tests are not included. The connector ships JUnit 5 unit and
  integration tests (run with `mvn test`); end-to-end validation against
  Mule EE is done via the sample app.

---

## File layout

```
mule-tcp-channel-connector/
├── pom.xml                                 # mule-extension 2.0.0, parent 1.9.11
├── README.md
├── docs/
│   └── sample-app-flows.png
├── example/
│   └── sample-app/                         # demo Mule app (LINE + LENGTH_PREFIX + FIXED_LENGTH + aggregator)
├── src/main/java/io/github/tmiya4ta/tcpchannel/
│   ├── api/
│   │   ├── Framing.java                    # LINE / LENGTH_PREFIX / FIXED_LENGTH
│   │   ├── LineDelimiter.java              # LF / CRLF / NUL
│   │   └── TcpChannelAttributes.java
│   └── internal/
│       ├── TcpChannelExtension.java
│       ├── config/TcpChannelConfiguration.java
│       ├── connection/
│       │   ├── ConnectionEntry.java                # per-conn lastActivity
│       │   ├── TcpChannelServer.java               # registry + sweeper + JMX + drain
│       │   └── TcpChannelConnectionProvider.java   # parameters + TLS wiring
│       ├── framing/FrameCodec.java                 # LINE / LENGTH_PREFIX / FIXED_LENGTH codec
│       ├── metrics/
│       │   ├── TcpChannelMetrics.java              # counters
│       │   └── TcpChannelMetricsMBean.java         # JMX interface
│       ├── operations/
│       │   ├── TcpChannelOperations.java           # write / disconnect / lastConnectionId
│       │   └── TcpChannelErrors.java
│       └── source/TcpChannelListener.java          # accept + read loops + drain
└── src/test/java/io/github/tmiya4ta/tcpchannel/
    ├── FrameCodecTest.java                         # 23 framing tests
    ├── TcpChannelServerIntegrationTest.java        # sweeper / drain / timeout / metrics
    ├── TcpChannelTlsTest.java                      # TLS handshake smoke test
    └── TestHarness.java                            # in-process driver
```
