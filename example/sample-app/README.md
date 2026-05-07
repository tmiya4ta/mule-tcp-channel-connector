# tcp-channel-sample-app

A Mule 4 application that demonstrates the
[`mule-tcp-channel-connector`](../../README.md) end-to-end:

* a **LINE-framed** listener on port `5557` that echoes each line back as
  `ACK#N: <line>`,
* a **LENGTH_PREFIX**-framed listener on port `5558` that echoes each binary
  frame straight back,
* a **FIXED_LENGTH**-framed listener on port `5559` (16-byte payloads with
  magic `0xAABB`) that echoes each fixed-size frame back, including resync
  on garbage bytes, and
* an HTTP listener on port `8282` exposing `/push` and `/close` to push an
  unsolicited message to (or disconnect) the most recently accepted LINE
  connection.

## Build & deploy

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  mvn clean package -DskipTests -DattachMuleSources

# Drop into a Mule standalone runtime
cp target/tcp-channel-sample-app-1.0.0-mule-application.jar \
   $MULE_HOME/apps/
```

Make sure the connector is in your local Maven repo first (the sample-app
depends on **`mule-tcp-channel-connector:2.0.0`**):

```bash
cd ../..
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn clean install
```

## Try it

LINE listener:

```
$ python3 -c '
import socket
s = socket.create_connection(("127.0.0.1", 5557))
for n in range(3):
    s.sendall(f"hello-{n}\n".encode())
    print(s.recv(64).decode().strip())
s.close()'
ACK#1: hello-0
ACK#2: hello-1
ACK#3: hello-2
```

HTTP push (text):

```
$ curl -s -X POST -H 'Content-Type: text/plain' --data 'pushed-from-http' \
       http://127.0.0.1:8282/push
{ "pushed": "pushed-from-http", "connectionId": "...", "time": "..." }
```

HTTP disconnect:

```
$ curl -s -X POST http://127.0.0.1:8282/close
{ "disconnected": "...", "time": "..." }
```

LENGTH_PREFIX listener (Python):

```python
import socket, struct
s = socket.create_connection(("127.0.0.1", 5558))
def send(b):  s.sendall(struct.pack(">I", len(b)) + b)
def recv():
    h = s.recv(4); (n,) = struct.unpack(">I", h)
    return s.recv(n)
send(b"\xff\xd8\xff\xe0 binary frame")
print(recv())   # echoed back as raw bytes
```

FIXED_LENGTH listener (Python). The server will resync if you inject garbage
before the next magic prefix:

```python
import socket
MAGIC, SIZE = b"\xaa\xbb", 16
s = socket.create_connection(("127.0.0.1", 5559))
s.sendall(MAGIC + b"clean-payload-01")     # echoed as MAGIC + payload
s.sendall(b"\x01\x02\x99" + MAGIC + b"after-garbage-03")  # resync, then echo
print(s.recv(SIZE + len(MAGIC), socket.MSG_WAITALL))
```

## Configuration

Ports come from `src/main/resources/config/config-local.yaml`:

```yaml
tcp.line.port: "5557"
tcp.binary.port: "5558"
http.port: "8282"
```

Override at deploy time with `+tcp.line.port=...` etc.

The LINE listener-config also explicitly sets the new connector parameters
introduced for production hardening:

```xml
<tcpc:listener-config name="tcpc-line-config">
    <tcpc:connection host="0.0.0.0" port="${tcp.line.port}" framing="LINE"
                     lineDelimiter="LF"
                     keepAlive="true"
                     maxConnections="200"
                     maxFrameLength="67108864"/>
</tcpc:listener-config>
```

To experiment with the connection cap, drop `maxConnections` to e.g. `2`,
redeploy, and open three sockets in parallel — the third gets closed
immediately and the server logs `connection limit (2) reached; rejecting ...`.

## Notes

* `/push` and `/close` use `<tcpc:last-connection-id/>` for convenience and
  therefore only target the latest LINE client; multi-client routing should
  capture `attributes.connectionId` from the listener flow and store it
  somewhere durable. See the connector's [README](../../README.md#caveats--limitations)
  for the full caveat list.
