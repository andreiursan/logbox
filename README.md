# logbox

TCP server that receives length-prefixed, protobuf-encoded log messages and
writes them to stdout.

## Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -e .
```

## Running locally

Start the server (listens on `127.0.0.1:15000`):

```sh
.venv/bin/logbox   # or: .venv/bin/python -m logbox
```

Then, from a second terminal, send a demo log message with the bundled client:

```sh
.venv/bin/python -m scripts.client
```

The server prints each received message to stdout:

```
ERROR [aa:bb:cc:dd:ee:ff] main: test message
```

## Configuration

All tunables live in the `Config` dataclass in `logbox/config.py`. The first
three are also available as command-line flags (see `--help`):

| Setting              | Default     | Purpose                                                          |
| -------------------- | ----------- | ---------------------------------------------------------------- |
| `host`               | `127.0.0.1` | Address to listen on.                                            |
| `port`               | `15000`     | TCP port to listen on.                                           |
| `max_connections`    | `100`       | Concurrent client limit; excess clients wait in the kernel's listen backlog. |
| `queue_capacity`     | `10000`     | Messages buffered for stdout while the consumer stalls; beyond it, new messages are dropped (and the loss reported). |
| `keepalive_idle`     | `60`        | Seconds of silence before the kernel starts probing a connection. |
| `keepalive_interval` | `10`        | Seconds between keepalive probes.                                |
| `keepalive_probes`   | `5`         | Failed probes before a vanished client is declared dead.         |
| `drain_grace`        | `2.0`       | Seconds granted to connected clients to finish transmitting before shutdown cuts them. |

Note on raising `max_connections`: every connection holds a worker thread,
and a CPython thread reserves ~8 MB of stack by default (far less is
actually touched). Hundreds of connections are fine; for thousands, the
thread-per-connection model stops making sense and a `selectors` event loop
is the right architecture — see the concurrency trade-off in the design
notes.

## Test

```sh
.venv/bin/python -m unittest
```

## Protocol

Clients send frames over TCP, each frame being a 4-byte big-endian unsigned
length followed by a serialized `LogMessage` (see `proto/logmessage.proto`).

The generated module `logbox/logmessage_pb2.py` is committed so the project
runs without `protoc`. To regenerate after changing the schema
(requires `protoc`, e.g. `brew install protobuf`):

```sh
protoc --proto_path=proto --python_out=logbox --pyi_out=logbox proto/logmessage.proto
```

## Design notes

**Layering**
- Protocol logic is pure and socket-free: `framing.py` (bytes → frames),
  `formatting.py` (message → line); `server.py` is a thin IO shell.
- Pure modules are unit-tested (frame decoding is chunking-invariant);
  the socket layer is integration-tested over real connections.

**Concurrency**
- Thread pool (`ThreadPoolExecutor`, `max_connections` workers), not a
  `selectors` loop.
- The pool cap maps directly onto the "up to 100 concurrent connections"
  requirement (the `max_connections` default).
- Per-client code stays a straight-line blocking read loop; idle clients
  cost nothing.
- The model scales to hundreds of connections; for thousands, a `selectors`
  loop (one thread, readiness-driven) would replace it.

**Backpressure**
- The accept loop takes a semaphore slot before accepting.
- At capacity the server stops accepting; excess clients wait in the
  kernel's listen backlog, so fd usage stays bounded.

**Slow output consumer**
- Messages reach stdout via a bounded queue (`queue_capacity`) and a single
  writer thread (`QueueHandler`/`QueueListener`).
- A stalled stdout reader never blocks ingestion; a full queue drops new
  messages, counted and reported.
- Prefer blocking over dropping? Flip `put_nowait` to `put`.

**Dead clients**
- Idle connections are legitimate, so vanished peers (power loss, NAT
  timeout) would leak worker threads.
- TCP keepalive with tightened timings (`keepalive_idle`, then
  `keepalive_probes` × `keepalive_interval`) lets the kernel reap them.

**Input hardening**
- Declared frame length is capped at 1 MiB before any payload is buffered.
- Undecodable, oversized, or abruptly closed connections cost only that
  connection.
- Unknown log levels are forwarded verbatim at INFO severity, each with a
  diagnostic warning; nothing is dropped.

**Shutdown**
- SIGINT/SIGTERM: stop the listener, wait `drain_grace` seconds for clients
  to finish, cut stragglers, drain workers, flush the output queue.

**Output conventions**
- stdout carries exactly the received messages (the data).
- Diagnostics go to stderr via `logging`, with timestamps.

**Known limits**
- No acknowledgements in the protocol → at-most-once delivery; buffered
  messages are lost on a crash.
- Auth, TLS, and rate limiting are out of scope; binds to localhost by
  default.