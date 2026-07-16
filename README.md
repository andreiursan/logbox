# logbox

TCP server that receives length-prefixed, protobuf-encoded log messages and
writes them to stdout.

## Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Running locally

Start the server (listens on `127.0.0.1:15000`):

```sh
.venv/bin/python -m logbox
```

Then, from a second terminal, send a demo log message with the bundled client:

```sh
.venv/bin/python -m scripts.client
```

The server prints each received message to stdout:

```
ERROR [aa:bb:cc:dd:ee:ff] main: test message
```

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
protoc --proto_path=proto --python_out=logbox proto/logmessage.proto
```
