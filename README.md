# logbox

TCP server that receives length-prefixed, protobuf-encoded log messages and
writes them to stdout.

## Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Run

```sh
.venv/bin/python -m logbox
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
