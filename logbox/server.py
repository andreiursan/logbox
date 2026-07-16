"""TCP server that prints received LogMessages to stdout.

Each connection is handled by a worker thread from a bounded pool, so up to
MAX_CONNECTIONS clients can be connected at once, idle or active.
"""

import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

from logbox.formatting import format_log_message
from logbox.framing import FrameDecoder
from logbox.logmessage_pb2 import LogMessage

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 15000
MAX_CONNECTIONS = 100
RECV_SIZE = 4096


def serve(host=DEFAULT_HOST, port=DEFAULT_PORT, max_connections=MAX_CONNECTIONS):
    """Accept connections forever, dispatching each to a worker thread."""
    with (
        socket.create_server((host, port)) as server_sock,
        ThreadPoolExecutor(max_connections, thread_name_prefix="client") as pool,
    ):
        print(f"listening on {host}:{port}", file=sys.stderr)
        while True:
            conn, _ = server_sock.accept()
            pool.submit(_handle_client, conn)


def _handle_client(conn):
    with conn, suppress(ConnectionError):
        handle_connection(conn)


def handle_connection(conn):
    """Read frames from one client until it disconnects, emitting each message."""
    decoder = FrameDecoder()
    while data := conn.recv(RECV_SIZE):
        for frame in decoder.feed(data):
            _emit(format_log_message(LogMessage.FromString(frame)))
    if decoder.has_partial_frame:
        print("client disconnected mid-message", file=sys.stderr)


def _emit(line):
    # a single write() call per line, so concurrent handlers can't interleave
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
