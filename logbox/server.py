"""TCP server that prints received LogMessages to stdout.

This version handles one connection at a time; concurrency comes later.
"""

import socket
import sys
from contextlib import suppress

from logbox.formatting import format_log_message
from logbox.framing import FrameDecoder
from logbox.logmessage_pb2 import LogMessage

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 15000
RECV_SIZE = 4096


def serve(host=DEFAULT_HOST, port=DEFAULT_PORT):
    """Accept and handle one connection at a time, forever."""
    with socket.create_server((host, port)) as server_sock:
        print(f"listening on {host}:{port}", file=sys.stderr)
        while True:
            conn, _addr = server_sock.accept()
            with conn, suppress(ConnectionError):
                handle_connection(conn)


def handle_connection(conn):
    """Read frames from one client until it disconnects, printing each message."""
    decoder = FrameDecoder()
    while data := conn.recv(RECV_SIZE):
        for frame in decoder.feed(data):
            print(format_log_message(LogMessage.FromString(frame)), flush=True)
    if decoder.has_partial_frame:
        print("client disconnected mid-message", file=sys.stderr)
