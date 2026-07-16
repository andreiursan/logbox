import io
import socket
from contextlib import ExitStack
import struct
import sys
import threading
import time
import unittest

from logbox.logmessage_pb2 import LogMessage
from logbox.server import serve

TIMEOUT = 5.0


def encode(**fields):
    payload = LogMessage(**fields).SerializeToString()
    return struct.pack(">L", len(payload)) + payload


class TestServerIntegration(unittest.TestCase):
    """End-to-end tests driving the real server over real sockets."""

    @classmethod
    def setUpClass(cls):
        with socket.socket() as probe:  # let the OS pick a free port
            probe.bind(("127.0.0.1", 0))
            cls.port = probe.getsockname()[1]
        cls.stdout = io.StringIO()
        sys.stdout = cls.stdout
        threading.Thread(target=serve, kwargs={"port": cls.port}, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        sys.stdout = sys.__stdout__

    def connect(self):
        deadline = time.monotonic() + TIMEOUT
        while True:
            try:
                return socket.create_connection(("127.0.0.1", self.port), timeout=TIMEOUT)
            except ConnectionRefusedError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

    def assert_logged(self, line):
        deadline = time.monotonic() + TIMEOUT
        while line not in self.stdout.getvalue().splitlines():
            if time.monotonic() > deadline:
                self.fail(f"timed out waiting for {line!r} in {self.stdout.getvalue()!r}")
            time.sleep(0.02)

    def test_delivers_messages_end_to_end(self):
        with self.connect() as sock:
            sock.sendall(encode(log_level="ERROR", logger="app", mac=b"\x01", message="hi"))
            sock.sendall(encode(log_level="INFO", logger="app", mac=b"\x01"))
        self.assert_logged("ERROR [01] app: hi")
        self.assert_logged("INFO [01] app")

    def test_serves_concurrent_connections(self):
        with ExitStack() as stack:
            socks = [stack.enter_context(self.connect()) for _ in range(20)]
            for i, sock in enumerate(socks):  # all connected before any sends
                sock.sendall(encode(log_level="INFO", logger=f"client-{i}", mac=b"\x0c"))
            for i in range(len(socks)):
                self.assert_logged(f"INFO [0c] client-{i}")

    def test_survives_mid_frame_disconnect(self):
        with self.connect() as sock:
            sock.sendall(struct.pack(">L", 100) + b"only a few bytes")
        with self.connect() as sock:  # the next client must still be served
            sock.sendall(encode(log_level="WARNING", logger="after", mac=b"\x02"))
        self.assert_logged("WARNING [02] after")

    def test_drops_undecodable_client_and_keeps_serving(self):
        with self.connect() as sock:
            sock.sendall(struct.pack(">L", 8) + b"\xff" * 8)  # valid frame, garbage protobuf
        with self.connect() as sock:
            sock.sendall(encode(log_level="ERROR", logger="clean", mac=b"\x06"))
        self.assert_logged("ERROR [06] clean")
