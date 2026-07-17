import io
import logging
import queue
import socket
from contextlib import ExitStack
import struct
import sys
import threading
import time
import unittest

from logbox.logmessage_pb2 import LogMessage
from logbox.server import _DropWhenFullHandler, _enable_keepalive, _message_level, serve

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

    def test_serves_100_clients_and_queues_the_next(self):
        with ExitStack() as stack:
            socks = [stack.enter_context(self.connect()) for _ in range(100)]
            for i, sock in enumerate(socks):  # all connected before any sends
                sock.sendall(encode(log_level="INFO", logger=f"client-{i}", mac=b"\x0c"))
            for i in range(len(socks)):
                self.assert_logged(f"INFO [0c] client-{i}")  # all 100 served at once

            extra = stack.enter_context(self.connect())  # 101st: kernel backlog
            extra.sendall(encode(log_level="INFO", logger="waiting", mac=b"\x0d"))
            time.sleep(0.15)
            self.assertNotIn("INFO [0d] waiting", self.stdout.getvalue())

            socks[0].close()  # a slot frees; the queued client gets served
            self.assert_logged("INFO [0d] waiting")

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

    def test_drops_client_declaring_oversized_frame(self):
        with self.connect() as sock:
            sock.sendall(struct.pack(">L", 2 * 1024 * 1024))  # 2 MiB claim, no payload
        with self.connect() as sock:
            sock.sendall(encode(log_level="ERROR", logger="modest", mac=b"\x07"))
        self.assert_logged("ERROR [07] modest")

    def test_unknown_log_level_is_emitted_not_dropped(self):
        with self.connect() as sock:
            sock.sendall(encode(log_level="TRACE", logger="weird", mac=b"\x0e", message="kept"))
        self.assert_logged("TRACE [0e] weird: kept")

    def test_connection_survives_pause_between_messages(self):
        with self.connect() as sock:
            sock.sendall(encode(log_level="INFO", logger="patient", mac=b"\x08", message="before"))
            self.assert_logged("INFO [08] patient: before")
            time.sleep(0.3)  # idle connection must stay usable
            sock.sendall(encode(log_level="INFO", logger="patient", mac=b"\x08", message="after"))
        self.assert_logged("INFO [08] patient: after")


class TestKeepalive(unittest.TestCase):
    def test_enables_keepalive_on_socket(self):
        with socket.socket() as sock:
            _enable_keepalive(sock)
            # nonzero means enabled; the exact value is platform-specific
            self.assertNotEqual(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 0)


class TestMessageLevel(unittest.TestCase):
    ADDR = ("192.0.2.1", 1234)

    def test_maps_the_four_protocol_levels(self):
        for name in ("DEBUG", "INFO", "WARNING", "ERROR"):
            self.assertEqual(_message_level(name, self.ADDR), getattr(logging, name))

    def test_unknown_level_falls_back_to_info_and_warns_each_time(self):
        for _ in range(2):
            with self.assertLogs("logbox.server", level="WARNING"):
                self.assertEqual(_message_level("VERBOSE", self.ADDR), logging.INFO)


class TestDropWhenFullHandler(unittest.TestCase):
    def test_full_queue_drops_instead_of_blocking(self):
        record_queue: queue.Queue = queue.Queue(maxsize=1)
        handler = _DropWhenFullHandler(record_queue)
        record = logging.LogRecord("t", logging.INFO, "", 0, "msg", None, None)
        handler.enqueue(record)
        handler.enqueue(record)  # queue full: must return, not block
        self.assertEqual(record_queue.qsize(), 1)
