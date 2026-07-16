"""TCP server that emits received LogMessages to stdout via the logging module.

Each connection is handled by a worker thread from a bounded pool, so up to
MAX_CONNECTIONS clients can be connected at once, idle or active. A
misbehaving client (abrupt disconnect, undecodable or oversized message)
costs only its own connection; the server keeps serving the rest.
"""

import logging
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

from google.protobuf.message import DecodeError

from logbox.formatting import format_log_message
from logbox.framing import FrameDecoder, FrameTooLargeError
from logbox.logmessage_pb2 import LogMessage

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 15000
MAX_CONNECTIONS = 100
RECV_SIZE = 4096

log = logging.getLogger(__name__)  # server diagnostics, to stderr
message_log = logging.getLogger("logbox.messages")  # received messages, to stdout
_LEVELS = logging.getLevelNamesMapping()


def serve(host=DEFAULT_HOST, port=DEFAULT_PORT, max_connections=MAX_CONNECTIONS):
    """Accept connections forever, dispatching each to a worker thread.

    On KeyboardInterrupt or SystemExit (e.g. a SIGTERM handler calling
    sys.exit), open client sockets are shut down so blocked worker threads
    finish promptly, then the pool is drained.
    """
    _setup_logging()
    clients = _ClientSet()
    pool = ThreadPoolExecutor(max_connections, thread_name_prefix="client")
    try:
        with socket.create_server((host, port)) as server_sock:
            log.info("listening on %s:%d", host, port)
            while True:
                conn, addr = server_sock.accept()
                clients.add(conn)
                pool.submit(_handle_client, conn, addr, clients)
    except (KeyboardInterrupt, SystemExit):
        log.info("shutting down")
    finally:
        clients.shutdown_all()
        pool.shutdown()


def _handle_client(conn, addr, clients):
    """Run one connection to completion, isolating its failures."""
    try:
        with conn:
            handle_connection(conn)
    except (DecodeError, FrameTooLargeError) as exc:
        log.warning("dropping client %s:%d: %s", *addr, exc)
    except OSError as exc:
        log.info("client %s:%d went away: %s", *addr, exc)
    except Exception:
        log.exception("unexpected error handling client %s:%d", *addr)
    finally:
        clients.discard(conn)


def handle_connection(conn):
    """Read frames from one client until it disconnects, emitting each message."""
    decoder = FrameDecoder()
    while data := conn.recv(RECV_SIZE):
        for frame in decoder.feed(data):
            lm = LogMessage.FromString(frame)
            message_log.log(_LEVELS.get(lm.log_level, logging.INFO), format_log_message(lm))
    if decoder.has_partial_frame:
        log.info("client disconnected mid-message")


class _ClientSet:
    """Thread-safe registry of open client sockets, so shutdown can unblock
    worker threads parked in recv()."""

    def __init__(self):
        self._lock = threading.Lock()
        self._socks = set()

    def add(self, sock):
        with self._lock:
            self._socks.add(sock)

    def discard(self, sock):
        with self._lock:
            self._socks.discard(sock)

    def shutdown_all(self):
        with self._lock:
            for sock in self._socks:
                with suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)


def _setup_logging():
    """Received messages go to stdout bare; diagnostics go to stderr."""
    if message_log.handlers:  # idempotent: serve() may be called more than once
        return
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    message_log.addHandler(stdout_handler)
    message_log.setLevel(logging.DEBUG)
    message_log.propagate = False
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
