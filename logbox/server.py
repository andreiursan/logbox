"""TCP server that emits received LogMessages to stdout via the logging module.

Each connection is handled by a worker thread from a bounded pool, so up to
MAX_CONNECTIONS clients can be connected at once, idle or active. A
misbehaving client (abrupt disconnect, undecodable or oversized message)
costs only its own connection; the server keeps serving the rest.
"""

import atexit
import logging
import queue
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from logging.handlers import QueueHandler, QueueListener

from google.protobuf.message import DecodeError

from logbox.formatting import format_log_message
from logbox.framing import FrameDecoder, FrameTooLargeError
from logbox.logmessage_pb2 import LogMessage

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 15000
MAX_CONNECTIONS = 100
RECV_SIZE = 4096
QUEUE_CAPACITY = 10_000  # buffered messages while the stdout consumer stalls

log = logging.getLogger(__name__)  # server diagnostics, to stderr
message_log = logging.getLogger("logbox.messages")  # received messages, to stdout
_LEVELS = logging.getLevelNamesMapping()

Address = tuple[str, int]


def serve(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    max_connections: int = MAX_CONNECTIONS,
) -> None:
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


def _handle_client(conn: socket.socket, addr: Address, clients: "_ClientSet") -> None:
    """Run one connection to completion, isolating its failures."""
    try:
        with conn:
            handle_connection(conn, addr)
    except (DecodeError, FrameTooLargeError) as exc:
        log.warning("dropping client %s:%d: %s", *addr, exc)
    except OSError as exc:
        log.info("client %s:%d went away: %s", *addr, exc)
    except Exception:
        log.exception("unexpected error handling client %s:%d", *addr)
    finally:
        clients.discard(conn)


def handle_connection(conn: socket.socket, addr: Address) -> None:
    """Read frames from one client until it disconnects, emitting each message."""
    decoder = FrameDecoder()
    while data := conn.recv(RECV_SIZE):
        for frame in decoder.feed(data):
            lm = LogMessage.FromString(frame)
            message_log.log(_LEVELS.get(lm.log_level, logging.INFO), format_log_message(lm))
    if decoder.has_partial_frame:
        log.info("client %s:%d disconnected mid-message", *addr)


class _ClientSet:
    """Thread-safe registry of open client sockets, so shutdown can unblock
    worker threads parked in recv()."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._socks: set[socket.socket] = set()

    def add(self, sock: socket.socket) -> None:
        with self._lock:
            self._socks.add(sock)

    def discard(self, sock: socket.socket) -> None:
        with self._lock:
            self._socks.discard(sock)

    def shutdown_all(self) -> None:
        with self._lock:
            for sock in self._socks:
                with suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)


class _DropWhenFullHandler(QueueHandler):
    """Enqueues records without ever blocking a network thread.

    When the queue is full (the stdout consumer has stalled), records are
    dropped and the loss is counted and reported, rather than freezing
    every client connection behind one slow reader.
    """

    def __init__(self, record_queue: queue.Queue[logging.LogRecord]) -> None:
        super().__init__(record_queue)
        self._dropped = 0

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                log.warning("output queue full; %d messages dropped so far", self._dropped)


def _setup_logging() -> None:
    """Received messages go to stdout bare, through a bounded queue drained
    by a single writer thread; diagnostics go to stderr."""
    if message_log.handlers:  # idempotent: serve() may be called more than once
        return
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    record_queue: queue.Queue[logging.LogRecord] = queue.Queue(QUEUE_CAPACITY)
    listener = QueueListener(record_queue, stdout_handler)
    listener.start()
    atexit.register(listener.stop)  # drains pending records on exit
    message_log.addHandler(_DropWhenFullHandler(record_queue))
    message_log.setLevel(logging.DEBUG)
    message_log.propagate = False
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
