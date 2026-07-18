"""TCP server that receives LogMessages and hands them to the output pipeline.

Each connection is handled by a worker thread from a bounded pool, so up to
Config.max_connections clients can be connected at once, idle or active. A
misbehaving client (abrupt disconnect, undecodable or oversized message)
costs only its own connection; the server keeps serving the rest.
"""

import logging
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

from google.protobuf.message import DecodeError

from logbox import output
from logbox.config import DEFAULT_CONFIG, Config
from logbox.framing import FrameDecoder, FrameTooLargeError
from logbox.logmessage_pb2 import LogMessage

RECV_SIZE = 4096

log = logging.getLogger(__name__)  # server diagnostics, to stderr

Address = tuple[str, int]


def serve(config: Config = DEFAULT_CONFIG) -> None:
    """Accept connections forever, dispatching each to a worker thread.

    On KeyboardInterrupt or SystemExit (e.g. a SIGTERM handler calling
    sys.exit), the listener closes, connected clients get a grace period
    to finish, stragglers are cut, and the pool is drained.
    """
    output.setup(config.queue_capacity)
    clients = _ClientSet()
    pool = ThreadPoolExecutor(config.max_connections, thread_name_prefix="client")
    slots = threading.Semaphore(config.max_connections)
    try:
        with socket.create_server((config.host, config.port)) as server_sock:
            log.info("listening on %s:%d", config.host, config.port)
            while True:
                # don't accept (and hold fds for) more clients than we can
                # serve; excess connections wait in the kernel's backlog
                slots.acquire()
                conn, addr = server_sock.accept()
                _enable_keepalive(conn, config)
                clients.add(conn)
                pool.submit(_handle_client, conn, addr, clients, slots)
    except (KeyboardInterrupt, SystemExit):
        log.info("shutting down")
    finally:
        # the listener is already closed: drain, then cut the stragglers
        if not clients.drain(config.drain_grace):
            log.info("grace period expired; disconnecting remaining clients")
            clients.shutdown_all()
        pool.shutdown()


def _enable_keepalive(conn: socket.socket, config: Config) -> None:
    """Detect clients that vanish without closing (power loss, NAT timeout).

    Idle connections are legitimate, so workers block in recv() forever — a
    dead peer would leak its worker. With keepalive the kernel probes and
    recv() fails once the peer is declared dead. (The idle option is
    TCP_KEEPIDLE on Linux, TCP_KEEPALIVE on macOS/BSD.)
    """
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle_opt = getattr(socket, "TCP_KEEPIDLE", None) or getattr(
        socket, "TCP_KEEPALIVE", None
    )
    if idle_opt is not None:
        conn.setsockopt(socket.IPPROTO_TCP, idle_opt, config.keepalive_idle)
    if hasattr(socket, "TCP_KEEPINTVL"):
        conn.setsockopt(
            socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, config.keepalive_interval
        )
    if hasattr(socket, "TCP_KEEPCNT"):
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, config.keepalive_probes)


def _handle_client(
    conn: socket.socket,
    addr: Address,
    clients: "_ClientSet",
    slots: threading.Semaphore,
) -> None:
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
        slots.release()


def handle_connection(conn: socket.socket, addr: Address) -> None:
    """Read frames from one client until it disconnects, emitting each message."""
    decoder = FrameDecoder()
    while data := conn.recv(RECV_SIZE):
        for frame in decoder.feed(data):
            output.emit(LogMessage.FromString(frame), addr)
    if decoder.has_partial_frame:
        log.info("client %s:%d disconnected mid-message", *addr)


class _ClientSet:
    """Thread-safe registry of open client sockets, so shutdown can wait
    for them to finish — and cut them loose when they don't."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._socks: set[socket.socket] = set()

    def add(self, sock: socket.socket) -> None:
        with self._cond:
            self._socks.add(sock)

    def discard(self, sock: socket.socket) -> None:
        with self._cond:
            self._socks.discard(sock)
            if not self._socks:
                self._cond.notify_all()

    def drain(self, timeout: float) -> bool:
        """Wait until every connection has finished; False on timeout."""
        with self._cond:
            return self._cond.wait_for(lambda: not self._socks, timeout)

    def shutdown_all(self) -> None:
        with self._cond:
            for sock in self._socks:
                with suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)
