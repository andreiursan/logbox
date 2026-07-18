"""Output pipeline: received LogMessages go to stdout via the logging module.

Messages flow through a bounded queue drained by a single writer thread, so
a stalled stdout consumer never blocks the network threads. Diagnostics go
to stderr.
"""

import atexit
import logging
import queue
import sys
from logging.handlers import QueueHandler, QueueListener

from logbox.formatting import format_log_message
from logbox.logmessage_pb2 import LogMessage

log = logging.getLogger(__name__)  # pipeline diagnostics, to stderr
message_log = logging.getLogger("logbox.messages")  # received messages, to stdout

# exactly the four levels the protocol allows
_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def emit(lm: LogMessage, addr: tuple[str, int]) -> None:
    """Emit one received message on stdout at its mapped level."""
    message_log.log(_message_level(lm.log_level, addr), format_log_message(lm))


def _message_level(raw: str, addr: tuple[str, int]) -> int:
    """Map a protocol log level to a logging level.

    Unknown values aren't dropped: they're emitted at INFO (the line keeps
    the raw value) and each occurrence is reported with its sender.
    """
    level = _LEVELS.get(raw)
    if level is not None:
        return level
    log.warning("client %s:%d sent unknown log level %r; emitting at INFO", *addr, raw)
    return logging.INFO


class _DropWhenFullHandler(QueueHandler):
    """Enqueues records without ever blocking a network thread.

    A full queue means the stdout consumer has stalled; we drop the record,
    count it and report it — one slow reader must not freeze every client.
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
                log.warning(
                    "output queue full; %d messages dropped so far", self._dropped
                )


def setup(queue_capacity: int) -> None:
    """Install the handlers: messages to stdout through the bounded queue,
    diagnostics to stderr."""
    if message_log.handlers:  # idempotent: serve() may be called more than once
        return
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    record_queue: queue.Queue[logging.LogRecord] = queue.Queue(queue_capacity)
    listener = QueueListener(record_queue, stdout_handler)
    listener.start()
    atexit.register(listener.stop)  # drains pending records on exit
    message_log.addHandler(_DropWhenFullHandler(record_queue))
    message_log.setLevel(logging.DEBUG)
    message_log.propagate = False
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
