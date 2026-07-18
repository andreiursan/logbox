import logging
import queue
import unittest

from logbox.output import _DropWhenFullHandler, _message_level


class TestMessageLevel(unittest.TestCase):
    ADDR = ("192.0.2.1", 1234)

    def test_maps_the_four_protocol_levels(self):
        for name in ("DEBUG", "INFO", "WARNING", "ERROR"):
            self.assertEqual(_message_level(name, self.ADDR), getattr(logging, name))

    def test_unknown_level_falls_back_to_info_and_warns_each_time(self):
        for _ in range(2):
            with self.assertLogs("logbox.output", level="WARNING"):
                self.assertEqual(_message_level("VERBOSE", self.ADDR), logging.INFO)


class TestDropWhenFullHandler(unittest.TestCase):
    def test_full_queue_drops_instead_of_blocking(self):
        record_queue: queue.Queue = queue.Queue(maxsize=1)
        handler = _DropWhenFullHandler(record_queue)
        record = logging.LogRecord("t", logging.INFO, "", 0, "msg", None, None)
        handler.enqueue(record)
        handler.enqueue(record)  # queue full: must return, not block
        self.assertEqual(record_queue.qsize(), 1)
