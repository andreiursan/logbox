import unittest

from logbox.formatting import format_log_message
from logbox.logmessage_pb2 import LogMessage

DEFAULTS = {"log_level": "INFO", "logger": "main", "mac": b"\xaa\xbb\xcc\xdd\xee\xff"}


def log_message(**fields):
    return LogMessage(**DEFAULTS | fields)


class TestFormatLogMessage(unittest.TestCase):
    def test_formats_all_fields(self):
        lm = log_message(log_level="ERROR", message="test message")
        self.assertEqual(
            format_log_message(lm), "ERROR [aa:bb:cc:dd:ee:ff] main: test message"
        )

    def test_omits_unset_optional_message(self):
        self.assertEqual(format_log_message(log_message()), "INFO [aa:bb:cc:dd:ee:ff] main")

    def test_empty_message_is_distinct_from_unset(self):
        self.assertEqual(
            format_log_message(log_message(message="")), "INFO [aa:bb:cc:dd:ee:ff] main: "
        )

    def test_unusual_mac_lengths_are_rendered_verbatim(self):
        self.assertEqual(format_log_message(log_message(mac=b"\x01")), "INFO [01] main")
