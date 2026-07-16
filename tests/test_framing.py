import struct
import unittest

from logbox.framing import FrameDecoder, FrameTooLargeError


def frame(payload):
    return struct.pack(">L", len(payload)) + payload


class TestFrameDecoder(unittest.TestCase):
    def test_decodes_frames_regardless_of_chunking(self):
        payloads = [b"one", b"", b"two" * 10, bytes(range(256))]
        data = b"".join(frame(p) for p in payloads)
        for chunk_size in (1, 2, 7, len(data)):
            with self.subTest(chunk_size=chunk_size):
                decoder = FrameDecoder()
                frames = []
                for i in range(0, len(data), chunk_size):
                    frames.extend(decoder.feed(data[i : i + chunk_size]))
                self.assertEqual(frames, payloads)
                self.assertFalse(decoder.has_partial_frame)

    def test_rejects_oversized_frame_from_prefix_alone(self):
        decoder = FrameDecoder(max_frame_size=10)
        with self.assertRaises(FrameTooLargeError):
            decoder.feed(struct.pack(">L", 11))  # no payload needed to reject

    def test_incomplete_frame_is_buffered_until_finished(self):
        decoder = FrameDecoder()
        data = frame(b"payload")
        self.assertEqual(decoder.feed(data[:-1]), [])
        self.assertTrue(decoder.has_partial_frame)
        self.assertEqual(decoder.feed(data[-1:]), [b"payload"])
        self.assertFalse(decoder.has_partial_frame)
