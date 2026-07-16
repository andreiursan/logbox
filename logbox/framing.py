"""Incremental decoder for length-prefixed frames.

Wire format: each frame is a 4-byte big-endian unsigned length followed by
that many payload bytes. TCP preserves no message boundaries, so data may
arrive in arbitrary chunks; the decoder buffers partial frames between feeds.
"""

import struct

_PREFIX = struct.Struct(">L")


class FrameDecoder:
    def __init__(self):
        self._buffer = bytearray()

    def feed(self, data):
        """Buffer received bytes and return the payloads of all completed frames."""
        self._buffer.extend(data)
        frames = []
        while len(self._buffer) >= _PREFIX.size:
            (length,) = _PREFIX.unpack_from(self._buffer)
            end = _PREFIX.size + length
            if len(self._buffer) < end:
                break
            frames.append(bytes(self._buffer[_PREFIX.size:end]))
            del self._buffer[:end]
        return frames

    @property
    def has_partial_frame(self):
        """True if buffered bytes form an incomplete frame (e.g. the client
        disconnected mid-transmission)."""
        return bool(self._buffer)
