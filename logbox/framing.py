"""Incremental decoder for length-prefixed frames.

Wire format: each frame is a 4-byte big-endian unsigned length followed by
that many payload bytes. TCP preserves no message boundaries, so data may
arrive in arbitrary chunks; the decoder buffers partial frames between feeds.
"""

import struct

_PREFIX = struct.Struct(">L")

DEFAULT_MAX_FRAME_SIZE = 1024 * 1024  # generous for a single log message


class FrameTooLargeError(ValueError):
    """A length prefix declared a frame larger than the configured maximum."""


class FrameDecoder:
    def __init__(self, max_frame_size=DEFAULT_MAX_FRAME_SIZE):
        self._buffer = bytearray()
        self._max_frame_size = max_frame_size

    def feed(self, data):
        """Buffer received bytes and return the payloads of all completed frames.

        Raises FrameTooLargeError as soon as a length prefix exceeds the
        maximum, before any payload is buffered — a corrupt or malicious
        prefix must not make the server allocate unbounded memory.
        """
        self._buffer.extend(data)
        frames = []
        while len(self._buffer) >= _PREFIX.size:
            (length,) = _PREFIX.unpack_from(self._buffer)
            if length > self._max_frame_size:
                raise FrameTooLargeError(
                    f"declared frame size {length} exceeds maximum {self._max_frame_size}"
                )
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
