"""Incremental decoder for length-prefixed frames.

A frame is a 4-byte big-endian unsigned length followed by that many payload
bytes. TCP doesn't preserve message boundaries, so data arrives in arbitrary
chunks; the decoder buffers partial frames between feeds.
"""

import struct

_PREFIX = struct.Struct(">L")

DEFAULT_MAX_FRAME_SIZE = 1024 * 1024  # generous for a single log message


class FrameTooLargeError(ValueError):
    """A length prefix declared a frame larger than the configured maximum."""


class FrameDecoder:
    def __init__(self, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE) -> None:
        self._buffer = bytearray()
        self._max_frame_size = max_frame_size

    def feed(self, data: bytes) -> list[bytes]:
        """Buffer received bytes and return the payloads of all completed frames.

        Raises FrameTooLargeError as soon as a length prefix exceeds the
        maximum, before buffering any payload — a corrupt prefix must not
        make us allocate gigabytes. Discard the decoder after an error.
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
            frames.append(bytes(self._buffer[_PREFIX.size : end]))
            del self._buffer[:end]
        return frames

    @property
    def has_partial_frame(self) -> bool:
        """True if a frame is still incomplete — e.g. the client
        disconnected mid-transmission."""
        return bool(self._buffer)
