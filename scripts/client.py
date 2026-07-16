"""Demo client, mirroring the naive implementation from the task description.

Run from the repository root: python -m scripts.client
"""

import socket
import struct

from logbox.logmessage_pb2 import LogMessage
from logbox.server import DEFAULT_HOST, DEFAULT_PORT


def main() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((DEFAULT_HOST, DEFAULT_PORT))

        lm = LogMessage()
        lm.log_level = "ERROR"
        lm.logger = "main"
        lm.mac = bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])
        lm.message = "test message"

        payload = lm.SerializeToString()
        sock.sendall(struct.pack(">L", len(payload)) + payload)


if __name__ == "__main__":
    main()
