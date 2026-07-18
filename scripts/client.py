"""Demo client, mirroring the naive implementation from the task description.

Run from the repository root: python -m scripts.client
"""

import socket
import struct

from logbox.config import Config
from logbox.logmessage_pb2 import LogMessage


def main() -> None:
    target = Config()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((target.host, target.port))

        lm = LogMessage()
        lm.log_level = "ERROR"
        lm.logger = "main"
        lm.mac = bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])
        lm.message = "test message"

        payload = lm.SerializeToString()
        sock.sendall(struct.pack(">L", len(payload)) + payload)


if __name__ == "__main__":
    main()
