import argparse
import signal
import sys

from logbox.config import Config
from logbox.server import serve


def main() -> None:
    defaults = Config()
    parser = argparse.ArgumentParser(
        prog="logbox",
        description="TCP server that receives length-prefixed protobuf log "
        "messages and writes them to stdout.",
    )
    parser.add_argument(
        "--host",
        default=defaults.host,
        help="address to listen on (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=defaults.port,
        help="port to listen on (default: %(default)s)",
    )
    parser.add_argument(
        "--max-connections",
        type=int,
        default=defaults.max_connections,
        help="concurrent client limit (default: %(default)s)",
    )
    args = parser.parse_args()

    # SIGTERM raises SystemExit in the main thread; serve() shuts down cleanly
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    serve(Config(host=args.host, port=args.port, max_connections=args.max_connections))


if __name__ == "__main__":
    main()
