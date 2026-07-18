import argparse
import signal
import sys

from logbox.server import DEFAULT_HOST, DEFAULT_PORT, MAX_CONNECTIONS, serve


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="logbox",
        description="TCP server that receives length-prefixed protobuf log "
        "messages and writes them to stdout.",
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST, help="address to listen on (default: %(default)s)"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="port to listen on (default: %(default)s)"
    )
    parser.add_argument(
        "--max-connections",
        type=int,
        default=MAX_CONNECTIONS,
        help="concurrent client limit (default: %(default)s)",
    )
    args = parser.parse_args()

    # SIGTERM raises SystemExit in the main thread; serve() shuts down cleanly
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    serve(args.host, args.port, args.max_connections)


if __name__ == "__main__":
    main()
