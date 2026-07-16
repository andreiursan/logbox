import signal
import sys

from logbox.server import serve


def main() -> None:
    # SIGTERM raises SystemExit in the main thread; serve() shuts down cleanly
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    serve()


if __name__ == "__main__":
    main()
