"""Formatting of decoded LogMessages into output lines."""


def format_log_message(lm):
    """Render a LogMessage as a single line, e.g.

        ERROR [aa:bb:cc:dd:ee:ff] main: test message

    The free-form message part is omitted when the optional field is unset.
    The log level is passed through verbatim; validating it is not the
    formatter's concern.
    """
    suffix = f": {lm.message}" if lm.HasField("message") else ""
    return f"{lm.log_level} [{lm.mac.hex(':')}] {lm.logger}{suffix}"
