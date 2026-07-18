"""Deployment-tunable settings for the server."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Config:
    """Defaults match the task description."""

    host: str = "127.0.0.1"
    port: int = 15000
    max_connections: int = 100
    queue_capacity: int = 10_000  # buffered messages while the stdout consumer stalls
    keepalive_idle: int = 60  # seconds of silence before the kernel starts probing
    keepalive_interval: int = 10  # seconds between probes
    keepalive_probes: int = 5  # failed probes before the connection is declared dead
    drain_grace: float = 2.0  # seconds for clients to finish before shutdown cuts them


DEFAULT_CONFIG = Config()
