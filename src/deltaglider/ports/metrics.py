"""Metrics port interface."""

from typing import Protocol


class MetricsPort(Protocol):
    """Port for metrics operations."""

    def increment(self, name: str, value: int = 1, tags: dict[str, str] | None = None) -> None:
        """Increment counter."""
        ...

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Set gauge value."""
        ...

    def timing(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record timing."""
        ...
