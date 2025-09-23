"""Logger port interface."""

from typing import Any, Protocol


class LoggerPort(Protocol):
    """Port for logging operations."""

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        ...

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        ...

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        ...

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        ...

    def log_operation(
        self,
        op: str,
        key: str,
        deltaspace: str,
        sizes: dict[str, int],
        durations: dict[str, float],
        cache_hit: bool = False,
    ) -> None:
        """Log structured operation data."""
        ...
