"""Standard logger adapter."""

import json
import logging
import sys
from typing import Any

from ..ports.logger import LoggerPort


class StdLoggerAdapter(LoggerPort):
    """Standard logging implementation of LoggerPort."""

    def __init__(self, name: str = "deltaglider", level: str = "INFO"):
        """Initialize logger."""
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._log(logging.DEBUG, message, kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self._log(logging.INFO, message, kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._log(logging.WARNING, message, kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self._log(logging.ERROR, message, kwargs)

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
        data = {
            "op": op,
            "key": key,
            "deltaspace": deltaspace,
            "sizes": sizes,
            "durations": durations,
            "cache_hit": cache_hit,
        }
        self.info(f"Operation: {op}", **data)

    def _log(self, level: int, message: str, data: dict[str, Any]) -> None:
        """Log with structured data."""
        if data:
            message = f"{message} - {json.dumps(data)}"
        self.logger.log(level, message)
