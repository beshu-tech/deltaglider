"""Clock port interface."""

from datetime import datetime
from typing import Protocol


class ClockPort(Protocol):
    """Port for time operations."""

    def now(self) -> datetime:
        """Get current UTC time."""
        ...
