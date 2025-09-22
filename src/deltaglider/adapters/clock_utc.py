"""UTC clock adapter."""

from datetime import UTC, datetime

from ..ports.clock import ClockPort


class UtcClockAdapter(ClockPort):
    """UTC implementation of ClockPort."""

    def now(self) -> datetime:
        """Get current UTC time."""
        return datetime.now(UTC).replace(tzinfo=None)
