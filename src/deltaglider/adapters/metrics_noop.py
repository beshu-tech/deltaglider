"""No-op metrics adapter."""

from ..ports.metrics import MetricsPort


class NoopMetricsAdapter(MetricsPort):
    """No-op implementation of MetricsPort."""

    def increment(self, name: str, value: int = 1, tags: dict[str, str] | None = None) -> None:
        """No-op increment counter."""
        pass

    def gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """No-op set gauge."""
        pass

    def timing(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """No-op record timing."""
        pass
