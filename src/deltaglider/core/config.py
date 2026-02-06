"""Centralized configuration for DeltaGlider."""

import os
from dataclasses import dataclass, field


@dataclass(slots=True)
class DeltaGliderConfig:
    """All DeltaGlider configuration in one place.

    Environment variables (all optional):
        DG_MAX_RATIO:           Max delta/file ratio before falling back to direct storage.
                                Range 0.0-1.0, default 0.5.
        DG_LOG_LEVEL:           Logging level. Default "INFO".
        DG_CACHE_BACKEND:       "filesystem" (default) or "memory".
        DG_CACHE_MEMORY_SIZE_MB: Memory cache size in MB. Default 100.
        DG_METRICS:             Metrics backend: "noop", "logging" (default), "cloudwatch".
        DG_METRICS_NAMESPACE:   CloudWatch namespace. Default "DeltaGlider".
    """

    max_ratio: float = 0.5
    log_level: str = "INFO"
    cache_backend: str = "filesystem"
    cache_memory_size_mb: int = 100
    metrics_type: str = "logging"
    metrics_namespace: str = "DeltaGlider"

    # Connection params (typically passed by CLI, not env vars)
    endpoint_url: str | None = field(default=None, repr=False)
    region: str | None = None
    profile: str | None = None

    @classmethod
    def from_env(
        cls,
        *,
        log_level: str = "INFO",
        endpoint_url: str | None = None,
        region: str | None = None,
        profile: str | None = None,
    ) -> "DeltaGliderConfig":
        """Build config from environment variables + explicit overrides."""
        return cls(
            max_ratio=float(os.environ.get("DG_MAX_RATIO", "0.5")),
            log_level=os.environ.get("DG_LOG_LEVEL", log_level),
            cache_backend=os.environ.get("DG_CACHE_BACKEND", "filesystem"),
            cache_memory_size_mb=int(os.environ.get("DG_CACHE_MEMORY_SIZE_MB", "100")),
            metrics_type=os.environ.get("DG_METRICS", "logging"),
            metrics_namespace=os.environ.get("DG_METRICS_NAMESPACE", "DeltaGlider"),
            endpoint_url=endpoint_url,
            region=region,
            profile=profile,
        )
