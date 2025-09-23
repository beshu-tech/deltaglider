"""Simplified client API for DeltaGlider."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .adapters import (
    FsCacheAdapter,
    NoopMetricsAdapter,
    S3StorageAdapter,
    Sha256Adapter,
    StdLoggerAdapter,
    UtcClockAdapter,
    XdeltaAdapter,
)
from .core import DeltaService, DeltaSpace, ObjectKey


@dataclass
class UploadSummary:
    """User-friendly upload summary."""

    operation: str
    bucket: str
    key: str
    original_size: int
    stored_size: int
    is_delta: bool
    delta_ratio: float = 0.0

    @property
    def original_size_mb(self) -> float:
        """Original size in MB."""
        return self.original_size / (1024 * 1024)

    @property
    def stored_size_mb(self) -> float:
        """Stored size in MB."""
        return self.stored_size / (1024 * 1024)

    @property
    def savings_percent(self) -> float:
        """Percentage saved through compression."""
        if self.original_size == 0:
            return 0.0
        return ((self.original_size - self.stored_size) / self.original_size) * 100


class DeltaGliderClient:
    """Simplified client for DeltaGlider operations."""

    def __init__(self, service: DeltaService, endpoint_url: str | None = None):
        """Initialize client with service."""
        self.service = service
        self.endpoint_url = endpoint_url

    def upload(
        self,
        file_path: str | Path,
        s3_url: str,
        tags: dict[str, str] | None = None,
        max_ratio: float = 0.5,
    ) -> UploadSummary:
        """Upload a file to S3 with automatic delta compression.

        Args:
            file_path: Local file to upload
            s3_url: S3 destination URL (s3://bucket/prefix/)
            tags: Optional tags to add to the object
            max_ratio: Maximum acceptable delta/file ratio (default 0.5)

        Returns:
            UploadSummary with compression statistics
        """
        file_path = Path(file_path)

        # Parse S3 URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:].rstrip("/")
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        # Create delta space and upload
        delta_space = DeltaSpace(bucket=bucket, prefix=prefix)
        summary = self.service.put(file_path, delta_space, max_ratio)

        # TODO: Add tags support when implemented

        # Convert to user-friendly summary
        is_delta = summary.delta_size is not None
        stored_size = summary.delta_size if is_delta else summary.file_size

        return UploadSummary(
            operation=summary.operation,
            bucket=summary.bucket,
            key=summary.key,
            original_size=summary.file_size,
            stored_size=stored_size or summary.file_size,  # Ensure stored_size is never None
            is_delta=is_delta,
            delta_ratio=summary.delta_ratio or 0.0,
        )

    def download(self, s3_url: str, output_path: str | Path) -> None:
        """Download and reconstruct a file from S3.

        Args:
            s3_url: S3 source URL (s3://bucket/key)
            output_path: Local destination path
        """
        output_path = Path(output_path)

        # Parse S3 URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:]
        parts = s3_path.split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"S3 URL must include key: {s3_url}")

        bucket = parts[0]
        key = parts[1]

        # Auto-append .delta if the file doesn't exist without it
        # This allows users to specify the original name and we'll find the delta
        obj_key = ObjectKey(bucket=bucket, key=key)

        # Try to get metadata first to see if it exists
        try:
            self.service.get(obj_key, output_path)
        except Exception:
            # Try with .delta suffix
            if not key.endswith(".delta"):
                obj_key = ObjectKey(bucket=bucket, key=key + ".delta")
                self.service.get(obj_key, output_path)
            else:
                raise

    def verify(self, s3_url: str) -> bool:
        """Verify integrity of a stored file.

        Args:
            s3_url: S3 URL of the file to verify

        Returns:
            True if verification passed, False otherwise
        """
        # Parse S3 URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:]
        parts = s3_path.split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"S3 URL must include key: {s3_url}")

        bucket = parts[0]
        key = parts[1]

        obj_key = ObjectKey(bucket=bucket, key=key)
        result = self.service.verify(obj_key)
        return result.valid

    def lifecycle_policy(
        self, s3_prefix: str, days_before_archive: int = 30, days_before_delete: int = 90
    ) -> None:
        """Set lifecycle policy for a prefix (placeholder for future implementation).

        Args:
            s3_prefix: S3 prefix to apply policy to
            days_before_archive: Days before transitioning to archive storage
            days_before_delete: Days before deletion
        """
        # TODO: Implement lifecycle policy management
        # This would integrate with S3 lifecycle policies
        # For now, this is a placeholder for the API
        pass


def create_client(
    endpoint_url: str | None = None,
    log_level: str = "INFO",
    cache_dir: str = "/tmp/.deltaglider/cache",
    **kwargs: Any,
) -> DeltaGliderClient:
    """Create a DeltaGlider client with sensible defaults.

    Args:
        endpoint_url: Optional S3 endpoint URL (for MinIO, R2, etc.)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        cache_dir: Directory for reference cache
        **kwargs: Additional arguments passed to DeltaService

    Returns:
        Configured DeltaGliderClient instance

    Examples:
        >>> # Use with AWS S3 (credentials from environment)
        >>> client = create_client()

        >>> # Use with MinIO
        >>> client = create_client(endpoint_url="http://localhost:9000")

        >>> # Use with debug logging
        >>> client = create_client(log_level="DEBUG")
    """
    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter(endpoint_url=endpoint_url)
    diff = XdeltaAdapter()
    cache = FsCacheAdapter(Path(cache_dir), hasher)
    clock = UtcClockAdapter()
    logger = StdLoggerAdapter(level=log_level)
    metrics = NoopMetricsAdapter()

    # Get default values
    tool_version = kwargs.pop("tool_version", "deltaglider/0.1.0")
    max_ratio = kwargs.pop("max_ratio", 0.5)

    # Create service
    service = DeltaService(
        storage=storage,
        diff=diff,
        hasher=hasher,
        cache=cache,
        clock=clock,
        logger=logger,
        metrics=metrics,
        tool_version=tool_version,
        max_ratio=max_ratio,
        **kwargs,
    )

    return DeltaGliderClient(service, endpoint_url)
