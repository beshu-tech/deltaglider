"""Cache port interface."""

from pathlib import Path
from typing import Protocol


class CachePort(Protocol):
    """Port for cache operations."""

    def ref_path(self, bucket: str, prefix: str) -> Path:
        """Get path where reference should be cached."""
        ...

    def has_ref(self, bucket: str, prefix: str, sha: str) -> bool:
        """Check if reference exists and matches SHA."""
        ...

    def get_validated_ref(self, bucket: str, prefix: str, expected_sha: str) -> Path:
        """Get cached reference with atomic SHA validation.

        This method MUST be used instead of ref_path() to prevent TOCTOU attacks.
        It validates the SHA256 hash at the time of use, not just at cache check time.

        Args:
            bucket: S3 bucket name
            prefix: Prefix/deltaspace within bucket
            expected_sha: Expected SHA256 hash of the file

        Returns:
            Path to the validated cached file

        Raises:
            CacheMissError: If cached file doesn't exist
            CacheCorruptionError: If SHA doesn't match (file corrupted or tampered)
        """
        ...

    def write_ref(self, bucket: str, prefix: str, src: Path) -> Path:
        """Cache reference file."""
        ...

    def evict(self, bucket: str, prefix: str) -> None:
        """Remove cached reference."""
        ...
