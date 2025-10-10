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

    def clear(self) -> None:
        """Clear all cached references.

        This method forcibly removes all cached data, useful for:
        - Long-running applications that need to free memory
        - Test cleanup
        - Manual cache invalidation
        - Ensuring fresh data fetch

        Note: For filesystem caches, this removes all files in the cache directory.
              For memory caches, this clears all in-memory data.
              For encrypted caches, this also clears encryption key mappings.
        """
        ...
