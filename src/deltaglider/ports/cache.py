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

    def write_ref(self, bucket: str, prefix: str, src: Path) -> Path:
        """Cache reference file."""
        ...

    def evict(self, bucket: str, prefix: str) -> None:
        """Remove cached reference."""
        ...
