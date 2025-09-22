"""Cache port interface."""

from pathlib import Path
from typing import Protocol


class CachePort(Protocol):
    """Port for cache operations."""

    def ref_path(self, bucket: str, leaf: str) -> Path:
        """Get path where reference should be cached."""
        ...

    def has_ref(self, bucket: str, leaf: str, sha: str) -> bool:
        """Check if reference exists and matches SHA."""
        ...

    def write_ref(self, bucket: str, leaf: str, src: Path) -> Path:
        """Cache reference file."""
        ...

    def evict(self, bucket: str, leaf: str) -> None:
        """Remove cached reference."""
        ...
