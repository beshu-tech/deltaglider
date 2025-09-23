"""Filesystem cache adapter."""

import shutil
from pathlib import Path

from ..ports.cache import CachePort
from ..ports.hash import HashPort


class FsCacheAdapter(CachePort):
    """Filesystem implementation of CachePort."""

    def __init__(self, base_dir: Path, hasher: HashPort):
        """Initialize with base directory."""
        self.base_dir = base_dir
        self.hasher = hasher

    def ref_path(self, bucket: str, prefix: str) -> Path:
        """Get path where reference should be cached."""
        cache_dir = self.base_dir / bucket / prefix
        return cache_dir / "reference.bin"

    def has_ref(self, bucket: str, prefix: str, sha: str) -> bool:
        """Check if reference exists and matches SHA."""
        path = self.ref_path(bucket, prefix)
        if not path.exists():
            return False

        actual_sha = self.hasher.sha256(path)
        return actual_sha == sha

    def write_ref(self, bucket: str, prefix: str, src: Path) -> Path:
        """Cache reference file."""
        path = self.ref_path(bucket, prefix)
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, path)
        return path

    def evict(self, bucket: str, prefix: str) -> None:
        """Remove cached reference."""
        path = self.ref_path(bucket, prefix)
        if path.exists():
            path.unlink()
        # Clean up empty directories
        try:
            path.parent.rmdir()
            (path.parent.parent).rmdir()
        except OSError:
            pass  # Directory not empty
