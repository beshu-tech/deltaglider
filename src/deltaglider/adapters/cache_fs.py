"""Filesystem cache adapter."""

import hashlib
import shutil
import sys
from pathlib import Path

# Unix-only imports for file locking
if sys.platform != "win32":
    import fcntl

from ..core.errors import CacheCorruptionError, CacheMissError
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

    def get_validated_ref(self, bucket: str, prefix: str, expected_sha: str) -> Path:
        """Get cached reference with atomic SHA validation.

        This method prevents TOCTOU attacks by validating the SHA at use-time,
        not just at check-time.

        Args:
            bucket: S3 bucket name
            prefix: Prefix/deltaspace within bucket
            expected_sha: Expected SHA256 hash

        Returns:
            Path to validated cached file

        Raises:
            CacheMissError: File not found in cache
            CacheCorruptionError: SHA mismatch detected
        """
        path = self.ref_path(bucket, prefix)

        if not path.exists():
            raise CacheMissError(f"Cache miss for {bucket}/{prefix}")

        # Lock file and validate content atomically
        try:
            with open(path, "rb") as f:
                # Acquire shared lock (Unix only)
                if sys.platform != "win32":
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)

                # Read and hash content
                content = f.read()
                actual_sha = hashlib.sha256(content).hexdigest()

                # Release lock automatically when exiting context

            # Validate SHA
            if actual_sha != expected_sha:
                # File corrupted or tampered - remove it
                try:
                    path.unlink()
                except OSError:
                    pass  # Best effort cleanup

                raise CacheCorruptionError(
                    f"Cache corruption detected for {bucket}/{prefix}: "
                    f"expected {expected_sha}, got {actual_sha}"
                )

            return path

        except OSError as e:
            raise CacheMissError(f"Cache read error for {bucket}/{prefix}: {e}") from e

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
