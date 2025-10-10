"""Content-Addressed Storage (CAS) cache adapter.

This adapter stores cached references using their SHA256 hash as the filename,
eliminating collision risks and enabling automatic deduplication.
"""

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


class ContentAddressedCache(CachePort):
    """Content-addressed storage cache using SHA256 as filename.

    Key Features:
    - Zero collision risk (SHA256 namespace is the filename)
    - Automatic deduplication (same content = same filename)
    - No metadata tracking needed (self-describing)
    - Secure by design (tampering changes SHA, breaks lookup)

    Storage Layout:
    - base_dir/
      - ab/
        - cd/
          - abcdef123456... (full SHA256 as filename)

    The two-level directory structure (first 2 chars, next 2 chars) prevents
    filesystem performance degradation from too many files in one directory.
    """

    def __init__(self, base_dir: Path, hasher: HashPort):
        """Initialize content-addressed cache.

        Args:
            base_dir: Root directory for cache storage
            hasher: Hash adapter for SHA256 computation
        """
        self.base_dir = base_dir
        self.hasher = hasher
        # Mapping of (bucket, prefix) -> sha256 for compatibility
        # This is ephemeral and only used within a single process
        self._deltaspace_to_sha: dict[tuple[str, str], str] = {}

    def _cas_path(self, sha256: str) -> Path:
        """Get content-addressed path from SHA256 hash.

        Uses two-level directory structure for filesystem optimization:
        - First 2 hex chars as L1 directory (256 buckets)
        - Next 2 hex chars as L2 directory (256 buckets per L1)
        - Full SHA as filename

        Example: abcdef1234... -> ab/cd/abcdef1234...

        Args:
            sha256: Full SHA256 hash (64 hex chars)

        Returns:
            Path to file in content-addressed storage
        """
        if len(sha256) < 4:
            raise ValueError(f"Invalid SHA256: {sha256}")

        # Two-level directory structure
        l1_dir = sha256[:2]  # First 2 chars
        l2_dir = sha256[2:4]  # Next 2 chars

        return self.base_dir / l1_dir / l2_dir / sha256

    def ref_path(self, bucket: str, prefix: str) -> Path:
        """Get path where reference should be cached.

        For CAS, we need the SHA to compute the path. This method looks up
        the SHA from the ephemeral mapping. If not found, it returns a
        placeholder path (backward compatibility with has_ref checks).

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix

        Returns:
            Path to cached reference (may not exist)
        """
        key = (bucket, prefix)

        # If we have the SHA mapping, use CAS path
        if key in self._deltaspace_to_sha:
            sha = self._deltaspace_to_sha[key]
            return self._cas_path(sha)

        # Fallback: return a non-existent placeholder
        # This enables has_ref to return False for unmapped deltaspaces
        return self.base_dir / "_unmapped" / bucket / prefix / "reference.bin"

    def has_ref(self, bucket: str, prefix: str, sha: str) -> bool:
        """Check if reference exists with given SHA.

        In CAS, existence check is simple: if file exists at SHA path,
        it MUST have that SHA (content-addressed guarantee).

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            sha: Expected SHA256 hash

        Returns:
            True if reference exists with this SHA
        """
        path = self._cas_path(sha)
        return path.exists()

    def get_validated_ref(self, bucket: str, prefix: str, expected_sha: str) -> Path:
        """Get cached reference with atomic SHA validation.

        In CAS, the SHA IS the filename, so if the file exists, it's already
        validated by definition. We still perform an integrity check to detect
        filesystem corruption.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            expected_sha: Expected SHA256 hash

        Returns:
            Path to validated cached file

        Raises:
            CacheMissError: File not found in cache
            CacheCorruptionError: SHA mismatch (filesystem corruption)
        """
        path = self._cas_path(expected_sha)

        if not path.exists():
            raise CacheMissError(f"Cache miss for SHA {expected_sha[:8]}...")

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

            # Validate SHA (should never fail in CAS unless filesystem corruption)
            if actual_sha != expected_sha:
                # Filesystem corruption detected
                try:
                    path.unlink()
                except OSError:
                    pass  # Best effort cleanup

                raise CacheCorruptionError(
                    f"Filesystem corruption detected: file {path.name} has wrong content. "
                    f"Expected SHA {expected_sha}, got {actual_sha}"
                )

            # Update mapping for ref_path compatibility
            self._deltaspace_to_sha[(bucket, prefix)] = expected_sha

            return path

        except OSError as e:
            raise CacheMissError(f"Cache read error for SHA {expected_sha[:8]}...: {e}") from e

    def write_ref(self, bucket: str, prefix: str, src: Path) -> Path:
        """Cache reference file using content-addressed storage.

        The file is stored at a path determined by its SHA256 hash.
        If a file with the same content already exists, it's reused
        (automatic deduplication).

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            src: Source file to cache

        Returns:
            Path to cached file (content-addressed)
        """
        # Compute SHA of source file
        sha = self.hasher.sha256(src)
        path = self._cas_path(sha)

        # If file already exists, we're done (deduplication)
        if path.exists():
            # Update mapping
            self._deltaspace_to_sha[(bucket, prefix)] = sha
            return path

        # Create directory structure with secure permissions
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)

        # Atomic write using temp file + rename
        temp_path = path.parent / f".tmp.{sha}"
        try:
            shutil.copy2(src, temp_path)
            # Atomic rename (POSIX guarantee)
            temp_path.rename(path)
        except Exception:
            # Cleanup on failure
            if temp_path.exists():
                temp_path.unlink()
            raise

        # Update mapping
        self._deltaspace_to_sha[(bucket, prefix)] = sha

        return path

    def evict(self, bucket: str, prefix: str) -> None:
        """Remove cached reference for given deltaspace.

        In CAS, eviction is more complex because:
        1. Multiple deltaspaces may reference the same SHA (deduplication)
        2. We can't delete the file unless we know no other deltaspace uses it

        For safety, we only remove the mapping, not the actual file.
        Orphaned files will be cleaned up by cache expiry (future feature).

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
        """
        key = (bucket, prefix)

        # Remove mapping (safe operation)
        if key in self._deltaspace_to_sha:
            del self._deltaspace_to_sha[key]

        # NOTE: We don't delete the actual CAS file because:
        # - Other deltaspaces may reference the same SHA

    def clear(self) -> None:
        """Clear all cached references.

        Removes all cached files and mappings. This is a destructive operation
        that forcibly removes the entire cache directory.

        Use cases:
        - Long-running applications that need to free disk space
        - Manual cache invalidation
        - Test cleanup
        - Ensuring fresh data fetch after configuration changes
        """
        import shutil

        # Clear in-memory mapping
        self._deltaspace_to_sha.clear()

        # Remove all cache files (destructive!)
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir, ignore_errors=True)

        # Recreate base directory with secure permissions
        self.base_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        # - The ephemeral cache will be cleaned on process exit anyway
        # - For persistent cache (future), we'd need reference counting
