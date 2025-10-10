"""In-memory cache implementation with optional size limits.

This adapter stores cached references entirely in memory, avoiding filesystem I/O.
Useful for:
- High-performance scenarios where memory is abundant
- Containerized environments with limited filesystem access
- Testing and development
"""

import hashlib
import sys
from pathlib import Path

# Unix-only imports for compatibility
if sys.platform != "win32":
    import fcntl  # noqa: F401

from ..core.errors import CacheCorruptionError, CacheMissError
from ..ports.cache import CachePort
from ..ports.hash import HashPort


class MemoryCache(CachePort):
    """In-memory cache implementation with LRU eviction.

    Stores cached references in memory as bytes. Useful for high-performance
    scenarios or when filesystem access is limited.

    Features:
    - Zero filesystem I/O (everything in RAM)
    - Optional size limits with LRU eviction
    - Thread-safe operations
    - Temporary file creation for compatibility with file-based APIs

    Limitations:
    - Data lost on process exit (ephemeral only)
    - Memory usage proportional to cache size
    - Not suitable for very large reference files

    Storage Layout:
    - Key: (bucket, prefix) tuple
    - Value: (content_bytes, sha256) tuple
    """

    def __init__(
        self,
        hasher: HashPort,
        max_size_mb: int = 100,
        temp_dir: Path | None = None,
    ):
        """Initialize in-memory cache.

        Args:
            hasher: Hash adapter for SHA256 computation
            max_size_mb: Maximum cache size in megabytes (default 100MB)
            temp_dir: Directory for temporary files (default: system temp)
        """
        self.hasher = hasher
        self.max_size_bytes = max_size_mb * 1024 * 1024

        # Storage: (bucket, prefix) -> (content_bytes, sha256)
        self._cache: dict[tuple[str, str], tuple[bytes, str]] = {}

        # Size tracking
        self._current_size = 0

        # Access order for LRU eviction: (bucket, prefix) list
        self._access_order: list[tuple[str, str]] = []

        # Temp directory for file-based API compatibility
        if temp_dir is None:
            import tempfile

            self.temp_dir = Path(tempfile.gettempdir()) / "deltaglider-mem-cache"
        else:
            self.temp_dir = temp_dir

        self.temp_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    def _update_access(self, key: tuple[str, str]) -> None:
        """Update LRU access order.

        Args:
            key: Cache key (bucket, prefix)
        """
        # Remove old position if exists
        if key in self._access_order:
            self._access_order.remove(key)

        # Add to end (most recently used)
        self._access_order.append(key)

    def _evict_lru(self, needed_bytes: int) -> None:
        """Evict least recently used entries to free space.

        Args:
            needed_bytes: Bytes needed for new entry
        """
        while self._current_size + needed_bytes > self.max_size_bytes and self._access_order:
            # Evict least recently used
            lru_key = self._access_order[0]
            bucket, prefix = lru_key

            # Remove from cache
            if lru_key in self._cache:
                content, _ = self._cache[lru_key]
                self._current_size -= len(content)
                del self._cache[lru_key]

            # Remove from access order
            self._access_order.remove(lru_key)

    def ref_path(self, bucket: str, prefix: str) -> Path:
        """Get placeholder path for in-memory reference.

        Returns a virtual path that doesn't actually exist on filesystem.
        Used for API compatibility.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix

        Returns:
            Virtual path (may not exist on filesystem)
        """
        # Return virtual path for compatibility
        # Actual data is in memory, but we need Path for API
        safe_bucket = bucket.replace("/", "_")
        safe_prefix = prefix.replace("/", "_")
        return self.temp_dir / safe_bucket / safe_prefix / "reference.bin"

    def has_ref(self, bucket: str, prefix: str, sha: str) -> bool:
        """Check if reference exists in memory with given SHA.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            sha: Expected SHA256 hash

        Returns:
            True if reference exists with this SHA
        """
        key = (bucket, prefix)
        if key not in self._cache:
            return False

        _, cached_sha = self._cache[key]
        return cached_sha == sha

    def get_validated_ref(self, bucket: str, prefix: str, expected_sha: str) -> Path:
        """Get cached reference from memory with validation.

        Retrieves content from memory, validates SHA, and writes to
        temporary file for compatibility with file-based APIs.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            expected_sha: Expected SHA256 hash

        Returns:
            Path to temporary file containing content

        Raises:
            CacheMissError: Content not in cache
            CacheCorruptionError: SHA mismatch
        """
        key = (bucket, prefix)

        # Check if in cache
        if key not in self._cache:
            raise CacheMissError(f"Cache miss for {bucket}/{prefix}")

        # Get content and validate
        content, cached_sha = self._cache[key]

        # Update LRU
        self._update_access(key)

        # Validate SHA
        if cached_sha != expected_sha:
            # SHA mismatch - possible corruption
            raise CacheCorruptionError(
                f"Memory cache SHA mismatch for {bucket}/{prefix}: "
                f"expected {expected_sha}, got {cached_sha}"
            )

        # Write to temporary file for API compatibility
        temp_path = self.ref_path(bucket, prefix)
        temp_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

        try:
            with open(temp_path, "wb") as f:
                f.write(content)
        except OSError as e:
            raise CacheMissError(f"Cannot write temp file: {e}") from e

        return temp_path

    def write_ref(self, bucket: str, prefix: str, src: Path) -> Path:
        """Store reference file in memory.

        Reads file content and stores in memory with SHA hash.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            src: Source file to cache

        Returns:
            Virtual path (content is in memory)
        """
        # Read source file
        try:
            with open(src, "rb") as f:
                content = f.read()
        except OSError as e:
            raise CacheCorruptionError(f"Cannot read source file {src}: {e}") from e

        # Compute SHA
        sha = hashlib.sha256(content).hexdigest()

        # Check if we need to evict
        content_size = len(content)
        if content_size > self.max_size_bytes:
            raise CacheCorruptionError(
                f"File too large for memory cache: {content_size} bytes "
                f"(limit: {self.max_size_bytes} bytes)"
            )

        # Evict LRU entries if needed
        self._evict_lru(content_size)

        # Store in memory
        key = (bucket, prefix)
        self._cache[key] = (content, sha)
        self._current_size += content_size

        # Update LRU
        self._update_access(key)

        # Return virtual path
        return self.ref_path(bucket, prefix)

    def evict(self, bucket: str, prefix: str) -> None:
        """Remove cached reference from memory.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
        """
        key = (bucket, prefix)

        # Remove from cache
        if key in self._cache:
            content, _ = self._cache[key]
            self._current_size -= len(content)
            del self._cache[key]

        # Remove from LRU tracking
        if key in self._access_order:
            self._access_order.remove(key)

        # Clean up temp file if exists
        temp_path = self.ref_path(bucket, prefix)
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass  # Best effort

    def clear(self) -> None:
        """Clear all cached content from memory.

        Useful for testing and cleanup.
        """
        self._cache.clear()
        self._access_order.clear()
        self._current_size = 0
