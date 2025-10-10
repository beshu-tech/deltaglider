"""Tests for in-memory cache adapter."""

import tempfile
from pathlib import Path

import pytest

from deltaglider.adapters import MemoryCache, Sha256Adapter
from deltaglider.core.errors import CacheCorruptionError, CacheMissError


class TestMemoryCache:
    """Test in-memory cache functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def hasher(self):
        """Create SHA256 hasher."""
        return Sha256Adapter()

    @pytest.fixture
    def memory_cache(self, hasher, temp_dir):
        """Create memory cache with 1MB limit."""
        return MemoryCache(hasher, max_size_mb=1, temp_dir=temp_dir)

    def test_write_and_read(self, memory_cache, temp_dir):
        """Test basic write and read functionality."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_content = b"Hello, memory cache!"
        test_file.write_bytes(test_content)

        # Compute expected SHA
        import hashlib

        expected_sha = hashlib.sha256(test_content).hexdigest()

        # Write to memory cache
        memory_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Read back
        retrieved_path = memory_cache.get_validated_ref("test-bucket", "test-prefix", expected_sha)

        # Verify content
        assert retrieved_path.read_bytes() == test_content

    def test_has_ref_true(self, memory_cache, temp_dir):
        """Test has_ref returns True for existing content."""
        test_file = temp_dir / "test.txt"
        test_content = b"Test"
        test_file.write_bytes(test_content)

        import hashlib

        sha = hashlib.sha256(test_content).hexdigest()

        memory_cache.write_ref("test-bucket", "test-prefix", test_file)

        assert memory_cache.has_ref("test-bucket", "test-prefix", sha) is True

    def test_has_ref_false(self, memory_cache):
        """Test has_ref returns False for non-existent content."""
        assert memory_cache.has_ref("no-bucket", "no-prefix", "fakehash") is False

    def test_cache_miss(self, memory_cache):
        """Test cache miss error."""
        with pytest.raises(CacheMissError):
            memory_cache.get_validated_ref("no-bucket", "no-prefix", "fakehash")

    def test_sha_mismatch_detection(self, memory_cache, temp_dir):
        """Test that SHA mismatch is detected."""
        test_file = temp_dir / "test.txt"
        test_file.write_bytes(b"Content")

        memory_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Try to read with wrong SHA
        with pytest.raises(CacheCorruptionError, match="SHA mismatch"):
            memory_cache.get_validated_ref("test-bucket", "test-prefix", "wrong_sha")

    def test_lru_eviction(self, hasher, temp_dir):
        """Test LRU eviction when cache is full."""
        # Create small cache (only 10KB)
        small_cache = MemoryCache(hasher, max_size_mb=0.01, temp_dir=temp_dir)

        # Create files that will exceed cache limit
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file3 = temp_dir / "file3.txt"

        # Each file is 5KB
        file1.write_bytes(b"A" * 5000)
        file2.write_bytes(b"B" * 5000)
        file3.write_bytes(b"C" * 5000)

        # Write file1 and file2 (total 10KB, at limit)
        small_cache.write_ref("bucket", "prefix1", file1)
        small_cache.write_ref("bucket", "prefix2", file2)

        # Verify both are in cache
        import hashlib

        sha1 = hashlib.sha256(b"A" * 5000).hexdigest()
        sha2 = hashlib.sha256(b"B" * 5000).hexdigest()

        assert small_cache.has_ref("bucket", "prefix1", sha1) is True
        assert small_cache.has_ref("bucket", "prefix2", sha2) is True

        # Write file3 (5KB) - should evict file1 (LRU)
        small_cache.write_ref("bucket", "prefix3", file3)

        # file1 should be evicted
        assert small_cache.has_ref("bucket", "prefix1", sha1) is False

        # file2 and file3 should still be in cache
        sha3 = hashlib.sha256(b"C" * 5000).hexdigest()
        assert small_cache.has_ref("bucket", "prefix2", sha2) is True
        assert small_cache.has_ref("bucket", "prefix3", sha3) is True

    def test_file_too_large_for_cache(self, hasher, temp_dir):
        """Test error when file exceeds cache size limit."""
        small_cache = MemoryCache(hasher, max_size_mb=0.001, temp_dir=temp_dir)  # 1KB limit

        large_file = temp_dir / "large.txt"
        large_file.write_bytes(b"X" * 2000)  # 2KB file

        with pytest.raises(CacheCorruptionError, match="too large"):
            small_cache.write_ref("bucket", "prefix", large_file)

    def test_evict_removes_from_memory(self, memory_cache, temp_dir):
        """Test that evict removes content from memory."""
        test_file = temp_dir / "test.txt"
        test_content = b"Test"
        test_file.write_bytes(test_content)

        import hashlib

        sha = hashlib.sha256(test_content).hexdigest()

        memory_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Verify it's in cache
        assert memory_cache.has_ref("test-bucket", "test-prefix", sha) is True

        # Evict
        memory_cache.evict("test-bucket", "test-prefix")

        # Verify it's gone
        assert memory_cache.has_ref("test-bucket", "test-prefix", sha) is False

    def test_clear_removes_all(self, memory_cache, temp_dir):
        """Test that clear removes all cached content."""
        # Add multiple files
        for i in range(3):
            test_file = temp_dir / f"test{i}.txt"
            test_file.write_bytes(f"Content {i}".encode())
            memory_cache.write_ref("bucket", f"prefix{i}", test_file)

        # Verify cache is not empty
        assert memory_cache._current_size > 0
        assert len(memory_cache._cache) == 3

        # Clear
        memory_cache.clear()

        # Verify cache is empty
        assert memory_cache._current_size == 0
        assert len(memory_cache._cache) == 0
        assert len(memory_cache._access_order) == 0

    def test_access_order_updated_on_read(self, memory_cache, temp_dir):
        """Test that LRU access order is updated on reads."""
        # Create two files
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_bytes(b"File 1")
        file2.write_bytes(b"File 2")

        # Write both
        memory_cache.write_ref("bucket", "prefix1", file1)
        memory_cache.write_ref("bucket", "prefix2", file2)

        # Access order should be: [prefix1, prefix2]
        assert memory_cache._access_order[0] == ("bucket", "prefix1")
        assert memory_cache._access_order[1] == ("bucket", "prefix2")

        # Read prefix1 again
        import hashlib

        sha1 = hashlib.sha256(b"File 1").hexdigest()
        memory_cache.get_validated_ref("bucket", "prefix1", sha1)

        # Access order should now be: [prefix2, prefix1]
        assert memory_cache._access_order[0] == ("bucket", "prefix2")
        assert memory_cache._access_order[1] == ("bucket", "prefix1")
