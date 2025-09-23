"""Unit tests for adapters."""

import hashlib
from datetime import UTC, datetime

from deltaglider.adapters import (
    FsCacheAdapter,
    NoopMetricsAdapter,
    Sha256Adapter,
    StdLoggerAdapter,
    UtcClockAdapter,
)


class TestSha256Adapter:
    """Test SHA256 adapter."""

    def test_sha256_from_path(self, temp_dir):
        """Test computing SHA256 from file path."""
        # Setup
        file_path = temp_dir / "test.txt"
        content = b"Hello, World!"
        file_path.write_bytes(content)

        # Expected SHA256
        expected = hashlib.sha256(content).hexdigest()

        # Execute
        adapter = Sha256Adapter()
        actual = adapter.sha256(file_path)

        # Verify
        assert actual == expected

    def test_sha256_from_stream(self, temp_dir):
        """Test computing SHA256 from stream."""
        # Setup
        content = b"Hello, Stream!"
        expected = hashlib.sha256(content).hexdigest()

        # Execute
        adapter = Sha256Adapter()
        import io

        stream = io.BytesIO(content)
        actual = adapter.sha256(stream)

        # Verify
        assert actual == expected


class TestFsCacheAdapter:
    """Test filesystem cache adapter."""

    def test_ref_path(self, temp_dir):
        """Test reference path generation."""
        # Setup
        hasher = Sha256Adapter()
        adapter = FsCacheAdapter(temp_dir / "cache", hasher)

        # Execute
        path = adapter.ref_path("my-bucket", "path/to/deltaspace")

        # Verify
        expected = temp_dir / "cache" / "my-bucket" / "path/to/deltaspace" / "reference.bin"
        assert path == expected

    def test_has_ref_not_exists(self, temp_dir):
        """Test checking non-existent reference."""
        # Setup
        hasher = Sha256Adapter()
        adapter = FsCacheAdapter(temp_dir / "cache", hasher)

        # Execute
        result = adapter.has_ref("bucket", "deltaspace", "abc123")

        # Verify
        assert result is False

    def test_has_ref_wrong_sha(self, temp_dir):
        """Test checking reference with wrong SHA."""
        # Setup
        hasher = Sha256Adapter()
        adapter = FsCacheAdapter(temp_dir / "cache", hasher)

        # Create reference with known content
        ref_path = adapter.ref_path("bucket", "deltaspace")
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        content = b"reference content"
        ref_path.write_bytes(content)

        # Execute with wrong SHA
        result = adapter.has_ref("bucket", "deltaspace", "wrong_sha")

        # Verify
        assert result is False

    def test_has_ref_correct_sha(self, temp_dir):
        """Test checking reference with correct SHA."""
        # Setup
        hasher = Sha256Adapter()
        adapter = FsCacheAdapter(temp_dir / "cache", hasher)

        # Create reference with known content
        ref_path = adapter.ref_path("bucket", "deltaspace")
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        content = b"reference content"
        ref_path.write_bytes(content)
        correct_sha = hasher.sha256(ref_path)

        # Execute with correct SHA
        result = adapter.has_ref("bucket", "deltaspace", correct_sha)

        # Verify
        assert result is True

    def test_write_ref(self, temp_dir):
        """Test writing reference to cache."""
        # Setup
        hasher = Sha256Adapter()
        adapter = FsCacheAdapter(temp_dir / "cache", hasher)

        # Create source file
        src = temp_dir / "source.bin"
        src.write_text("source content")

        # Execute
        cached = adapter.write_ref("bucket", "deltaspace/path", src)

        # Verify
        assert cached.exists()
        assert cached.read_text() == "source content"
        assert cached == temp_dir / "cache" / "bucket" / "deltaspace/path" / "reference.bin"

    def test_evict(self, temp_dir):
        """Test evicting cached reference."""
        # Setup
        hasher = Sha256Adapter()
        adapter = FsCacheAdapter(temp_dir / "cache", hasher)

        # Create cached reference
        ref_path = adapter.ref_path("bucket", "deltaspace")
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        ref_path.write_text("cached")

        # Execute
        adapter.evict("bucket", "deltaspace")

        # Verify
        assert not ref_path.exists()


class TestUtcClockAdapter:
    """Test UTC clock adapter."""

    def test_now_returns_utc(self):
        """Test that now() returns UTC time."""
        # Execute
        adapter = UtcClockAdapter()
        now = adapter.now()

        # Verify
        assert isinstance(now, datetime)
        # Should be close to current UTC time
        utc_now = datetime.now(UTC).replace(tzinfo=None)
        diff = abs((now - utc_now).total_seconds())
        assert diff < 1  # Within 1 second


class TestStdLoggerAdapter:
    """Test standard logger adapter."""

    def test_log_levels(self):
        """Test different log levels."""
        # Setup
        adapter = StdLoggerAdapter(level="DEBUG")

        # Execute - should not raise
        adapter.debug("Debug message", extra="data")
        adapter.info("Info message", key="value")
        adapter.warning("Warning message", count=123)
        adapter.error("Error message", error="details")

    def test_log_operation(self):
        """Test structured operation logging."""
        # Setup
        adapter = StdLoggerAdapter()

        # Execute - should not raise
        adapter.log_operation(
            op="put",
            key="test/key",
            deltaspace="bucket/prefix",
            sizes={"file": 1000, "delta": 100},
            durations={"total": 1.5},
            cache_hit=True,
        )


class TestNoopMetricsAdapter:
    """Test no-op metrics adapter."""

    def test_noop_methods(self):
        """Test that all methods are no-ops."""
        # Setup
        adapter = NoopMetricsAdapter()

        # Execute - should not raise or do anything
        adapter.increment("counter", 1, {"tag": "value"})
        adapter.gauge("gauge", 42.5, {"env": "test"})
        adapter.timing("timer", 1.234, {"op": "test"})
