"""Unit tests for bucket stats caching functionality."""

import json
from unittest.mock import MagicMock

from deltaglider.client_models import BucketStats
from deltaglider.client_operations.stats import (
    _get_cache_key,
    _is_cache_valid,
    _read_stats_cache,
    _write_stats_cache,
)


def test_get_cache_key():
    """Test cache key generation for different modes."""
    assert _get_cache_key("quick") == ".deltaglider/stats_quick.json"
    assert _get_cache_key("sampled") == ".deltaglider/stats_sampled.json"
    assert _get_cache_key("detailed") == ".deltaglider/stats_detailed.json"


def test_is_cache_valid_when_unchanged():
    """Test cache validation when bucket hasn't changed."""
    cached_validation = {
        "object_count": 100,
        "compressed_size": 50000,
    }

    assert _is_cache_valid(cached_validation, 100, 50000) is True


def test_is_cache_valid_when_count_changed():
    """Test cache validation when object count changed."""
    cached_validation = {
        "object_count": 100,
        "compressed_size": 50000,
    }

    # Object count changed
    assert _is_cache_valid(cached_validation, 101, 50000) is False


def test_is_cache_valid_when_size_changed():
    """Test cache validation when compressed size changed."""
    cached_validation = {
        "object_count": 100,
        "compressed_size": 50000,
    }

    # Compressed size changed
    assert _is_cache_valid(cached_validation, 100, 60000) is False


def test_write_and_read_cache_roundtrip():
    """Test writing and reading cache with valid data."""
    # Create mock client and storage
    mock_storage = MagicMock()
    mock_logger = MagicMock()
    mock_service = MagicMock()
    mock_service.storage = mock_storage
    mock_service.logger = mock_logger
    mock_client = MagicMock()
    mock_client.service = mock_service

    # Create test stats
    test_stats = BucketStats(
        bucket="test-bucket",
        object_count=150,
        total_size=1000000,
        compressed_size=50000,
        space_saved=950000,
        average_compression_ratio=0.95,
        delta_objects=140,
        direct_objects=10,
    )

    # Capture what was written to storage
    written_data = None

    def capture_put(address, data, metadata):
        nonlocal written_data
        written_data = data

    mock_storage.put = capture_put

    # Write cache
    _write_stats_cache(
        client=mock_client,
        bucket="test-bucket",
        mode="quick",
        stats=test_stats,
        object_count=150,
        compressed_size=50000,
    )

    # Verify something was written
    assert written_data is not None

    # Parse written data
    cache_data = json.loads(written_data.decode("utf-8"))

    # Verify structure
    assert cache_data["version"] == "1.0"
    assert cache_data["mode"] == "quick"
    assert "computed_at" in cache_data
    assert cache_data["validation"]["object_count"] == 150
    assert cache_data["validation"]["compressed_size"] == 50000
    assert cache_data["stats"]["bucket"] == "test-bucket"
    assert cache_data["stats"]["object_count"] == 150
    assert cache_data["stats"]["delta_objects"] == 140

    # Now test reading it back
    mock_obj = MagicMock()
    mock_obj.data = written_data
    mock_storage.get = MagicMock(return_value=mock_obj)

    stats, validation = _read_stats_cache(mock_client, "test-bucket", "quick")

    # Verify read stats match original
    assert stats is not None
    assert validation is not None
    assert stats.bucket == "test-bucket"
    assert stats.object_count == 150
    assert stats.delta_objects == 140
    assert stats.average_compression_ratio == 0.95
    assert validation["object_count"] == 150
    assert validation["compressed_size"] == 50000


def test_read_cache_missing_file():
    """Test reading cache when file doesn't exist."""
    mock_storage = MagicMock()
    mock_logger = MagicMock()
    mock_service = MagicMock()
    mock_service.storage = mock_storage
    mock_service.logger = mock_logger
    mock_client = MagicMock()
    mock_client.service = mock_service

    # Simulate FileNotFoundError
    mock_storage.get.side_effect = FileNotFoundError("No such key")

    stats, validation = _read_stats_cache(mock_client, "test-bucket", "quick")

    assert stats is None
    assert validation is None


def test_read_cache_invalid_json():
    """Test reading cache with corrupted JSON."""
    mock_storage = MagicMock()
    mock_logger = MagicMock()
    mock_service = MagicMock()
    mock_service.storage = mock_storage
    mock_service.logger = mock_logger
    mock_client = MagicMock()
    mock_client.service = mock_service

    # Return invalid JSON
    mock_obj = MagicMock()
    mock_obj.data = b"not valid json {]["
    mock_storage.get = MagicMock(return_value=mock_obj)

    stats, validation = _read_stats_cache(mock_client, "test-bucket", "quick")

    assert stats is None
    assert validation is None
    mock_logger.warning.assert_called_once()


def test_read_cache_version_mismatch():
    """Test reading cache with wrong version."""
    mock_storage = MagicMock()
    mock_logger = MagicMock()
    mock_service = MagicMock()
    mock_service.storage = mock_storage
    mock_service.logger = mock_logger
    mock_client = MagicMock()
    mock_client.service = mock_service

    # Cache with wrong version
    cache_data = {
        "version": "2.0",  # Wrong version
        "mode": "quick",
        "validation": {"object_count": 100, "compressed_size": 50000},
        "stats": {
            "bucket": "test",
            "object_count": 100,
            "total_size": 1000,
            "compressed_size": 500,
            "space_saved": 500,
            "average_compression_ratio": 0.5,
            "delta_objects": 90,
            "direct_objects": 10,
        },
    }

    mock_obj = MagicMock()
    mock_obj.data = json.dumps(cache_data).encode("utf-8")
    mock_storage.get = MagicMock(return_value=mock_obj)

    stats, validation = _read_stats_cache(mock_client, "test-bucket", "quick")

    assert stats is None
    assert validation is None
    mock_logger.warning.assert_called_once()


def test_read_cache_mode_mismatch():
    """Test reading cache with wrong mode."""
    mock_storage = MagicMock()
    mock_logger = MagicMock()
    mock_service = MagicMock()
    mock_service.storage = mock_storage
    mock_service.logger = mock_logger
    mock_client = MagicMock()
    mock_client.service = mock_service

    # Cache with mismatched mode
    cache_data = {
        "version": "1.0",
        "mode": "detailed",  # Wrong mode
        "validation": {"object_count": 100, "compressed_size": 50000},
        "stats": {
            "bucket": "test",
            "object_count": 100,
            "total_size": 1000,
            "compressed_size": 500,
            "space_saved": 500,
            "average_compression_ratio": 0.5,
            "delta_objects": 90,
            "direct_objects": 10,
        },
    }

    mock_obj = MagicMock()
    mock_obj.data = json.dumps(cache_data).encode("utf-8")
    mock_storage.get = MagicMock(return_value=mock_obj)

    # Request "quick" mode but cache has "detailed"
    stats, validation = _read_stats_cache(mock_client, "test-bucket", "quick")

    assert stats is None
    assert validation is None
    mock_logger.warning.assert_called_once()


def test_write_cache_handles_errors_gracefully():
    """Test that cache write failures don't crash the program."""
    mock_storage = MagicMock()
    mock_logger = MagicMock()
    mock_service = MagicMock()
    mock_service.storage = mock_storage
    mock_service.logger = mock_logger
    mock_client = MagicMock()
    mock_client.service = mock_service

    # Simulate S3 permission error
    mock_storage.put.side_effect = PermissionError("Access denied")

    test_stats = BucketStats(
        bucket="test-bucket",
        object_count=150,
        total_size=1000000,
        compressed_size=50000,
        space_saved=950000,
        average_compression_ratio=0.95,
        delta_objects=140,
        direct_objects=10,
    )

    # Should not raise exception
    _write_stats_cache(
        client=mock_client,
        bucket="test-bucket",
        mode="quick",
        stats=test_stats,
        object_count=150,
        compressed_size=50000,
    )

    # Should log warning
    mock_logger.warning.assert_called_once()
    assert "Failed to write cache" in str(mock_logger.warning.call_args)
