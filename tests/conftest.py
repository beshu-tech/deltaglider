"""Pytest configuration and fixtures."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from deltaglider.adapters import (
    ContentAddressedCache,
    NoopMetricsAdapter,
    Sha256Adapter,
    StdLoggerAdapter,
    UtcClockAdapter,
)
from deltaglider.core import DeltaService


@pytest.fixture
def temp_dir():
    """Create temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_file(temp_dir):
    """Create sample test file."""
    file_path = temp_dir / "test.zip"
    file_path.write_text("Sample content for testing")
    return file_path


@pytest.fixture
def mock_storage():
    """Create mock storage port."""
    return Mock()


@pytest.fixture
def mock_diff():
    """Create mock diff port."""
    mock = Mock()

    # Make encode create empty delta file
    def encode_side_effect(base, target, out):
        out.write_bytes(b"delta content")

    mock.encode.side_effect = encode_side_effect
    return mock


@pytest.fixture
def real_hasher():
    """Create real SHA256 hasher."""
    return Sha256Adapter()


@pytest.fixture
def cache_adapter(temp_dir, real_hasher):
    """Create content-addressed storage cache adapter."""
    cache_dir = temp_dir / "cache"
    return ContentAddressedCache(cache_dir, real_hasher)


@pytest.fixture
def clock_adapter():
    """Create UTC clock adapter."""
    return UtcClockAdapter()


@pytest.fixture
def logger_adapter():
    """Create logger adapter."""
    return StdLoggerAdapter(level="DEBUG")


@pytest.fixture
def metrics_adapter():
    """Create metrics adapter."""
    return NoopMetricsAdapter()


@pytest.fixture
def service(
    mock_storage,
    mock_diff,
    real_hasher,
    cache_adapter,
    clock_adapter,
    logger_adapter,
    metrics_adapter,
):
    """Create DeltaService with test adapters."""
    return DeltaService(
        storage=mock_storage,
        diff=mock_diff,
        hasher=real_hasher,
        cache=cache_adapter,
        clock=clock_adapter,
        logger=logger_adapter,
        metrics=metrics_adapter,
    )


@pytest.fixture
def skip_if_no_xdelta():
    """Skip test if xdelta3 not available."""
    if shutil.which("xdelta3") is None:
        pytest.skip("xdelta3 not available")
