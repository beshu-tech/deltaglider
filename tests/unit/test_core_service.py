"""Unit tests for DeltaService."""

import warnings

import pytest

from deltaglider.core import (
    DeltaSpace,
    NotFoundError,
    ObjectKey,
    PolicyViolationWarning,
)
from deltaglider.ports.storage import ObjectHead, PutResult


class TestDeltaServicePut:
    """Test DeltaService.put method."""

    def test_create_reference_first_file(self, service, sample_file, mock_storage):
        """Test creating reference for first file."""
        # Setup
        delta_space = DeltaSpace(bucket="test-bucket", prefix="test/prefix")
        mock_storage.head.return_value = None  # No reference exists
        mock_storage.put.return_value = PutResult(etag="abc123")

        # Execute
        summary = service.put(sample_file, delta_space)

        # Verify
        assert summary.operation == "create_reference"
        assert summary.bucket == "test-bucket"
        assert summary.key == "test/prefix/reference.bin"
        assert summary.original_name == "test.zip"
        assert summary.file_size > 0
        assert summary.file_sha256 is not None

        # Check storage calls
        assert mock_storage.head.call_count == 2  # Initial check + re-check
        assert mock_storage.put.call_count == 2  # Reference + zero-diff delta

    def test_create_delta_subsequent_file(self, service, sample_file, mock_storage, mock_diff):
        """Test creating delta for subsequent file."""
        # Setup
        delta_space = DeltaSpace(bucket="test-bucket", prefix="test/prefix")

        # Create reference content and compute its SHA
        import io

        ref_content = b"reference content for test"
        ref_sha = service.hasher.sha256(io.BytesIO(ref_content))

        ref_metadata = {
            "dg-tool": "deltaglider/0.1.0",
            "dg-source-name": "original.zip",
            "dg-file-sha256": ref_sha,
            "dg-created-at": "2025-01-01T00:00:00Z",
        }
        mock_storage.head.return_value = ObjectHead(
            key="test/prefix/reference.bin",
            size=1000,
            etag="ref123",
            last_modified=None,
            metadata=ref_metadata,
        )
        mock_storage.put.return_value = PutResult(etag="delta123")

        # Mock storage.get to return the reference content
        mock_storage.get.return_value = io.BytesIO(ref_content)

        # Create cached reference with matching content
        ref_path = service.cache.ref_path(delta_space.bucket, delta_space.prefix)
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        ref_path.write_bytes(ref_content)

        # Execute
        summary = service.put(sample_file, delta_space)

        # Verify
        assert summary.operation == "create_delta"
        assert summary.bucket == "test-bucket"
        assert summary.key == "test/prefix/test.zip.delta"
        assert summary.delta_size is not None
        assert summary.delta_ratio is not None
        assert summary.ref_key == "test/prefix/reference.bin"

        # Check diff was called
        mock_diff.encode.assert_called_once()

    def test_delta_ratio_warning(self, service, sample_file, mock_storage, mock_diff):
        """Test warning when delta ratio exceeds threshold."""
        # Setup
        delta_space = DeltaSpace(bucket="test-bucket", prefix="test/prefix")

        # Create reference content and compute its SHA
        import io

        ref_content = b"reference content for test"
        ref_sha = service.hasher.sha256(io.BytesIO(ref_content))

        ref_metadata = {
            "dg-file-sha256": ref_sha,
        }
        mock_storage.head.return_value = ObjectHead(
            key="test/prefix/reference.bin",
            size=1000,
            etag="ref123",
            last_modified=None,
            metadata=ref_metadata,
        )
        mock_storage.put.return_value = PutResult(etag="delta123")

        # Mock storage.get to return the reference content
        mock_storage.get.return_value = io.BytesIO(ref_content)

        # Make delta large (exceeds ratio)
        def large_encode(base, target, out):
            out.write_bytes(b"x" * 10000)  # Large delta

        mock_diff.encode.side_effect = large_encode

        # Create cached reference with matching content
        ref_path = service.cache.ref_path(delta_space.bucket, delta_space.prefix)
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        ref_path.write_bytes(ref_content)

        # Execute and check warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            service.put(sample_file, delta_space, max_ratio=0.1)

            assert len(w) == 1
            assert issubclass(w[0].category, PolicyViolationWarning)
            assert "exceeds threshold" in str(w[0].message)


class TestDeltaServiceGet:
    """Test DeltaService.get method."""

    def test_get_not_found(self, service, mock_storage, temp_dir):
        """Test get with non-existent delta."""
        # Setup
        delta_key = ObjectKey(bucket="test-bucket", key="test/file.zip.delta")
        mock_storage.head.return_value = None

        # Execute and verify
        with pytest.raises(NotFoundError):
            service.get(delta_key, temp_dir / "output.zip")

    def test_get_missing_metadata(self, service, mock_storage, temp_dir):
        """Test get with missing metadata (regular S3 object)."""
        # Setup
        delta_key = ObjectKey(bucket="test-bucket", key="test/file.zip.delta")

        # Create test content
        test_content = b"regular S3 file content"

        # Mock a regular S3 object without DeltaGlider metadata
        mock_storage.head.return_value = ObjectHead(
            key="test/file.zip.delta",
            size=len(test_content),
            etag="abc",
            last_modified=None,
            metadata={},  # Missing DeltaGlider metadata - this is a regular S3 object
        )

        # Mock the storage.get to return the content
        from unittest.mock import MagicMock

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [test_content, b""]  # Return content then EOF
        mock_storage.get.return_value = mock_stream

        # Execute - should successfully download regular S3 object
        output_path = temp_dir / "output.zip"
        service.get(delta_key, output_path)

        # Verify - file should be downloaded
        assert output_path.exists()
        assert output_path.read_bytes() == test_content


class TestDeltaServiceVerify:
    """Test DeltaService.verify method."""

    def test_verify_valid(self, service, mock_storage, mock_diff, temp_dir):
        """Test verify with valid delta."""
        # Setup
        delta_key = ObjectKey(bucket="test-bucket", key="test/file.zip.delta")

        # Create test file content
        test_content = b"test file content"
        temp_file = temp_dir / "temp"
        temp_file.write_bytes(test_content)
        test_sha = service.hasher.sha256(temp_file)

        # Create reference content for mock
        import io

        ref_content = b"reference content for test"
        ref_sha = service.hasher.sha256(io.BytesIO(ref_content))

        delta_metadata = {
            "dg-tool": "deltaglider/0.1.0",
            "dg-original-name": "file.zip",
            "dg-file-sha256": test_sha,
            "dg-file-size": str(len(test_content)),
            "dg-created-at": "2025-01-01T00:00:00Z",
            "dg-ref-key": "test/reference.bin",
            "dg-ref-sha256": ref_sha,
            "dg-delta-size": "100",
            "dg-delta-cmd": "xdelta3 -e -9 -s reference.bin file.zip file.zip.delta",
        }
        mock_storage.head.return_value = ObjectHead(
            key="test/file.zip.delta",
            size=100,
            etag="delta123",
            last_modified=None,
            metadata=delta_metadata,
        )

        # Mock storage.get to return content based on which key is requested
        # Storage.get is called with full keys like "bucket/path/file"
        def get_side_effect(key):
            # Check the actual key passed
            if "delta" in key:
                return io.BytesIO(b"delta content")
            elif "reference.bin" in key:
                # Return reference content for the reference file
                return io.BytesIO(ref_content)
            else:
                # Default case - return reference content
                return io.BytesIO(ref_content)

        mock_storage.get.side_effect = get_side_effect

        # Setup mock diff decode to create correct file
        def decode_correct(base, delta, out):
            out.write_bytes(test_content)

        mock_diff.decode.side_effect = decode_correct

        # Create cached reference
        ref_path = service.cache.ref_path("test-bucket", "test")
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        ref_path.write_bytes(ref_content)

        # Execute
        result = service.verify(delta_key)

        # Verify
        assert result.valid is True
        assert result.expected_sha256 == test_sha
        assert result.actual_sha256 == test_sha
        assert "verified" in result.message.lower()
