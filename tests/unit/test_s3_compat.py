"""Tests for S3-compatible storage compatibility.

Ensures the S3 adapter works with non-AWS S3 endpoints (Hetzner, MinIO, etc.)
that don't support newer AWS-specific features like automatic request checksums.
"""

from unittest.mock import MagicMock, patch

from deltaglider.adapters.storage_s3 import S3StorageAdapter


class TestS3CompatibleEndpoints:
    """Verify S3 adapter configuration for non-AWS endpoint compatibility."""

    def test_client_disables_automatic_checksums(self):
        """boto3 1.36+ sends CRC32/CRC64 checksums by default.

        S3-compatible stores (Hetzner, MinIO) reject these with BadRequest.
        The adapter must set request_checksum_calculation='when_required'.
        """
        with patch("deltaglider.adapters.storage_s3.boto3.client") as mock_client:
            S3StorageAdapter(endpoint_url="https://example.com")

            mock_client.assert_called_once()
            call_kwargs = mock_client.call_args
            config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")

            assert config is not None, "boto3 client must be created with a Config object"
            assert config.request_checksum_calculation == "when_required"
            assert config.response_checksum_validation == "when_required"

    def test_put_object_no_checksum_kwargs(self, temp_dir):
        """put_object must not pass ChecksumAlgorithm or similar kwargs."""
        mock_client = MagicMock()
        mock_client.put_object.return_value = {"ETag": '"abc123"'}

        adapter = S3StorageAdapter(client=mock_client)

        test_file = temp_dir / "test.sha1"
        test_file.write_text("abc123")

        adapter.put(
            "my-bucket/test/test.sha1",
            test_file,
            {"compression": "none", "tool": "deltaglider"},
        )

        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args.kwargs

        checksum_keys = {
            "ChecksumAlgorithm",
            "ChecksumCRC32",
            "ChecksumCRC32C",
            "ChecksumCRC64NVME",
            "ChecksumSHA1",
            "ChecksumSHA256",
            "ContentMD5",
        }
        passed_checksum_keys = checksum_keys & set(call_kwargs.keys())
        assert not passed_checksum_keys, (
            f"put_object must not pass checksum kwargs for S3-compatible "
            f"endpoint support, but found: {passed_checksum_keys}"
        )

    def test_preconfigured_client_is_used_as_is(self):
        """When a pre-configured client is passed, it should be used directly."""
        mock_client = MagicMock()
        adapter = S3StorageAdapter(client=mock_client)
        assert adapter.client is mock_client
