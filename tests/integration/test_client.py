"""Tests for the DeltaGlider client with boto3-compatible APIs."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from deltaglider import create_client
from deltaglider.client import (
    BucketStats,
    CompressionEstimate,
    ObjectInfo,
)


class MockStorage:
    """Mock storage for testing."""

    def __init__(self):
        self.objects = {}

    def head(self, key):
        """Mock head operation."""
        from deltaglider.ports.storage import ObjectHead

        if key in self.objects:
            obj = self.objects[key]
            return ObjectHead(
                key=key,
                size=obj["size"],
                etag=obj.get("etag", "mock-etag"),
                last_modified=obj.get("last_modified", datetime.now(UTC)),
                metadata=obj.get("metadata", {}),
            )
        return None

    def list(self, prefix):
        """Mock list operation for StoragePort interface."""
        for key, _obj in self.objects.items():
            if key.startswith(prefix):
                obj_head = self.head(key)
                if obj_head is not None:
                    yield obj_head

    def list_objects(
        self,
        bucket,
        prefix="",
        delimiter="",
        max_keys=1000,
        start_after=None,
        continuation_token=None,
    ):
        """Mock list_objects operation for S3 features."""
        objects = []
        common_prefixes = set()

        for key in sorted(self.objects.keys()):
            if not key.startswith(f"{bucket}/"):
                continue

            obj_key = key[len(bucket) + 1 :]  # Remove bucket prefix
            if prefix and not obj_key.startswith(prefix):
                continue

            if delimiter:
                # Find common prefixes
                rel_key = obj_key[len(prefix) :] if prefix else obj_key
                delimiter_pos = rel_key.find(delimiter)
                if delimiter_pos > -1:
                    common_prefix = prefix + rel_key[: delimiter_pos + 1]
                    common_prefixes.add(common_prefix)
                    continue

            obj = self.objects[key]
            objects.append(
                {
                    "key": obj_key,
                    "size": obj["size"],
                    "last_modified": obj.get("last_modified", "2025-01-01T00:00:00Z"),
                    "etag": obj.get("etag", "mock-etag"),
                    "storage_class": obj.get("storage_class", "STANDARD"),
                }
            )

            if len(objects) >= max_keys:
                break

        return {
            "objects": objects,
            "common_prefixes": sorted(list(common_prefixes)),
            "is_truncated": False,
            "next_continuation_token": None,
            "key_count": len(objects),
        }

    def get(self, key):
        """Mock get operation."""
        import io

        if key in self.objects:
            return io.BytesIO(self.objects[key].get("data", b"mock data"))
        raise FileNotFoundError(f"Object not found: {key}")

    def put(self, key, body, metadata, content_type="application/octet-stream"):
        """Mock put operation."""
        from deltaglider.ports.storage import PutResult

        if hasattr(body, "read"):
            data = body.read()
        elif isinstance(body, Path):
            data = body.read_bytes()
        else:
            data = body

        self.objects[key] = {
            "data": data,
            "size": len(data),
            "metadata": metadata,
            "content_type": content_type,
        }

        return PutResult(etag="mock-etag", version_id=None)

    def delete(self, key):
        """Mock delete operation."""
        if key in self.objects:
            del self.objects[key]


@pytest.fixture
def client(tmp_path):
    """Create a client with mocked storage."""
    client = create_client()

    # Replace storage with mock
    mock_storage = MockStorage()
    client.service.storage = mock_storage

    # Pre-populate some test objects
    mock_storage.objects = {
        "test-bucket/file1.txt": {"size": 100, "metadata": {}},
        "test-bucket/folder1/file2.txt": {"size": 200, "metadata": {}},
        "test-bucket/folder1/file3.txt": {"size": 300, "metadata": {}},
        "test-bucket/folder2/file4.txt": {"size": 400, "metadata": {}},
        "test-bucket/archive.zip.delta": {
            "size": 50,
            "metadata": {"file_size": "1000", "compression_ratio": "0.95"},
        },
    }

    return client


class TestCredentialHandling:
    """Test AWS credential passing."""

    def test_create_client_with_explicit_credentials(self, tmp_path):
        """Test that credentials can be passed directly to create_client."""
        # This test verifies the API accepts credentials, not that they work
        # (we'd need a real S3 or LocalStack for that)
        client = create_client(
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region_name="us-west-2",
        )

        # Verify the client was created
        assert client is not None
        assert client.service is not None

        # Verify credentials were passed to the storage adapter's boto3 client
        # The storage adapter should have a client with these credentials
        storage = client.service.storage
        assert hasattr(storage, "client")

        # Check that the boto3 client was configured with our credentials
        # Note: boto3 doesn't expose credentials directly, but we can verify
        # the client was created (if credentials were invalid, this would fail)
        assert storage.client is not None

    def test_create_client_with_session_token(self, tmp_path):
        """Test passing temporary credentials with session token."""
        client = create_client(
            aws_access_key_id="ASIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            aws_session_token="FwoGZXIvYXdzEBEaDH...",
        )

        assert client is not None
        assert client.service.storage.client is not None

    def test_create_client_without_credentials_uses_environment(self, tmp_path):
        """Test that omitting credentials falls back to environment/IAM."""
        # This should use boto3's default credential chain
        client = create_client()

        assert client is not None
        assert client.service.storage.client is not None

    def test_create_client_with_endpoint_and_credentials(self, tmp_path):
        """Test passing both endpoint URL and credentials."""
        client = create_client(
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        assert client is not None
        # Endpoint should be available
        assert client.endpoint_url == "http://localhost:9000"


class TestBoto3Compatibility:
    """Test boto3-compatible methods."""

    def test_put_object_with_bytes(self, client):
        """Test put_object with byte data."""
        response = client.put_object(Bucket="test-bucket", Key="test.txt", Body=b"Hello World")

        assert "ETag" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Check object was stored
        obj = client.service.storage.objects["test-bucket/test.txt"]
        assert obj["data"] == b"Hello World"

    def test_put_object_with_string(self, client):
        """Test put_object with string data."""
        response = client.put_object(Bucket="test-bucket", Key="test2.txt", Body="Hello String")

        assert "ETag" in response
        obj = client.service.storage.objects["test-bucket/test2.txt"]
        assert obj["data"] == b"Hello String"

    def test_get_object(self, client):
        """Test get_object retrieval."""
        # For this test, we'll bypass the DeltaGlider logic and test the client directly
        # Since the core DeltaGlider always looks for .delta files, we'll mock a .delta file
        import hashlib

        content = b"Test Content"
        sha256 = hashlib.sha256(content).hexdigest()

        # Add as a direct file (not delta)
        client.service.storage.objects["test-bucket/get-test.txt"] = {
            "data": content,
            "size": len(content),
            "metadata": {
                "file_sha256": sha256,
                "file_size": str(len(content)),
                "original_name": "get-test.txt",
                "compression": "none",  # Mark as direct upload
                "tool": "deltaglider/0.2.0",
            },
        }

        response = client.get_object(Bucket="test-bucket", Key="get-test.txt")

        assert "Body" in response
        content = response["Body"].read()
        assert content == b"Test Content"

    def test_get_object_regular_s3_file(self, client):
        """Test get_object with regular S3 files (not uploaded via DeltaGlider)."""

        content = b"Regular S3 File Content"

        # Add as a regular S3 object WITHOUT DeltaGlider metadata
        client.service.storage.objects["test-bucket/regular-file.pdf"] = {
            "data": content,
            "size": len(content),
            "metadata": {},  # No DeltaGlider metadata
        }

        # Should successfully download the regular S3 object
        response = client.get_object(Bucket="test-bucket", Key="regular-file.pdf")

        assert "Body" in response
        downloaded_content = response["Body"].read()
        assert downloaded_content == content
        assert response["ContentLength"] == len(content)

    def test_list_objects(self, client):
        """Test list_objects with various options (boto3-compatible dict response)."""
        # List all objects (default: FetchMetadata=False)
        response = client.list_objects(Bucket="test-bucket")

        # Response is now a boto3-compatible dict (not ListObjectsResponse)
        assert isinstance(response, dict)
        assert response["KeyCount"] > 0
        assert len(response["Contents"]) > 0

        # Verify S3Object structure
        for obj in response["Contents"]:
            assert "Key" in obj
            assert "Size" in obj
            assert "LastModified" in obj
            assert "Metadata" in obj  # DeltaGlider metadata

        # Test with FetchMetadata=True (should only affect delta files)
        response_with_metadata = client.list_objects(Bucket="test-bucket", FetchMetadata=True)
        assert isinstance(response_with_metadata, dict)
        assert response_with_metadata["KeyCount"] > 0

    def test_list_objects_with_delimiter(self, client):
        """Test list_objects with delimiter for folder simulation (boto3-compatible dict response)."""
        response = client.list_objects(Bucket="test-bucket", Prefix="", Delimiter="/")

        # Should have common prefixes for folders
        assert len(response.get("CommonPrefixes", [])) > 0
        assert {"Prefix": "folder1/"} in response["CommonPrefixes"]
        assert {"Prefix": "folder2/"} in response["CommonPrefixes"]

    def test_delete_object(self, client):
        """Test delete_object."""
        # Add object
        client.service.storage.objects["test-bucket/to-delete.txt"] = {"size": 10}

        response = client.delete_object(Bucket="test-bucket", Key="to-delete.txt")

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
        assert "test-bucket/to-delete.txt" not in client.service.storage.objects

    def test_delete_object_with_delta_suffix_fallback(self, client):
        """Test delete_object with automatic .delta suffix fallback."""
        # Add object with .delta suffix (as DeltaGlider stores it)
        client.service.storage.objects["test-bucket/file.zip.delta"] = {
            "size": 100,
            "metadata": {
                "original_name": "file.zip",
                "compression": "delta",
            },
        }

        # Delete using original name (without .delta)
        response = client.delete_object(Bucket="test-bucket", Key="file.zip")

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
        assert response["DeltaGliderInfo"]["Deleted"] is True
        assert "test-bucket/file.zip.delta" not in client.service.storage.objects

    def test_delete_objects(self, client):
        """Test batch delete."""
        # Add objects
        client.service.storage.objects["test-bucket/del1.txt"] = {"size": 10}
        client.service.storage.objects["test-bucket/del2.txt"] = {"size": 20}

        response = client.delete_objects(
            Bucket="test-bucket",
            Delete={"Objects": [{"Key": "del1.txt"}, {"Key": "del2.txt"}]},
        )

        assert len(response["Deleted"]) == 2
        assert "test-bucket/del1.txt" not in client.service.storage.objects


class TestDeltaGliderFeatures:
    """Test DeltaGlider-specific features."""

    def test_compression_estimation_for_archive(self, client, tmp_path):
        """Test compression estimation for archive files."""
        # Create a fake zip file
        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"PK\x03\x04" + b"0" * 1000)

        estimate = client.estimate_compression(test_file, "test-bucket", "archives/")

        assert isinstance(estimate, CompressionEstimate)
        assert estimate.should_use_delta is True
        assert estimate.original_size == test_file.stat().st_size

    def test_compression_estimation_for_image(self, client, tmp_path):
        """Test compression estimation for incompressible files."""
        test_file = tmp_path / "image.jpg"
        test_file.write_bytes(b"\xff\xd8\xff" + b"0" * 1000)  # JPEG header

        estimate = client.estimate_compression(test_file, "test-bucket", "images/")

        assert estimate.should_use_delta is False
        assert estimate.estimated_ratio == 0.0

    def test_find_similar_files(self, client):
        """Test finding similar files for delta compression."""
        similar = client.find_similar_files("test-bucket", "folder1/", "file_v1.txt")

        assert isinstance(similar, list)
        # Should find files in folder1
        assert any("folder1/" in item["Key"] for item in similar)

    def test_upload_batch(self, client, tmp_path):
        """Test batch upload functionality."""
        # Create test files
        files = []
        for i in range(3):
            f = tmp_path / f"batch{i}.txt"
            f.write_text(f"Content {i}")
            files.append(f)

        results = client.upload_batch(files, "s3://test-bucket/batch/")

        assert len(results) == 3
        for result in results:
            assert result.original_size > 0

    def test_download_batch(self, client, tmp_path):
        """Test batch download functionality."""
        # Add test objects with proper metadata
        for i in range(3):
            key = f"test-bucket/download/file{i}.txt"
            content = f"Content {i}".encode()
            client.service.storage.objects[key] = {
                "data": content,
                "size": len(content),
                "metadata": {
                    "file_sha256": hashlib.sha256(content).hexdigest(),
                    "file_size": str(len(content)),
                    "compression": "none",  # Mark as direct upload
                    "tool": "deltaglider/0.2.0",
                },
            }

        s3_urls = [f"s3://test-bucket/download/file{i}.txt" for i in range(3)]
        results = client.download_batch(s3_urls, tmp_path)

        assert len(results) == 3
        for i, path in enumerate(results):
            assert path.exists()
            assert path.read_text() == f"Content {i}"

    def test_get_object_info(self, client):
        """Test getting detailed object information."""
        # Use the pre-populated delta object
        info = client.get_object_info("s3://test-bucket/archive.zip.delta")

        assert isinstance(info, ObjectInfo)
        assert info.is_delta is True
        assert info.original_size == 1000
        assert info.compressed_size == 50
        assert info.compression_ratio == 0.95

    def test_get_bucket_stats(self, client):
        """Test getting bucket statistics."""
        # Test quick stats (LIST only)
        stats = client.get_bucket_stats("test-bucket")

        assert isinstance(stats, BucketStats)
        assert stats.object_count > 0
        assert stats.total_size > 0
        assert stats.delta_objects >= 1  # We have archive.zip.delta

        # Test with detailed mode
        detailed_stats = client.get_bucket_stats("test-bucket", mode="detailed")
        assert isinstance(detailed_stats, BucketStats)
        assert detailed_stats.object_count == stats.object_count

    def test_upload_chunked(self, client, tmp_path):
        """Test chunked upload with progress callback."""
        # Create a test file
        test_file = tmp_path / "large.bin"
        test_file.write_bytes(b"X" * (10 * 1024))  # 10KB

        progress_calls = []

        def progress_callback(chunk_num, total_chunks, bytes_sent, total_bytes):
            progress_calls.append((chunk_num, total_chunks, bytes_sent, total_bytes))

        result = client.upload_chunked(
            test_file,
            "s3://test-bucket/large.bin",
            chunk_size=3 * 1024,  # 3KB chunks
            progress_callback=progress_callback,
        )

        assert result.original_size == 10 * 1024
        assert len(progress_calls) > 0  # Progress was reported

    def test_generate_presigned_url(self, client):
        """Test presigned URL generation (placeholder)."""
        url = client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": "test-bucket", "Key": "file.txt"},
            ExpiresIn=3600,
        )

        assert isinstance(url, str)
        assert "file.txt" in url
        assert "expires=3600" in url
