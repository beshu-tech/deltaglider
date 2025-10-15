"""Tests for bucket management APIs."""

from typing import Any
from unittest.mock import Mock

import pytest

from deltaglider.app.cli.main import create_service
from deltaglider.client import DeltaGliderClient
from deltaglider.client_models import BucketStats


class TestBucketManagement:
    """Test bucket creation, listing, and deletion."""

    def test_create_bucket_success(self):
        """Test creating a bucket successfully."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client
        mock_boto3_client = Mock()
        mock_boto3_client.create_bucket.return_value = {"Location": "/test-bucket"}
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.create_bucket(Bucket="test-bucket")

        # Verify response
        assert response["Location"] == "/test-bucket"
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify boto3 was called correctly
        mock_boto3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")

    def test_create_bucket_with_region(self):
        """Test creating a bucket in a specific region."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client
        mock_boto3_client = Mock()
        mock_boto3_client.create_bucket.return_value = {
            "Location": "http://test-bucket.s3.us-west-2.amazonaws.com/"
        }
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )

        # Verify response
        assert "Location" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify boto3 was called with region config
        mock_boto3_client.create_bucket.assert_called_once_with(
            Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )

    def test_create_bucket_already_exists(self):
        """Test creating a bucket that already exists returns success."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client to raise BucketAlreadyExists
        mock_boto3_client = Mock()
        mock_boto3_client.create_bucket.side_effect = Exception("BucketAlreadyOwnedByYou")
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.create_bucket(Bucket="existing-bucket")

        # Should return success (idempotent)
        assert response["Location"] == "/existing-bucket"
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_buckets_success(self):
        """Test listing buckets."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client
        mock_boto3_client = Mock()
        mock_boto3_client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket1", "CreationDate": "2025-01-01T00:00:00Z"},
                {"Name": "bucket2", "CreationDate": "2025-01-02T00:00:00Z"},
            ],
            "Owner": {"DisplayName": "test-user", "ID": "12345"},
        }
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.list_buckets()

        # Verify response
        assert len(response["Buckets"]) == 2
        assert response["Buckets"][0]["Name"] == "bucket1"
        assert response["Buckets"][1]["Name"] == "bucket2"
        assert response["Owner"]["DisplayName"] == "test-user"
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_buckets_empty(self):
        """Test listing buckets when none exist."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client with empty result
        mock_boto3_client = Mock()
        mock_boto3_client.list_buckets.return_value = {"Buckets": [], "Owner": {}}
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.list_buckets()

        # Verify empty list
        assert response["Buckets"] == []
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_buckets_includes_cached_stats(self):
        """Bucket list should merge cached stats when available."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        mock_boto3_client = Mock()
        mock_boto3_client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket1", "CreationDate": "2025-01-01T00:00:00Z"},
                {"Name": "bucket2", "CreationDate": "2025-01-02T00:00:00Z"},
            ],
            "Owner": {"DisplayName": "test-user", "ID": "12345"},
        }
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)

        cached_stats = BucketStats(
            bucket="bucket1",
            object_count=10,
            total_size=1000,
            compressed_size=600,
            space_saved=400,
            average_compression_ratio=0.4,
            delta_objects=6,
            direct_objects=4,
        )
        client._store_bucket_stats_cache("bucket1", mode="detailed", stats=cached_stats)

        response = client.list_buckets()

        bucket1 = next(bucket for bucket in response["Buckets"] if bucket["Name"] == "bucket1")
        assert bucket1["DeltaGliderStats"]["Cached"] is True
        assert bucket1["DeltaGliderStats"]["Detailed"] is True
        assert bucket1["DeltaGliderStats"]["Mode"] == "detailed"
        assert bucket1["DeltaGliderStats"]["ObjectCount"] == cached_stats.object_count
        assert bucket1["DeltaGliderStats"]["TotalSize"] == cached_stats.total_size

        bucket2 = next(bucket for bucket in response["Buckets"] if bucket["Name"] == "bucket2")
        assert "DeltaGliderStats" not in bucket2

    def test_delete_bucket_success(self):
        """Test deleting a bucket successfully."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client
        mock_boto3_client = Mock()
        mock_boto3_client.delete_bucket.return_value = None
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.delete_bucket(Bucket="test-bucket")

        # Verify response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

        # Verify boto3 was called
        mock_boto3_client.delete_bucket.assert_called_once_with(Bucket="test-bucket")

    def test_delete_bucket_not_found(self):
        """Test deleting a bucket that doesn't exist returns success."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client to raise NoSuchBucket
        mock_boto3_client = Mock()
        mock_boto3_client.delete_bucket.side_effect = Exception("NoSuchBucket")
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)
        response = client.delete_bucket(Bucket="nonexistent-bucket")

        # Should return success (idempotent)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_delete_bucket_not_empty_raises_error(self):
        """Test deleting a non-empty bucket raises an error."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client to raise BucketNotEmpty
        mock_boto3_client = Mock()
        mock_boto3_client.delete_bucket.side_effect = Exception(
            "BucketNotEmpty: The bucket you tried to delete is not empty"
        )
        mock_storage.client = mock_boto3_client

        client = DeltaGliderClient(service)

        with pytest.raises(RuntimeError, match="Failed to delete bucket"):
            client.delete_bucket(Bucket="full-bucket")

    def test_get_bucket_stats_caches_per_session(self, monkeypatch):
        """Verify bucket stats are cached within the client session."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        mock_storage.client = Mock()

        client = DeltaGliderClient(service)

        quick_stats = BucketStats(
            bucket="bucket1",
            object_count=5,
            total_size=500,
            compressed_size=300,
            space_saved=200,
            average_compression_ratio=0.4,
            delta_objects=3,
            direct_objects=2,
        )
        detailed_stats = BucketStats(
            bucket="bucket1",
            object_count=5,
            total_size=520,
            compressed_size=300,
            space_saved=220,
            average_compression_ratio=0.423,
            delta_objects=3,
            direct_objects=2,
        )

        call_count = {"value": 0}

        def fake_get_bucket_stats(
            _: Any, bucket: str, mode: str, use_cache: bool = True, refresh_cache: bool = False
        ) -> BucketStats:
            call_count["value"] += 1
            assert bucket == "bucket1"
            if mode == "detailed":
                return detailed_stats
            if mode == "sampled":
                return detailed_stats  # sampled treated as detailed for cache propagation
            return quick_stats

        monkeypatch.setattr("deltaglider.client._get_bucket_stats", fake_get_bucket_stats)

        # First call should invoke underlying function
        result_quick = client.get_bucket_stats("bucket1")
        assert result_quick is quick_stats
        assert call_count["value"] == 1

        # Second quick call - caching is now done in _get_bucket_stats (S3-based)
        # So each call goes through _get_bucket_stats (which handles caching internally)
        assert client.get_bucket_stats("bucket1") is quick_stats
        assert call_count["value"] == 2

        # Detailed call triggers new computation
        result_detailed = client.get_bucket_stats("bucket1", mode="detailed")
        assert result_detailed is detailed_stats
        assert call_count["value"] == 3

        # Quick call - each mode has its own cache in _get_bucket_stats
        assert client.get_bucket_stats("bucket1") is quick_stats
        assert call_count["value"] == 4

    def test_bucket_methods_without_boto3_client(self):
        """Test that bucket methods raise NotImplementedError when storage doesn't support it."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Storage adapter without boto3 client (no 'client' attribute)
        delattr(mock_storage, "client")

        client = DeltaGliderClient(service)

        # All bucket methods should raise NotImplementedError
        with pytest.raises(NotImplementedError):
            client.create_bucket(Bucket="test")

        with pytest.raises(NotImplementedError):
            client.delete_bucket(Bucket="test")

        with pytest.raises(NotImplementedError):
            client.list_buckets()

    def test_complete_bucket_lifecycle(self):
        """Test complete bucket lifecycle: create, use, delete."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock boto3 client
        mock_boto3_client = Mock()
        mock_storage.client = mock_boto3_client

        # Setup responses
        mock_boto3_client.create_bucket.return_value = {"Location": "/test-lifecycle"}
        mock_boto3_client.list_buckets.return_value = {
            "Buckets": [{"Name": "test-lifecycle", "CreationDate": "2025-01-01T00:00:00Z"}],
            "Owner": {},
        }
        mock_boto3_client.delete_bucket.return_value = None

        client = DeltaGliderClient(service)

        # 1. Create bucket
        create_response = client.create_bucket(Bucket="test-lifecycle")
        assert create_response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # 2. List buckets - verify it exists
        list_response = client.list_buckets()
        bucket_names = [b["Name"] for b in list_response["Buckets"]]
        assert "test-lifecycle" in bucket_names

        # 3. Delete bucket
        delete_response = client.delete_bucket(Bucket="test-lifecycle")
        assert delete_response["ResponseMetadata"]["HTTPStatusCode"] == 204


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
