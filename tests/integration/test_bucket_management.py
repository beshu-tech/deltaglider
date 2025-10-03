"""Tests for bucket management APIs."""

from unittest.mock import Mock

import pytest

from deltaglider.app.cli.main import create_service
from deltaglider.client import DeltaGliderClient


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
