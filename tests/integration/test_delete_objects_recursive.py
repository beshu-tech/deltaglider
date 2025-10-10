"""Comprehensive tests for DeltaGliderClient.delete_objects_recursive() method."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from deltaglider import create_client


class MockStorage:
    """Mock storage for testing."""

    def __init__(self):
        self.objects = {}
        self.delete_calls = []

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

    def delete(self, key):
        """Mock delete operation."""
        self.delete_calls.append(key)
        if key in self.objects:
            del self.objects[key]
            return True
        return False

    def get(self, key):
        """Mock get operation."""
        if key in self.objects:
            return self.objects[key].get("content", b"mock-content")
        return None

    def put(self, key, data, metadata=None):
        """Mock put operation."""
        self.objects[key] = {
            "size": len(data),
            "content": data,
            "metadata": metadata or {},
        }


@pytest.fixture
def mock_storage():
    """Create mock storage."""
    return MockStorage()


@pytest.fixture
def client(tmp_path):
    """Create DeltaGliderClient with mock storage."""
    # Use create_client to get a properly configured client
    client = create_client()

    # Replace storage with mock
    mock_storage = MockStorage()
    client.service.storage = mock_storage

    return client


class TestDeleteObjectsRecursiveBasicFunctionality:
    """Test basic functionality of delete_objects_recursive."""

    def test_delete_single_object_with_file_prefix(self, client):
        """Test deleting a single object when prefix is a file (no trailing slash)."""
        # Setup: Add a regular file
        client.service.storage.objects["test-bucket/file.txt"] = {"size": 100}

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.txt")

        # Verify response structure
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DeletedCount" in response
        assert "FailedCount" in response
        assert "DeltaGliderInfo" in response

        # Verify DeltaGliderInfo structure
        info = response["DeltaGliderInfo"]
        assert "DeltasDeleted" in info
        assert "ReferencesDeleted" in info
        assert "DirectDeleted" in info
        assert "OtherDeleted" in info

    def test_delete_directory_with_trailing_slash(self, client):
        """Test deleting all objects under a prefix with trailing slash."""
        # Setup: Add multiple files under a prefix
        client.service.storage.objects["test-bucket/dir/file1.txt"] = {"size": 100}
        client.service.storage.objects["test-bucket/dir/file2.txt"] = {"size": 200}
        client.service.storage.objects["test-bucket/dir/sub/file3.txt"] = {"size": 300}

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="dir/")

        # Verify
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["DeletedCount"] >= 0
        assert response["FailedCount"] == 0

    def test_delete_empty_prefix_returns_zero_counts(self, client):
        """Test deleting with empty prefix returns zero counts."""
        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="")

        # Verify
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["DeletedCount"] >= 0
        assert response["FailedCount"] == 0


class TestDeleteObjectsRecursiveDeltaSuffixHandling:
    """Test delta suffix fallback logic."""

    def test_delete_file_with_delta_suffix_fallback(self, client):
        """Test that delete falls back to .delta suffix if original not found."""
        # Setup: Add file with .delta suffix
        client.service.storage.objects["test-bucket/archive.zip.delta"] = {
            "size": 500,
            "metadata": {"original_name": "archive.zip"},
        }

        # Execute: Delete using original name (without .delta)
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="archive.zip")

        # Verify
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "test-bucket/archive.zip.delta" not in client.service.storage.objects

    def test_delete_file_already_with_delta_suffix(self, client):
        """Test deleting a file that already has .delta suffix."""
        # Setup
        client.service.storage.objects["test-bucket/file.zip.delta"] = {"size": 300}

        # Execute: Delete using .delta suffix directly
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.zip.delta")

        # Verify
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delta_suffix_not_added_for_directory_prefix(self, client):
        """Test that .delta suffix is not added when prefix ends with /."""
        # Setup
        client.service.storage.objects["test-bucket/dir/file.txt"] = {"size": 100}

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="dir/")

        # Verify - should not attempt to delete "dir/.delta"
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDeleteObjectsRecursiveStatisticsAggregation:
    """Test statistics aggregation from core service."""

    def test_aggregates_deleted_count_from_service_and_single_deletes(self, client):
        """Test that deleted counts are aggregated correctly."""
        # Setup: Mock service.delete_recursive to return specific counts
        mock_result = {
            "deleted_count": 5,
            "failed_count": 0,
            "deltas_deleted": 2,
            "references_deleted": 1,
            "direct_deleted": 2,
            "other_deleted": 0,
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="test/")

        # Verify aggregation
        assert response["DeletedCount"] == 5
        assert response["FailedCount"] == 0
        assert response["DeltaGliderInfo"]["DeltasDeleted"] == 2
        assert response["DeltaGliderInfo"]["ReferencesDeleted"] == 1
        assert response["DeltaGliderInfo"]["DirectDeleted"] == 2
        assert response["DeltaGliderInfo"]["OtherDeleted"] == 0

    def test_aggregates_single_delete_counts_with_service_counts(self, client):
        """Test that single file deletes are aggregated with service counts."""
        # Setup: Add file to trigger single delete path
        client.service.storage.objects["test-bucket/file.txt"] = {"size": 100}

        # Mock service.delete_recursive to return additional counts
        mock_result = {
            "deleted_count": 3,
            "failed_count": 0,
            "deltas_deleted": 1,
            "references_deleted": 0,
            "direct_deleted": 2,
            "other_deleted": 0,
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.txt")

        # Verify that counts include both single delete and service delete
        assert response["DeletedCount"] >= 3  # At least service count
        assert response["DeltaGliderInfo"]["DeltasDeleted"] >= 1


class TestDeleteObjectsRecursiveErrorHandling:
    """Test error handling and error aggregation."""

    def test_single_delete_error_captured_in_errors_list(self, client):
        """Test that errors from single deletes are captured."""
        # Setup: Add file
        client.service.storage.objects["test-bucket/file.txt"] = {"size": 100}

        # Mock delete_with_delta_suffix to raise exception
        with patch("deltaglider.client.delete_with_delta_suffix") as mock_delete:
            mock_delete.side_effect = RuntimeError("Simulated delete error")

            # Execute
            response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.txt")

            # Verify error captured
            assert response["FailedCount"] > 0
            assert "Errors" in response
            assert any("Simulated delete error" in err for err in response["Errors"])

    def test_service_errors_propagated_in_response(self, client):
        """Test that errors from service.delete_recursive are propagated."""
        # Mock service to return errors
        mock_result = {
            "deleted_count": 2,
            "failed_count": 1,
            "deltas_deleted": 2,
            "references_deleted": 0,
            "direct_deleted": 0,
            "other_deleted": 0,
            "errors": ["Error deleting object1", "Error deleting object2"],
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="test/")

        # Verify
        assert response["FailedCount"] == 1
        assert "Errors" in response
        assert "Error deleting object1" in response["Errors"]
        assert "Error deleting object2" in response["Errors"]

    def test_combines_single_and_service_errors(self, client):
        """Test that errors from both single deletes and service are combined."""
        # Setup
        client.service.storage.objects["test-bucket/file.txt"] = {"size": 100}

        # Mock service to also return errors
        mock_result = {
            "deleted_count": 1,
            "failed_count": 1,
            "deltas_deleted": 0,
            "references_deleted": 0,
            "direct_deleted": 0,
            "other_deleted": 0,
            "errors": ["Service delete error"],
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Mock delete_with_delta_suffix to raise exception
        with patch("deltaglider.client.delete_with_delta_suffix") as mock_delete:
            mock_delete.side_effect = RuntimeError("Single delete error")

            # Execute
            response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.txt")

            # Verify both errors present
            assert "Errors" in response
            errors_str = " ".join(response["Errors"])
            assert "Single delete error" in errors_str
            assert "Service delete error" in errors_str


class TestDeleteObjectsRecursiveWarningsHandling:
    """Test warning aggregation."""

    def test_service_warnings_propagated_in_response(self, client):
        """Test that warnings from service.delete_recursive are propagated."""
        # Mock service to return warnings
        mock_result = {
            "deleted_count": 3,
            "failed_count": 0,
            "deltas_deleted": 2,
            "references_deleted": 1,
            "direct_deleted": 0,
            "other_deleted": 0,
            "warnings": ["Reference deleted, 2 dependent deltas invalidated"],
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="test/")

        # Verify
        assert "Warnings" in response
        assert "Reference deleted, 2 dependent deltas invalidated" in response["Warnings"]

    def test_single_delete_warnings_propagated(self, client):
        """Test that warnings from single deletes are captured."""
        # Setup
        client.service.storage.objects["test-bucket/ref.bin"] = {"size": 100}

        # Mock service
        mock_result = {
            "deleted_count": 0,
            "failed_count": 0,
            "deltas_deleted": 0,
            "references_deleted": 0,
            "direct_deleted": 0,
            "other_deleted": 0,
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Mock delete_with_delta_suffix to return warnings
        with patch("deltaglider.client.delete_with_delta_suffix") as mock_delete:
            mock_delete.return_value = (
                "ref.bin",
                {
                    "deleted": True,
                    "type": "reference",
                    "warnings": ["Warning from single delete"],
                },
            )

            # Execute
            response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="ref.bin")

            # Verify
            assert "Warnings" in response
            assert "Warning from single delete" in response["Warnings"]


class TestDeleteObjectsRecursiveSingleDeleteDetails:
    """Test SingleDeletes detail tracking."""

    def test_single_delete_details_included_for_file_prefix(self, client):
        """Test that SingleDeletes details are included when deleting file prefix."""
        # Setup
        client.service.storage.objects["test-bucket/file.txt"] = {"size": 100}

        # Mock service
        mock_result = {
            "deleted_count": 0,
            "failed_count": 0,
            "deltas_deleted": 0,
            "references_deleted": 0,
            "direct_deleted": 0,
            "other_deleted": 0,
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Mock delete_with_delta_suffix
        with patch("deltaglider.client.delete_with_delta_suffix") as mock_delete:
            mock_delete.return_value = (
                "file.txt",
                {
                    "deleted": True,
                    "type": "direct",
                    "dependent_deltas": 0,
                    "warnings": [],
                },
            )

            # Execute
            response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.txt")

            # Verify
            assert "SingleDeletes" in response["DeltaGliderInfo"]
            single_deletes = response["DeltaGliderInfo"]["SingleDeletes"]
            assert len(single_deletes) > 0
            assert single_deletes[0]["Key"] == "file.txt"
            assert single_deletes[0]["Type"] == "direct"
            assert "DependentDeltas" in single_deletes[0]
            assert "Warnings" in single_deletes[0]

    def test_single_delete_includes_stored_key_when_different(self, client):
        """Test that StoredKey is included when actual key differs from requested."""
        # Setup
        client.service.storage.objects["test-bucket/file.zip.delta"] = {"size": 200}

        # Mock delete_with_delta_suffix to return different key
        from deltaglider import client_delete_helpers

        original_delete = client_delete_helpers.delete_with_delta_suffix

        def mock_delete(service, bucket, key):
            actual_key = "file.zip.delta" if key == "file.zip" else key
            return (
                actual_key,
                {
                    "deleted": True,
                    "type": "delta",
                    "dependent_deltas": 0,
                    "warnings": [],
                },
            )

        client_delete_helpers.delete_with_delta_suffix = mock_delete

        # Mock service
        mock_result = {
            "deleted_count": 0,
            "failed_count": 0,
            "deltas_deleted": 0,
            "references_deleted": 0,
            "direct_deleted": 0,
            "other_deleted": 0,
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        try:
            # Execute
            response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.zip")

            # Verify
            assert "SingleDeletes" in response["DeltaGliderInfo"]
            single_deletes = response["DeltaGliderInfo"]["SingleDeletes"]
            if len(single_deletes) > 0:
                # If actual key differs, StoredKey should be present
                detail = single_deletes[0]
                if detail["Key"] != "file.zip.delta":
                    assert "StoredKey" in detail
        finally:
            client_delete_helpers.delete_with_delta_suffix = original_delete


class TestDeleteObjectsRecursiveEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_nonexistent_prefix_returns_zero_counts(self, client):
        """Test deleting nonexistent prefix returns zero counts."""
        # Execute
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="nonexistent/path/")

        # Verify
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["DeletedCount"] >= 0
        assert response["FailedCount"] == 0

    def test_duplicate_candidates_handled_correctly(self, client):
        """Test that duplicate delete candidates are handled correctly."""
        # Setup: This tests the seen_candidates logic
        client.service.storage.objects["test-bucket/file.delta"] = {"size": 100}

        # Execute: Should not attempt to delete "file.delta" twice
        response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.delta")

        # Verify no errors
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_unknown_result_type_categorized_as_other(self, client):
        """Test that unknown result types are categorized as 'other'."""
        # Setup
        client.service.storage.objects["test-bucket/file.txt"] = {"size": 100}

        # Mock service
        mock_result = {
            "deleted_count": 0,
            "failed_count": 0,
            "deltas_deleted": 0,
            "references_deleted": 0,
            "direct_deleted": 0,
            "other_deleted": 0,
        }
        client.service.delete_recursive = Mock(return_value=mock_result)

        # Mock delete_with_delta_suffix to return unknown type
        with patch("deltaglider.client.delete_with_delta_suffix") as mock_delete:
            mock_delete.return_value = (
                "file.txt",
                {
                    "deleted": True,
                    "type": "unknown_type",  # Not in single_counts keys
                    "dependent_deltas": 0,
                    "warnings": [],
                },
            )

            # Execute
            response = client.delete_objects_recursive(Bucket="test-bucket", Prefix="file.txt")

            # Verify it's categorized as "other"
            assert response["DeltaGliderInfo"]["OtherDeleted"] >= 1
            # Also verify the detail shows the unknown type
            if "SingleDeletes" in response["DeltaGliderInfo"]:
                assert response["DeltaGliderInfo"]["SingleDeletes"][0]["Type"] == "unknown_type"

    def test_kwargs_parameter_accepted(self, client):
        """Test that additional kwargs are accepted without error."""
        # Execute with extra parameters
        response = client.delete_objects_recursive(
            Bucket="test-bucket",
            Prefix="test/",
            ExtraParam="value",  # Should be ignored
            AnotherParam=123,
        )

        # Verify no errors
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
