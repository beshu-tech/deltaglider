"""Tests for SDK filtering and delete cleanup functionality."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from deltaglider.app.cli.main import create_service
from deltaglider.client import DeltaGliderClient
from deltaglider.core import ObjectKey
from deltaglider.ports.storage import ObjectHead


class TestSDKFiltering:
    """Test that SDK filters .delta and reference.bin from list_objects()."""

    def test_list_objects_filters_delta_suffix(self):
        """Test that .delta suffix is stripped from object keys."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock list_objects response with .delta files
        mock_storage.list_objects.return_value = {
            "objects": [
                {
                    "key": "releases/app-v1.zip.delta",
                    "size": 1000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "abc123",
                    "storage_class": "STANDARD",
                },
                {
                    "key": "releases/app-v2.zip.delta",
                    "size": 1500,
                    "last_modified": "2025-01-02T00:00:00Z",
                    "etag": "def456",
                    "storage_class": "STANDARD",
                },
                {
                    "key": "releases/README.md",
                    "size": 500,
                    "last_modified": "2025-01-03T00:00:00Z",
                    "etag": "ghi789",
                    "storage_class": "STANDARD",
                },
            ],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }

        client = DeltaGliderClient(service)
        response = client.list_objects(Bucket="test-bucket", Prefix="releases/")

        # Response is now a boto3-compatible dict
        contents = response["Contents"]

        # Verify .delta suffix is stripped
        keys = [obj["Key"] for obj in contents]
        assert "releases/app-v1.zip" in keys
        assert "releases/app-v2.zip" in keys
        assert "releases/README.md" in keys

        # Verify NO .delta suffixes in output
        for key in keys:
            assert not key.endswith(".delta"), f"Found .delta suffix in: {key}"

        # Verify is_delta flag is set correctly in Metadata
        delta_objects = [
            obj for obj in contents if obj.get("Metadata", {}).get("deltaglider-is-delta") == "true"
        ]
        assert len(delta_objects) == 2

    def test_list_objects_filters_reference_bin(self):
        """Test that reference.bin files are completely filtered out."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock list_objects response with reference.bin files
        mock_storage.list_objects.return_value = {
            "objects": [
                {
                    "key": "releases/reference.bin",
                    "size": 50000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "ref123",
                    "storage_class": "STANDARD",
                },
                {
                    "key": "releases/1.0/reference.bin",
                    "size": 50000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "ref456",
                    "storage_class": "STANDARD",
                },
                {
                    "key": "releases/app.zip.delta",
                    "size": 1000,
                    "last_modified": "2025-01-02T00:00:00Z",
                    "etag": "app123",
                    "storage_class": "STANDARD",
                },
            ],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }

        client = DeltaGliderClient(service)
        response = client.list_objects(Bucket="test-bucket", Prefix="releases/")

        # Response is now a boto3-compatible dict
        contents = response["Contents"]

        # Verify NO reference.bin files in output
        keys = [obj["Key"] for obj in contents]
        for key in keys:
            assert not key.endswith("reference.bin"), f"Found reference.bin in: {key}"

        # Should only have the app.zip (with .delta stripped)
        assert len(contents) == 1
        assert contents[0]["Key"] == "releases/app.zip"
        assert contents[0].get("Metadata", {}).get("deltaglider-is-delta") == "true"

    def test_list_objects_combined_filtering(self):
        """Test filtering of both .delta and reference.bin together."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock comprehensive file list
        mock_storage.list_objects.return_value = {
            "objects": [
                {
                    "key": "data/reference.bin",
                    "size": 50000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "1",
                },
                {
                    "key": "data/file1.zip.delta",
                    "size": 1000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "2",
                },
                {
                    "key": "data/file2.zip.delta",
                    "size": 1500,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "3",
                },
                {
                    "key": "data/file3.txt",
                    "size": 500,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "4",
                },
                {
                    "key": "data/sub/reference.bin",
                    "size": 50000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "5",
                },
                {
                    "key": "data/sub/app.jar.delta",
                    "size": 2000,
                    "last_modified": "2025-01-01T00:00:00Z",
                    "etag": "6",
                },
            ],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }

        client = DeltaGliderClient(service)
        response = client.list_objects(Bucket="test-bucket", Prefix="data/")

        # Response is now a boto3-compatible dict
        contents = response["Contents"]

        # Should filter out 2 reference.bin files
        # Should strip .delta from 3 files
        # Should keep 1 regular file as-is
        assert len(contents) == 4  # 3 deltas + 1 regular file

        keys = [obj["Key"] for obj in contents]
        expected_keys = ["data/file1.zip", "data/file2.zip", "data/file3.txt", "data/sub/app.jar"]
        assert sorted(keys) == sorted(expected_keys)

        # Verify no internal files visible
        for key in keys:
            assert not key.endswith(".delta")
            assert not key.endswith("reference.bin")


class TestSingleDeleteCleanup:
    """Test that single delete() cleans up orphaned reference.bin."""

    def test_delete_last_delta_cleans_reference(self):
        """Test that deleting the last delta file removes orphaned reference.bin."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock head for both delta and reference.bin
        def mock_head_func(key):
            if key.endswith("app.zip.delta"):
                return ObjectHead(
                    key="releases/app.zip.delta",
                    size=1000,
                    etag="abc123",
                    last_modified=datetime.now(UTC),
                    metadata={"original_name": "app.zip", "ref_key": "releases/reference.bin"},
                )
            elif key.endswith("reference.bin"):
                return ObjectHead(
                    key="releases/reference.bin",
                    size=50000,
                    etag="ref123",
                    last_modified=datetime.now(UTC),
                    metadata={},
                )
            return None

        mock_storage.head.side_effect = mock_head_func

        # Mock list to show NO other deltas remain
        mock_storage.list.return_value = [
            ObjectHead(
                key="releases/reference.bin",
                size=50000,
                etag="ref123",
                last_modified=datetime.now(UTC),
                metadata={},
            ),
        ]
        mock_storage.delete.return_value = None

        # Delete the last delta
        result = service.delete(ObjectKey(bucket="test-bucket", key="releases/app.zip.delta"))

        # Verify delta was deleted
        assert result["deleted"] is True
        assert result["type"] == "delta"

        # Verify reference.bin cleanup was triggered
        assert "cleaned_reference" in result
        assert result["cleaned_reference"] == "releases/reference.bin"

        # Verify both files were deleted
        assert mock_storage.delete.call_count == 2
        delete_calls = [call[0][0] for call in mock_storage.delete.call_args_list]
        assert "test-bucket/releases/app.zip.delta" in delete_calls
        assert "test-bucket/releases/reference.bin" in delete_calls

    def test_delete_delta_keeps_reference_when_others_exist(self):
        """Test that reference.bin is kept when other deltas remain."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock the delta file being deleted
        mock_storage.head.return_value = ObjectHead(
            key="releases/app-v1.zip.delta",
            size=1000,
            etag="abc123",
            last_modified=datetime.now(UTC),
            metadata={"original_name": "app-v1.zip"},
        )

        # Mock list to show OTHER deltas still exist
        mock_storage.list.return_value = [
            ObjectHead(
                key="releases/app-v2.zip.delta",
                size=1500,
                etag="def456",
                last_modified=datetime.now(UTC),
                metadata={},
            ),
            ObjectHead(
                key="releases/reference.bin",
                size=50000,
                etag="ref123",
                last_modified=datetime.now(UTC),
                metadata={},
            ),
        ]

        mock_storage.delete.return_value = None

        # Delete one delta (but others remain)
        result = service.delete(ObjectKey(bucket="test-bucket", key="releases/app-v1.zip.delta"))

        # Verify delta was deleted
        assert result["deleted"] is True
        assert result["type"] == "delta"

        # Verify reference.bin was NOT cleaned up
        assert "cleaned_reference" not in result

        # Verify only the delta was deleted, not reference.bin
        assert mock_storage.delete.call_count == 1
        mock_storage.delete.assert_called_once_with("test-bucket/releases/app-v1.zip.delta")

    def test_delete_delta_no_reference_exists(self):
        """Test deleting delta when reference.bin doesn't exist (edge case)."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock the delta file
        mock_storage.head.return_value = ObjectHead(
            key="releases/app.zip.delta",
            size=1000,
            etag="abc123",
            last_modified=datetime.now(UTC),
            metadata={"original_name": "app.zip"},
        )

        # Mock list shows no other deltas
        mock_storage.list.return_value = []

        # Mock head for reference.bin returns None (doesn't exist)
        def mock_head_func(key):
            if key.endswith("reference.bin"):
                return None
            return ObjectHead(
                key="releases/app.zip.delta",
                size=1000,
                etag="abc123",
                last_modified=datetime.now(UTC),
                metadata={},
            )

        mock_storage.head.side_effect = mock_head_func
        mock_storage.delete.return_value = None

        # Delete the delta
        result = service.delete(ObjectKey(bucket="test-bucket", key="releases/app.zip.delta"))

        # Verify delta was deleted
        assert result["deleted"] is True
        assert result["type"] == "delta"

        # Verify no reference cleanup (since it didn't exist)
        assert "cleaned_reference" not in result

        # Only delta should be deleted
        assert mock_storage.delete.call_count == 1

    def test_delete_isolated_deltaspaces(self):
        """Test that cleanup only affects the specific DeltaSpace."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock head for both delta and reference.bin
        def mock_head_func(key):
            if "1.0/app.zip.delta" in key:
                return ObjectHead(
                    key="releases/1.0/app.zip.delta",
                    size=1000,
                    etag="abc123",
                    last_modified=datetime.now(UTC),
                    metadata={"original_name": "app.zip"},
                )
            elif "1.0/reference.bin" in key:
                return ObjectHead(
                    key="releases/1.0/reference.bin",
                    size=50000,
                    etag="ref1",
                    last_modified=datetime.now(UTC),
                    metadata={},
                )
            return None

        mock_storage.head.side_effect = mock_head_func

        # Mock list for 1.0 - no other deltas
        mock_storage.list.return_value = [
            ObjectHead(
                key="releases/1.0/reference.bin",
                size=50000,
                etag="ref1",
                last_modified=datetime.now(UTC),
                metadata={},
            ),
        ]
        mock_storage.delete.return_value = None

        # Delete from 1.0
        result = service.delete(ObjectKey(bucket="test-bucket", key="releases/1.0/app.zip.delta"))

        # Should clean up only 1.0/reference.bin
        assert result["cleaned_reference"] == "releases/1.0/reference.bin"

        # Verify correct files deleted
        delete_calls = [call[0][0] for call in mock_storage.delete.call_args_list]
        assert "test-bucket/releases/1.0/app.zip.delta" in delete_calls
        assert "test-bucket/releases/1.0/reference.bin" in delete_calls


class TestRecursiveDeleteCleanup:
    """Test that recursive delete properly cleans up references."""

    def test_recursive_delete_reference_cleanup_already_works(self):
        """Verify existing recursive delete reference cleanup is working."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock objects in deltaspace
        mock_storage.list.return_value = [
            ObjectHead(
                key="data/app.zip.delta",
                size=1000,
                etag="1",
                last_modified=datetime.now(UTC),
                metadata={},
            ),
            ObjectHead(
                key="data/reference.bin",
                size=50000,
                etag="2",
                last_modified=datetime.now(UTC),
                metadata={},
            ),
        ]

        mock_storage.head.return_value = None
        mock_storage.delete.return_value = None

        result = service.delete_recursive("test-bucket", "data/")

        # Should delete both delta and reference
        assert result["deleted_count"] == 2
        assert result["deltas_deleted"] == 1
        assert result["references_deleted"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
