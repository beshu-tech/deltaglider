"""Focused tests for recursive delete reference cleanup functionality."""

from unittest.mock import Mock, patch

import pytest

from deltaglider.app.cli.main import create_service
from deltaglider.ports.storage import ObjectHead


class TestRecursiveDeleteReferenceCleanup:
    """Test the core reference cleanup intelligence in recursive delete."""

    def test_core_service_delete_recursive_method_exists(self):
        """Test that the core service has the delete_recursive method."""
        service = create_service()
        assert hasattr(service, "delete_recursive")
        assert callable(service.delete_recursive)

    def test_delete_recursive_handles_empty_prefix(self):
        """Test delete_recursive gracefully handles empty prefixes."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock empty result
        mock_storage.list.return_value = []

        result = service.delete_recursive("test-bucket", "nonexistent/")

        assert result["deleted_count"] == 0
        assert result["failed_count"] == 0
        assert isinstance(result["errors"], list)
        assert isinstance(result["warnings"], list)

    def test_delete_recursive_returns_structured_result(self):
        """Test that delete_recursive returns a properly structured result."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock some objects
        mock_storage.list.return_value = [
            ObjectHead(
                key="test/file1.zip.delta", size=100, etag="1", last_modified=None, metadata={}
            ),
            ObjectHead(
                key="test/file2.txt",
                size=200,
                etag="2",
                last_modified=None,
                metadata={"compression": "none"},
            ),
        ]
        mock_storage.head.return_value = None
        mock_storage.delete.return_value = None

        result = service.delete_recursive("test-bucket", "test/")

        # Verify structure
        required_keys = [
            "bucket",
            "prefix",
            "deleted_count",
            "failed_count",
            "deltas_deleted",
            "references_deleted",
            "direct_deleted",
            "other_deleted",
            "errors",
            "warnings",
        ]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"

        assert isinstance(result["deleted_count"], int)
        assert isinstance(result["failed_count"], int)
        assert isinstance(result["errors"], list)
        assert isinstance(result["warnings"], list)

    def test_delete_recursive_categorizes_objects_correctly(self):
        """Test that delete_recursive correctly categorizes different object types."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock different types of objects
        mock_objects = [
            ObjectHead(
                key="test/app.zip.delta",
                size=100,
                etag="1",
                last_modified=None,
                metadata={"ref_key": "test/reference.bin"},
            ),
            ObjectHead(
                key="test/reference.bin",
                size=50,
                etag="2",
                last_modified=None,
                metadata={"file_sha256": "abc123"},
            ),
            ObjectHead(
                key="test/readme.txt",
                size=200,
                etag="3",
                last_modified=None,
                metadata={"compression": "none"},
            ),
            ObjectHead(key="test/config.json", size=300, etag="4", last_modified=None, metadata={}),
        ]

        mock_storage.list.return_value = mock_objects
        mock_storage.head.return_value = None  # No dependencies found
        mock_storage.delete.return_value = None

        result = service.delete_recursive("test-bucket", "test/")

        # Should categorize correctly - the exact categorization depends on implementation
        assert result["deltas_deleted"] == 1  # app.zip.delta
        assert result["references_deleted"] == 1  # reference.bin
        # Direct and other files may be categorized differently based on metadata detection
        assert result["direct_deleted"] + result["other_deleted"] == 2  # readme.txt + config.json
        assert result["deleted_count"] == 4  # total
        assert result["failed_count"] == 0

    def test_delete_recursive_handles_storage_errors_gracefully(self):
        """Test that delete_recursive handles individual storage errors gracefully."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mock objects
        mock_storage.list.return_value = [
            ObjectHead(
                key="test/good.zip.delta", size=100, etag="1", last_modified=None, metadata={}
            ),
            ObjectHead(
                key="test/bad.zip.delta", size=200, etag="2", last_modified=None, metadata={}
            ),
        ]
        mock_storage.head.return_value = None

        # Mock delete to fail for one file
        def failing_delete(key):
            if "bad" in key:
                raise Exception("Simulated S3 error")

        mock_storage.delete.side_effect = failing_delete

        result = service.delete_recursive("test-bucket", "test/")

        # Should handle partial failure
        assert result["deleted_count"] == 1  # good.zip.delta succeeded
        assert result["failed_count"] == 1  # bad.zip.delta failed
        assert len(result["errors"]) == 1
        assert "bad" in result["errors"][0]

    def test_affected_deltaspaces_discovery(self):
        """Test that the system discovers affected deltaspaces when deleting deltas."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Create delta files that should trigger parent reference checking
        mock_objects = [
            ObjectHead(
                key="project/team-a/v1/app.zip.delta",
                size=100,
                etag="1",
                last_modified=None,
                metadata={"ref_key": "project/reference.bin"},
            ),
        ]

        # Mock list to return objects for initial scan, then parent reference when checked
        list_calls = []

        def mock_list(prefix):
            list_calls.append(prefix)
            if prefix == "test-bucket/project/team-a/v1/":
                return mock_objects
            elif prefix == "test-bucket/project":
                # Return parent reference when checking deltaspace
                return [
                    ObjectHead(
                        key="project/reference.bin",
                        size=50,
                        etag="ref",
                        last_modified=None,
                        metadata={"file_sha256": "abc123"},
                    )
                ]
            return []

        mock_storage.list.side_effect = mock_list
        mock_storage.head.return_value = ObjectHead(
            key="project/reference.bin",
            size=50,
            etag="ref",
            last_modified=None,
            metadata={"file_sha256": "abc123"},
        )
        mock_storage.delete.return_value = None

        result = service.delete_recursive("test-bucket", "project/team-a/v1/")

        # Should have discovered and evaluated the parent reference
        assert result["deleted_count"] >= 1  # At least the delta file
        assert result["failed_count"] == 0

    def test_cli_uses_core_service_method(self):
        """Test that CLI rm -r command uses the core service delete_recursive method."""
        from click.testing import CliRunner

        from deltaglider.app.cli.main import cli

        runner = CliRunner()

        with patch("deltaglider.app.cli.main.create_service") as mock_create_service:
            mock_service = Mock()
            mock_create_service.return_value = mock_service

            # Mock successful deletion
            mock_service.delete_recursive.return_value = {
                "bucket": "test-bucket",
                "prefix": "test/",
                "deleted_count": 2,
                "failed_count": 0,
                "warnings": [],
                "errors": [],
            }

            result = runner.invoke(cli, ["rm", "-r", "s3://test-bucket/test/"])

            assert result.exit_code == 0
            mock_service.delete_recursive.assert_called_once_with("test-bucket", "test")
            assert "Deleted 2 object(s)" in result.output

    def test_cli_dryrun_does_not_call_delete_recursive(self):
        """Test that CLI dryrun does not call the actual delete_recursive method."""
        from click.testing import CliRunner

        from deltaglider.app.cli.main import cli

        runner = CliRunner()

        with patch("deltaglider.app.cli.main.create_service") as mock_create_service:
            mock_service = Mock()
            mock_create_service.return_value = mock_service

            # Mock list for dryrun preview
            mock_service.storage.list.return_value = [
                ObjectHead(
                    key="test/file1.zip.delta", size=100, etag="1", last_modified=None, metadata={}
                ),
                ObjectHead(
                    key="test/file2.txt", size=200, etag="2", last_modified=None, metadata={}
                ),
            ]

            result = runner.invoke(cli, ["rm", "-r", "--dryrun", "s3://test-bucket/test/"])

            assert result.exit_code == 0
            mock_service.delete_recursive.assert_not_called()  # Should not call actual deletion
            assert "(dryrun) delete:" in result.output
            assert "Would delete 2 object(s)" in result.output

    def test_integration_with_existing_single_delete(self):
        """Test that recursive delete integrates well with existing single delete functionality."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Test that both methods exist and are callable
        assert hasattr(service, "delete")
        assert hasattr(service, "delete_recursive")
        assert callable(service.delete)
        assert callable(service.delete_recursive)

        # Mock for single delete
        mock_storage.head.return_value = ObjectHead(
            key="test/file.zip.delta",
            size=100,
            etag="1",
            last_modified=None,
            metadata={"original_name": "file.zip"},
        )
        mock_storage.list.return_value = []  # No other deltas remain
        mock_storage.delete.return_value = None

        # Test single delete
        from deltaglider.core import ObjectKey

        result = service.delete(ObjectKey(bucket="test-bucket", key="test/file.zip.delta"))

        assert result["deleted"]
        assert result["type"] == "delta"

    def test_reference_cleanup_intelligence_basic(self):
        """Basic test to verify reference cleanup intelligence is working."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Simple scenario: one delta and its reference
        mock_objects = [
            ObjectHead(
                key="simple/file.zip.delta",
                size=100,
                etag="1",
                last_modified=None,
                metadata={"ref_key": "simple/reference.bin"},
            ),
            ObjectHead(
                key="simple/reference.bin",
                size=50,
                etag="2",
                last_modified=None,
                metadata={"file_sha256": "abc123"},
            ),
        ]

        mock_storage.list.return_value = mock_objects
        mock_storage.head.return_value = None  # No other dependencies
        mock_storage.delete.return_value = None

        result = service.delete_recursive("test-bucket", "simple/")

        # Should delete both delta and reference since there are no other dependencies
        assert result["deleted_count"] == 2
        assert result["deltas_deleted"] == 1
        assert result["references_deleted"] == 1
        assert result["failed_count"] == 0

    def test_comprehensive_result_validation(self):
        """Test that all result fields are properly populated."""
        service = create_service()
        mock_storage = Mock()
        service.storage = mock_storage

        # Mix of different object types
        mock_objects = [
            ObjectHead(
                key="mixed/app.zip.delta", size=100, etag="1", last_modified=None, metadata={}
            ),
            ObjectHead(
                key="mixed/reference.bin", size=50, etag="2", last_modified=None, metadata={}
            ),
            ObjectHead(
                key="mixed/readme.txt",
                size=200,
                etag="3",
                last_modified=None,
                metadata={"compression": "none"},
            ),
            ObjectHead(
                key="mixed/config.json", size=300, etag="4", last_modified=None, metadata={}
            ),
        ]

        mock_storage.list.return_value = mock_objects
        mock_storage.head.return_value = None
        mock_storage.delete.return_value = None

        result = service.delete_recursive("test-bucket", "mixed/")

        # Validate all expected fields are present and have correct types
        assert isinstance(result["bucket"], str)
        assert isinstance(result["prefix"], str)
        assert isinstance(result["deleted_count"], int)
        assert isinstance(result["failed_count"], int)
        assert isinstance(result["deltas_deleted"], int)
        assert isinstance(result["references_deleted"], int)
        assert isinstance(result["direct_deleted"], int)
        assert isinstance(result["other_deleted"], int)
        assert isinstance(result["errors"], list)
        assert isinstance(result["warnings"], list)

        # Validate counts add up
        total_by_type = (
            result["deltas_deleted"]
            + result["references_deleted"]
            + result["direct_deleted"]
            + result["other_deleted"]
        )
        assert result["deleted_count"] == total_by_type

        # Validate specific counts for this scenario
        assert result["deltas_deleted"] == 1
        assert result["references_deleted"] == 1
        # Direct and other files may be categorized differently
        assert result["direct_deleted"] + result["other_deleted"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
