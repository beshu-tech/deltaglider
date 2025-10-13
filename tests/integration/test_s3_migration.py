"""Test S3-to-S3 migration functionality."""

from unittest.mock import MagicMock, patch

import pytest

from deltaglider.app.cli.aws_compat import migrate_s3_to_s3
from deltaglider.core import DeltaService
from deltaglider.ports import ObjectHead


@pytest.fixture
def mock_service():
    """Create a mock DeltaService."""
    service = MagicMock(spec=DeltaService)
    service.storage = MagicMock()
    return service


def test_migrate_s3_to_s3_with_resume(mock_service):
    """Test migration with resume support (skips existing files)."""
    # Setup mock storage with source files
    source_objects = [
        ObjectHead(
            key="file1.zip",
            size=1024,
            etag="abc123",
            last_modified="2024-01-01T00:00:00Z",
            metadata={},
        ),
        ObjectHead(
            key="file2.zip",
            size=2048,
            etag="def456",
            last_modified="2024-01-01T00:00:00Z",
            metadata={},
        ),
        ObjectHead(
            key="subdir/file3.zip",
            size=512,
            etag="ghi789",
            last_modified="2024-01-01T00:00:00Z",
            metadata={},
        ),
    ]

    # Destination already has file1.zip (as .delta)
    dest_objects = [
        ObjectHead(
            key="file1.zip.delta",
            size=100,
            last_modified="2024-01-02T00:00:00Z",
            etag="delta123",
            metadata={},
        ),
    ]

    # Configure mock to return appropriate objects
    def list_side_effect(prefix):
        if "source-bucket" in prefix:
            return iter(source_objects)
        elif "dest-bucket" in prefix:
            return iter(dest_objects)
        return iter([])

    mock_service.storage.list.side_effect = list_side_effect

    # Mock the copy operation and click functions
    # Use quiet=True to skip EC2 detection logging
    with patch("deltaglider.app.cli.aws_compat.copy_s3_to_s3") as mock_copy:
        with patch("deltaglider.app.cli.aws_compat.click.confirm", return_value=True):
            migrate_s3_to_s3(
                mock_service,
                "s3://source-bucket/",
                "s3://dest-bucket/",
                exclude=None,
                include=None,
                quiet=True,  # Skip EC2 detection and logging
                no_delta=False,
                max_ratio=None,
                dry_run=False,
                skip_confirm=False,
            )

    # Should copy only file2.zip and subdir/file3.zip (file1 already exists)
    assert mock_copy.call_count == 2

    # Verify the files being migrated
    call_args = [call[0] for call in mock_copy.call_args_list]
    migrated_files = [(args[1], args[2]) for args in call_args]

    assert ("s3://source-bucket/file2.zip", "s3://dest-bucket/file2.zip") in migrated_files
    assert (
        "s3://source-bucket/subdir/file3.zip",
        "s3://dest-bucket/subdir/file3.zip",
    ) in migrated_files


def test_migrate_s3_to_s3_dry_run(mock_service):
    """Test dry run mode shows what would be migrated without actually migrating."""
    source_objects = [
        ObjectHead(
            key="file1.zip",
            size=1024,
            last_modified="2024-01-01T00:00:00Z",
            etag="abc123",
            metadata={},
        ),
    ]

    mock_service.storage.list.return_value = iter(source_objects)

    # Mock the copy operation and EC2 detection
    with patch("deltaglider.app.cli.aws_compat.copy_s3_to_s3") as mock_copy:
        with patch("deltaglider.app.cli.aws_compat.click.echo") as mock_echo:
            with patch("deltaglider.app.cli.aws_compat.log_aws_region"):
                migrate_s3_to_s3(
                    mock_service,
                    "s3://source-bucket/",
                    "s3://dest-bucket/",
                    exclude=None,
                    include=None,
                    quiet=False,  # Allow output to test dry run messages
                    no_delta=False,
                    max_ratio=None,
                    dry_run=True,
                    skip_confirm=False,
                )

    # Should not actually copy anything in dry run mode
    mock_copy.assert_not_called()

    # Should show dry run message
    echo_calls = [str(call[0][0]) for call in mock_echo.call_args_list if call[0]]
    assert any("DRY RUN MODE" in msg for msg in echo_calls)


def test_migrate_s3_to_s3_with_filters(mock_service):
    """Test migration with include/exclude filters."""
    source_objects = [
        ObjectHead(
            key="file1.zip",
            size=1024,
            last_modified="2024-01-01T00:00:00Z",
            etag="abc123",
            metadata={},
        ),
        ObjectHead(
            key="file2.log",
            size=256,
            last_modified="2024-01-01T00:00:00Z",
            etag="def456",
            metadata={},
        ),
        ObjectHead(
            key="file3.tar",
            size=512,
            last_modified="2024-01-01T00:00:00Z",
            etag="ghi789",
            metadata={},
        ),
    ]

    mock_service.storage.list.return_value = iter(source_objects)

    # Mock the copy operation
    with patch("deltaglider.app.cli.aws_compat.copy_s3_to_s3") as mock_copy:
        with patch("click.echo"):
            with patch("deltaglider.app.cli.aws_compat.click.confirm", return_value=True):
                # Exclude .log files
                migrate_s3_to_s3(
                    mock_service,
                    "s3://source-bucket/",
                    "s3://dest-bucket/",
                    exclude="*.log",
                    include=None,
                    quiet=True,  # Skip EC2 detection
                    no_delta=False,
                    max_ratio=None,
                    dry_run=False,
                    skip_confirm=False,
                )

    # Should copy file1.zip and file3.tar, but not file2.log
    assert mock_copy.call_count == 2

    call_args = [call[0] for call in mock_copy.call_args_list]
    migrated_sources = [args[1] for args in call_args]

    assert "s3://source-bucket/file1.zip" in migrated_sources
    assert "s3://source-bucket/file3.tar" in migrated_sources
    assert "s3://source-bucket/file2.log" not in migrated_sources


def test_migrate_s3_to_s3_skip_confirm(mock_service):
    """Test skipping confirmation prompt with skip_confirm=True."""
    source_objects = [
        ObjectHead(
            key="file1.zip",
            size=1024,
            last_modified="2024-01-01T00:00:00Z",
            etag="abc123",
            metadata={},
        ),
    ]

    mock_service.storage.list.return_value = iter(source_objects)

    with patch("deltaglider.app.cli.aws_compat.copy_s3_to_s3") as mock_copy:
        with patch("click.echo"):
            with patch("deltaglider.app.cli.aws_compat.click.confirm") as mock_confirm:
                migrate_s3_to_s3(
                    mock_service,
                    "s3://source-bucket/",
                    "s3://dest-bucket/",
                    exclude=None,
                    include=None,
                    quiet=True,  # Skip EC2 detection
                    no_delta=False,
                    max_ratio=None,
                    dry_run=False,
                    skip_confirm=True,  # Skip confirmation
                )

    # Should not ask for confirmation
    mock_confirm.assert_not_called()

    # Should still perform the copy
    mock_copy.assert_called_once()


def test_migrate_s3_to_s3_with_prefix(mock_service):
    """Test migration with source and destination prefixes."""
    source_objects = [
        ObjectHead(
            key="data/file1.zip",
            size=1024,
            last_modified="2024-01-01T00:00:00Z",
            etag="abc123",
            metadata={},
        ),
    ]

    def list_side_effect(prefix):
        if "source-bucket/data" in prefix:
            return iter(source_objects)
        return iter([])

    mock_service.storage.list.side_effect = list_side_effect

    with patch("deltaglider.app.cli.aws_compat.copy_s3_to_s3") as mock_copy:
        with patch("click.echo"):
            with patch("deltaglider.app.cli.aws_compat.click.confirm", return_value=True):
                migrate_s3_to_s3(
                    mock_service,
                    "s3://source-bucket/data/",
                    "s3://dest-bucket/archive/",
                    exclude=None,
                    include=None,
                    quiet=True,  # Skip EC2 detection
                    no_delta=False,
                    max_ratio=None,
                    dry_run=False,
                    skip_confirm=False,
                )

    # Verify the correct destination path is used
    mock_copy.assert_called_once()
    call_args = mock_copy.call_args[0]
    assert call_args[1] == "s3://source-bucket/data/file1.zip"
    assert call_args[2] == "s3://dest-bucket/archive/file1.zip"
