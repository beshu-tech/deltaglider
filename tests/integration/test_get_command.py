"""Integration test for get command."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from deltaglider.app.cli.main import cli
from deltaglider.core import ObjectKey


@pytest.fixture
def mock_service():
    """Create a mock DeltaService."""
    return Mock()


def test_get_command_with_original_name(mock_service):
    """Test get command with original filename (auto-appends .delta)."""
    runner = CliRunner()

    # Mock the service.get method and storage.head
    mock_service.get = Mock()
    mock_service.storage.head = Mock(
        side_effect=[
            None,  # First check for original file returns None
            Mock(),  # Second check for .delta file returns something
        ]
    )

    with patch("deltaglider.app.cli.main.create_service", return_value=mock_service):
        # Run get with original filename (should auto-append .delta)
        result = runner.invoke(cli, ["get", "s3://test-bucket/data/myfile.zip"])

        # Check it was successful
        assert result.exit_code == 0
        assert "Found delta file: s3://test-bucket/data/myfile.zip.delta" in result.output
        assert "Successfully retrieved: myfile.zip" in result.output

        # Verify the service was called with the correct arguments
        mock_service.get.assert_called_once()
        call_args = mock_service.get.call_args
        obj_key = call_args[0][0]
        output_path = call_args[0][1]

        assert isinstance(obj_key, ObjectKey)
        assert obj_key.bucket == "test-bucket"
        assert obj_key.key == "data/myfile.zip.delta"
        assert output_path == Path("myfile.zip")


def test_get_command_with_delta_name(mock_service):
    """Test get command with explicit .delta filename."""
    runner = CliRunner()

    # Mock the service.get method and storage.head
    mock_service.get = Mock()
    mock_service.storage.head = Mock(return_value=Mock())  # File exists

    with patch("deltaglider.app.cli.main.create_service", return_value=mock_service):
        # Run get with explicit .delta filename
        result = runner.invoke(cli, ["get", "s3://test-bucket/data/myfile.zip.delta"])

        # Check it was successful
        assert result.exit_code == 0
        assert "Found file: s3://test-bucket/data/myfile.zip.delta" in result.output
        assert "Successfully retrieved: myfile.zip" in result.output

        # Verify the service was called with the correct arguments
        mock_service.get.assert_called_once()
        call_args = mock_service.get.call_args
        obj_key = call_args[0][0]
        output_path = call_args[0][1]

        assert isinstance(obj_key, ObjectKey)
        assert obj_key.bucket == "test-bucket"
        assert obj_key.key == "data/myfile.zip.delta"
        assert output_path == Path("myfile.zip")


def test_get_command_with_output_option(mock_service):
    """Test get command with custom output path."""
    runner = CliRunner()

    # Mock the service.get method and storage.head
    mock_service.get = Mock()
    mock_service.storage.head = Mock(
        side_effect=[
            None,  # First check for original file returns None
            Mock(),  # Second check for .delta file returns something
        ]
    )

    with patch("deltaglider.app.cli.main.create_service", return_value=mock_service):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "custom_output.zip"

            # Run get with custom output path
            result = runner.invoke(
                cli, ["get", "s3://test-bucket/data/myfile.zip", "-o", str(output_file)]
            )

            # Check it was successful
            assert result.exit_code == 0
            assert f"Successfully retrieved: {output_file}" in result.output

            # Verify the service was called with the correct arguments
            mock_service.get.assert_called_once()
            call_args = mock_service.get.call_args
            obj_key = call_args[0][0]
            output_path = call_args[0][1]

            assert isinstance(obj_key, ObjectKey)
            assert obj_key.bucket == "test-bucket"
            assert obj_key.key == "data/myfile.zip.delta"
            assert output_path == output_file


def test_get_command_error_handling(mock_service):
    """Test get command error handling."""
    runner = CliRunner()

    # Mock the service.get method to raise an error
    mock_service.get = Mock(side_effect=FileNotFoundError("Delta not found"))

    with patch("deltaglider.app.cli.main.create_service", return_value=mock_service):
        # Run get command
        result = runner.invoke(cli, ["get", "s3://test-bucket/data/missing.zip"])

        # Check it failed with error message
        assert result.exit_code == 1
        assert "Error: Delta not found" in result.output


def test_get_command_invalid_url():
    """Test get command with invalid S3 URL."""
    runner = CliRunner()

    # Run get with invalid URL
    result = runner.invoke(cli, ["get", "http://invalid-url/file.zip"])

    # Check it failed with error message
    assert result.exit_code == 1
    assert "Error: Invalid S3 URL" in result.output
