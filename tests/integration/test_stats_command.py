"""Integration tests for stats CLI command."""

import json
from unittest.mock import Mock, patch

from click.testing import CliRunner

from deltaglider.app.cli.main import cli
from deltaglider.client_models import BucketStats


class TestStatsCommand:
    """Test stats CLI command."""

    def test_stats_json_output(self):
        """Test stats command with JSON output."""
        # Create mock bucket stats
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=10,
            total_size=1000000,
            compressed_size=500000,
            space_saved=500000,
            average_compression_ratio=0.5,
            delta_objects=7,
            direct_objects=3,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            # Setup mock client
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            # Run command
            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "test-bucket", "--json"])

            # Verify
            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["bucket"] == "test-bucket"
            assert output["object_count"] == 10
            assert output["total_size"] == 1000000
            assert output["compressed_size"] == 500000
            assert output["space_saved"] == 500000
            assert output["average_compression_ratio"] == 0.5
            assert output["delta_objects"] == 7
            assert output["direct_objects"] == 3

            # Verify client was called correctly
            mock_client.get_bucket_stats.assert_called_once_with(
                "test-bucket", mode="quick", use_cache=True, refresh_cache=False
            )

    def test_stats_json_output_detailed(self):
        """Test stats command with detailed JSON output."""
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=5,
            total_size=2000000,
            compressed_size=100000,
            space_saved=1900000,
            average_compression_ratio=0.95,
            delta_objects=5,
            direct_objects=0,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "test-bucket", "--detailed", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["average_compression_ratio"] == 0.95

            # Verify detailed flag was passed
            mock_client.get_bucket_stats.assert_called_once_with(
                "test-bucket", mode="detailed", use_cache=True, refresh_cache=False
            )

    def test_stats_json_output_sampled(self):
        """Test stats command with sampled JSON output."""
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=5,
            total_size=2000000,
            compressed_size=100000,
            space_saved=1900000,
            average_compression_ratio=0.95,
            delta_objects=5,
            direct_objects=0,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "test-bucket", "--sampled", "--json"])

            assert result.exit_code == 0
            mock_client.get_bucket_stats.assert_called_once_with(
                "test-bucket", mode="sampled", use_cache=True, refresh_cache=False
            )

    def test_stats_sampled_and_detailed_conflict(self):
        """--sampled and --detailed flags must be mutually exclusive."""

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "test-bucket", "--sampled", "--detailed"])

            assert result.exit_code == 1
            assert "cannot be used together" in result.output

    def test_stats_human_readable_output(self):
        """Test stats command with human-readable output."""
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=10,
            total_size=1500000,  # ~1.43 MB
            compressed_size=300000,  # ~293 KB
            space_saved=1200000,  # ~1.14 MB
            average_compression_ratio=0.8,
            delta_objects=7,
            direct_objects=3,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "test-bucket"])

            assert result.exit_code == 0
            output = result.output

            # Verify human-readable format
            assert "Bucket Statistics: test-bucket" in output
            assert "Total Objects:" in output
            assert "10" in output
            assert "Delta Objects:" in output
            assert "7" in output
            assert "Direct Objects:" in output
            assert "3" in output
            assert "Original Size:" in output
            assert "Compressed Size:" in output
            assert "Space Saved:" in output
            assert "Compression Ratio:" in output
            assert "80.0%" in output  # 0.8 = 80%

    def test_stats_error_handling(self):
        """Test stats command error handling."""
        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.side_effect = Exception("Bucket not found")
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "nonexistent-bucket"])

            assert result.exit_code == 1
            assert "Error: Bucket not found" in result.output

    def test_stats_with_s3_url(self):
        """Test stats command with s3:// URL format."""
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=5,
            total_size=1000000,
            compressed_size=500000,
            space_saved=500000,
            average_compression_ratio=0.5,
            delta_objects=3,
            direct_objects=2,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "s3://test-bucket", "--json"])

            assert result.exit_code == 0
            # Verify bucket name was parsed correctly from S3 URL
            mock_client.get_bucket_stats.assert_called_once_with(
                "test-bucket", mode="quick", use_cache=True, refresh_cache=False
            )

    def test_stats_with_s3_url_trailing_slash(self):
        """Test stats command with s3:// URL format with trailing slash."""
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=5,
            total_size=1000000,
            compressed_size=500000,
            space_saved=500000,
            average_compression_ratio=0.5,
            delta_objects=3,
            direct_objects=2,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "s3://test-bucket/", "--json"])

            assert result.exit_code == 0
            # Verify bucket name was parsed correctly from S3 URL with trailing slash
            mock_client.get_bucket_stats.assert_called_once_with(
                "test-bucket", mode="quick", use_cache=True, refresh_cache=False
            )

    def test_stats_with_s3_url_with_prefix(self):
        """Test stats command with s3:// URL format with prefix (should ignore prefix)."""
        mock_stats = BucketStats(
            bucket="test-bucket",
            object_count=5,
            total_size=1000000,
            compressed_size=500000,
            space_saved=500000,
            average_compression_ratio=0.5,
            delta_objects=3,
            direct_objects=2,
        )

        with patch("deltaglider.client.DeltaGliderClient") as mock_client_class:
            mock_client = Mock()
            mock_client.get_bucket_stats.return_value = mock_stats
            mock_client_class.return_value = mock_client

            runner = CliRunner()
            result = runner.invoke(cli, ["stats", "s3://test-bucket/some/prefix/", "--json"])

            assert result.exit_code == 0
            # Verify only bucket name was extracted, prefix ignored
            mock_client.get_bucket_stats.assert_called_once_with(
                "test-bucket", mode="quick", use_cache=True, refresh_cache=False
            )
