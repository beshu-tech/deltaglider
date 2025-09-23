"""E2E tests with LocalStack."""

import json
import os
import tempfile
from pathlib import Path

import boto3
import pytest
from click.testing import CliRunner

from deltaglider.app.cli.main import cli


def extract_json_from_cli_output(output: str) -> dict:
    """Extract JSON from CLI output that may contain log messages."""
    lines = output.split("\n")
    json_start = next(i for i, line in enumerate(lines) if line.strip().startswith("{"))
    json_end = next(i for i in range(json_start, len(lines)) if lines[i].strip() == "}") + 1
    json_text = "\n".join(lines[json_start:json_end])
    return json.loads(json_text)


@pytest.mark.e2e
@pytest.mark.usefixtures("skip_if_no_xdelta")
class TestLocalStackE2E:
    """E2E tests using LocalStack."""

    @pytest.fixture
    def s3_client(self):
        """Create S3 client for LocalStack."""
        return boto3.client(
            "s3",
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"),
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

    @pytest.fixture
    def test_bucket(self, s3_client):
        """Create test bucket."""
        bucket_name = "test-deltaglider-bucket"
        try:
            s3_client.create_bucket(Bucket=bucket_name)
        except s3_client.exceptions.BucketAlreadyExists:
            pass
        yield bucket_name
        # Cleanup
        try:
            # Delete all objects
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            if "Contents" in response:
                for obj in response["Contents"]:
                    s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
            s3_client.delete_bucket(Bucket=bucket_name)
        except Exception:
            pass

    def test_full_workflow(self, test_bucket, s3_client):
        """Test complete put/get/verify workflow."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create test files
            file1 = tmpdir / "plugin-v1.0.0.zip"
            file1.write_text("Plugin version 1.0.0 content")

            file2 = tmpdir / "plugin-v1.0.1.zip"
            file2.write_text("Plugin version 1.0.1 content with minor changes")

            # Upload first file (becomes reference)
            result = runner.invoke(cli, ["put", str(file1), f"s3://{test_bucket}/plugins/"])
            assert result.exit_code == 0
            output1 = extract_json_from_cli_output(result.output)
            assert output1["operation"] == "create_reference"
            assert output1["key"] == "plugins/reference.bin"

            # Verify reference was created
            objects = s3_client.list_objects_v2(Bucket=test_bucket, Prefix="plugins/")
            keys = [obj["Key"] for obj in objects["Contents"]]
            assert "plugins/reference.bin" in keys
            assert "plugins/plugin-v1.0.0.zip.delta" in keys

            # Upload second file (creates delta)
            result = runner.invoke(cli, ["put", str(file2), f"s3://{test_bucket}/plugins/"])
            assert result.exit_code == 0
            output2 = extract_json_from_cli_output(result.output)
            assert output2["operation"] == "create_delta"
            assert output2["key"] == "plugins/plugin-v1.0.1.zip.delta"
            assert "delta_ratio" in output2

            # Download and verify second file
            output_file = tmpdir / "downloaded.zip"
            result = runner.invoke(
                cli,
                [
                    "get",
                    f"s3://{test_bucket}/plugins/plugin-v1.0.1.zip.delta",
                    "-o",
                    str(output_file),
                ],
            )
            assert result.exit_code == 0
            assert output_file.read_text() == file2.read_text()

            # Verify integrity
            result = runner.invoke(
                cli,
                ["verify", f"s3://{test_bucket}/plugins/plugin-v1.0.1.zip.delta"],
            )
            assert result.exit_code == 0
            verify_output = extract_json_from_cli_output(result.output)
            assert verify_output["valid"] is True

    def test_multiple_leaves(self, test_bucket, s3_client):
        """Test multiple leaf directories with separate references."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create test files for different leaves
            file_a1 = tmpdir / "app-a-v1.zip"
            file_a1.write_text("Application A version 1")

            file_b1 = tmpdir / "app-b-v1.zip"
            file_b1.write_text("Application B version 1")

            # Upload to different leaves
            result = runner.invoke(cli, ["put", str(file_a1), f"s3://{test_bucket}/apps/app-a/"])
            assert result.exit_code == 0

            result = runner.invoke(cli, ["put", str(file_b1), f"s3://{test_bucket}/apps/app-b/"])
            assert result.exit_code == 0

            # Verify each leaf has its own reference
            objects_a = s3_client.list_objects_v2(Bucket=test_bucket, Prefix="apps/app-a/")
            keys_a = [obj["Key"] for obj in objects_a["Contents"]]
            assert "apps/app-a/reference.bin" in keys_a

            objects_b = s3_client.list_objects_v2(Bucket=test_bucket, Prefix="apps/app-b/")
            keys_b = [obj["Key"] for obj in objects_b["Contents"]]
            assert "apps/app-b/reference.bin" in keys_b

    def test_large_delta_warning(self, test_bucket, s3_client):
        """Test delta compression with different content."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create very different files
            file1 = tmpdir / "file1.zip"
            file1.write_text("A" * 1000)

            file2 = tmpdir / "file2.zip"
            file2.write_text("B" * 1000)  # Completely different

            # Upload first file
            result = runner.invoke(cli, ["put", str(file1), f"s3://{test_bucket}/test/"])
            assert result.exit_code == 0

            # Upload second file with low max-ratio
            result = runner.invoke(
                cli,
                [
                    "put",
                    str(file2),
                    f"s3://{test_bucket}/test/",
                    "--max-ratio",
                    "0.01",
                ],  # Very low threshold
            )
            assert result.exit_code == 0
            # Even with completely different content, xdelta3 is efficient
            output = extract_json_from_cli_output(result.output)
            assert output["operation"] == "create_delta"
            # Delta ratio should be small even for different files (xdelta3 is very efficient)
            assert "delta_ratio" in output
            assert output["delta_ratio"] > 0.01  # Should exceed the very low threshold we set
