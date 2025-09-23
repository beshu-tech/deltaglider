"""Integration test for full put/get workflow."""

import io
from pathlib import Path

from deltaglider.core import DeltaSpace, ObjectKey


def test_full_put_get_workflow(service, temp_dir, mock_storage, mock_diff):
    """Test complete workflow: put a file, then get it back."""
    # Create test files - use .zip extension to trigger delta compression
    file1_content = b"This is the first version of the file."
    file2_content = b"This is the second version of the file with changes."

    file1 = temp_dir / "version1.zip"
    file2 = temp_dir / "version2.zip"
    output_file = temp_dir / "recovered.zip"

    file1.write_bytes(file1_content)
    file2.write_bytes(file2_content)

    # Set up mock_diff decode to write the target content
    def decode_side_effect(base, delta, out):
        out.write_bytes(file2_content)

    mock_diff.decode.side_effect = decode_side_effect

    delta_space = DeltaSpace(bucket="test-bucket", prefix="test/data")

    # Storage state tracking
    storage_data = {}

    def mock_head(key):
        """Mock head_object."""
        if key in storage_data:
            return storage_data[key]["head"]
        return None

    def mock_put(key, body, metadata, content_type="application/octet-stream"):
        """Mock put_object."""
        from deltaglider.ports.storage import ObjectHead, PutResult

        # Read content if it's a Path
        if isinstance(body, Path):
            content = body.read_bytes()
        elif isinstance(body, bytes):
            content = body
        else:
            content = body.read()

        storage_data[key] = {
            "content": content,
            "head": ObjectHead(
                key=key.split("/", 1)[1],
                size=len(content),
                etag="mock-etag",
                last_modified=None,
                metadata=metadata,
            ),
        }
        return PutResult(etag="mock-etag")

    def mock_get(key):
        """Mock get_object."""
        # The key might come without bucket prefix, so check both formats
        if key in storage_data:
            return io.BytesIO(storage_data[key]["content"])
        # Also try with test-bucket prefix if not found
        full_key = f"test-bucket/{key}" if not key.startswith("test-bucket/") else key
        if full_key in storage_data:
            return io.BytesIO(storage_data[full_key]["content"])
        raise FileNotFoundError(f"Object not found: {key}")

    mock_storage.head.side_effect = mock_head
    mock_storage.put.side_effect = mock_put
    mock_storage.get.side_effect = mock_get

    # Step 1: Put the first file (creates reference)
    summary1 = service.put(file1, delta_space)
    assert summary1.operation == "create_reference"
    assert summary1.key == "test/data/reference.bin"

    # Verify reference was stored
    ref_key = f"{delta_space.bucket}/{delta_space.reference_key()}"
    assert ref_key in storage_data
    assert storage_data[ref_key]["content"] == file1_content

    # Step 2: Put the second file (creates delta)
    summary2 = service.put(file2, delta_space)
    assert summary2.operation == "create_delta"
    assert summary2.key == "test/data/version2.zip.delta"
    assert summary2.delta_size is not None
    assert summary2.ref_key == "test/data/reference.bin"

    # Verify delta was stored
    delta_key = f"{delta_space.bucket}/{summary2.key}"
    assert delta_key in storage_data

    # Step 3: Get the delta file back
    obj_key = ObjectKey(bucket=delta_space.bucket, key=summary2.key)
    service.get(obj_key, output_file)

    # Step 4: Verify the recovered file matches the original
    recovered_content = output_file.read_bytes()
    assert recovered_content == file2_content


def test_get_with_auto_delta_suffix(service, temp_dir, mock_storage, mock_diff):
    """Test get command behavior when .delta suffix is auto-appended."""
    # Create test file
    file_content = b"Test file content for auto-suffix test."
    test_file = temp_dir / "mydata.zip"
    test_file.write_bytes(file_content)

    # Set up mock_diff decode to write the target content
    def decode_side_effect(base, delta, out):
        out.write_bytes(file_content)

    mock_diff.decode.side_effect = decode_side_effect

    delta_space = DeltaSpace(bucket="test-bucket", prefix="archive")

    # Storage state tracking
    storage_data = {}

    def mock_head(key):
        """Mock head_object."""
        if key in storage_data:
            return storage_data[key]["head"]
        return None

    def mock_put(key, body, metadata, content_type="application/octet-stream"):
        """Mock put_object."""
        from deltaglider.ports.storage import ObjectHead, PutResult

        # Read content if it's a Path
        if isinstance(body, Path):
            content = body.read_bytes()
        elif isinstance(body, bytes):
            content = body
        else:
            content = body.read()

        storage_data[key] = {
            "content": content,
            "head": ObjectHead(
                key=key.split("/", 1)[1],
                size=len(content),
                etag="mock-etag",
                last_modified=None,
                metadata=metadata,
            ),
        }
        return PutResult(etag="mock-etag")

    def mock_get(key):
        """Mock get_object."""
        # The key might come without bucket prefix, so check both formats
        if key in storage_data:
            return io.BytesIO(storage_data[key]["content"])
        # Also try with test-bucket prefix if not found
        full_key = f"test-bucket/{key}" if not key.startswith("test-bucket/") else key
        if full_key in storage_data:
            return io.BytesIO(storage_data[full_key]["content"])
        raise FileNotFoundError(f"Object not found: {key}")

    mock_storage.head.side_effect = mock_head
    mock_storage.put.side_effect = mock_put
    mock_storage.get.side_effect = mock_get

    # Put the file
    summary = service.put(test_file, delta_space)

    # Get it back using original name (without .delta)
    # The service should internally look for "mydata.zip.delta"
    output_file = temp_dir / "recovered.zip"

    # Use the key without .delta suffix
    if summary.operation == "create_reference":
        # If it's a reference, the zero-diff delta was created
        obj_key = ObjectKey(bucket=delta_space.bucket, key="archive/mydata.zip.delta")
    else:
        obj_key = ObjectKey(bucket=delta_space.bucket, key=summary.key)

    service.get(obj_key, output_file)

    # Verify the recovered file matches the original
    recovered_content = output_file.read_bytes()
    assert recovered_content == file_content
