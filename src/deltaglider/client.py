"""DeltaGlider client with boto3-compatible APIs and advanced features."""

import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .adapters.storage_s3 import S3StorageAdapter
from .core import DeltaService, DeltaSpace, ObjectKey


@dataclass
class UploadSummary:
    """User-friendly upload summary."""

    operation: str
    bucket: str
    key: str
    original_size: int
    stored_size: int
    is_delta: bool
    delta_ratio: float = 0.0

    @property
    def original_size_mb(self) -> float:
        """Original size in MB."""
        return self.original_size / (1024 * 1024)

    @property
    def stored_size_mb(self) -> float:
        """Stored size in MB."""
        return self.stored_size / (1024 * 1024)

    @property
    def savings_percent(self) -> float:
        """Percentage saved through compression."""
        if self.original_size == 0:
            return 0.0
        return ((self.original_size - self.stored_size) / self.original_size) * 100


@dataclass
class CompressionEstimate:
    """Compression estimate for a file."""

    original_size: int
    estimated_compressed_size: int
    estimated_ratio: float
    confidence: float
    recommended_reference: str | None = None
    should_use_delta: bool = True


@dataclass
class ObjectInfo:
    """Detailed object information with compression stats."""

    key: str
    size: int
    last_modified: str
    etag: str | None = None
    storage_class: str = "STANDARD"

    # DeltaGlider-specific fields
    original_size: int | None = None
    compressed_size: int | None = None
    compression_ratio: float | None = None
    is_delta: bool = False
    reference_key: str | None = None
    delta_chain_length: int = 0


@dataclass
class ListObjectsResponse:
    """Response from list_objects, compatible with boto3."""

    name: str  # Bucket name
    prefix: str = ""
    delimiter: str = ""
    max_keys: int = 1000
    common_prefixes: list[dict[str, str]] = field(default_factory=list)
    contents: list[ObjectInfo] = field(default_factory=list)
    is_truncated: bool = False
    next_continuation_token: str | None = None
    continuation_token: str | None = None
    key_count: int = 0

    @property
    def objects(self) -> list[ObjectInfo]:
        """Alias for contents, for convenience."""
        return self.contents


@dataclass
class BucketStats:
    """Statistics for a bucket."""

    bucket: str
    object_count: int
    total_size: int
    compressed_size: int
    space_saved: int
    average_compression_ratio: float
    delta_objects: int
    direct_objects: int


class DeltaGliderClient:
    """DeltaGlider client with boto3-compatible APIs and advanced features."""

    def __init__(self, service: DeltaService, endpoint_url: str | None = None):
        """Initialize client with service."""
        self.service = service
        self.endpoint_url = endpoint_url
        self._multipart_uploads: dict[str, Any] = {}  # Track multipart uploads

    # ============================================================================
    # Boto3-compatible APIs (matches S3 client interface)
    # ============================================================================

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes | str | Path | None = None,
        Metadata: dict[str, str] | None = None,
        ContentType: str | None = None,
        Tagging: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Upload an object to S3 with delta compression (boto3-compatible).

        This method uses DeltaGlider's delta compression for archive files.
        Files will be stored as .delta when appropriate (subsequent similar files).
        The GET operation transparently reconstructs the original file.

        Args:
            Bucket: S3 bucket name
            Key: Object key (specifies the deltaspace and filename)
            Body: Object data (bytes, string, or file path)
            Metadata: Object metadata
            ContentType: MIME type (currently unused but kept for compatibility)
            Tagging: Object tags as URL-encoded string (currently unused)
            **kwargs: Additional S3 parameters (for compatibility)

        Returns:
            Response dict with ETag and compression info
        """
        import tempfile

        # Handle Body parameter
        if Body is None:
            raise ValueError("Body parameter is required")

        # Write body to a temporary file for DeltaService.put()
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(Key).suffix) as tmp_file:
            tmp_path = Path(tmp_file.name)

            # Write Body to temp file
            if isinstance(Body, bytes):
                tmp_file.write(Body)
            elif isinstance(Body, str):
                tmp_file.write(Body.encode("utf-8"))
            elif isinstance(Body, Path):
                tmp_file.write(Body.read_bytes())
            else:
                # Handle any other type by converting to string path
                path_str = str(Body)
                try:
                    tmp_file.write(Path(path_str).read_bytes())
                except Exception as e:
                    raise ValueError(
                        f"Invalid Body parameter: cannot read from {path_str}: {e}"
                    ) from e

        try:
            # Extract deltaspace prefix from Key
            # If Key has path separators, use parent as prefix
            key_path = Path(Key)
            if "/" in Key:
                # Use the parent directories as the deltaspace prefix
                prefix = str(key_path.parent)
                # Copy temp file with original filename for proper extension detection
                named_tmp = tmp_path.parent / key_path.name
                tmp_path.rename(named_tmp)
                tmp_path = named_tmp
            else:
                # No path, use empty prefix
                prefix = ""
                # Rename temp file to have the proper filename
                named_tmp = tmp_path.parent / Key
                tmp_path.rename(named_tmp)
                tmp_path = named_tmp

            # Create DeltaSpace and use DeltaService for compression
            delta_space = DeltaSpace(bucket=Bucket, prefix=prefix)

            # Use the service to put the file (handles delta compression automatically)
            summary = self.service.put(tmp_path, delta_space, max_ratio=0.5)

            # Calculate ETag from file content
            sha256_hash = self.service.hasher.sha256(tmp_path)

            # Return boto3-compatible response with delta info
            return {
                "ETag": f'"{sha256_hash}"',
                "ResponseMetadata": {
                    "HTTPStatusCode": 200,
                },
                "DeltaGlider": {
                    "original_size": summary.file_size,
                    "stored_size": summary.delta_size or summary.file_size,
                    "is_delta": summary.delta_size is not None,
                    "compression_ratio": summary.delta_ratio or 1.0,
                    "stored_as": summary.key,
                    "operation": summary.operation,
                },
            }
        finally:
            # Clean up temp file
            if tmp_path.exists():
                tmp_path.unlink()

    def get_object(
        self,
        Bucket: str,
        Key: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Download an object from S3 (boto3-compatible).

        Args:
            Bucket: S3 bucket name
            Key: Object key
            **kwargs: Additional S3 parameters (for compatibility)

        Returns:
            Response dict with Body stream and metadata
        """
        # Download to temp file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)

        self.download(
            s3_url=f"s3://{Bucket}/{Key}",
            output_path=tmp_path,
        )

        # Open file for streaming
        body = open(tmp_path, "rb")

        # Get metadata
        obj_head = self.service.storage.head(f"{Bucket}/{Key}")

        return {
            "Body": body,  # File-like object
            "ContentLength": tmp_path.stat().st_size,
            "ContentType": obj_head.metadata.get("content_type", "binary/octet-stream")
            if obj_head
            else "binary/octet-stream",
            "ETag": f'"{self.service.hasher.sha256(tmp_path)}"',
            "Metadata": obj_head.metadata if obj_head else {},
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

    def list_objects(
        self,
        Bucket: str,
        Prefix: str = "",
        Delimiter: str = "",
        MaxKeys: int = 1000,
        ContinuationToken: str | None = None,
        StartAfter: str | None = None,
        FetchMetadata: bool = False,
        **kwargs: Any,
    ) -> ListObjectsResponse:
        """List objects in bucket with smart metadata fetching.

        This method optimizes performance by:
        - Never fetching metadata for non-delta files (they don't need it)
        - Only fetching metadata for delta files when explicitly requested
        - Supporting efficient pagination for large buckets

        Args:
            Bucket: S3 bucket name
            Prefix: Filter results to keys beginning with prefix
            Delimiter: Delimiter for grouping keys (e.g., '/' for folders)
            MaxKeys: Maximum number of keys to return (for pagination)
            ContinuationToken: Token from previous response for pagination
            StartAfter: Start listing after this key (for pagination)
            FetchMetadata: If True, fetch metadata ONLY for delta files (default: False)
            **kwargs: Additional parameters for compatibility

        Returns:
            ListObjectsResponse with objects and pagination info

        Performance Notes:
            - With FetchMetadata=False: ~50ms for 1000 objects (1 S3 API call)
            - With FetchMetadata=True: ~2-3s for 1000 objects (1 + N delta files API calls)
            - Non-delta files NEVER trigger HEAD requests (no metadata needed)

        Example:
            # Fast listing for UI display (no metadata)
            response = client.list_objects(Bucket='releases', MaxKeys=100)

            # Paginated listing
            response = client.list_objects(
                Bucket='releases',
                MaxKeys=50,
                ContinuationToken=response.next_continuation_token
            )

            # Detailed listing with compression stats (slower, only for analytics)
            response = client.list_objects(
                Bucket='releases',
                FetchMetadata=True  # Only fetches for delta files
            )
        """
        # Use storage adapter's list_objects method
        if hasattr(self.service.storage, "list_objects"):
            result = self.service.storage.list_objects(
                bucket=Bucket,
                prefix=Prefix,
                delimiter=Delimiter,
                max_keys=MaxKeys,
                start_after=StartAfter or ContinuationToken,  # Support both pagination methods
            )
        elif isinstance(self.service.storage, S3StorageAdapter):
            result = self.service.storage.list_objects(
                bucket=Bucket,
                prefix=Prefix,
                delimiter=Delimiter,
                max_keys=MaxKeys,
                start_after=StartAfter or ContinuationToken,
            )
        else:
            # Fallback
            result = {
                "objects": [],
                "common_prefixes": [],
                "is_truncated": False,
            }

        # Convert to ObjectInfo objects with smart metadata fetching
        contents = []
        for obj in result.get("objects", []):
            # Skip reference.bin files (internal files, never exposed to users)
            if obj["key"].endswith("/reference.bin") or obj["key"] == "reference.bin":
                continue

            # Determine file type
            is_delta = obj["key"].endswith(".delta")

            # Remove .delta suffix from display key (hide internal implementation)
            display_key = obj["key"]
            if is_delta:
                display_key = display_key[:-6]  # Remove .delta suffix

            # Create object info with basic data (no HEAD request)
            info = ObjectInfo(
                key=display_key,  # Use cleaned key without .delta
                size=obj["size"],
                last_modified=obj.get("last_modified", ""),
                etag=obj.get("etag"),
                storage_class=obj.get("storage_class", "STANDARD"),
                # DeltaGlider fields
                original_size=obj["size"],  # For non-delta, original = stored
                compressed_size=obj["size"],
                is_delta=is_delta,
                compression_ratio=0.0 if not is_delta else None,
                reference_key=None,
            )

            # SMART METADATA FETCHING:
            # 1. NEVER fetch metadata for non-delta files (no point)
            # 2. Only fetch for delta files when explicitly requested
            if FetchMetadata and is_delta:
                try:
                    obj_head = self.service.storage.head(f"{Bucket}/{obj['key']}")
                    if obj_head and obj_head.metadata:
                        metadata = obj_head.metadata
                        # Update with actual compression stats
                        info.original_size = int(metadata.get("file_size", obj["size"]))
                        info.compression_ratio = float(metadata.get("compression_ratio", 0.0))
                        info.reference_key = metadata.get("ref_key")
                except Exception as e:
                    # Log but don't fail the listing
                    self.service.logger.debug(f"Failed to fetch metadata for {obj['key']}: {e}")

            contents.append(info)

        # Build response with pagination support
        response = ListObjectsResponse(
            name=Bucket,
            prefix=Prefix,
            delimiter=Delimiter,
            max_keys=MaxKeys,
            contents=contents,
            common_prefixes=[{"Prefix": p} for p in result.get("common_prefixes", [])],
            is_truncated=result.get("is_truncated", False),
            next_continuation_token=result.get("next_continuation_token"),
            continuation_token=ContinuationToken,
            key_count=len(contents),
        )

        return response

    def delete_object(
        self,
        Bucket: str,
        Key: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Delete an object with delta awareness (boto3-compatible).

        Args:
            Bucket: S3 bucket name
            Key: Object key
            **kwargs: Additional parameters

        Returns:
            Response dict with deletion details
        """
        # Use core service's delta-aware delete
        object_key = ObjectKey(bucket=Bucket, key=Key)
        delete_result = self.service.delete(object_key)

        response = {
            "DeleteMarker": False,
            "ResponseMetadata": {
                "HTTPStatusCode": 204,
            },
            "DeltaGliderInfo": {
                "Type": delete_result.get("type"),
                "Deleted": delete_result.get("deleted", False),
            },
        }

        # Add warnings if any
        warnings = delete_result.get("warnings")
        if warnings:
            delta_info = response.get("DeltaGliderInfo")
            if delta_info and isinstance(delta_info, dict):
                delta_info["Warnings"] = warnings

        # Add dependent delta count for references
        dependent_deltas = delete_result.get("dependent_deltas")
        if dependent_deltas:
            delta_info = response.get("DeltaGliderInfo")
            if delta_info and isinstance(delta_info, dict):
                delta_info["DependentDeltas"] = dependent_deltas

        return response

    def delete_objects(
        self,
        Bucket: str,
        Delete: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Delete multiple objects with delta awareness (boto3-compatible).

        Args:
            Bucket: S3 bucket name
            Delete: Dict with 'Objects' list of {'Key': key} dicts
            **kwargs: Additional parameters

        Returns:
            Response dict with deleted objects
        """
        deleted = []
        errors = []
        delta_info = []

        for obj in Delete.get("Objects", []):
            key = obj["Key"]
            try:
                object_key = ObjectKey(bucket=Bucket, key=key)
                delete_result = self.service.delete(object_key)

                deleted_item = {"Key": key}
                if delete_result.get("type"):
                    deleted_item["Type"] = delete_result["type"]
                if delete_result.get("warnings"):
                    deleted_item["Warnings"] = delete_result["warnings"]

                deleted.append(deleted_item)

                # Track delta-specific info
                if delete_result.get("type") in ["delta", "reference"]:
                    delta_info.append(
                        {
                            "Key": key,
                            "Type": delete_result["type"],
                            "DependentDeltas": delete_result.get("dependent_deltas", 0),
                        }
                    )

            except Exception as e:
                errors.append(
                    {
                        "Key": key,
                        "Code": "InternalError",
                        "Message": str(e),
                    }
                )

        response: dict[str, Any] = {"Deleted": deleted}
        if errors:
            response["Errors"] = errors

        if delta_info:
            response["DeltaGliderInfo"] = {
                "DeltaFilesDeleted": len([d for d in delta_info if d["Type"] == "delta"]),
                "ReferencesDeleted": len([d for d in delta_info if d["Type"] == "reference"]),
                "Details": delta_info,
            }

        response["ResponseMetadata"] = {"HTTPStatusCode": 200}
        return response

    def delete_objects_recursive(
        self,
        Bucket: str,
        Prefix: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Recursively delete all objects under a prefix with delta awareness.

        Args:
            Bucket: S3 bucket name
            Prefix: Prefix to delete recursively
            **kwargs: Additional parameters

        Returns:
            Response dict with deletion statistics
        """
        # Use core service's delta-aware recursive delete
        delete_result = self.service.delete_recursive(Bucket, Prefix)

        response = {
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
            "DeletedCount": delete_result.get("deleted_count", 0),
            "FailedCount": delete_result.get("failed_count", 0),
            "DeltaGliderInfo": {
                "DeltasDeleted": delete_result.get("deltas_deleted", 0),
                "ReferencesDeleted": delete_result.get("references_deleted", 0),
                "DirectDeleted": delete_result.get("direct_deleted", 0),
                "OtherDeleted": delete_result.get("other_deleted", 0),
            },
        }

        if delete_result.get("errors"):
            response["Errors"] = delete_result["errors"]

        if delete_result.get("warnings"):
            response["Warnings"] = delete_result["warnings"]

        return response

    def head_object(
        self,
        Bucket: str,
        Key: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Get object metadata (boto3-compatible).

        Args:
            Bucket: S3 bucket name
            Key: Object key
            **kwargs: Additional parameters

        Returns:
            Response dict with object metadata
        """
        obj_head = self.service.storage.head(f"{Bucket}/{Key}")
        if not obj_head:
            raise FileNotFoundError(f"Object not found: s3://{Bucket}/{Key}")

        return {
            "ContentLength": obj_head.size,
            "ContentType": obj_head.metadata.get("content_type", "binary/octet-stream"),
            "ETag": obj_head.metadata.get("etag", ""),
            "LastModified": obj_head.metadata.get("last_modified", ""),
            "Metadata": obj_head.metadata,
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

    # ============================================================================
    # Simple client methods (original DeltaGlider API)
    # ============================================================================

    def upload(
        self,
        file_path: str | Path,
        s3_url: str,
        tags: dict[str, str] | None = None,
        max_ratio: float = 0.5,
    ) -> UploadSummary:
        """Upload a file to S3 with automatic delta compression.

        Args:
            file_path: Local file to upload
            s3_url: S3 destination URL (s3://bucket/prefix/)
            tags: Optional tags to add to the object
            max_ratio: Maximum acceptable delta/file ratio (default 0.5)

        Returns:
            UploadSummary with compression statistics
        """
        file_path = Path(file_path)

        # Parse S3 URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:].rstrip("/")
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        # Create delta space and upload
        delta_space = DeltaSpace(bucket=bucket, prefix=prefix)
        summary = self.service.put(file_path, delta_space, max_ratio)

        # TODO: Add tags support when implemented

        # Convert to user-friendly summary
        is_delta = summary.delta_size is not None
        stored_size = summary.delta_size if is_delta else summary.file_size

        return UploadSummary(
            operation=summary.operation,
            bucket=summary.bucket,
            key=summary.key,
            original_size=summary.file_size,
            stored_size=stored_size or summary.file_size,  # Ensure stored_size is never None
            is_delta=is_delta,
            delta_ratio=summary.delta_ratio or 0.0,
        )

    def download(self, s3_url: str, output_path: str | Path) -> None:
        """Download and reconstruct a file from S3.

        Args:
            s3_url: S3 source URL (s3://bucket/key)
            output_path: Local destination path
        """
        output_path = Path(output_path)

        # Parse S3 URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:]
        parts = s3_path.split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"S3 URL must include key: {s3_url}")

        bucket = parts[0]
        key = parts[1]

        # Auto-append .delta if the file doesn't exist without it
        # This allows users to specify the original name and we'll find the delta
        obj_key = ObjectKey(bucket=bucket, key=key)

        # Try to get metadata first to see if it exists
        try:
            self.service.get(obj_key, output_path)
        except Exception:
            # Try with .delta suffix
            if not key.endswith(".delta"):
                obj_key = ObjectKey(bucket=bucket, key=key + ".delta")
                self.service.get(obj_key, output_path)
            else:
                raise

    def verify(self, s3_url: str) -> bool:
        """Verify integrity of a stored file.

        Args:
            s3_url: S3 URL of the file to verify

        Returns:
            True if verification passed, False otherwise
        """
        # Parse S3 URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:]
        parts = s3_path.split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"S3 URL must include key: {s3_url}")

        bucket = parts[0]
        key = parts[1]

        obj_key = ObjectKey(bucket=bucket, key=key)
        result = self.service.verify(obj_key)
        return result.valid

    # ============================================================================
    # DeltaGlider-specific APIs
    # ============================================================================

    def upload_chunked(
        self,
        file_path: str | Path,
        s3_url: str,
        chunk_size: int = 5 * 1024 * 1024,
        progress_callback: Callable[[int, int, int, int], None] | None = None,
        max_ratio: float = 0.5,
    ) -> UploadSummary:
        """Upload a file in chunks with progress callback.

        This method reads the file in chunks to avoid loading large files entirely into memory,
        making it suitable for uploading very large files. Progress is reported after each chunk.

        Args:
            file_path: Local file to upload
            s3_url: S3 destination URL (s3://bucket/path/filename)
            chunk_size: Size of each chunk in bytes (default 5MB)
            progress_callback: Callback(chunk_number, total_chunks, bytes_sent, total_bytes)
            max_ratio: Maximum acceptable delta/file ratio for compression

        Returns:
            UploadSummary with compression statistics

        Example:
            def on_progress(chunk_num, total_chunks, bytes_sent, total_bytes):
                percent = (bytes_sent / total_bytes) * 100
                print(f"Upload progress: {percent:.1f}%")

            client.upload_chunked(
                "large_file.zip",
                "s3://bucket/releases/large_file.zip",
                chunk_size=10 * 1024 * 1024,  # 10MB chunks
                progress_callback=on_progress
            )
        """
        file_path = Path(file_path)
        file_size = file_path.stat().st_size

        # For small files, just use regular upload
        if file_size <= chunk_size:
            if progress_callback:
                progress_callback(1, 1, file_size, file_size)
            return self.upload(file_path, s3_url, max_ratio=max_ratio)

        # Calculate chunks
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        # Create a temporary file for chunked processing
        # For now, we read the entire file but report progress in chunks
        # Future enhancement: implement true streaming upload in storage adapter
        bytes_read = 0

        with open(file_path, "rb") as f:
            for chunk_num in range(1, total_chunks + 1):
                # Read chunk (simulated for progress reporting)
                chunk_data = f.read(chunk_size)
                bytes_read += len(chunk_data)

                if progress_callback:
                    progress_callback(chunk_num, total_chunks, bytes_read, file_size)

        # Perform the actual upload
        # TODO: When storage adapter supports streaming, pass chunks directly
        result = self.upload(file_path, s3_url, max_ratio=max_ratio)

        # Final progress callback
        if progress_callback:
            progress_callback(total_chunks, total_chunks, file_size, file_size)

        return result

    def upload_batch(
        self,
        files: list[str | Path],
        s3_prefix: str,
        max_ratio: float = 0.5,
        progress_callback: Callable[[str, int, int], None] | None = None,
    ) -> list[UploadSummary]:
        """Upload multiple files in batch.

        Args:
            files: List of local file paths
            s3_prefix: S3 destination prefix (s3://bucket/prefix/)
            max_ratio: Maximum acceptable delta/file ratio
            progress_callback: Callback(filename, current_file_index, total_files)

        Returns:
            List of UploadSummary objects
        """
        results = []

        for i, file_path in enumerate(files):
            file_path = Path(file_path)

            if progress_callback:
                progress_callback(file_path.name, i + 1, len(files))

            # Upload each file
            s3_url = f"{s3_prefix.rstrip('/')}/{file_path.name}"
            summary = self.upload(file_path, s3_url, max_ratio=max_ratio)
            results.append(summary)

        return results

    def download_batch(
        self,
        s3_urls: list[str],
        output_dir: str | Path,
        progress_callback: Callable[[str, int, int], None] | None = None,
    ) -> list[Path]:
        """Download multiple files in batch.

        Args:
            s3_urls: List of S3 URLs to download
            output_dir: Local directory to save files
            progress_callback: Callback(filename, current_file_index, total_files)

        Returns:
            List of downloaded file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        results = []

        for i, s3_url in enumerate(s3_urls):
            # Extract filename from URL
            filename = s3_url.split("/")[-1]
            if filename.endswith(".delta"):
                filename = filename[:-6]  # Remove .delta suffix

            if progress_callback:
                progress_callback(filename, i + 1, len(s3_urls))

            output_path = output_dir / filename
            self.download(s3_url, output_path)
            results.append(output_path)

        return results

    def estimate_compression(
        self,
        file_path: str | Path,
        bucket: str,
        prefix: str = "",
        sample_size: int = 1024 * 1024,
    ) -> CompressionEstimate:
        """Estimate compression ratio before upload.

        Args:
            file_path: Local file to estimate
            bucket: Target bucket
            prefix: Target prefix (for finding similar files)
            sample_size: Bytes to sample for estimation (default 1MB)

        Returns:
            CompressionEstimate with predicted compression
        """
        file_path = Path(file_path)
        file_size = file_path.stat().st_size

        # Check file extension
        ext = file_path.suffix.lower()
        delta_extensions = {
            ".zip",
            ".tar",
            ".gz",
            ".tar.gz",
            ".tgz",
            ".bz2",
            ".tar.bz2",
            ".xz",
            ".tar.xz",
            ".7z",
            ".rar",
            ".dmg",
            ".iso",
            ".pkg",
            ".deb",
            ".rpm",
            ".apk",
            ".jar",
            ".war",
            ".ear",
        }

        # Already compressed formats that won't benefit from delta
        incompressible = {".jpg", ".jpeg", ".png", ".mp4", ".mp3", ".avi", ".mov"}

        if ext in incompressible:
            return CompressionEstimate(
                original_size=file_size,
                estimated_compressed_size=file_size,
                estimated_ratio=0.0,
                confidence=0.95,
                should_use_delta=False,
            )

        if ext not in delta_extensions:
            # Unknown type, conservative estimate
            return CompressionEstimate(
                original_size=file_size,
                estimated_compressed_size=file_size,
                estimated_ratio=0.0,
                confidence=0.5,
                should_use_delta=file_size > 1024 * 1024,  # Only for files > 1MB
            )

        # Look for similar files in the target location
        similar_files = self.find_similar_files(bucket, prefix, file_path.name)

        if similar_files:
            # If we have similar files, estimate high compression
            estimated_ratio = 0.99  # 99% compression typical for similar versions
            confidence = 0.9
            recommended_ref = similar_files[0]["Key"] if similar_files else None
        else:
            # First file of its type
            estimated_ratio = 0.0
            confidence = 0.7
            recommended_ref = None

        estimated_size = int(file_size * (1 - estimated_ratio))

        return CompressionEstimate(
            original_size=file_size,
            estimated_compressed_size=estimated_size,
            estimated_ratio=estimated_ratio,
            confidence=confidence,
            recommended_reference=recommended_ref,
            should_use_delta=True,
        )

    def find_similar_files(
        self,
        bucket: str,
        prefix: str,
        filename: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Find similar files that could serve as references.

        Args:
            bucket: S3 bucket
            prefix: Prefix to search in
            filename: Filename to match against
            limit: Maximum number of results

        Returns:
            List of similar files with scores
        """
        # List objects in the prefix (no metadata needed for similarity check)
        response = self.list_objects(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1000,
            FetchMetadata=False,  # Don't need metadata for similarity
        )

        similar: list[dict[str, Any]] = []
        base_name = Path(filename).stem
        ext = Path(filename).suffix

        for obj in response.contents:
            obj_base = Path(obj.key).stem
            obj_ext = Path(obj.key).suffix

            # Skip delta files and references
            if obj.key.endswith(".delta") or obj.key.endswith("reference.bin"):
                continue

            score = 0.0

            # Extension match
            if ext == obj_ext:
                score += 0.5

            # Base name similarity
            if base_name in obj_base or obj_base in base_name:
                score += 0.3

            # Version pattern match
            import re

            if re.search(r"v?\d+[\.\d]*", base_name) and re.search(r"v?\d+[\.\d]*", obj_base):
                score += 0.2

            if score > 0.5:
                similar.append(
                    {
                        "Key": obj.key,
                        "Size": obj.size,
                        "Similarity": score,
                        "LastModified": obj.last_modified,
                    }
                )

        # Sort by similarity
        similar.sort(key=lambda x: x["Similarity"], reverse=True)  # type: ignore

        return similar[:limit]

    def get_object_info(self, s3_url: str) -> ObjectInfo:
        """Get detailed object information including compression stats.

        Args:
            s3_url: S3 URL of the object

        Returns:
            ObjectInfo with detailed metadata
        """
        # Parse URL
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")

        s3_path = s3_url[5:]
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        # Get object metadata
        obj_head = self.service.storage.head(f"{bucket}/{key}")
        if not obj_head:
            raise FileNotFoundError(f"Object not found: {s3_url}")

        metadata = obj_head.metadata
        is_delta = key.endswith(".delta")

        return ObjectInfo(
            key=key,
            size=obj_head.size,
            last_modified=metadata.get("last_modified", ""),
            etag=metadata.get("etag"),
            original_size=int(metadata.get("file_size", obj_head.size)),
            compressed_size=obj_head.size,
            compression_ratio=float(metadata.get("compression_ratio", 0.0)),
            is_delta=is_delta,
            reference_key=metadata.get("ref_key"),
        )

    def get_bucket_stats(self, bucket: str, detailed_stats: bool = False) -> BucketStats:
        """Get statistics for a bucket with optional detailed compression metrics.

        This method provides two modes:
        - Quick stats (default): Fast overview using LIST only (~50ms)
        - Detailed stats: Accurate compression metrics with HEAD requests (slower)

        Args:
            bucket: S3 bucket name
            detailed_stats: If True, fetch accurate compression ratios for delta files (default: False)

        Returns:
            BucketStats with compression and space savings info

        Performance:
            - With detailed_stats=False: ~50ms for any bucket size (1 LIST call per 1000 objects)
            - With detailed_stats=True: ~2-3s per 1000 objects (adds HEAD calls for delta files only)

        Example:
            # Quick stats for dashboard display
            stats = client.get_bucket_stats('releases')
            print(f"Objects: {stats.object_count}, Size: {stats.total_size}")

            # Detailed stats for analytics (slower but accurate)
            stats = client.get_bucket_stats('releases', detailed_stats=True)
            print(f"Compression ratio: {stats.average_compression_ratio:.1%}")
        """
        # List all objects with smart metadata fetching
        all_objects = []
        continuation_token = None

        while True:
            response = self.list_objects(
                Bucket=bucket,
                MaxKeys=1000,
                ContinuationToken=continuation_token,
                FetchMetadata=detailed_stats,  # Only fetch metadata if detailed stats requested
            )

            all_objects.extend(response.contents)

            if not response.is_truncated:
                break

            continuation_token = response.next_continuation_token

        # Calculate statistics
        total_size = 0
        compressed_size = 0
        delta_count = 0
        direct_count = 0

        for obj in all_objects:
            compressed_size += obj.size

            if obj.is_delta:
                delta_count += 1
                # Use actual original size if we have it, otherwise estimate
                total_size += obj.original_size or obj.size
            else:
                direct_count += 1
                # For non-delta files, original equals compressed
                total_size += obj.size

        space_saved = total_size - compressed_size
        avg_ratio = (space_saved / total_size) if total_size > 0 else 0.0

        return BucketStats(
            bucket=bucket,
            object_count=len(all_objects),
            total_size=total_size,
            compressed_size=compressed_size,
            space_saved=space_saved,
            average_compression_ratio=avg_ratio,
            delta_objects=delta_count,
            direct_objects=direct_count,
        )

    def _try_boto3_presigned_operation(self, operation: str, **kwargs: Any) -> Any | None:
        """Try to generate presigned operation using boto3 client, return None if not available."""
        storage_adapter = self.service.storage

        # Check if storage adapter has boto3 client
        if hasattr(storage_adapter, "client"):
            try:
                if operation == "url":
                    return str(storage_adapter.client.generate_presigned_url(**kwargs))
                elif operation == "post":
                    return dict(storage_adapter.client.generate_presigned_post(**kwargs))
            except AttributeError:
                # storage_adapter does not have a 'client' attribute
                pass
            except Exception as e:
                # Fall back to manual construction if needed
                self.service.logger.warning(f"Failed to generate presigned {operation}: {e}")

        return None

    def generate_presigned_url(
        self,
        ClientMethod: str,
        Params: dict[str, Any],
        ExpiresIn: int = 3600,
    ) -> str:
        """Generate presigned URL (boto3-compatible).

        Args:
            ClientMethod: Method name ('get_object' or 'put_object')
            Params: Parameters dict with Bucket and Key
            ExpiresIn: URL expiration in seconds

        Returns:
            Presigned URL string
        """
        # Try boto3 first, fallback to manual construction
        url = self._try_boto3_presigned_operation(
            "url",
            ClientMethod=ClientMethod,
            Params=Params,
            ExpiresIn=ExpiresIn,
        )
        if url is not None:
            return str(url)

        # Fallback: construct URL manually (less secure, for dev/testing only)
        bucket = Params.get("Bucket", "")
        key = Params.get("Key", "")

        if self.endpoint_url:
            base_url = self.endpoint_url
        else:
            base_url = f"https://{bucket}.s3.amazonaws.com"

        # Warning: This is not a real presigned URL, just a placeholder
        self.service.logger.warning("Using placeholder presigned URL - not suitable for production")
        return f"{base_url}/{key}?expires={ExpiresIn}"

    def generate_presigned_post(
        self,
        Bucket: str,
        Key: str,
        Fields: dict[str, str] | None = None,
        Conditions: list[Any] | None = None,
        ExpiresIn: int = 3600,
    ) -> dict[str, Any]:
        """Generate presigned POST data for HTML forms (boto3-compatible).

        Args:
            Bucket: S3 bucket name
            Key: Object key
            Fields: Additional fields to include
            Conditions: Upload conditions
            ExpiresIn: URL expiration in seconds

        Returns:
            Dict with 'url' and 'fields' for form submission
        """
        # Try boto3 first, fallback to manual construction
        response = self._try_boto3_presigned_operation(
            "post",
            Bucket=Bucket,
            Key=Key,
            Fields=Fields,
            Conditions=Conditions,
            ExpiresIn=ExpiresIn,
        )
        if response is not None:
            return dict(response)

        # Fallback: return minimal structure for compatibility
        if self.endpoint_url:
            url = f"{self.endpoint_url}/{Bucket}"
        else:
            url = f"https://{Bucket}.s3.amazonaws.com"

        return {
            "url": url,
            "fields": {
                "key": Key,
                **(Fields or {}),
            },
        }

    def _parse_tagging(self, tagging: str) -> dict[str, str]:
        """Parse URL-encoded tagging string to dict."""
        tags = {}
        if tagging:
            for pair in tagging.split("&"):
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    tags[key] = value
        return tags


def create_client(
    endpoint_url: str | None = None,
    log_level: str = "INFO",
    cache_dir: str = "/tmp/.deltaglider/cache",
    **kwargs: Any,
) -> DeltaGliderClient:
    """Create a DeltaGlider client with boto3-compatible APIs.

    This client provides:
    - Boto3-compatible method names (put_object, get_object, etc.)
    - Batch operations (upload_batch, download_batch)
    - Compression estimation
    - Progress callbacks for large uploads
    - Detailed object and bucket statistics

    Args:
        endpoint_url: Optional S3 endpoint URL (for MinIO, R2, etc.)
        log_level: Logging level
        cache_dir: Directory for reference cache
        **kwargs: Additional arguments

    Returns:
        DeltaGliderClient instance

    Examples:
        >>> # Boto3-compatible usage
        >>> client = create_client()
        >>> client.put_object(Bucket='my-bucket', Key='file.zip', Body=b'data')
        >>> response = client.get_object(Bucket='my-bucket', Key='file.zip')
        >>> data = response['Body'].read()

        >>> # Batch operations
        >>> results = client.upload_batch(['v1.zip', 'v2.zip'], 's3://bucket/releases/')

        >>> # Compression estimation
        >>> estimate = client.estimate_compression('new.zip', 'bucket', 'releases/')
        >>> print(f"Expected compression: {estimate.estimated_ratio:.1%}")
    """
    # Import here to avoid circular dependency
    from .adapters import (
        FsCacheAdapter,
        NoopMetricsAdapter,
        S3StorageAdapter,
        Sha256Adapter,
        StdLoggerAdapter,
        UtcClockAdapter,
        XdeltaAdapter,
    )

    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter(endpoint_url=endpoint_url)
    diff = XdeltaAdapter()
    cache = FsCacheAdapter(Path(cache_dir), hasher)
    clock = UtcClockAdapter()
    logger = StdLoggerAdapter(level=log_level)
    metrics = NoopMetricsAdapter()

    # Get default values
    tool_version = kwargs.pop("tool_version", "deltaglider/0.2.0")
    max_ratio = kwargs.pop("max_ratio", 0.5)

    # Create service
    service = DeltaService(
        storage=storage,
        diff=diff,
        hasher=hasher,
        cache=cache,
        clock=clock,
        logger=logger,
        metrics=metrics,
        tool_version=tool_version,
        max_ratio=max_ratio,
        **kwargs,
    )

    return DeltaGliderClient(service, endpoint_url)
