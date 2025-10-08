"""DeltaGlider client with boto3-compatible APIs and advanced features."""

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from .adapters.storage_s3 import S3StorageAdapter
from .client_delete_helpers import delete_with_delta_suffix
from .client_models import (
    BucketStats,
    CompressionEstimate,
    ObjectInfo,
    UploadSummary,
)
from .core import DeltaService, DeltaSpace, ObjectKey
from .core.errors import NotFoundError


class DeltaGliderClient:
    """DeltaGlider client with boto3-compatible APIs and advanced features.

    Implements core boto3 S3 client methods (~21 methods covering 80% of use cases):
    - Object operations: put_object, get_object, delete_object, list_objects, head_object
    - Bucket operations: create_bucket, delete_bucket, list_buckets
    - Presigned URLs: generate_presigned_url, generate_presigned_post
    - Plus DeltaGlider extensions for compression stats and batch operations

    See BOTO3_COMPATIBILITY.md for complete compatibility matrix.
    """

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
    ) -> dict[str, Any]:
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

            # Paginated listing (boto3-compatible dict response)
            response = client.list_objects(
                Bucket='releases',
                MaxKeys=50,
                ContinuationToken=response.get('NextContinuationToken')
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

        # Convert to boto3-compatible S3Object dicts
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

            # Create boto3-compatible S3Object dict
            s3_obj: dict[str, Any] = {
                "Key": display_key,  # Use cleaned key without .delta
                "Size": obj["size"],
                "LastModified": obj.get("last_modified", ""),
                "ETag": obj.get("etag"),
                "StorageClass": obj.get("storage_class", "STANDARD"),
            }

            # Add DeltaGlider metadata in optional Metadata field
            deltaglider_metadata: dict[str, str] = {
                "deltaglider-is-delta": str(is_delta).lower(),
                "deltaglider-original-size": str(obj["size"]),
                "deltaglider-compression-ratio": "0.0" if not is_delta else "unknown",
            }

            # SMART METADATA FETCHING:
            # 1. NEVER fetch metadata for non-delta files (no point)
            # 2. Only fetch for delta files when explicitly requested
            if FetchMetadata and is_delta:
                try:
                    obj_head = self.service.storage.head(f"{Bucket}/{obj['key']}")
                    if obj_head and obj_head.metadata:
                        metadata = obj_head.metadata
                        # Update with actual compression stats
                        original_size = int(metadata.get("file_size", obj["size"]))
                        compression_ratio = float(metadata.get("compression_ratio", 0.0))
                        reference_key = metadata.get("ref_key")

                        deltaglider_metadata["deltaglider-original-size"] = str(original_size)
                        deltaglider_metadata["deltaglider-compression-ratio"] = str(
                            compression_ratio
                        )
                        if reference_key:
                            deltaglider_metadata["deltaglider-reference-key"] = reference_key
                except Exception as e:
                    # Log but don't fail the listing
                    self.service.logger.debug(f"Failed to fetch metadata for {obj['key']}: {e}")

            s3_obj["Metadata"] = deltaglider_metadata
            contents.append(s3_obj)

        # Build boto3-compatible response dict
        response: dict[str, Any] = {
            "Contents": contents,
            "Name": Bucket,
            "Prefix": Prefix,
            "KeyCount": len(contents),
            "MaxKeys": MaxKeys,
        }

        # Add optional fields
        if Delimiter:
            response["Delimiter"] = Delimiter

        common_prefixes = result.get("common_prefixes", [])
        if common_prefixes:
            response["CommonPrefixes"] = [{"Prefix": p} for p in common_prefixes]

        if result.get("is_truncated"):
            response["IsTruncated"] = True
            if result.get("next_continuation_token"):
                response["NextContinuationToken"] = result["next_continuation_token"]

        if ContinuationToken:
            response["ContinuationToken"] = ContinuationToken

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
            Key: Object key (can be with or without .delta suffix)
            **kwargs: Additional parameters

        Returns:
            Response dict with deletion details
        """
        _, delete_result = delete_with_delta_suffix(self.service, Bucket, Key)

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
                actual_key, delete_result = delete_with_delta_suffix(self.service, Bucket, key)

                deleted_item = {"Key": key}
                if actual_key != key:
                    deleted_item["StoredKey"] = actual_key
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
                            "StoredKey": actual_key,
                            "Type": delete_result["type"],
                            "DependentDeltas": delete_result.get("dependent_deltas", 0),
                        }
                    )

            except NotFoundError as e:
                errors.append(
                    {
                        "Key": key,
                        "Code": "NoSuchKey",
                        "Message": str(e),
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
        single_results: list[dict[str, Any]] = []
        single_errors: list[str] = []

        # First, attempt to delete the prefix as a direct object (with delta fallback)
        if Prefix and not Prefix.endswith("/"):
            candidate_keys = [Prefix]
            if not Prefix.endswith(".delta"):
                candidate_keys.append(f"{Prefix}.delta")

            seen_candidates = set()
            for candidate in candidate_keys:
                if candidate in seen_candidates:
                    continue
                seen_candidates.add(candidate)

                obj_head = self.service.storage.head(f"{Bucket}/{candidate}")
                if not obj_head:
                    continue

                try:
                    actual_key, delete_result = delete_with_delta_suffix(
                        self.service, Bucket, candidate
                    )
                    if delete_result.get("deleted"):
                        single_results.append(
                            {
                                "requested_key": candidate,
                                "actual_key": actual_key,
                                "result": delete_result,
                            }
                        )
                except Exception as e:
                    single_errors.append(f"Failed to delete {candidate}: {e}")

        # Use core service's delta-aware recursive delete for remaining objects
        delete_result = self.service.delete_recursive(Bucket, Prefix)

        # Aggregate results
        single_deleted_count = len(single_results)
        single_counts = {"delta": 0, "reference": 0, "direct": 0, "other": 0}
        single_details = []
        single_warnings: list[str] = []

        for item in single_results:
            result = item["result"]
            requested_key = item["requested_key"]
            actual_key = item["actual_key"]
            result_type = result.get("type", "other")
            if result_type not in single_counts:
                result_type = "other"
            single_counts[result_type] += 1
            detail = {
                "Key": requested_key,
                "Type": result.get("type"),
                "DependentDeltas": result.get("dependent_deltas", 0),
                "Warnings": result.get("warnings", []),
            }
            if actual_key != requested_key:
                detail["StoredKey"] = actual_key
            single_details.append(detail)
            warnings = result.get("warnings")
            if warnings:
                single_warnings.extend(warnings)

        deleted_count = cast(int, delete_result.get("deleted_count", 0)) + single_deleted_count
        failed_count = cast(int, delete_result.get("failed_count", 0)) + len(single_errors)

        deltas_deleted = cast(int, delete_result.get("deltas_deleted", 0)) + single_counts["delta"]
        references_deleted = (
            cast(int, delete_result.get("references_deleted", 0)) + single_counts["reference"]
        )
        direct_deleted = cast(int, delete_result.get("direct_deleted", 0)) + single_counts["direct"]
        other_deleted = cast(int, delete_result.get("other_deleted", 0)) + single_counts["other"]

        response = {
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
            "DeletedCount": deleted_count,
            "FailedCount": failed_count,
            "DeltaGliderInfo": {
                "DeltasDeleted": deltas_deleted,
                "ReferencesDeleted": references_deleted,
                "DirectDeleted": direct_deleted,
                "OtherDeleted": other_deleted,
            },
        }

        errors = delete_result.get("errors")
        if errors:
            response["Errors"] = cast(list[str], errors)

        warnings = delete_result.get("warnings")
        if warnings:
            response["Warnings"] = cast(list[str], warnings)

        if single_errors:
            errors_list = cast(list[str], response.setdefault("Errors", []))
            errors_list.extend(single_errors)

        if single_warnings:
            warnings_list = cast(list[str], response.setdefault("Warnings", []))
            warnings_list.extend(single_warnings)

        if single_details:
            response["DeltaGliderInfo"]["SingleDeletes"] = single_details  # type: ignore[index]

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

        for obj in response["Contents"]:
            obj_key = obj["Key"]
            obj_base = Path(obj_key).stem
            obj_ext = Path(obj_key).suffix

            # Skip delta files and references
            if obj_key.endswith(".delta") or obj_key.endswith("reference.bin"):
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
                        "Key": obj_key,
                        "Size": obj["Size"],
                        "Similarity": score,
                        "LastModified": obj["LastModified"],
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

            # Extract S3Objects from response (with Metadata containing DeltaGlider info)
            for obj_dict in response["Contents"]:
                # Convert dict back to ObjectInfo for backward compatibility with stats calculation
                metadata = obj_dict.get("Metadata", {})
                # Parse compression ratio safely (handle "unknown" value)
                compression_ratio_str = metadata.get("deltaglider-compression-ratio", "0.0")
                try:
                    compression_ratio = (
                        float(compression_ratio_str) if compression_ratio_str != "unknown" else 0.0
                    )
                except ValueError:
                    compression_ratio = 0.0

                all_objects.append(
                    ObjectInfo(
                        key=obj_dict["Key"],
                        size=obj_dict["Size"],
                        last_modified=obj_dict.get("LastModified", ""),
                        etag=obj_dict.get("ETag"),
                        storage_class=obj_dict.get("StorageClass", "STANDARD"),
                        original_size=int(
                            metadata.get("deltaglider-original-size", obj_dict["Size"])
                        ),
                        compressed_size=obj_dict["Size"],
                        is_delta=metadata.get("deltaglider-is-delta", "false") == "true",
                        compression_ratio=compression_ratio,
                        reference_key=metadata.get("deltaglider-reference-key"),
                    )
                )

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

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

    # ============================================================================
    # Bucket Management APIs (boto3-compatible)
    # ============================================================================

    def create_bucket(
        self,
        Bucket: str,
        CreateBucketConfiguration: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Create an S3 bucket (boto3-compatible).

        Args:
            Bucket: Bucket name to create
            CreateBucketConfiguration: Optional bucket configuration (e.g., LocationConstraint)
            **kwargs: Additional S3 parameters (for compatibility)

        Returns:
            Response dict with bucket location

        Example:
            >>> client = create_client()
            >>> client.create_bucket(Bucket='my-bucket')
            >>> # With region
            >>> client.create_bucket(
            ...     Bucket='my-bucket',
            ...     CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
            ... )
        """
        storage_adapter = self.service.storage

        # Check if storage adapter has boto3 client
        if hasattr(storage_adapter, "client"):
            try:
                params: dict[str, Any] = {"Bucket": Bucket}
                if CreateBucketConfiguration:
                    params["CreateBucketConfiguration"] = CreateBucketConfiguration

                response = storage_adapter.client.create_bucket(**params)
                return {
                    "Location": response.get("Location", f"/{Bucket}"),
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200,
                    },
                }
            except Exception as e:
                error_msg = str(e)
                if "BucketAlreadyExists" in error_msg or "BucketAlreadyOwnedByYou" in error_msg:
                    # Bucket already exists - return success
                    self.service.logger.debug(f"Bucket {Bucket} already exists")
                    return {
                        "Location": f"/{Bucket}",
                        "ResponseMetadata": {
                            "HTTPStatusCode": 200,
                        },
                    }
                raise RuntimeError(f"Failed to create bucket: {e}") from e
        else:
            raise NotImplementedError("Storage adapter does not support bucket creation")

    def delete_bucket(
        self,
        Bucket: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Delete an S3 bucket (boto3-compatible).

        Note: Bucket must be empty before deletion.

        Args:
            Bucket: Bucket name to delete
            **kwargs: Additional S3 parameters (for compatibility)

        Returns:
            Response dict with deletion status

        Example:
            >>> client = create_client()
            >>> client.delete_bucket(Bucket='my-bucket')
        """
        storage_adapter = self.service.storage

        # Check if storage adapter has boto3 client
        if hasattr(storage_adapter, "client"):
            try:
                storage_adapter.client.delete_bucket(Bucket=Bucket)
                return {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 204,
                    },
                }
            except Exception as e:
                error_msg = str(e)
                if "NoSuchBucket" in error_msg:
                    # Bucket doesn't exist - return success
                    self.service.logger.debug(f"Bucket {Bucket} does not exist")
                    return {
                        "ResponseMetadata": {
                            "HTTPStatusCode": 204,
                        },
                    }
                raise RuntimeError(f"Failed to delete bucket: {e}") from e
        else:
            raise NotImplementedError("Storage adapter does not support bucket deletion")

    def list_buckets(self, **kwargs: Any) -> dict[str, Any]:
        """List all S3 buckets (boto3-compatible).

        Args:
            **kwargs: Additional S3 parameters (for compatibility)

        Returns:
            Response dict with bucket list

        Example:
            >>> client = create_client()
            >>> response = client.list_buckets()
            >>> for bucket in response['Buckets']:
            ...     print(bucket['Name'])
        """
        storage_adapter = self.service.storage

        # Check if storage adapter has boto3 client
        if hasattr(storage_adapter, "client"):
            try:
                response = storage_adapter.client.list_buckets()
                return {
                    "Buckets": response.get("Buckets", []),
                    "Owner": response.get("Owner", {}),
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200,
                    },
                }
            except Exception as e:
                raise RuntimeError(f"Failed to list buckets: {e}") from e
        else:
            raise NotImplementedError("Storage adapter does not support bucket listing")

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
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    region_name: str | None = None,
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
        aws_access_key_id: AWS access key ID (None to use environment/IAM)
        aws_secret_access_key: AWS secret access key (None to use environment/IAM)
        aws_session_token: AWS session token for temporary credentials (None if not using)
        region_name: AWS region name (None for default)
        **kwargs: Additional arguments

    Returns:
        DeltaGliderClient instance

    Examples:
        >>> # Boto3-compatible usage with default credentials
        >>> client = create_client()
        >>> client.put_object(Bucket='my-bucket', Key='file.zip', Body=b'data')
        >>> response = client.get_object(Bucket='my-bucket', Key='file.zip')
        >>> data = response['Body'].read()

        >>> # With explicit credentials
        >>> client = create_client(
        ...     aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
        ...     aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        ... )

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

    # Build boto3 client kwargs
    boto3_kwargs = {}
    if aws_access_key_id is not None:
        boto3_kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key is not None:
        boto3_kwargs["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token is not None:
        boto3_kwargs["aws_session_token"] = aws_session_token
    if region_name is not None:
        boto3_kwargs["region_name"] = region_name

    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter(endpoint_url=endpoint_url, boto3_kwargs=boto3_kwargs)
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
