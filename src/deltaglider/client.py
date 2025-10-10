"""DeltaGlider client with boto3-compatible APIs and advanced features."""

# ruff: noqa: I001
import atexit
import shutil
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

# fmt: off - Keep all client_operations imports together
from .client_operations import (
    create_bucket as _create_bucket,
    delete_bucket as _delete_bucket,
    download_batch as _download_batch,
    estimate_compression as _estimate_compression,
    find_similar_files as _find_similar_files,
    generate_presigned_post as _generate_presigned_post,
    generate_presigned_url as _generate_presigned_url,
    get_bucket_stats as _get_bucket_stats,
    get_object_info as _get_object_info,
    list_buckets as _list_buckets,
    upload_batch as _upload_batch,
    upload_chunked as _upload_chunked,
)
# fmt: on

from .core import DeltaService, DeltaSpace, ObjectKey
from .core.errors import NotFoundError
from .response_builders import (
    build_delete_response,
    build_get_response,
    build_list_objects_response,
    build_put_response,
)
from .types import CommonPrefix, S3Object


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

            # Build DeltaGlider compression info
            deltaglider_info: dict[str, Any] = {
                "OriginalSizeMB": summary.file_size / (1024 * 1024),
                "StoredSizeMB": (summary.delta_size or summary.file_size) / (1024 * 1024),
                "IsDelta": summary.delta_size is not None,
                "CompressionRatio": summary.delta_ratio or 1.0,
                "SavingsPercent": (
                    (
                        (summary.file_size - (summary.delta_size or summary.file_size))
                        / summary.file_size
                        * 100
                    )
                    if summary.file_size > 0
                    else 0.0
                ),
                "StoredAs": summary.key,
                "Operation": summary.operation,
            }

            # Return as dict[str, Any] for public API (TypedDict is a dict at runtime!)
            return cast(
                dict[str, Any],
                build_put_response(
                    etag=f'"{sha256_hash}"',
                    deltaglider_info=deltaglider_info,
                ),
            )
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
        file_size = tmp_path.stat().st_size
        etag = f'"{self.service.hasher.sha256(tmp_path)}"'

        # Return as dict[str, Any] for public API (TypedDict is a dict at runtime!)
        return cast(
            dict[str, Any],
            build_get_response(
                body=body,  # type: ignore[arg-type]  # File object is compatible with bytes
                content_length=file_size,
                etag=etag,
                metadata=obj_head.metadata if obj_head else {},
            ),
        )

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

        # Convert to boto3-compatible S3Object TypedDicts (type-safe!)
        contents: list[S3Object] = []
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

            # Build DeltaGlider metadata
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

            # Create boto3-compatible S3Object TypedDict - mypy validates structure!
            s3_obj: S3Object = {
                "Key": display_key,  # Use cleaned key without .delta
                "Size": obj["size"],
                "LastModified": obj.get("last_modified", ""),
                "ETag": obj.get("etag"),
                "StorageClass": obj.get("storage_class", "STANDARD"),
                "Metadata": deltaglider_metadata,
            }
            contents.append(s3_obj)

        # Build type-safe boto3-compatible CommonPrefix TypedDicts
        common_prefixes = result.get("common_prefixes", [])
        common_prefix_dicts: list[CommonPrefix] | None = (
            [CommonPrefix(Prefix=p) for p in common_prefixes] if common_prefixes else None
        )

        # Return as dict[str, Any] for public API (TypedDict is a dict at runtime!)
        return cast(
            dict[str, Any],
            build_list_objects_response(
                bucket=Bucket,
                prefix=Prefix,
                delimiter=Delimiter,
                max_keys=MaxKeys,
                contents=contents,
                common_prefixes=common_prefix_dicts,
                is_truncated=result.get("is_truncated", False),
                next_continuation_token=result.get("next_continuation_token"),
                continuation_token=ContinuationToken,
            ),
        )

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

        # Build DeltaGlider-specific info
        deltaglider_info: dict[str, Any] = {
            "Type": delete_result.get("type"),
            "Deleted": delete_result.get("deleted", False),
        }

        # Add warnings if any
        warnings = delete_result.get("warnings")
        if warnings:
            deltaglider_info["Warnings"] = warnings

        # Add dependent delta count for references
        dependent_deltas = delete_result.get("dependent_deltas")
        if dependent_deltas:
            deltaglider_info["DependentDeltas"] = dependent_deltas

        # Return as dict[str, Any] for public API (TypedDict is a dict at runtime!)
        return cast(
            dict[str, Any],
            build_delete_response(
                delete_marker=False,
                status_code=204,
                deltaglider_info=deltaglider_info,
            ),
        )

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
        result: UploadSummary = _upload_chunked(
            self, file_path, s3_url, chunk_size, progress_callback, max_ratio
        )
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
        return _upload_batch(self, files, s3_prefix, max_ratio, progress_callback)

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
        return _download_batch(self, s3_urls, output_dir, progress_callback)

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
        result: CompressionEstimate = _estimate_compression(
            self, file_path, bucket, prefix, sample_size
        )
        return result

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
        return _find_similar_files(self, bucket, prefix, filename, limit)

    def get_object_info(self, s3_url: str) -> ObjectInfo:
        """Get detailed object information including compression stats.

        Args:
            s3_url: S3 URL of the object

        Returns:
            ObjectInfo with detailed metadata
        """
        result: ObjectInfo = _get_object_info(self, s3_url)
        return result

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
        result: BucketStats = _get_bucket_stats(self, bucket, detailed_stats)
        return result

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
        return _generate_presigned_url(self, ClientMethod, Params, ExpiresIn)

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
        return _generate_presigned_post(self, Bucket, Key, Fields, Conditions, ExpiresIn)

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
        return _create_bucket(self, Bucket, CreateBucketConfiguration, **kwargs)

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
        return _delete_bucket(self, Bucket, **kwargs)

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
        return _list_buckets(self, **kwargs)

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
    - Secure ephemeral cache (process-isolated, auto-cleanup)

    Args:
        endpoint_url: Optional S3 endpoint URL (for MinIO, R2, etc.)
        log_level: Logging level
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

    # SECURITY: Always use ephemeral process-isolated cache
    cache_dir = Path(tempfile.mkdtemp(prefix="deltaglider-", dir="/tmp"))
    # Register cleanup handler to remove cache on exit
    atexit.register(lambda: shutil.rmtree(cache_dir, ignore_errors=True))

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
    cache = FsCacheAdapter(cache_dir, hasher)
    clock = UtcClockAdapter()
    logger = StdLoggerAdapter(level=log_level)
    metrics = NoopMetricsAdapter()

    # Get default values
    tool_version = kwargs.pop("tool_version", "deltaglider/5.0.0")
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
