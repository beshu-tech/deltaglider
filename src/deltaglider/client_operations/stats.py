"""Statistics and analysis operations for DeltaGlider client.

This module contains DeltaGlider-specific statistics operations:
- get_bucket_stats
- get_object_info
- estimate_compression
- find_similar_files
"""

import concurrent.futures
import re
from pathlib import Path
from typing import Any

from ..client_models import BucketStats, CompressionEstimate, ObjectInfo

# ============================================================================
# Internal Helper Functions
# ============================================================================


def _collect_objects_with_pagination(
    client: Any,
    bucket: str,
    max_iterations: int = 10000,
) -> list[dict[str, Any]]:
    """Collect all objects from bucket with pagination safety.

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket name
        max_iterations: Max pagination iterations (default: 10000 = 10M objects)

    Returns:
        List of object dicts with 'key' and 'size' fields

    Raises:
        RuntimeError: If listing fails with no objects collected
    """
    raw_objects = []
    start_after = None
    iteration_count = 0

    try:
        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                client.service.logger.warning(
                    f"_collect_objects: Reached max iterations ({max_iterations}). "
                    f"Returning partial results: {len(raw_objects)} objects."
                )
                break

            try:
                response = client.service.storage.list_objects(
                    bucket=bucket,
                    prefix="",
                    max_keys=1000,
                    start_after=start_after,
                )
            except Exception as e:
                if len(raw_objects) == 0:
                    raise RuntimeError(f"Failed to list objects in bucket '{bucket}': {e}") from e
                client.service.logger.warning(
                    f"_collect_objects: Pagination error after {len(raw_objects)} objects: {e}. "
                    f"Returning partial results."
                )
                break

            # Collect objects
            for obj_dict in response.get("objects", []):
                raw_objects.append(obj_dict)

            # Check pagination status
            if not response.get("is_truncated"):
                break

            start_after = response.get("next_continuation_token")

            # Safety: missing token with truncated=True indicates broken pagination
            if not start_after:
                client.service.logger.warning(
                    f"_collect_objects: Pagination bug (truncated=True, no token). "
                    f"Processed {len(raw_objects)} objects."
                )
                break

    except Exception as e:
        if len(raw_objects) == 0:
            raise RuntimeError(f"Failed to collect bucket statistics for '{bucket}': {e}") from e
        client.service.logger.error(
            f"_collect_objects: Unexpected error after {len(raw_objects)} objects: {e}. "
            f"Returning partial results."
        )

    return raw_objects


def _fetch_delta_metadata(
    client: Any,
    bucket: str,
    delta_keys: list[str],
    max_timeout: int = 600,
) -> dict[str, dict[str, Any]]:
    """Fetch metadata for delta files in parallel with timeout.

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket name
        delta_keys: List of delta file keys
        max_timeout: Maximum total timeout in seconds (default: 600 = 10 min)

    Returns:
        Dict mapping delta key -> metadata dict
    """
    metadata_map = {}

    if not delta_keys:
        return metadata_map

    client.service.logger.info(f"Fetching metadata for {len(delta_keys)} delta files in parallel...")

    def fetch_single_metadata(key: str) -> tuple[str, dict[str, Any] | None]:
        try:
            obj_head = client.service.storage.head(f"{bucket}/{key}")
            if obj_head and obj_head.metadata:
                return key, obj_head.metadata
        except Exception as e:
            client.service.logger.debug(f"Failed to fetch metadata for {key}: {e}")
        return key, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(delta_keys))) as executor:
        futures = [executor.submit(fetch_single_metadata, key) for key in delta_keys]

        # Calculate timeout: 60s per file, capped at max_timeout
        timeout_per_file = 60
        total_timeout = min(len(delta_keys) * timeout_per_file, max_timeout)

        try:
            for future in concurrent.futures.as_completed(futures, timeout=total_timeout):
                try:
                    key, metadata = future.result(timeout=5)  # 5s per result
                    if metadata:
                        metadata_map[key] = metadata
                except concurrent.futures.TimeoutError:
                    client.service.logger.warning("Timeout fetching metadata for a delta file")
                    continue
        except concurrent.futures.TimeoutError:
            client.service.logger.warning(
                f"_fetch_delta_metadata: Timeout after {total_timeout}s. "
                f"Fetched {len(metadata_map)}/{len(delta_keys)} metadata entries. "
                f"Continuing with partial metadata..."
            )
            # Cancel remaining futures
            for future in futures:
                future.cancel()

    return metadata_map


def _build_object_info_list(
    raw_objects: list[dict[str, Any]],
    metadata_map: dict[str, dict[str, Any]],
    logger: Any,
) -> list[ObjectInfo]:
    """Build ObjectInfo list from raw objects and metadata.

    Args:
        raw_objects: List of raw object dicts from S3 LIST
        metadata_map: Dict of key -> metadata for delta files
        logger: Logger instance

    Returns:
        List of ObjectInfo objects
    """
    all_objects = []

    for obj_dict in raw_objects:
        key = obj_dict["key"]
        size = obj_dict["size"]
        is_delta = key.endswith(".delta")

        # Get metadata from map (empty dict if not present)
        metadata = metadata_map.get(key, {})

        # Parse compression ratio and original size
        compression_ratio = 0.0
        original_size = size

        if is_delta and metadata:
            try:
                ratio_str = metadata.get("compression_ratio", "0.0")
                compression_ratio = float(ratio_str) if ratio_str != "unknown" else 0.0
            except (ValueError, TypeError):
                compression_ratio = 0.0

            try:
                original_size = int(metadata.get("file_size", size))
                logger.debug(f"Delta {key}: using original_size={original_size}")
            except (ValueError, TypeError):
                original_size = size

        all_objects.append(
            ObjectInfo(
                key=key,
                size=size,
                last_modified=obj_dict.get("last_modified", ""),
                etag=obj_dict.get("etag"),
                storage_class=obj_dict.get("storage_class", "STANDARD"),
                original_size=original_size,
                compressed_size=size,
                is_delta=is_delta,
                compression_ratio=compression_ratio,
                reference_key=metadata.get("ref_key") if metadata else None,
            )
        )

    return all_objects


def _calculate_bucket_statistics(
    all_objects: list[ObjectInfo],
    bucket: str,
    logger: Any,
) -> BucketStats:
    """Calculate statistics from ObjectInfo list.

    Args:
        all_objects: List of ObjectInfo objects
        bucket: Bucket name for stats
        logger: Logger instance

    Returns:
        BucketStats object
    """
    total_original_size = 0
    total_compressed_size = 0
    delta_count = 0
    direct_count = 0
    reference_files = {}  # deltaspace -> size

    # First pass: identify object types and reference files
    for obj in all_objects:
        if obj.key.endswith("/reference.bin") or obj.key == "reference.bin":
            deltaspace = obj.key.rsplit("/reference.bin", 1)[0] if "/" in obj.key else ""
            reference_files[deltaspace] = obj.size
        elif obj.is_delta:
            delta_count += 1
        else:
            direct_count += 1

    # Second pass: calculate sizes
    for obj in all_objects:
        # Skip reference.bin (handled separately)
        if obj.key.endswith("/reference.bin") or obj.key == "reference.bin":
            continue

        if obj.is_delta:
            # Delta: use original_size if available, otherwise compressed size
            if obj.original_size and obj.original_size != obj.size:
                logger.debug(f"Delta {obj.key}: using original_size={obj.original_size}")
                total_original_size += obj.original_size
            else:
                logger.warning(f"Delta {obj.key}: no original_size, using compressed size={obj.size}")
                total_original_size += obj.size
            total_compressed_size += obj.size
        else:
            # Direct files: original = compressed
            total_original_size += obj.size
            total_compressed_size += obj.size

    # Handle reference.bin files
    total_reference_size = sum(reference_files.values())

    if delta_count > 0 and total_reference_size > 0:
        total_compressed_size += total_reference_size
        logger.info(
            f"Including {len(reference_files)} reference.bin file(s) "
            f"({total_reference_size:,} bytes) in compressed size"
        )
    elif delta_count == 0 and total_reference_size > 0:
        _log_orphaned_references(bucket, reference_files, total_reference_size, logger)

    # Calculate final metrics
    space_saved = total_original_size - total_compressed_size
    avg_ratio = (space_saved / total_original_size) if total_original_size > 0 else 0.0

    return BucketStats(
        bucket=bucket,
        object_count=delta_count + direct_count,
        total_size=total_original_size,
        compressed_size=total_compressed_size,
        space_saved=space_saved,
        average_compression_ratio=avg_ratio,
        delta_objects=delta_count,
        direct_objects=direct_count,
    )


def _log_orphaned_references(
    bucket: str,
    reference_files: dict[str, int],
    total_reference_size: int,
    logger: Any,
) -> None:
    """Log warning about orphaned reference.bin files.

    Args:
        bucket: Bucket name
        reference_files: Dict of deltaspace -> size
        total_reference_size: Total size of all reference files
        logger: Logger instance
    """
    waste_mb = total_reference_size / 1024 / 1024
    logger.warning(
        f"\n{'=' * 60}\n"
        f"WARNING: ORPHANED REFERENCE FILE(S) DETECTED!\n"
        f"{'=' * 60}\n"
        f"Found {len(reference_files)} reference.bin file(s) totaling "
        f"{total_reference_size:,} bytes ({waste_mb:.2f} MB)\n"
        f"but NO delta files are using them.\n"
        f"\n"
        f"This wastes {waste_mb:.2f} MB of storage!\n"
        f"\n"
        f"Orphaned reference files:\n"
    )

    for deltaspace, size in reference_files.items():
        path = f"{deltaspace}/reference.bin" if deltaspace else "reference.bin"
        logger.warning(f"  - s3://{bucket}/{path} ({size:,} bytes)")

    logger.warning("\nConsider removing these orphaned files:\n")
    for deltaspace in reference_files:
        path = f"{deltaspace}/reference.bin" if deltaspace else "reference.bin"
        logger.warning(f"  aws s3 rm s3://{bucket}/{path}")

    logger.warning(f"{'=' * 60}")


def get_object_info(
    client: Any,  # DeltaGliderClient
    s3_url: str,
) -> ObjectInfo:
    """Get detailed object information including compression stats.

    Args:
        client: DeltaGliderClient instance
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
    obj_head = client.service.storage.head(f"{bucket}/{key}")
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


def get_bucket_stats(
    client: Any,  # DeltaGliderClient
    bucket: str,
    detailed_stats: bool = False,
) -> BucketStats:
    """Get statistics for a bucket with optional detailed compression metrics.

    This method provides two modes:
    - Quick stats (default): Fast overview using LIST only (~50ms)
    - Detailed stats: Accurate compression metrics with HEAD requests (slower)

    **Robustness**: This function is designed to always return valid stats:
    - Returns partial stats if timeouts or pagination issues occur
    - Returns empty stats (zeros) if bucket listing completely fails
    - Never hangs indefinitely (max 10 min timeout, 10M object limit)

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket name
        detailed_stats: If True, fetch accurate compression ratios for delta files (default: False)

    Returns:
        BucketStats with compression and space savings info. Always returns a valid BucketStats
        object, even if errors occur (will return empty/partial stats with warnings logged).

    Raises:
        RuntimeError: Only if bucket listing fails immediately with no objects collected.
                     All other errors result in partial/empty stats being returned.

    Performance:
        - With detailed_stats=False: ~50ms for any bucket size (1 LIST call per 1000 objects)
        - With detailed_stats=True: ~2-3s per 1000 objects (adds HEAD calls for delta files only)
        - Max timeout: 10 minutes (prevents indefinite hangs)
        - Max objects: 10M (prevents infinite loops)

    Example:
        # Quick stats for dashboard display
        stats = client.get_bucket_stats('releases')
        print(f"Objects: {stats.object_count}, Size: {stats.total_size}")

        # Detailed stats for analytics (slower but accurate)
        stats = client.get_bucket_stats('releases', detailed_stats=True)
        print(f"Compression ratio: {stats.average_compression_ratio:.1%}")
    """
    try:
        # Phase 1: Collect all objects with pagination safety
        raw_objects = _collect_objects_with_pagination(client, bucket)

        # Phase 2: Extract delta keys for metadata fetching
        delta_keys = [obj["key"] for obj in raw_objects if obj["key"].endswith(".delta")]

        # Phase 3: Fetch metadata for delta files (only if detailed_stats requested)
        metadata_map = {}
        if detailed_stats and delta_keys:
            metadata_map = _fetch_delta_metadata(client, bucket, delta_keys)

        # Phase 4: Build ObjectInfo list
        all_objects = _build_object_info_list(raw_objects, metadata_map, client.service.logger)

        # Phase 5: Calculate final statistics
        return _calculate_bucket_statistics(all_objects, bucket, client.service.logger)

    except Exception as e:
        # Last resort: return empty stats with error indication
        client.service.logger.error(
            f"get_bucket_stats: Failed to build statistics for '{bucket}': {e}. "
            f"Returning empty stats."
        )
        return BucketStats(
            bucket=bucket,
            object_count=0,
            total_size=0,
            compressed_size=0,
            space_saved=0,
            average_compression_ratio=0.0,
            delta_objects=0,
            direct_objects=0,
        )


# ============================================================================
# Public API Functions
# ============================================================================


def estimate_compression(
    client: Any,  # DeltaGliderClient
    file_path: str | Path,
    bucket: str,
    prefix: str = "",
    sample_size: int = 1024 * 1024,
) -> CompressionEstimate:
    """Estimate compression ratio before upload.

    Args:
        client: DeltaGliderClient instance
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
    similar_files = find_similar_files(client, bucket, prefix, file_path.name)

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
    client: Any,  # DeltaGliderClient
    bucket: str,
    prefix: str,
    filename: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Find similar files that could serve as references.

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket
        prefix: Prefix to search in
        filename: Filename to match against
        limit: Maximum number of results

    Returns:
        List of similar files with scores
    """
    # List objects in the prefix (no metadata needed for similarity check)
    response = client.list_objects(
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
