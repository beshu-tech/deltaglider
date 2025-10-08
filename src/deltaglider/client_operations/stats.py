"""Statistics and analysis operations for DeltaGlider client.

This module contains DeltaGlider-specific statistics operations:
- get_bucket_stats
- get_object_info
- estimate_compression
- find_similar_files
"""

import re
from pathlib import Path
from typing import Any

from ..client_models import BucketStats, CompressionEstimate, ObjectInfo


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

    Args:
        client: DeltaGliderClient instance
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
        response = client.list_objects(
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
                    original_size=int(metadata.get("deltaglider-original-size", obj_dict["Size"])),
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
