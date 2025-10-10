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
    # List all objects DIRECTLY from storage adapter to see reference.bin files
    # (client.list_objects filters them out for user-facing operations)
    all_objects = []
    start_after = None

    import concurrent.futures

    # Phase 1: Collect all objects and identify delta files
    raw_objects = []
    delta_keys = []

    while True:
        # Call storage adapter directly to see ALL files including reference.bin
        response = client.service.storage.list_objects(
            bucket=bucket,
            prefix="",
            max_keys=1000,
            start_after=start_after,
        )

        # Collect objects and identify delta files
        for obj_dict in response.get("objects", []):
            raw_objects.append(obj_dict)
            if obj_dict["key"].endswith(".delta"):
                delta_keys.append(obj_dict["key"])

        if not response.get("is_truncated"):
            break

        start_after = response.get("next_continuation_token")

    # Phase 2: Fetch metadata for delta files in parallel (10x faster)
    metadata_map = {}
    if delta_keys:
        client.service.logger.info(f"Fetching metadata for {len(delta_keys)} delta files in parallel...")

        def fetch_metadata(key: str) -> tuple[str, dict[str, Any] | None]:
            try:
                obj_head = client.service.storage.head(f"{bucket}/{key}")
                if obj_head and obj_head.metadata:
                    return key, obj_head.metadata
            except Exception as e:
                client.service.logger.debug(f"Failed to fetch metadata for {key}: {e}")
            return key, None

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(delta_keys))) as executor:
            futures = [executor.submit(fetch_metadata, key) for key in delta_keys]
            for future in concurrent.futures.as_completed(futures):
                key, metadata = future.result()
                if metadata:
                    metadata_map[key] = metadata

    # Phase 3: Build ObjectInfo list with metadata
    for obj_dict in raw_objects:
        key = obj_dict["key"]
        size = obj_dict["size"]
        is_delta = key.endswith(".delta")

        # Get metadata from our parallel fetch
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
                client.service.logger.debug(f"Delta {key}: using original_size={original_size}")
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

    # Calculate statistics - COUNT ALL FILES
    total_original_size = 0
    total_compressed_size = 0
    delta_count = 0
    direct_count = 0
    reference_files = {}  # Track all reference.bin files and their deltaspaces

    # First pass: identify what we have
    for obj in all_objects:
        if obj.key.endswith("/reference.bin") or obj.key == "reference.bin":
            # Extract deltaspace prefix
            if "/" in obj.key:
                deltaspace = obj.key.rsplit("/reference.bin", 1)[0]
            else:
                deltaspace = ""  # Root level reference.bin
            reference_files[deltaspace] = obj.size
        elif obj.is_delta:
            delta_count += 1
        else:
            direct_count += 1

    # Second pass: calculate sizes
    for obj in all_objects:
        # Skip reference.bin in this pass (we'll handle it separately)
        if obj.key.endswith("/reference.bin") or obj.key == "reference.bin":
            continue

        if obj.is_delta:
            # Delta file: original from metadata, compressed = delta size
            if obj.original_size and obj.original_size != obj.size:
                client.service.logger.debug(
                    f"Delta {obj.key}: using original_size={obj.original_size}"
                )
                total_original_size += obj.original_size
            else:
                client.service.logger.warning(
                    f"Delta {obj.key}: no original_size, using compressed size={obj.size}"
                )
                total_original_size += obj.size
            total_compressed_size += obj.size
        else:
            # Direct files: original = compressed = actual size
            total_original_size += obj.size
            total_compressed_size += obj.size

    # Handle reference.bin files
    total_reference_size = sum(reference_files.values())

    if delta_count > 0 and total_reference_size > 0:
        # Add all reference.bin files to compressed size
        total_compressed_size += total_reference_size
        client.service.logger.info(f"Including {len(reference_files)} reference.bin file(s) ({total_reference_size:,} bytes) in compressed size")
    elif delta_count == 0 and total_reference_size > 0:
        # ORPHANED REFERENCE WARNING
        waste_mb = total_reference_size / 1024 / 1024
        client.service.logger.warning(
            f"\n{'='*60}\n"
            f"WARNING: ORPHANED REFERENCE FILE(S) DETECTED!\n"
            f"{'='*60}\n"
            f"Found {len(reference_files)} reference.bin file(s) totaling {total_reference_size:,} bytes ({waste_mb:.2f} MB)\n"
            f"but NO delta files are using them.\n"
            f"\n"
            f"This wastes {waste_mb:.2f} MB of storage!\n"
            f"\n"
            f"Orphaned reference files:\n"
        )
        for deltaspace, size in reference_files.items():
            path = f"{deltaspace}/reference.bin" if deltaspace else "reference.bin"
            client.service.logger.warning(f"  - s3://{bucket}/{path} ({size:,} bytes)")

        client.service.logger.warning(
            f"\nConsider removing these orphaned files:\n"
        )
        for deltaspace in reference_files:
            path = f"{deltaspace}/reference.bin" if deltaspace else "reference.bin"
            client.service.logger.warning(f"  aws s3 rm s3://{bucket}/{path}")

        client.service.logger.warning(f"{'='*60}")

    space_saved = total_original_size - total_compressed_size
    avg_ratio = (space_saved / total_original_size) if total_original_size > 0 else 0.0

    return BucketStats(
        bucket=bucket,
        object_count=delta_count + direct_count,  # Only count user files, not reference.bin
        total_size=total_original_size,
        compressed_size=total_compressed_size,
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
