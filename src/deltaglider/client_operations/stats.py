"""Statistics and analysis operations for DeltaGlider client.

This module contains DeltaGlider-specific statistics operations:
- get_bucket_stats
- get_object_info
- estimate_compression
- find_similar_files
"""

import concurrent.futures
import json
import re
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from ..client_models import BucketStats, CompressionEstimate, ObjectInfo
from ..core.delta_extensions import is_delta_candidate
from ..core.object_listing import list_all_objects
from ..core.s3_uri import parse_s3_url

StatsMode = Literal["quick", "sampled", "detailed"]

# Cache configuration
CACHE_VERSION = "1.0"
CACHE_PREFIX = ".deltaglider"

# ============================================================================
# Internal Helper Functions
# ============================================================================


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
    metadata_map: dict[str, dict[str, Any]] = {}

    if not delta_keys:
        return metadata_map

    client.service.logger.info(
        f"Fetching metadata for {len(delta_keys)} delta files in parallel..."
    )

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


def _extract_deltaspace(key: str) -> str:
    """Return the delta space (prefix) for a given object key."""
    if "/" in key:
        return key.rsplit("/", 1)[0]
    return ""


def _get_cache_key(mode: StatsMode) -> str:
    """Get the S3 key for a cache file based on mode.

    Args:
        mode: Stats mode (quick, sampled, or detailed)

    Returns:
        S3 key like ".deltaglider/stats_quick.json"
    """
    return f"{CACHE_PREFIX}/stats_{mode}.json"


def _read_stats_cache(
    client: Any,
    bucket: str,
    mode: StatsMode,
) -> tuple[BucketStats | None, dict[str, Any] | None]:
    """Read cached stats from S3 if available.

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket name
        mode: Stats mode to read cache for

    Returns:
        Tuple of (BucketStats | None, validation_data | None)
        Returns (None, None) if cache doesn't exist or is invalid
    """
    cache_key = _get_cache_key(mode)

    try:
        # Try to read cache file from S3
        obj = client.service.storage.get(f"{bucket}/{cache_key}")
        if not obj or not obj.data:
            return None, None

        # Parse JSON
        cache_data = json.loads(obj.data.decode("utf-8"))

        # Validate version
        if cache_data.get("version") != CACHE_VERSION:
            client.service.logger.warning(
                f"Cache version mismatch: expected {CACHE_VERSION}, got {cache_data.get('version')}"
            )
            return None, None

        # Validate mode
        if cache_data.get("mode") != mode:
            client.service.logger.warning(
                f"Cache mode mismatch: expected {mode}, got {cache_data.get('mode')}"
            )
            return None, None

        # Extract stats and validation data
        stats_dict = cache_data.get("stats")
        validation_data = cache_data.get("validation")

        if not stats_dict or not validation_data:
            client.service.logger.warning("Cache missing stats or validation data")
            return None, None

        # Reconstruct BucketStats from dict
        stats = BucketStats(**stats_dict)

        client.service.logger.debug(
            f"Successfully read cache for {bucket} (mode={mode}, "
            f"computed_at={cache_data.get('computed_at')})"
        )

        return stats, validation_data

    except FileNotFoundError:
        # Cache doesn't exist yet - this is normal
        client.service.logger.debug(f"No cache found for {bucket} (mode={mode})")
        return None, None
    except json.JSONDecodeError as e:
        client.service.logger.warning(f"Invalid JSON in cache file: {e}")
        return None, None
    except Exception as e:
        client.service.logger.warning(f"Error reading cache: {e}")
        return None, None


def _write_stats_cache(
    client: Any,
    bucket: str,
    mode: StatsMode,
    stats: BucketStats,
    object_count: int,
    compressed_size: int,
) -> None:
    """Write computed stats to S3 cache.

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket name
        mode: Stats mode being cached
        stats: Computed BucketStats to cache
        object_count: Current object count (for validation)
        compressed_size: Current compressed size (for validation)
    """
    cache_key = _get_cache_key(mode)

    try:
        # Build cache structure
        cache_data = {
            "version": CACHE_VERSION,
            "mode": mode,
            "computed_at": datetime.now(UTC).isoformat(),
            "validation": {
                "object_count": object_count,
                "compressed_size": compressed_size,
            },
            "stats": asdict(stats),
        }

        # Serialize to JSON
        cache_json = json.dumps(cache_data, indent=2)

        # Write to S3
        client.service.storage.put(
            address=f"{bucket}/{cache_key}",
            data=cache_json.encode("utf-8"),
            metadata={
                "content-type": "application/json",
                "x-deltaglider-cache": "true",
            },
        )

        client.service.logger.info(
            f"Wrote cache for {bucket} (mode={mode}, {len(cache_json)} bytes)"
        )

    except Exception as e:
        # Log warning but don't fail - caching is optional
        client.service.logger.warning(f"Failed to write cache (non-fatal): {e}")


def _is_cache_valid(
    cached_validation: dict[str, Any],
    current_object_count: int,
    current_compressed_size: int,
) -> bool:
    """Check if cached stats are still valid based on bucket state.

    Validation strategy: Compare object count and total compressed size.
    If either changed, the cache is stale.

    Args:
        cached_validation: Validation data from cache
        current_object_count: Current object count from LIST
        current_compressed_size: Current compressed size from LIST

    Returns:
        True if cache is still valid, False if stale
    """
    cached_count = cached_validation.get("object_count")
    cached_size = cached_validation.get("compressed_size")

    if cached_count != current_object_count:
        return False

    if cached_size != current_compressed_size:
        return False

    return True


def _build_object_info_list(
    raw_objects: list[dict[str, Any]],
    metadata_map: dict[str, dict[str, Any]],
    logger: Any,
    sampled_space_metadata: dict[str, dict[str, Any]] | None = None,
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

        deltaspace = _extract_deltaspace(key)

        # Get metadata from map (empty dict if not present)
        metadata = metadata_map.get(key)
        if metadata is None and sampled_space_metadata and deltaspace in sampled_space_metadata:
            metadata = sampled_space_metadata[deltaspace]
        if metadata is None:
            metadata = {}

        # Parse compression ratio and original size
        compression_ratio = 0.0
        # For delta files without metadata, set original_size to None to indicate unknown
        # This prevents nonsensical stats like "693 bytes compressed to 82MB"
        original_size = None if is_delta else size

        if is_delta and metadata:
            try:
                ratio_str = metadata.get("compression_ratio", "0.0")
                compression_ratio = float(ratio_str) if ratio_str != "unknown" else 0.0
            except (ValueError, TypeError):
                compression_ratio = 0.0

            try:
                if "dg-file-size" in metadata:
                    original_size = int(metadata["dg-file-size"])
                    logger.debug(
                        f"Delta {key}: using original_size={original_size} from metadata['dg-file-size']"
                    )
                else:
                    logger.warning(
                        f"Delta {key}: metadata missing 'dg-file-size' key. "
                        f"Available keys: {list(metadata.keys())}. "
                        f"Using None as original_size (unknown)"
                    )
                    original_size = None
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Delta {key}: failed to parse dg-file-size from metadata: {e}. "
                    f"Using None as original_size (unknown)"
                )
                original_size = None

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
    mode: StatsMode = "quick",
) -> BucketStats:
    """Calculate statistics from ObjectInfo list.

    Args:
        all_objects: List of ObjectInfo objects
        bucket: Bucket name for stats
        logger: Logger instance
        mode: Stats mode (quick, sampled, or detailed) - controls warning behavior

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
            # Delta: use original_size if available
            if obj.original_size is not None:
                logger.debug(f"Delta {obj.key}: using original_size={obj.original_size}")
                total_original_size += obj.original_size
            else:
                # original_size is None - metadata not available
                # In quick mode, this is expected (no HEAD requests)
                # In sampled/detailed mode, this means metadata is genuinely missing
                if mode != "quick":
                    logger.warning(
                        f"Delta {obj.key}: no original_size metadata available. "
                        f"Cannot calculate original size without metadata. "
                        f"Use --detailed mode for accurate stats."
                    )
                # Don't add anything to total_original_size for deltas without metadata
                # This prevents nonsensical stats
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
    # If we couldn't calculate original size (quick mode with deltas), set space_saved to 0
    # to avoid nonsensical negative numbers
    if total_original_size == 0 and total_compressed_size > 0:
        space_saved = 0
        avg_ratio = 0.0
    else:
        space_saved = total_original_size - total_compressed_size
        avg_ratio = (space_saved / total_original_size) if total_original_size > 0 else 0.0

    # Warn if quick mode with delta files (stats will be incomplete)
    if mode == "quick" and delta_count > 0 and total_original_size == 0:
        logger.warning(
            f"Quick mode cannot calculate original size for delta files (no metadata fetched). "
            f"Stats show {delta_count} delta file(s) with unknown original size. "
            f"Use --detailed for accurate compression metrics."
        )

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
    address = parse_s3_url(s3_url, allow_empty_key=False)
    bucket = address.bucket
    key = address.key

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
    mode: StatsMode = "quick",
    use_cache: bool = True,
    refresh_cache: bool = False,
) -> BucketStats:
    """Get statistics for a bucket with configurable metadata strategies and caching.

    Modes:
    - ``quick`` (default): Stream LIST results only. Compression metrics for delta files are
      approximate (falls back to delta size when metadata is unavailable).
    - ``sampled``: Fetch HEAD metadata for a single delta per delta-space and reuse the ratios for
      other deltas in the same space. Balances accuracy and speed.
    - ``detailed``: Fetch HEAD metadata for every delta object for the most accurate statistics.

    Caching:
    - Stats are cached per mode in ``.deltaglider/stats_{mode}.json``
    - Cache is validated using object count and compressed size from LIST
    - If bucket changed, cache is recomputed automatically
    - Use ``refresh_cache=True`` to force recomputation
    - Use ``use_cache=False`` to skip caching entirely

    **Robustness**: This function is designed to always return valid stats:
    - Returns partial stats if timeouts or pagination issues occur
    - Returns empty stats (zeros) if bucket listing completely fails
    - Never hangs indefinitely (max 10 min timeout, 10M object limit)

    Args:
        client: DeltaGliderClient instance
        bucket: S3 bucket name
        mode: Stats mode ("quick", "sampled", or "detailed")
        use_cache: If True, use cached stats when available (default: True)
        refresh_cache: If True, force cache recomputation even if valid (default: False)

    Returns:
        BucketStats with compression and space savings info. Always returns a valid BucketStats
        object, even if errors occur (will return empty/partial stats with warnings logged).

    Raises:
        RuntimeError: Only if bucket listing fails immediately with no objects collected.
                      All other errors result in partial/empty stats being returned.

    Performance:
        - With cache hit: ~50-100ms (LIST + cache read + validation)
        - quick (no cache): ~50ms for any bucket size (LIST calls only)
        - sampled (no cache): LIST + one HEAD per delta-space
        - detailed (no cache): LIST + HEAD for every delta (slowest but accurate)
        - Max timeout: 10 minutes (prevents indefinite hangs)
        - Max objects: 10M (prevents infinite loops)

    Example:
        # Use cached stats (fast, ~100ms)
        stats = client.get_bucket_stats('releases')

        # Force refresh (slow, recomputes everything)
        stats = client.get_bucket_stats('releases', refresh_cache=True)

        # Skip cache entirely
        stats = client.get_bucket_stats('releases', use_cache=False)

        # Different modes with caching
        stats_sampled = client.get_bucket_stats('releases', mode='sampled')
        stats_detailed = client.get_bucket_stats('releases', mode='detailed')
    """
    try:
        if mode not in {"quick", "sampled", "detailed"}:
            raise ValueError(f"Unknown stats mode: {mode}")

        # Phase 1: Always do a quick LIST to get current state (needed for validation)
        import time

        phase1_start = time.time()
        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 1: Starting LIST operation for bucket '{bucket}'"
        )

        listing = list_all_objects(
            client.service.storage,
            bucket=bucket,
            max_keys=1000,
            logger=client.service.logger,
        )
        raw_objects = listing.objects

        # Calculate validation metrics from LIST
        current_object_count = len(raw_objects)
        current_compressed_size = sum(obj["size"] for obj in raw_objects)

        phase1_duration = time.time() - phase1_start
        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 1: LIST completed in {phase1_duration:.2f}s - "
            f"Found {current_object_count} objects, {current_compressed_size:,} bytes total"
        )

        # Phase 2: Try to use cache if enabled and not forcing refresh
        phase2_start = time.time()
        if use_cache and not refresh_cache:
            client.service.logger.info(
                f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 2: Checking cache for mode '{mode}'"
            )
            cached_stats, cached_validation = _read_stats_cache(client, bucket, mode)

            if cached_stats and cached_validation:
                # Validate cache against current bucket state
                if _is_cache_valid(
                    cached_validation, current_object_count, current_compressed_size
                ):
                    phase2_duration = time.time() - phase2_start
                    client.service.logger.info(
                        f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 2: Cache HIT in {phase2_duration:.2f}s - "
                        f"Using cached stats for {bucket} (mode={mode}, bucket unchanged)"
                    )
                    return cached_stats
                else:
                    phase2_duration = time.time() - phase2_start
                    client.service.logger.info(
                        f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 2: Cache INVALID in {phase2_duration:.2f}s - "
                        f"Bucket changed: count {cached_validation.get('object_count')} → {current_object_count}, "
                        f"size {cached_validation.get('compressed_size')} → {current_compressed_size}"
                    )
            else:
                phase2_duration = time.time() - phase2_start
                client.service.logger.info(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 2: Cache MISS in {phase2_duration:.2f}s - "
                    f"No valid cache found"
                )
        else:
            if refresh_cache:
                client.service.logger.info(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 2: Cache SKIPPED (refresh requested)"
                )
            elif not use_cache:
                client.service.logger.info(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 2: Cache DISABLED"
                )

        # Phase 3: Cache miss or invalid - compute stats from scratch
        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 3: Computing stats (mode={mode})"
        )

        # Phase 4: Extract delta keys for metadata fetching
        phase4_start = time.time()
        delta_keys = [obj["key"] for obj in raw_objects if obj["key"].endswith(".delta")]
        phase4_duration = time.time() - phase4_start

        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 4: Delta extraction completed in {phase4_duration:.3f}s - "
            f"Found {len(delta_keys)} delta files"
        )

        # Phase 5: Fetch metadata for delta files based on mode
        phase5_start = time.time()
        metadata_map: dict[str, dict[str, Any]] = {}
        sampled_space_metadata: dict[str, dict[str, Any]] | None = None

        if delta_keys:
            if mode == "detailed":
                client.service.logger.info(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 5: Fetching metadata for ALL {len(delta_keys)} delta files"
                )
                metadata_map = _fetch_delta_metadata(client, bucket, delta_keys)

            elif mode == "sampled":
                # Sample one delta per deltaspace
                seen_spaces: set[str] = set()
                sampled_keys: list[str] = []
                for key in delta_keys:
                    space = _extract_deltaspace(key)
                    if space not in seen_spaces:
                        seen_spaces.add(space)
                        sampled_keys.append(key)

                client.service.logger.info(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 5: Sampling {len(sampled_keys)} delta files "
                    f"(one per deltaspace) out of {len(delta_keys)} total delta files"
                )

                # Log which files are being sampled
                if sampled_keys:
                    for idx, key in enumerate(sampled_keys[:10], 1):  # Show first 10
                        space = _extract_deltaspace(key)
                        client.service.logger.info(
                            f"  [{idx}] Sampling: {key} (deltaspace: '{space or '(root)'}')"
                        )
                    if len(sampled_keys) > 10:
                        client.service.logger.info(f"  ... and {len(sampled_keys) - 10} more")

                if sampled_keys:
                    metadata_map = _fetch_delta_metadata(client, bucket, sampled_keys)
                    sampled_space_metadata = {
                        _extract_deltaspace(k): metadata for k, metadata in metadata_map.items()
                    }

        phase5_duration = time.time() - phase5_start
        if mode == "quick":
            client.service.logger.info(
                f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 5: Skipped metadata fetching (quick mode) in {phase5_duration:.3f}s"
            )
        else:
            client.service.logger.info(
                f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 5: Metadata fetching completed in {phase5_duration:.2f}s - "
                f"Fetched {len(metadata_map)} metadata records"
            )

        # Phase 6: Build ObjectInfo list
        phase6_start = time.time()
        all_objects = _build_object_info_list(
            raw_objects,
            metadata_map,
            client.service.logger,
            sampled_space_metadata,
        )
        phase6_duration = time.time() - phase6_start
        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 6: ObjectInfo list built in {phase6_duration:.3f}s - "
            f"{len(all_objects)} objects processed"
        )

        # Phase 7: Calculate final statistics
        phase7_start = time.time()
        stats = _calculate_bucket_statistics(all_objects, bucket, client.service.logger, mode)
        phase7_duration = time.time() - phase7_start
        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 7: Statistics calculated in {phase7_duration:.3f}s - "
            f"{stats.delta_objects} delta, {stats.direct_objects} direct objects"
        )

        # Phase 8: Write cache if enabled
        phase8_start = time.time()
        if use_cache:
            _write_stats_cache(
                client=client,
                bucket=bucket,
                mode=mode,
                stats=stats,
                object_count=current_object_count,
                compressed_size=current_compressed_size,
            )
            phase8_duration = time.time() - phase8_start
            client.service.logger.info(
                f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 8: Cache written in {phase8_duration:.3f}s"
            )
        else:
            client.service.logger.info(
                f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] Phase 8: Cache write skipped (caching disabled)"
            )

        # Summary
        total_duration = time.time() - phase1_start
        client.service.logger.info(
            f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}] COMPLETE: Total time {total_duration:.2f}s for bucket '{bucket}' (mode={mode})"
        )

        return stats

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

    filename = file_path.name
    ext = file_path.suffix.lower()

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

    if not is_delta_candidate(filename):
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
