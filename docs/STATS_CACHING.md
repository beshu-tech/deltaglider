# Bucket Statistics Caching

**TL;DR**: Bucket stats are now cached in S3 with automatic validation. What took 20 minutes now takes ~100ms when the bucket hasn't changed.

## Overview

DeltaGlider's `get_bucket_stats()` operation now includes intelligent S3-based caching that dramatically improves performance for read-heavy workloads while maintaining accuracy through automatic validation.

## The Problem

Computing bucket statistics requires:
1. **LIST operation**: Get all objects (~50-100ms per 1000 objects)
2. **HEAD operations**: Fetch metadata for delta files (expensive!)
   - For a bucket with 10,000 delta files: 10,000 HEAD calls
   - Even with 10 parallel workers: ~1,000 sequential batches
   - At ~100ms per batch: **100+ seconds minimum**
   - With network issues or throttling: **20+ minutes** 😱

This made monitoring dashboards and repeated stats checks impractical.

## The Solution

### S3-Based Cache with Automatic Validation

Statistics are cached in S3 at `.deltaglider/stats_{mode}.json` (one per mode). On every call:

1. **Quick LIST operation** (~50-100ms) - always performed for validation
2. **Compare** current object_count + compressed_size with cache
3. **If unchanged** → Return cached stats instantly ✅ (**~100ms total**)
4. **If changed** → Recompute and update cache automatically

### Three Stats Modes

```bash
# Quick mode (default): Fast listing-only, approximate compression metrics
deltaglider stats my-bucket

# Sampled mode: One HEAD per deltaspace, balanced accuracy/speed
deltaglider stats my-bucket --sampled

# Detailed mode: All HEAD calls, most accurate (slowest)
deltaglider stats my-bucket --detailed
```

Each mode has its own independent cache file.

## Performance

| Scenario | Before | After | Speedup |
|----------|--------|-------|---------|
| **First run** (cold cache) | 20 min | 20 min | 1x (must compute) |
| **Bucket unchanged** (warm cache) | 20 min | **100ms** | **200x** ✨ |
| **Bucket changed** (stale cache) | 20 min | 20 min | 1x (auto-recompute) |
| **Dashboard monitoring** | 20 min/check | **100ms/check** | **200x** ✨ |

## CLI Usage

### Basic Usage

```bash
# Use cache (default behavior)
deltaglider stats my-bucket

# Force recomputation even if cache valid
deltaglider stats my-bucket --refresh

# Skip cache entirely (both read and write)
deltaglider stats my-bucket --no-cache

# Different modes with caching
deltaglider stats my-bucket --sampled
deltaglider stats my-bucket --detailed
```

### Cache Control Flags

| Flag | Description | Use Case |
|------|-------------|----------|
| *(none)* | Use cache if valid | **Default** - Fast monitoring |
| `--refresh` | Force recomputation | Updated data needed now |
| `--no-cache` | Skip caching entirely | Testing, one-off analysis |
| `--sampled` | Balanced mode | Good accuracy, faster than detailed |
| `--detailed` | Most accurate mode | Analytics, reports |

## Python SDK Usage

```python
from deltaglider import create_client

client = create_client()

# Use cache (fast, ~100ms with cache hit)
stats = client.get_bucket_stats('releases')

# Force refresh (slow, recomputes everything)
stats = client.get_bucket_stats('releases', refresh_cache=True)

# Skip cache entirely
stats = client.get_bucket_stats('releases', use_cache=False)

# Different modes with caching
stats = client.get_bucket_stats('releases', mode='quick')     # Fast
stats = client.get_bucket_stats('releases', mode='sampled')   # Balanced
stats = client.get_bucket_stats('releases', mode='detailed')  # Accurate
```

## Cache Structure

Cache files are stored at `.deltaglider/stats_{mode}.json` in your bucket:

```json
{
  "version": "1.0",
  "mode": "quick",
  "computed_at": "2025-10-14T10:30:00Z",
  "validation": {
    "object_count": 1523,
    "compressed_size": 1234567890
  },
  "stats": {
    "bucket": "releases",
    "object_count": 1523,
    "total_size": 50000000000,
    "compressed_size": 1234567890,
    "space_saved": 48765432110,
    "average_compression_ratio": 0.9753,
    "delta_objects": 1500,
    "direct_objects": 23
  }
}
```

## How Validation Works

**Smart Staleness Detection**:
1. Always perform quick LIST operation (required anyway, ~50-100ms)
2. Calculate current `object_count` and `compressed_size` from LIST
3. Compare with cached values
4. If **both match** → Cache valid, return instantly
5. If **either differs** → Bucket changed, recompute automatically

This catches:
- ✅ Objects added (count increases)
- ✅ Objects removed (count decreases)
- ✅ Objects replaced (size changes)
- ✅ Content modified (size changes)

**Edge Case**: If only metadata changes (tags, headers) but not content/count/size, cache remains valid. This is acceptable since metadata changes are rare and don't affect core statistics.

## Use Cases

### ✅ Perfect For

1. **Monitoring Dashboards**
   - Check stats every minute
   - Bucket rarely changes
   - **20 min → 100ms per check** ✨

2. **CI/CD Status Checks**
   - Verify upload success
   - Check compression effectiveness
   - Near-instant feedback

3. **Repeated Analysis**
   - Multiple stats queries during investigation
   - Cache persists across sessions
   - Huge time savings

### ⚠️ Less Beneficial For

1. **Write-Heavy Buckets**
   - Bucket changes on every check
   - Cache always stale
   - **No benefit, but no harm either** (graceful degradation)

2. **One-Off Queries**
   - Single stats check
   - Cache doesn't help (cold cache)
   - Still works normally

## Cache Management

### Automatic Management

- **Creation**: Automatic on first `get_bucket_stats()` call
- **Validation**: Automatic on every call (always current)
- **Updates**: Automatic when bucket changes
- **Cleanup**: Not needed (cache files are tiny ~1-10KB)

### Manual Management

```bash
# View cache files
deltaglider ls s3://my-bucket/.deltaglider/

# Delete cache manually (will be recreated automatically)
deltaglider rm s3://my-bucket/.deltaglider/stats_quick.json
deltaglider rm s3://my-bucket/.deltaglider/stats_sampled.json
deltaglider rm s3://my-bucket/.deltaglider/stats_detailed.json

# Or delete entire .deltaglider prefix
deltaglider rm -r s3://my-bucket/.deltaglider/
```

## Technical Details

### Cache Files

- **Location**: `.deltaglider/` prefix in each bucket
- **Naming**: `stats_{mode}.json` (quick, sampled, detailed)
- **Size**: ~1-10KB per file
- **Format**: JSON with version, mode, validation data, and stats

### Validation Logic

```python
def is_cache_valid(cached, current):
    """Cache is valid if object count and size unchanged."""
    return (
        cached['object_count'] == current['object_count'] and
        cached['compressed_size'] == current['compressed_size']
    )
```

### Error Handling

Cache operations are **non-fatal**:
- ✅ Cache read fails → Compute normally, log warning
- ✅ Cache write fails → Return computed stats, log warning
- ✅ Corrupted cache → Ignore, recompute, overwrite
- ✅ Version mismatch → Ignore, recompute with new version
- ✅ Permission denied → Log warning, continue without caching

**The stats operation never fails due to cache issues.**

## Future Enhancements

Potential improvements for the future:

1. **TTL-Based Expiration**: Auto-refresh after N hours even if unchanged
2. **Cache Cleanup Command**: `deltaglider cache clear` for manual invalidation
3. **Cache Statistics**: Show hit/miss rates, staleness info
4. **Async Cache Updates**: Background refresh for very large buckets
5. **Cross-Bucket Cache**: Share reference data across related buckets

## Comparison with Old Implementation

| Aspect | Old (In-Memory) | New (S3-Based) |
|--------|----------------|----------------|
| **Storage** | Process memory | S3 bucket |
| **Persistence** | Lost on restart | Survives restarts |
| **Sharing** | Per-process | Shared across all clients |
| **Validation** | None | Automatic on every call |
| **Staleness** | Always fresh | Automatically detected |
| **Use Case** | Single session | Monitoring, dashboards |

## Examples

### Example 1: Monitoring Dashboard

```python
from deltaglider import create_client
import time

client = create_client()

while True:
    # Fast stats check (~100ms with cache)
    stats = client.get_bucket_stats('releases')
    print(f"Objects: {stats.object_count}, "
          f"Compression: {stats.average_compression_ratio:.1%}")

    time.sleep(60)  # Check every minute

# First run: 20 min (computes and caches)
# All subsequent runs: ~100ms (cache hit)
```

### Example 2: CI/CD Pipeline

```python
from deltaglider import create_client

client = create_client()

# Upload new release
client.upload("v2.0.0.zip", "s3://releases/v2.0.0/")

# Quick verification (fast with cache)
stats = client.get_bucket_stats('releases')
if stats.average_compression_ratio < 0.90:
    print("Warning: Lower than expected compression")
```

### Example 3: Force Fresh Stats

```python
from deltaglider import create_client

client = create_client()

# Force recomputation for accurate report
stats = client.get_bucket_stats(
    'releases',
    mode='detailed',
    refresh_cache=True
)

print(f"Accurate compression report:")
print(f"  Original: {stats.total_size / 1e9:.1f} GB")
print(f"  Stored: {stats.compressed_size / 1e9:.1f} GB")
print(f"  Saved: {stats.space_saved / 1e9:.1f} GB ({stats.average_compression_ratio:.1%})")
```

## FAQ

**Q: Does caching affect accuracy?**
A: No! Cache is automatically validated on every call. If the bucket changed, stats are recomputed automatically.

**Q: What if I need fresh stats immediately?**
A: Use `--refresh` flag (CLI) or `refresh_cache=True` (SDK) to force recomputation.

**Q: Can I disable caching?**
A: Yes, use `--no-cache` flag (CLI) or `use_cache=False` (SDK).

**Q: How much space do cache files use?**
A: ~1-10KB per mode, negligible for any bucket.

**Q: What happens if cache write fails?**
A: The operation continues normally - computed stats are returned and a warning is logged. Caching is optional and non-fatal.

**Q: Do I need to clean up cache files?**
A: No, they're tiny and automatically managed. But you can delete `.deltaglider/` prefix if desired.

**Q: Does cache work across different modes?**
A: Each mode (quick, sampled, detailed) has its own independent cache file.

---

**Implementation**: See [PR #XX] for complete implementation details and test coverage.

**Related**: [SDK Documentation](sdk/README.md) | [CLI Reference](../README.md#cli-reference) | [Architecture](sdk/architecture.md)
