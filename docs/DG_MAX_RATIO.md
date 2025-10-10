# DG_MAX_RATIO - Delta Compression Efficiency Guard

## Overview

`DG_MAX_RATIO` is a **safety threshold** that prevents inefficient delta compression. It controls the maximum acceptable ratio of `delta_size / original_file_size`.

**Default**: `0.5` (50%)
**Range**: `0.0` to `1.0`
**Environment Variable**: `DG_MAX_RATIO`

## The Problem It Solves

When files are **too different**, xdelta3 can create a delta file that's **almost as large as the original file** (or even larger!). This defeats the purpose of compression and wastes:
- ❌ Storage space (storing a large delta instead of the original)
- ❌ CPU time (creating and applying the delta)
- ❌ Network bandwidth (downloading delta + reference instead of just the file)

## How It Works

```
1. Upload file → Create delta using xdelta3
2. Calculate ratio = delta_size / original_file_size
3. If ratio > DG_MAX_RATIO:
   ❌ Discard delta, store original file directly
   Else:
   ✅ Keep delta, save storage space
```

### Example Flow

```
Original file: 100MB
Reference file: 100MB (uploaded previously)

xdelta3 creates delta: 60MB
Ratio = 60MB / 100MB = 0.6 (60%)

With DG_MAX_RATIO=0.5:
  ❌ Delta rejected (60% > 50%)
  Action: Store 100MB original file
  Reason: Delta not efficient enough

With DG_MAX_RATIO=0.7:
  ✅ Delta accepted (60% ≤ 70%)
  Action: Store 60MB delta
  Savings: 40MB (40%)
```

## Default Value: 0.5 (50%)

**Meaning**: Only use delta compression if the delta is **≤50% of the original file size**

This default provides:
- ✅ Minimum 50% storage savings when delta compression is used
- ✅ Prevents wasting CPU on inefficient compression
- ✅ Works well for typical versioned releases (minor updates between versions)
- ✅ Balanced approach without manual tuning

## When to Adjust

### 🔽 Lower Value (More Conservative)

**Set `DG_MAX_RATIO=0.2-0.3` when:**
- Files change significantly between versions (major updates, refactors)
- Storage cost is **very high** (premium S3 tiers, small quotas)
- You want to avoid **any** inefficient compression
- You need guaranteed high-quality compression (≥70% savings)

**Example**:
```bash
export DG_MAX_RATIO=0.3

# Only accept deltas that are ≤30% of original size
# More files stored directly, but guaranteed ≥70% savings when using deltas
```

**Trade-off**:
- ✅ Higher quality compression when deltas are used
- ❌ Fewer files will use delta compression
- ❌ More direct uploads (higher storage for dissimilar files)

### 🔼 Higher Value (More Permissive)

**Set `DG_MAX_RATIO=0.6-0.8` when:**
- Files are very similar (minor patches, nightly builds, incremental changes)
- Storage cost is **cheap** (large S3 buckets, unlimited quotas)
- CPU time is **expensive** (you want to save re-uploading even with modest compression)
- You want to maximize delta compression usage

**Example**:
```bash
export DG_MAX_RATIO=0.7

# Accept deltas up to 70% of original size
# More files use delta compression, even with modest 30% savings
```

**Trade-off**:
- ✅ More files use delta compression
- ✅ Save bandwidth even with modest compression
- ❌ Some deltas may only save 20-30% space
- ❌ More CPU time spent on marginal compressions

## Real-World Scenarios

### Scenario 1: Nightly Builds (Minimal Changes) ⭐ IDEAL

```
my-app-v1.0.0.zip → 100MB (reference)
my-app-v1.0.1.zip → 100MB (0.1% code change)

Delta: 200KB (0.2% of original)
Ratio: 0.002

With ANY DG_MAX_RATIO: ✅ Use delta (99.8% savings!)
Result: Store 200KB instead of 100MB
```

**This is what DeltaGlider excels at!**

### Scenario 2: Major Version (Significant Changes)

```
my-app-v1.0.0.zip → 100MB (reference)
my-app-v2.0.0.zip → 100MB (complete rewrite, 85% different)

Delta: 85MB (85% of original)
Ratio: 0.85

With DG_MAX_RATIO=0.5: ❌ Store original (85% > 50%)
  → Stores 100MB directly
  → No compression benefit, but no CPU waste

With DG_MAX_RATIO=0.9: ✅ Use delta (85% ≤ 90%)
  → Stores 85MB delta
  → Only 15% savings, questionable benefit
```

**Recommendation**: For major versions, default `0.5` correctly rejects inefficient compression.

### Scenario 3: Different File Format (Same Content)

```
my-app-v1.0.0.zip → 100MB (ZIP archive)
my-app-v1.0.0.tar → 100MB (TAR archive, same content)

Delta: 70MB (completely different format structure)
Ratio: 0.70

With DG_MAX_RATIO=0.5: ❌ Store original (70% > 50%)
  → Stores 100MB directly
  → Correct decision: formats too different

With DG_MAX_RATIO=0.8: ✅ Use delta (70% ≤ 80%)
  → Stores 70MB delta
  → 30% savings, but CPU-intensive
```

**Recommendation**: Use consistent file formats for better compression. Default `0.5` correctly rejects cross-format compression.

### Scenario 4: Incremental Updates (Sweet Spot) ⭐

```
my-app-v1.0.0.zip → 100MB (reference)
my-app-v1.0.1.zip → 100MB (minor bugfix, 5% code change)

Delta: 5MB (5% of original)
Ratio: 0.05

With ANY DG_MAX_RATIO: ✅ Use delta (95% savings!)
Result: Store 5MB instead of 100MB
```

**This is the target use case for delta compression!**

## How to Choose the Right Value

### Decision Tree

```
Do your files have minimal changes between versions? (< 5% different)
├─ YES → Use default 0.5 ✅
│         Delta compression will work perfectly
│
└─ NO  → Are your files significantly different? (> 50% different)
          ├─ YES → Lower to 0.2-0.3 🔽
          │         Avoid wasting time on inefficient compression
          │
          └─ NO  → Are they moderately different? (20-50% different)
                   ├─ Storage is expensive → Lower to 0.3 🔽
                   │                         Only high-quality compression
                   │
                   └─ Storage is cheap → Raise to 0.6-0.7 🔼
                                          Accept modest savings
```

### Quick Reference Table

| File Similarity | Recommended DG_MAX_RATIO | Expected Behavior |
|----------------|--------------------------|-------------------|
| Nearly identical (< 5% change) | **0.5 (default)** | 🟢 95%+ savings |
| Minor updates (5-20% change) | **0.5 (default)** | 🟢 80-95% savings |
| Moderate changes (20-50% change) | **0.4-0.6** | 🟡 50-80% savings |
| Major changes (50-80% change) | **0.3 or lower** | 🔴 Store directly |
| Complete rewrites (> 80% change) | **0.3 or lower** | 🔴 Store directly |

### Use Cases by Industry

**Software Releases (SaaS)**:
```bash
export DG_MAX_RATIO=0.5  # Default
# Nightly builds with minor changes compress perfectly
```

**Mobile App Builds**:
```bash
export DG_MAX_RATIO=0.4  # Slightly conservative
# iOS/Android builds can vary, want quality compression only
```

**Database Backups**:
```bash
export DG_MAX_RATIO=0.7  # Permissive
# Daily backups are very similar, accept modest savings
```

**Document Archives**:
```bash
export DG_MAX_RATIO=0.6  # Moderate
# Documents change incrementally, accept good savings
```

**Video/Media Archives**:
```bash
export DG_MAX_RATIO=0.2  # Very conservative
# Media files are unique, only compress if very similar
```

## Configuration Examples

### Docker

**Conservative (Premium Storage)**:
```bash
docker run -e DG_MAX_RATIO=0.3 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  deltaglider/deltaglider cp file.zip s3://releases/
```

**Default (Balanced)**:
```bash
docker run -e DG_MAX_RATIO=0.5 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  deltaglider/deltaglider cp file.zip s3://releases/
```

**Permissive (Cheap Storage)**:
```bash
docker run -e DG_MAX_RATIO=0.7 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  deltaglider/deltaglider cp file.zip s3://releases/
```

### Python SDK

```python
import os

# Conservative (only high-quality compression)
os.environ['DG_MAX_RATIO'] = '0.3'

# Default (balanced)
os.environ['DG_MAX_RATIO'] = '0.5'

# Permissive (accept modest savings)
os.environ['DG_MAX_RATIO'] = '0.7'

from deltaglider import create_client
client = create_client()

summary = client.upload("file.zip", "s3://bucket/")
print(f"Delta ratio: {summary.delta_ratio:.2f}")
print(f"Used delta: {summary.is_delta}")
```

### CLI

```bash
# Conservative
export DG_MAX_RATIO=0.3
deltaglider cp my-app-v2.0.0.zip s3://releases/

# Default
export DG_MAX_RATIO=0.5
deltaglider cp my-app-v2.0.0.zip s3://releases/

# Permissive
export DG_MAX_RATIO=0.7
deltaglider cp my-app-v2.0.0.zip s3://releases/
```

### Override Per-Upload (CLI)

```bash
# Use custom ratio for specific file
deltaglider cp large-file.zip s3://releases/ --max-ratio 0.3
```

## Monitoring and Tuning

### Check Delta Ratios After Upload

```python
from deltaglider import create_client

client = create_client()
summary = client.upload("file.zip", "s3://bucket/")

print(f"Delta ratio: {summary.delta_ratio:.2f}")
print(f"Savings: {summary.savings_percent:.0f}%")
print(f"Used delta: {summary.is_delta}")

if summary.is_delta:
    print(f"✅ Used delta compression (ratio {summary.delta_ratio:.2f})")
    if summary.delta_ratio > 0.4:
        print(f"⚠️  Consider lowering DG_MAX_RATIO for better quality")
else:
    print(f"❌ Stored directly (delta would have been ratio ~{summary.delta_ratio:.2f})")
    if summary.delta_ratio < 0.6:
        print(f"💡 Consider raising DG_MAX_RATIO to enable compression")
```

### Batch Analysis

```python
from deltaglider import create_client
from pathlib import Path

client = create_client()
ratios = []

for file in Path("releases").glob("*.zip"):
    summary = client.upload(str(file), f"s3://bucket/{file.name}")
    if summary.is_delta:
        ratios.append(summary.delta_ratio)

if ratios:
    avg_ratio = sum(ratios) / len(ratios)
    max_ratio = max(ratios)

    print(f"Average delta ratio: {avg_ratio:.2f}")
    print(f"Maximum delta ratio: {max_ratio:.2f}")
    print(f"Files compressed: {len(ratios)}")

    if avg_ratio < 0.2:
        print("💡 Consider raising DG_MAX_RATIO - you're getting excellent compression")
    elif max_ratio > 0.6:
        print("⚠️  Consider lowering DG_MAX_RATIO - some deltas are inefficient")
```

### Optimization Strategy

1. **Start with default (0.5)**
2. **Monitor delta ratios** for 1 week of uploads
3. **Analyze results**:
   - If most ratios < 0.2: Consider raising to 0.6-0.7
   - If many ratios > 0.4: Consider lowering to 0.3-0.4
   - If ratios vary widely: Keep default 0.5
4. **Adjust and re-test** for 1 week
5. **Repeat until optimal** for your use case

## Advanced Usage

### Dynamic Ratio Based on File Type

```python
import os
from pathlib import Path
from deltaglider import create_client

def get_optimal_ratio(file_path: str) -> float:
    """Determine optimal ratio based on file type."""
    suffix = Path(file_path).suffix.lower()

    # Very compressible (source code archives)
    if suffix in ['.zip', '.tar', '.gz']:
        return 0.6

    # Moderately compressible (compiled binaries)
    elif suffix in ['.jar', '.war', '.deb', '.rpm']:
        return 0.5

    # Rarely compressible (media, already compressed)
    elif suffix in ['.mp4', '.jpg', '.png']:
        return 0.2

    # Default
    return 0.5

file = "my-app.zip"
os.environ['DG_MAX_RATIO'] = str(get_optimal_ratio(file))

client = create_client()
summary = client.upload(file, "s3://bucket/")
```

### A/B Testing Different Ratios

```python
from deltaglider import create_client
import os

def test_ratios(file_path: str, ratios: list[float]):
    """Test different ratios and report results."""
    results = []

    for ratio in ratios:
        os.environ['DG_MAX_RATIO'] = str(ratio)
        client = create_client()

        # Simulate upload (don't actually upload)
        summary = client.estimate_compression(file_path, "s3://bucket/test/")

        results.append({
            'ratio_threshold': ratio,
            'would_use_delta': summary.delta_ratio <= ratio,
            'delta_ratio': summary.delta_ratio,
            'savings': summary.savings_percent if summary.delta_ratio <= ratio else 0
        })

    return results

# Test different ratios
file = "my-app-v2.0.0.zip"
test_results = test_ratios(file, [0.3, 0.5, 0.7])

for result in test_results:
    print(f"Ratio {result['ratio_threshold']}: "
          f"Delta={result['would_use_delta']}, "
          f"Savings={result['savings']:.0f}%")
```

## FAQ

### Q: What happens if I set DG_MAX_RATIO=1.0?

**A**: Delta compression will **always** be used, even if the delta is larger than the original file! This is generally a bad idea and defeats the purpose of the threshold.

**Example**:
```bash
export DG_MAX_RATIO=1.0

# File: 100MB, Delta: 120MB
# Ratio: 1.2
# With DG_MAX_RATIO=1.0: ✅ Use delta (1.2 > 1.0 but we accept anything ≤1.0)
# Wait, that's wrong! The delta is LARGER than the original!

# NEVER set DG_MAX_RATIO to 1.0 or higher
```

### Q: What happens if I set DG_MAX_RATIO=0.0?

**A**: Delta compression will **never** be used. All files will be stored directly. This is equivalent to disabling DeltaGlider's compression entirely.

### Q: Can I disable the ratio check?

**A**: No, and you shouldn't want to. The ratio check is a critical safety feature that prevents wasting storage and CPU on inefficient compression.

### Q: Does DG_MAX_RATIO affect downloading?

**A**: No, `DG_MAX_RATIO` only affects **uploads**. During download, DeltaGlider automatically detects whether a file is stored as a delta or directly and handles reconstruction accordingly.

### Q: Can I set different ratios for different buckets?

**A**: Not directly via environment variables, but you can change `DG_MAX_RATIO` before each upload in your code:

```python
import os
from deltaglider import create_client

# High-quality compression for production releases
os.environ['DG_MAX_RATIO'] = '0.3'
client = create_client()
client.upload("prod-release.zip", "s3://production/")

# Permissive compression for dev builds
os.environ['DG_MAX_RATIO'] = '0.7'
client = create_client()
client.upload("dev-build.zip", "s3://development/")
```

### Q: How do I know if my DG_MAX_RATIO is set correctly?

**A**: Monitor your upload summaries. If most deltas have ratios close to your threshold (e.g., 0.45-0.50 with default 0.5), you might want to lower it. If most deltas have very low ratios (e.g., < 0.2), you could raise it.

**Ideal scenario**: Most successful delta compressions have ratios < 0.3, and inefficient deltas (> 0.5) are correctly rejected.

## Summary

**`DG_MAX_RATIO` prevents wasting time and storage on inefficient delta compression.**

### Quick Takeaways

✅ **Default 0.5 works for 90% of use cases**
✅ **Lower values (0.2-0.3) for dissimilar files or expensive storage**
✅ **Higher values (0.6-0.7) for very similar files or cheap storage**
✅ **Monitor delta ratios to tune for your use case**
✅ **Never set to 1.0 or higher (defeats the purpose)**
✅ **Never set to 0.0 (disables delta compression entirely)**

### Golden Rule

**If you're not sure, keep the default `0.5`.**

It's a sensible balance that:
- Prevents inefficient compression (no deltas > 50% of original size)
- Allows excellent savings on similar files (most deltas are < 20%)
- Works well for typical versioned releases
- Requires no manual tuning for most use cases

---

**Related Documentation**:
- [CLAUDE.md](../CLAUDE.md) - Environment variables reference
- [README.md](../README.md) - Docker usage and configuration
- [docs/sdk/getting-started.md](sdk/getting-started.md) - SDK configuration guide
