# Metadata Issue Diagnosis and Resolution

## Issue Summary

**Date**: 2025-10-14
**Severity**: Medium (affects stats accuracy, not functionality)
**Status**: Diagnosed, enhanced logging added

## The Problem

When running `deltaglider stats`, you saw warnings like:

```
Delta build/1.66.1/universal/readonlyrest_kbn_universal-1.66.1_es9.1.3.zip.delta:
no original_size metadata (original_size=342104, size=342104).
Using compressed size as fallback. This may undercount space savings.
```

This indicates that delta files are missing the `file_size` metadata key, which causes stats to undercount compression savings.

## Root Cause

The delta files in your bucket **do not have S3 object metadata** attached to them. Specifically, they're missing the `file_size` key that DeltaGlider uses to calculate the original file size before compression.

### Why Metadata is Missing

Possible causes (in order of likelihood):

1. **Uploaded with older DeltaGlider version**: Files uploaded before `file_size` metadata was added
2. **Direct S3 upload**: Files copied directly via AWS CLI, s3cmd, or other tools (bypassing DeltaGlider)
3. **Upload failure**: Metadata write failed during upload but file upload succeeded
4. **S3 storage issue**: Metadata was lost due to S3 provider issue (rare)

### What DeltaGlider Expects

When DeltaGlider uploads a delta file, it stores these metadata keys:

```python
{
    "tool": "deltaglider/5.x.x",
    "original_name": "file.zip",
    "file_sha256": "abc123...",
    "file_size": "1048576",        # ← MISSING in your files
    "created_at": "2025-01-01T00:00:00Z",
    "ref_key": "prefix/reference.bin",
    "ref_sha256": "def456...",
    "delta_size": "524288",
    "delta_cmd": "xdelta3 -e -9 -s reference.bin file.zip file.zip.delta"
}
```

Without `file_size`, DeltaGlider can't calculate the space savings accurately.

## Impact

### What Works
- ✅ File upload/download - completely unaffected
- ✅ Delta compression - works normally
- ✅ Verification - integrity checks work fine
- ✅ All other operations - sync, ls, cp, etc.

### What's Affected
- ❌ **Stats accuracy**: Compression metrics are undercounted
  - Files without metadata: counted as if they saved 0 bytes
  - Actual compression ratio: underestimated
  - Space saved: underestimated

### Example Impact

If you have 100 delta files:
- 90 files with metadata: accurate stats
- 10 files without metadata: counted at compressed size (no savings shown)
- **Result**: Stats show ~90% of actual compression savings

## The Fix (Already Applied)

### Enhanced Logging

We've improved the logging in `src/deltaglider/client_operations/stats.py` to help diagnose the issue:

**1. During metadata fetch (lines 317-333)**:
```python
if "file_size" in metadata:
    original_size = int(metadata["file_size"])
    logger.debug(f"Delta {key}: using original_size={original_size} from metadata")
else:
    logger.warning(
        f"Delta {key}: metadata missing 'file_size' key. "
        f"Available keys: {list(metadata.keys())}. "
        f"Using compressed size={size} as fallback"
    )
```

This will show you exactly which metadata keys ARE present on the object.

**2. During stats calculation (lines 395-405)**:
```python
logger.warning(
    f"Delta {obj.key}: no original_size metadata "
    f"(original_size={obj.original_size}, size={obj.size}). "
    f"Using compressed size as fallback. "
    f"This may undercount space savings."
)
```

This shows both values so you can see if they're equal (metadata missing) or different (metadata present).

### CLI Help Improvement

We've also improved the `stats` command help (line 750):
```python
@cli.command(short_help="Get bucket statistics and compression metrics")
```

And enhanced the option descriptions to be more informative.

## Verification

To check which files are missing metadata, you can use the diagnostic script:

```bash
# Create and run the metadata checker
python scripts/check_metadata.py <your-bucket-name>
```

This will show:
- Total delta files
- Files with complete metadata
- Files missing metadata
- Specific missing fields for each file

## Resolution Options

### Option 1: Re-upload Files (Recommended)

Re-uploading files will attach proper metadata:

```bash
# Re-upload a single file
deltaglider cp local-file.zip s3://bucket/path/file.zip

# Re-upload a directory
deltaglider sync local-dir/ s3://bucket/path/
```

**Pros**:
- Accurate stats for all files
- Proper metadata for future operations
- One-time fix

**Cons**:
- Takes time to re-upload
- Uses bandwidth

### Option 2: Accept Inaccurate Stats

Keep files as-is and accept that stats are undercounted:

**Pros**:
- No work required
- Files still work perfectly for download/verification

**Cons**:
- Stats show less compression than actually achieved
- Missing metadata for future features

### Option 3: Metadata Repair Tool (Future)

We could create a tool that:
1. Downloads each delta file
2. Reconstructs it to get original size
3. Updates metadata in-place

**Status**: Not implemented yet, but feasible if needed.

## Prevention

For future uploads, DeltaGlider **will always** attach complete metadata (assuming current version is used).

The code in `src/deltaglider/core/service.py` (lines 445-467) ensures metadata is set:

```python
delta_meta = DeltaMeta(
    tool=self.tool_version,
    original_name=original_name,
    file_sha256=file_sha256,
    file_size=file_size,           # ← Always set
    created_at=self.clock.now(),
    ref_key=ref_key,
    ref_sha256=ref_sha256,
    delta_size=delta_size,
    delta_cmd=f"xdelta3 -e -9 -s reference.bin {original_name} {original_name}.delta",
)

self.storage.put(
    full_delta_key,
    delta_path,
    delta_meta.to_dict(),  # ← Includes file_size
)
```

## Testing

After reinstalling from source, run stats with enhanced logging:

```bash
# Install from source
pip install -e .

# Run stats with INFO logging to see detailed messages
DG_LOG_LEVEL=INFO deltaglider stats mybucket --detailed

# Look for warnings like:
# "Delta X: metadata missing 'file_size' key. Available keys: [...]"
```

The warning will now show which metadata keys ARE present, helping you understand if:
- Metadata is completely empty: `Available keys: []`
- Metadata exists but incomplete: `Available keys: ['tool', 'ref_key', ...]`

## Summary

| Aspect | Status |
|--------|--------|
| File operations | ✅ Unaffected |
| Stats accuracy | ⚠️ Undercounted for files missing metadata |
| Logging | ✅ Enhanced to show missing keys |
| Future uploads | ✅ Will have complete metadata |
| Resolution | 📋 Re-upload or accept inaccuracy |

## Related Files

- `src/deltaglider/client_operations/stats.py` - Enhanced logging
- `src/deltaglider/core/service.py` - Metadata creation
- `src/deltaglider/core/models.py` - DeltaMeta definition
- `scripts/check_metadata.py` - Diagnostic tool (NEW)
- `docs/PAGINATION_BUG_FIX.md` - Related performance fix
