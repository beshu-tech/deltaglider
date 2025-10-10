# DeltaGlider API Reference

Complete API documentation for the DeltaGlider Python SDK.

## Table of Contents

- [Client Creation](#client-creation)
- [DeltaGliderClient](#deltaglidererclient)
- [UploadSummary](#uploadsummary)
- [DeltaService](#deltaservice)
- [Models](#models)
- [Exceptions](#exceptions)

## Client Creation

### `create_client`

Factory function to create a configured DeltaGlider client with sensible defaults.

```python
def create_client(
    endpoint_url: Optional[str] = None,
    log_level: str = "INFO",
    **kwargs
) -> DeltaGliderClient
```

#### Parameters

- **endpoint_url** (`Optional[str]`): S3 endpoint URL for MinIO, R2, or other S3-compatible storage. If None, uses AWS S3.
- **log_level** (`str`): Logging verbosity level. Options: "DEBUG", "INFO", "WARNING", "ERROR". Default: "INFO".
- **kwargs**: Additional arguments passed to `DeltaService`:
  - **tool_version** (`str`): Version string for metadata. Default: "deltaglider/0.1.0"
  - **max_ratio** (`float`): Maximum acceptable delta/file ratio. Default: 0.5

**Security Note**: DeltaGlider automatically uses ephemeral, process-isolated cache (`/tmp/deltaglider-*`) that is cleaned up on exit. No configuration needed.

#### Returns

`DeltaGliderClient`: Configured client instance ready for use.

#### Examples

```python
# Default AWS S3 configuration
client = create_client()

# Custom endpoint for MinIO
client = create_client(endpoint_url="http://localhost:9000")

# Debug mode
client = create_client(log_level="DEBUG")

# Custom delta ratio threshold
client = create_client(max_ratio=0.3)  # Only use delta if <30% of original
```

## DeltaGliderClient

Main client class for interacting with DeltaGlider.

### Constructor

```python
class DeltaGliderClient:
    def __init__(
        self,
        service: DeltaService,
        endpoint_url: Optional[str] = None
    )
```

**Note**: Use `create_client()` instead of instantiating directly.

### boto3-Compatible Methods (Recommended)

These methods provide compatibility with boto3's core S3 client operations. DeltaGlider implements 21 essential S3 methods covering ~80% of common use cases. See [BOTO3_COMPATIBILITY.md](../../BOTO3_COMPATIBILITY.md) for complete coverage details.

#### `list_objects`

List objects in a bucket with smart performance optimizations.

```python
def list_objects(
    self,
    Bucket: str,
    Prefix: str = "",
    Delimiter: str = "",
    MaxKeys: int = 1000,
    ContinuationToken: Optional[str] = None,
    StartAfter: Optional[str] = None,
    FetchMetadata: bool = False,
    **kwargs
) -> dict[str, Any]
```

##### Parameters

- **Bucket** (`str`): S3 bucket name.
- **Prefix** (`str`): Filter results to keys beginning with prefix.
- **Delimiter** (`str`): Delimiter for grouping keys (e.g., '/' for folders).
- **MaxKeys** (`int`): Maximum number of keys to return (for pagination). Default: 1000.
- **ContinuationToken** (`Optional[str]`): Token from previous response for pagination.
- **StartAfter** (`Optional[str]`): Start listing after this key (alternative pagination).
- **FetchMetadata** (`bool`): If True, fetch compression metadata for delta files only. Default: False.
  - **IMPORTANT**: Non-delta files NEVER trigger metadata fetching (no performance impact).
  - With `FetchMetadata=False`: ~50ms for 1000 objects (1 API call)
  - With `FetchMetadata=True`: ~2-3s for 1000 objects (1 + N delta files API calls)

##### Performance Optimization

The method intelligently optimizes performance by:
1. **Never** fetching metadata for non-delta files (they don't need it)
2. Only fetching metadata for delta files when explicitly requested
3. Supporting efficient pagination for large buckets

##### Returns

boto3-compatible dict with:
- **Contents** (`list[dict]`): List of S3Object dicts with Key, Size, LastModified, Metadata
- **CommonPrefixes** (`list[dict]`): Optional list of common prefixes (folders)
- **IsTruncated** (`bool`): Whether more results are available
- **NextContinuationToken** (`str`): Token for next page
- **KeyCount** (`int`): Number of keys returned

##### Examples

```python
# Fast listing for UI display (no metadata fetching)
response = client.list_objects(Bucket='releases')
for obj in response['Contents']:
    print(f"{obj['Key']}: {obj['Size']} bytes")

# Paginated listing for large buckets
response = client.list_objects(Bucket='releases', MaxKeys=100)
while response.get('IsTruncated'):
    for obj in response['Contents']:
        print(obj['Key'])
    response = client.list_objects(
        Bucket='releases',
        MaxKeys=100,
        ContinuationToken=response.get('NextContinuationToken')
    )

# Get detailed compression stats (slower, only for analytics)
response = client.list_objects(
    Bucket='releases',
    FetchMetadata=True  # Only fetches for delta files
)
for obj in response['Contents']:
    metadata = obj.get('Metadata', {})
    if metadata.get('deltaglider-is-delta') == 'true':
        compression = metadata.get('deltaglider-compression-ratio', 'unknown')
        print(f"{obj['Key']}: {compression} compression")
```

#### `get_bucket_stats`

Get statistics for a bucket with optional detailed compression metrics. Results are cached per client session for performance.

```python
def get_bucket_stats(
    self,
    bucket: str,
    detailed_stats: bool = False
) -> BucketStats
```

##### Parameters

- **bucket** (`str`): S3 bucket name.
- **detailed_stats** (`bool`): If True, fetch accurate compression ratios for delta files. Default: False.
  - With `detailed_stats=False`: ~50ms for any bucket size (LIST calls only)
  - With `detailed_stats=True`: ~2-3s per 1000 objects (adds HEAD calls for delta files)

##### Caching Behavior

- **Session-scoped cache**: Results cached within client instance lifetime
- **Automatic invalidation**: Cache cleared on bucket mutations (put, delete, bucket operations)
- **Intelligent reuse**: Detailed stats can serve quick stat requests
- **Manual cache control**: Use `clear_cache()` to invalidate all cached stats

##### Returns

`BucketStats`: Dataclass containing:
- **bucket** (`str`): Bucket name
- **object_count** (`int`): Total number of objects
- **total_size** (`int`): Original size in bytes (before compression)
- **compressed_size** (`int`): Actual stored size in bytes
- **space_saved** (`int`): Bytes saved through compression
- **average_compression_ratio** (`float`): Average compression ratio (0.0-1.0)
- **delta_objects** (`int`): Number of delta-compressed objects
- **direct_objects** (`int`): Number of directly stored objects

##### Examples

```python
# Quick stats for dashboard display (cached after first call)
stats = client.get_bucket_stats('releases')
print(f"Objects: {stats.object_count}, Size: {stats.total_size}")

# Second call hits cache (instant response)
stats = client.get_bucket_stats('releases')
print(f"Space saved: {stats.space_saved} bytes")

# Detailed stats for analytics (slower but accurate, also cached)
stats = client.get_bucket_stats('releases', detailed_stats=True)
print(f"Compression ratio: {stats.average_compression_ratio:.1%}")

# Quick call after detailed call reuses detailed cache (more accurate)
quick_stats = client.get_bucket_stats('releases')  # Uses detailed cache

# Clear cache to force refresh
client.clear_cache()
stats = client.get_bucket_stats('releases')  # Fresh computation
```

#### `put_object`

Upload an object to S3 with automatic delta compression (boto3-compatible).

```python
def put_object(
    self,
    Bucket: str,
    Key: str,
    Body: bytes | str | Path | None = None,
    Metadata: Optional[Dict[str, str]] = None,
    ContentType: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]
```

##### Parameters

- **Bucket** (`str`): S3 bucket name.
- **Key** (`str`): Object key (path in bucket).
- **Body** (`bytes | str | Path`): Object data.
- **Metadata** (`Optional[Dict[str, str]]`): Custom metadata.
- **ContentType** (`Optional[str]`): MIME type (for compatibility).

##### Returns

Dict with ETag and DeltaGlider compression info.

#### `get_object`

Download an object from S3 with automatic delta reconstruction (boto3-compatible).

```python
def get_object(
    self,
    Bucket: str,
    Key: str,
    **kwargs
) -> Dict[str, Any]
```

##### Returns

Dict with Body stream and metadata (identical to boto3).

#### `create_bucket`

Create an S3 bucket (boto3-compatible).

```python
def create_bucket(
    self,
    Bucket: str,
    CreateBucketConfiguration: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]
```

##### Parameters

- **Bucket** (`str`): Name of the bucket to create.
- **CreateBucketConfiguration** (`Optional[Dict[str, str]]`): Bucket configuration with optional LocationConstraint.

##### Returns

Dict with Location of created bucket.

##### Notes

- Idempotent: Creating an existing bucket returns success
- Use for basic bucket creation without advanced S3 features

##### Examples

```python
# Create bucket in default region
client.create_bucket(Bucket='my-releases')

# Create bucket in specific region
client.create_bucket(
    Bucket='my-backups',
    CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'}
)
```

#### `delete_bucket`

Delete an S3 bucket (boto3-compatible).

```python
def delete_bucket(
    self,
    Bucket: str,
    **kwargs
) -> Dict[str, Any]
```

##### Parameters

- **Bucket** (`str`): Name of the bucket to delete.

##### Returns

Dict confirming deletion.

##### Notes

- Idempotent: Deleting a non-existent bucket returns success
- Bucket must be empty before deletion

##### Examples

```python
# Delete empty bucket
client.delete_bucket(Bucket='old-releases')
```

#### `list_buckets`

List all S3 buckets (boto3-compatible). Includes cached statistics when available.

```python
def list_buckets(
    self,
    **kwargs
) -> Dict[str, Any]
```

##### Returns

Dict with list of buckets and owner information (identical to boto3). Each bucket may include optional `DeltaGliderStats` metadata if statistics have been previously cached.

##### Response Structure

```python
{
    'Buckets': [
        {
            'Name': 'bucket-name',
            'CreationDate': datetime(2025, 1, 1),
            'DeltaGliderStats': {  # Optional, only if cached
                'Cached': True,
                'Detailed': bool,  # Whether detailed stats were fetched
                'ObjectCount': int,
                'TotalSize': int,
                'CompressedSize': int,
                'SpaceSaved': int,
                'AverageCompressionRatio': float,
                'DeltaObjects': int,
                'DirectObjects': int
            }
        }
    ],
    'Owner': {...}
}
```

##### Examples

```python
# List all buckets
response = client.list_buckets()
for bucket in response['Buckets']:
    print(f"{bucket['Name']} - Created: {bucket['CreationDate']}")

    # Check if stats are cached
    if 'DeltaGliderStats' in bucket:
        stats = bucket['DeltaGliderStats']
        print(f"  Cached stats: {stats['ObjectCount']} objects, "
              f"{stats['AverageCompressionRatio']:.1%} compression")

# Fetch stats first, then list buckets to see cached data
client.get_bucket_stats('my-bucket', detailed_stats=True)
response = client.list_buckets()
# Now 'my-bucket' will include DeltaGliderStats in response
```

### Simple API Methods

#### `upload`

Upload a file to S3 with automatic delta compression.

```python
def upload(
    self,
    file_path: str | Path,
    s3_url: str,
    tags: Optional[Dict[str, str]] = None,
    max_ratio: float = 0.5
) -> UploadSummary
```

##### Parameters

- **file_path** (`str | Path`): Local file path to upload.
- **s3_url** (`str`): S3 destination URL in format `s3://bucket/prefix/`.
- **tags** (`Optional[Dict[str, str]]`): S3 object tags to attach. (Future feature)
- **max_ratio** (`float`): Maximum acceptable delta/file size ratio. Default: 0.5.

##### Returns

`UploadSummary`: Object containing upload statistics and compression details.

##### Raises

- `FileNotFoundError`: If local file doesn't exist.
- `ValueError`: If S3 URL is invalid.
- `PermissionError`: If S3 access is denied.

##### Examples

```python
# Simple upload
summary = client.upload("app.zip", "s3://releases/v1.0.0/")

# With custom compression threshold
summary = client.upload(
    "large-file.tar.gz",
    "s3://backups/",
    max_ratio=0.3  # Only use delta if compression > 70%
)

# Check results
if summary.is_delta:
    print(f"Stored as delta: {summary.stored_size_mb:.1f} MB")
else:
    print(f"Stored as full file: {summary.original_size_mb:.1f} MB")
```

#### `download`

Download and reconstruct a file from S3.

```python
def download(
    self,
    s3_url: str,
    output_path: str | Path
) -> None
```

##### Parameters

- **s3_url** (`str`): S3 source URL in format `s3://bucket/key`.
- **output_path** (`str | Path`): Local destination path.

##### Returns

None. File is written to `output_path`.

##### Raises

- `ValueError`: If S3 URL is invalid or missing key.
- `FileNotFoundError`: If S3 object doesn't exist.
- `PermissionError`: If local path is not writable or S3 access denied.

##### Examples

```python
# Download a file
client.download("s3://releases/v1.0.0/app.zip", "downloaded.zip")

# Auto-detects .delta suffix if needed
client.download("s3://releases/v1.0.0/app.zip", "app.zip")
# Will try app.zip first, then app.zip.delta if not found

# Download to specific directory
from pathlib import Path
output = Path("/tmp/downloads/app.zip")
output.parent.mkdir(parents=True, exist_ok=True)
client.download("s3://releases/v1.0.0/app.zip", output)
```

#### `verify`

Verify the integrity of a stored file using SHA256 checksums.

```python
def verify(
    self,
    s3_url: str
) -> bool
```

##### Parameters

- **s3_url** (`str`): S3 URL of the file to verify.

##### Returns

`bool`: True if verification passed, False if corrupted.

##### Raises

- `ValueError`: If S3 URL is invalid.
- `FileNotFoundError`: If S3 object doesn't exist.

##### Examples

```python
# Verify file integrity
is_valid = client.verify("s3://releases/v1.0.0/app.zip")

if is_valid:
    print("✓ File integrity verified")
else:
    print("✗ File is corrupted!")
    # Re-upload or investigate
```

### Cache Management Methods

DeltaGlider maintains two types of caches for performance optimization:
1. **Reference cache**: Binary reference files used for delta reconstruction
2. **Statistics cache**: Bucket statistics (session-scoped)

#### `clear_cache`

Clear all cached data including reference files and bucket statistics.

```python
def clear_cache(self) -> None
```

##### Description

Removes all cached reference files from the local filesystem and invalidates all bucket statistics. Useful for:
- Forcing fresh statistics computation
- Freeing disk space in long-running applications
- Ensuring latest data after external bucket modifications
- Testing and development workflows

##### Cache Types Cleared

1. **Reference Cache**: Binary reference files stored in `/tmp/deltaglider-*/`
   - Encrypted at rest with ephemeral keys
   - Content-addressed storage (SHA256-based filenames)
   - Automatically cleaned up on process exit

2. **Statistics Cache**: Bucket statistics cached per client session
   - Metadata about compression ratios and object counts
   - Session-scoped (not persisted to disk)
   - Automatically invalidated on bucket mutations

##### Examples

```python
# Long-running application
client = create_client()

# Work with files
for i in range(1000):
    client.upload(f"file_{i}.zip", "s3://bucket/")

    # Periodic cache cleanup to prevent disk buildup
    if i % 100 == 0:
        client.clear_cache()

# Force fresh statistics after external changes
stats_before = client.get_bucket_stats('releases')  # Cached
# ... external tool modifies bucket ...
client.clear_cache()
stats_after = client.get_bucket_stats('releases')  # Fresh data

# Development workflow
client.clear_cache()  # Start with clean state
```

#### `evict_cache`

Remove a specific cached reference file from the local cache.

```python
def evict_cache(self, s3_url: str) -> None
```

##### Parameters

- **s3_url** (`str`): S3 URL of the reference file to evict (e.g., `s3://bucket/prefix/reference.bin`)

##### Description

Removes a specific reference file from the cache without affecting other cached files or statistics. Useful for:
- Selective cache invalidation when specific references are updated
- Memory management in applications with many delta spaces
- Testing specific delta compression scenarios

##### Examples

```python
# Evict specific reference after update
client.upload("new-reference.zip", "s3://releases/v2.0.0/")
client.evict_cache("s3://releases/v2.0.0/reference.bin")

# Next upload will fetch fresh reference
client.upload("similar-file.zip", "s3://releases/v2.0.0/")

# Selective eviction for specific delta spaces
delta_spaces = ["v1.0.0", "v1.1.0", "v1.2.0"]
for space in delta_spaces:
    client.evict_cache(f"s3://releases/{space}/reference.bin")
```

##### See Also

- [docs/CACHE_MANAGEMENT.md](../../CACHE_MANAGEMENT.md): Complete cache management guide
- `clear_cache()`: Clear all caches

#### `lifecycle_policy`

Set lifecycle policy for S3 prefix (placeholder for future implementation).

```python
def lifecycle_policy(
    self,
    s3_prefix: str,
    days_before_archive: int = 30,
    days_before_delete: int = 90
) -> None
```

**Note**: This method is a placeholder for future S3 lifecycle policy management.

## UploadSummary

Data class containing upload operation results.

```python
@dataclass
class UploadSummary:
    operation: str           # Operation type: "PUT" or "PUT_DELTA"
    bucket: str              # S3 bucket name
    key: str                 # S3 object key
    original_size: int       # Original file size in bytes
    stored_size: int         # Actual stored size in bytes
    is_delta: bool           # Whether delta compression was used
    delta_ratio: float = 0.0 # Ratio of delta size to original
```

### Properties

#### `original_size_mb`

Original file size in megabytes.

```python
@property
def original_size_mb(self) -> float
```

#### `stored_size_mb`

Stored size in megabytes (after compression if applicable).

```python
@property
def stored_size_mb(self) -> float
```

#### `savings_percent`

Percentage saved through compression.

```python
@property
def savings_percent(self) -> float
```

### Example Usage

```python
summary = client.upload("app.zip", "s3://releases/")

print(f"Operation: {summary.operation}")
print(f"Location: s3://{summary.bucket}/{summary.key}")
print(f"Original: {summary.original_size_mb:.1f} MB")
print(f"Stored: {summary.stored_size_mb:.1f} MB")
print(f"Saved: {summary.savings_percent:.0f}%")
print(f"Delta used: {summary.is_delta}")

if summary.is_delta:
    print(f"Delta ratio: {summary.delta_ratio:.2%}")
```

## DeltaService

Core service class handling delta compression logic.

```python
class DeltaService:
    def __init__(
        self,
        storage: StoragePort,
        diff: DiffPort,
        hasher: HashPort,
        cache: CachePort,
        clock: ClockPort,
        logger: LoggerPort,
        metrics: MetricsPort,
        tool_version: str = "deltaglider/0.1.0",
        max_ratio: float = 0.5
    )
```

### Methods

#### `put`

Upload a file with automatic delta compression.

```python
def put(
    self,
    file: Path,
    delta_space: DeltaSpace,
    max_ratio: Optional[float] = None
) -> PutSummary
```

#### `get`

Download and reconstruct a file.

```python
def get(
    self,
    object_key: ObjectKey,
    output_path: Path
) -> GetSummary
```

#### `verify`

Verify file integrity.

```python
def verify(
    self,
    object_key: ObjectKey
) -> VerifyResult
```

## Models

### DeltaSpace

Represents a compression space in S3.

```python
@dataclass(frozen=True)
class DeltaSpace:
    bucket: str  # S3 bucket name
    prefix: str  # S3 prefix for related files
```

### ObjectKey

Represents an S3 object location.

```python
@dataclass(frozen=True)
class ObjectKey:
    bucket: str  # S3 bucket name
    key: str     # S3 object key
```

### PutSummary

Detailed upload operation results.

```python
@dataclass
class PutSummary:
    operation: str              # "PUT" or "PUT_DELTA"
    bucket: str                 # S3 bucket
    key: str                    # S3 key
    file_size: int              # Original file size
    file_hash: str              # SHA256 of original file
    delta_size: Optional[int]   # Size of delta (if used)
    delta_hash: Optional[str]   # SHA256 of delta
    delta_ratio: Optional[float] # Delta/original ratio
    reference_hash: Optional[str] # Reference file hash
```

### GetSummary

Download operation results.

```python
@dataclass
class GetSummary:
    operation: str    # "GET" or "GET_DELTA"
    bucket: str       # S3 bucket
    key: str          # S3 key
    size: int         # Downloaded size
    hash: str         # SHA256 hash
    reconstructed: bool # Whether reconstruction was needed
```

### VerifyResult

Verification operation results.

```python
@dataclass
class VerifyResult:
    valid: bool           # Verification result
    operation: str        # "VERIFY" or "VERIFY_DELTA"
    expected_hash: str    # Expected SHA256
    actual_hash: Optional[str] # Actual SHA256 (if computed)
    details: Optional[str] # Error details if invalid
```

## Exceptions

DeltaGlider uses standard Python exceptions with descriptive messages:

### Common Exceptions

- **FileNotFoundError**: Local file or S3 object not found
- **PermissionError**: Access denied (S3 or local filesystem)
- **ValueError**: Invalid parameters (malformed URLs, invalid ratios)
- **IOError**: I/O operations failed
- **RuntimeError**: xdelta3 binary not found or failed

### Exception Handling Example

```python
from deltaglider import create_client

client = create_client()

try:
    summary = client.upload("app.zip", "s3://bucket/path/")

except FileNotFoundError as e:
    print(f"File not found: {e}")

except PermissionError as e:
    print(f"Permission denied: {e}")
    print("Check AWS credentials and S3 bucket permissions")

except ValueError as e:
    print(f"Invalid parameters: {e}")

except RuntimeError as e:
    print(f"System error: {e}")
    print("Ensure xdelta3 is installed: apt-get install xdelta3")

except Exception as e:
    print(f"Unexpected error: {e}")
    # Log for investigation
    import traceback
    traceback.print_exc()
```

## Environment Variables

DeltaGlider respects these environment variables:

### AWS Configuration

- **AWS_ACCESS_KEY_ID**: AWS access key
- **AWS_SECRET_ACCESS_KEY**: AWS secret key
- **AWS_DEFAULT_REGION**: AWS region (default: us-east-1)
- **AWS_ENDPOINT_URL**: Custom S3 endpoint (for MinIO/R2)
- **AWS_PROFILE**: AWS profile to use

### DeltaGlider Configuration

- **DG_LOG_LEVEL**: Logging level (DEBUG, INFO, WARNING, ERROR)
- **DG_MAX_RATIO**: Default maximum delta ratio

**Note**: Cache is automatically managed (ephemeral, process-isolated) and requires no configuration.

### Example

```bash
# Configure for MinIO
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# Configure DeltaGlider
export DG_LOG_LEVEL=DEBUG
export DG_MAX_RATIO=0.3

# Now use normally (cache managed automatically)
python my_script.py
```

## Thread Safety

DeltaGlider clients are thread-safe for read operations but should not be shared across threads for write operations. For multi-threaded applications:

```python
import threading
from deltaglider import create_client

# Create separate client per thread
def worker(file_path, s3_url):
    client = create_client()  # Each thread gets its own client
    summary = client.upload(file_path, s3_url)
    print(f"Thread {threading.current_thread().name}: {summary.savings_percent:.0f}%")

# Create threads
threads = []
for i, (file, url) in enumerate(files_to_upload):
    t = threading.Thread(target=worker, args=(file, url), name=f"Worker-{i}")
    threads.append(t)
    t.start()

# Wait for completion
for t in threads:
    t.join()
```

## Performance Considerations

### Upload Performance

- **First file**: No compression overhead (becomes reference)
- **Similar files**: 3-4 files/second with compression
- **Network bound**: Limited by S3 upload speed
- **CPU bound**: xdelta3 compression for large files

### Download Performance

- **Direct files**: Limited by S3 download speed
- **Delta files**: <100ms reconstruction overhead
- **Cache hits**: Near-instant for cached references

### Optimization Tips

1. **Group related files**: Upload similar files to same prefix
2. **Batch operations**: Use concurrent uploads for independent files
3. **Cache management**: Don't clear cache during operations
4. **Compression threshold**: Tune `max_ratio` for your use case
5. **Network optimization**: Use S3 Transfer Acceleration if available

## Logging

DeltaGlider uses Python's standard logging framework:

```python
import logging

# Configure logging before creating client
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deltaglider.log'),
        logging.StreamHandler()
    ]
)

# Create client (will use configured logging)
client = create_client(log_level="DEBUG")
```

### Log Levels

- **DEBUG**: Detailed operations, xdelta3 commands
- **INFO**: Normal operations, compression statistics
- **WARNING**: Non-critical issues, fallbacks
- **ERROR**: Operation failures, exceptions

## Version Compatibility

- **Python**: 3.11 or higher required
- **boto3**: 1.35.0 or higher
- **xdelta3**: System binary required
- **S3 API**: Compatible with S3 API v4

## Support

- **GitHub Issues**: [github.com/beshu-tech/deltaglider/issues](https://github.com/beshu-tech/deltaglider/issues)
- **Documentation**: [github.com/beshu-tech/deltaglider](https://github.com/beshu-tech/deltaglider)
- **PyPI Package**: [pypi.org/project/deltaglider](https://pypi.org/project/deltaglider)