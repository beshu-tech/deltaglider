# DeltaGlider

[![PyPI version](https://badge.fury.io/py/deltaglider.svg)](https://pypi.org/project/deltaglider/)
[![GitHub Repository](https://img.shields.io/badge/github-deltaglider-blue.svg)](https://github.com/beshu-tech/deltaglider)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![xdelta3](https://img.shields.io/badge/powered%20by-xdelta3-green.svg)](https://github.com/jmacd/xdelta)

<div align="center">
  <img src="https://github.com/beshu-tech/deltaglider/raw/main/docs/deltaglider.png" alt="DeltaGlider Logo" width="500"/>
</div>

**Store 4TB of similar files in 5GB. No, that's not a typo.**

DeltaGlider is a drop-in S3 replacement that achieves 99.9% compression for versioned artifacts, backups, and release archives through intelligent binary delta compression.

## The Problem We Solved

You're storing hundreds of versions of your releases. Each 100MB build differs by <1% from the previous version. You're paying to store 100GB of what's essentially 100MB of unique data.

Sound familiar?

## Real-World Impact

From our [ReadOnlyREST case study](docs/case-study-readonlyrest.md):
- **Before**: 201,840 files, 3.96TB storage, $1,120/year
- **After**: Same files, 4.9GB storage, $1.32/year
- **Compression**: 99.9% (not a typo)
- **Integration time**: 5 minutes

## How It Works

```
Traditional S3:
  v1.0.0.zip (100MB) → S3: 100MB
  v1.0.1.zip (100MB) → S3: 100MB (200MB total)
  v1.0.2.zip (100MB) → S3: 100MB (300MB total)

With DeltaGlider:
  v1.0.0.zip (100MB) → S3: 100MB reference + 0KB delta
  v1.0.1.zip (100MB) → S3: 98KB delta (100.1MB total)
  v1.0.2.zip (100MB) → S3: 97KB delta (100.3MB total)
```

## Quick Start

### Installation

```bash
# Via pip (Python 3.11+)
pip install deltaglider

# Via uv (faster)
uv pip install deltaglider

# Via Docker
docker run -v ~/.aws:/root/.aws deltaglider/deltaglider --help
```

### AWS S3 Compatible Commands

DeltaGlider is a **drop-in replacement** for AWS S3 CLI with automatic delta compression:

```bash
# Copy files to/from S3 (automatic delta compression for archives)
deltaglider cp my-app-v1.0.0.zip s3://releases/
deltaglider cp s3://releases/my-app-v1.0.0.zip ./downloaded.zip

# Recursive directory operations
deltaglider cp -r ./dist/ s3://releases/v1.0.0/
deltaglider cp -r s3://releases/v1.0.0/ ./local-copy/

# List buckets and objects
deltaglider ls                                    # List all buckets
deltaglider ls s3://releases/                     # List objects
deltaglider ls -r s3://releases/                  # Recursive listing
deltaglider ls -h --summarize s3://releases/      # Human-readable with summary

# Remove objects
deltaglider rm s3://releases/old-version.zip      # Remove single object
deltaglider rm -r s3://releases/old/              # Recursive removal
deltaglider rm --dryrun s3://releases/test.zip    # Preview deletion

# Sync directories (only transfers changes)
deltaglider sync ./local-dir/ s3://releases/      # Sync to S3
deltaglider sync s3://releases/ ./local-backup/   # Sync from S3
deltaglider sync --delete ./src/ s3://backup/     # Mirror exactly
deltaglider sync --exclude "*.log" ./src/ s3://backup/  # Exclude patterns

# Works with MinIO, R2, and S3-compatible storage
deltaglider cp file.zip s3://bucket/ --endpoint-url http://localhost:9000
```

## Why xdelta3 Excels at Archive Compression

Traditional diff algorithms (like `diff` or `git diff`) work line-by-line on text files. Binary diff tools like `bsdiff` or `courgette` are optimized for executables. But **xdelta3** is uniquely suited for compressed archives because:

1. **Block-level matching**: xdelta3 uses a rolling hash algorithm to find matching byte sequences at any offset, not just line boundaries. This is crucial for archives where small file changes can shift all subsequent byte positions.

2. **Large window support**: xdelta3 can use reference windows up to 2GB, allowing it to find matches even when content has moved significantly within the archive. Other delta algorithms typically use much smaller windows (64KB-1MB).

3. **Compression-aware**: When you update one file in a ZIP/TAR archive, the archive format itself remains largely identical - same compression dictionary, same structure. xdelta3 preserves these similarities while other algorithms might miss them.

4. **Format agnostic**: Unlike specialized tools (e.g., `courgette` for Chrome updates), xdelta3 works on raw bytes without understanding the file format, making it perfect for any archive type.

### Real-World Example
When you rebuild a JAR file with one class changed:
- **Text diff**: 100% different (it's binary data!)
- **bsdiff**: ~30-40% of original size (optimized for executables, not archives)
- **xdelta3**: ~0.1-1% of original size (finds the unchanged parts regardless of position)

This is why DeltaGlider achieves 99%+ compression on versioned archives - xdelta3 can identify that 99% of the archive structure and content remains identical between versions.

## Intelligent File Type Detection

DeltaGlider automatically detects file types and applies the optimal strategy:

| File Type | Strategy | Typical Compression | Why It Works |
|-----------|----------|-------------------|--------------|
| `.zip`, `.tar`, `.gz` | Binary delta | 99%+ for similar versions | Archive structure remains consistent between versions |
| `.dmg`, `.deb`, `.rpm` | Binary delta | 95%+ for similar versions | Package formats with predictable structure |
| `.jar`, `.war`, `.ear` | Binary delta | 90%+ for similar builds | Java archives with mostly unchanged classes |
| `.exe`, `.dll`, `.so` | Direct upload | 0% (no delta benefit) | Compiled code changes unpredictably |
| `.txt`, `.json`, `.xml` | Direct upload | 0% (use gzip instead) | Text files benefit more from standard compression |
| `.sha1`, `.sha512`, `.md5` | Direct upload | 0% (already minimal) | Hash files are unique by design |

## Performance Benchmarks

Testing with real software releases:

```python
# 513 Elasticsearch plugin releases (82.5MB each)
Original size:       42.3 GB
DeltaGlider size:    115 MB
Compression:         99.7%
Upload speed:        3-4 files/second
Download speed:      <100ms reconstruction
```

## Integration Examples

### Drop-in AWS CLI Replacement

```bash
# Before (aws-cli)
aws s3 cp release-v2.0.0.zip s3://releases/
aws s3 cp --recursive ./build/ s3://releases/v2.0.0/
aws s3 ls s3://releases/
aws s3 rm s3://releases/old-version.zip

# After (deltaglider) - Same commands, 99% less storage!
deltaglider cp release-v2.0.0.zip s3://releases/
deltaglider cp -r ./build/ s3://releases/v2.0.0/
deltaglider ls s3://releases/
deltaglider rm s3://releases/old-version.zip
```

### CI/CD Pipeline (GitHub Actions)

```yaml
- name: Upload Release with 99% compression
  run: |
    pip install deltaglider
    # Use AWS S3 compatible syntax
    deltaglider cp dist/*.zip s3://releases/${{ github.ref_name }}/

    # Or use recursive for entire directories
    deltaglider cp -r dist/ s3://releases/${{ github.ref_name }}/
```

### Backup Script

```bash
#!/bin/bash
# Daily backup with automatic deduplication
tar -czf backup-$(date +%Y%m%d).tar.gz /data
deltaglider cp backup-*.tar.gz s3://backups/
# Only changes are stored, not full backup

# List backups with human-readable sizes
deltaglider ls -h s3://backups/

# Clean up old backups
deltaglider rm -r s3://backups/2023/
```

### Python SDK - boto3-Compatible API

**[📚 Full SDK Documentation](docs/sdk/README.md)** | **[API Reference](docs/sdk/api.md)** | **[Examples](docs/sdk/examples.md)** | **[boto3 Compatibility Guide](BOTO3_COMPATIBILITY.md)**

#### Quick Start - boto3 Compatible API (Recommended)

DeltaGlider provides a **boto3-compatible API** for core S3 operations (21 methods covering 80% of use cases):

```python
from deltaglider import create_client

# Drop-in replacement for boto3.client('s3')
client = create_client()  # Uses AWS credentials automatically

# Identical to boto3 S3 API - just works with 99% compression!
response = client.put_object(
    Bucket='releases',
    Key='v2.0.0/my-app.zip',
    Body=open('my-app-v2.0.0.zip', 'rb')
)
print(f"Stored with ETag: {response['ETag']}")

# Standard boto3 get_object - handles delta reconstruction automatically
response = client.get_object(Bucket='releases', Key='v2.0.0/my-app.zip')
with open('downloaded.zip', 'wb') as f:
    f.write(response['Body'].read())

# Smart list_objects with optimized performance (NEW!)
# Fast listing (default) - no metadata fetching, ~50ms for 1000 objects
response = client.list_objects(Bucket='releases', Prefix='v2.0.0/')

# Paginated listing for large buckets
response = client.list_objects(Bucket='releases', MaxKeys=100)
while response.is_truncated:
    response = client.list_objects(
        Bucket='releases',
        MaxKeys=100,
        ContinuationToken=response.next_continuation_token
    )

# Get bucket statistics with smart defaults
stats = client.get_bucket_stats('releases')  # Quick stats (50ms)
stats = client.get_bucket_stats('releases', detailed_stats=True)  # With compression metrics

client.delete_object(Bucket='releases', Key='old-version.zip')
client.head_object(Bucket='releases', Key='v2.0.0/my-app.zip')

# Bucket management - no boto3 needed!
client.create_bucket(Bucket='my-new-bucket')
client.list_buckets()
client.delete_bucket(Bucket='my-new-bucket')
```

#### Bucket Management (NEW!)

**No boto3 required!** DeltaGlider now provides complete bucket management:

```python
from deltaglider import create_client

client = create_client()

# Create buckets
client.create_bucket(Bucket='my-releases')

# Create bucket in specific region (AWS only)
client.create_bucket(
    Bucket='my-regional-bucket',
    CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
)

# List all buckets
response = client.list_buckets()
for bucket in response['Buckets']:
    print(f"{bucket['Name']} - {bucket['CreationDate']}")

# Delete bucket (must be empty)
client.delete_bucket(Bucket='my-old-bucket')
```

**Benefits:**
- ✅ No need to import boto3 separately for bucket operations
- ✅ Consistent API with DeltaGlider object operations
- ✅ Works with AWS S3, MinIO, and S3-compatible storage
- ✅ Idempotent operations (safe to retry)

See [examples/bucket_management.py](examples/bucket_management.py) for complete example.

#### Simple API (Alternative)

For simpler use cases, DeltaGlider also provides a streamlined API:

```python
from deltaglider import create_client

client = create_client()

# Simple upload with automatic compression detection
summary = client.upload("my-app-v2.0.0.zip", "s3://releases/v2.0.0/")
print(f"Compressed from {summary.original_size_mb:.1f}MB to {summary.stored_size_mb:.1f}MB")
print(f"Saved {summary.savings_percent:.0f}% storage space")

# Simple download with automatic delta reconstruction
client.download("s3://releases/v2.0.0/my-app-v2.0.0.zip", "local-app.zip")
```

#### Real-World Example: Software Release Storage with boto3 API

```python
from deltaglider import create_client

# Works exactly like boto3, but with 99% compression!
client = create_client()

# Upload multiple versions using boto3-compatible API
versions = ["v1.0.0", "v1.0.1", "v1.0.2", "v1.1.0"]
for version in versions:
    with open(f"dist/my-app-{version}.zip", 'rb') as f:
        response = client.put_object(
            Bucket='releases',
            Key=f'{version}/my-app-{version}.zip',
            Body=f,
            Metadata={'version': version, 'build': 'production'}
        )

    # Check compression stats (DeltaGlider extension)
    if 'DeltaGliderInfo' in response:
        info = response['DeltaGliderInfo']
        if info.get('IsDelta'):
            print(f"{version}: Stored as {info['StoredSizeMB']:.1f}MB delta "
                  f"(saved {info['SavingsPercent']:.0f}%)")
        else:
            print(f"{version}: Stored as reference ({info['OriginalSizeMB']:.1f}MB)")

# Result:
# v1.0.0: Stored as reference (100.0MB)
# v1.0.1: Stored as 0.2MB delta (saved 99.8%)
# v1.0.2: Stored as 0.3MB delta (saved 99.7%)
# v1.1.0: Stored as 5.2MB delta (saved 94.8%)

# Download using standard boto3 API
response = client.get_object(Bucket='releases', Key='v1.1.0/my-app-v1.1.0.zip')
with open('my-app-latest.zip', 'wb') as f:
    f.write(response['Body'].read())
```

#### Advanced Example: Automated Backup with boto3 API

```python
from datetime import datetime
from deltaglider import create_client

# Works with any S3-compatible storage
client = create_client(endpoint_url="http://minio.internal:9000")

def backup_database():
    """Daily database backup with automatic deduplication using boto3 API."""
    date = datetime.now().strftime("%Y%m%d")

    # Create database dump
    dump_file = f"backup-{date}.sql.gz"

    # Upload using boto3-compatible API
    with open(dump_file, 'rb') as f:
        response = client.put_object(
            Bucket='backups',
            Key=f'postgres/{date}/{dump_file}',
            Body=f,
            Tagging='type=daily&database=production',
            Metadata={'date': date, 'source': 'production'}
        )

    # Check compression effectiveness (DeltaGlider extension)
    if 'DeltaGliderInfo' in response:
        info = response['DeltaGliderInfo']
        if info['DeltaRatio'] > 0.1:  # If delta is >10% of original
            print(f"Warning: Low compression ({info['SavingsPercent']:.0f}%), "
                  "database might have significant changes")
        print(f"Backup stored: {info['StoredSizeMB']:.1f}MB "
              f"(compressed from {info['OriginalSizeMB']:.1f}MB)")

    # List recent backups using boto3 API
    response = client.list_objects(
        Bucket='backups',
        Prefix='postgres/',
        MaxKeys=30
    )

    # Clean up old backups
    for obj in response.get('Contents', []):
        # Parse date from key
        obj_date = obj['Key'].split('/')[1]
        if days_old(obj_date) > 30:
            client.delete_object(Bucket='backups', Key=obj['Key'])

# Run backup
backup_database()
```

For more examples and detailed API documentation, see the [SDK Documentation](docs/sdk/README.md).

## Migration from AWS CLI

Migrating from `aws s3` to `deltaglider` is as simple as changing the command name:

| AWS CLI | DeltaGlider | Compression Benefit |
|---------|------------|-------------------|
| `aws s3 cp file.zip s3://bucket/` | `deltaglider cp file.zip s3://bucket/` | ✅ 99% for similar files |
| `aws s3 cp -r dir/ s3://bucket/` | `deltaglider cp -r dir/ s3://bucket/` | ✅ 99% for archives |
| `aws s3 ls s3://bucket/` | `deltaglider ls s3://bucket/` | - |
| `aws s3 rm s3://bucket/file` | `deltaglider rm s3://bucket/file` | - |
| `aws s3 sync dir/ s3://bucket/` | `deltaglider sync dir/ s3://bucket/` | ✅ 99% incremental |

### Compatibility Flags

```bash
# All standard AWS flags work
deltaglider cp file.zip s3://bucket/ \
  --endpoint-url http://localhost:9000 \
  --profile production \
  --region us-west-2

# DeltaGlider-specific flags
deltaglider cp file.zip s3://bucket/ \
  --no-delta              # Disable compression for specific files
  --max-ratio 0.8         # Only use delta if compression > 20%
```

## Architecture

DeltaGlider uses a clean hexagonal architecture:

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Your App  │────▶│ DeltaGlider  │────▶│  S3/MinIO   │
│   (CLI/SDK) │     │    Core      │     │   Storage   │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                    ┌──────▼───────┐
                    │ Local Cache  │
                    │ (References) │
                    └──────────────┘
```

**Key Components:**
- **Binary diff engine**: xdelta3 for optimal compression
- **Intelligent routing**: Automatic file type detection
- **Integrity verification**: SHA256 on every operation
- **Local caching**: Fast repeated operations
- **Zero dependencies**: No database, no manifest files

## When to Use DeltaGlider

✅ **Perfect for:**
- Software releases and versioned artifacts
- Container images and layers
- Database backups and snapshots
- Machine learning model checkpoints
- Game assets and updates
- Any versioned binary data

❌ **Not ideal for:**
- Already compressed unique files
- Streaming media files
- Frequently changing unstructured data
- Files smaller than 1MB

## Comparison

| Solution | Compression | Speed | Integration | Cost |
|----------|------------|-------|-------------|------|
| **DeltaGlider** | 99%+ | Fast | Drop-in | Open source |
| S3 Versioning | 0% | Native | Built-in | $$ per version |
| Deduplication | 30-50% | Slow | Complex | Enterprise $$$ |
| Git LFS | Good | Slow | Git-only | $ per GB |
| Restic/Borg | 80-90% | Medium | Backup-only | Open source |

## Production Ready

- ✅ **Battle tested**: 200K+ files in production
- ✅ **Data integrity**: SHA256 verification on every operation
- ✅ **S3 compatible**: Works with AWS, MinIO, Cloudflare R2, etc.
- ✅ **Atomic operations**: No partial states
- ✅ **Concurrent safe**: Multiple clients supported
- ✅ **Well tested**: 95%+ code coverage

## Development

```bash
# Clone the repo
git clone https://github.com/beshu-tech/deltaglider
cd deltaglider

# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest

# Run with local MinIO
docker-compose up -d
export AWS_ENDPOINT_URL=http://localhost:9000
deltaglider cp test.zip s3://test/
```

## FAQ

**Q: What if my reference file gets corrupted?**
A: Every operation includes SHA256 verification. Corruption is detected immediately.

**Q: How fast is reconstruction?**
A: Sub-100ms for typical files. The delta is applied in-memory using xdelta3.

**Q: Can I use this with existing S3 data?**
A: Yes! DeltaGlider can start optimizing new uploads immediately. Old data remains accessible.

**Q: What's the overhead for unique files?**
A: Zero. Files without similarity are uploaded directly.

**Q: Is this compatible with S3 encryption?**
A: Yes, DeltaGlider respects all S3 settings including SSE, KMS, and bucket policies.

## The Math

For `N` versions of a `S` MB file with `D%` difference between versions:

**Traditional S3**: `N × S` MB
**DeltaGlider**: `S + (N-1) × S × D%` MB

Example: 100 versions of 100MB files with 1% difference:
- **Traditional**: 10,000 MB
- **DeltaGlider**: 199 MB
- **Savings**: 98%

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

Key areas we're exploring:
- Cloud-native reference management
- Rust implementation for 10x speed
- Automatic similarity detection
- Multi-threaded delta generation
- WASM support for browser usage

## License

MIT - Use it freely in your projects.

## Success Stories

> "We reduced our artifact storage from 4TB to 5GB. This isn't hyperbole—it's math."
> — [ReadOnlyREST Case Study](docs/case-study-readonlyrest.md)

> "Our CI/CD pipeline now uploads 100x faster. Deploys that took minutes now take seconds."
> — Platform Engineer at [redacted]

> "We were about to buy expensive deduplication storage. DeltaGlider saved us $50K/year."
> — CTO at [stealth startup]

---

**Try it now**: Got versioned files in S3? See your potential savings:

```bash
# Analyze your S3 bucket
deltaglider analyze s3://your-bucket/
# Output: "Potential savings: 95.2% (4.8TB → 237GB)"
```

Built with ❤️ by engineers who were tired of paying to store the same bytes over and over.