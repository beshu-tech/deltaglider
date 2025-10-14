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

DeltaGlider is a drop-in S3 replacement that may achieve 99.9% size reduction for versioned compressed artifacts, backups, and release archives through intelligent binary delta compression (via xdelta3).

## The Problem We Solved

You're storing hundreds of versions of your software releases. Each 100MB build differs by <1% from the previous version. You're paying to store 100GB of what's essentially 100MB of unique data.

Sound familiar?

## Real-World Impact

From our [ReadOnlyREST case study](docs/case-study-readonlyrest.md):
- **Before**: 201,840 files, 3.96TB storage, $1,120/year
- **After**: Same files, 4.9GB storage, $1.32/year
- **Compression**: 99.9% (not a typo)
- **Integration time**: 5 minutes

## Quick Start

The quickest way to start is using the GUI
* https://github.com/sscarduzio/dg_commander/

### CLI Installation

```bash
# Via pip (Python 3.11+)
pip install deltaglider

# Via uv (faster)
uv pip install deltaglider

# Via Docker
docker run -v ~/.aws:/root/.aws deltaglider/deltaglider --help
```

### Docker Usage

DeltaGlider provides a secure, production-ready Docker image with encryption always enabled:

```bash
# Basic usage with AWS credentials from environment
docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  deltaglider/deltaglider ls s3://my-bucket/

# Mount AWS credentials
docker run -v ~/.aws:/root/.aws:ro \
  deltaglider/deltaglider cp file.zip s3://releases/

# Use memory cache for ephemeral CI/CD pipelines (faster)
docker run -e DG_CACHE_BACKEND=memory \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  deltaglider/deltaglider sync ./dist/ s3://releases/v1.0.0/

# Configure memory cache size (default: 100MB)
docker run -e DG_CACHE_BACKEND=memory \
  -e DG_CACHE_MEMORY_SIZE_MB=500 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  deltaglider/deltaglider cp large-file.zip s3://releases/

# Use MinIO or custom S3 endpoint
docker run -e AWS_ENDPOINT_URL=http://minio:9000 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  deltaglider/deltaglider ls s3://test-bucket/

# Persistent encryption key for cross-container cache sharing
# (Only needed if sharing cache across containers via volume mount)
docker run -v /shared-cache:/tmp/.deltaglider \
  -e DG_CACHE_ENCRYPTION_KEY=$(openssl rand -base64 32) \
  deltaglider/deltaglider cp file.zip s3://releases/
```

**Environment Variables**:
- `DG_LOG_LEVEL`: Logging level (default: `INFO`, options: `DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `DG_MAX_RATIO`: Maximum delta/file ratio (default: `0.5`, range: `0.0-1.0`) - [📖 Complete Guide](docs/DG_MAX_RATIO.md)
- `DG_CACHE_BACKEND`: Cache backend (default: `filesystem`, options: `filesystem`, `memory`)
- `DG_CACHE_MEMORY_SIZE_MB`: Memory cache size in MB (default: `100`)
- `DG_CACHE_ENCRYPTION_KEY`: Optional base64-encoded encryption key for cross-process cache sharing
- `DG_DISABLE_EC2_DETECTION`: Disable EC2 instance detection (default: `false`, set to `true` to disable)
- `AWS_ENDPOINT_URL`: S3 endpoint URL (default: AWS S3)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_DEFAULT_REGION`: AWS region (default: `us-east-1`)

> **💡 Tip**: `DG_MAX_RATIO` is a powerful tuning parameter. See the [DG_MAX_RATIO guide](docs/DG_MAX_RATIO.md) to learn how to optimize compression for your use case.

**Security Notes**:
- Encryption is **always enabled** (cannot be disabled)
- Each container gets ephemeral encryption keys for maximum security
- Corrupted cache files are automatically deleted
- Use `DG_CACHE_ENCRYPTION_KEY` only for persistent cache sharing (store securely)

### Basic Usage

```bash
# Upload a file (automatic delta compression)
deltaglider cp my-app-v1.0.0.zip s3://releases/

# Download a file (automatic delta reconstruction)
deltaglider cp s3://releases/my-app-v1.0.0.zip ./downloaded.zip

# List objects
deltaglider ls s3://releases/

# Sync directories
deltaglider sync ./dist/ s3://releases/v1.0.0/

# Migrate existing S3 bucket to DeltaGlider-compressed storage
deltaglider migrate s3://old-bucket/ s3://new-bucket/
```

**That's it!** DeltaGlider automatically detects similar files and applies 99%+ compression. For more commands and options, see [CLI Reference](#cli-reference).

## Core Concepts

### How It Works

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

DeltaGlider stores the first file as a reference and subsequent similar files as tiny deltas (differences). When you download, it reconstructs the original file perfectly using the reference + delta.

### Intelligent File Type Detection

DeltaGlider automatically detects file types and applies the optimal strategy:

| File Type | Strategy | Typical Compression | Why It Works |
|-----------|----------|---------------------|--------------|
| `.zip`, `.tar`, `.gz` | Binary delta | 99%+ for similar versions | Archive structure remains consistent between versions |
| `.dmg`, `.deb`, `.rpm` | Binary delta | 95%+ for similar versions | Package formats with predictable structure |
| `.jar`, `.war`, `.ear` | Binary delta | 90%+ for similar builds | Java archives with mostly unchanged classes |
| `.exe`, `.dll`, `.so` | Direct upload | 0% (no delta benefit) | Compiled code changes unpredictably |
| `.txt`, `.json`, `.xml` | Direct upload | 0% (use gzip instead) | Text files benefit more from standard compression |
| `.sha1`, `.sha512`, `.md5` | Direct upload | 0% (already minimal) | Hash files are unique by design |

### Key Features

- **AWS CLI Replacement**: Same commands as `aws s3` with automatic compression
- **boto3-Compatible SDK**: Works with existing boto3 code with minimal changes
- **Zero Configuration**: No databases, no manifest files, no complex setup
- **Data Integrity**: SHA256 verification on every operation
- **S3 Compatible**: Works with AWS S3, MinIO, Cloudflare R2, and any S3-compatible storage

## CLI Reference

### All Commands

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

# Get bucket statistics with intelligent S3-based caching
deltaglider stats my-bucket                       # Quick stats (~100ms with cache)
deltaglider stats s3://my-bucket                  # Also accepts s3:// format
deltaglider stats s3://my-bucket/                 # With or without trailing slash
deltaglider stats my-bucket --sampled             # Balanced (one sample per deltaspace)
deltaglider stats my-bucket --detailed            # Most accurate (slower, all metadata)
deltaglider stats my-bucket --refresh             # Force cache refresh
deltaglider stats my-bucket --no-cache            # Skip caching entirely
deltaglider stats my-bucket --json                # JSON output for automation

# Migrate existing S3 buckets to DeltaGlider compression
deltaglider migrate s3://old-bucket/ s3://new-bucket/         # Interactive migration
deltaglider migrate s3://old-bucket/ s3://new-bucket/ --yes   # Skip confirmation
deltaglider migrate --dry-run s3://old-bucket/ s3://new/      # Preview migration
deltaglider migrate s3://bucket/v1/ s3://bucket/v2/           # Migrate prefixes

# Works with MinIO, R2, and S3-compatible storage
deltaglider cp file.zip s3://bucket/ --endpoint-url http://localhost:9000
```

### Command Flags

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

### CI/CD Integration

#### GitHub Actions

```yaml
- name: Upload Release with 99% compression
  run: |
    pip install deltaglider
    deltaglider cp dist/*.zip s3://releases/${{ github.ref_name }}/
    # Or recursive for entire directories
    deltaglider cp -r dist/ s3://releases/${{ github.ref_name }}/
```

#### Daily Backup Script

```bash
#!/bin/bash
# Daily backup with automatic deduplication
tar -czf backup-$(date +%Y%m%d).tar.gz /data
deltaglider cp backup-*.tar.gz s3://backups/
# Only changes are stored, not full backup

# Clean up old backups
deltaglider rm -r s3://backups/2023/
```

## Python SDK

**[📚 Full SDK Documentation](docs/sdk/README.md)** | **[API Reference](docs/sdk/api.md)** | **[Examples](docs/sdk/examples.md)** | **[boto3 Compatibility Guide](BOTO3_COMPATIBILITY.md)**

### boto3-Compatible API (Recommended)

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

# Smart list_objects with optimized performance
response = client.list_objects(Bucket='releases', Prefix='v2.0.0/')
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

# Delete and inspect objects
client.delete_object(Bucket='releases', Key='old-version.zip')
client.head_object(Bucket='releases', Key='v2.0.0/my-app.zip')
```

### Bucket Management

**No boto3 required!** DeltaGlider provides complete bucket management:

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

See [examples/bucket_management.py](examples/bucket_management.py) for complete example.

### Simple API (Alternative)

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

### Real-World Examples

#### Software Release Storage

```python
from deltaglider import create_client

client = create_client()

# Upload multiple versions
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
```

#### Automated Database Backup

```python
from datetime import datetime
from deltaglider import create_client

client = create_client(endpoint_url="http://minio.internal:9000")

def backup_database():
    """Daily database backup with automatic deduplication."""
    date = datetime.now().strftime("%Y%m%d")
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

    # Check compression effectiveness
    if 'DeltaGliderInfo' in response:
        info = response['DeltaGliderInfo']
        if info['DeltaRatio'] > 0.1:
            print(f"Warning: Low compression ({info['SavingsPercent']:.0f}%), "
                  "database might have significant changes")
        print(f"Backup stored: {info['StoredSizeMB']:.1f}MB "
              f"(compressed from {info['OriginalSizeMB']:.1f}MB)")

backup_database()
```

For more examples and detailed API documentation, see the [SDK Documentation](docs/sdk/README.md).

## Performance & Benchmarks

### Real-World Results

Testing with 513 Elasticsearch plugin releases (82.5MB each):

```
Original size:       42.3 GB
DeltaGlider size:    115 MB
Compression:         99.7%
Upload speed:        3-4 files/second
Download speed:      <100ms reconstruction
```

### The Math

For `N` versions of a `S` MB file with `D%` difference between versions:

**Traditional S3**: `N × S` MB
**DeltaGlider**: `S + (N-1) × S × D%` MB

Example: 100 versions of 100MB files with 1% difference:
- **Traditional**: 10,000 MB
- **DeltaGlider**: 199 MB
- **Savings**: 98%

### Comparison

| Solution | Compression | Speed | Integration | Cost |
|----------|------------|-------|-------------|------|
| **DeltaGlider** | 99%+ | Fast | Drop-in | Open source |
| S3 Versioning | 0% | Native | Built-in | $$ per version |
| Deduplication | 30-50% | Slow | Complex | Enterprise $$$ |
| Git LFS | Good | Slow | Git-only | $ per GB |
| Restic/Borg | 80-90% | Medium | Backup-only | Open source |

## Architecture & Technical Deep Dive

### Why xdelta3 Excels at Archive Compression

Traditional diff algorithms (like `diff` or `git diff`) work line-by-line on text files. Binary diff tools like `bsdiff` or `courgette` are optimized for executables. But **xdelta3** is uniquely suited for compressed archives because:

1. **Block-level matching**: xdelta3 uses a rolling hash algorithm to find matching byte sequences at any offset, not just line boundaries. This is crucial for archives where small file changes can shift all subsequent byte positions.

2. **Large window support**: xdelta3 can use reference windows up to 2GB, allowing it to find matches even when content has moved significantly within the archive. Other delta algorithms typically use much smaller windows (64KB-1MB).

3. **Compression-aware**: When you update one file in a ZIP/TAR archive, the archive format itself remains largely identical - same compression dictionary, same structure. xdelta3 preserves these similarities while other algorithms might miss them.

4. **Format agnostic**: Unlike specialized tools (e.g., `courgette` for Chrome updates), xdelta3 works on raw bytes without understanding the file format, making it perfect for any archive type.

#### Real-World Example

When you rebuild a JAR file with one class changed:
- **Text diff**: 100% different (it's binary data!)
- **bsdiff**: ~30-40% of original size (optimized for executables, not archives)
- **xdelta3**: ~0.1-1% of original size (finds the unchanged parts regardless of position)

This is why DeltaGlider achieves 99%+ compression on versioned archives - xdelta3 can identify that 99% of the archive structure and content remains identical between versions.

### System Architecture

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

### When to Use DeltaGlider

✅ **Perfect for:**
- Software releases and versioned artifacts
- Container images and layers
- Database backups and snapshots
- Machine learning model checkpoints
- Game assets and updates
- Any versioned binary data

❌ **Not ideal for:**
- Already compressed **unique** files
- Streaming or multimedia files
- Frequently changing unstructured data
- Files smaller than 1MB

## Migration from AWS CLI

Migrating from `aws s3` to `deltaglider` is as simple as changing the command name:

| AWS CLI | DeltaGlider | Compression Benefit |
|---------|------------|---------------------|
| `aws s3 cp file.zip s3://bucket/` | `deltaglider cp file.zip s3://bucket/` | ✅ 99% for similar files |
| `aws s3 cp -r dir/ s3://bucket/` | `deltaglider cp -r dir/ s3://bucket/` | ✅ 99% for archives |
| `aws s3 ls s3://bucket/` | `deltaglider ls s3://bucket/` | - |
| `aws s3 rm s3://bucket/file` | `deltaglider rm s3://bucket/file` | - |
| `aws s3 sync dir/ s3://bucket/` | `deltaglider sync dir/ s3://bucket/` | ✅ 99% incremental |

### Migrating Existing S3 Buckets

DeltaGlider provides a dedicated `migrate` command to compress your existing S3 data:

```bash
# Migrate an entire bucket
deltaglider migrate s3://old-bucket/ s3://compressed-bucket/

# Migrate a prefix (preserves prefix structure by default)
deltaglider migrate s3://bucket/releases/ s3://bucket/archive/
# Result: s3://bucket/archive/releases/ contains the files

# Migrate without preserving source prefix
deltaglider migrate --no-preserve-prefix s3://bucket/v1/ s3://bucket/archive/
# Result: Files go directly into s3://bucket/archive/

# Preview migration (dry run)
deltaglider migrate --dry-run s3://old/ s3://new/

# Skip confirmation prompt
deltaglider migrate --yes s3://old/ s3://new/

# Exclude certain file patterns
deltaglider migrate --exclude "*.log" s3://old/ s3://new/
```

**Key Features:**
- **Resume Support**: Migration automatically skips files that already exist in the destination
- **Progress Tracking**: Shows real-time migration progress and statistics
- **Safety First**: Interactive confirmation shows file count before starting
- **EC2 Cost Optimization**: Automatically detects EC2 instance region and warns about cross-region charges
  - ✅ Green checkmark when regions align (no extra charges)
  - ℹ️ INFO when auto-detected mismatch (suggests optimal region)
  - ⚠️ WARNING when user explicitly set wrong `--region` (expect data transfer costs)
  - Disable with `DG_DISABLE_EC2_DETECTION=true` if needed
- **AWS Region Transparency**: Displays the actual AWS region being used
- **Prefix Preservation**: By default, source prefix is preserved in destination (use `--no-preserve-prefix` to disable)
- **S3-to-S3 Transfer**: Both regular S3 and DeltaGlider buckets supported

**Prefix Preservation Examples:**
- `s3://src/data/` → `s3://dest/` creates `s3://dest/data/`
- `s3://src/a/b/c/` → `s3://dest/x/` creates `s3://dest/x/c/`
- Use `--no-preserve-prefix` to place files directly in destination without the source prefix

The migration preserves all file names and structure while applying DeltaGlider's compression transparently.

## Production Ready

- ✅ **Battle tested**: 200K+ files in production
- ✅ **Data integrity**: SHA256 verification on every operation
- ✅ **Cost optimization**: Automatic EC2 region detection warns about cross-region charges - [📖 EC2 Detection Guide](docs/EC2_REGION_DETECTION.md)
- ✅ **S3 compatible**: Works with AWS, MinIO, Cloudflare R2, etc.
- ✅ **Atomic operations**: No partial states
- ✅ **Concurrent safe**: Multiple clients supported
- ✅ **Thoroughly tested**: 99 integration/unit tests, comprehensive test coverage
- ✅ **Type safe**: Full mypy type checking, zero type errors
- ✅ **Code quality**: Automated linting with ruff, clean codebase

## Development

```bash
# Clone the repo
git clone https://github.com/beshu-tech/deltaglider
cd deltaglider

# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests (99 integration/unit tests)
uv run pytest

# Run quality checks
uv run ruff check src/        # Linting
uv run mypy src/              # Type checking

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
