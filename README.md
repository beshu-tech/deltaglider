# DeltaGlider

[![GitHub Repository](https://img.shields.io/badge/github-deltaglider-blue.svg)](https://github.com/your-org/deltaglider)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![xdelta3](https://img.shields.io/badge/powered%20by-xdelta3-green.svg)](https://github.com/jmacd/xdelta)

<div align="center">
  <img src="docs/deltaglider.png" alt="DeltaGlider Logo" width="500"/>
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

### Legacy Commands (still supported)

```bash
# Original DeltaGlider commands
deltaglider put my-app-v1.0.0.zip s3://releases/
deltaglider get s3://releases/my-app-v1.0.1.zip
deltaglider verify s3://releases/my-app-v1.0.1.zip.delta
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

### Python SDK

```python
from pathlib import Path
from deltaglider.core import DeltaService, DeltaSpace, ObjectKey
from deltaglider.adapters import (
    S3StorageAdapter,
    XdeltaAdapter,
    Sha256Adapter,
    FsCacheAdapter,
    UtcClockAdapter,
    StdLoggerAdapter,
    NoopMetricsAdapter,
)

# Method 1: Use environment variables (recommended)
# Export these before running your Python script:
# export AWS_ACCESS_KEY_ID=your-access-key
# export AWS_SECRET_ACCESS_KEY=your-secret-key
# export AWS_DEFAULT_REGION=us-east-1
# export AWS_ENDPOINT_URL=http://localhost:9000  # For MinIO

# Method 2: Use AWS profiles
# Configure with: aws configure --profile production
# export AWS_PROFILE=production

# Method 3: Explicit configuration in code
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'your-access-key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your-secret-key'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# For MinIO or other S3-compatible storage
os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:9000'

# Create service with full configuration
def create_service(
    cache_dir="/tmp/.deltaglider/cache",
    log_level="INFO",
    endpoint_url=None,  # Override for MinIO/R2
):
    """Create a configured DeltaService instance."""

    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter(endpoint_url=endpoint_url)
    diff = XdeltaAdapter()
    cache = FsCacheAdapter(Path(cache_dir), hasher)
    clock = UtcClockAdapter()
    logger = StdLoggerAdapter(level=log_level)
    metrics = NoopMetricsAdapter()

    # Create service
    return DeltaService(
        storage=storage,
        diff=diff,
        hasher=hasher,
        cache=cache,
        clock=clock,
        logger=logger,
        metrics=metrics,
        tool_version="deltaglider/0.1.0",
        max_ratio=0.5,  # Only use delta if compression > 50%
    )

# Basic usage
service = create_service()

# Upload a file with automatic delta compression
delta_space = DeltaSpace(bucket="my-releases", prefix="v2.0.0")
summary = service.put(Path("my-app-v2.0.0.zip"), delta_space)

print(f"Operation: {summary.operation}")  # 'create_reference' or 'create_delta'
print(f"Stored at: s3://{summary.bucket}/{summary.key}")
print(f"Original size: {summary.file_size:,} bytes")
if summary.delta_size:
    print(f"Delta size: {summary.delta_size:,} bytes")
    print(f"Compression: {(1 - summary.delta_ratio) * 100:.1f}%")

# Download and reconstruct a file
obj_key = ObjectKey(bucket="my-releases", key="v2.0.0/my-app-v2.0.0.zip.delta")
output_path = Path("downloaded-app.zip")
service.get(obj_key, output_path)

# Verify file integrity
result = service.verify(obj_key)
print(f"Verification: {'✓ Passed' if result.valid else '✗ Failed'}")
if not result.valid:
    print(f"Expected: {result.expected_sha256}")
    print(f"Actual: {result.actual_sha256}")

# For MinIO specifically
minio_service = create_service(
    endpoint_url="http://localhost:9000",
    log_level="DEBUG"  # See detailed operations
)

# For Cloudflare R2
r2_service = create_service(
    endpoint_url="https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com"
)

# Advanced: Control delta behavior
service.should_use_delta("my-app.zip")  # Returns True (archive file)
service.should_use_delta("config.json")  # Returns False (text file)
service.should_use_delta("small.txt")   # Returns False (too small)
```

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
git clone https://github.com/your-org/deltaglider
cd deltaglider

# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest

# Run with local MinIO
docker-compose up -d
export AWS_ENDPOINT_URL=http://localhost:9000
deltaglider put test.zip s3://test/
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