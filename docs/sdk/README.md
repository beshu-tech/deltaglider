# DeltaGlider Python SDK Documentation

The DeltaGlider Python SDK provides a **100% boto3-compatible API** that works as a drop-in replacement for AWS S3 SDK, while achieving 99%+ compression for versioned artifacts through intelligent binary delta compression.

## 🎯 Key Highlights

- **Drop-in boto3 Replacement**: Use your existing boto3 S3 code, just change the import
- **99%+ Compression**: Automatically for versioned files and archives
- **Zero Learning Curve**: If you know boto3, you already know DeltaGlider
- **Full Compatibility**: Works with AWS S3, MinIO, Cloudflare R2, and all S3-compatible storage

## Quick Links

- [Getting Started](getting-started.md) - Installation and first steps
- [Examples](examples.md) - Real-world usage patterns
- [API Reference](api.md) - Complete API documentation
- [Architecture](architecture.md) - How it works under the hood

## Overview

DeltaGlider provides three ways to interact with your S3 storage:

### 1. boto3-Compatible API (Recommended) 🌟

Drop-in replacement for boto3 S3 client with automatic compression:

```python
from deltaglider import create_client

# Exactly like boto3.client('s3'), but with 99% compression!
client = create_client()

# Standard boto3 S3 methods - just work!
client.put_object(Bucket='releases', Key='v1.0.0/app.zip', Body=data)
response = client.get_object(Bucket='releases', Key='v1.0.0/app.zip')

# Optimized list_objects with smart performance defaults (NEW!)
# Fast by default - no unnecessary metadata fetching
response = client.list_objects(Bucket='releases', Prefix='v1.0.0/')

# Pagination for large buckets
response = client.list_objects(Bucket='releases', MaxKeys=100,
                              ContinuationToken=response.next_continuation_token)

# Get detailed compression stats only when needed
response = client.list_objects(Bucket='releases', FetchMetadata=True)  # Slower but detailed

# Quick bucket statistics
stats = client.get_bucket_stats('releases')  # Fast overview
stats = client.get_bucket_stats('releases', detailed_stats=True)  # With compression metrics

client.delete_object(Bucket='releases', Key='old-version.zip')
```

### 2. Simple API

For straightforward use cases:

```python
from deltaglider import create_client

client = create_client()
summary = client.upload("my-app-v1.0.0.zip", "s3://releases/v1.0.0/")
client.download("s3://releases/v1.0.0/my-app-v1.0.0.zip", "local.zip")
```

### 3. CLI (Command Line Interface)

Drop-in replacement for AWS S3 CLI:

```bash
deltaglider cp my-app-v1.0.0.zip s3://releases/
deltaglider ls s3://releases/
deltaglider sync ./builds/ s3://releases/
```

## Migration from boto3

Migrating from boto3 to DeltaGlider is as simple as changing your import:

```python
# Before (boto3)
import boto3
client = boto3.client('s3')
client.put_object(Bucket='mybucket', Key='myfile.zip', Body=data)

# After (DeltaGlider) - That's it! 99% compression automatically
from deltaglider import create_client
client = create_client()
client.put_object(Bucket='mybucket', Key='myfile.zip', Body=data)
```

## Key Features

- **100% boto3 Compatibility**: All S3 methods work exactly as expected
- **99%+ Compression**: For versioned artifacts and similar files
- **Intelligent Detection**: Automatically determines when to use delta compression
- **Data Integrity**: SHA256 verification on every operation
- **Transparent**: Works with existing tools and workflows
- **Production Ready**: Battle-tested with 200K+ files

## When to Use DeltaGlider

### Perfect For
- Software releases and versioned artifacts
- Container images and layers
- Database backups and snapshots
- Machine learning model checkpoints
- Game assets and updates
- Any versioned binary data

### Not Ideal For
- Already compressed unique files
- Streaming media files
- Frequently changing unstructured data
- Files smaller than 1MB

## Installation

```bash
pip install deltaglider
```

For development or testing with MinIO:
```bash
docker run -p 9000:9000 minio/minio server /data
export AWS_ENDPOINT_URL=http://localhost:9000
```

## Basic Usage

### boto3-Compatible Usage (Recommended)

```python
from deltaglider import create_client

# Create client (uses AWS credentials automatically)
client = create_client()

# Upload using boto3 API
with open('release-v2.0.0.zip', 'rb') as f:
    response = client.put_object(
        Bucket='releases',
        Key='v2.0.0/release.zip',
        Body=f,
        Metadata={'version': '2.0.0'}
    )

# Check compression stats (DeltaGlider extension)
if 'DeltaGliderInfo' in response:
    info = response['DeltaGliderInfo']
    print(f"Saved {info['SavingsPercent']:.0f}% storage space")

# Download using boto3 API
response = client.get_object(Bucket='releases', Key='v2.0.0/release.zip')
with open('local-copy.zip', 'wb') as f:
    f.write(response['Body'].read())

# List objects
response = client.list_objects(Bucket='releases', Prefix='v2.0.0/')
for obj in response.get('Contents', []):
    print(f"{obj['Key']}: {obj['Size']} bytes")

# Delete object
client.delete_object(Bucket='releases', Key='old-version.zip')
```

### Simple API Usage

```python
from deltaglider import create_client

# Create client (uses AWS credentials from environment)
client = create_client()

# Upload a file
summary = client.upload("release-v2.0.0.zip", "s3://releases/v2.0.0/")
print(f"Saved {summary.savings_percent:.0f}% storage space")

# Download a file
client.download("s3://releases/v2.0.0/release-v2.0.0.zip", "local-copy.zip")
```

### With Custom Configuration

```python
from deltaglider import create_client

client = create_client(
    endpoint_url="http://minio.internal:9000",  # Custom S3 endpoint
    log_level="DEBUG",                           # Detailed logging
    cache_dir="/var/cache/deltaglider",         # Custom cache location
)
```

## Real-World Example

```python
from deltaglider import create_client

# Works exactly like boto3!
client = create_client()

# Upload multiple software versions
versions = ["v1.0.0", "v1.0.1", "v1.0.2", "v1.1.0"]
for version in versions:
    with open(f"dist/my-app-{version}.zip", 'rb') as f:
        response = client.put_object(
            Bucket='releases',
            Key=f'{version}/my-app.zip',
            Body=f
        )

        # DeltaGlider provides compression stats
        if 'DeltaGliderInfo' in response:
            info = response['DeltaGliderInfo']
            print(f"{version}: {info['StoredSizeMB']:.1f}MB "
                  f"(saved {info['SavingsPercent']:.0f}%)")

# Result:
# v1.0.0: 100.0MB (saved 0%)    <- First file becomes reference
# v1.0.1: 0.2MB (saved 99.8%)   <- Only differences stored
# v1.0.2: 0.3MB (saved 99.7%)   <- Delta from reference
# v1.1.0: 5.2MB (saved 94.8%)   <- Larger changes, still huge savings
```

## How It Works

1. **First Upload**: The first file uploaded to a prefix becomes the reference
2. **Delta Compression**: Subsequent similar files are compared using xdelta3
3. **Smart Storage**: Only the differences (deltas) are stored
4. **Transparent Reconstruction**: Files are automatically reconstructed on download
5. **boto3 Compatibility**: All operations maintain full boto3 API compatibility

## Performance

Based on real-world usage:
- **Compression**: 99%+ for similar versions
- **Upload Speed**: 3-4 files/second
- **Download Speed**: <100ms reconstruction
- **Storage Savings**: 4TB → 5GB (ReadOnlyREST case study)

## Advanced Features

### Multipart Upload Support

```python
# Large file uploads work automatically
with open('large-file.zip', 'rb') as f:
    client.put_object(
        Bucket='backups',
        Key='database/backup.zip',
        Body=f  # Handles multipart automatically for large files
    )
```

### Batch Operations

```python
# Upload multiple files efficiently
files = ['app.zip', 'docs.zip', 'assets.zip']
for file in files:
    with open(file, 'rb') as f:
        client.put_object(Bucket='releases', Key=file, Body=f)
```

### Presigned URLs

```python
# Generate presigned URLs for secure sharing
url = client.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'releases', 'Key': 'v1.0.0/app.zip'},
    ExpiresIn=3600
)
```

## Support

- GitHub Issues: [github.com/beshu-tech/deltaglider/issues](https://github.com/beshu-tech/deltaglider/issues)
- Documentation: [github.com/beshu-tech/deltaglider#readme](https://github.com/beshu-tech/deltaglider#readme)

## License

MIT License - See [LICENSE](https://github.com/beshu-tech/deltaglider/blob/main/LICENSE) for details.