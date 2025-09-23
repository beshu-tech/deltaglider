# DeltaGlider Python SDK Documentation

The DeltaGlider Python SDK provides a simple, intuitive interface for integrating delta compression into your Python applications. Whether you're managing software releases, database backups, or any versioned binary data, DeltaGlider can reduce your storage costs by up to 99%.

## Quick Links

- [Getting Started](getting-started.md) - Installation and first steps
- [Examples](examples.md) - Real-world usage patterns
- [API Reference](api.md) - Complete API documentation
- [Architecture](architecture.md) - How it works under the hood

## Overview

DeltaGlider provides two ways to interact with your S3 storage:

### 1. CLI (Command Line Interface)
Drop-in replacement for AWS S3 CLI with automatic delta compression:
```bash
deltaglider cp my-app-v1.0.0.zip s3://releases/
deltaglider ls s3://releases/
deltaglider sync ./builds/ s3://releases/
```

### 2. Python SDK
Programmatic interface for Python applications:
```python
from deltaglider import create_client

client = create_client()
summary = client.upload("my-app-v1.0.0.zip", "s3://releases/v1.0.0/")
print(f"Compressed from {summary.original_size_mb:.1f}MB to {summary.stored_size_mb:.1f}MB")
```

## Key Features

- **99%+ Compression**: For versioned artifacts and similar files
- **Drop-in Replacement**: Works with existing AWS S3 workflows
- **Intelligent Detection**: Automatically determines when to use delta compression
- **Data Integrity**: SHA256 verification on every operation
- **S3 Compatible**: Works with AWS, MinIO, Cloudflare R2, and other S3-compatible storage

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

### Simple Upload/Download

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

## How It Works

1. **First Upload**: The first file uploaded to a prefix becomes the reference
2. **Delta Compression**: Subsequent similar files are compared using xdelta3
3. **Smart Storage**: Only the differences (deltas) are stored
4. **Transparent Reconstruction**: Files are automatically reconstructed on download

## Performance

Based on real-world usage:
- **Compression**: 99%+ for similar versions
- **Upload Speed**: 3-4 files/second
- **Download Speed**: <100ms reconstruction
- **Storage Savings**: 4TB → 5GB (ReadOnlyREST case study)

## Support

- GitHub Issues: [github.com/beshu-tech/deltaglider/issues](https://github.com/beshu-tech/deltaglider/issues)
- Documentation: [github.com/beshu-tech/deltaglider#readme](https://github.com/beshu-tech/deltaglider#readme)

## License

MIT License - See [LICENSE](https://github.com/beshu-tech/deltaglider/blob/main/LICENSE) for details.