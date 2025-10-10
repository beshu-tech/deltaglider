# Getting Started with DeltaGlider SDK

This guide will help you get up and running with the DeltaGlider Python SDK in minutes.

## Prerequisites

- Python 3.11 or higher
- AWS credentials configured (or access to MinIO/S3-compatible storage)
- xdelta3 installed on your system (installed automatically with the package)

## Installation

### Using pip

```bash
pip install deltaglider
```

### Using uv (faster)

```bash
uv pip install deltaglider
```

### Development Installation

```bash
git clone https://github.com/beshu-tech/deltaglider
cd deltaglider
pip install -e ".[dev]"
```

## Configuration

### AWS Credentials

DeltaGlider uses standard AWS credential discovery:

1. **Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2
```

2. **AWS Credentials File** (`~/.aws/credentials`)
```ini
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
region = us-west-2
```

3. **IAM Role** (when running on EC2/ECS/Lambda)
Automatically uses instance/task role credentials

### Custom S3 Endpoints

For MinIO, Cloudflare R2, or other S3-compatible storage:

```python
from deltaglider import create_client

client = create_client(endpoint_url="http://minio.local:9000")
```

Or via environment variable:
```bash
export AWS_ENDPOINT_URL=http://minio.local:9000
```

### DeltaGlider Configuration

DeltaGlider supports the following environment variables:

**Logging & Performance**:
- `DG_LOG_LEVEL`: Logging level (default: `INFO`, options: `DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `DG_MAX_RATIO`: Maximum delta/file ratio (default: `0.5`, range: `0.0-1.0`)
  - **See [DG_MAX_RATIO.md](../DG_MAX_RATIO.md) for complete tuning guide**
  - Controls when to use delta compression vs. direct storage
  - Lower (0.2-0.3) = conservative, only high-quality compression
  - Higher (0.6-0.7) = permissive, accept modest savings

**Cache Configuration**:
- `DG_CACHE_BACKEND`: Cache backend type (default: `filesystem`, options: `filesystem`, `memory`)
- `DG_CACHE_MEMORY_SIZE_MB`: Memory cache size in MB (default: `100`)
- `DG_CACHE_ENCRYPTION_KEY`: Optional base64-encoded Fernet key for persistent encryption

**Security**:
- Encryption is **always enabled** (cannot be disabled)
- Ephemeral encryption keys per process (forward secrecy)
- Corrupted cache files automatically deleted
- Set `DG_CACHE_ENCRYPTION_KEY` only for cross-process cache sharing

**Example**:
```bash
# Use memory cache for faster performance in CI/CD
export DG_CACHE_BACKEND=memory
export DG_CACHE_MEMORY_SIZE_MB=500

# Enable debug logging
export DG_LOG_LEVEL=DEBUG

# Adjust delta compression threshold
export DG_MAX_RATIO=0.3  # More aggressive compression
```

## Your First Upload

### Basic Example

```python
from deltaglider import create_client

# Create a client
client = create_client()

# Upload a file
summary = client.upload(
    file_path="my-app-v1.0.0.zip",
    s3_url="s3://my-bucket/releases/v1.0.0/"
)

# Check the results
print(f"Upload completed!")
print(f"Original size: {summary.original_size_mb:.1f} MB")
print(f"Stored size: {summary.stored_size_mb:.1f} MB")
print(f"Compression: {summary.savings_percent:.0f}%")
print(f"Is delta: {summary.is_delta}")
```

### Understanding the Results

When you upload a file, DeltaGlider returns an `UploadSummary` with:

- `operation`: What was done (`PUT` for new reference, `PUT_DELTA` for delta)
- `original_size_mb`: Original file size in MB
- `stored_size_mb`: Actual size stored in S3
- `savings_percent`: Percentage of storage saved
- `is_delta`: Whether delta compression was used
- `delta_ratio`: Ratio of delta size to original (smaller is better)

## Downloading Files

```python
# Download a file
client.download(
    s3_url="s3://my-bucket/releases/v1.0.0/my-app-v1.0.0.zip",
    output_path="downloaded-app.zip"
)

# The file is automatically reconstructed if it was stored as a delta
```

## Working with Multiple Versions

Here's where DeltaGlider shines - uploading multiple versions:

```python
from deltaglider import create_client
from pathlib import Path

client = create_client()

# Upload multiple versions
versions = ["v1.0.0", "v1.0.1", "v1.0.2", "v1.1.0"]

for version in versions:
    file = f"builds/my-app-{version}.zip"

    summary = client.upload(
        file_path=file,
        s3_url=f"s3://releases/{version}/"
    )

    if summary.is_delta:
        print(f"{version}: Compressed to {summary.stored_size_mb:.1f}MB "
              f"(saved {summary.savings_percent:.0f}%)")
    else:
        print(f"{version}: Stored as reference ({summary.original_size_mb:.1f}MB)")

# Typical output:
# v1.0.0: Stored as reference (100.0MB)
# v1.0.1: Compressed to 0.2MB (saved 99.8%)
# v1.0.2: Compressed to 0.3MB (saved 99.7%)
# v1.1.0: Compressed to 5.2MB (saved 94.8%)
```

## Verification

Verify the integrity of stored files:

```python
# Verify a stored file
is_valid = client.verify("s3://releases/v1.0.0/my-app-v1.0.0.zip")
print(f"File integrity: {'✓ Valid' if is_valid else '✗ Corrupted'}")
```

## Error Handling

```python
from deltaglider import create_client

client = create_client()

try:
    summary = client.upload("app.zip", "s3://bucket/path/")
except FileNotFoundError:
    print("Local file not found")
except PermissionError:
    print("S3 access denied - check credentials")
except Exception as e:
    print(f"Upload failed: {e}")
```

## Logging

Control logging verbosity:

```python
# Debug logging for troubleshooting
client = create_client(log_level="DEBUG")

# Quiet mode
client = create_client(log_level="WARNING")

# Default is INFO
client = create_client()  # INFO level
```

## Local Testing with MinIO

For development and testing without AWS:

1. **Start MinIO**
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

2. **Create a bucket** (via MinIO Console at http://localhost:9001)

3. **Use DeltaGlider**
```python
from deltaglider import create_client

client = create_client(
    endpoint_url="http://localhost:9000"
)

# Set credentials via environment
import os
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

# Now use normally
summary = client.upload("test.zip", "s3://test-bucket/")
```

## Best Practices

1. **Group Similar Files**: Upload related files to the same S3 prefix for optimal compression
2. **Version Naming**: Use consistent naming for versions (e.g., `app-v1.0.0.zip`, `app-v1.0.1.zip`)
3. **Cache Management**: The local reference cache improves performance - don't clear it unnecessarily
4. **Error Recovery**: Always handle exceptions for production code
5. **Monitoring**: Log compression ratios to track effectiveness

## Next Steps

- [Examples](examples.md) - See real-world usage patterns
- [API Reference](api.md) - Complete API documentation
- [Architecture](architecture.md) - Understand how it works