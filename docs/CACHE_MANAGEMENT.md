# Cache Management for Long-Running Applications

## Overview

DeltaGlider uses caching to store reference files locally for efficient delta compression. However, unlike the CLI which automatically cleans up cache on exit, **programmatic SDK usage requires manual cache management** for long-running applications.

This guide explains how to manage cache in production applications, including:
- When and how to clear cache
- Encryption key management strategies
- Memory vs. filesystem cache trade-offs
- Best practices for different application types

## The Problem

**CLI (Automatic Cleanup)**:
```bash
# Cache created in /tmp/deltaglider-xyz123/
deltaglider cp file.zip s3://bucket/

# Process exits → Cache automatically deleted via atexit handler
# ✅ No manual cleanup needed
```

**SDK (Manual Cleanup Required)**:
```python
from deltaglider import create_client

client = create_client()

# Long-running application (runs for hours/days)
while True:
    client.put_object(Bucket='releases', Key='file.zip', Body=data)
    time.sleep(600)  # Upload every 10 minutes

# ❌ Process never exits → Cache never cleaned
# ❌ Cache grows indefinitely
# ❌ Memory/disk exhaustion after days/weeks
```

## Solution: Manual Cache Management

### Basic Cache Clearing

```python
from deltaglider import create_client

client = create_client()

# Do some uploads
client.put_object(Bucket='releases', Key='file1.zip', Body=data1)
client.put_object(Bucket='releases', Key='file2.zip', Body=data2)

# Clear cache to free resources
client.clear_cache()

# ✅ All cached references removed
# ✅ Memory/disk freed
# ✅ Next upload will fetch fresh reference from S3
```

### When to Clear Cache

| Scenario | Frequency | Reason |
|----------|-----------|--------|
| **Long-running services** | Every 1-4 hours | Prevent memory/disk growth |
| **After config changes** | Immediately | Old cache is invalid |
| **High memory pressure** | As needed | Free resources |
| **Test cleanup** | After each test | Ensure clean state |
| **Scheduled jobs** | After job completes | Clean up before next run |
| **Key rotation** | After rotation | Old encrypted cache unusable |

## Cache Strategies by Application Type

### 1. Long-Running Background Service

**Scenario**: Continuous upload service running 24/7

```python
from deltaglider import create_client
import schedule
import time
import logging

logger = logging.getLogger(__name__)

client = create_client()

def upload_task():
    """Upload latest build."""
    try:
        with open('latest-build.zip', 'rb') as f:
            response = client.put_object(
                Bucket='releases',
                Key=f'builds/{datetime.now().isoformat()}.zip',
                Body=f
            )
        logger.info(f"Uploaded: {response['ETag']}")
    except Exception as e:
        logger.error(f"Upload failed: {e}")

def cleanup_task():
    """Clear cache to prevent growth."""
    client.clear_cache()
    logger.info("Cache cleared - freed resources")

# Upload every 10 minutes
schedule.every(10).minutes.do(upload_task)

# Clear cache every 2 hours (balance performance vs. memory)
schedule.every(2).hours.do(cleanup_task)

# Run indefinitely
while True:
    schedule.run_pending()
    time.sleep(60)
```

**Cache Clearing Frequency Guidelines**:
- Every 1 hour: High upload frequency (>100/day), memory-constrained
- Every 2-4 hours: Moderate upload frequency (10-100/day), normal memory
- Every 6-12 hours: Low upload frequency (<10/day), abundant memory

### 2. Periodic Batch Job

**Scenario**: Daily backup script

```python
from deltaglider import create_client
import glob
from pathlib import Path

def daily_backup():
    """Upload daily backups and clean up."""
    client = create_client()

    try:
        # Upload all backup files
        for backup in glob.glob('/backups/*.zip'):
            with open(backup, 'rb') as f:
                client.put_object(
                    Bucket='backups',
                    Key=f'daily/{Path(backup).name}',
                    Body=f
                )
            print(f"Backed up: {backup}")

    finally:
        # ALWAYS clear cache at end of job
        client.clear_cache()
        print("Cache cleared - job complete")

# Run daily via cron/systemd timer
if __name__ == '__main__':
    daily_backup()
```

**Best Practice**: Always clear cache in `finally` block to ensure cleanup even if job fails.

### 3. Web Application / API Server

**Scenario**: Flask/FastAPI app with upload endpoints

```python
from fastapi import FastAPI, UploadFile, BackgroundTasks
from deltaglider import create_client
import asyncio

app = FastAPI()

# Create client once at startup
client = create_client()

async def periodic_cache_cleanup():
    """Background task to clear cache periodically."""
    while True:
        await asyncio.sleep(3600)  # Every hour
        client.clear_cache()
        print("Cache cleared in background")

@app.on_event("startup")
async def startup_event():
    """Start background cache cleanup task."""
    asyncio.create_task(periodic_cache_cleanup())

@app.post("/upload")
async def upload_file(file: UploadFile):
    """Upload endpoint with automatic cache management."""
    content = await file.read()

    response = client.put_object(
        Bucket='uploads',
        Key=f'files/{file.filename}',
        Body=content
    )

    return {"message": "Uploaded", "etag": response['ETag']}

@app.post("/admin/clear-cache")
async def admin_clear_cache():
    """Manual cache clear endpoint for admin."""
    client.clear_cache()
    return {"message": "Cache cleared"}
```

**Best Practice**: Run periodic cache cleanup in background task, provide manual clear endpoint for emergencies.

### 4. Testing / CI/CD

**Scenario**: Test suite using DeltaGlider

```python
import pytest
from deltaglider import create_client

@pytest.fixture
def deltaglider_client():
    """Provide clean client for each test."""
    client = create_client()
    yield client
    # ALWAYS clear cache after test
    client.clear_cache()

def test_upload(deltaglider_client):
    """Test upload with automatic cleanup."""
    response = deltaglider_client.put_object(
        Bucket='test-bucket',
        Key='test-file.zip',
        Body=b'test data'
    )
    assert response['ETag'] is not None
    # Cache automatically cleared by fixture

def test_download(deltaglider_client):
    """Test download with clean cache."""
    # Cache is clean from previous test
    deltaglider_client.put_object(Bucket='test', Key='file.zip', Body=b'data')
    response = deltaglider_client.get_object(Bucket='test', Key='file.zip')
    assert response['Body'].read() == b'data'
    # Cache automatically cleared by fixture
```

**Best Practice**: Use pytest fixtures to ensure cache is cleared after each test.

### 5. AWS Lambda / Serverless

**Scenario**: Lambda function with warm container reuse

```python
import os
from deltaglider import create_client

# Initialize client outside handler (reused across invocations)
client = create_client()

# Track invocation count for cache clearing
invocation_count = 0

def lambda_handler(event, context):
    """Lambda handler with periodic cache clearing."""
    global invocation_count
    invocation_count += 1

    try:
        # Upload file
        response = client.put_object(
            Bucket='lambda-uploads',
            Key=event['filename'],
            Body=event['data']
        )

        # Clear cache every 50 invocations (warm container optimization)
        if invocation_count % 50 == 0:
            client.clear_cache()
            print(f"Cache cleared after {invocation_count} invocations")

        return {'statusCode': 200, 'etag': response['ETag']}

    except Exception as e:
        # Clear cache on error to prevent poisoned state
        client.clear_cache()
        return {'statusCode': 500, 'error': str(e)}
```

**Best Practice**: Clear cache periodically (every N invocations) and on errors. Lambda warm containers can reuse cache across invocations for performance.

## Encryption Key Management

DeltaGlider always encrypts cache data. Understanding key management is critical for programmatic usage.

### Ephemeral Keys (Default - Recommended)

**How It Works**:
- New encryption key generated per client instance
- Cache encrypted with instance-specific key
- Key lost when client is garbage collected
- **Maximum security** - keys never persist

**When to Use**:
- Single-process applications
- Short-lived scripts
- CI/CD pipelines
- Testing
- Maximum security requirements

**Example**:
```python
from deltaglider import create_client

# Create client (generates ephemeral key automatically)
client = create_client()

# Upload file (encrypted with ephemeral key)
client.put_object(Bucket='bucket', Key='file.zip', Body=data)

# Clear cache
client.clear_cache()

# ✅ Encrypted cache cleared
# ✅ Key was never persisted
# ✅ Perfect forward secrecy
```

**Characteristics**:
- ✅ Maximum security (keys never leave process)
- ✅ Perfect forward secrecy
- ✅ No key management overhead
- ❌ Cache not shareable between processes
- ❌ Cache not reusable after client recreation

### Persistent Keys (Advanced - Shared Cache)

**How It Works**:
- Use same encryption key across multiple processes/clients
- Key stored in environment variable or secrets manager
- All processes can read each other's encrypted cache
- **Trade-off**: Convenience vs. security

**When to Use**:
- Multi-process applications (workers, replicas)
- Shared cache across containers
- Cache persistence across application restarts
- Horizontal scaling scenarios

**Example - Environment Variable**:
```python
import os
from cryptography.fernet import Fernet
import base64

# Generate persistent key (do this ONCE, securely)
key = Fernet.generate_key()
key_b64 = base64.b64encode(key).decode('utf-8')
print(f"DG_CACHE_ENCRYPTION_KEY={key_b64}")  # Store in secrets manager!

# Set in environment (or use secrets manager)
os.environ['DG_CACHE_ENCRYPTION_KEY'] = key_b64

# All client instances use same key
client1 = create_client()
client2 = create_client()

# Client1 writes to cache
client1.put_object(Bucket='bucket', Key='file.zip', Body=data)

# Client2 can read same cached data (same key!)
client2.get_object(Bucket='bucket', Key='file.zip')

# ✅ Cache shared between processes
# ⚠️  Key must be securely managed
```

**Example - AWS Secrets Manager**:
```python
import boto3
import json
from deltaglider import create_client

def get_encryption_key_from_secrets_manager():
    """Retrieve encryption key from AWS Secrets Manager."""
    secrets = boto3.client('secretsmanager', region_name='us-west-2')
    response = secrets.get_secret_value(SecretId='deltaglider/cache-encryption-key')
    secret = json.loads(response['SecretString'])
    return secret['encryption_key']

# Retrieve key securely
os.environ['DG_CACHE_ENCRYPTION_KEY'] = get_encryption_key_from_secrets_manager()

# Create client with persistent key
client = create_client()
```

**Security Best Practices for Persistent Keys**:
1. **Generate once**: Never regenerate unless rotating
2. **Store securely**: AWS Secrets Manager, HashiCorp Vault, etc.
3. **Rotate regularly**: Follow your key rotation policy
4. **Audit access**: Log who accesses encryption keys
5. **Principle of least privilege**: Only processes that need shared cache get the key

### Key Rotation

**When to Rotate**:
- Regular schedule (every 90 days recommended)
- After security incident
- When team members leave
- After key exposure

**How to Rotate**:
```python
import os
from deltaglider import create_client

# OLD KEY (existing cache encrypted with this)
old_key = os.environ.get('DG_CACHE_ENCRYPTION_KEY')

# Generate NEW KEY
from cryptography.fernet import Fernet
new_key = Fernet.generate_key()
new_key_b64 = base64.b64encode(new_key).decode('utf-8')

# Steps for rotation:
# 1. Clear old cache (encrypted with old key)
client_old = create_client()  # Uses old key from env
client_old.clear_cache()

# 2. Update environment with new key
os.environ['DG_CACHE_ENCRYPTION_KEY'] = new_key_b64

# 3. Create new client with new key
client_new = create_client()  # Uses new key

# 4. Continue operations
client_new.put_object(Bucket='bucket', Key='file.zip', Body=data)

# ✅ Cache now encrypted with new key
# ✅ Old encrypted cache cleared
```

## Memory vs. Filesystem Cache

### Filesystem Cache (Default)

**Characteristics**:
- Stored in `/tmp/deltaglider-*/`
- Persistent across client recreations (within same process)
- Can be shared between processes (with persistent encryption key)
- Slower than memory cache (disk I/O)

**Configuration**:
```python
import os

# Explicitly set filesystem cache (this is the default)
os.environ['DG_CACHE_BACKEND'] = 'filesystem'

from deltaglider import create_client
client = create_client()
```

**When to Use**:
- Default choice for most applications
- When cache should persist across client recreations
- Multi-process applications (with persistent key)
- Memory-constrained environments

**Cache Clearing**:
```python
client.clear_cache()
# Removes all files from /tmp/deltaglider-*/
# Frees disk space
```

### Memory Cache

**Characteristics**:
- Stored in process memory (RAM)
- Fast access (no disk I/O)
- Automatically freed when process exits
- LRU eviction prevents unlimited growth
- Not shared between processes

**Configuration**:
```python
import os

# Enable memory cache
os.environ['DG_CACHE_BACKEND'] = 'memory'
os.environ['DG_CACHE_MEMORY_SIZE_MB'] = '200'  # Default: 100MB

from deltaglider import create_client
client = create_client()
```

**When to Use**:
- High-performance requirements
- Ephemeral environments (containers, Lambda)
- Short-lived applications
- CI/CD pipelines
- When disk I/O is bottleneck

**Cache Clearing**:
```python
client.clear_cache()
# Frees memory immediately
# No disk I/O
```

**LRU Eviction**:
Memory cache automatically evicts least recently used entries when size limit is reached. No manual intervention needed.

## Best Practices Summary

### ✅ DO

1. **Clear cache periodically** in long-running applications
2. **Clear cache in `finally` blocks** for batch jobs
3. **Use fixtures for tests** to ensure clean state
4. **Monitor cache size** in production
5. **Use ephemeral keys** when possible (maximum security)
6. **Store persistent keys securely** (Secrets Manager, Vault)
7. **Rotate encryption keys** regularly
8. **Use memory cache** for ephemeral environments
9. **Clear cache after config changes**
10. **Document cache strategy** for your application

### ❌ DON'T

1. **Never let cache grow unbounded** in long-running apps
2. **Don't share ephemeral encrypted cache** between processes
3. **Don't store persistent keys in code** or version control
4. **Don't forget to clear cache in tests**
5. **Don't assume cache is automatically cleaned** in SDK usage
6. **Don't use persistent keys** unless you need cross-process sharing
7. **Don't skip key rotation**
8. **Don't ignore memory limits** for memory cache

## Monitoring Cache Health

### Cache Size Tracking

```python
import os
from pathlib import Path
from deltaglider import create_client

def get_cache_size_mb(cache_dir: Path) -> float:
    """Calculate total cache size in MB."""
    total_bytes = sum(f.stat().st_size for f in cache_dir.rglob('*') if f.is_file())
    return total_bytes / (1024 * 1024)

# Get cache directory (ephemeral, changes per process)
cache_dir = Path(tempfile.gettempdir())
deltaglider_caches = list(cache_dir.glob('deltaglider-*'))

if deltaglider_caches:
    cache_path = deltaglider_caches[0]
    cache_size_mb = get_cache_size_mb(cache_path)
    print(f"Cache size: {cache_size_mb:.2f} MB")

    # Clear if over threshold
    if cache_size_mb > 500:
        client = create_client()
        client.clear_cache()
        print("Cache cleared - exceeded 500MB")
```

### Memory Cache Monitoring

```python
import os
os.environ['DG_CACHE_BACKEND'] = 'memory'

from deltaglider import create_client

client = create_client()

# Memory cache auto-evicts at configured limit
# Monitor via application memory usage tools (not direct API)

# Example with memory_profiler
from memory_profiler import profile

@profile
def upload_many_files():
    for i in range(1000):
        client.put_object(Bucket='test', Key=f'file{i}.zip', Body=b'data' * 1000)
    # Check memory profile to see cache memory usage

upload_many_files()
```

## Troubleshooting

### Problem: Cache Growing Too Large

**Symptoms**:
- Disk space running out (`/tmp` filling up)
- High memory usage

**Solution**:
```python
# Implement automatic cache clearing
import psutil

client = create_client()

def smart_cache_clear():
    """Clear cache if memory/disk pressure detected."""
    # Check disk space
    disk = psutil.disk_usage('/tmp')
    if disk.percent > 80:
        client.clear_cache()
        print("Cache cleared - disk pressure")
        return

    # Check memory usage
    memory = psutil.virtual_memory()
    if memory.percent > 80:
        client.clear_cache()
        print("Cache cleared - memory pressure")

# Call periodically
schedule.every(15).minutes.do(smart_cache_clear)
```

### Problem: Decryption Failures After Key Rotation

**Symptoms**:
- `CacheCorruptionError: Decryption failed`
- After rotating encryption keys

**Solution**:
```python
# Clear cache before using new key
old_client = create_client()  # Old key
old_client.clear_cache()

# Update encryption key
os.environ['DG_CACHE_ENCRYPTION_KEY'] = new_key

# Create new client
new_client = create_client()  # New key
```

### Problem: Tests Failing Due to Cached Data

**Symptoms**:
- Tests pass in isolation, fail when run together
- Unexpected data in downloads

**Solution**:
```python
# Always clear cache in test teardown
@pytest.fixture(autouse=True)
def clear_cache_after_test():
    """Automatically clear cache after every test."""
    yield
    # Teardown
    client = create_client()
    client.clear_cache()
```

## Related Documentation

- [docs/sdk/getting-started.md](sdk/getting-started.md) - SDK configuration
- [README.md](../README.md) - Docker and environment variables
- [CLAUDE.md](../CLAUDE.md) - Development guide

## Quick Reference

```python
from deltaglider import create_client

# Create client
client = create_client()

# Clear all cache
client.clear_cache()

# Use memory cache
os.environ['DG_CACHE_BACKEND'] = 'memory'

# Use persistent encryption key
os.environ['DG_CACHE_ENCRYPTION_KEY'] = 'base64-encoded-key'
```
