# DeltaGlider SDK Examples

Real-world examples and patterns for using DeltaGlider in production applications.

## Table of Contents

1. [Performance-Optimized Bucket Listing](#performance-optimized-bucket-listing)
2. [Bucket Statistics and Monitoring](#bucket-statistics-and-monitoring)
3. [Session-Level Cache Management](#session-level-cache-management)
4. [Bucket Management](#bucket-management)
5. [Software Release Management](#software-release-management)
6. [Database Backup System](#database-backup-system)
7. [CI/CD Pipeline Integration](#cicd-pipeline-integration)
8. [Container Registry Storage](#container-registry-storage)
9. [Machine Learning Model Versioning](#machine-learning-model-versioning)
10. [Game Asset Distribution](#game-asset-distribution)
11. [Log Archive Management](#log-archive-management)
12. [Multi-Region Replication](#multi-region-replication)

## Performance-Optimized Bucket Listing

DeltaGlider's smart `list_objects` method eliminates the N+1 query problem by intelligently managing metadata fetching.

### Fast Web UI Listing (No Metadata)

```python
from deltaglider import create_client
import time

client = create_client()

def fast_bucket_listing(bucket: str):
    """Ultra-fast listing for web UI display (~50ms for 1000 objects)."""
    start = time.time()

    # Default: FetchMetadata=False - no HEAD requests
    response = client.list_objects(
        Bucket=bucket,
        MaxKeys=100  # Pagination for UI
    )

    # Process objects for display
    items = []
    for obj in response.contents:
        items.append({
            "key": obj.key,
            "size": obj.size,
            "last_modified": obj.last_modified,
            "is_delta": obj.is_delta,  # Determined from filename
            # No compression_ratio - would require HEAD request
        })

    elapsed = time.time() - start
    print(f"Listed {len(items)} objects in {elapsed*1000:.0f}ms")

    return items, response.next_continuation_token

# Example: List first page
items, next_token = fast_bucket_listing('releases')
```

### Paginated Listing for Large Buckets

```python
def paginated_listing(bucket: str, page_size: int = 50):
    """Efficiently paginate through large buckets."""
    all_objects = []
    continuation_token = None

    while True:
        response = client.list_objects(
            Bucket=bucket,
            MaxKeys=page_size,
            ContinuationToken=continuation_token,
            FetchMetadata=False  # Keep it fast
        )

        all_objects.extend(response.contents)

        if not response.is_truncated:
            break

        continuation_token = response.next_continuation_token
        print(f"Fetched {len(all_objects)} objects so far...")

    return all_objects

# Example: List all objects efficiently
all_objects = paginated_listing('releases', page_size=100)
print(f"Total objects: {len(all_objects)}")
```

### Analytics Dashboard with Compression Stats

```python
def dashboard_with_stats(bucket: str):
    """Dashboard view with optional detailed stats."""

    # Quick overview (fast - no metadata)
    stats = client.get_bucket_stats(bucket, detailed_stats=False)

    print(f"Quick Stats for {bucket}:")
    print(f"  Total Objects: {stats.object_count}")
    print(f"  Delta Files: {stats.delta_objects}")
    print(f"  Regular Files: {stats.direct_objects}")
    print(f"  Total Size: {stats.total_size / (1024**3):.2f} GB")
    print(f"  Stored Size: {stats.compressed_size / (1024**3):.2f} GB")

    # Detailed compression analysis (slower - fetches metadata for deltas only)
    if stats.delta_objects > 0:
        detailed_stats = client.get_bucket_stats(bucket, detailed_stats=True)
        print(f"\nDetailed Compression Stats:")
        print(f"  Average Compression: {detailed_stats.average_compression_ratio:.1%}")
        print(f"  Space Saved: {detailed_stats.space_saved / (1024**3):.2f} GB")

# Example usage
dashboard_with_stats('releases')
```

### Smart Metadata Fetching for Analytics

```python
def compression_analysis(bucket: str, prefix: str = ""):
    """Analyze compression effectiveness with selective metadata fetching."""

    # Only fetch metadata when we need compression stats
    response = client.list_objects(
        Bucket=bucket,
        Prefix=prefix,
        FetchMetadata=True  # Fetches metadata ONLY for .delta files
    )

    # Analyze compression effectiveness
    delta_files = [obj for obj in response.contents if obj.is_delta]

    if delta_files:
        total_original = sum(obj.original_size for obj in delta_files)
        total_compressed = sum(obj.compressed_size for obj in delta_files)
        avg_ratio = (total_original - total_compressed) / total_original

        print(f"Compression Analysis for {prefix or 'all files'}:")
        print(f"  Delta Files: {len(delta_files)}")
        print(f"  Original Size: {total_original / (1024**2):.1f} MB")
        print(f"  Compressed Size: {total_compressed / (1024**2):.1f} MB")
        print(f"  Average Compression: {avg_ratio:.1%}")

        # Find best and worst compression
        best = max(delta_files, key=lambda x: x.compression_ratio or 0)
        worst = min(delta_files, key=lambda x: x.compression_ratio or 1)

        print(f"  Best Compression: {best.key} ({best.compression_ratio:.1%})")
        print(f"  Worst Compression: {worst.key} ({worst.compression_ratio:.1%})")

# Example: Analyze v2.0 releases
compression_analysis('releases', 'v2.0/')
```

### Performance Comparison

```python
def performance_comparison(bucket: str):
    """Compare performance with and without metadata fetching."""
    import time

    # Test 1: Fast listing (no metadata)
    start = time.time()
    response_fast = client.list_objects(
        Bucket=bucket,
        MaxKeys=100,
        FetchMetadata=False  # Default
    )
    time_fast = (time.time() - start) * 1000

    # Test 2: Detailed listing (with metadata for deltas)
    start = time.time()
    response_detailed = client.list_objects(
        Bucket=bucket,
        MaxKeys=100,
        FetchMetadata=True  # Fetches for delta files only
    )
    time_detailed = (time.time() - start) * 1000

    delta_count = sum(1 for obj in response_fast.contents if obj.is_delta)

    print(f"Performance Comparison for {bucket}:")
    print(f"  Fast Listing: {time_fast:.0f}ms (1 API call)")
    print(f"  Detailed Listing: {time_detailed:.0f}ms (1 + {delta_count} API calls)")
    print(f"  Speed Improvement: {time_detailed/time_fast:.1f}x slower with metadata")
    print(f"\nRecommendation: Use FetchMetadata=True only when you need:")
    print("  - Exact original file sizes for delta files")
    print("  - Accurate compression ratios")
    print("  - Reference key information")

# Example: Compare performance
performance_comparison('releases')
```

### Best Practices

1. **Default to Fast Mode**: Always use `FetchMetadata=False` (default) unless you specifically need compression stats.

2. **Never Fetch for Non-Deltas**: The SDK automatically skips metadata fetching for non-delta files even when `FetchMetadata=True`.

## Bucket Statistics and Monitoring

DeltaGlider provides powerful bucket statistics with session-level caching for performance.

### Quick Dashboard Stats (Cached)

```python
from deltaglider import create_client

client = create_client()

def show_bucket_dashboard(bucket: str):
    """Display real-time bucket statistics with caching."""

    # First call: computes stats (~50ms)
    stats = client.get_bucket_stats(bucket)

    # Second call: instant (cached)
    stats = client.get_bucket_stats(bucket)

    print(f"Dashboard for {stats.bucket}")
    print(f"=" * 60)
    print(f"Total Objects:        {stats.object_count:,}")
    print(f"  Delta Objects:      {stats.delta_objects:,}")
    print(f"  Direct Objects:     {stats.direct_objects:,}")
    print()
    print(f"Original Size:        {stats.total_size / (1024**3):.2f} GB")
    print(f"Stored Size:          {stats.compressed_size / (1024**3):.2f} GB")
    print(f"Space Saved:          {stats.space_saved / (1024**3):.2f} GB")
    print(f"Compression Ratio:    {stats.average_compression_ratio:.1%}")

# Example: Show stats for multiple buckets (each cached separately)
for bucket_name in ['releases', 'backups', 'archives']:
    show_bucket_dashboard(bucket_name)
```

### Detailed Compression Analysis

```python
def detailed_compression_report(bucket: str):
    """Generate detailed compression report with accurate ratios."""

    # Detailed stats fetch metadata for delta files (slower, accurate)
    stats = client.get_bucket_stats(bucket, detailed_stats=True)

    efficiency = (stats.space_saved / stats.total_size * 100) if stats.total_size > 0 else 0

    print(f"Detailed Compression Report: {stats.bucket}")
    print(f"=" * 60)
    print(f"Object Distribution:")
    print(f"  Total:              {stats.object_count:,}")
    print(f"  Delta-Compressed:   {stats.delta_objects:,} ({stats.delta_objects/stats.object_count*100:.1f}%)")
    print(f"  Direct Storage:     {stats.direct_objects:,} ({stats.direct_objects/stats.object_count*100:.1f}%)")
    print()
    print(f"Storage Efficiency:")
    print(f"  Original Data:      {stats.total_size / (1024**3):.2f} GB")
    print(f"  Actual Storage:     {stats.compressed_size / (1024**3):.2f} GB")
    print(f"  Space Saved:        {stats.space_saved / (1024**3):.2f} GB")
    print(f"  Efficiency:         {efficiency:.1f}%")
    print(f"  Avg Compression:    {stats.average_compression_ratio:.2%}")

    # Calculate estimated monthly costs (example: $0.023/GB S3 Standard)
    cost_without = stats.total_size / (1024**3) * 0.023
    cost_with = stats.compressed_size / (1024**3) * 0.023
    monthly_savings = cost_without - cost_with

    print()
    print(f"Estimated Monthly S3 Costs ($0.023/GB):")
    print(f"  Without DeltaGlider: ${cost_without:.2f}")
    print(f"  With DeltaGlider:    ${cost_with:.2f}")
    print(f"  Monthly Savings:     ${monthly_savings:.2f}")

# Example: Detailed report
detailed_compression_report('releases')
```

### List Buckets with Cached Stats

```python
def list_buckets_with_stats():
    """List all buckets and show cached statistics if available."""

    # Pre-fetch stats for important buckets
    important_buckets = ['releases', 'backups']
    for bucket_name in important_buckets:
        client.get_bucket_stats(bucket_name, detailed_stats=True)

    # List all buckets (includes cached stats automatically)
    response = client.list_buckets()

    print("All Buckets:")
    print(f"{'Name':<30} {'Objects':<10} {'Compression':<15} {'Cached'}")
    print("=" * 70)

    for bucket in response['Buckets']:
        name = bucket['Name']

        # Check if stats are cached
        if 'DeltaGliderStats' in bucket:
            stats = bucket['DeltaGliderStats']
            obj_count = f"{stats['ObjectCount']:,}"
            compression = f"{stats['AverageCompressionRatio']:.1%}"
            cached = "✓ (detailed)" if stats['Detailed'] else "✓ (quick)"
        else:
            obj_count = "N/A"
            compression = "N/A"
            cached = "✗"

        print(f"{name:<30} {obj_count:<10} {compression:<15} {cached}")

# Example: List with stats
list_buckets_with_stats()
```

### Monitoring Dashboard (Real-Time)

```python
import time

def monitoring_dashboard(buckets: list[str], refresh_seconds: int = 60):
    """Real-time monitoring dashboard with periodic refresh."""

    while True:
        print("\033[2J\033[H")  # Clear screen
        print(f"DeltaGlider Monitoring Dashboard - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        for bucket_name in buckets:
            # Get cached stats (instant) or compute fresh
            stats = client.get_bucket_stats(bucket_name)

            print(f"\n{bucket_name}:")
            print(f"  Objects: {stats.object_count:,} | "
                  f"Delta: {stats.delta_objects:,} | "
                  f"Direct: {stats.direct_objects:,}")
            print(f"  Size: {stats.compressed_size/(1024**3):.2f} GB | "
                  f"Saved: {stats.space_saved/(1024**3):.2f} GB | "
                  f"Compression: {stats.average_compression_ratio:.1%}")

        print(f"\n{'=' * 80}")
        print(f"Refreshing in {refresh_seconds} seconds... (Ctrl+C to exit)")

        time.sleep(refresh_seconds)

        # Clear cache for fresh data on next iteration
        client.clear_cache()

# Example: Monitor key buckets
try:
    monitoring_dashboard(['releases', 'backups', 'archives'], refresh_seconds=30)
except KeyboardInterrupt:
    print("\nMonitoring stopped.")
```

## Session-Level Cache Management

DeltaGlider maintains session-level caches for optimal performance in long-running applications.

### Long-Running Application Pattern

```python
from deltaglider import create_client
import time

def long_running_upload_service():
    """Upload service with periodic cache cleanup."""

    client = create_client()
    processed_count = 0

    while True:
        # Simulate file processing
        files_to_upload = get_pending_files()  # Your file queue

        for file_path in files_to_upload:
            try:
                summary = client.upload(file_path, "s3://releases/")
                processed_count += 1

                print(f"Uploaded {file_path}: {summary.savings_percent:.0f}% saved")

                # Periodic cache cleanup (every 100 files)
                if processed_count % 100 == 0:
                    client.clear_cache()
                    print(f"Cache cleared after {processed_count} files")

            except Exception as e:
                print(f"Error uploading {file_path}: {e}")

        time.sleep(60)  # Check for new files every minute

# Example: Run upload service
# long_running_upload_service()
```

### Cache Invalidation After External Changes

```python
def handle_external_bucket_changes(bucket: str):
    """Refresh statistics after external tools modify bucket."""

    # Get initial stats (cached)
    stats_before = client.get_bucket_stats(bucket)
    print(f"Before: {stats_before.object_count} objects")

    # External process modifies bucket
    print("External backup tool running...")
    run_external_backup_tool(bucket)  # Your external tool

    # Clear cache to get fresh data
    client.clear_cache()

    # Get updated stats
    stats_after = client.get_bucket_stats(bucket)
    print(f"After: {stats_after.object_count} objects")
    print(f"Added: {stats_after.object_count - stats_before.object_count} objects")

# Example usage
handle_external_bucket_changes('backups')
```

### Selective Cache Eviction

```python
def selective_cache_management():
    """Manage cache for specific delta spaces."""

    client = create_client()

    # Upload to multiple delta spaces
    versions = ['v1.0.0', 'v1.1.0', 'v1.2.0']

    for version in versions:
        client.upload(f"app-{version}.zip", f"s3://releases/{version}/")

    # Update reference for specific version
    print("Updating v1.1.0 reference...")
    client.upload("new-reference.zip", "s3://releases/v1.1.0/")

    # Evict only v1.1.0 cache (others remain cached)
    client.evict_cache("s3://releases/v1.1.0/reference.bin")

    # Next upload to v1.1.0 fetches fresh reference
    # v1.0.0 and v1.2.0 still use cached references
    client.upload("similar-file.zip", "s3://releases/v1.1.0/")

# Example: Selective eviction
selective_cache_management()
```

### Testing with Clean Cache

```python
import pytest
from deltaglider import create_client

def test_upload_workflow():
    """Test with clean cache state."""

    client = create_client()
    client.clear_cache()  # Start with clean state

    # Test first upload (no reference exists)
    summary1 = client.upload("file1.zip", "s3://test-bucket/prefix/")
    assert not summary1.is_delta  # First file is reference

    # Test subsequent upload (uses cached reference)
    summary2 = client.upload("file2.zip", "s3://test-bucket/prefix/")
    assert summary2.is_delta  # Should use delta

    # Clear and test again
    client.clear_cache()
    summary3 = client.upload("file3.zip", "s3://test-bucket/prefix/")
    assert summary3.is_delta  # Still delta (reference in S3)

# Run test
# test_upload_workflow()
```

### Cache Performance Monitoring

```python
import time

def measure_cache_performance(bucket: str):
    """Measure performance impact of caching."""

    client = create_client()

    # Test 1: Cold cache
    client.clear_cache()
    start = time.time()
    stats1 = client.get_bucket_stats(bucket, detailed_stats=True)
    cold_time = (time.time() - start) * 1000

    # Test 2: Warm cache
    start = time.time()
    stats2 = client.get_bucket_stats(bucket, detailed_stats=True)
    warm_time = (time.time() - start) * 1000

    # Test 3: Quick stats from detailed cache
    start = time.time()
    stats3 = client.get_bucket_stats(bucket, detailed_stats=False)
    reuse_time = (time.time() - start) * 1000

    print(f"Cache Performance for {bucket}:")
    print(f"  Cold Cache (detailed):     {cold_time:.0f}ms")
    print(f"  Warm Cache (detailed):     {warm_time:.0f}ms")
    print(f"  Cache Reuse (quick):       {reuse_time:.0f}ms")
    print(f"  Speedup (detailed):        {cold_time/warm_time:.1f}x")
    print(f"  Speedup (reuse):           {cold_time/reuse_time:.1f}x")

# Example: Measure cache performance
measure_cache_performance('releases')
```

3. **Use Pagination**: For large buckets, use `MaxKeys` and `ContinuationToken` to paginate results.

4. **Cache Results**: If you need metadata frequently, consider caching the results to avoid repeated HEAD requests.

5. **Batch Analytics**: When doing analytics, fetch metadata once and process the results rather than making multiple calls.

## Bucket Management

DeltaGlider provides boto3-compatible bucket management methods for creating, listing, and deleting buckets without requiring boto3.

### Complete Bucket Lifecycle

```python
from deltaglider import create_client

client = create_client()

# Create bucket
client.create_bucket(Bucket='my-releases')

# Create bucket in specific region
client.create_bucket(
    Bucket='eu-backups',
    CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'}
)

# List all buckets
response = client.list_buckets()
for bucket in response['Buckets']:
    print(f"{bucket['Name']} - Created: {bucket['CreationDate']}")

# Upload some objects
with open('app-v1.0.0.zip', 'rb') as f:
    client.put_object(Bucket='my-releases', Key='v1.0.0/app.zip', Body=f)

# Delete objects first (bucket must be empty)
client.delete_object(Bucket='my-releases', Key='v1.0.0/app.zip')

# Delete bucket
client.delete_bucket(Bucket='my-releases')
```

### Idempotent Operations

Bucket management operations are idempotent for safe automation:

```python
# Creating existing bucket returns success (no error)
client.create_bucket(Bucket='my-releases')
client.create_bucket(Bucket='my-releases')  # Safe, returns success

# Deleting non-existent bucket returns success (no error)
client.delete_bucket(Bucket='non-existent')  # Safe, returns success
```

### Hybrid boto3/DeltaGlider Usage

For advanced S3 features not in DeltaGlider's 21 core methods, use boto3 directly:

```python
from deltaglider import create_client
import boto3

# DeltaGlider for core operations with compression
dg_client = create_client()

# boto3 for advanced features
s3_client = boto3.client('s3')

# Use DeltaGlider for object operations (with compression)
with open('release.zip', 'rb') as f:
    dg_client.put_object(Bucket='releases', Key='v1.0.0/release.zip', Body=f)

# Use boto3 for advanced bucket features
s3_client.put_bucket_versioning(
    Bucket='releases',
    VersioningConfiguration={'Status': 'Enabled'}
)

# Use boto3 for bucket policies
policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::releases/*"
    }]
}
s3_client.put_bucket_policy(Bucket='releases', Policy=json.dumps(policy))
```

See [BOTO3_COMPATIBILITY.md](../../BOTO3_COMPATIBILITY.md) for complete method coverage.

## Software Release Management

### Managing Multiple Product Lines

```python
from deltaglider import create_client
from pathlib import Path
import json
from datetime import datetime

class ReleaseManager:
    def __init__(self, bucket="releases"):
        self.client = create_client()
        self.bucket = bucket
        self.manifest = {}

    def upload_release(self, product, version, file_path, metadata=None):
        """Upload a release with metadata and tracking."""
        s3_url = f"s3://{self.bucket}/{product}/{version}/"

        # Upload the release file
        summary = self.client.upload(file_path, s3_url)

        # Track in manifest
        self.manifest[f"{product}-{version}"] = {
            "uploaded_at": datetime.utcnow().isoformat(),
            "original_size": summary.original_size,
            "stored_size": summary.stored_size,
            "is_delta": summary.is_delta,
            "compression_ratio": summary.savings_percent,
            "metadata": metadata or {}
        }

        # Save manifest
        self._save_manifest()

        return summary

    def get_release(self, product, version, output_dir="downloads"):
        """Download a specific release."""
        s3_url = f"s3://{self.bucket}/{product}/{version}/{product}-{version}.zip"
        output_path = Path(output_dir) / f"{product}-{version}.zip"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.client.download(s3_url, str(output_path))
        return output_path

    def get_compression_stats(self, product=None):
        """Get compression statistics for releases."""
        stats = {
            "total_original": 0,
            "total_stored": 0,
            "total_saved": 0,
            "releases": 0,
            "delta_releases": 0
        }

        for key, info in self.manifest.items():
            if product and not key.startswith(product):
                continue

            stats["releases"] += 1
            stats["total_original"] += info["original_size"]
            stats["total_stored"] += info["stored_size"]
            if info["is_delta"]:
                stats["delta_releases"] += 1

        stats["total_saved"] = stats["total_original"] - stats["total_stored"]
        stats["compression_percent"] = (
            (stats["total_saved"] / stats["total_original"] * 100)
            if stats["total_original"] > 0 else 0
        )

        return stats

    def _save_manifest(self):
        """Save manifest to S3."""
        manifest_json = json.dumps(self.manifest, indent=2)
        # This would typically save to S3, for now just return
        return manifest_json

# Usage
manager = ReleaseManager()

# Upload multiple product releases
products = [
    ("webapp", "v2.0.0", "builds/webapp-v2.0.0.tar.gz"),
    ("webapp", "v2.0.1", "builds/webapp-v2.0.1.tar.gz"),
    ("api", "v1.5.0", "builds/api-v1.5.0.jar"),
    ("api", "v1.5.1", "builds/api-v1.5.1.jar"),
]

for product, version, file_path in products:
    summary = manager.upload_release(
        product, version, file_path,
        metadata={"branch": "main", "commit": "abc123"}
    )
    print(f"{product} {version}: {summary.savings_percent:.0f}% compression")

# Get statistics
stats = manager.get_compression_stats()
print(f"Total savings: {stats['total_saved'] / (1024**3):.2f} GB")
print(f"Compression rate: {stats['compression_percent']:.1f}%")
```

## Database Backup System

### Automated Daily Backups with Retention

```python
from deltaglider import create_client
from datetime import datetime, timedelta
import subprocess
import os
from pathlib import Path

class DatabaseBackupManager:
    def __init__(self, db_name, bucket="backups"):
        self.client = create_client(log_level="INFO")
        self.db_name = db_name
        self.bucket = bucket
        self.backup_dir = Path("/tmp/backups")
        self.backup_dir.mkdir(exist_ok=True)

    def backup_postgres(self, connection_string):
        """Create and upload PostgreSQL backup."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"{self.db_name}_{timestamp}.sql.gz"

        # Create database dump
        print(f"Creating backup of {self.db_name}...")
        subprocess.run(
            f"pg_dump {connection_string} | gzip > {backup_file}",
            shell=True,
            check=True
        )

        # Upload to S3 with delta compression
        s3_url = f"s3://{self.bucket}/postgres/{self.db_name}/{timestamp}/"
        summary = self.client.upload(str(backup_file), s3_url)

        # Log results
        self._log_backup(timestamp, summary)

        # Clean up local file
        backup_file.unlink()

        # Check if compression is effective
        if summary.is_delta and summary.delta_ratio > 0.2:
            self._alert_high_change_rate(timestamp, summary)

        return summary

    def backup_mysql(self, host, user, password, database):
        """Create and upload MySQL backup."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"{database}_{timestamp}.sql.gz"

        # Create database dump
        print(f"Creating MySQL backup of {database}...")
        cmd = (
            f"mysqldump -h {host} -u {user} -p{password} {database} | "
            f"gzip > {backup_file}"
        )
        subprocess.run(cmd, shell=True, check=True)

        # Upload to S3
        s3_url = f"s3://{self.bucket}/mysql/{database}/{timestamp}/"
        summary = self.client.upload(str(backup_file), s3_url)

        # Clean up
        backup_file.unlink()

        return summary

    def restore_backup(self, timestamp, output_path=None):
        """Download and restore a backup."""
        if output_path is None:
            output_path = self.backup_dir / f"restore_{timestamp}.sql.gz"

        s3_url = (
            f"s3://{self.bucket}/postgres/{self.db_name}/"
            f"{timestamp}/{self.db_name}_{timestamp}.sql.gz"
        )

        print(f"Downloading backup from {timestamp}...")
        self.client.download(s3_url, str(output_path))

        print(f"Backup downloaded to {output_path}")
        print("To restore: gunzip -c {output_path} | psql {connection_string}")

        return output_path

    def cleanup_old_backups(self, retention_days=30):
        """Remove backups older than retention period."""
        # This would typically list S3 objects and delete old ones
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        print(f"Cleaning up backups older than {cutoff_date}")
        # Implementation would go here

    def _log_backup(self, timestamp, summary):
        """Log backup metrics."""
        print(f"Backup {timestamp} completed:")
        print(f"  Original size: {summary.original_size_mb:.1f} MB")
        print(f"  Stored size: {summary.stored_size_mb:.1f} MB")
        print(f"  Compression: {summary.savings_percent:.0f}%")
        print(f"  Type: {'Delta' if summary.is_delta else 'Full'}")

    def _alert_high_change_rate(self, timestamp, summary):
        """Alert when database changes are unusually high."""
        print(f"⚠️ High change rate detected in backup {timestamp}")
        print(f"  Delta ratio: {summary.delta_ratio:.2%}")
        print("  This may indicate significant database changes")

# Usage
backup_manager = DatabaseBackupManager("production_db")

# Daily backup job
summary = backup_manager.backup_postgres(
    connection_string="postgresql://user:pass@localhost/mydb"
)

# Restore a specific backup
backup_manager.restore_backup("20240115_020000")

# Clean up old backups
backup_manager.cleanup_old_backups(retention_days=30)
```

## CI/CD Pipeline Integration

### GitHub Actions Integration

```python
# ci_deploy.py - CI/CD deployment script

from deltaglider import create_client
import os
import sys
from pathlib import Path
import hashlib
import json

class CIDeployment:
    def __init__(self):
        self.client = create_client()
        self.build_info = self._get_build_info()

    def _get_build_info(self):
        """Get build information from environment."""
        return {
            "commit": os.environ.get("GITHUB_SHA", "unknown"),
            "branch": os.environ.get("GITHUB_REF_NAME", "unknown"),
            "run_id": os.environ.get("GITHUB_RUN_ID", "unknown"),
            "actor": os.environ.get("GITHUB_ACTOR", "unknown"),
        }

    def deploy_artifacts(self, artifact_dir="dist"):
        """Deploy all build artifacts with delta compression."""
        artifact_path = Path(artifact_dir)
        results = []

        for artifact in artifact_path.glob("*"):
            if artifact.is_file():
                result = self._deploy_single_artifact(artifact)
                results.append(result)

        self._generate_deployment_report(results)
        return results

    def _deploy_single_artifact(self, artifact_path):
        """Deploy a single artifact."""
        # Generate unique key based on content
        file_hash = self._calculate_hash(artifact_path)[:8]

        # Construct S3 path
        s3_url = (
            f"s3://artifacts/"
            f"{self.build_info['branch']}/"
            f"{self.build_info['commit'][:8]}/"
            f"{artifact_path.name}"
        )

        # Upload with delta compression
        summary = self.client.upload(str(artifact_path), s3_url)

        return {
            "file": artifact_path.name,
            "hash": file_hash,
            "s3_url": s3_url,
            "original_size": summary.original_size,
            "stored_size": summary.stored_size,
            "compression": summary.savings_percent,
            "is_delta": summary.is_delta,
        }

    def _calculate_hash(self, file_path):
        """Calculate SHA256 hash of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _generate_deployment_report(self, results):
        """Generate deployment report."""
        total_original = sum(r["original_size"] for r in results)
        total_stored = sum(r["stored_size"] for r in results)
        total_saved = total_original - total_stored

        report = {
            "build_info": self.build_info,
            "artifacts": results,
            "summary": {
                "total_artifacts": len(results),
                "total_original_size": total_original,
                "total_stored_size": total_stored,
                "total_saved": total_saved,
                "compression_percent": (total_saved / total_original * 100)
                                      if total_original > 0 else 0
            }
        }

        # Save report
        with open("deployment_report.json", "w") as f:
            json.dump(report, f, indent=2)

        # Print summary
        print(f"Deployed {len(results)} artifacts")
        print(f"Total compression: {report['summary']['compression_percent']:.1f}%")
        print(f"Storage saved: {total_saved / (1024*1024):.2f} MB")

    def promote_to_production(self, commit_sha):
        """Promote a specific build to production."""
        source_prefix = f"s3://artifacts/main/{commit_sha[:8]}/"
        dest_prefix = f"s3://artifacts/production/latest/"

        # This would copy artifacts from staging to production
        print(f"Promoting {commit_sha[:8]} to production")
        # Implementation would go here

# Usage in CI/CD pipeline
if __name__ == "__main__":
    deployer = CIDeployment()

    # Deploy artifacts from build
    results = deployer.deploy_artifacts("dist")

    # Exit with appropriate code
    if all(r["is_delta"] or r["compression"] > 0 for r in results):
        print("✅ Deployment successful with compression")
        sys.exit(0)
    else:
        print("⚠️ Deployment completed but compression was not effective")
        sys.exit(1)
```

## Container Registry Storage

### Docker Image Layer Management

```python
from deltaglider import create_client
import docker
import tarfile
import tempfile
from pathlib import Path

class ContainerRegistry:
    def __init__(self, registry_bucket="container-registry"):
        self.client = create_client()
        self.docker_client = docker.from_env()
        self.bucket = registry_bucket

    def push_image(self, image_name, tag="latest"):
        """Push Docker image with delta compression for layers."""
        image = self.docker_client.images.get(f"{image_name}:{tag}")

        # Export image to tar
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = Path(tmpdir) / f"{image_name}-{tag}.tar"

            print(f"Exporting {image_name}:{tag}...")
            with open(tar_path, "wb") as f:
                for chunk in image.save():
                    f.write(chunk)

            # Extract and upload layers
            self._process_layers(tar_path, image_name, tag)

    def _process_layers(self, tar_path, image_name, tag):
        """Extract and upload individual layers with compression."""
        with tempfile.TemporaryDirectory() as extract_dir:
            extract_path = Path(extract_dir)

            # Extract tar
            with tarfile.open(tar_path, "r") as tar:
                tar.extractall(extract_path)

            # Process each layer
            layers_dir = extract_path / "layers"
            if layers_dir.exists():
                for layer_file in layers_dir.glob("*.tar"):
                    self._upload_layer(layer_file, image_name, tag)

    def _upload_layer(self, layer_path, image_name, tag):
        """Upload a single layer with delta compression."""
        layer_id = layer_path.stem
        s3_url = f"s3://{self.bucket}/{image_name}/{tag}/{layer_id}/"

        summary = self.client.upload(str(layer_path), s3_url)

        print(f"Layer {layer_id[:12]}: {summary.savings_percent:.0f}% compression")

        return summary

    def pull_image(self, image_name, tag="latest", output_dir="."):
        """Pull and reconstruct Docker image."""
        # Download all layers
        layers_prefix = f"s3://{self.bucket}/{image_name}/{tag}/"
        output_path = Path(output_dir) / f"{image_name}-{tag}.tar"

        # This would download and reconstruct the image
        print(f"Pulling {image_name}:{tag}...")
        # Implementation would download layers and reconstruct tar

# Usage
registry = ContainerRegistry()

# Push image with layer compression
registry.push_image("myapp", "v2.0.0")
# Typical output:
# Layer abc123def456: 99% compression (base layer)
# Layer 789ghi012jkl: 95% compression (app code changes)
# Layer mno345pqr678: 98% compression (config changes)
```

## Machine Learning Model Versioning

### Model Checkpoint Management

```python
from deltaglider import create_client
import pickle
import json
import numpy as np
from datetime import datetime
from pathlib import Path

class ModelVersionControl:
    def __init__(self, project_name, bucket="ml-models"):
        self.client = create_client()
        self.project = project_name
        self.bucket = bucket
        self.metadata = {}

    def save_checkpoint(self, model, epoch, metrics, optimizer_state=None):
        """Save model checkpoint with delta compression."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        checkpoint_name = f"{self.project}_epoch{epoch}_{timestamp}"

        # Serialize model
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            checkpoint = {
                "model_state": model.state_dict() if hasattr(model, 'state_dict') else model,
                "epoch": epoch,
                "metrics": metrics,
                "optimizer_state": optimizer_state,
                "timestamp": timestamp
            }
            pickle.dump(checkpoint, tmp)
            tmp_path = tmp.name

        # Upload with compression
        s3_url = f"s3://{self.bucket}/{self.project}/checkpoints/epoch_{epoch}/"
        summary = self.client.upload(tmp_path, s3_url)

        # Track metadata
        self.metadata[checkpoint_name] = {
            "epoch": epoch,
            "metrics": metrics,
            "size_original": summary.original_size,
            "size_stored": summary.stored_size,
            "compression": summary.savings_percent,
            "is_delta": summary.is_delta,
            "timestamp": timestamp
        }

        # Clean up
        Path(tmp_path).unlink()

        self._log_checkpoint(epoch, metrics, summary)

        return checkpoint_name

    def load_checkpoint(self, epoch=None, checkpoint_name=None):
        """Load a specific checkpoint."""
        if checkpoint_name:
            # Load by name
            info = self.metadata.get(checkpoint_name)
            if not info:
                raise ValueError(f"Checkpoint {checkpoint_name} not found")
            epoch = info["epoch"]
        elif epoch is None:
            # Load latest
            epoch = self._get_latest_epoch()

        # Download checkpoint
        s3_url = f"s3://{self.bucket}/{self.project}/checkpoints/epoch_{epoch}/"

        with tempfile.NamedTemporaryFile(suffix=".pkl") as tmp:
            self.client.download(s3_url + "checkpoint.pkl", tmp.name)

            with open(tmp.name, "rb") as f:
                checkpoint = pickle.load(f)

        return checkpoint

    def save_production_model(self, model, version, metrics):
        """Save production-ready model version."""
        model_file = f"{self.project}_v{version}.pkl"

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            pickle.dump(model, tmp)
            tmp_path = tmp.name

        # Upload to production prefix
        s3_url = f"s3://{self.bucket}/{self.project}/production/v{version}/"
        summary = self.client.upload(tmp_path, s3_url)

        # Save metadata
        metadata = {
            "version": version,
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat(),
            "compression": summary.savings_percent,
            "size_mb": summary.original_size_mb
        }

        # Save metadata file
        metadata_path = Path(tmp_path).parent / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        # Clean up
        Path(tmp_path).unlink()

        print(f"Model v{version} saved with {summary.savings_percent:.0f}% compression")

        return summary

    def compare_checkpoints(self, epoch1, epoch2):
        """Compare metrics between two checkpoints."""
        cp1 = self.metadata.get(f"{self.project}_epoch{epoch1}")
        cp2 = self.metadata.get(f"{self.project}_epoch{epoch2}")

        if not cp1 or not cp2:
            raise ValueError("One or both checkpoints not found")

        comparison = {
            "epoch1": epoch1,
            "epoch2": epoch2,
            "metrics_diff": {},
            "size_diff_mb": (cp2["size_original"] - cp1["size_original"]) / (1024*1024),
            "compression_diff": cp2["compression"] - cp1["compression"]
        }

        # Compare metrics
        for key in cp1["metrics"]:
            if key in cp2["metrics"]:
                comparison["metrics_diff"][key] = cp2["metrics"][key] - cp1["metrics"][key]

        return comparison

    def _log_checkpoint(self, epoch, metrics, summary):
        """Log checkpoint information."""
        print(f"Checkpoint saved - Epoch {epoch}:")
        print(f"  Metrics: {metrics}")
        print(f"  Compression: {summary.savings_percent:.0f}%")
        print(f"  Storage saved: {(summary.original_size - summary.stored_size) / (1024*1024):.2f} MB")

    def _get_latest_epoch(self):
        """Get the latest epoch number."""
        if not self.metadata:
            return 0

        epochs = [m["epoch"] for m in self.metadata.values()]
        return max(epochs) if epochs else 0

# Usage
model_vc = ModelVersionControl("sentiment_analyzer")

# Training loop with checkpoint saving
for epoch in range(100):
    # Training code here...
    metrics = {
        "loss": 0.05 * (100 - epoch),  # Simulated improving loss
        "accuracy": 0.80 + (epoch * 0.002),
        "val_loss": 0.06 * (100 - epoch),
        "val_accuracy": 0.78 + (epoch * 0.002)
    }

    # Save checkpoint every 10 epochs
    if epoch % 10 == 0:
        model = {"weights": np.random.randn(1000, 1000)}  # Simulated model
        checkpoint = model_vc.save_checkpoint(
            model, epoch, metrics,
            optimizer_state={"lr": 0.001}
        )

        # Compression gets better as models are similar
        # Epoch 0: Stored as reference
        # Epoch 10: 95% compression
        # Epoch 20: 98% compression

# Save production model
model_vc.save_production_model(model, version="1.0.0", metrics=metrics)
```

## Game Asset Distribution

### Game Update System

```python
from deltaglider import create_client
import hashlib
import json
from pathlib import Path
from typing import Dict, List

class GameAssetManager:
    def __init__(self, game_id, platform="pc"):
        self.client = create_client()
        self.game_id = game_id
        self.platform = platform
        self.manifest_cache = {}

    def create_update_package(self, version, asset_dir):
        """Create and upload game update package."""
        assets_path = Path(asset_dir)
        manifest = {
            "version": version,
            "platform": self.platform,
            "files": []
        }

        # Process each asset file
        for asset_file in assets_path.rglob("*"):
            if asset_file.is_file():
                result = self._upload_asset(asset_file, version, assets_path)
                manifest["files"].append(result)

        # Save manifest
        self._save_manifest(version, manifest)

        # Calculate total savings
        total_original = sum(f["original_size"] for f in manifest["files"])
        total_stored = sum(f["stored_size"] for f in manifest["files"])

        print(f"Update package v{version} created:")
        print(f"  Files: {len(manifest['files'])}")
        print(f"  Original size: {total_original / (1024**3):.2f} GB")
        print(f"  Stored size: {total_stored / (1024**3):.2f} GB")
        print(f"  Compression: {(1 - total_stored/total_original) * 100:.1f}%")

        return manifest

    def _upload_asset(self, file_path, version, base_path):
        """Upload a single game asset."""
        relative_path = file_path.relative_to(base_path)

        # Determine asset type for optimal compression
        asset_type = self._get_asset_type(file_path)

        s3_url = f"s3://game-assets/{self.game_id}/{version}/{relative_path}"

        # Upload with delta compression
        summary = self.client.upload(str(file_path), s3_url)

        return {
            "path": str(relative_path),
            "type": asset_type,
            "hash": self._calculate_hash(file_path),
            "original_size": summary.original_size,
            "stored_size": summary.stored_size,
            "is_delta": summary.is_delta,
            "compression": summary.savings_percent
        }

    def download_update(self, from_version, to_version, output_dir):
        """Download update package for client."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Get manifest for target version
        manifest = self._load_manifest(to_version)

        downloaded = []
        for file_info in manifest["files"]:
            # Download file
            s3_url = f"s3://game-assets/{self.game_id}/{to_version}/{file_info['path']}"
            local_path = output_path / file_info["path"]
            local_path.parent.mkdir(parents=True, exist_ok=True)

            self.client.download(s3_url, str(local_path))

            # Verify integrity
            if self._calculate_hash(local_path) != file_info["hash"]:
                raise ValueError(f"Integrity check failed for {file_info['path']}")

            downloaded.append(file_info["path"])

        print(f"Downloaded {len(downloaded)} files for update {from_version} -> {to_version}")

        return downloaded

    def create_delta_patch(self, from_version, to_version):
        """Create minimal patch between versions."""
        from_manifest = self._load_manifest(from_version)
        to_manifest = self._load_manifest(to_version)

        # Find changed files
        from_files = {f["path"]: f["hash"] for f in from_manifest["files"]}
        to_files = {f["path"]: f for f in to_manifest["files"]}

        patch_files = []
        for path, file_info in to_files.items():
            if path not in from_files or from_files[path] != file_info["hash"]:
                patch_files.append(file_info)

        patch_size = sum(f["stored_size"] for f in patch_files)

        print(f"Delta patch {from_version} -> {to_version}:")
        print(f"  Changed files: {len(patch_files)}")
        print(f"  Patch size: {patch_size / (1024*1024):.2f} MB")

        return patch_files

    def _get_asset_type(self, file_path):
        """Determine asset type from file extension."""
        ext = file_path.suffix.lower()

        type_map = {
            ".pak": "archive",
            ".zip": "archive",
            ".png": "texture",
            ".jpg": "texture",
            ".dds": "texture",
            ".wav": "audio",
            ".ogg": "audio",
            ".mp3": "audio",
            ".fbx": "model",
            ".obj": "model",
            ".json": "data",
            ".xml": "data",
        }

        return type_map.get(ext, "other")

    def _calculate_hash(self, file_path):
        """Calculate file hash for integrity check."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(8192), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _save_manifest(self, version, manifest):
        """Save version manifest."""
        self.manifest_cache[version] = manifest
        # Would typically save to S3

    def _load_manifest(self, version):
        """Load version manifest."""
        if version in self.manifest_cache:
            return self.manifest_cache[version]
        # Would typically load from S3
        return {}

# Usage
asset_manager = GameAssetManager("rpg_adventure")

# Create update packages
v1_manifest = asset_manager.create_update_package("1.0.0", "game_files/v1.0.0")
# Output: 15 GB of assets stored in 15 GB (first version, no compression)

v1_1_manifest = asset_manager.create_update_package("1.1.0", "game_files/v1.1.0")
# Output: 15.5 GB of assets stored in 0.5 GB (97% compression!)

# Create delta patch
patch_files = asset_manager.create_delta_patch("1.0.0", "1.1.0")
# Output: 45 changed files, patch size: 487 MB

# Download update for client
asset_manager.download_update("1.0.0", "1.1.0", "client_update")
```

## Log Archive Management

### Compressed Log Storage

```python
from deltaglider import create_client
import gzip
import json
from datetime import datetime, timedelta
from pathlib import Path

class LogArchiver:
    def __init__(self, service_name, bucket="logs"):
        self.client = create_client(log_level="WARNING")  # Quiet mode for log archival
        self.service = service_name
        self.bucket = bucket

    def archive_logs(self, log_dir, older_than_hours=24):
        """Archive logs older than specified hours."""
        log_path = Path(log_dir)
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)

        archived_count = 0
        total_saved = 0

        for log_file in log_path.glob("*.log"):
            # Check file age
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)

            if file_time < cutoff_time:
                # Compress and archive
                summary = self._archive_single_log(log_file)

                archived_count += 1
                total_saved += (summary.original_size - summary.stored_size)

                # Remove local file after successful archive
                log_file.unlink()

        print(f"Archived {archived_count} logs, saved {total_saved / (1024*1024):.2f} MB")

        return archived_count, total_saved

    def _archive_single_log(self, log_file):
        """Archive a single log file."""
        # Parse log date from filename (assuming format: service_YYYYMMDD.log)
        date_str = log_file.stem.split("_")[-1]

        try:
            log_date = datetime.strptime(date_str, "%Y%m%d")
            year = log_date.year
            month = log_date.month
            day = log_date.day
        except:
            # Fallback to file modification time
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            year = file_time.year
            month = file_time.month
            day = file_time.day

        # Compress log file
        compressed_path = log_file.with_suffix(".log.gz")
        with open(log_file, "rb") as f_in:
            with gzip.open(compressed_path, "wb") as f_out:
                f_out.writelines(f_in)

        # Upload with delta compression
        s3_url = f"s3://{self.bucket}/{self.service}/{year}/{month:02d}/{day:02d}/"
        summary = self.client.upload(str(compressed_path), s3_url)

        # Clean up compressed file
        compressed_path.unlink()

        return summary

    def search_logs(self, date_range, search_term=None):
        """Search archived logs for specific content."""
        start_date, end_date = date_range

        results = []
        current_date = start_date

        while current_date <= end_date:
            # Download logs for this date
            s3_prefix = (
                f"s3://{self.bucket}/{self.service}/"
                f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}/"
            )

            # Download and search
            # Implementation would download and search logs

            current_date += timedelta(days=1)

        return results

    def get_storage_stats(self, year=None, month=None):
        """Get storage statistics for archived logs."""
        # Would query S3 for storage metrics
        stats = {
            "total_files": 0,
            "total_original_size": 0,
            "total_stored_size": 0,
            "compression_rate": 0,
            "by_month": {}
        }

        return stats

# Usage
archiver = LogArchiver("web-api")

# Archive logs older than 24 hours
count, saved = archiver.archive_logs("/var/log/myapp", older_than_hours=24)

# Schedule this to run daily via cron:
# 0 2 * * * python3 /opt/scripts/archive_logs.py
```

## Multi-Region Replication

### Cross-Region Backup System

```python
from deltaglider import create_client
import concurrent.futures
from typing import List, Dict

class MultiRegionReplicator:
    def __init__(self, regions: List[str]):
        """Initialize clients for multiple regions."""
        self.clients = {}
        self.primary_region = regions[0]

        for region in regions:
            # Create client for each region
            self.clients[region] = create_client(
                # Region-specific endpoint if needed
                log_level="INFO"
            )

    def replicate_object(self, source_bucket, key, target_regions=None):
        """Replicate an object across regions with delta compression."""
        if target_regions is None:
            target_regions = [r for r in self.clients.keys() if r != self.primary_region]

        source_url = f"s3://{source_bucket}/{key}"
        results = {}

        # Download from primary region once
        with tempfile.NamedTemporaryFile() as tmp:
            self.clients[self.primary_region].download(source_url, tmp.name)

            # Upload to each target region in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(target_regions)) as executor:
                futures = {
                    executor.submit(
                        self._replicate_to_region,
                        tmp.name,
                        region,
                        source_bucket,
                        key
                    ): region
                    for region in target_regions
                }

                for future in concurrent.futures.as_completed(futures):
                    region = futures[future]
                    try:
                        results[region] = future.result()
                    except Exception as e:
                        results[region] = {"error": str(e)}

        return results

    def _replicate_to_region(self, file_path, region, bucket, key):
        """Replicate file to a specific region."""
        target_url = f"s3://{bucket}-{region}/{key}"

        summary = self.clients[region].upload(file_path, target_url)

        return {
            "region": region,
            "url": target_url,
            "compression": summary.savings_percent,
            "is_delta": summary.is_delta
        }

    def verify_replication(self, bucket, key):
        """Verify object exists in all regions."""
        verification = {}

        for region, client in self.clients.items():
            region_bucket = bucket if region == self.primary_region else f"{bucket}-{region}"
            s3_url = f"s3://{region_bucket}/{key}"

            try:
                is_valid = client.verify(s3_url)
                verification[region] = {"exists": True, "valid": is_valid}
            except:
                verification[region] = {"exists": False, "valid": False}

        return verification

# Usage
replicator = MultiRegionReplicator(["us-east-1", "eu-west-1", "ap-southeast-1"])

# Replicate critical backup
results = replicator.replicate_object("backups", "database/prod_20240115.sql.gz")

# Verify replication
status = replicator.verify_replication("backups", "database/prod_20240115.sql.gz")
for region, info in status.items():
    print(f"{region}: {'✓' if info['valid'] else '✗'}")
```

## Best Practices

### Error Handling and Retry Logic

```python
from deltaglider import create_client
import time
from functools import wraps

def retry_with_backoff(retries=3, backoff_factor=2):
    """Decorator for retry with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        raise

                    wait_time = backoff_factor ** attempt
                    print(f"Attempt {attempt + 1} failed: {e}")
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

            return None
        return wrapper
    return decorator

class RobustUploader:
    def __init__(self):
        self.client = create_client()

    @retry_with_backoff(retries=3)
    def upload_with_retry(self, file_path, s3_url):
        """Upload with automatic retry on failure."""
        return self.client.upload(file_path, s3_url)

    def upload_batch(self, files_and_urls):
        """Upload multiple files with error tracking."""
        results = {
            "successful": [],
            "failed": []
        }

        for file_path, s3_url in files_and_urls:
            try:
                summary = self.upload_with_retry(file_path, s3_url)
                results["successful"].append({
                    "file": file_path,
                    "url": s3_url,
                    "compression": summary.savings_percent
                })
            except Exception as e:
                results["failed"].append({
                    "file": file_path,
                    "url": s3_url,
                    "error": str(e)
                })

        # Report results
        print(f"Uploaded: {len(results['successful'])}/{len(files_and_urls)}")

        if results["failed"]:
            print("Failed uploads:")
            for failure in results["failed"]:
                print(f"  {failure['file']}: {failure['error']}")

        return results

# Usage
uploader = RobustUploader()

files_to_upload = [
    ("build1.zip", "s3://artifacts/build1/"),
    ("build2.zip", "s3://artifacts/build2/"),
    ("build3.zip", "s3://artifacts/build3/"),
]

results = uploader.upload_batch(files_to_upload)
```

These examples demonstrate real-world usage patterns for DeltaGlider across various domains. Each example includes error handling, monitoring, and best practices for production deployments.