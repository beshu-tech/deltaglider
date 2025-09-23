# DeltaGlider Architecture

Understanding how DeltaGlider achieves 99.9% compression through intelligent binary delta compression.

## Table of Contents

1. [Overview](#overview)
2. [Hexagonal Architecture](#hexagonal-architecture)
3. [Core Concepts](#core-concepts)
4. [Compression Algorithm](#compression-algorithm)
5. [Storage Strategy](#storage-strategy)
6. [Performance Optimizations](#performance-optimizations)
7. [Security & Integrity](#security--integrity)
8. [Comparison with Alternatives](#comparison-with-alternatives)

## Overview

DeltaGlider is built on a simple yet powerful idea: **most versioned files share 99% of their content**. Instead of storing complete files repeatedly, we store one reference file and only the differences (deltas) for similar files.

### High-Level Flow

```
First Upload (v1.0.0):
┌──────────┐        ┌─────────────┐       ┌──────┐
│  100MB   │───────▶│ DeltaGlider │──────▶│  S3  │
│   File   │        │             │       │100MB │
└──────────┘        └─────────────┘       └──────┘

Second Upload (v1.0.1):
┌──────────┐        ┌─────────────┐       ┌──────┐
│  100MB   │───────▶│ DeltaGlider │──────▶│  S3  │
│   File   │        │   (xdelta3) │       │ 98KB │
└──────────┘        └─────────────┘       └──────┘
                           │
                    Creates 98KB delta
                    by comparing with
                    v1.0.0 reference
```

## Hexagonal Architecture

DeltaGlider follows the hexagonal (ports and adapters) architecture pattern for maximum flexibility and testability.

### Architecture Diagram

```
                    ┌─────────────────┐
                    │   Application   │
                    │   (CLI / SDK)   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │                 │
                    │   DeltaService  │
                    │   (Core Logic)  │
                    │                 │
                    └────┬─────┬──────┘
                         │     │
              ┌──────────▼─┬───▼──────────┐
              │            │              │
              │   Ports    │    Ports    │
              │ (Interfaces)│ (Interfaces)│
              │            │              │
              └──────┬─────┴────┬─────────┘
                     │          │
         ┌───────────▼──┐  ┌───▼───────────┐
         │              │  │               │
         │   Adapters   │  │   Adapters   │
         │              │  │               │
         ├──────────────┤  ├───────────────┤
         │ S3Storage    │  │ XdeltaDiff   │
         │ Sha256Hash   │  │ FsCache      │
         │ UtcClock     │  │ StdLogger    │
         │ NoopMetrics  │  │              │
         └──────────────┘  └───────────────┘
                │                 │
         ┌──────▼─────┐    ┌─────▼──────┐
         │    AWS     │    │   xdelta3  │
         │     S3     │    │   binary   │
         └────────────┘    └────────────┘
```

### Ports (Interfaces)

Ports define contracts that adapters must implement:

```python
# StoragePort - Abstract S3 operations
class StoragePort(Protocol):
    def put_object(self, bucket: str, key: str, data: bytes, metadata: Dict) -> None
    def get_object(self, bucket: str, key: str) -> Tuple[bytes, Dict]
    def object_exists(self, bucket: str, key: str) -> bool
    def delete_object(self, bucket: str, key: str) -> None

# DiffPort - Abstract delta operations
class DiffPort(Protocol):
    def create_delta(self, reference: bytes, target: bytes) -> bytes
    def apply_delta(self, reference: bytes, delta: bytes) -> bytes

# HashPort - Abstract integrity checks
class HashPort(Protocol):
    def hash(self, data: bytes) -> str
    def hash_file(self, path: Path) -> str

# CachePort - Abstract local caching
class CachePort(Protocol):
    def get(self, key: str) -> Optional[Path]
    def put(self, key: str, path: Path) -> None
    def exists(self, key: str) -> bool
```

### Adapters (Implementations)

Adapters provide concrete implementations:

- **S3StorageAdapter**: Uses boto3 for S3 operations
- **XdeltaAdapter**: Wraps xdelta3 binary for delta compression
- **Sha256Adapter**: Provides SHA256 hashing
- **FsCacheAdapter**: File system based reference cache
- **UtcClockAdapter**: UTC timestamp provider
- **StdLoggerAdapter**: Console logging

### Benefits

1. **Testability**: Mock any adapter for unit testing
2. **Flexibility**: Swap implementations (e.g., different storage backends)
3. **Separation**: Business logic isolated from infrastructure
4. **Extensibility**: Add new adapters without changing core

## Core Concepts

### DeltaSpace

A DeltaSpace is an S3 prefix containing related files that share a common reference:

```python
@dataclass
class DeltaSpace:
    bucket: str  # S3 bucket
    prefix: str  # Prefix for related files

# Example:
# DeltaSpace(bucket="releases", prefix="myapp/v1/")
# Contains:
#   - reference.bin (first uploaded file)
#   - file1.zip.delta
#   - file2.zip.delta
```

### Reference File

The first file uploaded to a DeltaSpace becomes the reference:

```
s3://bucket/prefix/reference.bin       # Full file (e.g., 100MB)
s3://bucket/prefix/reference.bin.sha256 # Integrity checksum
```

### Delta Files

Subsequent files are stored as deltas:

```
s3://bucket/prefix/myfile.zip.delta    # Delta file (e.g., 98KB)

Metadata (S3 tags):
  - original_name: myfile.zip
  - original_size: 104857600
  - original_hash: abc123...
  - reference_hash: def456...
  - tool_version: deltaglider/0.1.0
```

## Compression Algorithm

### xdelta3: The Secret Sauce

DeltaGlider uses [xdelta3](http://xdelta.org/), a binary diff algorithm optimized for large files:

#### How xdelta3 Works

1. **Rolling Hash**: Scans reference file with a rolling hash window
2. **Block Matching**: Finds matching byte sequences at any offset
3. **Instruction Stream**: Generates copy/insert instructions
4. **Compression**: Further compresses the instruction stream

```
Original: ABCDEFGHIJKLMNOP
Modified: ABCXYZGHIJKLMNOP

Delta instructions:
- COPY 0-2 (ABC)       # Copy bytes 0-2 from reference
- INSERT XYZ           # Insert new bytes
- COPY 6-15 (GHIJKLMNOP) # Copy bytes 6-15 from reference

Delta size: ~10 bytes instead of 16 bytes
```

#### Why xdelta3 Excels at Archives

Archive files (ZIP, TAR, JAR) have predictable structure:

```
ZIP Structure:
┌─────────────┐
│  Headers    │ ← Usually identical between versions
├─────────────┤
│  File 1     │ ← May be unchanged
├─────────────┤
│  File 2     │ ← Small change
├─────────────┤
│  File 3     │ ← May be unchanged
├─────────────┤
│  Directory  │ ← Structure mostly same
└─────────────┘
```

Even when one file changes inside the archive, xdelta3 can:
- Identify unchanged sections (even if byte positions shift)
- Compress repeated patterns efficiently
- Handle binary data optimally

### Intelligent File Type Detection

```python
def should_use_delta(file_path: Path) -> bool:
    """Determine if file should use delta compression."""

    # File size check
    if file_path.stat().st_size < 1_000_000:  # < 1MB
        return False  # Overhead not worth it

    # Extension-based detection
    DELTA_EXTENSIONS = {
        '.zip', '.tar', '.gz', '.tgz', '.bz2',  # Archives
        '.jar', '.war', '.ear',                  # Java
        '.dmg', '.pkg', '.deb', '.rpm',         # Packages
        '.iso', '.img', '.vhd',                 # Disk images
    }

    DIRECT_EXTENSIONS = {
        '.txt', '.md', '.json', '.xml',         # Text (use gzip)
        '.jpg', '.png', '.mp4',                 # Media (already compressed)
        '.sha1', '.sha256', '.md5',             # Checksums (unique)
    }

    ext = file_path.suffix.lower()

    if ext in DELTA_EXTENSIONS:
        return True
    elif ext in DIRECT_EXTENSIONS:
        return False
    else:
        # Unknown type - use heuristic
        return is_likely_archive(file_path)
```

## Storage Strategy

### S3 Object Layout

```
bucket/
├── releases/
│   ├── v1.0.0/
│   │   ├── reference.bin          # First uploaded file (full)
│   │   ├── reference.bin.sha256   # Checksum
│   │   ├── app-linux.tar.gz.delta # Delta from reference
│   │   ├── app-mac.dmg.delta      # Delta from reference
│   │   └── app-win.zip.delta      # Delta from reference
│   ├── v1.0.1/
│   │   ├── reference.bin          # New reference for this version
│   │   └── ...
│   └── v1.1.0/
│       └── ...
└── backups/
    └── ...
```

### Metadata Strategy

DeltaGlider stores metadata in S3 object tags/metadata:

```python
# For delta files
metadata = {
    "x-amz-meta-original-name": "app.zip",
    "x-amz-meta-original-size": "104857600",
    "x-amz-meta-original-hash": "sha256:abc123...",
    "x-amz-meta-reference-hash": "sha256:def456...",
    "x-amz-meta-tool-version": "deltaglider/0.1.0",
    "x-amz-meta-compression-ratio": "0.001",  # 0.1% of original
}
```

Benefits:
- No separate metadata store needed
- Atomic operations (metadata stored with object)
- Works with S3 versioning and lifecycle policies
- Queryable via S3 API

### Local Cache Strategy

```
/tmp/.deltaglider/cache/
├── references/
│   ├── sha256_abc123.bin    # Cached reference files
│   ├── sha256_def456.bin
│   └── ...
└── metadata.json             # Cache index
```

Cache benefits:
- Avoid repeated reference downloads
- Speed up delta creation for multiple files
- Reduce S3 API calls and bandwidth

## Performance Optimizations

### 1. Reference Caching

```python
class FsCacheAdapter:
    def get_reference(self, hash: str) -> Optional[Path]:
        cache_path = self.cache_dir / f"sha256_{hash}.bin"
        if cache_path.exists():
            # Verify integrity
            if self.verify_hash(cache_path, hash):
                return cache_path
        return None

    def put_reference(self, hash: str, path: Path) -> None:
        cache_path = self.cache_dir / f"sha256_{hash}.bin"
        shutil.copy2(path, cache_path)
        # Update cache index
        self.update_index(hash, cache_path)
```

### 2. Streaming Operations

For large files, DeltaGlider uses streaming:

```python
def upload_large_file(file_path: Path, s3_url: str):
    # Stream file to S3 using multipart upload
    with open(file_path, 'rb') as f:
        # boto3 automatically uses multipart for large files
        s3.upload_fileobj(f, bucket, key,
                         Config=TransferConfig(
                             multipart_threshold=1024 * 25,  # 25MB
                             max_concurrency=10,
                             use_threads=True))
```

### 3. Parallel Processing

```python
def process_batch(files: List[Path]):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for file in files:
            future = executor.submit(process_file, file)
            futures.append(future)

        for future in as_completed(futures):
            result = future.result()
            print(f"Processed: {result}")
```

### 4. Delta Ratio Optimization

```python
def optimize_compression(file: Path, reference: Path) -> bytes:
    # Create delta
    delta = create_delta(reference, file)

    # Check compression effectiveness
    ratio = len(delta) / file.stat().st_size

    if ratio > MAX_RATIO:  # Default: 0.5 (50%)
        # Delta too large, store original
        return None
    else:
        # Good compression, use delta
        return delta
```

## Security & Integrity

### SHA256 Verification

Every operation includes checksum verification:

```python
def verify_integrity(data: bytes, expected_hash: str) -> bool:
    actual_hash = hashlib.sha256(data).hexdigest()
    return actual_hash == expected_hash

# Upload flow
file_hash = calculate_hash(file)
upload_to_s3(file, metadata={"hash": file_hash})

# Download flow
data, metadata = download_from_s3(key)
if not verify_integrity(data, metadata["hash"]):
    raise IntegrityError("File corrupted")
```

### Atomic Operations

All S3 operations are atomic:

```python
def atomic_upload(file: Path, bucket: str, key: str):
    try:
        # Upload to temporary key
        temp_key = f"{key}.tmp.{uuid.uuid4()}"
        s3.upload_file(file, bucket, temp_key)

        # Atomic rename (S3 copy + delete)
        s3.copy_object(
            CopySource={'Bucket': bucket, 'Key': temp_key},
            Bucket=bucket,
            Key=key
        )
        s3.delete_object(Bucket=bucket, Key=temp_key)

    except Exception:
        # Cleanup on failure
        try:
            s3.delete_object(Bucket=bucket, Key=temp_key)
        except:
            pass
        raise
```

### Encryption Support

DeltaGlider respects S3 encryption settings:

```python
# Server-side encryption with S3-managed keys
s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=data,
    ServerSideEncryption='AES256'
)

# Server-side encryption with KMS
s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=data,
    ServerSideEncryption='aws:kms',
    SSEKMSKeyId='arn:aws:kms:...'
)
```

## Comparison with Alternatives

### vs. S3 Versioning

| Aspect | DeltaGlider | S3 Versioning |
|--------|-------------|---------------|
| Storage | Only stores deltas | Stores full copies |
| Compression | 99%+ for similar files | 0% |
| Cost | Minimal | $$ per version |
| Complexity | Transparent | Built-in |
| Recovery | Download + reconstruct | Direct download |

### vs. Git LFS

| Aspect | DeltaGlider | Git LFS |
|--------|-------------|---------|
| Use case | Any S3 storage | Git repositories |
| Compression | Binary delta | Deduplication |
| Integration | S3 API | Git workflow |
| Scalability | Unlimited | Repository-bound |

### vs. Deduplication Systems

| Aspect | DeltaGlider | Dedup Systems |
|--------|-------------|---------------|
| Approach | File-level delta | Block-level dedup |
| Compression | 99%+ for similar | 30-50% typical |
| Complexity | Simple | Complex |
| Cost | Open source | Enterprise $$$ |

### vs. Backup Tools (Restic/Borg)

| Aspect | DeltaGlider | Restic/Borg |
|--------|-------------|-------------|
| Purpose | S3 optimization | Full backup |
| Storage | S3-native | Custom format |
| Granularity | File-level | Repository |
| Use case | Artifacts/releases | System backups |

## Advanced Topics

### Reference Rotation Strategy

Currently, the first file becomes the permanent reference. Future versions may implement:

```python
class ReferenceRotationStrategy:
    def should_rotate(self, stats: ReferenceStats) -> bool:
        # Rotate if average delta ratio is too high
        if stats.avg_delta_ratio > 0.4:
            return True

        # Rotate if reference is too old
        if stats.age_days > 90:
            return True

        # Rotate if better candidate exists
        if stats.better_candidate_score > 0.8:
            return True

        return False

    def select_new_reference(self, files: List[FileStats]) -> Path:
        # Select file that minimizes total delta sizes
        best_score = float('inf')
        best_file = None

        for candidate in files:
            total_delta_size = sum(
                compute_delta_size(candidate, other)
                for other in files
                if other != candidate
            )
            if total_delta_size < best_score:
                best_score = total_delta_size
                best_file = candidate

        return best_file
```

### Multi-Reference Support

For diverse file sets, multiple references could be used:

```python
class MultiReferenceStrategy:
    def assign_reference(self, file: Path, references: List[Reference]) -> Reference:
        # Find best matching reference
        best_reference = None
        best_ratio = float('inf')

        for ref in references:
            delta = create_delta(ref.path, file)
            ratio = len(delta) / file.stat().st_size

            if ratio < best_ratio:
                best_ratio = ratio
                best_reference = ref

        # Create new reference if no good match
        if best_ratio > 0.5:
            return self.create_new_reference(file)

        return best_reference
```

### Incremental Delta Chains

For frequently updated files:

```python
class DeltaChain:
    """
    v1.0.0 (reference) <- v1.0.1 (delta) <- v1.0.2 (delta) <- v1.0.3 (delta)
    """
    def reconstruct(self, version: str) -> bytes:
        # Start with reference
        data = self.load_reference()

        # Apply deltas in sequence
        for delta in self.get_delta_chain(version):
            data = apply_delta(data, delta)

        return data
```

## Monitoring & Observability

### Metrics to Track

```python
@dataclass
class CompressionMetrics:
    total_uploads: int
    total_original_size: int
    total_stored_size: int
    average_compression_ratio: float
    delta_files_count: int
    reference_files_count: int
    cache_hit_rate: float
    average_upload_time: float
    average_download_time: float
    failed_compressions: int
```

### Health Checks

```python
class HealthCheck:
    def check_xdelta3(self) -> bool:
        """Verify xdelta3 binary is available."""
        return shutil.which('xdelta3') is not None

    def check_s3_access(self) -> bool:
        """Verify S3 credentials and permissions."""
        try:
            s3.list_buckets()
            return True
        except:
            return False

    def check_cache_space(self) -> bool:
        """Verify adequate cache space."""
        cache_dir = Path('/tmp/.deltaglider/cache')
        free_space = shutil.disk_usage(cache_dir).free
        return free_space > 1_000_000_000  # 1GB minimum
```

## Future Enhancements

1. **Cloud-Native Reference Management**: Store references in distributed cache
2. **Rust Implementation**: 10x performance improvement
3. **Automatic Similarity Detection**: ML-based reference selection
4. **Multi-Threaded Compression**: Parallel delta generation
5. **WASM Support**: Browser-based delta compression
6. **S3 Batch Operations**: Bulk compression of existing data
7. **Compression Prediction**: Estimate compression before upload
8. **Adaptive Strategies**: Auto-tune based on workload patterns

## Contributing

See [CONTRIBUTING.md](https://github.com/beshu-tech/deltaglider/blob/main/CONTRIBUTING.md) for development setup and guidelines.

## Additional Resources

- [xdelta3 Documentation](http://xdelta.org/)
- [S3 API Reference](https://docs.aws.amazon.com/s3/index.html)
- [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/)
- [Binary Diff Algorithms](https://en.wikipedia.org/wiki/Delta_encoding)