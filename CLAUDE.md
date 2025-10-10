# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DeltaGlider is a drop-in S3 replacement that achieves 99.9% compression for versioned artifacts through intelligent binary delta compression using xdelta3. It's designed to store 4TB of similar files in 5GB by storing only the differences between versions.

## Essential Commands

### Development Setup
```bash
# Install with development dependencies using uv (preferred)
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

### Testing
```bash
# Run all tests
uv run pytest

# Run unit tests only
uv run pytest tests/unit

# Run integration tests only
uv run pytest tests/integration

# Run a specific test file
uv run pytest tests/integration/test_full_workflow.py

# Run a specific test
uv run pytest tests/integration/test_full_workflow.py::test_full_put_get_workflow

# Run with verbose output
uv run pytest -v

# Run with coverage
uv run pytest --cov=deltaglider
```

### Code Quality
```bash
# Run linter (ruff)
uv run ruff check src/

# Fix linting issues automatically
uv run ruff check --fix src/

# Format code
uv run ruff format src/

# Type checking with mypy
uv run mypy src/

# Run all checks (linting + type checking)
uv run ruff check src/ && uv run mypy src/
```

### Local Testing with MinIO
```bash
# Start MinIO for local S3 testing
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Test with local MinIO
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# Now you can use deltaglider commands
deltaglider cp test.zip s3://test-bucket/
deltaglider stats test-bucket                  # Get bucket statistics
```

### Available CLI Commands
```bash
cp      # Copy files to/from S3 (AWS S3 compatible)
ls      # List S3 buckets or objects (AWS S3 compatible)
rm      # Remove S3 objects (AWS S3 compatible)
sync    # Synchronize directories with S3 (AWS S3 compatible)
stats   # Get bucket statistics and compression metrics
verify  # Verify integrity of delta file
```

## Architecture

### Hexagonal Architecture Pattern

The codebase follows a clean hexagonal (ports and adapters) architecture:

```
src/deltaglider/
├── core/           # Domain logic (pure Python, no external dependencies)
│   ├── service.py  # Main DeltaService orchestration
│   ├── models.py   # Data models (DeltaSpace, ObjectKey, PutSummary, etc.)
│   └── errors.py   # Domain-specific exceptions
├── ports/          # Abstract interfaces (protocols)
│   ├── storage.py  # StoragePort protocol for S3-like operations
│   ├── diff.py     # DiffPort protocol for delta operations
│   ├── hash.py     # HashPort protocol for integrity checks
│   ├── cache.py    # CachePort protocol for local references
│   ├── clock.py    # ClockPort protocol for time operations
│   ├── logger.py   # LoggerPort protocol for logging
│   └── metrics.py  # MetricsPort protocol for observability
├── adapters/       # Concrete implementations
│   ├── storage_s3.py      # S3StorageAdapter using boto3
│   ├── diff_xdelta.py     # XdeltaAdapter using xdelta3 binary
│   ├── hash_sha256.py     # Sha256Adapter for checksums
│   ├── cache_cas.py       # ContentAddressedCache (SHA256-based storage)
│   ├── cache_encrypted.py # EncryptedCache (Fernet encryption wrapper)
│   ├── cache_memory.py    # MemoryCache (LRU in-memory cache)
│   ├── clock_utc.py       # UtcClockAdapter for UTC timestamps
│   ├── logger_std.py      # StdLoggerAdapter for console output
│   └── metrics_noop.py    # NoopMetricsAdapter (placeholder)
└── app/
    └── cli/        # Click-based CLI application
        ├── main.py          # Main CLI entry point with AWS S3 commands
        ├── aws_compat.py    # AWS S3 compatibility helpers
        └── sync.py          # Sync command implementation
```

### Core Concepts

1. **DeltaSpace**: A prefix in S3 where related files are stored for delta compression. Contains a `reference.bin` file that serves as the base for delta compression.

2. **Delta Compression Flow**:
   - First file uploaded to a DeltaSpace becomes the reference (stored as `reference.bin`)
   - Subsequent files are compared against the reference using xdelta3
   - Only the differences (delta) are stored with `.delta` suffix
   - Metadata in S3 tags preserves original file info and delta relationships

3. **File Type Intelligence**:
   - Archive files (`.zip`, `.tar`, `.gz`, `.jar`, etc.) use delta compression
   - Text files, small files, and already-compressed unique files bypass delta
   - Decision made by `should_use_delta()` in `core/service.py`

4. **AWS S3 CLI Compatibility**:
   - Commands (`cp`, `ls`, `rm`, `sync`) mirror AWS CLI syntax exactly
   - Located in `app/cli/main.py` with helpers in `aws_compat.py`

### Key Algorithms

1. **Delta Ratio Check** (`core/service.py`):
   - After creating a delta, checks if `delta_size / file_size > max_ratio` (default 0.5)
   - If delta is too large (>50% of original), stores file directly instead
   - Prevents inefficient compression for dissimilar files

2. **Reference Management** (`core/service.py`):
   - Reference stored at `{deltaspace.prefix}/reference.bin`
   - SHA256 verification on every read/write
   - **Content-Addressed Storage (CAS)** cache in `/tmp/deltaglider-*` (ephemeral)
   - Cache uses SHA256 as filename with two-level directory structure (ab/cd/abcdef...)
   - Automatic deduplication: same content = same SHA = same cache file
   - Zero collision risk: SHA256 namespace guarantees uniqueness
   - **Encryption**: Optional Fernet (AES-128-CBC + HMAC) encryption at rest (enabled by default)
   - Ephemeral encryption keys per process for forward secrecy
   - **Cache Backends**: Configurable filesystem or in-memory cache with LRU eviction

3. **Sync Algorithm** (`app/cli/sync.py`):
   - Compares local vs S3 using size and modification time
   - For delta files, uses timestamp comparison with 1-second tolerance
   - Supports `--delete` flag for true mirroring

## Testing Strategy

- **Unit Tests** (`tests/unit/`): Test individual adapters and core logic with mocks
- **Integration Tests** (`tests/integration/`): Test CLI commands and workflows
- **E2E Tests** (`tests/e2e/`): Require LocalStack for full S3 simulation

Key test files:
- `test_full_workflow.py`: Complete put/get cycle testing
- `test_aws_cli_commands_v2.py`: AWS S3 CLI compatibility tests
- `test_xdelta.py`: Binary diff engine integration tests

## Common Development Tasks

### Adding a New CLI Command
1. Add command function to `src/deltaglider/app/cli/main.py`
2. Use `@cli.command()` decorator and `@click.pass_obj` for service access
3. Follow AWS S3 CLI conventions for flags and arguments
4. Add tests to `tests/integration/test_aws_cli_commands_v2.py`

### Adding a New Port/Adapter Pair
1. Define protocol in `src/deltaglider/ports/`
2. Implement adapter in `src/deltaglider/adapters/`
3. Wire adapter in `create_service()` in `app/cli/main.py`
4. Add unit tests in `tests/unit/test_adapters.py`

### Modifying Delta Logic
Core delta logic is in `src/deltaglider/core/service.py`:
- `put()`: Handles upload with delta compression
- `get()`: Handles download with delta reconstruction
- `should_use_delta()`: File type discrimination logic

## Environment Variables

- `DG_LOG_LEVEL`: Logging level (default: "INFO")
- `DG_MAX_RATIO`: Maximum acceptable delta/file ratio (default: "0.5", range: "0.0-1.0")
  - **See [docs/DG_MAX_RATIO.md](docs/DG_MAX_RATIO.md) for complete tuning guide**
  - Controls when to use delta vs. direct storage
  - Lower (0.2-0.3) = conservative, only high-quality compression
  - Higher (0.6-0.7) = permissive, accept modest savings
- `DG_CACHE_BACKEND`: Cache backend type - "filesystem" (default) or "memory"
- `DG_CACHE_MEMORY_SIZE_MB`: Memory cache size limit in MB (default: "100")
- `DG_CACHE_ENCRYPTION_KEY`: Optional base64-encoded Fernet key for persistent encryption (ephemeral by default)
- `AWS_ENDPOINT_URL`: Override S3 endpoint for MinIO/LocalStack
- `AWS_ACCESS_KEY_ID`: AWS credentials
- `AWS_SECRET_ACCESS_KEY`: AWS credentials
- `AWS_DEFAULT_REGION`: AWS region

**Security Notes**:
- **Encryption Always On**: Cache data is ALWAYS encrypted (cannot be disabled)
- **Ephemeral Keys**: Encryption keys auto-generated per process for maximum security
- **Auto-Cleanup**: Corrupted cache files automatically deleted on decryption failures
- **Process Isolation**: Each process gets isolated cache in `/tmp/deltaglider-*`, cleaned up on exit
- **Persistent Keys**: Set `DG_CACHE_ENCRYPTION_KEY` only if you need cross-process cache sharing (e.g., shared filesystems)

## Important Implementation Details

1. **xdelta3 Binary Dependency**: The system requires xdelta3 binary installed on the system. The `XdeltaAdapter` uses subprocess to call it.

2. **Metadata Storage**: File metadata is stored in S3 object metadata/tags, not in a separate database. This keeps the system simple and stateless.

3. **SHA256 Verification**: Every read and write operation includes SHA256 verification for data integrity.

4. **Atomic Operations**: All S3 operations are atomic - no partial states are left if operations fail.

5. **Reference File Updates**: Currently, the first file uploaded to a DeltaSpace becomes the permanent reference. Future versions may implement reference rotation.

## Performance Considerations

- **Content-Addressed Storage**: SHA256-based deduplication eliminates redundant storage
- **Cache Backends**:
  - Filesystem cache (default): persistent across processes, good for shared workflows
  - Memory cache: faster, zero I/O, perfect for ephemeral CI/CD pipelines
- **Encryption Overhead**: ~10-15% performance impact, provides security at rest
- Delta compression is CPU-intensive; consider parallelization for bulk uploads
- The default max_ratio of 0.5 prevents storing inefficient deltas
- For files <1MB, delta overhead may exceed benefits

## Security Notes

- Never store AWS credentials in code
- Use IAM roles when possible
- All S3 operations respect bucket policies and encryption settings
- SHA256 checksums prevent tampering and corruption
- **Encryption Always On**: Cache data is ALWAYS encrypted using Fernet (AES-128-CBC + HMAC) - cannot be disabled
- **Ephemeral Keys**: Encryption keys auto-generated per process for forward secrecy and process isolation
- **Auto-Cleanup**: Corrupted or tampered cache files automatically deleted on decryption failures
- **Persistent Keys**: Set `DG_CACHE_ENCRYPTION_KEY` only for cross-process cache sharing (use secrets management)
- **Content-Addressed Storage**: SHA256-based filenames prevent collision attacks
- **Zero-Trust Cache**: All cache operations include cryptographic validation