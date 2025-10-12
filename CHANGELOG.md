# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **EC2 Region Detection & Cost Optimization**
  - Automatic detection of EC2 instance region using IMDSv2
  - Warns when EC2 region ≠ S3 client region (potential cross-region charges)
  - Different warnings for auto-detected vs. explicit `--region` flag mismatches
  - Green checkmark when regions are aligned (optimal configuration)
  - Can be disabled with `DG_DISABLE_EC2_DETECTION=true` environment variable
  - Helps users optimize for cost and performance before migration starts
- **New CLI Command**: `deltaglider migrate` for S3-to-S3 bucket migration with compression
  - Supports resume capability (skips already migrated files)
  - Real-time progress tracking with file count and statistics
  - Interactive confirmation prompt (use `--yes` to skip)
  - Prefix preservation by default (use `--no-preserve-prefix` to disable)
  - Dry run mode with `--dry-run` flag
  - Include/exclude pattern filtering
  - Shows compression statistics after migration
  - **EC2-aware region logging**: Detects EC2 instance and warns about cross-region charges
  - **FIXED**: Now correctly preserves original filenames during migration
- **S3-to-S3 Recursive Copy**: `deltaglider cp -r s3://source/ s3://dest/` now supported
  - Automatically uses migration functionality with prefix preservation
  - Applies delta compression during transfer
  - Preserves original filenames correctly
- **Version Command**: Added `--version` flag to show deltaglider version
  - Usage: `deltaglider --version`
- **DeltaService API Enhancement**: Added `override_name` parameter to `put()` method
  - Allows specifying destination filename independently of source filesystem path
  - Enables proper S3-to-S3 transfers without filesystem renaming tricks

### Fixed
- **Critical**: S3-to-S3 migration now preserves original filenames
  - Previously created files with temp names like `tmp1b9cpdsn.zip`
  - Now correctly uses original filenames from source S3 keys
  - Fixed by adding `override_name` parameter to `DeltaService.put()`
- **CLI Region Support**: `--region` flag now properly passes region to boto3 client
  - Previously only set environment variable, relied on boto3 auto-detection
  - Now explicitly passes `region_name` to `boto3.client()` via `boto3_kwargs`
  - Ensures consistent behavior with `DeltaGliderClient` SDK

### Changed
- Recursive S3-to-S3 copy operations now preserve source prefix structure by default
- Migration operations show formatted output with source and destination paths

### Documentation
- Added comprehensive migration guide in README.md
- Updated CLI reference with migrate command examples
- Added prefix preservation behavior documentation

## [5.1.1] - 2025-01-10

### Fixed
- **Stats Command**: Fixed incorrect compression ratio calculations
  - Now correctly counts ALL files including reference.bin in compressed size
  - Fixed handling of orphaned reference.bin files (reference files with no delta files)
  - Added prominent warnings for orphaned reference files with cleanup commands
  - Fixed stats for buckets with no compression (now shows 0% instead of negative)
  - SHA1 checksum files are now properly included in calculations

### Improved
- **Stats Performance**: Optimized metadata fetching with parallel requests
  - 5-10x faster for buckets with many delta files
  - Uses ThreadPoolExecutor for concurrent HEAD requests
  - Single-pass calculation algorithm for better efficiency

## [5.1.0] - 2025-10-10

### Added
- **New CLI Command**: `deltaglider stats <bucket>` for bucket statistics and compression metrics
  - Supports `--detailed` flag for comprehensive analysis
  - Supports `--json` flag for machine-readable output
  - Accepts multiple formats: `s3://bucket/`, `s3://bucket`, `bucket`
- **Session-Level Statistics Caching**: Bucket stats now cached per client instance
  - Automatic cache invalidation on mutations (put, delete, bucket operations)
  - Intelligent cache reuse (detailed stats serve quick stat requests)
  - Enhanced `list_buckets()` includes cached stats when available
- **Programmatic Cache Management**: Added cache management APIs for long-running applications
  - `clear_cache()`: Clear all cached references
  - `evict_cache()`: Remove specific cached reference
  - Session-scoped cache lifecycle management

### Changed
- Bucket statistics are now cached within client session for performance
- `list_buckets()` response includes `DeltaGliderStats` metadata when cached

### Documentation
- Added comprehensive DG_MAX_RATIO tuning guide in docs/
- Updated CLI command reference in CLAUDE.md and README.md
- Added detailed cache management documentation

## [5.0.3] - 2025-10-10

### Security
- **BREAKING**: Removed all legacy shared cache code for security
- **BREAKING**: Encryption is now ALWAYS ON (cannot be disabled)
- Ephemeral process-isolated cache is now the ONLY mode (no opt-out)
- **Content-Addressed Storage (CAS)**: Implemented SHA256-based cache storage
  - Zero collision risk (SHA256 namespace guarantees uniqueness)
  - Automatic deduplication (same content = same filename)
  - Tampering protection (changing content changes SHA, breaks lookup)
  - Two-level directory structure for filesystem optimization
- **Encrypted Cache**: All cache data encrypted at rest using Fernet (AES-128-CBC + HMAC)
  - Ephemeral encryption keys per process (forward secrecy)
  - Optional persistent keys via `DG_CACHE_ENCRYPTION_KEY` for shared filesystems
  - Automatic cleanup of corrupted cache files on decryption failures
- Fixed TOCTOU vulnerabilities with atomic SHA validation at use-time
- Added `get_validated_ref()` method to prevent cache poisoning
- Eliminated multi-user data exposure through mandatory cache isolation

### Removed
- **BREAKING**: Removed `DG_UNSAFE_SHARED_CACHE` environment variable
- **BREAKING**: Removed `DG_CACHE_DIR` environment variable
- **BREAKING**: Removed `DG_CACHE_ENCRYPTION` environment variable (encryption always on)
- **BREAKING**: Removed `cache_dir` parameter from `create_client()`

### Changed
- Cache is now auto-created in `/tmp/deltaglider-*` and cleaned on exit
- All cache operations use file locking (Unix) and SHA validation
- Added `CacheMissError` and `CacheCorruptionError` exceptions

### Added
- New `ContentAddressedCache` adapter in `adapters/cache_cas.py`
- New `EncryptedCache` wrapper in `adapters/cache_encrypted.py`
- New `MemoryCache` adapter in `adapters/cache_memory.py` with LRU eviction
- Self-describing cache structure with SHA256-based filenames
- Configurable cache backends via `DG_CACHE_BACKEND` (filesystem or memory)
- Memory cache size limit via `DG_CACHE_MEMORY_SIZE_MB` (default: 100MB)

### Internal
- Updated all tests to use Content-Addressed Storage and encryption
- All 119 tests passing with zero errors (99 original + 20 new cache tests)
- Type checking: 0 errors (mypy)
- Linting: All checks passed (ruff)
- Completed Phase 1, 2, and 7 of SECURITY_FIX_ROADMAP.md
- Added comprehensive test suites for encryption (13 tests) and memory cache (10 tests)

## [5.0.1] - 2025-01-10

### Changed
- **Code Organization**: Refactored client.py from 1560 to 1154 lines (26% reduction)
- Extracted client operations into modular `client_operations/` package:
  - `bucket.py` - S3 bucket management operations
  - `presigned.py` - Presigned URL generation
  - `batch.py` - Batch upload/download operations
  - `stats.py` - Analytics and statistics operations
- Improved code maintainability with logical separation of concerns
- Better developer experience with cleaner module structure

### Internal
- Full type safety maintained with mypy (0 errors)
- All 99 tests passing
- Code quality checks passing (ruff)
- No breaking changes - all public APIs remain unchanged

## [5.0.0] - 2025-01-10

### Added
- boto3-compatible TypedDict types for S3 responses (no boto3 import needed)
- Complete boto3 compatibility vision document
- Type-safe response builders using TypedDict patterns

### Changed
- **BREAKING**: `list_objects()` now returns boto3-compatible dict instead of custom dataclass
  - Use `response['Contents']` instead of `response.contents`
  - Use `response.get('IsTruncated')` instead of `response.is_truncated`
  - Use `response.get('NextContinuationToken')` instead of `response.next_continuation_token`
  - DeltaGlider metadata now in `Metadata` field of each object
- Internal response building now uses TypedDict for compile-time type safety
- All S3 responses are dicts at runtime (TypedDict is a dict!)

### Fixed
- Updated all documentation examples to use dict-based responses
- Fixed pagination examples in README and API docs
- Corrected SDK documentation with accurate method signatures

## [4.2.4] - 2025-01-10

### Fixed
- Show only filename in `ls` output instead of full path for cleaner display
- Correct `ls` command path handling and prefix display logic

## [4.2.3] - 2025-01-07

### Added
- Comprehensive test coverage for `delete_objects_recursive()` method with 19 thorough tests
- Tests cover delta suffix handling, error/warning aggregation, statistics tracking, and edge cases
- Better code organization with separate `client_models.py` and `client_delete_helpers.py` modules

### Fixed
- Fixed all mypy type errors using proper `cast()` for type safety
- Improved type hints for dictionary operations in client code

### Changed
- Refactored client code into logical modules for better maintainability
- Enhanced code quality with comprehensive linting and type checking
- All 99 integration/unit tests passing with zero type errors

### Internal
- Better separation of concerns in client module
- Improved developer experience with clearer code structure

## [4.2.2] - 2024-10-06

### Fixed
- Add .delta suffix fallback for `delete_object()` method
- Handle regular S3 objects without DeltaGlider metadata
- Update mypy type ignore comment for compatibility

## [4.2.1] - 2024-10-06

### Fixed
- Make GitHub release creation non-blocking in workflows

## [4.2.0] - 2024-10-03

### Added
- AWS credential parameters to `create_client()` function
- Support for custom endpoint URLs
- Enhanced boto3 compatibility

## [4.1.0] - 2024-09-29

### Added
- boto3-compatible client API
- Bucket management methods
- Comprehensive SDK documentation

## [4.0.0] - 2024-09-21

### Added
- Initial public release
- CLI with AWS S3 compatibility
- Delta compression for versioned artifacts
- 99%+ compression for similar files

[5.1.0]: https://github.com/beshu-tech/deltaglider/compare/v5.0.3...v5.1.0
[5.0.3]: https://github.com/beshu-tech/deltaglider/compare/v5.0.1...v5.0.3
[5.0.1]: https://github.com/beshu-tech/deltaglider/compare/v5.0.0...v5.0.1
[5.0.0]: https://github.com/beshu-tech/deltaglider/compare/v4.2.4...v5.0.0
[4.2.4]: https://github.com/beshu-tech/deltaglider/compare/v4.2.3...v4.2.4
[4.2.3]: https://github.com/beshu-tech/deltaglider/compare/v4.2.2...v4.2.3
[4.2.2]: https://github.com/beshu-tech/deltaglider/compare/v4.2.1...v4.2.2
[4.2.1]: https://github.com/beshu-tech/deltaglider/compare/v4.2.0...v4.2.1
[4.2.0]: https://github.com/beshu-tech/deltaglider/compare/v4.1.0...v4.2.0
[4.1.0]: https://github.com/beshu-tech/deltaglider/compare/v4.0.0...v4.1.0
[4.0.0]: https://github.com/beshu-tech/deltaglider/releases/tag/v4.0.0
