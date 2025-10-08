# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- boto3-compatible TypedDict types for S3 responses (no boto3 import needed)
- Complete boto3 compatibility vision document

### Changed
- **BREAKING**: `list_objects()` now returns boto3-compatible dict instead of custom dataclass
  - Use `response['Contents']` instead of `response.contents`
  - Use `response.get('IsTruncated')` instead of `response.is_truncated`
  - Use `response.get('NextContinuationToken')` instead of `response.next_continuation_token`
  - DeltaGlider metadata now in `Metadata` field of each object

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

[4.2.4]: https://github.com/beshu-tech/deltaglider/compare/v4.2.3...v4.2.4
[4.2.3]: https://github.com/beshu-tech/deltaglider/compare/v4.2.2...v4.2.3
[4.2.2]: https://github.com/beshu-tech/deltaglider/compare/v4.2.1...v4.2.2
[4.2.1]: https://github.com/beshu-tech/deltaglider/compare/v4.2.0...v4.2.1
[4.2.0]: https://github.com/beshu-tech/deltaglider/compare/v4.1.0...v4.2.0
[4.1.0]: https://github.com/beshu-tech/deltaglider/compare/v4.0.0...v4.1.0
[4.0.0]: https://github.com/beshu-tech/deltaglider/releases/tag/v4.0.0
