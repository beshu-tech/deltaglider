# AWS S3 CLI Compatibility for DeltaGlider

## Current State

DeltaGlider provides AWS S3 CLI compatible commands with automatic delta compression:

### Commands
- `deltaglider cp <source> <destination>` - Copy files with delta compression
- `deltaglider ls [s3_url]` - List buckets and objects
- `deltaglider rm <s3_url>` - Remove objects
- `deltaglider sync <source> <destination>` - Synchronize directories
- `deltaglider migrate <source> <destination>` - Migrate S3 buckets with compression and EC2 cost warnings
- `deltaglider stats <bucket>` - Get bucket statistics and compression metrics
- `deltaglider verify <s3_url>` - Verify file integrity

### Current Usage Examples
```bash
# Upload a file
deltaglider cp myfile.zip s3://bucket/path/to/file.zip

# Download a file
deltaglider cp s3://bucket/path/to/file.zip .

# Verify integrity
deltaglider verify s3://bucket/path/to/file.zip.delta
```

## Target State: AWS S3 CLI Compatibility

To serve as a drop-in replacement for AWS S3 CLI, DeltaGlider needs to support AWS S3 command syntax and behavior.

### Required AWS S3 Commands

#### 1. `cp` - Copy Command (Priority: HIGH)
```bash
# Upload file
deltaglider cp myfile.zip s3://bucket/path/to/file.zip

# Download file
deltaglider cp s3://bucket/path/to/file.zip myfile.zip

# Recursive copy
deltaglider cp --recursive local_dir/ s3://bucket/path/
deltaglider cp --recursive s3://bucket/path/ local_dir/

# Copy between S3 locations
deltaglider cp s3://bucket1/file.zip s3://bucket2/file.zip
```

#### 2. `sync` - Synchronize Command (Priority: HIGH)
```bash
# Sync local to S3
deltaglider sync local_dir/ s3://bucket/path/

# Sync S3 to local
deltaglider sync s3://bucket/path/ local_dir/

# Sync with delete
deltaglider sync --delete local_dir/ s3://bucket/path/

# Exclude patterns
deltaglider sync --exclude "*.log" local_dir/ s3://bucket/path/
```

#### 3. `ls` - List Command (Priority: HIGH)
```bash
# List buckets
deltaglider ls

# List objects in bucket
deltaglider ls s3://bucket/

# List with prefix
deltaglider ls s3://bucket/path/

# Recursive listing
deltaglider ls --recursive s3://bucket/path/

# Human readable sizes
deltaglider ls --human-readable s3://bucket/path/
```

#### 4. `rm` - Remove Command (Priority: MEDIUM)
```bash
# Remove single object
deltaglider rm s3://bucket/path/to/file.zip.delta

# Recursive remove
deltaglider rm --recursive s3://bucket/path/

# Dry run
deltaglider rm --dryrun s3://bucket/path/to/file.zip.delta
```

#### 5. `mb` - Make Bucket (Priority: LOW)
```bash
deltaglider mb s3://new-bucket
```

#### 6. `rb` - Remove Bucket (Priority: LOW)
```bash
deltaglider rb s3://bucket-to-remove
deltaglider rb --force s3://bucket-with-objects
```

#### 7. `mv` - Move Command (Priority: LOW)
```bash
deltaglider mv s3://bucket/old-path/file.zip s3://bucket/new-path/file.zip
```

### Common Flags Support

All commands should support these common AWS S3 CLI flags:

- `--profile` - AWS profile to use
- `--region` - AWS region
- `--endpoint-url` - Custom endpoint (for MinIO, etc.)
- `--no-verify-ssl` - Skip SSL verification
- `--storage-class` - S3 storage class
- `--debug` - Debug output
- `--quiet` - Suppress output
- `--dryrun` - Preview operations without executing

### Delta-Specific Flags

Additional flags specific to DeltaGlider's delta compression:

- `--no-delta` - Disable delta compression for this operation
- `--force-delta` - Force delta compression even for non-archive files
- `--delta-ratio` - Maximum delta/file size ratio (default: 0.5)
- `--reference-strategy` - How to select reference files (first|largest|newest)

## Implementation Plan

### Phase 1: Core Command Structure Refactoring
1. Restructure CLI to support source/destination syntax
2. Create command dispatcher that handles both upload and download
3. Maintain backward compatibility with old commands

### Phase 2: CP Command Implementation
1. Implement bidirectional `cp` command
2. Add support for S3-to-S3 copies
3. Implement `--recursive` flag for directories
4. Add progress indicators

### Phase 3: SYNC Command Implementation
1. Implement diff algorithm to detect changes
2. Add `--delete` flag support
3. Implement `--exclude` and `--include` patterns
4. Add dry-run support

### Phase 4: LS Command Implementation
1. Implement bucket listing
2. Add object listing with prefixes
3. Support `--recursive` flag
4. Add human-readable formatting

### Phase 5: RM Command Implementation
1. Implement single object deletion
2. Add `--recursive` support
3. Implement safety checks and `--dryrun`

### Phase 6: Advanced Features
1. Add mb/rb bucket management commands
2. Implement mv command (copy + delete)
3. Add support for all common AWS flags
4. Implement parallel uploads/downloads

### Phase 7: Testing & Documentation
1. Comprehensive test suite for all commands
2. Update README with AWS S3 compatibility examples
3. Create migration guide from aws-cli
4. Performance benchmarks comparing to aws-cli

## Environment Variables
- `DELTAGLIDER_AWS_COMPAT=1` - Strict AWS S3 CLI compatibility mode

## Success Criteria

1. **Drop-in Replacement**: Users can replace `aws s3` with `deltaglider` in scripts
2. **Feature Parity**: Support 90% of common aws s3 operations
3. **Performance**: Equal or better performance than aws-cli
4. **Delta Benefits**: Transparent 99.9% compression for versioned files
5. **Compatibility**: Works with S3, MinIO, R2, and other S3-compatible services

## Example Use Cases After Implementation

```bash
# CI/CD Pipeline - Direct replacement
# Before: aws s3 cp --recursive build/ s3://releases/v1.2.3/
# After:  deltaglider cp --recursive build/ s3://releases/v1.2.3/

# Backup Script - With compression benefits
# Before: aws s3 sync /backups/ s3://backups/daily/
# After:  deltaglider sync /backups/ s3://backups/daily/
# Result: 99.9% storage savings for similar files

# DevOps Deployment - Faster with delta
# Before: aws s3 cp app-v2.0.0.zip s3://deployments/
# After:  deltaglider cp app-v2.0.0.zip s3://deployments/
# Result: Only 5MB delta uploaded instead of 500MB full file
```

## Timeline

- **Week 1-2**: Phase 1-2 (Core refactoring and cp command)
- **Week 3-4**: Phase 3-4 (sync and ls commands)
- **Week 5**: Phase 5 (rm command)
- **Week 6**: Phase 6 (Advanced features)
- **Week 7-8**: Phase 7 (Testing and documentation)

Total estimated effort: 8 weeks for full AWS S3 CLI compatibility