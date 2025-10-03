# boto3 S3 Client Compatibility

DeltaGlider implements a **subset** of boto3's S3 client API, focusing on the most commonly used operations. This is **not** a 100% drop-in replacement, but covers the core functionality needed for most use cases.

## ✅ Implemented Methods (21 core methods)

### Object Operations
- ✅ `put_object()` - Upload objects (with automatic delta compression)
- ✅ `get_object()` - Download objects (with automatic delta reconstruction)
- ✅ `delete_object()` - Delete single object
- ✅ `delete_objects()` - Delete multiple objects
- ✅ `head_object()` - Get object metadata
- ✅ `list_objects()` - List objects (list_objects_v2 compatible)
- ✅ `copy_object()` - Copy objects between locations

### Bucket Operations
- ✅ `create_bucket()` - Create buckets
- ✅ `delete_bucket()` - Delete empty buckets
- ✅ `list_buckets()` - List all buckets

### Presigned URLs
- ✅ `generate_presigned_url()` - Generate presigned URLs
- ✅ `generate_presigned_post()` - Generate presigned POST data

### DeltaGlider Extensions
- ✅ `upload()` - Simple upload with S3 URL
- ✅ `download()` - Simple download with S3 URL
- ✅ `verify()` - Verify object integrity
- ✅ `upload_chunked()` - Upload with progress callback
- ✅ `upload_batch()` - Batch upload multiple files
- ✅ `download_batch()` - Batch download multiple files
- ✅ `estimate_compression()` - Estimate compression ratio
- ✅ `find_similar_files()` - Find similar files for delta reference
- ✅ `get_object_info()` - Get detailed object info with compression stats
- ✅ `get_bucket_stats()` - Get bucket statistics
- ✅ `delete_objects_recursive()` - Recursively delete objects

## ❌ Not Implemented (80+ methods)

### Multipart Upload
- ❌ `create_multipart_upload()`
- ❌ `upload_part()`
- ❌ `complete_multipart_upload()`
- ❌ `abort_multipart_upload()`
- ❌ `list_multipart_uploads()`
- ❌ `list_parts()`

### Access Control (ACL)
- ❌ `get_bucket_acl()`
- ❌ `put_bucket_acl()`
- ❌ `get_object_acl()`
- ❌ `put_object_acl()`
- ❌ `get_public_access_block()`
- ❌ `put_public_access_block()`
- ❌ `delete_public_access_block()`

### Bucket Configuration
- ❌ `get_bucket_location()`
- ❌ `get_bucket_versioning()`
- ❌ `put_bucket_versioning()`
- ❌ `get_bucket_logging()`
- ❌ `put_bucket_logging()`
- ❌ `get_bucket_website()`
- ❌ `put_bucket_website()`
- ❌ `delete_bucket_website()`
- ❌ `get_bucket_cors()`
- ❌ `put_bucket_cors()`
- ❌ `delete_bucket_cors()`
- ❌ `get_bucket_lifecycle_configuration()`
- ❌ `put_bucket_lifecycle_configuration()`
- ❌ `delete_bucket_lifecycle()`
- ❌ `get_bucket_policy()`
- ❌ `put_bucket_policy()`
- ❌ `delete_bucket_policy()`
- ❌ `get_bucket_encryption()`
- ❌ `put_bucket_encryption()`
- ❌ `delete_bucket_encryption()`
- ❌ `get_bucket_notification_configuration()`
- ❌ `put_bucket_notification_configuration()`
- ❌ `get_bucket_accelerate_configuration()`
- ❌ `put_bucket_accelerate_configuration()`
- ❌ `get_bucket_request_payment()`
- ❌ `put_bucket_request_payment()`
- ❌ `get_bucket_replication()`
- ❌ `put_bucket_replication()`
- ❌ `delete_bucket_replication()`

### Tagging & Metadata
- ❌ `get_object_tagging()`
- ❌ `put_object_tagging()`
- ❌ `delete_object_tagging()`
- ❌ `get_bucket_tagging()`
- ❌ `put_bucket_tagging()`
- ❌ `delete_bucket_tagging()`

### Advanced Features
- ❌ `restore_object()` - Glacier restore
- ❌ `select_object_content()` - S3 Select
- ❌ `get_object_torrent()` - BitTorrent
- ❌ `get_object_legal_hold()` - Object Lock
- ❌ `put_object_legal_hold()`
- ❌ `get_object_retention()`
- ❌ `put_object_retention()`
- ❌ `get_bucket_analytics_configuration()`
- ❌ `put_bucket_analytics_configuration()`
- ❌ `delete_bucket_analytics_configuration()`
- ❌ `list_bucket_analytics_configurations()`
- ❌ `get_bucket_metrics_configuration()`
- ❌ `put_bucket_metrics_configuration()`
- ❌ `delete_bucket_metrics_configuration()`
- ❌ `list_bucket_metrics_configurations()`
- ❌ `get_bucket_inventory_configuration()`
- ❌ `put_bucket_inventory_configuration()`
- ❌ `delete_bucket_inventory_configuration()`
- ❌ `list_bucket_inventory_configurations()`
- ❌ `get_bucket_intelligent_tiering_configuration()`
- ❌ `put_bucket_intelligent_tiering_configuration()`
- ❌ `delete_bucket_intelligent_tiering_configuration()`
- ❌ `list_bucket_intelligent_tiering_configurations()`

### Helper Methods
- ❌ `download_file()` - High-level download
- ❌ `upload_file()` - High-level upload
- ❌ `download_fileobj()` - Download to file object
- ❌ `upload_fileobj()` - Upload from file object

### Other
- ❌ `get_bucket_ownership_controls()`
- ❌ `put_bucket_ownership_controls()`
- ❌ `delete_bucket_ownership_controls()`
- ❌ `get_bucket_policy_status()`
- ❌ `list_object_versions()`
- ❌ `create_session()` - S3 Express
- And 20+ more metadata/configuration methods...

## Coverage Analysis

**Implemented:** ~21 methods
**Total boto3 S3 methods:** ~100+ methods
**Coverage:** ~20%

## What's Covered

DeltaGlider focuses on:
1. ✅ **Core CRUD operations** - put, get, delete, list
2. ✅ **Bucket management** - create, delete, list buckets
3. ✅ **Basic metadata** - head_object
4. ✅ **Presigned URLs** - generate_presigned_url/post
5. ✅ **Delta compression** - automatic for archive files
6. ✅ **Batch operations** - upload_batch, download_batch
7. ✅ **Compression stats** - get_bucket_stats, estimate_compression

## What's NOT Covered

❌ **Advanced bucket configuration** (versioning, lifecycle, logging, etc.)
❌ **Access control** (ACLs, bucket policies)
❌ **Multipart uploads** (for >5GB files)
❌ **Advanced features** (S3 Select, Glacier, Object Lock)
❌ **Tagging APIs** (object/bucket tags)
❌ **High-level transfer utilities** (upload_file, download_file)

## Use Cases

### ✅ DeltaGlider is PERFECT for:
- Storing versioned releases/builds
- Backup storage with deduplication
- CI/CD artifact storage
- Docker layer storage
- Archive file storage (zip, tar, etc.)
- Simple S3 storage needs

### ❌ Use boto3 directly for:
- Complex bucket policies
- Versioning/lifecycle management
- Multipart uploads (>5GB files)
- S3 Select queries
- Glacier deep archive
- Object Lock/Legal Hold
- Advanced ACL management

## Migration Strategy

If you need both boto3 and DeltaGlider:

```python
from deltaglider import create_client
import boto3

# Use DeltaGlider for objects (with compression!)
dg_client = create_client()
dg_client.put_object(Bucket='releases', Key='app.zip', Body=data)

# Use boto3 for advanced features
s3_client = boto3.client('s3')
s3_client.put_bucket_versioning(
    Bucket='releases',
    VersioningConfiguration={'Status': 'Enabled'}
)
```

## Future Additions

Likely to be added:
- `upload_file()` / `download_file()` - High-level helpers
- `copy_object()` - Object copying
- Basic tagging support
- Multipart upload (for large files)

Unlikely to be added:
- Advanced bucket configuration
- ACL management
- S3 Select
- Glacier operations

## Conclusion

**DeltaGlider is NOT a 100% drop-in boto3 replacement.**

It implements the **20% of boto3 methods that cover 80% of use cases**, with a focus on:
- Core object operations
- Bucket management
- Delta compression for storage savings
- Simple, clean API

For advanced S3 features, use boto3 directly or in combination with DeltaGlider.
