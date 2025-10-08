"""Client operation modules for DeltaGliderClient.

This package contains modular operation implementations:
- bucket: S3 bucket management (create, delete, list)
- presigned: Presigned URL generation for temporary access
- batch: Batch upload/download operations
- stats: Statistics and analytics operations
"""

from .batch import download_batch, upload_batch, upload_chunked
from .bucket import create_bucket, delete_bucket, list_buckets
from .presigned import generate_presigned_post, generate_presigned_url
from .stats import (
    estimate_compression,
    find_similar_files,
    get_bucket_stats,
    get_object_info,
)

__all__ = [
    # Bucket operations
    "create_bucket",
    "delete_bucket",
    "list_buckets",
    # Presigned operations
    "generate_presigned_url",
    "generate_presigned_post",
    # Batch operations
    "upload_chunked",
    "upload_batch",
    "download_batch",
    # Stats operations
    "get_bucket_stats",
    "get_object_info",
    "estimate_compression",
    "find_similar_files",
]
