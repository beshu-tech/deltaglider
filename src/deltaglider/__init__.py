"""DeltaGlider - Delta-aware S3 file storage wrapper."""

try:
    from ._version import version as __version__
except ImportError:
    # Package is not installed, so version is not available
    __version__ = "0.0.0+unknown"

# Import client API
from .client import DeltaGliderClient, create_client
from .client_models import (
    BucketStats,
    CompressionEstimate,
    ListObjectsResponse,
    ObjectInfo,
    UploadSummary,
)
from .core import DeltaService, DeltaSpace, ObjectKey

# Import boto3-compatible type aliases (no boto3 import required!)
from .types import (
    CopyObjectResponse,
    CreateBucketResponse,
    DeleteObjectResponse,
    DeleteObjectsResponse,
    GetObjectResponse,
    HeadObjectResponse,
    ListBucketsResponse,
    ListObjectsV2Response,
    PutObjectResponse,
    S3Object,
)

__all__ = [
    "__version__",
    # Client
    "DeltaGliderClient",
    "create_client",
    # Data classes (legacy - will be deprecated in favor of TypedDict)
    "UploadSummary",
    "CompressionEstimate",
    "ObjectInfo",
    "ListObjectsResponse",
    "BucketStats",
    # Core classes
    "DeltaService",
    "DeltaSpace",
    "ObjectKey",
    # boto3-compatible types (no boto3 import needed!)
    "ListObjectsV2Response",
    "PutObjectResponse",
    "GetObjectResponse",
    "DeleteObjectResponse",
    "DeleteObjectsResponse",
    "HeadObjectResponse",
    "ListBucketsResponse",
    "CreateBucketResponse",
    "CopyObjectResponse",
    "S3Object",
]
