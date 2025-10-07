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

__all__ = [
    "__version__",
    # Client
    "DeltaGliderClient",
    "create_client",
    # Data classes
    "UploadSummary",
    "CompressionEstimate",
    "ObjectInfo",
    "ListObjectsResponse",
    "BucketStats",
    # Core classes
    "DeltaService",
    "DeltaSpace",
    "ObjectKey",
]
