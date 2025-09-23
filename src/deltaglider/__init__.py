"""DeltaGlider - Delta-aware S3 file storage wrapper."""

try:
    from ._version import version as __version__
except ImportError:
    # Package is not installed, so version is not available
    __version__ = "0.0.0+unknown"

# Import simplified client API
from .client import DeltaGliderClient, create_client
from .core import DeltaService, DeltaSpace, ObjectKey

__all__ = [
    "__version__",
    "DeltaGliderClient",
    "create_client",
    "DeltaService",
    "DeltaSpace",
    "ObjectKey",
]
