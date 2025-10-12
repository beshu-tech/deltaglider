"""Adapters for DeltaGlider."""

from .cache_cas import ContentAddressedCache
from .cache_encrypted import EncryptedCache
from .cache_fs import FsCacheAdapter
from .cache_memory import MemoryCache
from .clock_utc import UtcClockAdapter
from .diff_xdelta import XdeltaAdapter
from .ec2_metadata import EC2MetadataAdapter
from .hash_sha import Sha256Adapter
from .logger_std import StdLoggerAdapter
from .metrics_noop import NoopMetricsAdapter
from .storage_s3 import S3StorageAdapter

__all__ = [
    "ContentAddressedCache",
    "EC2MetadataAdapter",
    "EncryptedCache",
    "FsCacheAdapter",
    "MemoryCache",
    "NoopMetricsAdapter",
    "S3StorageAdapter",
    "Sha256Adapter",
    "StdLoggerAdapter",
    "UtcClockAdapter",
    "XdeltaAdapter",
]
