"""Adapters for DeltaGlider."""

from .cache_fs import FsCacheAdapter
from .clock_utc import UtcClockAdapter
from .diff_xdelta import XdeltaAdapter
from .hash_sha import Sha256Adapter
from .logger_std import StdLoggerAdapter
from .metrics_noop import NoopMetricsAdapter
from .storage_s3 import S3StorageAdapter

__all__ = [
    "S3StorageAdapter",
    "XdeltaAdapter",
    "Sha256Adapter",
    "FsCacheAdapter",
    "UtcClockAdapter",
    "StdLoggerAdapter",
    "NoopMetricsAdapter",
]
