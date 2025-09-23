"""DeltaGlider - Delta-aware S3 file storage wrapper."""

try:
    from ._version import version as __version__
except ImportError:
    # Package is not installed, so version is not available
    __version__ = "0.0.0+unknown"
