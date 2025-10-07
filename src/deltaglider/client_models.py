"""Shared data models for the DeltaGlider client."""

from dataclasses import dataclass, field


@dataclass
class UploadSummary:
    """User-friendly upload summary."""

    operation: str
    bucket: str
    key: str
    original_size: int
    stored_size: int
    is_delta: bool
    delta_ratio: float = 0.0

    @property
    def original_size_mb(self) -> float:
        """Original size in MB."""
        return self.original_size / (1024 * 1024)

    @property
    def stored_size_mb(self) -> float:
        """Stored size in MB."""
        return self.stored_size / (1024 * 1024)

    @property
    def savings_percent(self) -> float:
        """Percentage saved through compression."""
        if self.original_size == 0:
            return 0.0
        return ((self.original_size - self.stored_size) / self.original_size) * 100


@dataclass
class CompressionEstimate:
    """Compression estimate for a file."""

    original_size: int
    estimated_compressed_size: int
    estimated_ratio: float
    confidence: float
    recommended_reference: str | None = None
    should_use_delta: bool = True


@dataclass
class ObjectInfo:
    """Detailed object information with compression stats."""

    key: str
    size: int
    last_modified: str
    etag: str | None = None
    storage_class: str = "STANDARD"

    # DeltaGlider-specific fields
    original_size: int | None = None
    compressed_size: int | None = None
    compression_ratio: float | None = None
    is_delta: bool = False
    reference_key: str | None = None
    delta_chain_length: int = 0


@dataclass
class ListObjectsResponse:
    """Response from list_objects, compatible with boto3."""

    name: str  # Bucket name
    prefix: str = ""
    delimiter: str = ""
    max_keys: int = 1000
    common_prefixes: list[dict[str, str]] = field(default_factory=list)
    contents: list[ObjectInfo] = field(default_factory=list)
    is_truncated: bool = False
    next_continuation_token: str | None = None
    continuation_token: str | None = None
    key_count: int = 0

    @property
    def objects(self) -> list[ObjectInfo]:
        """Alias for contents, for convenience."""
        return self.contents


@dataclass
class BucketStats:
    """Statistics for a bucket."""

    bucket: str
    object_count: int
    total_size: int
    compressed_size: int
    space_saved: int
    average_compression_ratio: float
    delta_objects: int
    direct_objects: int
