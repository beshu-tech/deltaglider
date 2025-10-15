"""Core domain models."""

from dataclasses import dataclass
from datetime import datetime

# Metadata key prefix for DeltaGlider
# AWS S3 automatically adds 'x-amz-meta-' prefix, so our keys become 'x-amz-meta-dg-*'
METADATA_PREFIX = "dg-"


@dataclass(frozen=True)
class DeltaSpace:
    """S3 delta compression space - a prefix containing related files for delta compression."""

    bucket: str
    prefix: str

    def reference_key(self) -> str:
        """Get reference file key."""
        return f"{self.prefix}/reference.bin" if self.prefix else "reference.bin"


@dataclass(frozen=True)
class ObjectKey:
    """S3 object key."""

    bucket: str
    key: str


@dataclass(frozen=True)
class Sha256:
    """SHA256 hash."""

    hex: str

    def __post_init__(self) -> None:
        """Validate hash format."""
        if len(self.hex) != 64 or not all(c in "0123456789abcdef" for c in self.hex.lower()):
            raise ValueError(f"Invalid SHA256: {self.hex}")


@dataclass
class ReferenceMeta:
    """Reference file metadata."""

    tool: str
    source_name: str
    file_sha256: str
    created_at: datetime
    note: str = "reference"

    def to_dict(self) -> dict[str, str]:
        """Convert to S3 metadata dict with DeltaGlider namespace prefix."""
        return {
            f"{METADATA_PREFIX}tool": self.tool,
            f"{METADATA_PREFIX}source-name": self.source_name,
            f"{METADATA_PREFIX}file-sha256": self.file_sha256,
            f"{METADATA_PREFIX}created-at": self.created_at.isoformat() + "Z",
            f"{METADATA_PREFIX}note": self.note,
        }


@dataclass
class DeltaMeta:
    """Delta file metadata."""

    tool: str
    original_name: str
    file_sha256: str
    file_size: int
    created_at: datetime
    ref_key: str
    ref_sha256: str
    delta_size: int
    delta_cmd: str
    note: str | None = None

    def to_dict(self) -> dict[str, str]:
        """Convert to S3 metadata dict with DeltaGlider namespace prefix."""
        meta = {
            f"{METADATA_PREFIX}tool": self.tool,
            f"{METADATA_PREFIX}original-name": self.original_name,
            f"{METADATA_PREFIX}file-sha256": self.file_sha256,
            f"{METADATA_PREFIX}file-size": str(self.file_size),
            f"{METADATA_PREFIX}created-at": self.created_at.isoformat() + "Z",
            f"{METADATA_PREFIX}ref-key": self.ref_key,
            f"{METADATA_PREFIX}ref-sha256": self.ref_sha256,
            f"{METADATA_PREFIX}delta-size": str(self.delta_size),
            f"{METADATA_PREFIX}delta-cmd": self.delta_cmd,
        }
        if self.note:
            meta[f"{METADATA_PREFIX}note"] = self.note
        return meta

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "DeltaMeta":
        """Create from S3 metadata dict with DeltaGlider namespace prefix."""
        return cls(
            tool=data[f"{METADATA_PREFIX}tool"],
            original_name=data[f"{METADATA_PREFIX}original-name"],
            file_sha256=data[f"{METADATA_PREFIX}file-sha256"],
            file_size=int(data[f"{METADATA_PREFIX}file-size"]),
            created_at=datetime.fromisoformat(data[f"{METADATA_PREFIX}created-at"].rstrip("Z")),
            ref_key=data[f"{METADATA_PREFIX}ref-key"],
            ref_sha256=data[f"{METADATA_PREFIX}ref-sha256"],
            delta_size=int(data[f"{METADATA_PREFIX}delta-size"]),
            delta_cmd=data[f"{METADATA_PREFIX}delta-cmd"],
            note=data.get(f"{METADATA_PREFIX}note"),
        )


@dataclass
class PutSummary:
    """Summary of PUT operation."""

    operation: str  # "create_reference" or "create_delta"
    bucket: str
    key: str
    original_name: str
    file_size: int
    file_sha256: str
    delta_size: int | None = None
    delta_ratio: float | None = None
    ref_key: str | None = None
    ref_sha256: str | None = None
    cache_hit: bool = False


@dataclass
class VerifyResult:
    """Result of verification."""

    valid: bool
    expected_sha256: str
    actual_sha256: str
    message: str
