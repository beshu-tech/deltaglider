"""Core domain for DeltaGlider."""

from .delta_extensions import (
    DEFAULT_COMPOUND_DELTA_EXTENSIONS,
    DEFAULT_DELTA_EXTENSIONS,
    is_delta_candidate,
)
from .errors import (
    DeltaGliderError,
    DiffDecodeError,
    DiffEncodeError,
    IntegrityMismatchError,
    NotFoundError,
    PolicyViolationWarning,
    ReferenceCreationRaceError,
    StorageIOError,
)
from .models import (
    DeltaMeta,
    DeltaSpace,
    ObjectKey,
    PutSummary,
    ReferenceMeta,
    Sha256,
    VerifyResult,
)
from .s3_uri import S3Url, build_s3_url, is_s3_url, parse_s3_url
from .service import DeltaService

__all__ = [
    "DeltaGliderError",
    "NotFoundError",
    "ReferenceCreationRaceError",
    "IntegrityMismatchError",
    "DiffEncodeError",
    "DiffDecodeError",
    "StorageIOError",
    "PolicyViolationWarning",
    "DeltaSpace",
    "ObjectKey",
    "Sha256",
    "DeltaMeta",
    "ReferenceMeta",
    "PutSummary",
    "VerifyResult",
    "DeltaService",
    "DEFAULT_DELTA_EXTENSIONS",
    "DEFAULT_COMPOUND_DELTA_EXTENSIONS",
    "is_delta_candidate",
    "S3Url",
    "build_s3_url",
    "is_s3_url",
    "parse_s3_url",
]
