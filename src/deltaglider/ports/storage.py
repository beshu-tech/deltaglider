"""Storage port interface."""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Protocol


@dataclass
class ObjectHead:
    """S3 object metadata."""

    key: str
    size: int
    etag: str
    last_modified: datetime
    metadata: dict[str, str]


@dataclass
class PutResult:
    """Result of a PUT operation."""

    etag: str
    version_id: str | None = None


class StoragePort(Protocol):
    """Port for storage operations."""

    def head(self, key: str) -> ObjectHead | None:
        """Get object metadata."""
        ...

    def list(self, prefix: str) -> Iterator[ObjectHead]:
        """List objects by prefix."""
        ...

    def get(self, key: str) -> BinaryIO:
        """Get object content as stream."""
        ...

    def put(
        self,
        key: str,
        body: BinaryIO | bytes | Path,
        metadata: dict[str, str],
        content_type: str = "application/octet-stream",
    ) -> PutResult:
        """Put object with metadata."""
        ...

    def delete(self, key: str) -> None:
        """Delete object."""
        ...
