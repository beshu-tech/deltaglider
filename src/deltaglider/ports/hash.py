"""Hash port interface."""

from pathlib import Path
from typing import BinaryIO, Protocol


class HashPort(Protocol):
    """Port for hash operations."""

    def sha256(self, path_or_stream: Path | BinaryIO) -> str:
        """Compute SHA256 hash."""
        ...
