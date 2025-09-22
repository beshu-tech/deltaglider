"""SHA256 hash adapter."""

import hashlib
from pathlib import Path
from typing import BinaryIO

from ..ports.hash import HashPort


class Sha256Adapter(HashPort):
    """SHA256 implementation of HashPort."""

    def sha256(self, path_or_stream: Path | BinaryIO) -> str:
        """Compute SHA256 hash."""
        hasher = hashlib.sha256()

        if isinstance(path_or_stream, Path):
            with open(path_or_stream, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
        else:
            # Reset position if possible
            if hasattr(path_or_stream, "seek"):
                path_or_stream.seek(0)

            while chunk := path_or_stream.read(8192):
                hasher.update(chunk)

        return hasher.hexdigest()
