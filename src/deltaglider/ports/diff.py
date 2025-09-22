"""Diff port interface."""

from pathlib import Path
from typing import Protocol


class DiffPort(Protocol):
    """Port for diff operations."""

    def encode(self, base: Path, target: Path, out: Path) -> None:
        """Create delta from base to target."""
        ...

    def decode(self, base: Path, delta: Path, out: Path) -> None:
        """Apply delta to base to recreate target."""
        ...
