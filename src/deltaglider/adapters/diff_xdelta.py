"""Xdelta3 diff adapter."""

import subprocess
from pathlib import Path

from ..ports.diff import DiffPort


class XdeltaAdapter(DiffPort):
    """Xdelta3 implementation of DiffPort."""

    def __init__(self, xdelta_path: str = "xdelta3"):
        """Initialize with xdelta3 path."""
        self.xdelta_path = xdelta_path

    def encode(self, base: Path, target: Path, out: Path) -> None:
        """Create delta from base to target."""
        cmd = [
            self.xdelta_path,
            "-e",  # encode
            "-f",  # force overwrite
            "-9",  # compression level
            "-s",
            str(base),  # source file
            str(target),  # target file
            str(out),  # output delta
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(f"xdelta3 encode failed: {result.stderr}")

    def decode(self, base: Path, delta: Path, out: Path) -> None:
        """Apply delta to base to recreate target."""
        cmd = [
            self.xdelta_path,
            "-d",  # decode
            "-f",  # force overwrite
            "-s",
            str(base),  # source file
            str(delta),  # delta file
            str(out),  # output file
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(f"xdelta3 decode failed: {result.stderr}")
