"""Shared delta compression extension policy."""

from __future__ import annotations

from collections.abc import Collection, Iterable

# Compound extensions must be checked before simple suffix matching so that
# multi-part archives like ".tar.gz" are handled correctly.
DEFAULT_COMPOUND_DELTA_EXTENSIONS: tuple[str, ...] = (".tar.gz", ".tar.bz2", ".tar.xz")

# Simple extensions that benefit from delta compression. Keep this structure
# immutable so it can be safely reused across modules.
DEFAULT_DELTA_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".zip",
        ".tar",
        ".gz",
        ".tgz",
        ".bz2",
        ".xz",
        ".7z",
        ".rar",
        ".dmg",
        ".iso",
        ".pkg",
        ".deb",
        ".rpm",
        ".apk",
        ".jar",
        ".war",
        ".ear",
    }
)


def is_delta_candidate(
    filename: str,
    *,
    simple_extensions: Collection[str] = DEFAULT_DELTA_EXTENSIONS,
    compound_extensions: Iterable[str] = DEFAULT_COMPOUND_DELTA_EXTENSIONS,
) -> bool:
    """Check if a filename should use delta compression based on extension."""
    name_lower = filename.lower()

    for ext in compound_extensions:
        if name_lower.endswith(ext):
            return True

    return any(name_lower.endswith(ext) for ext in simple_extensions)


__all__ = [
    "DEFAULT_COMPOUND_DELTA_EXTENSIONS",
    "DEFAULT_DELTA_EXTENSIONS",
    "is_delta_candidate",
]
