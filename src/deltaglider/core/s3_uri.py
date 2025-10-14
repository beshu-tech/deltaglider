"""Utilities for working with S3-style URLs and keys."""

from __future__ import annotations

from typing import NamedTuple

S3_SCHEME = "s3://"


class S3Url(NamedTuple):
    """Normalized representation of an S3 URL."""

    bucket: str
    key: str = ""

    def to_url(self) -> str:
        """Return the canonical string form."""
        if self.key:
            return f"{S3_SCHEME}{self.bucket}/{self.key}"
        return f"{S3_SCHEME}{self.bucket}"

    def with_key(self, key: str) -> S3Url:
        """Return a new S3Url with a different key."""
        return S3Url(self.bucket, key.lstrip("/"))

    def join_key(self, suffix: str) -> S3Url:
        """Append a suffix to the key using '/' semantics."""
        suffix = suffix.lstrip("/")
        if not self.key:
            return self.with_key(suffix)
        if not suffix:
            return self
        return self.with_key(f"{self.key.rstrip('/')}/{suffix}")


def is_s3_url(value: str) -> bool:
    """Check if a string is an S3 URL."""
    return value.startswith(S3_SCHEME)


def parse_s3_url(
    url: str,
    *,
    allow_empty_key: bool = True,
    strip_trailing_slash: bool = False,
) -> S3Url:
    """Parse an S3 URL into bucket and key components."""
    if not is_s3_url(url):
        raise ValueError(f"Invalid S3 URL: {url}")

    path = url[len(S3_SCHEME) :]
    if strip_trailing_slash:
        path = path.rstrip("/")

    bucket, sep, key = path.partition("/")
    if not bucket:
        raise ValueError(f"S3 URL missing bucket: {url}")

    if not sep:
        key = ""

    key = key.lstrip("/")
    if not key and not allow_empty_key:
        raise ValueError(f"S3 URL must include a key: {url}")

    return S3Url(bucket=bucket, key=key)


def build_s3_url(bucket: str, key: str | None = None) -> str:
    """Build an S3 URL from components."""
    if not bucket:
        raise ValueError("Bucket name cannot be empty")

    if key:
        key = key.lstrip("/")
        return f"{S3_SCHEME}{bucket}/{key}"
    return f"{S3_SCHEME}{bucket}"


__all__ = [
    "S3Url",
    "build_s3_url",
    "is_s3_url",
    "parse_s3_url",
]
