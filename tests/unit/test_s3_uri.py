"""Tests for S3 URI helpers."""

import pytest

from deltaglider.core.s3_uri import build_s3_url, is_s3_url, parse_s3_url


def test_is_s3_url_detects_scheme() -> None:
    """is_s3_url should only match the S3 scheme."""
    assert is_s3_url("s3://bucket/path")
    assert not is_s3_url("https://example.com/object")


def test_parse_s3_url_returns_bucket_and_key() -> None:
    """Parsing should split bucket and key correctly."""
    parsed = parse_s3_url("s3://my-bucket/path/to/object.txt")
    assert parsed.bucket == "my-bucket"
    assert parsed.key == "path/to/object.txt"


def test_parse_strips_trailing_slash_when_requested() -> None:
    """strip_trailing_slash should normalise directory-style URLs."""
    parsed = parse_s3_url("s3://my-bucket/path/to/", strip_trailing_slash=True)
    assert parsed.bucket == "my-bucket"
    assert parsed.key == "path/to"


def test_parse_requires_key_when_configured() -> None:
    """allow_empty_key=False should reject bucket-only URLs."""
    with pytest.raises(ValueError):
        parse_s3_url("s3://bucket-only", allow_empty_key=False)


def test_build_s3_url_round_trip() -> None:
    """build_s3_url should round-trip with parse_s3_url."""
    url = build_s3_url("bucket", "dir/file.tar")
    parsed = parse_s3_url(url)
    assert parsed.bucket == "bucket"
    assert parsed.key == "dir/file.tar"


def test_build_s3_url_for_bucket_root() -> None:
    """When key is missing, build_s3_url should omit the trailing slash."""
    assert build_s3_url("root-bucket") == "s3://root-bucket"
