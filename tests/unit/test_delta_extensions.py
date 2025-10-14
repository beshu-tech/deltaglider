"""Tests for shared delta extension policy."""

from deltaglider.core.delta_extensions import (
    DEFAULT_COMPOUND_DELTA_EXTENSIONS,
    DEFAULT_DELTA_EXTENSIONS,
    is_delta_candidate,
)


def test_is_delta_candidate_matches_default_extensions():
    """All default extensions should be detected as delta candidates."""
    for ext in DEFAULT_DELTA_EXTENSIONS:
        assert is_delta_candidate(f"file{ext}")


def test_is_delta_candidate_matches_compound_extensions():
    """Compound extensions should be handled even with multiple suffixes."""
    for ext in DEFAULT_COMPOUND_DELTA_EXTENSIONS:
        assert is_delta_candidate(f"file{ext}")


def test_is_delta_candidate_rejects_other_extensions():
    """Non delta-friendly extensions should return False."""
    assert not is_delta_candidate("document.txt")
    assert not is_delta_candidate("image.jpeg")
