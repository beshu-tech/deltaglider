"""Regression tests for the dual-scheme metadata read/write contract.

The CLI historically wrote direct-upload metadata with bare,
underscored keys (``original_name``, ``file_sha256``, ``compression``)
while delta uploads used the canonical dashed namespace
(``dg-original-name``, ``dg-file-sha256``, etc.). Downstream
consumers — most notably the Rust S3 proxy — only knew the dashed
form, so every ``.sha1`` / ``.sha512`` direct upload triggered a
PATHOLOGICAL warning when listed.

v6.1.2 aligned the writer to the dashed form, but the read path
must keep recognising the legacy bare keys forever so already-stored
objects don't break. These tests pin both halves of the contract.
"""

from deltaglider.core.models import (
    METADATA_KEY_ALIASES,
    METADATA_PREFIX,
    resolve_metadata,
)


class TestResolveMetadataAliases:
    """Verify resolve_metadata accepts every documented alias."""

    def test_new_dashed_keys_resolve(self):
        """The current canonical scheme: dg-*-with-dashes."""
        meta = {
            f"{METADATA_PREFIX}tool": "deltaglider/6.1.2",
            f"{METADATA_PREFIX}original-name": "build.zip",
            f"{METADATA_PREFIX}file-sha256": "deadbeef",
            f"{METADATA_PREFIX}file-size": "1024",
            f"{METADATA_PREFIX}created-at": "2026-05-17T00:00:00Z",
            f"{METADATA_PREFIX}compression": "none",
        }
        assert resolve_metadata(meta, "tool") == "deltaglider/6.1.2"
        assert resolve_metadata(meta, "original_name") == "build.zip"
        assert resolve_metadata(meta, "file_sha256") == "deadbeef"
        assert resolve_metadata(meta, "file_size") == "1024"
        assert resolve_metadata(meta, "created_at") == "2026-05-17T00:00:00Z"
        assert resolve_metadata(meta, "compression") == "none"

    def test_legacy_bare_underscored_keys_resolve(self):
        """Pre-v6.1.2 direct-upload shape used by historical .sha files."""
        meta = {
            "tool": "deltaglider/6.1.1",
            "original_name": "build.zip.sha1",
            "file_sha256": "feedface",
            "file_size": "41",
            "created_at": "2026-05-16T03:28:01.000000",
            "compression": "none",
        }
        assert resolve_metadata(meta, "tool") == "deltaglider/6.1.1"
        assert resolve_metadata(meta, "original_name") == "build.zip.sha1"
        assert resolve_metadata(meta, "file_sha256") == "feedface"
        assert resolve_metadata(meta, "file_size") == "41"
        assert resolve_metadata(meta, "created_at") == "2026-05-16T03:28:01.000000"
        assert resolve_metadata(meta, "compression") == "none"

    def test_legacy_hyphenated_keys_resolve(self):
        """Some old paths used hyphens without the dg- prefix."""
        meta = {
            "original-name": "old.zip",
            "file-sha256": "cafe1234",
            "file-size": "2048",
        }
        assert resolve_metadata(meta, "original_name") == "old.zip"
        assert resolve_metadata(meta, "file_sha256") == "cafe1234"
        assert resolve_metadata(meta, "file_size") == "2048"

    def test_priority_new_wins_when_both_present(self):
        """If both schemes happen to coexist on one object, prefer the
        canonical dashed key — that's the writer's current intent."""
        meta = {
            f"{METADATA_PREFIX}original-name": "new.zip",
            "original_name": "old.zip",
        }
        assert resolve_metadata(meta, "original_name") == "new.zip"

    def test_missing_returns_none(self):
        assert resolve_metadata({}, "tool") is None
        assert resolve_metadata({"unrelated": "x"}, "original_name") is None

    def test_empty_string_treated_as_missing(self):
        """Empty values must not satisfy the resolver — callers rely on
        None to trigger the fallback branch."""
        meta = {f"{METADATA_PREFIX}original-name": ""}
        assert resolve_metadata(meta, "original_name") is None


class TestAliasTableContract:
    """Pin the alias-table shape so a future regression on the
    ordering (which would break `priority_new_wins_when_both_present`)
    is caught immediately."""

    def test_every_field_lists_new_dashed_first(self):
        """The first alias in each tuple must be the canonical
        dg-*-with-dashes form. This is what `resolve_metadata` relies
        on for the "new wins over legacy when both present" rule."""
        for field, aliases in METADATA_KEY_ALIASES.items():
            assert aliases[0].startswith(METADATA_PREFIX), (
                f"{field}: first alias {aliases[0]!r} must be dashed namespace"
            )

    def test_every_field_includes_legacy_underscored_form(self):
        """Backward compat: bare underscored key must always be in the
        alias list. Pre-v6.1.2 direct uploads use them, and they
        must keep resolving forever."""
        for field, aliases in METADATA_KEY_ALIASES.items():
            assert field in aliases, (
                f"{field}: alias list must include the bare underscored "
                f"key {field!r} for legacy-upload compatibility"
            )

    def test_compression_field_present(self):
        """v6.1.2 added `compression` to the alias table so the
        direct-upload sentinel works on both schemes."""
        assert "compression" in METADATA_KEY_ALIASES

    def test_source_name_field_present(self):
        """Reference files' source_name should resolve uniformly."""
        assert "source_name" in METADATA_KEY_ALIASES
