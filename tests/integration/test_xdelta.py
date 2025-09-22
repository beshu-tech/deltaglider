"""Integration tests for xdelta3."""

import pytest

from deltaglider.adapters import XdeltaAdapter


@pytest.mark.usefixtures("skip_if_no_xdelta")
class TestXdeltaIntegration:
    """Test xdelta3 integration."""

    def test_encode_decode_roundtrip(self, temp_dir):
        """Test encoding and decoding roundtrip."""
        # Setup
        adapter = XdeltaAdapter()

        # Create base and target files
        base = temp_dir / "base.txt"
        base.write_text("This is the base file content.")

        target = temp_dir / "target.txt"
        target.write_text("This is the modified target file content with changes.")

        delta = temp_dir / "delta.bin"
        output = temp_dir / "output.txt"

        # Encode
        adapter.encode(base, target, delta)

        # Verify delta was created
        assert delta.exists()
        assert delta.stat().st_size > 0

        # Decode
        adapter.decode(base, delta, output)

        # Verify output matches target
        assert output.read_text() == target.read_text()

    def test_encode_identical_files(self, temp_dir):
        """Test encoding identical files produces small delta."""
        # Setup
        adapter = XdeltaAdapter()

        # Create identical files
        base = temp_dir / "base.txt"
        content = "This is identical content in both files." * 100
        base.write_text(content)

        target = temp_dir / "target.txt"
        target.write_text(content)

        delta = temp_dir / "delta.bin"

        # Encode
        adapter.encode(base, target, delta)

        # Verify delta is small (much smaller than original)
        assert delta.exists()
        assert delta.stat().st_size < len(content) / 10  # Delta should be <10% of original

    def test_encode_completely_different_files(self, temp_dir):
        """Test encoding completely different files."""
        # Setup
        adapter = XdeltaAdapter()

        # Create completely different files
        base = temp_dir / "base.txt"
        base.write_text("A" * 1000)

        target = temp_dir / "target.txt"
        target.write_text("B" * 1000)

        delta = temp_dir / "delta.bin"

        # Encode
        adapter.encode(base, target, delta)

        # Delta will be roughly the size of target since files are completely different
        assert delta.exists()
        # Note: xdelta3 compression may still reduce size somewhat

    def test_encode_binary_files(self, temp_dir):
        """Test encoding binary files."""
        # Setup
        adapter = XdeltaAdapter()

        # Create binary files
        base = temp_dir / "base.bin"
        base.write_bytes(b"\x00\x01\x02\x03" * 256)

        target = temp_dir / "target.bin"
        target.write_bytes(b"\x00\x01\x02\x03" * 200 + b"\xff\xfe\xfd\xfc" * 56)

        delta = temp_dir / "delta.bin"
        output = temp_dir / "output.bin"

        # Encode
        adapter.encode(base, target, delta)

        # Decode
        adapter.decode(base, delta, output)

        # Verify
        assert output.read_bytes() == target.read_bytes()
