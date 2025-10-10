"""Tests for encrypted cache adapter."""

import tempfile
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from deltaglider.adapters import ContentAddressedCache, EncryptedCache, Sha256Adapter
from deltaglider.core.errors import CacheCorruptionError, CacheMissError


class TestEncryptedCache:
    """Test encrypted cache wrapper functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def hasher(self):
        """Create SHA256 hasher."""
        return Sha256Adapter()

    @pytest.fixture
    def backend(self, temp_dir, hasher):
        """Create CAS backend."""
        return ContentAddressedCache(temp_dir, hasher)

    @pytest.fixture
    def encrypted_cache(self, backend):
        """Create encrypted cache with ephemeral key."""
        return EncryptedCache(backend)

    def test_ephemeral_key_generation(self, backend):
        """Test that ephemeral key is generated automatically."""
        cache = EncryptedCache(backend)

        assert cache._ephemeral is True
        assert cache._key is not None
        assert len(cache._key) == 44  # Base64-encoded 32-byte key

    def test_provided_key_usage(self, backend):
        """Test using provided encryption key."""
        key = Fernet.generate_key()
        cache = EncryptedCache(backend, encryption_key=key)

        assert cache._ephemeral is False
        assert cache._key == key

    def test_write_and_read_encrypted(self, encrypted_cache, temp_dir):
        """Test writing and reading encrypted content."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_content = b"Secret data that should be encrypted"
        test_file.write_bytes(test_content)

        # Compute expected SHA
        import hashlib

        expected_sha = hashlib.sha256(test_content).hexdigest()

        # Write to encrypted cache
        encrypted_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Read back and validate
        decrypted_path = encrypted_cache.get_validated_ref(
            "test-bucket", "test-prefix", expected_sha
        )

        # Verify decrypted content matches original
        decrypted_content = decrypted_path.read_bytes()
        assert decrypted_content == test_content

    def test_encrypted_storage_not_readable(self, encrypted_cache, backend, temp_dir):
        """Test that stored data is actually encrypted."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_content = b"Plaintext secret"
        test_file.write_bytes(test_content)

        # Write to encrypted cache
        encrypted_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Get the encrypted file path from backend
        backend_path = backend.ref_path("test-bucket", "test-prefix")

        # Read encrypted content directly
        encrypted_content = backend_path.read_bytes()

        # Verify content is NOT the same as plaintext
        assert encrypted_content != test_content
        # Verify content doesn't contain plaintext substring
        assert b"secret" not in encrypted_content.lower()

    def test_cache_miss(self, encrypted_cache):
        """Test cache miss error."""
        with pytest.raises(CacheMissError):
            encrypted_cache.get_validated_ref("no-bucket", "no-prefix", "fakehash")

    def test_decryption_with_wrong_sha(self, encrypted_cache, temp_dir):
        """Test that wrong SHA is detected after decryption."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_content = b"Test content"
        test_file.write_bytes(test_content)

        # Write to cache
        encrypted_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Try to read with wrong SHA
        with pytest.raises(CacheCorruptionError, match="SHA mismatch"):
            encrypted_cache.get_validated_ref("test-bucket", "test-prefix", "wrong_sha_hash_here")

    def test_decryption_with_wrong_key(self, temp_dir):
        """Test that decryption fails with wrong key."""
        # Create shared backend
        from deltaglider.adapters import ContentAddressedCache, Sha256Adapter

        hasher = Sha256Adapter()
        backend = ContentAddressedCache(temp_dir / "shared", hasher)

        # Create two caches with different keys sharing same backend
        cache1 = EncryptedCache(backend)

        # Write with cache1
        test_file = temp_dir / "test.txt"
        test_content = b"Encrypted data"
        test_file.write_bytes(test_content)

        import hashlib

        expected_sha = hashlib.sha256(test_content).hexdigest()

        cache1.write_ref("test-bucket", "test-prefix", test_file)

        # Create cache2 with different key (fresh instance, different ephemeral key)
        # and manually add to its mapping (simulating persistent storage scenario)
        cache2 = EncryptedCache(backend)
        cache2._plaintext_sha_map[("test-bucket", "test-prefix")] = expected_sha

        # Try to read with cache2 (different key) - should fail decryption
        with pytest.raises(CacheCorruptionError, match="Decryption failed"):
            cache2.get_validated_ref("test-bucket", "test-prefix", expected_sha)

    def test_evict_cleans_decrypted_files(self, encrypted_cache, temp_dir):
        """Test that evict cleans up .decrypted temporary files."""
        # Create and store file
        test_file = temp_dir / "test.txt"
        test_content = b"Test"
        test_file.write_bytes(test_content)

        import hashlib

        expected_sha = hashlib.sha256(test_content).hexdigest()

        encrypted_cache.write_ref("test-bucket", "test-prefix", test_file)

        # Read to create .decrypted file
        decrypted_path = encrypted_cache.get_validated_ref(
            "test-bucket", "test-prefix", expected_sha
        )
        assert decrypted_path.exists()

        # Evict
        encrypted_cache.evict("test-bucket", "test-prefix")

        # Verify .decrypted file is removed
        assert not decrypted_path.exists()

    def test_from_env_with_no_key(self, backend, monkeypatch):
        """Test from_env creates ephemeral key when env var not set."""
        monkeypatch.delenv("DG_CACHE_ENCRYPTION_KEY", raising=False)

        cache = EncryptedCache.from_env(backend)

        assert cache._ephemeral is True

    def test_from_env_with_key(self, backend, monkeypatch):
        """Test from_env uses key from environment."""
        key = Fernet.generate_key()
        monkeypatch.setenv("DG_CACHE_ENCRYPTION_KEY", key.decode("utf-8"))

        cache = EncryptedCache.from_env(backend)

        assert cache._ephemeral is False
        assert cache._key == key
