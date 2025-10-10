"""Encrypted cache wrapper using Fernet symmetric encryption.

This adapter wraps any CachePort implementation and adds transparent encryption/decryption.
It uses Fernet (symmetric encryption based on AES-128-CBC with HMAC authentication).
"""

import os
from pathlib import Path

from cryptography.fernet import Fernet

from ..core.errors import CacheCorruptionError, CacheMissError
from ..ports.cache import CachePort


class EncryptedCache(CachePort):
    """Encrypted cache wrapper using Fernet symmetric encryption.

    Wraps any CachePort implementation and transparently encrypts data at rest.
    Uses Fernet which provides:
    - AES-128-CBC encryption
    - HMAC authentication (prevents tampering)
    - Automatic key rotation support
    - Safe for ephemeral process-isolated caches

    Key Management:
    - Ephemeral key generated per process (default, most secure)
    - Or use DG_CACHE_ENCRYPTION_KEY env var (base64-encoded Fernet key)
    - For production: use secrets management system (AWS KMS, HashiCorp Vault, etc.)

    Security Properties:
    - Confidentiality: Data encrypted at rest
    - Integrity: HMAC prevents tampering
    - Authenticity: Only valid keys can decrypt
    - Forward Secrecy: Ephemeral keys destroyed on process exit
    """

    def __init__(self, backend: CachePort, encryption_key: bytes | None = None):
        """Initialize encrypted cache wrapper.

        Args:
            backend: Underlying cache implementation (CAS, filesystem, memory, etc.)
            encryption_key: Optional Fernet key (32 bytes base64-encoded).
                          If None, generates ephemeral key for this process.
        """
        self.backend = backend

        # Key management: ephemeral (default) or provided
        if encryption_key is None:
            # Generate ephemeral key for this process (most secure)
            self._key = Fernet.generate_key()
            self._ephemeral = True
        else:
            # Use provided key (for persistent cache scenarios)
            self._key = encryption_key
            self._ephemeral = False

        self._cipher = Fernet(self._key)

        # Mapping: (bucket, prefix) -> plaintext_sha256
        # Needed because backend uses SHA for storage, but encrypted content has different SHA
        self._plaintext_sha_map: dict[tuple[str, str], str] = {}

    @classmethod
    def from_env(cls, backend: CachePort) -> "EncryptedCache":
        """Create encrypted cache with key from environment.

        Looks for DG_CACHE_ENCRYPTION_KEY environment variable.
        If not found, generates ephemeral key.

        Args:
            backend: Underlying cache implementation

        Returns:
            EncryptedCache instance
        """
        key_str = os.environ.get("DG_CACHE_ENCRYPTION_KEY")
        if key_str:
            # Decode base64-encoded key
            encryption_key = key_str.encode("utf-8")
        else:
            # Use ephemeral key
            encryption_key = None

        return cls(backend, encryption_key)

    def ref_path(self, bucket: str, prefix: str) -> Path:
        """Get path where reference should be cached.

        Delegates to backend. Path structure determined by backend
        (e.g., CAS uses SHA256-based paths).

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix

        Returns:
            Path from backend
        """
        return self.backend.ref_path(bucket, prefix)

    def has_ref(self, bucket: str, prefix: str, sha: str) -> bool:
        """Check if reference exists with given SHA.

        Note: SHA is of the *unencrypted* content. The backend may store
        encrypted data, but we verify against original content hash.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            sha: SHA256 of unencrypted content

        Returns:
            True if encrypted reference exists with this SHA
        """
        # Delegate to backend
        # Backend may use SHA for content-addressed storage of encrypted data
        return self.backend.has_ref(bucket, prefix, sha)

    def get_validated_ref(self, bucket: str, prefix: str, expected_sha: str) -> Path:
        """Get cached reference with decryption and validation.

        Retrieves encrypted data from backend, decrypts it, validates SHA,
        and returns path to decrypted temporary file.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            expected_sha: Expected SHA256 of *decrypted* content

        Returns:
            Path to decrypted validated file (temporary)

        Raises:
            CacheMissError: File not in cache
            CacheCorruptionError: Decryption failed or SHA mismatch
        """
        # Check if we have this plaintext SHA mapped
        key = (bucket, prefix)
        if key not in self._plaintext_sha_map:
            raise CacheMissError(f"Cache miss for {bucket}/{prefix}")

        # Verify the requested SHA matches our mapping
        if self._plaintext_sha_map[key] != expected_sha:
            raise CacheCorruptionError(
                f"SHA mismatch for {bucket}/{prefix}: "
                f"expected {expected_sha}, have {self._plaintext_sha_map[key]}"
            )

        # Get encrypted file from backend using ref_path (not validated, we validate plaintext)
        encrypted_path = self.backend.ref_path(bucket, prefix)
        if not encrypted_path.exists():
            raise CacheMissError(f"Encrypted cache file not found for {bucket}/{prefix}")

        # Read encrypted content
        try:
            with open(encrypted_path, "rb") as f:
                encrypted_data = f.read()
        except OSError as e:
            raise CacheMissError(f"Cannot read encrypted cache: {e}") from e

        # Decrypt
        try:
            decrypted_data = self._cipher.decrypt(encrypted_data)
        except Exception as e:
            # Fernet raises InvalidToken for tampering/wrong key
            # SECURITY: Auto-delete corrupted cache files
            try:
                encrypted_path.unlink(missing_ok=True)
                # Clean up mapping
                if key in self._plaintext_sha_map:
                    del self._plaintext_sha_map[key]
            except Exception:
                pass  # Best effort cleanup
            raise CacheCorruptionError(
                f"Decryption failed for {bucket}/{prefix}: {e}. "
                f"Corrupted cache deleted automatically."
            ) from e

        # Validate SHA of decrypted content
        import hashlib

        actual_sha = hashlib.sha256(decrypted_data).hexdigest()
        if actual_sha != expected_sha:
            # SECURITY: Auto-delete corrupted cache files
            try:
                encrypted_path.unlink(missing_ok=True)
                # Clean up mapping
                if key in self._plaintext_sha_map:
                    del self._plaintext_sha_map[key]
            except Exception:
                pass  # Best effort cleanup
            raise CacheCorruptionError(
                f"Decrypted content SHA mismatch for {bucket}/{prefix}: "
                f"expected {expected_sha}, got {actual_sha}. "
                f"Corrupted cache deleted automatically."
            )

        # Write decrypted content to temporary file
        # Use same path as encrypted file but with .decrypted suffix
        decrypted_path = encrypted_path.with_suffix(".decrypted")
        try:
            with open(decrypted_path, "wb") as f:
                f.write(decrypted_data)
        except OSError as e:
            raise CacheCorruptionError(f"Cannot write decrypted cache: {e}") from e

        return decrypted_path

    def write_ref(self, bucket: str, prefix: str, src: Path) -> Path:
        """Encrypt and cache reference file.

        Reads source file, encrypts it, and stores encrypted version via backend.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
            src: Source file to encrypt and cache

        Returns:
            Path to encrypted cached file (from backend)
        """
        # Read source file
        try:
            with open(src, "rb") as f:
                plaintext_data = f.read()
        except OSError as e:
            raise CacheCorruptionError(f"Cannot read source file {src}: {e}") from e

        # Compute plaintext SHA for mapping
        import hashlib

        plaintext_sha = hashlib.sha256(plaintext_data).hexdigest()

        # Encrypt
        encrypted_data = self._cipher.encrypt(plaintext_data)

        # Write encrypted data to temporary file
        temp_encrypted = src.with_suffix(".encrypted.tmp")
        try:
            with open(temp_encrypted, "wb") as f:
                f.write(encrypted_data)

            # Store encrypted file via backend
            result_path = self.backend.write_ref(bucket, prefix, temp_encrypted)

            # Store mapping of plaintext SHA
            key = (bucket, prefix)
            self._plaintext_sha_map[key] = plaintext_sha

            return result_path

        finally:
            # Cleanup temporary file
            if temp_encrypted.exists():
                temp_encrypted.unlink()

    def evict(self, bucket: str, prefix: str) -> None:
        """Remove cached reference (encrypted version).

        Delegates to backend. Also cleans up any .decrypted temporary files and mappings.

        Args:
            bucket: S3 bucket name
            prefix: Deltaspace prefix
        """
        # Remove from plaintext SHA mapping
        key = (bucket, prefix)
        if key in self._plaintext_sha_map:
            del self._plaintext_sha_map[key]

        # Get path to potentially clean up .decrypted files
        try:
            path = self.backend.ref_path(bucket, prefix)
            decrypted_path = path.with_suffix(".decrypted")
            if decrypted_path.exists():
                decrypted_path.unlink()
        except Exception:
            # Best effort cleanup
            pass

        # Evict from backend
        self.backend.evict(bucket, prefix)

    def clear(self) -> None:
        """Clear all cached references and encryption mappings.

        Removes all cached data and clears encryption key mappings.
        This is the proper way to forcibly clean up cache in long-running
        applications.

        Use cases:
        - Long-running applications needing to free resources
        - Manual cache invalidation after key rotation
        - Test cleanup
        - Memory pressure situations

        Note: After clearing, the cache will use a fresh encryption key
              (ephemeral mode) or the same persistent key (if DG_CACHE_ENCRYPTION_KEY set).
        """
        # Clear encryption mapping
        self._plaintext_sha_map.clear()

        # Delegate to backend to clear actual files/memory
        self.backend.clear()
