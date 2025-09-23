"""Core DeltaService orchestration."""

import tempfile
import warnings
from pathlib import Path
from typing import BinaryIO

from ..ports import (
    CachePort,
    ClockPort,
    DiffPort,
    HashPort,
    LoggerPort,
    MetricsPort,
    StoragePort,
)
from ..ports.storage import ObjectHead
from .errors import (
    DiffDecodeError,
    DiffEncodeError,
    IntegrityMismatchError,
    NotFoundError,
    PolicyViolationWarning,
    StorageIOError,
)
from .models import (
    DeltaMeta,
    DeltaSpace,
    ObjectKey,
    PutSummary,
    ReferenceMeta,
    VerifyResult,
)


class DeltaService:
    """Core service for delta operations."""

    def __init__(
        self,
        storage: StoragePort,
        diff: DiffPort,
        hasher: HashPort,
        cache: CachePort,
        clock: ClockPort,
        logger: LoggerPort,
        metrics: MetricsPort,
        tool_version: str = "deltaglider/0.1.0",
        max_ratio: float = 0.5,
    ):
        """Initialize service with ports."""
        self.storage = storage
        self.diff = diff
        self.hasher = hasher
        self.cache = cache
        self.clock = clock
        self.logger = logger
        self.metrics = metrics
        self.tool_version = tool_version
        self.max_ratio = max_ratio

        # File extensions that should use delta compression
        self.delta_extensions = {
            ".zip",
            ".tar",
            ".gz",
            ".tar.gz",
            ".tgz",
            ".bz2",
            ".tar.bz2",
            ".xz",
            ".tar.xz",
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

    def should_use_delta(self, filename: str) -> bool:
        """Check if file should use delta compression based on extension."""
        name_lower = filename.lower()
        # Check compound extensions first
        for ext in [".tar.gz", ".tar.bz2", ".tar.xz"]:
            if name_lower.endswith(ext):
                return True
        # Check simple extensions
        return any(name_lower.endswith(ext) for ext in self.delta_extensions)

    def put(
        self, local_file: Path, delta_space: DeltaSpace, max_ratio: float | None = None
    ) -> PutSummary:
        """Upload file as reference or delta (for archive files) or directly (for other files)."""
        if max_ratio is None:
            max_ratio = self.max_ratio

        start_time = self.clock.now()
        file_size = local_file.stat().st_size
        file_sha256 = self.hasher.sha256(local_file)
        original_name = local_file.name

        self.logger.info(
            "Starting put operation",
            file=str(local_file),
            leaf=f"{delta_space.bucket}/{delta_space.prefix}",
            size=file_size,
        )

        # Check if this file type should use delta compression
        use_delta = self.should_use_delta(original_name)

        if not use_delta:
            # For non-archive files, upload directly without delta
            self.logger.info(
                "Uploading file directly (no delta for this type)",
                file_type=Path(original_name).suffix,
            )
            summary = self._upload_direct(
                local_file, delta_space, file_sha256, original_name, file_size
            )
        else:
            # For archive files, use the delta compression system
            # Check for existing reference
            ref_key = delta_space.reference_key()
            ref_head = self.storage.head(f"{delta_space.bucket}/{ref_key}")

            if ref_head is None:
                # Create reference
                summary = self._create_reference(
                    local_file, delta_space, file_sha256, original_name, file_size
                )
            else:
                # Create delta
                summary = self._create_delta(
                    local_file,
                    delta_space,
                    ref_head,
                    file_sha256,
                    original_name,
                    file_size,
                    max_ratio,
                )

        duration = (self.clock.now() - start_time).total_seconds()
        self.logger.log_operation(
            op="put",
            key=summary.key,
            leaf=f"{delta_space.bucket}/{delta_space.prefix}",
            sizes={"file": file_size, "delta": summary.delta_size or file_size},
            durations={"total": duration},
            cache_hit=summary.cache_hit,
        )
        self.metrics.timing("deltaglider.put.duration", duration)

        return summary

    def get(self, object_key: ObjectKey, out: BinaryIO | Path) -> None:
        """Download and hydrate file (delta or direct)."""
        start_time = self.clock.now()

        self.logger.info("Starting get operation", key=object_key.key)

        # Get object metadata
        obj_head = self.storage.head(f"{object_key.bucket}/{object_key.key}")
        if obj_head is None:
            raise NotFoundError(f"Object not found: {object_key.key}")

        if "file_sha256" not in obj_head.metadata:
            raise StorageIOError(f"Missing metadata on {object_key.key}")

        # Check if this is a direct upload (non-delta)
        if obj_head.metadata.get("compression") == "none":
            # Direct download without delta processing
            self._get_direct(object_key, obj_head, out)
            duration = (self.clock.now() - start_time).total_seconds()
            self.logger.log_operation(
                op="get",
                key=object_key.key,
                leaf=f"{object_key.bucket}",
                sizes={"file": int(obj_head.metadata.get("file_size", 0))},
                durations={"total": duration},
                cache_hit=False,
            )
            self.metrics.timing("deltaglider.get.duration", duration)
            return

        # It's a delta file, process as before
        delta_meta = DeltaMeta.from_dict(obj_head.metadata)

        # Ensure reference is cached
        # The ref_key stored in metadata is relative to the bucket
        # So we use the same bucket as the delta
        if "/" in delta_meta.ref_key:
            ref_parts = delta_meta.ref_key.split("/")
            leaf_prefix = "/".join(ref_parts[:-1])
        else:
            leaf_prefix = ""
        delta_space = DeltaSpace(bucket=object_key.bucket, prefix=leaf_prefix)

        cache_hit = self.cache.has_ref(
            delta_space.bucket, delta_space.prefix, delta_meta.ref_sha256
        )
        if not cache_hit:
            self._cache_reference(delta_space, delta_meta.ref_sha256)

        # Download delta and decode
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            delta_path = tmp_path / "delta"
            ref_path = self.cache.ref_path(delta_space.bucket, delta_space.prefix)
            out_path = tmp_path / "output"

            # Download delta
            with open(delta_path, "wb") as f:
                delta_stream = self.storage.get(f"{object_key.bucket}/{object_key.key}")
                for chunk in iter(lambda: delta_stream.read(8192), b""):
                    f.write(chunk)

            # Decode
            try:
                self.diff.decode(ref_path, delta_path, out_path)
            except Exception as e:
                raise DiffDecodeError(f"Failed to decode delta: {e}") from e

            # Verify integrity
            actual_sha = self.hasher.sha256(out_path)
            if actual_sha != delta_meta.file_sha256:
                raise IntegrityMismatchError(
                    f"SHA256 mismatch: expected {delta_meta.file_sha256}, got {actual_sha}"
                )

            # Write output
            if isinstance(out, Path):
                out_path.rename(out)
            else:
                with open(out_path, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        out.write(chunk)

        duration = (self.clock.now() - start_time).total_seconds()
        self.logger.log_operation(
            op="get",
            key=object_key.key,
            leaf=f"{delta_space.bucket}/{delta_space.prefix}",
            sizes={"delta": delta_meta.delta_size, "file": delta_meta.file_size},
            durations={"total": duration},
            cache_hit=cache_hit,
        )
        self.metrics.timing("deltaglider.get.duration", duration)

    def verify(self, delta_key: ObjectKey) -> VerifyResult:
        """Verify delta file integrity."""
        start_time = self.clock.now()

        self.logger.info("Starting verify operation", key=delta_key.key)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "output"
            self.get(delta_key, out_path)

            delta_head = self.storage.head(f"{delta_key.bucket}/{delta_key.key}")
            if delta_head is None:
                raise NotFoundError(f"Delta not found: {delta_key.key}")

            delta_meta = DeltaMeta.from_dict(delta_head.metadata)
            actual_sha = self.hasher.sha256(out_path)
            valid = actual_sha == delta_meta.file_sha256

        duration = (self.clock.now() - start_time).total_seconds()
        self.logger.info(
            "Verify complete",
            key=delta_key.key,
            valid=valid,
            duration=duration,
        )
        self.metrics.timing("deltaglider.verify.duration", duration)

        return VerifyResult(
            valid=valid,
            expected_sha256=delta_meta.file_sha256,
            actual_sha256=actual_sha,
            message="Integrity verified" if valid else "Integrity check failed",
        )

    def _create_reference(
        self,
        local_file: Path,
        delta_space: DeltaSpace,
        file_sha256: str,
        original_name: str,
        file_size: int,
    ) -> PutSummary:
        """Create reference file."""
        ref_key = delta_space.reference_key()
        full_ref_key = f"{delta_space.bucket}/{ref_key}"

        # Create reference metadata
        ref_meta = ReferenceMeta(
            tool=self.tool_version,
            source_name=original_name,
            file_sha256=file_sha256,
            created_at=self.clock.now(),
        )

        # Upload reference
        self.logger.info("Creating reference", key=ref_key)
        self.storage.put(
            full_ref_key,
            local_file,
            ref_meta.to_dict(),
        )

        # Re-check for race condition
        ref_head = self.storage.head(full_ref_key)
        if ref_head and ref_head.metadata.get("file_sha256") != file_sha256:
            self.logger.warning("Reference creation race detected, using existing")
            # Proceed with existing reference
            ref_sha256 = ref_head.metadata["file_sha256"]
        else:
            ref_sha256 = file_sha256

        # Cache reference
        cached_path = self.cache.write_ref(delta_space.bucket, delta_space.prefix, local_file)
        self.logger.debug("Cached reference", path=str(cached_path))

        # Also create zero-diff delta
        delta_key = (
            f"{delta_space.prefix}/{original_name}.delta"
            if delta_space.prefix
            else f"{original_name}.delta"
        )
        full_delta_key = f"{delta_space.bucket}/{delta_key}"

        with tempfile.NamedTemporaryFile() as zero_delta:
            # Create empty delta using xdelta3
            self.diff.encode(local_file, local_file, Path(zero_delta.name))
            delta_size = Path(zero_delta.name).stat().st_size

            delta_meta = DeltaMeta(
                tool=self.tool_version,
                original_name=original_name,
                file_sha256=file_sha256,
                file_size=file_size,
                created_at=self.clock.now(),
                ref_key=ref_key,
                ref_sha256=ref_sha256,
                delta_size=delta_size,
                delta_cmd=f"xdelta3 -e -9 -s reference.bin {original_name} {original_name}.delta",
                note="zero-diff (reference identical)",
            )

            self.logger.info("Creating zero-diff delta", key=delta_key)
            self.storage.put(
                full_delta_key,
                Path(zero_delta.name),
                delta_meta.to_dict(),
            )

        self.metrics.increment("deltaglider.reference.created")
        return PutSummary(
            operation="create_reference",
            bucket=delta_space.bucket,
            key=ref_key,
            original_name=original_name,
            file_size=file_size,
            file_sha256=file_sha256,
        )

    def _create_delta(
        self,
        local_file: Path,
        delta_space: DeltaSpace,
        ref_head: ObjectHead,
        file_sha256: str,
        original_name: str,
        file_size: int,
        max_ratio: float,
    ) -> PutSummary:
        """Create delta file."""
        ref_key = delta_space.reference_key()
        ref_sha256 = ref_head.metadata["file_sha256"]

        # Ensure reference is cached
        cache_hit = self.cache.has_ref(delta_space.bucket, delta_space.prefix, ref_sha256)
        if not cache_hit:
            self._cache_reference(delta_space, ref_sha256)

        ref_path = self.cache.ref_path(delta_space.bucket, delta_space.prefix)

        # Create delta
        with tempfile.NamedTemporaryFile(suffix=".delta") as delta_file:
            delta_path = Path(delta_file.name)

            try:
                self.diff.encode(ref_path, local_file, delta_path)
            except Exception as e:
                raise DiffEncodeError(f"Failed to encode delta: {e}") from e

            delta_size = delta_path.stat().st_size
            delta_ratio = delta_size / file_size

            # Warn if delta is too large
            if delta_ratio > max_ratio:
                warnings.warn(
                    f"Delta ratio {delta_ratio:.2f} exceeds threshold {max_ratio}",
                    PolicyViolationWarning,
                    stacklevel=2,
                )
                self.logger.warning(
                    "Delta ratio exceeds threshold",
                    ratio=delta_ratio,
                    threshold=max_ratio,
                )

            # Create delta metadata
            delta_key = (
                f"{delta_space.prefix}/{original_name}.delta"
                if delta_space.prefix
                else f"{original_name}.delta"
            )
            full_delta_key = f"{delta_space.bucket}/{delta_key}"

            delta_meta = DeltaMeta(
                tool=self.tool_version,
                original_name=original_name,
                file_sha256=file_sha256,
                file_size=file_size,
                created_at=self.clock.now(),
                ref_key=ref_key,
                ref_sha256=ref_sha256,
                delta_size=delta_size,
                delta_cmd=f"xdelta3 -e -9 -s reference.bin {original_name} {original_name}.delta",
            )

            # Upload delta
            self.logger.info(
                "Creating delta",
                key=delta_key,
                ratio=f"{delta_ratio:.2f}",
            )
            self.storage.put(
                full_delta_key,
                delta_path,
                delta_meta.to_dict(),
            )

        self.metrics.increment("deltaglider.delta.created")
        self.metrics.gauge("deltaglider.delta.ratio", delta_ratio)

        return PutSummary(
            operation="create_delta",
            bucket=delta_space.bucket,
            key=delta_key,
            original_name=original_name,
            file_size=file_size,
            file_sha256=file_sha256,
            delta_size=delta_size,
            delta_ratio=delta_ratio,
            ref_key=ref_key,
            ref_sha256=ref_sha256,
            cache_hit=cache_hit,
        )

    def _cache_reference(self, delta_space: DeltaSpace, expected_sha: str) -> None:
        """Download and cache reference."""
        ref_key = delta_space.reference_key()
        full_ref_key = f"{delta_space.bucket}/{ref_key}"

        self.logger.info("Caching reference", key=ref_key)

        with tempfile.NamedTemporaryFile(delete=False) as tmp_ref:
            tmp_path = Path(tmp_ref.name)

            # Download reference
            ref_stream = self.storage.get(full_ref_key)
            for chunk in iter(lambda: ref_stream.read(8192), b""):
                tmp_ref.write(chunk)
            tmp_ref.flush()

        # Verify SHA (after closing the file)
        actual_sha = self.hasher.sha256(tmp_path)
        if actual_sha != expected_sha:
            tmp_path.unlink()
            raise IntegrityMismatchError(
                f"Reference SHA mismatch: expected {expected_sha}, got {actual_sha}"
            )

        # Cache it
        self.cache.write_ref(delta_space.bucket, delta_space.prefix, tmp_path)
        tmp_path.unlink()

    def _get_direct(
        self,
        object_key: ObjectKey,
        obj_head: ObjectHead,
        out: BinaryIO | Path,
    ) -> None:
        """Download file directly from S3 without delta processing."""
        # Download the file directly
        file_stream = self.storage.get(f"{object_key.bucket}/{object_key.key}")

        if isinstance(out, Path):
            # Write to file path
            with open(out, "wb") as f:
                for chunk in iter(lambda: file_stream.read(8192), b""):
                    f.write(chunk)
        else:
            # Write to binary stream
            for chunk in iter(lambda: file_stream.read(8192), b""):
                out.write(chunk)

        # Verify integrity if SHA256 is present
        expected_sha = obj_head.metadata.get("file_sha256")
        if expected_sha:
            if isinstance(out, Path):
                actual_sha = self.hasher.sha256(out)
            else:
                # For streams, we can't verify after writing
                # This would need a different approach (e.g., computing on the fly)
                self.logger.warning(
                    "Cannot verify SHA256 for stream output",
                    key=object_key.key,
                )
                return

            if actual_sha != expected_sha:
                raise IntegrityMismatchError(
                    f"SHA256 mismatch: expected {expected_sha}, got {actual_sha}"
                )

        self.logger.info(
            "Direct download complete",
            key=object_key.key,
            size=obj_head.metadata.get("file_size"),
        )

    def _upload_direct(
        self,
        local_file: Path,
        delta_space: DeltaSpace,
        file_sha256: str,
        original_name: str,
        file_size: int,
    ) -> PutSummary:
        """Upload file directly to S3 without delta compression."""
        # Construct the key path
        if delta_space.prefix:
            key = f"{delta_space.prefix}/{original_name}"
        else:
            key = original_name
        full_key = f"{delta_space.bucket}/{key}"

        # Create metadata for the file
        metadata = {
            "tool": self.tool_version,
            "original_name": original_name,
            "file_sha256": file_sha256,
            "file_size": str(file_size),
            "created_at": self.clock.now().isoformat(),
            "compression": "none",  # Mark as non-compressed
        }

        # Upload the file directly
        self.logger.info("Uploading file directly", key=key)
        self.storage.put(
            full_key,
            local_file,
            metadata,
        )

        self.metrics.increment("deltaglider.direct.uploaded")

        return PutSummary(
            operation="upload_direct",
            bucket=delta_space.bucket,
            key=key,
            original_name=original_name,
            file_size=file_size,
            file_sha256=file_sha256,
        )
