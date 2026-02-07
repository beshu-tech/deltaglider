"""Core DeltaService orchestration."""

import tempfile
import warnings
from datetime import UTC, timedelta
from pathlib import Path
from typing import Any, BinaryIO

from .. import __version__
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
from .delta_extensions import (
    DEFAULT_COMPOUND_DELTA_EXTENSIONS,
    DEFAULT_DELTA_EXTENSIONS,
    is_delta_candidate,
)
from .errors import (
    DiffDecodeError,
    DiffEncodeError,
    IntegrityMismatchError,
    NotFoundError,
    PolicyViolationWarning,
)
from .models import (
    DeleteResult,
    DeltaMeta,
    DeltaSpace,
    ObjectKey,
    PutSummary,
    RecursiveDeleteResult,
    ReferenceMeta,
    VerifyResult,
    resolve_metadata,
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
        tool_version: str | None = None,
        max_ratio: float = 0.5,
    ):
        """Initialize service with ports.

        Args:
            tool_version: Version string for metadata. If None, uses package __version__.
        """
        # Use real package version if not explicitly provided
        if tool_version is None:
            tool_version = f"deltaglider/{__version__}"
        self.storage = storage
        self.diff = diff
        self.hasher = hasher
        self.cache = cache
        self.clock = clock
        self.logger = logger
        self.metrics = metrics
        self.tool_version = tool_version
        self.max_ratio = max_ratio

        # File extensions that should use delta compression. Keep mutable copies
        # so advanced callers can customize the policy if needed.
        self.delta_extensions = set(DEFAULT_DELTA_EXTENSIONS)
        self.compound_delta_extensions = DEFAULT_COMPOUND_DELTA_EXTENSIONS

    def should_use_delta(self, filename: str) -> bool:
        """Check if file should use delta compression based on extension."""
        return is_delta_candidate(
            filename,
            simple_extensions=self.delta_extensions,
            compound_extensions=self.compound_delta_extensions,
        )

    def put(
        self,
        local_file: Path,
        delta_space: DeltaSpace,
        max_ratio: float | None = None,
        override_name: str | None = None,
    ) -> PutSummary:
        """Upload file as reference or delta (for archive files) or directly (for other files).

        Args:
            local_file: Path to the local file to upload
            delta_space: DeltaSpace (bucket + prefix) for the upload
            max_ratio: Maximum acceptable delta/file ratio (default: service max_ratio)
            override_name: Optional name to use instead of local_file.name (useful for S3-to-S3 copies)
        """
        if max_ratio is None:
            max_ratio = self.max_ratio

        start_time = self.clock.now()
        file_size = local_file.stat().st_size
        file_sha256 = self.hasher.sha256(local_file)
        original_name = override_name if override_name else local_file.name

        self.logger.info(
            "Starting put operation",
            file=str(local_file),
            deltaspace=f"{delta_space.bucket}/{delta_space.prefix}",
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
            deltaspace=f"{delta_space.bucket}/{delta_space.prefix}",
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
        obj_head = self.storage.head(object_key.full_key)
        if obj_head is None:
            raise NotFoundError(f"Object not found: {object_key.key}")

        # Check if this is a regular S3 object (not uploaded via DeltaGlider)
        # Regular S3 objects won't have DeltaGlider metadata (dg-file-sha256 key)
        if "dg-file-sha256" not in obj_head.metadata:
            # This is a regular S3 object, download it directly
            self.logger.info(
                "Downloading regular S3 object (no DeltaGlider metadata)",
                key=object_key.key,
            )
            self._get_direct(object_key, obj_head, out)
            duration = (self.clock.now() - start_time).total_seconds()
            self.logger.log_operation(
                op="get",
                key=object_key.key,
                deltaspace=f"{object_key.bucket}",
                sizes={"file": obj_head.size},
                durations={"total": duration},
                cache_hit=False,
            )
            self.metrics.timing("deltaglider.get.duration", duration)
            return

        # Check if this is a direct upload (non-delta) uploaded via DeltaGlider
        if obj_head.metadata.get("compression") == "none":
            # Direct download without delta processing
            self._get_direct(object_key, obj_head, out)
            duration = (self.clock.now() - start_time).total_seconds()
            file_size_meta = resolve_metadata(obj_head.metadata, "file_size")
            file_size_value = int(file_size_meta) if file_size_meta else obj_head.size
            self.logger.log_operation(
                op="get",
                key=object_key.key,
                deltaspace=f"{object_key.bucket}",
                sizes={"file": file_size_value},
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
            deltaspace_prefix = "/".join(ref_parts[:-1])
        else:
            deltaspace_prefix = ""
        delta_space = DeltaSpace(bucket=object_key.bucket, prefix=deltaspace_prefix)

        cache_hit = self.cache.has_ref(
            delta_space.bucket, delta_space.prefix, delta_meta.ref_sha256
        )
        if not cache_hit:
            self._cache_reference(delta_space, delta_meta.ref_sha256)

        # Download delta and decode
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            delta_path = tmp_path / "delta"
            # SECURITY: Use validated ref to prevent TOCTOU attacks
            ref_path = self.cache.get_validated_ref(
                delta_space.bucket, delta_space.prefix, delta_meta.ref_sha256
            )
            out_path = tmp_path / "output"

            # Download delta
            with open(delta_path, "wb") as f:
                delta_stream = self.storage.get(object_key.full_key)
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
            deltaspace=f"{delta_space.bucket}/{delta_space.prefix}",
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
        existing_sha = None
        if ref_head:
            existing_sha = resolve_metadata(ref_head.metadata, "file_sha256")
        if ref_head and existing_sha and existing_sha != file_sha256:
            self.logger.warning("Reference creation race detected, using existing")
            # Proceed with existing reference
            ref_sha256 = existing_sha
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
        ref_sha256 = resolve_metadata(ref_head.metadata, "file_sha256")
        if not ref_sha256:
            raise ValueError("Reference metadata missing file SHA256")

        # Ensure reference is cached
        cache_hit = self.cache.has_ref(delta_space.bucket, delta_space.prefix, ref_sha256)
        if not cache_hit:
            self._cache_reference(delta_space, ref_sha256)

        # SECURITY: Use validated ref to prevent TOCTOU attacks
        ref_path = self.cache.get_validated_ref(delta_space.bucket, delta_space.prefix, ref_sha256)

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
        file_stream = self.storage.get(object_key.full_key)

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
        expected_sha = resolve_metadata(obj_head.metadata, "file_sha256")
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
            size=resolve_metadata(obj_head.metadata, "file_size"),
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

    def delete(self, object_key: ObjectKey) -> DeleteResult:
        """Delete an object (delta-aware).

        For delta files, just deletes the delta.
        For reference files, checks if any deltas depend on it first.
        For direct uploads, simply deletes the file.
        """
        start_time = self.clock.now()
        full_key = object_key.full_key

        self.logger.info("Starting delete operation", key=object_key.key)

        obj_head = self.storage.head(full_key)
        if obj_head is None:
            raise NotFoundError(f"Object not found: {object_key.key}")

        result = DeleteResult(key=object_key.key, bucket=object_key.bucket)

        if object_key.key.endswith("/reference.bin"):
            self._delete_reference(object_key, full_key, result)
        elif object_key.key.endswith(".delta"):
            self._delete_delta(object_key, full_key, obj_head, result)
        elif obj_head.metadata.get("compression") == "none":
            self.storage.delete(full_key)
            result.deleted = True
            result.type = "direct"
            result.original_name = obj_head.metadata.get("original_name", object_key.key)
        else:
            self.storage.delete(full_key)
            result.deleted = True
            result.type = "unknown"

        duration = (self.clock.now() - start_time).total_seconds()
        self.logger.log_operation(
            op="delete",
            key=object_key.key,
            deltaspace=f"{object_key.bucket}",
            durations={"total": duration},
            sizes={},
            cache_hit=False,
        )
        self.metrics.timing("deltaglider.delete.duration", duration)
        self.metrics.increment(f"deltaglider.delete.{result.type}")

        return result

    def _delete_reference(self, object_key: ObjectKey, full_key: str, result: DeleteResult) -> None:
        """Handle deletion of a reference.bin file."""
        prefix = object_key.key.rsplit("/", 1)[0] if "/" in object_key.key else ""
        dependent_deltas = []

        for obj in self.storage.list(f"{object_key.bucket}/{prefix}"):
            if obj.key.endswith(".delta") and obj.key != object_key.key:
                delta_head = self.storage.head(f"{object_key.bucket}/{obj.key}")
                if delta_head and delta_head.metadata.get("ref_key") == object_key.key:
                    dependent_deltas.append(obj.key)

        if dependent_deltas:
            result.warnings.append(
                f"Reference has {len(dependent_deltas)} dependent delta(s). "
                "Deleting this will make those deltas unrecoverable."
            )
            self.logger.warning(
                "Reference has dependent deltas",
                ref_key=object_key.key,
                delta_count=len(dependent_deltas),
                deltas=dependent_deltas[:5],
            )

        self.storage.delete(full_key)
        result.deleted = True
        result.type = "reference"
        result.dependent_deltas = len(dependent_deltas)

        if "/" in object_key.key:
            deltaspace_prefix = object_key.key.rsplit("/", 1)[0]
            try:
                self.cache.evict(object_key.bucket, deltaspace_prefix)
            except Exception as e:
                self.logger.debug(f"Could not clear cache for {object_key.key}: {e}")

    def _delete_delta(
        self,
        object_key: ObjectKey,
        full_key: str,
        obj_head: ObjectHead,
        result: DeleteResult,
    ) -> None:
        """Handle deletion of a delta file, cleaning up orphaned references."""
        self.storage.delete(full_key)
        result.deleted = True
        result.type = "delta"
        result.original_name = obj_head.metadata.get("original_name", "unknown")

        if "/" not in object_key.key:
            return

        deltaspace_prefix = "/".join(object_key.key.split("/")[:-1])
        ref_key = f"{deltaspace_prefix}/reference.bin"

        remaining_deltas = [
            obj.key
            for obj in self.storage.list(f"{object_key.bucket}/{deltaspace_prefix}")
            if obj.key.endswith(".delta") and obj.key != object_key.key
        ]

        if not remaining_deltas:
            ref_full_key = f"{object_key.bucket}/{ref_key}"
            ref_head = self.storage.head(ref_full_key)
            if ref_head:
                self.storage.delete(ref_full_key)
                self.logger.info(
                    "Cleaned up orphaned reference.bin",
                    ref_key=ref_key,
                    reason="no remaining deltas",
                )
                result.cleaned_reference = ref_key

                try:
                    self.cache.evict(object_key.bucket, deltaspace_prefix)
                except Exception as e:
                    self.logger.debug(f"Could not clear cache for {deltaspace_prefix}: {e}")

    def delete_recursive(self, bucket: str, prefix: str) -> RecursiveDeleteResult:
        """Recursively delete all objects under a prefix (delta-aware).

        Handles delta relationships intelligently:
        - Deletes deltas before references
        - Warns about orphaned deltas
        - Handles direct uploads
        """
        start_time = self.clock.now()
        self.logger.info("Starting recursive delete", bucket=bucket, prefix=prefix)

        if prefix and not prefix.endswith("/"):
            prefix = f"{prefix}/"

        # Phase 1: classify objects by type
        references, deltas, direct_uploads, other_objects, affected_deltaspaces = (
            self._classify_objects_for_deletion(bucket, prefix)
        )

        # Also check for references in parent deltaspaces affected by delta deletion
        for ds_prefix in affected_deltaspaces:
            ref_key = f"{ds_prefix}/reference.bin"
            if ref_key not in references:
                ref_head = self.storage.head(f"{bucket}/{ref_key}")
                if ref_head:
                    references.append(ref_key)

        result = RecursiveDeleteResult(
            bucket=bucket,
            prefix=prefix,
            deltas_deleted=len(deltas),
            references_deleted=len(references),
            direct_deleted=len(direct_uploads),
            other_deleted=len(other_objects),
        )

        # Phase 2: delete non-reference files first (dependency order)
        for key in other_objects + direct_uploads + deltas:
            try:
                self.storage.delete(f"{bucket}/{key}")
                result.deleted_count += 1
                self.logger.debug(f"Deleted {key}")
            except Exception as e:
                result.failed_count += 1
                result.errors.append(f"Failed to delete {key}: {str(e)}")
                self.logger.error(f"Failed to delete {key}: {e}")

        # Phase 3: delete references only if safe
        references_kept = self._delete_references_if_safe(bucket, prefix, references, result)
        result.references_deleted -= references_kept

        # Clear cached references
        if references:
            try:
                self.cache.evict(bucket, prefix.rstrip("/") if prefix else "")
            except Exception as e:
                self.logger.debug(f"Could not clear cache for {bucket}/{prefix}: {e}")

        duration = (self.clock.now() - start_time).total_seconds()
        self.logger.info(
            "Recursive delete complete",
            bucket=bucket,
            prefix=prefix,
            deleted=result.deleted_count,
            failed=result.failed_count,
            duration=duration,
        )
        self.metrics.timing("deltaglider.delete_recursive.duration", duration)
        self.metrics.increment("deltaglider.delete_recursive.completed")

        return result

    def _classify_objects_for_deletion(
        self, bucket: str, prefix: str
    ) -> tuple[list[str], list[str], list[str], list[str], set[str]]:
        """Classify objects under a prefix into references, deltas, direct uploads, and other.

        Returns:
            (references, deltas, direct_uploads, other_objects, affected_deltaspaces)
        """
        references: list[str] = []
        deltas: list[str] = []
        direct_uploads: list[str] = []
        other_objects: list[str] = []
        affected_deltaspaces: set[str] = set()

        for obj in self.storage.list(f"{bucket}/{prefix}" if prefix else bucket):
            if prefix and not obj.key.startswith(prefix):
                continue

            if obj.key.endswith("/reference.bin"):
                references.append(obj.key)
            elif obj.key.endswith(".delta"):
                deltas.append(obj.key)
                if "/" in obj.key:
                    affected_deltaspaces.add("/".join(obj.key.split("/")[:-1]))
            else:
                obj_head = self.storage.head(f"{bucket}/{obj.key}")
                if obj_head and obj_head.metadata.get("compression") == "none":
                    direct_uploads.append(obj.key)
                else:
                    other_objects.append(obj.key)

        return references, deltas, direct_uploads, other_objects, affected_deltaspaces

    def _delete_references_if_safe(
        self,
        bucket: str,
        prefix: str,
        references: list[str],
        result: RecursiveDeleteResult,
    ) -> int:
        """Delete references only if no files outside the deletion scope depend on them.

        Returns the number of references kept (not deleted).
        """
        references_kept = 0
        deletion_prefix_full = f"{bucket}/{prefix}" if prefix else bucket

        for ref_key in references:
            try:
                if ref_key.endswith("/reference.bin"):
                    deltaspace_prefix = ref_key[:-14]  # Remove "/reference.bin"
                else:
                    deltaspace_prefix = ""

                ds_list_prefix = f"{bucket}/{deltaspace_prefix}" if deltaspace_prefix else bucket
                has_remaining_files = any(
                    not (prefix and f"{bucket}/{obj.key}".startswith(deletion_prefix_full))
                    and obj.key != ref_key
                    for obj in self.storage.list(ds_list_prefix)
                )

                if not has_remaining_files:
                    self.storage.delete(f"{bucket}/{ref_key}")
                    result.deleted_count += 1
                    self.logger.debug(f"Deleted reference {ref_key}")
                else:
                    references_kept += 1
                    result.warnings.append(f"Kept reference {ref_key} (still in use)")
                    self.logger.info(
                        f"Kept reference {ref_key} - still in use outside deletion scope"
                    )

            except Exception as e:
                result.failed_count += 1
                result.errors.append(f"Failed to delete reference {ref_key}: {str(e)}")
                self.logger.error(f"Failed to delete reference {ref_key}: {e}")

        return references_kept

    def rehydrate_for_download(
        self,
        bucket: str,
        key: str,
        expires_in_seconds: int = 3600,
    ) -> str | None:
        """Rehydrate a deltaglider-compressed file for direct download.

        If the file is deltaglider-compressed, this will:
        1. Download and decompress the file
        2. Re-upload to .deltaglider/tmp/ with expiration metadata
        3. Return the new temporary file key

        If the file is not deltaglider-compressed, returns None.

        Args:
            bucket: S3 bucket name
            key: Object key
            expires_in_seconds: How long the temporary file should exist

        Returns:
            New key for temporary file, or None if not deltaglider-compressed
        """
        start_time = self.clock.now()

        # Check if object exists and is deltaglider-compressed
        obj_head = self.storage.head(f"{bucket}/{key}")

        # If not found directly, try with .delta extension
        if obj_head is None and not key.endswith(".delta"):
            obj_head = self.storage.head(f"{bucket}/{key}.delta")
            if obj_head is not None:
                # Found the delta version, update the key
                key = f"{key}.delta"

        if obj_head is None:
            raise NotFoundError(f"Object not found: {key}")

        # Check if this is a deltaglider file
        is_delta = key.endswith(".delta")
        has_dg_metadata = "dg-file-sha256" in obj_head.metadata

        if not is_delta and not has_dg_metadata:
            # Not a deltaglider file, return None
            self.logger.debug(f"File {key} is not deltaglider-compressed")
            return None

        # Generate temporary file path
        import uuid

        # Use the original filename without .delta extension for the temp file
        original_name = key.removesuffix(".delta") if key.endswith(".delta") else key
        temp_filename = f"{uuid.uuid4().hex}_{Path(original_name).name}"
        temp_key = f".deltaglider/tmp/{temp_filename}"

        # Download and decompress the file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            decompressed_path = tmp_path / "decompressed"

            # Use the existing get method to decompress
            object_key = ObjectKey(bucket=bucket, key=key)
            self.get(object_key, decompressed_path)

            # Calculate expiration time
            expires_at = self.clock.now() + timedelta(seconds=expires_in_seconds)

            # Create metadata for temporary file
            metadata = {
                "dg-expires-at": expires_at.isoformat(),
                "dg-original-key": key,
                "dg-original-filename": Path(original_name).name,
                "dg-rehydrated": "true",
                "dg-created-at": self.clock.now().isoformat(),
            }

            # Upload the decompressed file
            self.logger.info(
                "Uploading rehydrated file",
                original_key=key,
                temp_key=temp_key,
                expires_at=expires_at.isoformat(),
            )

            self.storage.put(
                f"{bucket}/{temp_key}",
                decompressed_path,
                metadata,
            )

        duration = (self.clock.now() - start_time).total_seconds()
        self.logger.info(
            "Rehydration complete",
            original_key=key,
            temp_key=temp_key,
            duration=duration,
        )
        self.metrics.timing("deltaglider.rehydrate.duration", duration)
        self.metrics.increment("deltaglider.rehydrate.completed")

        return temp_key

    def purge_temp_files(self, bucket: str) -> dict[str, Any]:
        """Purge expired temporary files from .deltaglider/tmp/.

        Scans the .deltaglider/tmp/ prefix and deletes any files
        whose dg-expires-at metadata indicates they have expired.

        Args:
            bucket: S3 bucket to purge temp files from

        Returns:
            dict with purge statistics
        """
        start_time = self.clock.now()
        prefix = ".deltaglider/tmp/"

        self.logger.info("Starting temp file purge", bucket=bucket, prefix=prefix)

        deleted_count = 0
        expired_count = 0
        error_count = 0
        total_size_freed = 0
        errors = []

        # List all objects in temp directory
        for obj in self.storage.list(f"{bucket}/{prefix}"):
            if not obj.key.startswith(prefix):
                continue

            try:
                # Get object metadata
                obj_head = self.storage.head(f"{bucket}/{obj.key}")
                if obj_head is None:
                    continue

                # Check expiration
                expires_at_str = obj_head.metadata.get("dg-expires-at")
                if not expires_at_str:
                    # No expiration metadata, skip
                    self.logger.debug(f"No expiration metadata for {obj.key}")
                    continue

                # Parse expiration time
                from datetime import datetime

                try:
                    expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=UTC)
                except ValueError:
                    self.logger.warning(
                        f"Invalid expiration format for {obj.key}: {expires_at_str}"
                    )
                    continue

                # Check if expired
                if self.clock.now() >= expires_at:
                    expired_count += 1
                    # Delete the file
                    self.storage.delete(f"{bucket}/{obj.key}")
                    deleted_count += 1
                    total_size_freed += obj.size
                    self.logger.debug(
                        f"Deleted expired temp file {obj.key}",
                        expired_at=expires_at_str,
                        size=obj.size,
                    )

            except Exception as e:
                error_count += 1
                errors.append(f"Error processing {obj.key}: {str(e)}")
                self.logger.error(f"Failed to process temp file {obj.key}: {e}")

        duration = (self.clock.now() - start_time).total_seconds()

        result = {
            "bucket": bucket,
            "prefix": prefix,
            "deleted_count": deleted_count,
            "expired_count": expired_count,
            "error_count": error_count,
            "total_size_freed": total_size_freed,
            "duration_seconds": duration,
            "errors": errors,
        }

        self.logger.info(
            "Temp file purge complete",
            bucket=bucket,
            deleted=deleted_count,
            size_freed=total_size_freed,
            duration=duration,
        )

        self.metrics.timing("deltaglider.purge.duration", duration)
        self.metrics.gauge("deltaglider.purge.deleted_count", deleted_count)
        self.metrics.gauge("deltaglider.purge.size_freed", total_size_freed)

        return result
