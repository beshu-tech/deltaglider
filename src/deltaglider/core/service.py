"""Core DeltaService orchestration."""

import tempfile
import warnings
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
        obj_head = self.storage.head(f"{object_key.bucket}/{object_key.key}")
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
            self.logger.log_operation(
                op="get",
                key=object_key.key,
                deltaspace=f"{object_key.bucket}",
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
        if ref_head and ref_head.metadata.get("dg-file-sha256") != file_sha256:
            self.logger.warning("Reference creation race detected, using existing")
            # Proceed with existing reference
            ref_sha256 = ref_head.metadata["dg-file-sha256"]
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
        ref_sha256 = ref_head.metadata["dg-file-sha256"]

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

    def delete(self, object_key: ObjectKey) -> dict[str, Any]:
        """Delete an object (delta-aware).

        For delta files, just deletes the delta.
        For reference files, checks if any deltas depend on it first.
        For direct uploads, simply deletes the file.

        Returns:
            dict with deletion details including type and any warnings
        """
        start_time = self.clock.now()
        full_key = f"{object_key.bucket}/{object_key.key}"

        self.logger.info("Starting delete operation", key=object_key.key)

        # Check if object exists
        obj_head = self.storage.head(full_key)
        if obj_head is None:
            raise NotFoundError(f"Object not found: {object_key.key}")

        # Determine object type
        is_reference = object_key.key.endswith("/reference.bin")
        is_delta = object_key.key.endswith(".delta")
        is_direct = obj_head.metadata.get("compression") == "none"

        result: dict[str, Any] = {
            "key": object_key.key,
            "bucket": object_key.bucket,
            "deleted": False,
            "type": "unknown",
            "warnings": [],
        }

        if is_reference:
            # Check if any deltas depend on this reference
            prefix = object_key.key.rsplit("/", 1)[0] if "/" in object_key.key else ""
            dependent_deltas = []

            for obj in self.storage.list(f"{object_key.bucket}/{prefix}"):
                if obj.key.endswith(".delta") and obj.key != object_key.key:
                    # Check if this delta references our reference
                    delta_head = self.storage.head(f"{object_key.bucket}/{obj.key}")
                    if delta_head and delta_head.metadata.get("ref_key") == object_key.key:
                        dependent_deltas.append(obj.key)

            if dependent_deltas:
                warnings_list = result["warnings"]
                assert isinstance(warnings_list, list)
                warnings_list.append(
                    f"Reference has {len(dependent_deltas)} dependent delta(s). "
                    "Deleting this will make those deltas unrecoverable."
                )
                self.logger.warning(
                    "Reference has dependent deltas",
                    ref_key=object_key.key,
                    delta_count=len(dependent_deltas),
                    deltas=dependent_deltas[:5],  # Log first 5
                )

            # Delete the reference
            self.storage.delete(full_key)
            result["deleted"] = True
            result["type"] = "reference"
            result["dependent_deltas"] = len(dependent_deltas)

            # Clear from cache if present
            if "/" in object_key.key:
                deltaspace_prefix = object_key.key.rsplit("/", 1)[0]
                try:
                    self.cache.evict(object_key.bucket, deltaspace_prefix)
                except Exception as e:
                    self.logger.debug(f"Could not clear cache for {object_key.key}: {e}")

        elif is_delta:
            # Delete the delta file
            self.storage.delete(full_key)
            result["deleted"] = True
            result["type"] = "delta"
            result["original_name"] = obj_head.metadata.get("original_name", "unknown")

            # Check if this was the last delta in the DeltaSpace - if so, clean up reference.bin
            if "/" in object_key.key:
                deltaspace_prefix = "/".join(object_key.key.split("/")[:-1])
                ref_key = f"{deltaspace_prefix}/reference.bin"

                # Check if any other delta files exist in this DeltaSpace
                remaining_deltas = []
                for obj in self.storage.list(f"{object_key.bucket}/{deltaspace_prefix}"):
                    if obj.key.endswith(".delta") and obj.key != object_key.key:
                        remaining_deltas.append(obj.key)

                if not remaining_deltas:
                    # No more deltas - clean up the orphaned reference.bin
                    ref_full_key = f"{object_key.bucket}/{ref_key}"
                    ref_head = self.storage.head(ref_full_key)
                    if ref_head:
                        self.storage.delete(ref_full_key)
                        self.logger.info(
                            "Cleaned up orphaned reference.bin",
                            ref_key=ref_key,
                            reason="no remaining deltas",
                        )
                        result["cleaned_reference"] = ref_key

                        # Clear from cache
                        try:
                            self.cache.evict(object_key.bucket, deltaspace_prefix)
                        except Exception as e:
                            self.logger.debug(f"Could not clear cache for {deltaspace_prefix}: {e}")

        elif is_direct:
            # Simply delete the direct upload
            self.storage.delete(full_key)
            result["deleted"] = True
            result["type"] = "direct"
            result["original_name"] = obj_head.metadata.get("original_name", object_key.key)

        else:
            # Unknown file type, delete anyway
            self.storage.delete(full_key)
            result["deleted"] = True
            result["type"] = "unknown"

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
        self.metrics.increment(f"deltaglider.delete.{result['type']}")

        return result

    def delete_recursive(self, bucket: str, prefix: str) -> dict[str, Any]:
        """Recursively delete all objects under a prefix (delta-aware).

        Handles delta relationships intelligently:
        - Deletes deltas before references
        - Warns about orphaned deltas
        - Handles direct uploads

        Args:
            bucket: S3 bucket name
            prefix: Prefix to delete recursively

        Returns:
            dict with deletion statistics and any warnings
        """
        start_time = self.clock.now()
        self.logger.info("Starting recursive delete", bucket=bucket, prefix=prefix)

        # Ensure prefix ends with / for proper directory deletion
        if prefix and not prefix.endswith("/"):
            prefix = f"{prefix}/"

        # Collect all objects under prefix
        objects_to_delete = []
        references = []
        deltas = []
        direct_uploads = []
        affected_deltaspaces = set()

        for obj in self.storage.list(f"{bucket}/{prefix}" if prefix else bucket):
            if not obj.key.startswith(prefix) and prefix:
                continue

            if obj.key.endswith("/reference.bin"):
                references.append(obj.key)
            elif obj.key.endswith(".delta"):
                deltas.append(obj.key)
                # Track which deltaspaces are affected by this deletion
                if "/" in obj.key:
                    deltaspace_prefix = "/".join(obj.key.split("/")[:-1])
                    affected_deltaspaces.add(deltaspace_prefix)
            else:
                # Check if it's a direct upload
                obj_head = self.storage.head(f"{bucket}/{obj.key}")
                if obj_head and obj_head.metadata.get("compression") == "none":
                    direct_uploads.append(obj.key)
                else:
                    objects_to_delete.append(obj.key)

        # Also check for references in parent directories that might be affected
        # by the deletion of delta files in affected deltaspaces
        for deltaspace_prefix in affected_deltaspaces:
            ref_key = f"{deltaspace_prefix}/reference.bin"
            if ref_key not in references:
                # Check if this reference exists
                ref_head = self.storage.head(f"{bucket}/{ref_key}")
                if ref_head:
                    references.append(ref_key)

        result: dict[str, Any] = {
            "bucket": bucket,
            "prefix": prefix,
            "deleted_count": 0,
            "failed_count": 0,
            "deltas_deleted": len(deltas),
            "references_deleted": len(references),
            "direct_deleted": len(direct_uploads),
            "other_deleted": len(objects_to_delete),
            "errors": [],
            "warnings": [],
        }

        # Delete in order: other files -> direct uploads -> deltas -> references (with checks)
        # This ensures we don't delete references that deltas depend on prematurely
        regular_files = objects_to_delete + direct_uploads + deltas

        # Delete regular files first
        for key in regular_files:
            try:
                self.storage.delete(f"{bucket}/{key}")
                deleted_count = result["deleted_count"]
                assert isinstance(deleted_count, int)
                result["deleted_count"] = deleted_count + 1
                self.logger.debug(f"Deleted {key}")
            except Exception as e:
                failed_count = result["failed_count"]
                assert isinstance(failed_count, int)
                result["failed_count"] = failed_count + 1
                errors_list = result["errors"]
                assert isinstance(errors_list, list)
                errors_list.append(f"Failed to delete {key}: {str(e)}")
                self.logger.error(f"Failed to delete {key}: {e}")

        # Handle references intelligently - only delete if no files outside deletion scope depend on them
        references_kept = 0
        for ref_key in references:
            try:
                # Extract deltaspace prefix from reference.bin path
                if ref_key.endswith("/reference.bin"):
                    deltaspace_prefix = ref_key[:-14]  # Remove "/reference.bin"
                else:
                    deltaspace_prefix = ""

                # Check if there are any remaining files in this deltaspace
                # (outside of the deletion prefix)
                deltaspace_list_prefix = (
                    f"{bucket}/{deltaspace_prefix}" if deltaspace_prefix else bucket
                )
                remaining_objects = list(self.storage.list(deltaspace_list_prefix))

                # Filter out objects that are being deleted (within our deletion scope)
                # and the reference.bin file itself
                deletion_prefix_full = f"{bucket}/{prefix}" if prefix else bucket
                has_remaining_files = False

                for remaining_obj in remaining_objects:
                    obj_full_path = f"{bucket}/{remaining_obj.key}"
                    # Skip if this object is within our deletion scope
                    if prefix and obj_full_path.startswith(deletion_prefix_full):
                        continue
                    # Skip if this is the reference.bin file itself
                    if remaining_obj.key == ref_key:
                        continue
                    # If we find any other file, the reference is still needed
                    has_remaining_files = True
                    break

                if not has_remaining_files:
                    # Safe to delete this reference.bin
                    self.storage.delete(f"{bucket}/{ref_key}")
                    deleted_count = result["deleted_count"]
                    assert isinstance(deleted_count, int)
                    result["deleted_count"] = deleted_count + 1
                    self.logger.debug(f"Deleted reference {ref_key}")
                else:
                    # Keep the reference as it's still needed
                    references_kept += 1
                    warnings_list = result["warnings"]
                    assert isinstance(warnings_list, list)
                    warnings_list.append(f"Kept reference {ref_key} (still in use)")
                    self.logger.info(
                        f"Kept reference {ref_key} - still in use outside deletion scope"
                    )

            except Exception as e:
                failed_count = result["failed_count"]
                assert isinstance(failed_count, int)
                result["failed_count"] = failed_count + 1
                errors_list = result["errors"]
                assert isinstance(errors_list, list)
                errors_list.append(f"Failed to delete reference {ref_key}: {str(e)}")
                self.logger.error(f"Failed to delete reference {ref_key}: {e}")

        # Update reference deletion count
        references_deleted = result["references_deleted"]
        assert isinstance(references_deleted, int)
        result["references_deleted"] = references_deleted - references_kept

        # Clear any cached references for this prefix
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
            deleted=result["deleted_count"],
            failed=result["failed_count"],
            duration=duration,
        )
        self.metrics.timing("deltaglider.delete_recursive.duration", duration)
        self.metrics.increment("deltaglider.delete_recursive.completed")

        return result
