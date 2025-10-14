"""AWS S3 CLI compatible commands."""

import shutil
import sys
from pathlib import Path

import click

from ...core import (
    DeltaService,
    DeltaSpace,
    ObjectKey,
    build_s3_url,
    is_s3_url,
)
from ...core import parse_s3_url as core_parse_s3_url
from .sync import fetch_s3_object_heads

__all__ = [
    "is_s3_path",
    "parse_s3_url",
    "determine_operation",
    "upload_file",
    "download_file",
    "copy_s3_to_s3",
    "migrate_s3_to_s3",
    "handle_recursive",
    "log_aws_region",
]


def log_aws_region(service: DeltaService, region_override: bool = False) -> None:
    """Log the AWS region being used and warn about cross-region charges.

    This function:
    1. Detects if running on EC2
    2. Compares EC2 region with S3 client region
    3. Warns about potential cross-region data transfer charges
    4. Helps users optimize for cost and performance

    Args:
        service: DeltaService instance with storage adapter
        region_override: True if user explicitly specified --region flag
    """
    try:
        from ...adapters.ec2_metadata import EC2MetadataAdapter
        from ...adapters.storage_s3 import S3StorageAdapter

        if not isinstance(service.storage, S3StorageAdapter):
            return  # Not using S3 storage, skip

        # Get S3 client region
        s3_region = service.storage.client.meta.region_name
        if not s3_region:
            s3_region = "us-east-1"  # boto3 default

        # Check if running on EC2
        ec2_metadata = EC2MetadataAdapter()
        if ec2_metadata.is_running_on_ec2():
            ec2_region = ec2_metadata.get_region()
            ec2_az = ec2_metadata.get_availability_zone()

            # Log EC2 context
            click.echo(f"EC2 Instance: {ec2_az or ec2_region or 'unknown'}")
            click.echo(f"S3 Client Region: {s3_region}")

            # Check for region mismatch
            if ec2_region and ec2_region != s3_region:
                if region_override:
                    # User explicitly set --region, warn about costs
                    click.echo("")
                    click.secho(
                        f"⚠️  WARNING: EC2 region={ec2_region} != S3 client region={s3_region}",
                        fg="yellow",
                        bold=True,
                    )
                    click.secho(
                        f"    Expect cross-region/NAT data charges. Align regions (set client region={ec2_region})",
                        fg="yellow",
                    )
                    click.secho(
                        "    before proceeding. Or drop --region for automatic region resolution.",
                        fg="yellow",
                    )
                    click.echo("")
                else:
                    # Auto-detected mismatch, but user can still cancel
                    click.echo("")
                    click.secho(
                        f"ℹ️  INFO: EC2 region ({ec2_region}) differs from configured S3 region ({s3_region})",
                        fg="cyan",
                    )
                    click.secho(
                        f"    Consider using --region {ec2_region} to avoid cross-region charges.",
                        fg="cyan",
                    )
                    click.echo("")
            elif ec2_region and ec2_region == s3_region:
                # Regions match - optimal configuration
                click.secho("✓ Regions aligned - no cross-region charges", fg="green")
        else:
            # Not on EC2, just show S3 region
            click.echo(f"S3 Client Region: {s3_region}")

    except Exception:
        pass  # Silently ignore errors getting region info


def is_s3_path(path: str) -> bool:
    """Check if path is an S3 URL."""
    return is_s3_url(path)


def parse_s3_url(url: str) -> tuple[str, str]:
    """Parse S3 URL into bucket and key."""
    parsed = core_parse_s3_url(url, strip_trailing_slash=True)
    return parsed.bucket, parsed.key


def determine_operation(source: str, dest: str) -> str:
    """Determine operation type based on source and destination."""
    source_is_s3 = is_s3_path(source)
    dest_is_s3 = is_s3_path(dest)

    if not source_is_s3 and dest_is_s3:
        return "upload"
    elif source_is_s3 and not dest_is_s3:
        return "download"
    elif source_is_s3 and dest_is_s3:
        return "copy"
    else:
        raise ValueError("At least one path must be an S3 URL")


def upload_file(
    service: DeltaService,
    local_path: Path,
    s3_url: str,
    max_ratio: float | None = None,
    no_delta: bool = False,
    quiet: bool = False,
) -> None:
    """Upload a file to S3 with delta compression."""
    bucket, key = parse_s3_url(s3_url)

    # If key is empty or ends with /, append filename
    if not key or key.endswith("/"):
        key = (key + local_path.name).lstrip("/")

    delta_space = DeltaSpace(bucket=bucket, prefix="/".join(key.split("/")[:-1]))

    dest_url = build_s3_url(bucket, key)

    try:
        # Check if delta should be disabled
        if no_delta:
            # Direct upload without delta compression
            with open(local_path, "rb") as f:
                service.storage.put(f"{bucket}/{key}", f, {})

            if not quiet:
                file_size = local_path.stat().st_size
                click.echo(f"upload: '{local_path}' to '{dest_url}' ({file_size} bytes)")
        else:
            # Use delta compression
            summary = service.put(local_path, delta_space, max_ratio)

            if not quiet:
                if summary.delta_size:
                    ratio = round((summary.delta_size / summary.file_size) * 100, 1)
                    click.echo(
                        f"upload: '{local_path}' to '{build_s3_url(bucket, summary.key)}' "
                        f"(delta: {ratio}% of original)"
                    )
                else:
                    click.echo(
                        f"upload: '{local_path}' to '{build_s3_url(bucket, summary.key)}' "
                        f"(reference: {summary.file_size} bytes)"
                    )

    except Exception as e:
        click.echo(f"upload failed: {e}", err=True)
        sys.exit(1)


def download_file(
    service: DeltaService,
    s3_url: str,
    local_path: Path | None = None,
    quiet: bool = False,
) -> None:
    """Download a file from S3 with delta reconstruction."""
    bucket, key = parse_s3_url(s3_url)

    # Auto-detect .delta file if needed
    obj_key = ObjectKey(bucket=bucket, key=key)
    actual_key = key

    try:
        # Check if file exists, try adding .delta if not found
        obj_head = service.storage.head(f"{bucket}/{key}")
        if obj_head is None and not key.endswith(".delta"):
            delta_key = f"{key}.delta"
            delta_head = service.storage.head(f"{bucket}/{delta_key}")
            if delta_head is not None:
                actual_key = delta_key
                obj_key = ObjectKey(bucket=bucket, key=delta_key)
                if not quiet:
                    click.echo(f"Auto-detected delta: {build_s3_url(bucket, delta_key)}")

        # Determine output path
        if local_path is None:
            # If S3 path ends with /, it's an error
            if not key:
                click.echo("Error: Cannot download bucket root, specify a key", err=True)
                sys.exit(1)

            # Use filename from S3 key
            if actual_key.endswith(".delta"):
                local_path = Path(Path(actual_key).stem)
            else:
                local_path = Path(Path(actual_key).name)

        # Create parent directories if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download and reconstruct
        service.get(obj_key, local_path)

        if not quiet:
            file_size = local_path.stat().st_size
            click.echo(
                f"download: '{build_s3_url(bucket, actual_key)}' to '{local_path}' ({file_size} bytes)"
            )

    except Exception as e:
        click.echo(f"download failed: {e}", err=True)
        sys.exit(1)


def copy_s3_to_s3(
    service: DeltaService,
    source_url: str,
    dest_url: str,
    quiet: bool = False,
    max_ratio: float | None = None,
    no_delta: bool = False,
) -> None:
    """Copy object between S3 locations with optional delta compression.

    This performs a direct S3-to-S3 transfer using streaming to preserve
    the original file content and apply delta compression at the destination.
    """
    source_bucket, source_key = parse_s3_url(source_url)
    dest_bucket, dest_key = parse_s3_url(dest_url)

    if not quiet:
        click.echo(
            f"copy: '{build_s3_url(source_bucket, source_key)}' "
            f"to '{build_s3_url(dest_bucket, dest_key)}'"
        )

    try:
        # Get the source object as a stream
        source_stream = service.storage.get(f"{source_bucket}/{source_key}")

        # Determine the destination deltaspace
        dest_key_parts = dest_key.split("/")
        if len(dest_key_parts) > 1:
            dest_prefix = "/".join(dest_key_parts[:-1])
        else:
            dest_prefix = ""

        dest_deltaspace = DeltaSpace(bucket=dest_bucket, prefix=dest_prefix)

        # If delta is disabled or max_ratio specified, use direct put
        if no_delta:
            # Direct storage put without delta compression
            service.storage.put(f"{dest_bucket}/{dest_key}", source_stream, {})
            if not quiet:
                click.echo("Copy completed (no delta compression)")
        else:
            # Write to a temporary file and use override_name to preserve original filename
            import tempfile

            # Extract original filename from source
            original_filename = Path(source_key).name

            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(source_key).suffix) as tmp:
                tmp_path = Path(tmp.name)

                # Write stream to temp file
                with open(tmp_path, "wb") as f:
                    shutil.copyfileobj(source_stream, f)

            try:
                # Use DeltaService.put() with override_name to preserve original filename
                summary = service.put(
                    tmp_path, dest_deltaspace, max_ratio, override_name=original_filename
                )

                if not quiet:
                    if summary.delta_size:
                        ratio = round((summary.delta_size / summary.file_size) * 100, 1)
                        click.echo(f"Copy completed with delta compression ({ratio}% of original)")
                    else:
                        click.echo("Copy completed (stored as reference)")
            finally:
                # Clean up temp file
                tmp_path.unlink(missing_ok=True)

    except Exception as e:
        click.echo(f"S3-to-S3 copy failed: {e}", err=True)
        raise


def migrate_s3_to_s3(
    service: DeltaService,
    source_url: str,
    dest_url: str,
    exclude: str | None = None,
    include: str | None = None,
    quiet: bool = False,
    no_delta: bool = False,
    max_ratio: float | None = None,
    dry_run: bool = False,
    skip_confirm: bool = False,
    preserve_prefix: bool = False,
    region_override: bool = False,
) -> None:
    """Migrate objects from one S3 location to another with delta compression.

    Features:
    - Resume support: Only copies files that don't exist in destination
    - Progress tracking: Shows migration progress
    - Confirmation prompt: Shows file count before starting
    - Prefix preservation: Optionally preserves source prefix structure in destination
    - EC2 region detection: Warns about cross-region data transfer charges

    Args:
        service: DeltaService instance
        source_url: Source S3 URL
        dest_url: Destination S3 URL
        exclude: Pattern to exclude files
        include: Pattern to include files
        quiet: Suppress output
        no_delta: Disable delta compression
        max_ratio: Maximum delta/file ratio
        dry_run: Show what would be migrated without migrating
        skip_confirm: Skip confirmation prompt
        preserve_prefix: Preserve source prefix in destination
        region_override: True if user explicitly specified --region flag
    """
    import fnmatch

    source_bucket, source_prefix = parse_s3_url(source_url)
    dest_bucket, dest_prefix = parse_s3_url(dest_url)

    # Ensure prefixes end with / if they exist
    if source_prefix and not source_prefix.endswith("/"):
        source_prefix += "/"
    if dest_prefix and not dest_prefix.endswith("/"):
        dest_prefix += "/"

    # Determine the effective destination prefix based on preserve_prefix setting
    effective_dest_prefix = dest_prefix
    if preserve_prefix and source_prefix:
        # Extract the last component of the source prefix (e.g., "prefix1/" from "path/to/prefix1/")
        source_prefix_name = source_prefix.rstrip("/").split("/")[-1]
        if source_prefix_name:
            # Append source prefix name to destination
            effective_dest_prefix = (dest_prefix or "") + source_prefix_name + "/"

    if not quiet:
        # Log AWS region being used (helps users verify their configuration)
        # Pass region_override to warn about cross-region charges if user explicitly set --region
        log_aws_region(service, region_override=region_override)

        source_display = build_s3_url(source_bucket, source_prefix)
        dest_display = build_s3_url(dest_bucket, dest_prefix)
        effective_dest_display = build_s3_url(dest_bucket, effective_dest_prefix)

        if preserve_prefix and source_prefix:
            click.echo(f"Migrating from {source_display}")
            click.echo(f"           to {effective_dest_display}")
        else:
            click.echo(f"Migrating from {source_display} to {dest_display}")
        click.echo("Scanning source and destination buckets...")

    # List source objects
    source_list_prefix = f"{source_bucket}/{source_prefix}" if source_prefix else source_bucket
    source_objects = []

    for obj in service.storage.list(source_list_prefix):
        # Skip reference.bin files (internal delta reference)
        if obj.key.endswith("/reference.bin"):
            continue
        # Skip .delta files in source (we'll handle the original files)
        if obj.key.endswith(".delta"):
            continue

        # Apply include/exclude filters
        rel_key = obj.key.removeprefix(source_prefix) if source_prefix else obj.key
        if exclude and fnmatch.fnmatch(rel_key, exclude):
            continue
        if include and not fnmatch.fnmatch(rel_key, include):
            continue

        source_objects.append(obj)

    # List destination objects to detect what needs copying
    dest_list_prefix = (
        f"{dest_bucket}/{effective_dest_prefix}" if effective_dest_prefix else dest_bucket
    )
    dest_keys = set()

    for obj in service.storage.list(dest_list_prefix):
        # Get the relative key in destination
        rel_key = obj.key.removeprefix(effective_dest_prefix) if effective_dest_prefix else obj.key
        # Remove .delta suffix for comparison
        if rel_key.endswith(".delta"):
            rel_key = rel_key[:-6]
        # Skip reference.bin
        if not rel_key.endswith("/reference.bin"):
            dest_keys.add(rel_key)

    # Determine files to migrate (not in destination)
    files_to_migrate = []
    total_size = 0

    for source_obj in source_objects:
        # Get relative path from source prefix
        rel_key = source_obj.key.removeprefix(source_prefix) if source_prefix else source_obj.key

        # Check if already exists in destination
        if rel_key not in dest_keys:
            files_to_migrate.append((source_obj, rel_key))
            total_size += source_obj.size

    # Show summary and ask for confirmation
    if not files_to_migrate:
        if not quiet:
            click.echo("All files are already migrated. Nothing to do.")
        return

    if not quiet:

        def format_bytes(size: int) -> str:
            size_float = float(size)
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if size_float < 1024.0:
                    return f"{size_float:.2f} {unit}"
                size_float /= 1024.0
            return f"{size_float:.2f} PB"

        click.echo("")
        click.echo(f"Files to migrate: {len(files_to_migrate)}")
        click.echo(f"Total size: {format_bytes(total_size)}")
        if len(dest_keys) > 0:
            click.echo(f"Already migrated: {len(dest_keys)} files (will be skipped)")

    # Handle dry run mode early (before confirmation prompt)
    if dry_run:
        if not quiet:
            click.echo("\n--- DRY RUN MODE ---")
            for _obj, rel_key in files_to_migrate[:10]:  # Show first 10 files
                click.echo(f"  Would migrate: {rel_key}")
            if len(files_to_migrate) > 10:
                click.echo(f"  ... and {len(files_to_migrate) - 10} more files")
        return

    # Ask for confirmation before proceeding with actual migration
    if not quiet and not skip_confirm:
        click.echo("")
        if not click.confirm("Do you want to proceed with the migration?"):
            click.echo("Migration cancelled.")
            return

    # Perform migration
    if not quiet:
        click.echo(f"\nStarting migration of {len(files_to_migrate)} files...")

    successful = 0
    failed = 0
    failed_files = []

    for i, (source_obj, rel_key) in enumerate(files_to_migrate, 1):
        source_s3_url = build_s3_url(source_bucket, source_obj.key)

        # Construct destination URL using effective prefix
        if effective_dest_prefix:
            dest_key = effective_dest_prefix + rel_key
        else:
            dest_key = rel_key
        dest_s3_url = build_s3_url(dest_bucket, dest_key)

        try:
            if not quiet:
                progress = f"[{i}/{len(files_to_migrate)}]"
                click.echo(f"{progress} Migrating {rel_key}...", nl=False)

            # Copy with delta compression
            copy_s3_to_s3(
                service,
                source_s3_url,
                dest_s3_url,
                quiet=True,
                max_ratio=max_ratio,
                no_delta=no_delta,
            )

            successful += 1
            if not quiet:
                click.echo(" ✓")

        except Exception as e:
            failed += 1
            failed_files.append((rel_key, str(e)))
            if not quiet:
                click.echo(f" ✗ ({e})")

    # Show final summary
    if not quiet:
        click.echo("")
        click.echo("Migration Summary:")
        click.echo(f"  Successfully migrated: {successful} files")
        if failed > 0:
            click.echo(f"  Failed: {failed} files")
            click.echo("\nFailed files:")
            for file, error in failed_files[:10]:  # Show first 10 failures
                click.echo(f"    {file}: {error}")
            if len(failed_files) > 10:
                click.echo(f"    ... and {len(failed_files) - 10} more failures")

        # Show compression statistics from cache if available (no bucket scan)
        if successful > 0 and not no_delta:
            try:
                from ...client import DeltaGliderClient

                client = DeltaGliderClient(service)
                # Use cached stats only - don't scan bucket (prevents blocking)
                cached_stats = client._get_cached_bucket_stats(dest_bucket, "quick")
                if cached_stats and cached_stats.delta_objects > 0:
                    click.echo(
                        f"\nCompression achieved: {cached_stats.average_compression_ratio:.1%}"
                    )
                    click.echo(f"Space saved: {format_bytes(cached_stats.space_saved)}")
            except Exception:
                pass  # Ignore stats errors


def handle_recursive(
    service: DeltaService,
    source: str,
    dest: str,
    recursive: bool,
    exclude: str | None,
    include: str | None,
    quiet: bool,
    no_delta: bool,
    max_ratio: float | None,
) -> None:
    """Handle recursive operations for directories."""
    operation = determine_operation(source, dest)

    if operation == "upload":
        # Local directory to S3
        source_path = Path(source)
        if not source_path.is_dir():
            click.echo(f"Error: {source} is not a directory", err=True)
            sys.exit(1)

        # Get all files recursively
        import fnmatch

        files = []
        for file_path in source_path.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(source_path)

                # Apply exclude/include filters
                if exclude and fnmatch.fnmatch(str(rel_path), exclude):
                    continue
                if include and not fnmatch.fnmatch(str(rel_path), include):
                    continue

                files.append((file_path, rel_path))

        if not quiet:
            click.echo(f"Uploading {len(files)} files...")

        # Upload each file
        for file_path, rel_path in files:
            # Construct S3 key
            dest_key = dest.rstrip("/") + "/" + str(rel_path).replace("\\", "/")
            upload_file(service, file_path, dest_key, max_ratio, no_delta, quiet)

    elif operation == "download":
        # S3 to local directory
        bucket, prefix = parse_s3_url(source)
        dest_path = Path(dest)
        dest_path.mkdir(parents=True, exist_ok=True)

        objects = fetch_s3_object_heads(service, bucket, prefix)

        if not quiet:
            click.echo(f"Downloading {len(objects)} files...")

        # Download each object
        for obj in objects:
            # Skip reference.bin files (internal delta reference)
            if obj.key.endswith("/reference.bin"):
                continue

            # Skip if not matching include/exclude patterns
            rel_key = obj.key.removeprefix(prefix).lstrip("/")

            import fnmatch

            if exclude and fnmatch.fnmatch(rel_key, exclude):
                continue
            if include and not fnmatch.fnmatch(rel_key, include):
                continue

            # Construct local path - remove .delta extension if present
            local_rel_key = rel_key
            if local_rel_key.endswith(".delta"):
                local_rel_key = local_rel_key[:-6]  # Remove .delta extension

            local_path = dest_path / local_rel_key
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download file
            s3_url = build_s3_url(bucket, obj.key)
            download_file(service, s3_url, local_path, quiet)

    elif operation == "copy":
        # S3-to-S3 recursive copy with migration support
        migrate_s3_to_s3(
            service,
            source,
            dest,
            exclude=exclude,
            include=include,
            quiet=quiet,
            no_delta=no_delta,
            max_ratio=max_ratio,
            dry_run=False,
            skip_confirm=True,  # Don't prompt for cp command
            preserve_prefix=True,  # Always preserve prefix for cp -r
            region_override=False,  # cp command doesn't track region override explicitly
        )
