"""CLI main entry point."""

import json
import os
import sys
from pathlib import Path

import click

from ...adapters import (
    FsCacheAdapter,
    NoopMetricsAdapter,
    S3StorageAdapter,
    Sha256Adapter,
    StdLoggerAdapter,
    UtcClockAdapter,
    XdeltaAdapter,
)
from ...core import DeltaService, DeltaSpace, ObjectKey
from .aws_compat import (
    copy_s3_to_s3,
    determine_operation,
    download_file,
    handle_recursive,
    is_s3_path,
    parse_s3_url,
    upload_file,
)
from .sync import sync_from_s3, sync_to_s3


def create_service(
    log_level: str = "INFO",
    endpoint_url: str | None = None,
    region: str | None = None,
    profile: str | None = None,
) -> DeltaService:
    """Create service with wired adapters."""
    # Get config from environment
    cache_dir = Path(os.environ.get("DG_CACHE_DIR", "/tmp/.deltaglider/reference_cache"))
    max_ratio = float(os.environ.get("DG_MAX_RATIO", "0.5"))

    # Set AWS environment variables if provided
    if endpoint_url:
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    if region:
        os.environ["AWS_DEFAULT_REGION"] = region
    if profile:
        os.environ["AWS_PROFILE"] = profile

    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter(endpoint_url=endpoint_url)
    diff = XdeltaAdapter()
    cache = FsCacheAdapter(cache_dir, hasher)
    clock = UtcClockAdapter()
    logger = StdLoggerAdapter(level=log_level)
    metrics = NoopMetricsAdapter()

    # Create service
    return DeltaService(
        storage=storage,
        diff=diff,
        hasher=hasher,
        cache=cache,
        clock=clock,
        logger=logger,
        metrics=metrics,
        max_ratio=max_ratio,
    )


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx: click.Context, debug: bool) -> None:
    """DeltaGlider - Delta-aware S3 file storage wrapper."""
    log_level = "DEBUG" if debug else os.environ.get("DG_LOG_LEVEL", "INFO")
    ctx.obj = create_service(log_level)


@cli.command()
@click.argument("source")
@click.argument("dest")
@click.option("--recursive", "-r", is_flag=True, help="Copy files recursively")
@click.option("--exclude", help="Exclude files matching pattern")
@click.option("--include", help="Include only files matching pattern")
@click.option("--quiet", "-q", is_flag=True, help="Suppress output")
@click.option("--no-delta", is_flag=True, help="Disable delta compression")
@click.option("--max-ratio", type=float, help="Max delta/file ratio (default: 0.5)")
@click.option("--endpoint-url", help="Override S3 endpoint URL")
@click.option("--region", help="AWS region")
@click.option("--profile", help="AWS profile to use")
@click.pass_obj
def cp(
    service: DeltaService,
    source: str,
    dest: str,
    recursive: bool,
    exclude: str | None,
    include: str | None,
    quiet: bool,
    no_delta: bool,
    max_ratio: float | None,
    endpoint_url: str | None,
    region: str | None,
    profile: str | None,
) -> None:
    """Copy files to/from S3 (AWS S3 compatible).

    Examples:
        deltaglider cp myfile.zip s3://bucket/path/
        deltaglider cp s3://bucket/file.zip ./
        deltaglider cp -r local_dir/ s3://bucket/path/
        deltaglider cp s3://bucket1/file s3://bucket2/file
    """
    # Recreate service with AWS parameters if provided
    if endpoint_url or region or profile:
        service = create_service(
            log_level=os.environ.get("DG_LOG_LEVEL", "INFO"),
            endpoint_url=endpoint_url,
            region=region,
            profile=profile,
        )

    try:
        # Determine operation type
        operation = determine_operation(source, dest)

        # Handle recursive operations for directories
        if recursive:
            if operation == "copy":
                click.echo("S3-to-S3 recursive copy not yet implemented", err=True)
                sys.exit(1)
            handle_recursive(
                service, source, dest, recursive, exclude, include, quiet, no_delta, max_ratio
            )
            return

        # Handle single file operations
        if operation == "upload":
            local_path = Path(source)
            if not local_path.exists():
                click.echo(f"Error: File not found: {source}", err=True)
                sys.exit(1)
            upload_file(service, local_path, dest, max_ratio, no_delta, quiet)

        elif operation == "download":
            # Determine local path
            local_path = None
            if dest != ".":
                local_path = Path(dest)
            download_file(service, source, local_path, quiet)

        elif operation == "copy":
            copy_s3_to_s3(service, source, dest, quiet)

    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("s3_url", required=False)
@click.option("--recursive", "-r", is_flag=True, help="List recursively")
@click.option("--human-readable", "-h", is_flag=True, help="Human readable sizes")
@click.option("--summarize", is_flag=True, help="Display summary information")
@click.option("--endpoint-url", help="Override S3 endpoint URL")
@click.option("--region", help="AWS region")
@click.option("--profile", help="AWS profile to use")
@click.pass_obj
def ls(
    service: DeltaService,
    s3_url: str | None,
    recursive: bool,
    human_readable: bool,
    summarize: bool,
    endpoint_url: str | None,
    region: str | None,
    profile: str | None,
) -> None:
    """List S3 buckets or objects (AWS S3 compatible).

    Examples:
        deltaglider ls                           # List all buckets
        deltaglider ls s3://bucket/               # List objects in bucket
        deltaglider ls s3://bucket/prefix/        # List objects with prefix
        deltaglider ls -r s3://bucket/            # List recursively
        deltaglider ls -h s3://bucket/            # Human readable sizes
    """
    # Recreate service with AWS parameters if provided
    if endpoint_url or region or profile:
        service = create_service(
            log_level=os.environ.get("DG_LOG_LEVEL", "INFO"),
            endpoint_url=endpoint_url,
            region=region,
            profile=profile,
        )

    try:
        if not s3_url:
            # List all buckets
            import boto3

            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url or os.environ.get("AWS_ENDPOINT_URL"),
            )
            response = s3_client.list_buckets()
            for bucket in response.get("Buckets", []):
                click.echo(
                    f"{bucket['CreationDate'].strftime('%Y-%m-%d %H:%M:%S')}  s3://{bucket['Name']}"
                )

        else:
            # List objects in bucket/prefix
            bucket_name: str
            prefix_str: str
            bucket_name, prefix_str = parse_s3_url(s3_url)

            # Format bytes to human readable
            def format_bytes(size: int) -> str:
                if not human_readable:
                    return str(size)
                size_float = float(size)
                for unit in ["B", "K", "M", "G", "T"]:
                    if size_float < 1024.0:
                        return f"{size_float:6.1f}{unit}"
                    size_float /= 1024.0
                return f"{size_float:.1f}P"

            # List objects
            list_prefix = f"{bucket_name}/{prefix_str}" if prefix_str else bucket_name
            objects = list(service.storage.list(list_prefix))

            # Filter by recursive flag
            if not recursive:
                # Only show direct children
                seen_prefixes = set()
                filtered_objects = []
                for obj in objects:
                    rel_path = obj.key[len(prefix_str) :] if prefix_str else obj.key
                    if "/" in rel_path:
                        # It's in a subdirectory
                        subdir = rel_path.split("/")[0] + "/"
                        if subdir not in seen_prefixes:
                            seen_prefixes.add(subdir)
                            # Show as directory
                            full_prefix = f"{prefix_str}{subdir}" if prefix_str else subdir
                            click.echo(f"                           PRE {full_prefix}")
                    else:
                        # Direct file
                        if rel_path:  # Only add if there's actually a file at this level
                            filtered_objects.append(obj)
                objects = filtered_objects

            # Display objects
            total_size = 0
            total_count = 0

            for obj in objects:
                # Skip reference.bin files (internal)
                if obj.key.endswith("/reference.bin"):
                    continue

                total_size += obj.size
                total_count += 1

                # Format the display
                size_str = format_bytes(obj.size)
                date_str = obj.last_modified.strftime("%Y-%m-%d %H:%M:%S")

                # Remove .delta extension from display
                display_key = obj.key
                if display_key.endswith(".delta"):
                    display_key = display_key[:-6]

                click.echo(f"{date_str} {size_str:>10} s3://{bucket_name}/{display_key}")

            # Show summary if requested
            if summarize:
                click.echo("")
                click.echo(f"Total Objects: {total_count}")
                click.echo(f"   Total Size: {format_bytes(total_size)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("s3_url")
@click.option("--recursive", "-r", is_flag=True, help="Remove recursively")
@click.option("--dryrun", is_flag=True, help="Show what would be deleted without deleting")
@click.option("--quiet", "-q", is_flag=True, help="Suppress output")
@click.option("--endpoint-url", help="Override S3 endpoint URL")
@click.option("--region", help="AWS region")
@click.option("--profile", help="AWS profile to use")
@click.pass_obj
def rm(
    service: DeltaService,
    s3_url: str,
    recursive: bool,
    dryrun: bool,
    quiet: bool,
    endpoint_url: str | None,
    region: str | None,
    profile: str | None,
) -> None:
    """Remove S3 objects (AWS S3 compatible).

    Examples:
        deltaglider rm s3://bucket/file.zip           # Remove single file
        deltaglider rm -r s3://bucket/prefix/         # Remove recursively
        deltaglider rm --dryrun s3://bucket/file     # Preview what would be deleted
    """
    # Recreate service with AWS parameters if provided
    if endpoint_url or region or profile:
        service = create_service(
            log_level=os.environ.get("DG_LOG_LEVEL", "INFO"),
            endpoint_url=endpoint_url,
            region=region,
            profile=profile,
        )

    try:
        bucket, prefix = parse_s3_url(s3_url)

        # Check if this is a single object or prefix
        if not recursive and not prefix.endswith("/"):
            # Single object deletion
            objects_to_delete = []

            # Check for the object itself
            obj_key = prefix
            obj = service.storage.head(f"{bucket}/{obj_key}")
            if obj:
                objects_to_delete.append(obj_key)

            # Check for .delta version
            if not obj_key.endswith(".delta"):
                delta_key = f"{obj_key}.delta"
                delta_obj = service.storage.head(f"{bucket}/{delta_key}")
                if delta_obj:
                    objects_to_delete.append(delta_key)

            # Check for reference.bin in the same leaf
            if "/" in obj_key:
                leaf_prefix = "/".join(obj_key.split("/")[:-1])
                ref_key = f"{leaf_prefix}/reference.bin"
            else:
                ref_key = "reference.bin"

            # Only delete reference.bin if it's the last file in the leaf
            ref_obj = service.storage.head(f"{bucket}/{ref_key}")
            if ref_obj:
                # Check if there are other files in this leaf
                list_prefix = f"{bucket}/{leaf_prefix}" if "/" in obj_key else bucket
                other_files = list(service.storage.list(list_prefix))
                # Count files excluding reference.bin
                non_ref_files = [o for o in other_files if not o.key.endswith("/reference.bin")]
                if len(non_ref_files) <= len(objects_to_delete):
                    # This would be the last file(s), safe to delete reference.bin
                    objects_to_delete.append(ref_key)

            if not objects_to_delete:
                if not quiet:
                    click.echo(f"delete: Object not found: s3://{bucket}/{obj_key}")
                return

            # Delete objects
            for key in objects_to_delete:
                if dryrun:
                    click.echo(f"(dryrun) delete: s3://{bucket}/{key}")
                else:
                    service.storage.delete(f"{bucket}/{key}")
                    if not quiet:
                        click.echo(f"delete: s3://{bucket}/{key}")

        else:
            # Recursive deletion or prefix deletion
            if not recursive:
                click.echo("Error: Cannot remove directories. Use --recursive", err=True)
                sys.exit(1)

            # List all objects with prefix
            list_prefix = f"{bucket}/{prefix}" if prefix else bucket
            objects = list(service.storage.list(list_prefix))

            if not objects:
                if not quiet:
                    click.echo(f"delete: No objects found with prefix: s3://{bucket}/{prefix}")
                return

            # Delete all objects
            deleted_count = 0
            for obj in objects:
                if dryrun:
                    click.echo(f"(dryrun) delete: s3://{bucket}/{obj.key}")
                else:
                    service.storage.delete(f"{bucket}/{obj.key}")
                    if not quiet:
                        click.echo(f"delete: s3://{bucket}/{obj.key}")
                deleted_count += 1

            if not quiet and not dryrun:
                click.echo(f"Deleted {deleted_count} object(s)")

    except Exception as e:
        click.echo(f"delete failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("source")
@click.argument("dest")
@click.option("--delete", is_flag=True, help="Delete dest files not in source")
@click.option("--exclude", help="Exclude files matching pattern")
@click.option("--include", help="Include only files matching pattern")
@click.option("--dryrun", is_flag=True, help="Show what would be synced without syncing")
@click.option("--quiet", "-q", is_flag=True, help="Suppress output")
@click.option("--size-only", is_flag=True, help="Compare only file sizes, not timestamps")
@click.option("--no-delta", is_flag=True, help="Disable delta compression")
@click.option("--max-ratio", type=float, help="Max delta/file ratio (default: 0.5)")
@click.option("--endpoint-url", help="Override S3 endpoint URL")
@click.option("--region", help="AWS region")
@click.option("--profile", help="AWS profile to use")
@click.pass_obj
def sync(
    service: DeltaService,
    source: str,
    dest: str,
    delete: bool,
    exclude: str | None,
    include: str | None,
    dryrun: bool,
    quiet: bool,
    size_only: bool,
    no_delta: bool,
    max_ratio: float | None,
    endpoint_url: str | None,
    region: str | None,
    profile: str | None,
) -> None:
    """Synchronize directories with S3 (AWS S3 compatible).

    Examples:
        deltaglider sync ./local-dir/ s3://bucket/path/     # Local to S3
        deltaglider sync s3://bucket/path/ ./local-dir/     # S3 to local
        deltaglider sync --delete ./dir/ s3://bucket/       # Mirror exactly
        deltaglider sync --exclude "*.log" ./dir/ s3://bucket/
    """
    # Recreate service with AWS parameters if provided
    if endpoint_url or region or profile:
        service = create_service(
            log_level=os.environ.get("DG_LOG_LEVEL", "INFO"),
            endpoint_url=endpoint_url,
            region=region,
            profile=profile,
        )

    try:
        # Determine sync direction
        source_is_s3 = is_s3_path(source)
        dest_is_s3 = is_s3_path(dest)

        if source_is_s3 and dest_is_s3:
            click.echo("Error: S3 to S3 sync not yet implemented", err=True)
            sys.exit(1)
        elif not source_is_s3 and not dest_is_s3:
            click.echo("Error: At least one path must be an S3 URL", err=True)
            sys.exit(1)

        if dest_is_s3:
            # Sync local to S3
            local_dir = Path(source)
            if not local_dir.is_dir():
                click.echo(f"Error: Source must be a directory: {source}", err=True)
                sys.exit(1)

            bucket, prefix = parse_s3_url(dest)
            sync_to_s3(
                service,
                local_dir,
                bucket,
                prefix,
                delete,
                dryrun,
                quiet,
                exclude,
                include,
                size_only,
                no_delta,
                max_ratio,
            )
        else:
            # Sync S3 to local
            bucket, prefix = parse_s3_url(source)
            local_dir = Path(dest)

            sync_from_s3(
                service,
                bucket,
                prefix,
                local_dir,
                delete,
                dryrun,
                quiet,
                exclude,
                include,
                size_only,
            )

    except Exception as e:
        click.echo(f"sync failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True, path_type=Path))
@click.argument("s3_url")
@click.option("--max-ratio", type=float, help="Max delta/file ratio (default: 0.5)")
@click.pass_obj
def put(service: DeltaService, file: Path, s3_url: str, max_ratio: float | None) -> None:
    """Upload file as reference or delta (legacy command, use 'cp' instead)."""
    # Parse S3 URL
    if not s3_url.startswith("s3://"):
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

    # Extract bucket and prefix
    s3_path = s3_url[5:].rstrip("/")
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    delta_space = DeltaSpace(bucket=bucket, prefix=prefix)

    try:
        summary = service.put(file, delta_space, max_ratio)

        # Output JSON summary
        output = {
            "operation": summary.operation,
            "bucket": summary.bucket,
            "key": summary.key,
            "original_name": summary.original_name,
            "file_size": summary.file_size,
            "file_sha256": summary.file_sha256,
        }

        if summary.delta_size is not None:
            output["delta_size"] = summary.delta_size
            output["delta_ratio"] = round(summary.delta_ratio or 0, 3)

        if summary.ref_key:
            output["ref_key"] = summary.ref_key
            output["ref_sha256"] = summary.ref_sha256

        output["cache_hit"] = summary.cache_hit

        click.echo(json.dumps(output, indent=2))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("s3_url")
@click.option("-o", "--output", type=click.Path(path_type=Path), help="Output file path")
@click.pass_obj
def get(service: DeltaService, s3_url: str, output: Path | None) -> None:
    """Download and hydrate delta file.

    The S3 URL can be either:
    - Full path to delta file: s3://bucket/path/to/file.zip.delta
    - Path to original file (will append .delta): s3://bucket/path/to/file.zip
    """
    # Parse S3 URL
    if not s3_url.startswith("s3://"):
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

    s3_path = s3_url[5:]
    parts = s3_path.split("/", 1)
    if len(parts) != 2:
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

    bucket = parts[0]
    key = parts[1]

    # Try to determine if this is a direct file or needs .delta appended
    # First try the key as-is
    obj_key = ObjectKey(bucket=bucket, key=key)

    # Check if the file exists using the service's storage port
    # which already has proper credentials configured
    try:
        # Try to head the object as-is
        obj_head = service.storage.head(f"{bucket}/{key}")
        if obj_head is not None:
            click.echo(f"Found file: s3://{bucket}/{key}")
        else:
            # If not found and doesn't end with .delta, try adding .delta
            if not key.endswith(".delta"):
                delta_key = f"{key}.delta"
                delta_head = service.storage.head(f"{bucket}/{delta_key}")
                if delta_head is not None:
                    key = delta_key
                    obj_key = ObjectKey(bucket=bucket, key=key)
                    click.echo(f"Found delta file: s3://{bucket}/{key}")
                else:
                    click.echo(
                        f"Error: File not found: s3://{bucket}/{key} (also tried .delta)", err=True
                    )
                    sys.exit(1)
            else:
                click.echo(f"Error: File not found: s3://{bucket}/{key}", err=True)
                sys.exit(1)
    except Exception:
        # For unexpected errors, just proceed with the original key
        click.echo(f"Warning: Could not check file existence, proceeding with: s3://{bucket}/{key}")

    # Determine output path
    if output is None:
        # Extract original name from delta name
        if key.endswith(".delta"):
            output = Path(Path(key).stem)
        else:
            output = Path(Path(key).name)

    try:
        service.get(obj_key, output)
        click.echo(f"Successfully retrieved: {output}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("s3_url")
@click.pass_obj
def verify(service: DeltaService, s3_url: str) -> None:
    """Verify integrity of delta file."""
    # Parse S3 URL
    if not s3_url.startswith("s3://"):
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

    s3_path = s3_url[5:]
    parts = s3_path.split("/", 1)
    if len(parts) != 2:
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

    bucket = parts[0]
    key = parts[1]

    obj_key = ObjectKey(bucket=bucket, key=key)

    try:
        result = service.verify(obj_key)

        output = {
            "valid": result.valid,
            "expected_sha256": result.expected_sha256,
            "actual_sha256": result.actual_sha256,
            "message": result.message,
        }

        click.echo(json.dumps(output, indent=2))

        if not result.valid:
            sys.exit(1)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    cli()
