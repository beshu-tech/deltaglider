"""CLI main entry point."""

import atexit
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import click

from ... import __version__
from ...adapters import (
    NoopMetricsAdapter,
    S3StorageAdapter,
    Sha256Adapter,
    StdLoggerAdapter,
    UtcClockAdapter,
    XdeltaAdapter,
)
from ...core import DeltaService, ObjectKey
from ...ports import MetricsPort
from ...ports.cache import CachePort
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
    max_ratio = float(os.environ.get("DG_MAX_RATIO", "0.5"))
    metrics_type = os.environ.get("DG_METRICS", "logging")  # Options: noop, logging, cloudwatch

    # SECURITY: Always use ephemeral process-isolated cache
    cache_dir = Path(tempfile.mkdtemp(prefix="deltaglider-", dir="/tmp"))
    # Register cleanup handler to remove cache on exit
    atexit.register(lambda: shutil.rmtree(cache_dir, ignore_errors=True))

    # Set AWS environment variables if provided (for compatibility with other AWS tools)
    if endpoint_url:
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    if region:
        os.environ["AWS_DEFAULT_REGION"] = region
    if profile:
        os.environ["AWS_PROFILE"] = profile

    # Build boto3_kwargs for explicit parameter passing (preferred over env vars)
    boto3_kwargs: dict[str, Any] = {}
    if region:
        boto3_kwargs["region_name"] = region

    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter(endpoint_url=endpoint_url, boto3_kwargs=boto3_kwargs)
    diff = XdeltaAdapter()

    # SECURITY: Configurable cache with encryption and backend selection
    from deltaglider.adapters import ContentAddressedCache, EncryptedCache, MemoryCache

    # Select backend: memory or filesystem
    cache_backend = os.environ.get("DG_CACHE_BACKEND", "filesystem")  # Options: filesystem, memory
    base_cache: CachePort
    if cache_backend == "memory":
        max_size_mb = int(os.environ.get("DG_CACHE_MEMORY_SIZE_MB", "100"))
        base_cache = MemoryCache(hasher, max_size_mb=max_size_mb, temp_dir=cache_dir)
    else:
        # Filesystem-backed with Content-Addressed Storage
        base_cache = ContentAddressedCache(cache_dir, hasher)

    # Always apply encryption with ephemeral keys (security hardening)
    # Encryption key is optional via DG_CACHE_ENCRYPTION_KEY (ephemeral if not set)
    cache: CachePort = EncryptedCache.from_env(base_cache)

    clock = UtcClockAdapter()
    logger = StdLoggerAdapter(level=log_level)

    # Create metrics adapter based on configuration
    metrics: MetricsPort
    if metrics_type == "cloudwatch":
        # Import here to avoid dependency if not used
        from ...adapters.metrics_cloudwatch import CloudWatchMetricsAdapter

        metrics = CloudWatchMetricsAdapter(
            namespace=os.environ.get("DG_METRICS_NAMESPACE", "DeltaGlider"),
            region=region,
            endpoint_url=endpoint_url if endpoint_url and "localhost" in endpoint_url else None,
        )
    elif metrics_type == "logging":
        from ...adapters.metrics_cloudwatch import LoggingMetricsAdapter

        metrics = LoggingMetricsAdapter(log_level=log_level)
    else:
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


def _version_callback(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    """Callback for --version option."""
    if value:
        click.echo(f"deltaglider {__version__}")
        ctx.exit(0)


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=_version_callback,
    help="Show version and exit",
)
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
            copy_s3_to_s3(service, source, dest, quiet, max_ratio, no_delta)

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

            # Ensure prefix ends with / if it's meant to be a directory
            # This helps with proper path handling
            if prefix_str and not prefix_str.endswith("/"):
                # Check if this is a file or directory by listing
                # For now, assume it's a directory prefix
                prefix_str = prefix_str + "/"

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

            # List objects using SDK (automatically filters .delta and reference.bin)
            from deltaglider.client import DeltaGliderClient

            client = DeltaGliderClient(service)
            dg_response = client.list_objects(
                Bucket=bucket_name,
                Prefix=prefix_str,
                MaxKeys=10000,
                Delimiter="/" if not recursive else "",
            )
            objects = dg_response["Contents"]

            # Filter by recursive flag
            if not recursive:
                # Show common prefixes (subdirectories) from S3 response
                for common_prefix in dg_response.get("CommonPrefixes", []):
                    prefix_path = common_prefix.get("Prefix", "")
                    # Show only the directory name, not the full path
                    if prefix_str:
                        # Strip the current prefix to show only the subdirectory
                        display_name = prefix_path[len(prefix_str) :]
                    else:
                        display_name = prefix_path
                    click.echo(f"                           PRE {display_name}")

                # Only show files at current level (not in subdirectories)
                filtered_objects = []
                for obj in objects:
                    obj_key = obj["Key"]
                    rel_path = obj_key[len(prefix_str) :] if prefix_str else obj_key
                    # Only include if it's a direct child (no / in relative path)
                    if "/" not in rel_path and rel_path:
                        filtered_objects.append(obj)
                objects = filtered_objects

            # Display objects (SDK already filters reference.bin and strips .delta)
            total_size = 0
            total_count = 0

            for obj in objects:
                total_size += obj["Size"]
                total_count += 1

                # Format the display
                size_str = format_bytes(obj["Size"])
                # last_modified is a string from SDK, parse it if needed
                last_modified = obj.get("LastModified", "")
                if isinstance(last_modified, str):
                    # Already a string, extract date portion
                    date_str = last_modified[:19].replace("T", " ")
                else:
                    date_str = last_modified.strftime("%Y-%m-%d %H:%M:%S")

                # Show only the filename relative to current prefix (like AWS CLI)
                if prefix_str:
                    display_key = obj["Key"][len(prefix_str) :]
                else:
                    display_key = obj["Key"]

                click.echo(f"{date_str} {size_str:>10} {display_key}")

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

            # Check for reference.bin in the same deltaspace
            if "/" in obj_key:
                deltaspace_prefix = "/".join(obj_key.split("/")[:-1])
                ref_key = f"{deltaspace_prefix}/reference.bin"
            else:
                ref_key = "reference.bin"

            # Only delete reference.bin if it's the last file in the deltaspace
            ref_obj = service.storage.head(f"{bucket}/{ref_key}")
            if ref_obj:
                # Check if there are other files in this deltaspace
                list_prefix = f"{bucket}/{deltaspace_prefix}" if "/" in obj_key else bucket
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

            # Use the service's delete_recursive method for proper delta-aware deletion
            if dryrun:
                # For dryrun, we need to simulate what would be deleted
                objects = list(service.storage.list(f"{bucket}/{prefix}" if prefix else bucket))
                if not objects:
                    if not quiet:
                        click.echo(f"delete: No objects found with prefix: s3://{bucket}/{prefix}")
                    return

                for obj in objects:
                    click.echo(f"(dryrun) delete: s3://{bucket}/{obj.key}")

                if not quiet:
                    click.echo(f"Would delete {len(objects)} object(s)")
            else:
                # Use the core service method for actual deletion
                result = service.delete_recursive(bucket, prefix)

                # Report the results
                if not quiet:
                    if result["deleted_count"] == 0:
                        click.echo(f"delete: No objects found with prefix: s3://{bucket}/{prefix}")
                    else:
                        click.echo(f"Deleted {result['deleted_count']} object(s)")

                        # Show warnings if any references were kept
                        for warning in result.get("warnings", []):
                            if "Kept reference" in warning:
                                click.echo(
                                    f"Keeping reference file (still in use): s3://{bucket}/{warning.split()[2]}"
                                )

                # Report any errors
                if result["failed_count"] > 0:
                    for error in result.get("errors", []):
                        click.echo(f"Error: {error}", err=True)

                    if result["failed_count"] > 0:
                        sys.exit(1)

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
@click.argument("s3_url")
@click.pass_obj
def verify(service: DeltaService, s3_url: str) -> None:
    """Verify integrity of delta file."""
    try:
        bucket, key = parse_s3_url(s3_url)
        if not key:
            raise ValueError("Missing key")
    except ValueError:
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

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


@cli.command()
@click.argument("source")
@click.argument("dest")
@click.option("--exclude", help="Exclude files matching pattern")
@click.option("--include", help="Include only files matching pattern")
@click.option("--quiet", "-q", is_flag=True, help="Suppress output")
@click.option("--no-delta", is_flag=True, help="Disable delta compression")
@click.option("--max-ratio", type=float, help="Max delta/file ratio (default: 0.5)")
@click.option("--dry-run", is_flag=True, help="Show what would be migrated without migrating")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.option(
    "--no-preserve-prefix", is_flag=True, help="Don't preserve source prefix in destination"
)
@click.option("--endpoint-url", help="Override S3 endpoint URL")
@click.option("--region", help="AWS region")
@click.option("--profile", help="AWS profile to use")
@click.pass_obj
def migrate(
    service: DeltaService,
    source: str,
    dest: str,
    exclude: str | None,
    include: str | None,
    quiet: bool,
    no_delta: bool,
    max_ratio: float | None,
    dry_run: bool,
    yes: bool,
    no_preserve_prefix: bool,
    endpoint_url: str | None,
    region: str | None,
    profile: str | None,
) -> None:
    """Migrate S3 bucket/prefix to DeltaGlider-compressed storage.

    This command facilitates the migration of existing S3 objects to another bucket
    with DeltaGlider compression. It supports:
    - Resume capability: Only copies files that don't exist in destination
    - Progress tracking: Shows migration progress
    - Confirmation prompt: Shows file count before starting (use --yes to skip)
    - Prefix preservation: By default, source prefix is preserved in destination

    When migrating a prefix, the source prefix name is preserved by default:
        s3://src/prefix1/ → s3://dest/      creates s3://dest/prefix1/
        s3://src/a/b/c/  → s3://dest/x/    creates s3://dest/x/c/

    Use --no-preserve-prefix to disable this behavior:
        s3://src/prefix1/ → s3://dest/      creates s3://dest/ (files at root)

    Examples:
        deltaglider migrate s3://old-bucket/ s3://new-bucket/
        deltaglider migrate s3://old-bucket/data/ s3://new-bucket/
        deltaglider migrate --no-preserve-prefix s3://src/v1/ s3://dest/
        deltaglider migrate --dry-run s3://old-bucket/ s3://new-bucket/
        deltaglider migrate --yes --quiet s3://old-bucket/ s3://new-bucket/
    """
    from .aws_compat import is_s3_path, migrate_s3_to_s3

    # Recreate service with AWS parameters if provided
    if endpoint_url or region or profile:
        service = create_service(
            log_level=os.environ.get("DG_LOG_LEVEL", "INFO"),
            endpoint_url=endpoint_url,
            region=region,
            profile=profile,
        )

    try:
        # Validate both paths are S3
        if not is_s3_path(source) or not is_s3_path(dest):
            click.echo("Error: Both source and destination must be S3 paths", err=True)
            sys.exit(1)

        # Perform migration
        migrate_s3_to_s3(
            service,
            source,
            dest,
            exclude=exclude,
            include=include,
            quiet=quiet,
            no_delta=no_delta,
            max_ratio=max_ratio,
            dry_run=dry_run,
            skip_confirm=yes,
            preserve_prefix=not no_preserve_prefix,
            region_override=region is not None,  # True if user explicitly specified --region
        )

    except Exception as e:
        click.echo(f"Migration failed: {e}", err=True)
        sys.exit(1)


@cli.command(short_help="Get bucket statistics and compression metrics")
@click.argument("bucket")
@click.option("--sampled", is_flag=True, help="Balanced mode: one sample per deltaspace (~5-15s)")
@click.option(
    "--detailed", is_flag=True, help="Most accurate: HEAD for all deltas (slowest, ~1min+)"
)
@click.option("--refresh", is_flag=True, help="Force cache refresh even if valid")
@click.option("--no-cache", is_flag=True, help="Skip caching entirely (both read and write)")
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
@click.pass_obj
def stats(
    service: DeltaService,
    bucket: str,
    sampled: bool,
    detailed: bool,
    refresh: bool,
    no_cache: bool,
    output_json: bool,
) -> None:
    """Get bucket statistics and compression metrics with intelligent S3-based caching.

    BUCKET can be specified as:
      - s3://bucket-name/
      - s3://bucket-name
      - bucket-name

    Modes (mutually exclusive):
      - quick (default): Fast listing-only stats (~0.5s), approximate compression metrics
      - --sampled: Balanced mode - one HEAD per deltaspace (~5-15s for typical buckets)
      - --detailed: Most accurate - HEAD for every delta file (slowest, ~1min+ for large buckets)

    Caching (NEW - massive performance improvement!):
      Stats are cached in S3 at .deltaglider/stats_{mode}.json (one per mode).
      Cache is automatically validated on every call using object count + size.
      If bucket changed, stats are recomputed automatically.

      Performance with cache:
        - Cache hit: ~0.1s (200x faster than recomputation!)
        - Cache miss: Full computation time (creates cache for next time)
        - Cache invalid: Auto-recomputes when bucket changes

    Options:
      --refresh: Force cache refresh even if valid (use when you need fresh data now)
      --no-cache: Skip caching entirely - always recompute (useful for testing/debugging)
      --json: Output in JSON format for automation/scripting

    Examples:
      deltaglider stats mybucket                    # Fast (~0.1s with cache, ~0.5s without)
      deltaglider stats mybucket --sampled          # Balanced accuracy/speed (~5-15s first run)
      deltaglider stats mybucket --detailed         # Most accurate (~1-10min first run, ~0.1s cached)
      deltaglider stats mybucket --refresh          # Force recomputation even if cached
      deltaglider stats mybucket --no-cache         # Always compute fresh (skip cache)
      deltaglider stats mybucket --json             # JSON output for scripts
      deltaglider stats s3://mybucket/              # Also accepts s3:// URLs

    Timing Logs:
      Set DG_LOG_LEVEL=INFO to see detailed phase timing with timestamps:
        [HH:MM:SS.mmm] Phase 1: LIST completed in 0.52s - Found 1523 objects
        [HH:MM:SS.mmm] Phase 2: Cache HIT in 0.06s - Using cached stats
        [HH:MM:SS.mmm] COMPLETE: Total time 0.58s

    See docs/STATS_CACHING.md for complete documentation.
    """
    from ...client import DeltaGliderClient
    from ...client_operations.stats import StatsMode

    try:
        # Parse bucket from S3 URL if needed
        if is_s3_path(bucket):
            bucket, _prefix = parse_s3_url(bucket)

        if not bucket:
            click.echo("Error: Invalid bucket name", err=True)
            sys.exit(1)

        if sampled and detailed:
            click.echo("Error: --sampled and --detailed cannot be used together", err=True)
            sys.exit(1)

        if refresh and no_cache:
            click.echo("Error: --refresh and --no-cache cannot be used together", err=True)
            sys.exit(1)

        mode: StatsMode = "quick"
        if sampled:
            mode = "sampled"
        if detailed:
            mode = "detailed"

        # Create client from service
        client = DeltaGliderClient(service=service)

        # Get bucket stats with caching control
        use_cache = not no_cache
        bucket_stats = client.get_bucket_stats(
            bucket, mode=mode, use_cache=use_cache, refresh_cache=refresh
        )

        if output_json:
            # JSON output
            output = {
                "bucket": bucket_stats.bucket,
                "object_count": bucket_stats.object_count,
                "total_size": bucket_stats.total_size,
                "compressed_size": bucket_stats.compressed_size,
                "space_saved": bucket_stats.space_saved,
                "average_compression_ratio": bucket_stats.average_compression_ratio,
                "delta_objects": bucket_stats.delta_objects,
                "direct_objects": bucket_stats.direct_objects,
            }
            click.echo(json.dumps(output, indent=2))
        else:
            # Human-readable output
            def format_bytes(size: float) -> str:
                """Format bytes to human-readable size."""
                for unit in ["B", "KB", "MB", "GB", "TB"]:
                    if size < 1024.0:
                        return f"{size:.2f} {unit}"
                    size /= 1024.0
                return f"{size:.2f} PB"

            click.echo(f"Bucket Statistics: {bucket_stats.bucket}")
            click.echo(f"{'=' * 60}")
            click.echo(f"Total Objects:        {bucket_stats.object_count:,}")
            click.echo(f"  Delta Objects:      {bucket_stats.delta_objects:,}")
            click.echo(f"  Direct Objects:     {bucket_stats.direct_objects:,}")
            click.echo("")
            click.echo(
                f"Original Size:        {format_bytes(bucket_stats.total_size)} ({bucket_stats.total_size:,} bytes)"
            )
            click.echo(
                f"Compressed Size:      {format_bytes(bucket_stats.compressed_size)} ({bucket_stats.compressed_size:,} bytes)"
            )
            click.echo(
                f"Space Saved:          {format_bytes(bucket_stats.space_saved)} ({bucket_stats.space_saved:,} bytes)"
            )
            click.echo(f"Compression Ratio:    {bucket_stats.average_compression_ratio:.1%}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    cli()
