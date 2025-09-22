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
from ...core import DeltaService, Leaf, ObjectKey


def create_service(log_level: str = "INFO") -> DeltaService:
    """Create service with wired adapters."""
    # Get config from environment
    cache_dir = Path(os.environ.get("DG_CACHE_DIR", "/tmp/.deltaglider/reference_cache"))
    max_ratio = float(os.environ.get("DG_MAX_RATIO", "0.5"))

    # Create adapters
    hasher = Sha256Adapter()
    storage = S3StorageAdapter()
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
@click.argument("file", type=click.Path(exists=True, path_type=Path))
@click.argument("s3_url")
@click.option("--max-ratio", type=float, help="Max delta/file ratio (default: 0.5)")
@click.pass_obj
def put(service: DeltaService, file: Path, s3_url: str, max_ratio: float | None) -> None:
    """Upload file as reference or delta."""
    # Parse S3 URL
    if not s3_url.startswith("s3://"):
        click.echo(f"Error: Invalid S3 URL: {s3_url}", err=True)
        sys.exit(1)

    # Extract bucket and prefix
    s3_path = s3_url[5:].rstrip("/")
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    leaf = Leaf(bucket=bucket, prefix=prefix)

    try:
        summary = service.put(file, leaf, max_ratio)

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
                    click.echo(f"Error: File not found: s3://{bucket}/{key} (also tried .delta)", err=True)
                    sys.exit(1)
            else:
                click.echo(f"Error: File not found: s3://{bucket}/{key}", err=True)
                sys.exit(1)
    except Exception as e:
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
