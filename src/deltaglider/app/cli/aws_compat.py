"""AWS S3 CLI compatible commands."""

import sys
from pathlib import Path

import click

from ...core import DeltaService, DeltaSpace, ObjectKey


def is_s3_path(path: str) -> bool:
    """Check if path is an S3 URL."""
    return path.startswith("s3://")


def parse_s3_url(url: str) -> tuple[str, str]:
    """Parse S3 URL into bucket and key."""
    if not url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL: {url}")

    s3_path = url[5:].rstrip("/")
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


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

    try:
        # Check if delta should be disabled
        if no_delta:
            # Direct upload without delta compression
            with open(local_path, "rb") as f:
                service.storage.put(f"{bucket}/{key}", f, {})

            if not quiet:
                file_size = local_path.stat().st_size
                click.echo(f"upload: '{local_path}' to 's3://{bucket}/{key}' ({file_size} bytes)")
        else:
            # Use delta compression
            summary = service.put(local_path, delta_space, max_ratio)

            if not quiet:
                if summary.delta_size:
                    ratio = round((summary.delta_size / summary.file_size) * 100, 1)
                    click.echo(
                        f"upload: '{local_path}' to 's3://{bucket}/{summary.key}' "
                        f"(delta: {ratio}% of original)"
                    )
                else:
                    click.echo(
                        f"upload: '{local_path}' to 's3://{bucket}/{summary.key}' "
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
                    click.echo(f"Auto-detected delta: s3://{bucket}/{delta_key}")

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
                f"download: 's3://{bucket}/{actual_key}' to '{local_path}' ({file_size} bytes)"
            )

    except Exception as e:
        click.echo(f"download failed: {e}", err=True)
        sys.exit(1)


def copy_s3_to_s3(
    service: DeltaService,
    source_url: str,
    dest_url: str,
    quiet: bool = False,
) -> None:
    """Copy object between S3 locations."""
    # For now, implement as download + upload
    # TODO: Optimize with server-side copy when possible

    source_bucket, source_key = parse_s3_url(source_url)
    dest_bucket, dest_key = parse_s3_url(dest_url)

    if not quiet:
        click.echo(f"copy: 's3://{source_bucket}/{source_key}' to 's3://{dest_bucket}/{dest_key}'")

    # Use temporary file
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=Path(source_key).suffix) as tmp:
        tmp_path = Path(tmp.name)

        # Download from source
        download_file(service, source_url, tmp_path, quiet=True)

        # Upload to destination
        upload_file(service, tmp_path, dest_url, quiet=True)

        if not quiet:
            click.echo("Copy completed")


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

        # List all objects with prefix
        # Note: S3StorageAdapter.list() expects "bucket/prefix" format
        list_prefix = f"{bucket}/{prefix}" if prefix else bucket
        objects = list(service.storage.list(list_prefix))

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
            s3_url = f"s3://{bucket}/{obj.key}"
            download_file(service, s3_url, local_path, quiet)

    else:
        click.echo("S3-to-S3 recursive copy not yet implemented", err=True)
        sys.exit(1)
