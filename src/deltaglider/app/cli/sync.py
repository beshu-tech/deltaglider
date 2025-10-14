"""AWS S3 sync command implementation."""

from pathlib import Path

import click

from ...core import DeltaService
from ...core.object_listing import list_all_objects, object_dict_to_head
from ...ports import ObjectHead


def fetch_s3_object_heads(service: DeltaService, bucket: str, prefix: str) -> list[ObjectHead]:
    """Retrieve all objects for a prefix, falling back to iterator when needed."""
    try:
        listing = list_all_objects(
            service.storage,
            bucket=bucket,
            prefix=prefix,
            max_keys=1000,
            logger=getattr(service, "logger", None),
        )
    except (RuntimeError, NotImplementedError):
        list_prefix = f"{bucket}/{prefix}" if prefix else bucket
        return list(service.storage.list(list_prefix))

    return [object_dict_to_head(obj) for obj in listing.objects]


def get_local_files(
    local_dir: Path, exclude: str | None = None, include: str | None = None
) -> dict[str, tuple[Path, int]]:
    """Get all local files with relative paths and sizes."""
    import fnmatch

    files = {}
    for file_path in local_dir.rglob("*"):
        if file_path.is_file():
            rel_path = file_path.relative_to(local_dir)
            rel_path_str = str(rel_path).replace("\\", "/")

            # Apply exclude/include filters
            if exclude and fnmatch.fnmatch(rel_path_str, exclude):
                continue
            if include and not fnmatch.fnmatch(rel_path_str, include):
                continue

            files[rel_path_str] = (file_path, file_path.stat().st_size)

    return files


def get_s3_files(
    service: DeltaService,
    bucket: str,
    prefix: str,
    exclude: str | None = None,
    include: str | None = None,
) -> dict[str, ObjectHead]:
    """Get all S3 objects with relative paths."""
    import fnmatch

    files = {}
    objects = fetch_s3_object_heads(service, bucket, prefix)

    for obj in objects:
        # Skip reference.bin files (internal)
        if obj.key.endswith("/reference.bin"):
            continue

        # Get relative path from prefix
        rel_path = obj.key[len(prefix) :] if prefix else obj.key
        rel_path = rel_path.lstrip("/")

        # Remove .delta extension for comparison
        display_path = rel_path
        if display_path.endswith(".delta"):
            display_path = display_path[:-6]

        # Apply exclude/include filters
        if exclude and fnmatch.fnmatch(display_path, exclude):
            continue
        if include and not fnmatch.fnmatch(display_path, include):
            continue

        files[display_path] = obj

    return files


def should_sync_file(
    local_path: Path, local_size: int, s3_obj: ObjectHead | None, size_only: bool = False
) -> bool:
    """Determine if a file should be synced."""
    if s3_obj is None:
        # File doesn't exist in S3
        return True

    # For delta files, we can't easily compare sizes
    if s3_obj.key.endswith(".delta"):
        # Compare by modification time if available
        local_mtime = local_path.stat().st_mtime_ns // 1_000_000  # Convert to milliseconds
        s3_mtime = int(s3_obj.last_modified.timestamp() * 1000)
        # Sync if local is newer (with 1 second tolerance)
        return local_mtime > (s3_mtime + 1000)

    if size_only:
        # Only compare sizes
        return local_size != s3_obj.size

    # Compare by modification time and size
    local_mtime = local_path.stat().st_mtime_ns // 1_000_000
    s3_mtime = int(s3_obj.last_modified.timestamp() * 1000)

    # Sync if sizes differ or local is newer
    return local_size != s3_obj.size or local_mtime > (s3_mtime + 1000)


def sync_to_s3(
    service: DeltaService,
    local_dir: Path,
    bucket: str,
    prefix: str,
    delete: bool = False,
    dryrun: bool = False,
    quiet: bool = False,
    exclude: str | None = None,
    include: str | None = None,
    size_only: bool = False,
    no_delta: bool = False,
    max_ratio: float | None = None,
) -> None:
    """Sync local directory to S3."""
    from .aws_compat import upload_file

    # Get file lists
    local_files = get_local_files(local_dir, exclude, include)
    s3_files = get_s3_files(service, bucket, prefix, exclude, include)

    # Find files to upload
    files_to_upload = []
    for rel_path, (local_path, local_size) in local_files.items():
        s3_obj = s3_files.get(rel_path)
        if should_sync_file(local_path, local_size, s3_obj, size_only):
            files_to_upload.append((rel_path, local_path))

    # Find files to delete
    files_to_delete = []
    if delete:
        for rel_path, s3_obj in s3_files.items():
            if rel_path not in local_files:
                files_to_delete.append((rel_path, s3_obj))

    # Upload files
    upload_count = 0
    for rel_path, local_path in files_to_upload:
        s3_key = f"{prefix}/{rel_path}" if prefix else rel_path
        s3_url = f"s3://{bucket}/{s3_key}"

        if dryrun:
            click.echo(f"(dryrun) upload: {local_path} to {s3_url}")
        else:
            if not quiet:
                click.echo(f"upload: {local_path} to {s3_url}")
            upload_file(service, local_path, s3_url, max_ratio, no_delta, quiet=True)
            upload_count += 1

    # Delete files
    delete_count = 0
    for _rel_path, s3_obj in files_to_delete:
        s3_url = f"s3://{bucket}/{s3_obj.key}"

        if dryrun:
            click.echo(f"(dryrun) delete: {s3_url}")
        else:
            if not quiet:
                click.echo(f"delete: {s3_url}")
            service.storage.delete(f"{bucket}/{s3_obj.key}")
            delete_count += 1

    # Summary
    if not quiet and not dryrun:
        if upload_count > 0 or delete_count > 0:
            click.echo(f"Sync completed: {upload_count} uploaded, {delete_count} deleted")
        else:
            click.echo("Sync completed: Already up to date")


def sync_from_s3(
    service: DeltaService,
    bucket: str,
    prefix: str,
    local_dir: Path,
    delete: bool = False,
    dryrun: bool = False,
    quiet: bool = False,
    exclude: str | None = None,
    include: str | None = None,
    size_only: bool = False,
) -> None:
    """Sync S3 to local directory."""
    from .aws_compat import download_file

    # Create local directory if it doesn't exist
    local_dir.mkdir(parents=True, exist_ok=True)

    # Get file lists
    local_files = get_local_files(local_dir, exclude, include)
    s3_files = get_s3_files(service, bucket, prefix, exclude, include)

    # Find files to download
    files_to_download = []
    for rel_path, s3_obj in s3_files.items():
        local_path = local_dir / rel_path
        local_info = local_files.get(rel_path)

        if local_info is None:
            # File doesn't exist locally
            files_to_download.append((rel_path, s3_obj, local_path))
        else:
            local_file_path, local_size = local_info
            if should_sync_file(local_file_path, local_size, s3_obj, size_only):
                files_to_download.append((rel_path, s3_obj, local_path))

    # Find files to delete
    files_to_delete = []
    if delete:
        for rel_path, (local_path, _) in local_files.items():
            if rel_path not in s3_files:
                files_to_delete.append(local_path)

    # Download files
    download_count = 0
    for _rel_path, s3_obj, local_path in files_to_download:
        s3_url = f"s3://{bucket}/{s3_obj.key}"

        if dryrun:
            click.echo(f"(dryrun) download: {s3_url} to {local_path}")
        else:
            if not quiet:
                click.echo(f"download: {s3_url} to {local_path}")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            download_file(service, s3_url, local_path, quiet=True)
            download_count += 1

    # Delete files
    delete_count = 0
    for local_path in files_to_delete:
        if dryrun:
            click.echo(f"(dryrun) delete: {local_path}")
        else:
            if not quiet:
                click.echo(f"delete: {local_path}")
            local_path.unlink()
            # Clean up empty directories
            try:
                local_path.parent.rmdir()
            except OSError:
                pass  # Directory not empty
            delete_count += 1

    # Summary
    if not quiet and not dryrun:
        if download_count > 0 or delete_count > 0:
            click.echo(f"Sync completed: {download_count} downloaded, {delete_count} deleted")
        else:
            click.echo("Sync completed: Already up to date")
