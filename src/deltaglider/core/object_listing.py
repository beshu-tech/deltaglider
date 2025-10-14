"""Shared helpers for listing bucket objects with pagination support."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..ports.storage import ObjectHead


@dataclass(slots=True)
class ObjectListing:
    """All objects and prefixes returned from a bucket listing."""

    objects: list[dict[str, Any]] = field(default_factory=list)
    common_prefixes: list[str] = field(default_factory=list)
    key_count: int = 0
    is_truncated: bool = False
    next_continuation_token: str | None = None


def list_objects_page(
    storage: Any,
    *,
    bucket: str,
    prefix: str = "",
    delimiter: str = "",
    max_keys: int = 1000,
    start_after: str | None = None,
    continuation_token: str | None = None,
) -> ObjectListing:
    """Perform a single list_objects call using the storage adapter."""
    if not hasattr(storage, "list_objects"):
        raise NotImplementedError("Storage adapter does not support list_objects")

    response = storage.list_objects(
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        start_after=start_after,
        continuation_token=continuation_token,
    )

    return ObjectListing(
        objects=list(response.get("objects", [])),
        common_prefixes=list(response.get("common_prefixes", [])),
        key_count=response.get("key_count", len(response.get("objects", []))),
        is_truncated=bool(response.get("is_truncated", False)),
        next_continuation_token=response.get("next_continuation_token"),
    )


def list_all_objects(
    storage: Any,
    *,
    bucket: str,
    prefix: str = "",
    delimiter: str = "",
    max_keys: int = 1000,
    logger: Any | None = None,
    max_iterations: int = 10_000,
) -> ObjectListing:
    """Fetch all objects under the given bucket/prefix with pagination safety."""
    import time
    from datetime import UTC, datetime

    aggregated = ObjectListing()
    continuation_token: str | None = None
    iteration_count = 0
    list_start_time = time.time()

    while True:
        iteration_count += 1
        if iteration_count > max_iterations:
            if logger:
                logger.warning(
                    "list_all_objects: reached max iterations (%s). Returning partial results.",
                    max_iterations,
                )
            aggregated.is_truncated = True
            aggregated.next_continuation_token = continuation_token
            break

        # Log progress every 10 pages or on first page
        if logger and (iteration_count == 1 or iteration_count % 10 == 0):
            elapsed = time.time() - list_start_time
            objects_per_sec = len(aggregated.objects) / elapsed if elapsed > 0 else 0
            token_info = f", token={continuation_token[:20]}..." if continuation_token else ""
            logger.info(
                f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}]   LIST pagination: "
                f"page {iteration_count}, {len(aggregated.objects)} objects so far "
                f"({objects_per_sec:.0f} obj/s, {elapsed:.1f}s elapsed{token_info})"
            )

            # Warn if taking very long (>60s)
            if elapsed > 60 and iteration_count % 50 == 0:
                estimated_total = (len(aggregated.objects) / iteration_count) * max_iterations
                logger.warning(
                    f"LIST operation is slow ({elapsed:.0f}s elapsed). "
                    f"This bucket has MANY objects ({len(aggregated.objects)} so far). "
                    f"Consider using a smaller prefix or enabling caching. "
                    f"Estimated remaining: {estimated_total - len(aggregated.objects):.0f} objects"
                )

        try:
            page = list_objects_page(
                storage,
                bucket=bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                continuation_token=continuation_token,
            )
        except Exception as exc:
            if not aggregated.objects:
                raise RuntimeError(f"Failed to list objects for bucket '{bucket}': {exc}") from exc
            if logger:
                logger.warning(
                    "list_all_objects: pagination error after %s objects: %s. Returning partial results.",
                    len(aggregated.objects),
                    exc,
                )
            aggregated.is_truncated = True
            aggregated.next_continuation_token = continuation_token
            break

        aggregated.objects.extend(page.objects)
        aggregated.common_prefixes.extend(page.common_prefixes)
        aggregated.key_count += page.key_count

        if not page.is_truncated:
            aggregated.is_truncated = False
            aggregated.next_continuation_token = None
            if logger:
                elapsed = time.time() - list_start_time
                logger.info(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S.%f')[:-3]}]   LIST complete: "
                    f"{iteration_count} pages, {len(aggregated.objects)} objects total in {elapsed:.2f}s"
                )
            break

        continuation_token = page.next_continuation_token
        if not continuation_token:
            if logger:
                logger.warning(
                    "list_all_objects: truncated response without continuation token after %s objects.",
                    len(aggregated.objects),
                )
            aggregated.is_truncated = True
            aggregated.next_continuation_token = None
            break

    if aggregated.common_prefixes:
        seen: set[str] = set()
        unique_prefixes: list[str] = []
        for prefix in aggregated.common_prefixes:
            if prefix not in seen:
                seen.add(prefix)
                unique_prefixes.append(prefix)
        aggregated.common_prefixes = unique_prefixes
    aggregated.key_count = len(aggregated.objects)
    return aggregated


def _parse_last_modified(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif value:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = datetime.fromtimestamp(0, tz=timezone.utc)  # noqa: UP017
    else:
        dt = datetime.fromtimestamp(0, tz=timezone.utc)  # noqa: UP017

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # noqa: UP017
    return dt


def object_dict_to_head(obj: dict[str, Any]) -> ObjectHead:
    """Convert a list_objects entry into ObjectHead for compatibility uses."""
    metadata = obj.get("metadata")
    if metadata is None or not isinstance(metadata, dict):
        metadata = {}

    return ObjectHead(
        key=obj["key"],
        size=int(obj.get("size", 0)),
        etag=str(obj.get("etag", "")),
        last_modified=_parse_last_modified(obj.get("last_modified")),
        metadata=metadata,
    )


__all__ = [
    "ObjectListing",
    "list_objects_page",
    "list_all_objects",
    "object_dict_to_head",
]
