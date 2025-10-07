"""Helper utilities for client delete operations."""

from .core import DeltaService, ObjectKey
from .core.errors import NotFoundError


def delete_with_delta_suffix(
    service: DeltaService, bucket: str, key: str
) -> tuple[str, dict[str, object]]:
    """Delete an object, retrying with '.delta' suffix when needed.

    Args:
        service: DeltaService-like instance exposing ``delete(ObjectKey)``.
        bucket: Target bucket.
        key: Requested key (without forcing .delta suffix).

    Returns:
        Tuple containing the actual key deleted in storage and the delete result dict.

    Raises:
        NotFoundError: Propagated when both the direct and '.delta' keys are missing.
    """
    actual_key = key
    object_key = ObjectKey(bucket=bucket, key=actual_key)

    try:
        delete_result = service.delete(object_key)
    except NotFoundError:
        if key.endswith(".delta"):
            raise
        actual_key = f"{key}.delta"
        object_key = ObjectKey(bucket=bucket, key=actual_key)
        delete_result = service.delete(object_key)

    return actual_key, delete_result
