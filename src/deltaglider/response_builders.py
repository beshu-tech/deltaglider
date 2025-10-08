"""Type-safe response builders using TypedDicts for internal type safety.

This module provides builder functions that construct boto3-compatible responses
with full compile-time type validation using TypedDicts. At runtime, TypedDicts
are plain dicts, so there's no conversion overhead.

Benefits:
- Field name typos caught by mypy (e.g., "HTTPStatusCode" → "HttpStatusCode")
- Wrong types caught by mypy (e.g., string instead of int)
- Missing required fields caught by mypy
- Extra unknown fields caught by mypy
"""

from typing import Any

from .types import (
    CommonPrefix,
    DeleteObjectResponse,
    GetObjectResponse,
    ListObjectsV2Response,
    PutObjectResponse,
    ResponseMetadata,
    S3Object,
)


def build_response_metadata(status_code: int = 200) -> ResponseMetadata:
    """Build ResponseMetadata with full type safety via TypedDict.

    TypedDict is a dict at runtime - no conversion needed!
    mypy validates all fields match ResponseMetadata TypedDict.
    Uses our types.py TypedDict which has proper NotRequired fields.
    """
    # Build as TypedDict - mypy validates field names and types!
    metadata: ResponseMetadata = {
        "HTTPStatusCode": status_code,
        # All other fields are NotRequired - can be omitted!
    }
    return metadata  # Returns dict at runtime, ResponseMetadata type at compile-time


def build_put_response(
    etag: str,
    *,
    version_id: str | None = None,
    deltaglider_info: dict[str, Any] | None = None,
) -> PutObjectResponse:
    """Build PutObjectResponse with full type safety via TypedDict.

    Uses our types.py TypedDict which has proper NotRequired fields.
    mypy validates all field names, types, and structure.
    """
    # Build as TypedDict - mypy catches typos and type errors!
    response: PutObjectResponse = {
        "ETag": etag,
        "ResponseMetadata": build_response_metadata(),
    }

    if version_id:
        response["VersionId"] = version_id

    # DeltaGlider extension - add as Any field
    if deltaglider_info:
        response["DeltaGliderInfo"] = deltaglider_info  # type: ignore[typeddict-item]

    return response  # Returns dict at runtime, PutObjectResponse type at compile-time


def build_get_response(
    body: Any,
    content_length: int,
    etag: str,
    metadata: dict[str, Any],
) -> GetObjectResponse:
    """Build GetObjectResponse with full type safety via TypedDict.

    Uses our types.py TypedDict which has proper NotRequired fields.
    mypy validates all field names, types, and structure.
    """
    # Build as TypedDict - mypy catches typos and type errors!
    response: GetObjectResponse = {
        "Body": body,
        "ContentLength": content_length,
        "ETag": etag,
        "Metadata": metadata,
        "ResponseMetadata": build_response_metadata(),
    }
    return response  # Returns dict at runtime, GetObjectResponse type at compile-time


def build_list_objects_response(
    bucket: str,
    prefix: str,
    delimiter: str,
    max_keys: int,
    contents: list[S3Object],
    common_prefixes: list[CommonPrefix] | None,
    is_truncated: bool,
    next_continuation_token: str | None,
    continuation_token: str | None,
) -> ListObjectsV2Response:
    """Build ListObjectsV2Response with full type safety via TypedDict.

    Uses our types.py TypedDict which has proper NotRequired fields.
    mypy validates all field names, types, and structure.
    """
    # Build as TypedDict - mypy catches typos and type errors!
    response: ListObjectsV2Response = {
        "IsTruncated": is_truncated,
        "Contents": contents,
        "Name": bucket,
        "Prefix": prefix,
        "Delimiter": delimiter,
        "MaxKeys": max_keys,
        "KeyCount": len(contents),
        "ResponseMetadata": build_response_metadata(),
    }

    # Add optional fields
    if common_prefixes:
        response["CommonPrefixes"] = common_prefixes

    if next_continuation_token:
        response["NextContinuationToken"] = next_continuation_token

    if continuation_token:
        response["ContinuationToken"] = continuation_token

    return response  # Returns dict at runtime, ListObjectsV2Response type at compile-time


def build_delete_response(
    delete_marker: bool = False,
    status_code: int = 204,
    deltaglider_info: dict[str, Any] | None = None,
) -> DeleteObjectResponse:
    """Build DeleteObjectResponse with full type safety via TypedDict.

    Uses our types.py TypedDict which has proper NotRequired fields.
    mypy validates all field names, types, and structure.
    """
    # Build as TypedDict - mypy catches typos and type errors!
    response: DeleteObjectResponse = {
        "DeleteMarker": delete_marker,
        "ResponseMetadata": build_response_metadata(status_code),
    }

    # DeltaGlider extension
    if deltaglider_info:
        response["DeltaGliderInfo"] = deltaglider_info  # type: ignore[typeddict-item]

    return response  # Returns dict at runtime, DeleteObjectResponse type at compile-time
