"""Type definitions for boto3-compatible responses.

These TypedDict definitions provide type safety and IDE autocomplete
without requiring boto3 imports. At runtime, all responses are plain dicts
that are 100% compatible with boto3.

This allows DeltaGlider to be a true drop-in replacement for boto3.s3.Client.
"""

from datetime import datetime
from typing import Any, Literal, NotRequired, TypedDict

# ============================================================================
# S3 Object Types
# ============================================================================


class S3Object(TypedDict):
    """An S3 object returned in list operations.

    Compatible with boto3's S3.Client.list_objects_v2() response Contents.
    """

    Key: str
    Size: int
    LastModified: datetime
    ETag: NotRequired[str]
    StorageClass: NotRequired[str]
    Owner: NotRequired[dict[str, str]]
    Metadata: NotRequired[dict[str, str]]


class CommonPrefix(TypedDict):
    """A common prefix (directory) in S3 listing.

    Compatible with boto3's S3.Client.list_objects_v2() response CommonPrefixes.
    """

    Prefix: str


# ============================================================================
# List Operations Response Types
# ============================================================================


class ListObjectsV2Response(TypedDict):
    """Response from list_objects_v2 operation.

    100% compatible with boto3's S3.Client.list_objects_v2() response.

    Example:
        ```python
        client = create_client()
        response: ListObjectsV2Response = client.list_objects(
            Bucket='my-bucket',
            Prefix='path/',
            Delimiter='/'
        )

        for obj in response['Contents']:
            print(f"{obj['Key']}: {obj['Size']} bytes")

        for prefix in response.get('CommonPrefixes', []):
            print(f"Directory: {prefix['Prefix']}")
        ```
    """

    Contents: list[S3Object]
    Name: NotRequired[str]  # Bucket name
    Prefix: NotRequired[str]
    Delimiter: NotRequired[str]
    MaxKeys: NotRequired[int]
    CommonPrefixes: NotRequired[list[CommonPrefix]]
    EncodingType: NotRequired[str]
    KeyCount: NotRequired[int]
    ContinuationToken: NotRequired[str]
    NextContinuationToken: NotRequired[str]
    StartAfter: NotRequired[str]
    IsTruncated: NotRequired[bool]


# ============================================================================
# Put/Get/Delete Response Types
# ============================================================================


class ResponseMetadata(TypedDict):
    """Metadata about the API response.

    Compatible with all boto3 responses.
    """

    RequestId: NotRequired[str]
    HostId: NotRequired[str]
    HTTPStatusCode: int
    HTTPHeaders: NotRequired[dict[str, str]]
    RetryAttempts: NotRequired[int]


class PutObjectResponse(TypedDict):
    """Response from put_object operation.

    Compatible with boto3's S3.Client.put_object() response.
    """

    ETag: str
    VersionId: NotRequired[str]
    ServerSideEncryption: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


class GetObjectResponse(TypedDict):
    """Response from get_object operation.

    Compatible with boto3's S3.Client.get_object() response.
    """

    Body: Any  # StreamingBody in boto3, bytes in DeltaGlider
    ContentLength: int
    ContentType: NotRequired[str]
    ETag: NotRequired[str]
    LastModified: NotRequired[datetime]
    Metadata: NotRequired[dict[str, str]]
    VersionId: NotRequired[str]
    StorageClass: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


class DeleteObjectResponse(TypedDict):
    """Response from delete_object operation.

    Compatible with boto3's S3.Client.delete_object() response.
    """

    DeleteMarker: NotRequired[bool]
    VersionId: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


class DeletedObject(TypedDict):
    """A successfully deleted object.

    Compatible with boto3's S3.Client.delete_objects() response Deleted.
    """

    Key: str
    VersionId: NotRequired[str]
    DeleteMarker: NotRequired[bool]
    DeleteMarkerVersionId: NotRequired[str]


class DeleteError(TypedDict):
    """An error that occurred during deletion.

    Compatible with boto3's S3.Client.delete_objects() response Errors.
    """

    Key: str
    Code: str
    Message: str
    VersionId: NotRequired[str]


class DeleteObjectsResponse(TypedDict):
    """Response from delete_objects operation.

    Compatible with boto3's S3.Client.delete_objects() response.
    """

    Deleted: NotRequired[list[DeletedObject]]
    Errors: NotRequired[list[DeleteError]]
    ResponseMetadata: NotRequired[ResponseMetadata]


# ============================================================================
# Head Object Response
# ============================================================================


class HeadObjectResponse(TypedDict):
    """Response from head_object operation.

    Compatible with boto3's S3.Client.head_object() response.
    """

    ContentLength: int
    ContentType: NotRequired[str]
    ETag: NotRequired[str]
    LastModified: NotRequired[datetime]
    Metadata: NotRequired[dict[str, str]]
    VersionId: NotRequired[str]
    StorageClass: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


# ============================================================================
# Bucket Operations
# ============================================================================


class Bucket(TypedDict):
    """An S3 bucket.

    Compatible with boto3's S3.Client.list_buckets() response Buckets.
    """

    Name: str
    CreationDate: datetime


class ListBucketsResponse(TypedDict):
    """Response from list_buckets operation.

    Compatible with boto3's S3.Client.list_buckets() response.
    """

    Buckets: list[Bucket]
    Owner: NotRequired[dict[str, str]]
    ResponseMetadata: NotRequired[ResponseMetadata]


class CreateBucketResponse(TypedDict):
    """Response from create_bucket operation.

    Compatible with boto3's S3.Client.create_bucket() response.
    """

    Location: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


# ============================================================================
# Multipart Upload Types
# ============================================================================


class CompletedPart(TypedDict):
    """A completed part in a multipart upload."""

    PartNumber: int
    ETag: str


class CompleteMultipartUploadResponse(TypedDict):
    """Response from complete_multipart_upload operation."""

    Location: NotRequired[str]
    Bucket: NotRequired[str]
    Key: NotRequired[str]
    ETag: NotRequired[str]
    VersionId: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


# ============================================================================
# Copy Operations
# ============================================================================


class CopyObjectResponse(TypedDict):
    """Response from copy_object operation.

    Compatible with boto3's S3.Client.copy_object() response.
    """

    CopyObjectResult: NotRequired[dict[str, Any]]
    ETag: NotRequired[str]
    LastModified: NotRequired[datetime]
    VersionId: NotRequired[str]
    ResponseMetadata: NotRequired[ResponseMetadata]


# ============================================================================
# Type Aliases for Convenience
# ============================================================================

# Common parameter types
BucketName = str
ObjectKey = str
Prefix = str
Delimiter = str

# Storage class options
StorageClass = Literal[
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "DEEP_ARCHIVE",
    "GLACIER_IR",
]
