"""S3 storage adapter."""

import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Optional

import boto3
from botocore.exceptions import ClientError

from ..ports.storage import ObjectHead, PutResult, StoragePort

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


class S3StorageAdapter(StoragePort):
    """S3 implementation of StoragePort."""

    def __init__(
        self,
        client: Optional["S3Client"] = None,
        endpoint_url: str | None = None,
        boto3_kwargs: dict[str, Any] | None = None,
    ):
        """Initialize with S3 client.

        Args:
            client: Pre-configured S3 client (if None, one will be created)
            endpoint_url: S3 endpoint URL override (for MinIO, LocalStack, etc.)
            boto3_kwargs: Additional kwargs to pass to boto3.client() including:
                - aws_access_key_id: AWS access key
                - aws_secret_access_key: AWS secret key
                - aws_session_token: AWS session token (for temporary credentials)
                - region_name: AWS region name
        """
        if client is None:
            # Build boto3 client parameters
            client_params: dict[str, Any] = {
                "service_name": "s3",
                "endpoint_url": endpoint_url or os.environ.get("AWS_ENDPOINT_URL"),
            }

            # Merge in any additional boto3 kwargs (credentials, region, etc.)
            if boto3_kwargs:
                client_params.update(boto3_kwargs)

            self.client = boto3.client(**client_params)
        else:
            self.client = client

    def head(self, key: str) -> ObjectHead | None:
        """Get object metadata."""
        bucket, object_key = self._parse_key(key)

        try:
            response = self.client.head_object(Bucket=bucket, Key=object_key)
            extracted_metadata = self._extract_metadata(response.get("Metadata", {}))

            # Debug: Log metadata received (to verify it's stored correctly)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"HEAD {object_key}: Received metadata with {len(extracted_metadata)} keys: "
                    f"{list(extracted_metadata.keys())}"
                )

            return ObjectHead(
                key=object_key,
                size=response["ContentLength"],
                etag=response["ETag"].strip('"'),
                last_modified=response["LastModified"],
                metadata=extracted_metadata,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            raise

    def list(self, prefix: str) -> Iterator[ObjectHead]:
        """List objects by prefix (implements StoragePort interface).

        This is a simple iterator for core service compatibility.
        For advanced S3 features, use list_objects instead.
        """
        # Handle bucket-only prefix (e.g., "bucket" or "bucket/")
        if "/" not in prefix:
            bucket = prefix
            prefix_key = ""
        else:
            bucket, prefix_key = self._parse_key(prefix)

        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix_key)

        for page in pages:
            for obj in page.get("Contents", []):
                # Get full metadata
                head = self.head(f"{bucket}/{obj['Key']}")
                if head:
                    yield head

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        start_after: str | None = None,
        continuation_token: str | None = None,
    ) -> dict[str, Any]:
        """List objects with S3-compatible response.

        Args:
            bucket: S3 bucket name
            prefix: Filter results to keys beginning with prefix
            delimiter: Delimiter for grouping keys (e.g., '/' for folders)
            max_keys: Maximum number of keys to return
            start_after: Start listing after this key (for first page only)
            continuation_token: Token from previous response for pagination

        Returns:
            Dict with objects, common_prefixes, and pagination info
        """
        params: dict[str, Any] = {
            "Bucket": bucket,
            "MaxKeys": max_keys,
        }

        if prefix:
            params["Prefix"] = prefix
        if delimiter:
            params["Delimiter"] = delimiter

        # Use ContinuationToken for pagination if available, otherwise StartAfter
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        elif start_after:
            params["StartAfter"] = start_after

        try:
            response = self.client.list_objects_v2(**params)

            # Process objects
            objects = []
            for obj in response.get("Contents", []):
                objects.append(
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat()
                        if hasattr(obj["LastModified"], "isoformat")
                        else str(obj["LastModified"]),
                        "etag": obj.get("ETag", "").strip('"'),
                        "storage_class": obj.get("StorageClass", "STANDARD"),
                    }
                )

            # Process common prefixes (folders)
            common_prefixes = []
            for prefix_info in response.get("CommonPrefixes", []):
                common_prefixes.append(prefix_info["Prefix"])

            return {
                "objects": objects,
                "common_prefixes": common_prefixes,
                "is_truncated": response.get("IsTruncated", False),
                "next_continuation_token": response.get("NextContinuationToken"),
                "key_count": response.get("KeyCount", len(objects)),
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket":
                raise FileNotFoundError(f"Bucket not found: {bucket}") from e
            raise

    def get(self, key: str) -> BinaryIO:
        """Get object content as stream."""
        bucket, object_key = self._parse_key(key)

        try:
            response = self.client.get_object(Bucket=bucket, Key=object_key)
            return response["Body"]  # type: ignore[no-any-return]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {key}") from e
            raise

    def put(
        self,
        key: str,
        body: BinaryIO | bytes | Path,
        metadata: dict[str, str],
        content_type: str = "application/octet-stream",
    ) -> PutResult:
        """Put object with metadata."""
        bucket, object_key = self._parse_key(key)

        # Prepare body
        if isinstance(body, Path):
            with open(body, "rb") as f:
                body_data = f.read()
        elif isinstance(body, bytes):
            body_data = body
        else:
            body_data = body.read()

        # AWS requires lowercase metadata keys
        clean_metadata = {k.lower(): v for k, v in metadata.items()}

        # Calculate total metadata size (AWS has 2KB limit)
        total_metadata_size = sum(len(k) + len(v) for k, v in clean_metadata.items())

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"PUT {object_key}: Sending metadata with {len(clean_metadata)} keys "
                f"({total_metadata_size} bytes): {list(clean_metadata.keys())}"
            )

        # Warn if approaching AWS metadata size limit (2KB per key, 2KB total for user metadata)
        if total_metadata_size > 1800:  # Warn at 1.8KB
            logger.warning(
                f"PUT {object_key}: Metadata size ({total_metadata_size} bytes) approaching "
                f"AWS S3 limit (2KB). Some metadata may be lost!"
            )

        try:
            response = self.client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=body_data,
                ContentType=content_type,
                Metadata=clean_metadata,
            )

            # VERIFICATION: Check if metadata was actually stored (especially for delta files)
            if object_key.endswith(".delta") and clean_metadata:
                try:
                    # Verify metadata was stored by doing a HEAD immediately
                    verify_response = self.client.head_object(Bucket=bucket, Key=object_key)
                    stored_metadata = verify_response.get("Metadata", {})

                    if not stored_metadata:
                        logger.error(
                            f"PUT {object_key}: CRITICAL - Metadata was sent but NOT STORED! "
                            f"Sent {len(clean_metadata)} keys, received 0 keys back."
                        )
                    elif len(stored_metadata) < len(clean_metadata):
                        missing_keys = set(clean_metadata.keys()) - set(stored_metadata.keys())
                        logger.warning(
                            f"PUT {object_key}: Metadata partially stored. "
                            f"Sent {len(clean_metadata)} keys, stored {len(stored_metadata)} keys. "
                            f"Missing keys: {missing_keys}"
                        )
                    elif logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"PUT {object_key}: Metadata verified - all {len(clean_metadata)} keys stored"
                        )
                except Exception as e:
                    logger.warning(f"PUT {object_key}: Could not verify metadata: {e}")

            return PutResult(
                etag=response["ETag"].strip('"'),
                version_id=response.get("VersionId"),
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to put object: {e}") from e

    def delete(self, key: str) -> None:
        """Delete object."""
        bucket, object_key = self._parse_key(key)

        try:
            self.client.delete_object(Bucket=bucket, Key=object_key)
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise

    def _parse_key(self, key: str) -> tuple[str, str]:
        """Parse bucket/key from combined key."""
        parts = key.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid key format: {key}")
        return parts[0], parts[1]

    def _extract_metadata(self, raw_metadata: dict[str, str]) -> dict[str, str]:
        """Extract user metadata from S3 response."""
        # S3 returns user metadata as-is (already lowercase)
        return raw_metadata
