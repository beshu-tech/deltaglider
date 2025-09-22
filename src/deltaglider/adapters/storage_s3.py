"""S3 storage adapter."""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Optional

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

from ..ports.storage import ObjectHead, PutResult, StoragePort


class S3StorageAdapter(StoragePort):
    """S3 implementation of StoragePort."""

    def __init__(
        self,
        client: Optional["S3Client"] = None,
        endpoint_url: str | None = None,
    ):
        """Initialize with S3 client."""
        if client is None:
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint_url or os.environ.get("AWS_ENDPOINT_URL"),
            )
        else:
            self.client = client

    def head(self, key: str) -> ObjectHead | None:
        """Get object metadata."""
        bucket, object_key = self._parse_key(key)

        try:
            response = self.client.head_object(Bucket=bucket, Key=object_key)
            return ObjectHead(
                key=object_key,
                size=response["ContentLength"],
                etag=response["ETag"].strip('"'),
                last_modified=response["LastModified"],
                metadata=self._extract_metadata(response.get("Metadata", {})),
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            raise

    def list(self, prefix: str) -> Iterator[ObjectHead]:
        """List objects by prefix."""
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

    def get(self, key: str) -> BinaryIO:
        """Get object content as stream."""
        bucket, object_key = self._parse_key(key)

        try:
            response = self.client.get_object(Bucket=bucket, Key=object_key)
            return response["Body"]  # type: ignore[return-value]
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

        try:
            response = self.client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=body_data,
                ContentType=content_type,
                Metadata=clean_metadata,
            )
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
