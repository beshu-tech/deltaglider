"""Bucket management operations for DeltaGlider client.

This module contains boto3-compatible bucket operations:
- create_bucket
- delete_bucket
- list_buckets
"""

from typing import Any


def create_bucket(
    client: Any,  # DeltaGliderClient (avoiding circular import)
    Bucket: str,
    CreateBucketConfiguration: dict[str, str] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Create an S3 bucket (boto3-compatible).

    Args:
        client: DeltaGliderClient instance
        Bucket: Bucket name to create
        CreateBucketConfiguration: Optional bucket configuration (e.g., LocationConstraint)
        **kwargs: Additional S3 parameters (for compatibility)

    Returns:
        Response dict with bucket location

    Example:
        >>> client = create_client()
        >>> client.create_bucket(Bucket='my-bucket')
        >>> # With region
        >>> client.create_bucket(
        ...     Bucket='my-bucket',
        ...     CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        ... )
    """
    storage_adapter = client.service.storage

    # Check if storage adapter has boto3 client
    if hasattr(storage_adapter, "client"):
        try:
            params: dict[str, Any] = {"Bucket": Bucket}
            if CreateBucketConfiguration:
                params["CreateBucketConfiguration"] = CreateBucketConfiguration

            response = storage_adapter.client.create_bucket(**params)
            return {
                "Location": response.get("Location", f"/{Bucket}"),
                "ResponseMetadata": {
                    "HTTPStatusCode": 200,
                },
            }
        except Exception as e:
            error_msg = str(e)
            if "BucketAlreadyExists" in error_msg or "BucketAlreadyOwnedByYou" in error_msg:
                # Bucket already exists - return success
                client.service.logger.debug(f"Bucket {Bucket} already exists")
                return {
                    "Location": f"/{Bucket}",
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200,
                    },
                }
            raise RuntimeError(f"Failed to create bucket: {e}") from e
    else:
        raise NotImplementedError("Storage adapter does not support bucket creation")


def delete_bucket(
    client: Any,  # DeltaGliderClient
    Bucket: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Delete an S3 bucket (boto3-compatible).

    Note: Bucket must be empty before deletion.

    Args:
        client: DeltaGliderClient instance
        Bucket: Bucket name to delete
        **kwargs: Additional S3 parameters (for compatibility)

    Returns:
        Response dict with deletion status

    Example:
        >>> client = create_client()
        >>> client.delete_bucket(Bucket='my-bucket')
    """
    storage_adapter = client.service.storage

    # Check if storage adapter has boto3 client
    if hasattr(storage_adapter, "client"):
        try:
            storage_adapter.client.delete_bucket(Bucket=Bucket)
            return {
                "ResponseMetadata": {
                    "HTTPStatusCode": 204,
                },
            }
        except Exception as e:
            error_msg = str(e)
            if "NoSuchBucket" in error_msg:
                # Bucket doesn't exist - return success
                client.service.logger.debug(f"Bucket {Bucket} does not exist")
                return {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 204,
                    },
                }
            raise RuntimeError(f"Failed to delete bucket: {e}") from e
    else:
        raise NotImplementedError("Storage adapter does not support bucket deletion")


def list_buckets(
    client: Any,  # DeltaGliderClient
    **kwargs: Any,
) -> dict[str, Any]:
    """List all S3 buckets (boto3-compatible).

    Args:
        client: DeltaGliderClient instance
        **kwargs: Additional S3 parameters (for compatibility)

    Returns:
        Response dict with bucket list

    Example:
        >>> client = create_client()
        >>> response = client.list_buckets()
        >>> for bucket in response['Buckets']:
        ...     print(bucket['Name'])
    """
    storage_adapter = client.service.storage

    # Check if storage adapter has boto3 client
    if hasattr(storage_adapter, "client"):
        try:
            raw_response = storage_adapter.client.list_buckets()

            buckets: list[dict[str, Any]] = []
            for bucket_entry in raw_response.get("Buckets", []):
                bucket_data = dict(bucket_entry)
                name = bucket_data.get("Name")
                if isinstance(name, str) and name:
                    cached_stats, cached_mode = client._get_cached_bucket_stats_for_listing(name)
                    if cached_stats is not None and cached_mode is not None:
                        bucket_data["DeltaGliderStats"] = {
                            "Cached": True,
                            "Mode": cached_mode,
                            "Detailed": cached_mode == "detailed",
                            "ObjectCount": cached_stats.object_count,
                            "TotalSize": cached_stats.total_size,
                            "CompressedSize": cached_stats.compressed_size,
                            "SpaceSaved": cached_stats.space_saved,
                            "AverageCompressionRatio": cached_stats.average_compression_ratio,
                            "DeltaObjects": cached_stats.delta_objects,
                            "DirectObjects": cached_stats.direct_objects,
                        }

                buckets.append(bucket_data)

            return {
                "Buckets": buckets,
                "Owner": raw_response.get("Owner", {}),
                "ResponseMetadata": {
                    "HTTPStatusCode": 200,
                },
            }
        except Exception as e:
            raise RuntimeError(f"Failed to list buckets: {e}") from e
    else:
        raise NotImplementedError("Storage adapter does not support bucket listing")
