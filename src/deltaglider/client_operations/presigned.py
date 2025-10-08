"""Presigned URL operations for DeltaGlider client.

This module contains boto3-compatible presigned URL operations:
- generate_presigned_url
- generate_presigned_post
"""

from typing import Any


def try_boto3_presigned_operation(
    client: Any,  # DeltaGliderClient
    operation: str,
    **kwargs: Any,
) -> Any | None:
    """Try to generate presigned operation using boto3 client, return None if not available."""
    storage_adapter = client.service.storage

    # Check if storage adapter has boto3 client
    if hasattr(storage_adapter, "client"):
        try:
            if operation == "url":
                return str(storage_adapter.client.generate_presigned_url(**kwargs))
            elif operation == "post":
                return dict(storage_adapter.client.generate_presigned_post(**kwargs))
        except AttributeError:
            # storage_adapter does not have a 'client' attribute
            pass
        except Exception as e:
            # Fall back to manual construction if needed
            client.service.logger.warning(f"Failed to generate presigned {operation}: {e}")

    return None


def generate_presigned_url(
    client: Any,  # DeltaGliderClient
    ClientMethod: str,
    Params: dict[str, Any],
    ExpiresIn: int = 3600,
) -> str:
    """Generate presigned URL (boto3-compatible).

    Args:
        client: DeltaGliderClient instance
        ClientMethod: Method name ('get_object' or 'put_object')
        Params: Parameters dict with Bucket and Key
        ExpiresIn: URL expiration in seconds

    Returns:
        Presigned URL string
    """
    # Try boto3 first, fallback to manual construction
    url = try_boto3_presigned_operation(
        client,
        "url",
        ClientMethod=ClientMethod,
        Params=Params,
        ExpiresIn=ExpiresIn,
    )
    if url is not None:
        return str(url)

    # Fallback: construct URL manually (less secure, for dev/testing only)
    bucket = Params.get("Bucket", "")
    key = Params.get("Key", "")

    if client.endpoint_url:
        base_url = client.endpoint_url
    else:
        base_url = f"https://{bucket}.s3.amazonaws.com"

    # Warning: This is not a real presigned URL, just a placeholder
    client.service.logger.warning("Using placeholder presigned URL - not suitable for production")
    return f"{base_url}/{key}?expires={ExpiresIn}"


def generate_presigned_post(
    client: Any,  # DeltaGliderClient
    Bucket: str,
    Key: str,
    Fields: dict[str, str] | None = None,
    Conditions: list[Any] | None = None,
    ExpiresIn: int = 3600,
) -> dict[str, Any]:
    """Generate presigned POST data for HTML forms (boto3-compatible).

    Args:
        client: DeltaGliderClient instance
        Bucket: S3 bucket name
        Key: Object key
        Fields: Additional fields to include
        Conditions: Upload conditions
        ExpiresIn: URL expiration in seconds

    Returns:
        Dict with 'url' and 'fields' for form submission
    """
    # Try boto3 first, fallback to manual construction
    response = try_boto3_presigned_operation(
        client,
        "post",
        Bucket=Bucket,
        Key=Key,
        Fields=Fields,
        Conditions=Conditions,
        ExpiresIn=ExpiresIn,
    )
    if response is not None:
        return dict(response)

    # Fallback: return minimal structure for compatibility
    if client.endpoint_url:
        url = f"{client.endpoint_url}/{Bucket}"
    else:
        url = f"https://{Bucket}.s3.amazonaws.com"

    return {
        "url": url,
        "fields": {
            "key": Key,
            **(Fields or {}),
        },
    }
