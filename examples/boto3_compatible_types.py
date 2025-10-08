"""Example: Using boto3-compatible responses without importing boto3.

This demonstrates how DeltaGlider provides full type safety and boto3 compatibility
without requiring boto3 imports in user code.

As of v5.0.0, DeltaGlider returns plain dicts (not custom dataclasses) that are
100% compatible with boto3 S3 responses. You get IDE autocomplete through TypedDict
type hints without any runtime overhead.
"""

from deltaglider import ListObjectsV2Response, S3Object, create_client

# Create client (no boto3 import needed!)
client = create_client()

# Type hints work perfectly without boto3
def process_files(bucket: str, prefix: str) -> None:
    """Process files in S3 with full type safety."""
    # Return type is fully typed - IDE autocomplete works!
    response: ListObjectsV2Response = client.list_objects(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    )

    # Response is a plain dict - 100% boto3-compatible
    # TypedDict provides autocomplete and type checking
    for obj in response["Contents"]:
        # obj is typed as S3Object - all fields have autocomplete!
        key: str = obj["Key"]  # ✅ IDE knows this is str
        size: int = obj["Size"]  # ✅ IDE knows this is int
        print(f"{key}: {size} bytes")

        # DeltaGlider metadata is in the standard Metadata field
        metadata = obj.get("Metadata", {})
        if metadata.get("deltaglider-is-delta") == "true":
            compression = metadata.get("deltaglider-compression-ratio", "unknown")
            print(f"  └─ Delta file (compression: {compression})")

    # Optional fields work too
    for prefix_dict in response.get("CommonPrefixes", []):
        print(f"Directory: {prefix_dict['Prefix']}")

    # Pagination info
    if response.get("IsTruncated"):
        next_token = response.get("NextContinuationToken")
        print(f"More results available, token: {next_token}")


# This is 100% compatible with boto3 code!
def works_with_boto3_or_deltaglider(s3_client) -> None:
    """This function works with EITHER boto3 or DeltaGlider client."""
    # Because the response structure is identical!
    response = s3_client.list_objects(Bucket="my-bucket")

    for obj in response["Contents"]:
        print(obj["Key"])


if __name__ == "__main__":
    # Example usage
    print("✅ Full type safety without boto3 imports!")
    print("✅ 100% compatible with boto3")
    print("✅ Drop-in replacement")
    print("✅ Plain dict responses (not custom dataclasses)")
    print("✅ DeltaGlider metadata in standard Metadata field")
