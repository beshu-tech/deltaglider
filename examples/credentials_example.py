"""Example: Using explicit AWS credentials with DeltaGlider.

This example demonstrates how to pass AWS credentials directly to
DeltaGlider's create_client() function, which is useful when:

1. You need to use different credentials than your environment default
2. You're working with temporary credentials (session tokens)
3. You want to avoid relying on environment variables
4. You're implementing multi-tenant systems with different AWS accounts
"""

from deltaglider import create_client


def example_basic_credentials():
    """Use basic AWS credentials (access key + secret key)."""
    client = create_client(
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region_name="us-west-2",
    )

    # Now use the client normally
    # client.put_object(Bucket="my-bucket", Key="file.zip", Body=b"data")
    print("✓ Created client with explicit credentials")


def example_temporary_credentials():
    """Use temporary AWS credentials (with session token)."""
    client = create_client(
        aws_access_key_id="ASIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        aws_session_token="FwoGZXIvYXdzEBEaDH...",  # From STS
        region_name="us-east-1",
    )

    print("✓ Created client with temporary credentials")


def example_environment_credentials():
    """Use default credential chain (environment variables, IAM role, etc.)."""
    # When credentials are omitted, DeltaGlider uses boto3's default credential chain:
    # 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    # 2. AWS credentials file (~/.aws/credentials)
    # 3. IAM role (for EC2 instances)
    client = create_client()

    print("✓ Created client with default credential chain")


def example_minio_credentials():
    """Use credentials for MinIO or other S3-compatible services."""
    client = create_client(
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    print("✓ Created client for MinIO with custom credentials")


def example_multi_tenant():
    """Example: Different credentials for different tenants."""

    # Tenant A uses one AWS account
    tenant_a_client = create_client(
        aws_access_key_id="TENANT_A_KEY",
        aws_secret_access_key="TENANT_A_SECRET",
        region_name="us-west-2",
    )

    # Tenant B uses a different AWS account
    tenant_b_client = create_client(
        aws_access_key_id="TENANT_B_KEY",
        aws_secret_access_key="TENANT_B_SECRET",
        region_name="eu-west-1",
    )

    print("✓ Created separate clients for multi-tenant scenario")


if __name__ == "__main__":
    print("DeltaGlider Credentials Examples\n" + "=" * 40)

    print("\n1. Basic credentials:")
    example_basic_credentials()

    print("\n2. Temporary credentials:")
    example_temporary_credentials()

    print("\n3. Environment credentials:")
    example_environment_credentials()

    print("\n4. MinIO credentials:")
    example_minio_credentials()

    print("\n5. Multi-tenant scenario:")
    example_multi_tenant()

    print("\n" + "=" * 40)
    print("All examples completed successfully!")
