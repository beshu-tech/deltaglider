#!/usr/bin/env python3
"""Example: Bucket management without boto3.

This example shows how to use DeltaGlider's bucket management APIs
to create, list, and delete buckets without needing boto3 directly.
"""

from deltaglider import create_client

# Create client (works with AWS S3, MinIO, or any S3-compatible storage)
client = create_client()

# For local MinIO/S3-compatible storage:
# client = create_client(endpoint_url='http://localhost:9000')

print("=" * 70)
print("DeltaGlider Bucket Management Example")
print("=" * 70)

# 1. List existing buckets
print("\n1. List all buckets:")
try:
    response = client.list_buckets()
    if response["Buckets"]:
        for bucket in response["Buckets"]:
            print(f"  - {bucket['Name']} (created: {bucket.get('CreationDate', 'unknown')})")
    else:
        print("  No buckets found")
except Exception as e:
    print(f"  Error: {e}")

# 2. Create a new bucket
bucket_name = "my-deltaglider-bucket"
print(f"\n2. Create bucket '{bucket_name}':")
try:
    response = client.create_bucket(Bucket=bucket_name)
    print(f"  ✅ Created: {response['Location']}")
except Exception as e:
    print(f"  Error: {e}")

# 3. Create bucket with region (if using AWS)
# Uncomment for AWS S3:
# print("\n3. Create bucket in specific region:")
# try:
#     response = client.create_bucket(
#         Bucket='my-regional-bucket',
#         CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
#     )
#     print(f"  ✅ Created: {response['Location']}")
# except Exception as e:
#     print(f"  Error: {e}")

# 4. Upload some files to the bucket
print(f"\n4. Upload files to '{bucket_name}':")
try:
    # Upload a simple file
    client.put_object(
        Bucket=bucket_name,
        Key="test-file.txt",
        Body=b"Hello from DeltaGlider!",
    )
    print("  ✅ Uploaded: test-file.txt")
except Exception as e:
    print(f"  Error: {e}")

# 5. List objects in the bucket
print(f"\n5. List objects in '{bucket_name}':")
try:
    response = client.list_objects(Bucket=bucket_name)
    if response.contents:
        for obj in response.contents:
            print(f"  - {obj.key} ({obj.size} bytes)")
    else:
        print("  No objects found")
except Exception as e:
    print(f"  Error: {e}")

# 6. Delete all objects in the bucket (required before deleting bucket)
print(f"\n6. Delete all objects in '{bucket_name}':")
try:
    response = client.list_objects(Bucket=bucket_name)
    for obj in response.contents:
        client.delete_object(Bucket=bucket_name, Key=obj.key)
        print(f"  ✅ Deleted: {obj.key}")
except Exception as e:
    print(f"  Error: {e}")

# 7. Delete the bucket
print(f"\n7. Delete bucket '{bucket_name}':")
try:
    response = client.delete_bucket(Bucket=bucket_name)
    print(f"  ✅ Deleted bucket (status: {response['ResponseMetadata']['HTTPStatusCode']})")
except Exception as e:
    print(f"  Error: {e}")

# 8. Verify bucket is deleted
print("\n8. Verify bucket deletion:")
try:
    response = client.list_buckets()
    bucket_names = [b["Name"] for b in response["Buckets"]]
    if bucket_name in bucket_names:
        print(f"  ❌ Bucket still exists!")
    else:
        print(f"  ✅ Bucket successfully deleted")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 70)
print("✅ Bucket management complete - no boto3 required!")
print("=" * 70)

print("\n📚 Key Benefits:")
print("  - No need to import boto3 directly")
print("  - Consistent API with other DeltaGlider operations")
print("  - Works with AWS S3, MinIO, and S3-compatible storage")
print("  - Idempotent operations (safe to retry)")
