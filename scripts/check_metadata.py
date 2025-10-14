#!/usr/bin/env python3
"""Check which delta files are missing metadata."""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deltaglider import create_client


def check_bucket_metadata(bucket: str) -> None:
    """Check all delta files in a bucket for missing metadata.

    Args:
        bucket: S3 bucket name
    """
    client = create_client()

    print(f"Checking delta files in bucket: {bucket}\n")
    print("=" * 80)

    # List all objects
    response = client.service.storage.list_objects(bucket=bucket, max_keys=10000)

    missing_metadata = []
    has_metadata = []
    total_delta_files = 0

    for obj in response["objects"]:
        key = obj["key"]

        # Only check .delta files
        if not key.endswith(".delta"):
            continue

        total_delta_files += 1

        # Get metadata
        obj_head = client.service.storage.head(f"{bucket}/{key}")

        if not obj_head:
            print(f"❌ {key}: Object not found")
            continue

        metadata = obj_head.metadata

        # Check for required metadata fields
        required_fields = ["file_size", "file_sha256", "ref_key", "ref_sha256", "delta_size"]
        missing_fields = [f for f in required_fields if f not in metadata]

        if missing_fields:
            missing_metadata.append({
                "key": key,
                "missing_fields": missing_fields,
                "has_metadata": bool(metadata),
                "available_keys": list(metadata.keys()) if metadata else [],
            })
            status = "⚠️  MISSING"
            detail = f"missing: {', '.join(missing_fields)}"
        else:
            has_metadata.append(key)
            status = "✅ OK"
            detail = f"file_size={metadata.get('file_size')}"

        print(f"{status} {key}")
        print(f"    {detail}")
        if metadata:
            print(f"    Available keys: {', '.join(metadata.keys())}")
        print()

    # Summary
    print("=" * 80)
    print(f"\nSummary:")
    print(f"  Total delta files: {total_delta_files}")
    print(f"  With complete metadata: {len(has_metadata)} ({len(has_metadata)/total_delta_files*100:.1f}%)")
    print(f"  Missing metadata: {len(missing_metadata)} ({len(missing_metadata)/total_delta_files*100:.1f}%)")

    if missing_metadata:
        print(f"\n❌ Files with missing metadata:")
        for item in missing_metadata:
            print(f"  - {item['key']}")
            print(f"    Missing: {', '.join(item['missing_fields'])}")
            if item['available_keys']:
                print(f"    Has: {', '.join(item['available_keys'])}")

        print(f"\n💡 Recommendation:")
        print(f"  These files should be re-uploaded to get proper metadata and accurate stats.")
        print(f"  You can re-upload with: deltaglider cp <local-file> s3://{bucket}/<path>")
    else:
        print(f"\n✅ All delta files have complete metadata!")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_metadata.py <bucket-name>")
        sys.exit(1)

    bucket_name = sys.argv[1]
    check_bucket_metadata(bucket_name)
