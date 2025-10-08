# boto3 Compatibility Vision

## Current State (v4.2.3)

DeltaGlider currently uses custom dataclasses for responses:

```python
from deltaglider import create_client, ListObjectsResponse, ObjectInfo

client = create_client()
response: ListObjectsResponse = client.list_objects(Bucket='my-bucket')

for obj in response.contents:  # Custom field name
    print(f"{obj.key}: {obj.size}")  # Custom ObjectInfo dataclass
```

**Problems:**
- ❌ Not a true drop-in replacement for boto3
- ❌ Users need to learn DeltaGlider-specific types
- ❌ Can't use with tools expecting boto3 responses
- ❌ Different API surface (`.contents` vs `['Contents']`)

## Target State (v5.0.0)

DeltaGlider should return native boto3-compatible dicts with TypedDict type hints:

```python
from deltaglider import create_client, ListObjectsV2Response

client = create_client()
response: ListObjectsV2Response = client.list_objects(Bucket='my-bucket')

for obj in response['Contents']:  # boto3-compatible!
    print(f"{obj['Key']}: {obj['Size']}")  # Works exactly like boto3
```

**Benefits:**
- ✅ **True drop-in replacement** - swap `boto3.client('s3')` with `create_client()`
- ✅ **No learning curve** - if you know boto3, you know DeltaGlider
- ✅ **Tool compatibility** - works with any library expecting boto3 types
- ✅ **Type safety** - TypedDict provides IDE autocomplete without boto3 import
- ✅ **Zero runtime overhead** - TypedDict compiles to plain dict

## Implementation Plan

### Phase 1: Type Definitions ✅ (DONE)

Created `deltaglider/types.py` with comprehensive TypedDict definitions:

```python
from typing import TypedDict, NotRequired
from datetime import datetime

class S3Object(TypedDict):
    Key: str
    Size: int
    LastModified: datetime
    ETag: NotRequired[str]
    StorageClass: NotRequired[str]

class ListObjectsV2Response(TypedDict):
    Contents: list[S3Object]
    CommonPrefixes: NotRequired[list[dict[str, str]]]
    IsTruncated: NotRequired[bool]
    NextContinuationToken: NotRequired[str]
```

**Key insight:** TypedDict provides type safety at development time but compiles to plain `dict` at runtime!

### Phase 2: Refactor Client Methods (TODO)

Update all client methods to return boto3-compatible dicts:

#### `list_objects()`

**Before:**
```python
def list_objects(...) -> ListObjectsResponse:  # Custom dataclass
    return ListObjectsResponse(
        name=bucket,
        contents=[ObjectInfo(...), ...]  # Custom dataclass
    )
```

**After:**
```python
def list_objects(...) -> ListObjectsV2Response:  # TypedDict
    return {
        'Contents': [
            {
                'Key': 'file.zip',  # .delta suffix already stripped
                'Size': 1024,
                'LastModified': datetime(...),
                'ETag': '"abc123"',
            }
        ],
        'CommonPrefixes': [{'Prefix': 'dir/'}],
        'IsTruncated': False,
    }
```

**Key changes:**
1. Return plain dict instead of custom dataclass
2. Use boto3 field names: `Contents` not `contents`, `Key` not `key`
3. Strip `.delta` suffix transparently (already done)
4. Hide `reference.bin` files (already done)

#### `put_object()`

**Before:**
```python
def put_object(...) -> dict[str, Any]:
    return {
        "ETag": etag,
        "VersionId": None,
        "DeltaGliderInfo": {...}  # Custom field
    }
```

**After:**
```python
def put_object(...) -> PutObjectResponse:  # TypedDict
    return {
        'ETag': etag,
        'ResponseMetadata': {'HTTPStatusCode': 200},
        # DeltaGlider metadata goes in Metadata field
        'Metadata': {
            'deltaglider-is-delta': 'true',
            'deltaglider-compression-ratio': '0.99'
        }
    }
```

#### `get_object()`

**Before:**
```python
def get_object(...) -> dict[str, Any]:
    return {
        "Body": data,
        "ContentLength": len(data),
        "DeltaGliderInfo": {...}  # Custom field
    }
```

**After:**
```python
def get_object(...) -> GetObjectResponse:  # TypedDict
    return {
        'Body': data,  # bytes, not StreamingBody (simpler!)
        'ContentLength': len(data),
        'LastModified': datetime(...),
        'ETag': '"abc123"',
        'Metadata': {  # DeltaGlider metadata here
            'deltaglider-is-delta': 'true'
        }
    }
```

#### `delete_object()`, `delete_objects()`, `head_object()`, etc.

All follow the same pattern: return boto3-compatible dicts with TypedDict hints.

### Phase 3: Backward Compatibility (TODO)

Keep old dataclasses for 1-2 versions with deprecation warnings:

```python
class ListObjectsResponse:
    """DEPRECATED: Use dict responses with ListObjectsV2Response type hint.

    This will be removed in v6.0.0. Update your code:

    Before:
        response.contents[0].key

    After:
        response['Contents'][0]['Key']
    """
    def __init__(self, data: dict):
        warnings.warn(
            "ListObjectsResponse dataclass is deprecated. "
            "Use dict responses with ListObjectsV2Response type hint.",
            DeprecationWarning,
            stacklevel=2
        )
        self._data = data

    @property
    def contents(self):
        return [ObjectInfo(obj) for obj in self._data.get('Contents', [])]
```

### Phase 4: Update Documentation (TODO)

1. Update all examples to use dict responses
2. Add migration guide from v4.x to v5.0
3. Update BOTO3_COMPATIBILITY.md
4. Add "Drop-in Replacement" marketing language

### Phase 5: Update Tests (TODO)

Convert all tests from:
```python
assert response.contents[0].key == "file.zip"
```

To:
```python
assert response['Contents'][0]['Key'] == "file.zip"
```

## Migration Guide (for users)

### v4.x → v5.0

**Old code (v4.x):**
```python
from deltaglider import create_client

client = create_client()
response = client.list_objects(Bucket='my-bucket')

for obj in response.contents:  # Dataclass attribute
    print(f"{obj.key}: {obj.size}")  # Dataclass attributes
```

**New code (v5.0):**
```python
from deltaglider import create_client, ListObjectsV2Response

client = create_client()
response: ListObjectsV2Response = client.list_objects(Bucket='my-bucket')

for obj in response['Contents']:  # Dict key (boto3-compatible)
    print(f"{obj['Key']}: {obj['Size']}")  # Dict keys (boto3-compatible)
```

**Or even simpler - no type hint needed:**
```python
client = create_client()
response = client.list_objects(Bucket='my-bucket')

for obj in response['Contents']:
    print(f"{obj['Key']}: {obj['Size']}")
```

## Benefits Summary

### For Users
- **Zero learning curve** - if you know boto3, you're done
- **Drop-in replacement** - literally change one line (client creation)
- **Type safety** - TypedDict provides autocomplete without boto3 dependency
- **Tool compatibility** - works with all boto3-compatible libraries

### For DeltaGlider
- **Simpler codebase** - no custom dataclasses to maintain
- **Better marketing** - true "drop-in replacement" claim
- **Easier testing** - test against boto3 behavior directly
- **Future-proof** - if boto3 adds fields, users can access them immediately

## Technical Details

### How TypedDict Works

```python
from typing import TypedDict

class MyResponse(TypedDict):
    Key: str
    Size: int

# At runtime, this is just a dict!
response: MyResponse = {'Key': 'file.zip', 'Size': 1024}
print(type(response))  # <class 'dict'>

# But mypy and IDEs understand the structure
response['Key']  # ✅ Autocomplete works!
response['Nonexistent']  # ❌ Mypy error: Key 'Nonexistent' not found
```

### DeltaGlider-Specific Metadata

Store in standard boto3 `Metadata` field:

```python
{
    'Key': 'file.zip',
    'Size': 1024,
    'Metadata': {
        # DeltaGlider-specific fields (prefixed for safety)
        'deltaglider-is-delta': 'true',
        'deltaglider-compression-ratio': '0.99',
        'deltaglider-original-size': '100000',
        'deltaglider-reference-key': 'releases/v1.0.0/reference.bin',
    }
}
```

This is:
- ✅ boto3-compatible (Metadata is a standard field)
- ✅ Namespaced (deltaglider- prefix prevents conflicts)
- ✅ Optional (tools can ignore it)
- ✅ Type-safe (Metadata: NotRequired[dict[str, str]])

## Status

- ✅ **Phase 1:** TypedDict definitions created
- ✅ **Phase 2:** `list_objects()` refactored to return boto3-compatible dict
- ⏳ **Phase 3:** Refactor remaining methods (`put_object`, `get_object`, etc.) (TODO)
- ⏳ **Phase 4:** Backward compatibility with deprecation warnings (TODO)
- ⏳ **Phase 5:** Documentation updates (TODO)
- ⏳ **Phase 6:** Full test coverage updates (PARTIAL - list_objects tests done)

**Current:** v4.2.3+ (Phase 2 complete - `list_objects()` boto3-compatible)
**Target:** v5.0.0 release (all phases complete)
