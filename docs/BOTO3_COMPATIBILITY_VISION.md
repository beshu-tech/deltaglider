# boto3 Compatibility Vision

DeltaGlider is a drop-in replacement for boto3's S3 client. This document spells out what “drop-in”
means in practice so new projects can adopt the SDK with confidence.

## Current State (v5.x and newer)

- `DeltaGliderClient` methods such as `list_objects`, `put_object`, `get_object`, `delete_object`,
  `delete_objects`, `head_object`, etc. return **boto3-compatible dicts**.
- TypedDict aliases in `deltaglider.types` (e.g. `ListObjectsV2Response`, `PutObjectResponse`) give
  IDE/type-checking support without importing boto3.
- DeltaGlider-specific metadata lives inside standard boto3 fields (typically `Metadata`), so tools
  that ignore those keys see the exact same structures as they would from boto3.
- Tests and documentation exercise and describe the boto3-style responses (`response['Contents']`
  instead of `response.contents`).

```python
from deltaglider import create_client, ListObjectsV2Response

client = create_client()
response: ListObjectsV2Response = client.list_objects(Bucket='my-bucket')

for obj in response['Contents']:
    print(f"{obj['Key']}: {obj['Size']} bytes")
```

## Key Design Points

- **TypedDict everywhere** – `put_object`, `get_object`, `list_objects`, `delete_object`, etc.
  return the same shapes boto3 does. Use the provided aliases (`ListObjectsV2Response`,
  `PutObjectResponse`, …) for IDE/completion help.
- **Metadata namespace** – DeltaGlider-specific flags such as `deltaglider-is-delta` live under the
  regular `Metadata` key so every response remains valid boto3 output.
- **No shims required** – responses are plain dicts. If you already know boto3, you already know how
  to consume DeltaGlider outputs.

## Benefits Summary

### For Users
- **Zero learning curve** – identical data structures to boto3.
- **Tooling compatibility** – works with any boto3-aware tool or library.
- **Type safety** – TypedDicts provide IDE autocomplete even without boto3 installed.

### For DeltaGlider
- **Cleaner internals** – no custom dataclasses to maintain.
- **Simpler docs/tests** – examples mirror boto3 verbatim.
- **Marketing accuracy** – "drop-in replacement" is now literal.

## Technical Details

### TypedDict refresher
```python
from typing import TypedDict

class MyResponse(TypedDict):
    Key: str
    Size: int

resp: MyResponse = {'Key': 'file.zip', 'Size': 1024}
print(type(resp))  # <class 'dict'>
```
At runtime the structure is still a plain `dict`, but static type-checkers understand the shape.

### DeltaGlider Metadata

Delta-specific fields live inside the standard `Metadata` map. Example list_objects entry:
```python
{
    'Key': 'file.zip',
    'Size': 1024,
    'Metadata': {
        'deltaglider-is-delta': 'true',
        'deltaglider-compression-ratio': '0.99',
        'deltaglider-original-size': '50000000',
    }
}
```
These keys are namespaced (`deltaglider-...`) so they are safe to ignore if not needed.

## Status Snapshot

- ✅ TypedDict builders are used everywhere (`build_list_objects_response`, etc.).
- ✅ Tests assert boto3-style dict access (`response['Contents']`).
- ✅ Documentation (README, SDK docs, examples) shows the boto3 syntax.
