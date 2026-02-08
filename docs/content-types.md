# Content Types

Content types bundle encode, decode, and merge logic for typed values. They sit on top of the bytes-only `Versioned` layer -- vkv never interprets your data; you bring your own types.

## ContentType

```python
from dataclasses import dataclass
from typing import Any, Callable

@dataclass
class ContentType:
    encode: Callable[[Any], bytes]
    decode: Callable[[bytes], Any]
    merge: Callable[[Any | None, Any, Any], Any]
```

The `merge` function operates on **decoded** values: `(old_value | None, our_value, their_value) -> merged_value`.

### `as_merge_fn() -> MergeFn`

Converts the content type into a bytes-level merge function suitable for `Versioned.set_merge_fn()`. Handles encoding/decoding automatically.

### Registration

```python
from vkv import Versioned, counter

v = Versioned()
ct = counter()

# Register -- sets the merge function and stores the ContentType
v.set_content_type("hits", ct)

# Retrieve later
ct = v.get_content_type("hits")
ct.decode(v.get("hits"))
```

## Built-in Content Types

### `counter(encoding="big", byte_length=8)`

An integer counter. Values are stored as signed big-endian (default) or little-endian integers.

Merge strategy: `ours + theirs - old`. Both sides' increments are preserved.

```python
from vkv import Versioned, counter

ct = counter()

store = Memory()
v1 = Versioned(store)
v1.snapshot({"hits": ct.encode(100)})
v1.merge()

v2 = Versioned(store)
v2.set_content_type("hits", ct)

v1.snapshot({"hits": ct.encode(115)})  # +15 on main
v1.merge()
v2.snapshot({"hits": ct.encode(120)})  # +20 on v2

v2.merge()
ct.decode(v2.get("hits"))  # 135 (115 + 120 - 100)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `encoding` | `str` | `"big"` | Byte order: `"big"` or `"little"` |
| `byte_length` | `int` | `8` | Number of bytes for the integer |

### `last_writer_wins()`

Identity encode/decode (values must already be bytes). Merge always returns `theirs`.

```python
from vkv import last_writer_wins

ct = last_writer_wins()
ct.merge(b"old", b"ours", b"theirs")  # b"theirs"
```

### `json_value(merge_fn=None)`

JSON-encoded values. Defaults to last-writer-wins on decoded values. Pass a custom `merge_fn` for smarter merging.

```python
from vkv import json_value

# Default: LWW on decoded JSON
ct = json_value()
data = {"key": "value", "nested": [1, 2, 3]}
assert ct.decode(ct.encode(data)) == data

# Custom merge: union of lists
def merge_lists(old, ours, theirs):
    base = set(old or [])
    return sorted(base | set(ours or []) | set(theirs or []))

ct = json_value(merge_fn=merge_lists)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `merge_fn` | `Callable \| None` | `None` | Custom merge for decoded JSON values. Defaults to LWW. |

## Custom Content Types

Build your own by providing encode, decode, and merge functions:

```python
from vkv import ContentType

def encode_set(s: set) -> bytes:
    import json
    return json.dumps(sorted(s)).encode()

def decode_set(raw: bytes) -> set:
    import json
    return set(json.loads(raw))

def merge_sets(old, ours, theirs):
    base = old or set()
    return (base | ours | theirs) - (base - ours) - (base - theirs)

set_type = ContentType(encode=encode_set, decode=decode_set, merge=merge_sets)
v.set_content_type("tags", set_type)
```
