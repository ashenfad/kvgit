# Merge Functions

Merge functions resolve conflicts when two branches modify the same key. They operate on **decoded values** (not bytes) -- Staged handles encoding/decoding automatically.

## MergeFn

```python
MergeFn = Callable[[Any | None, Any, Any], Any]
# (old_value | None, our_value, their_value) -> merged_value
```

Any argument can be `None` (key absent or removed on that side).

### Registration

Register merge functions on `Staged` (or `Namespaced`):

```python
from kvit import counter

s = kvit.store()
s.set_merge_fn("hits", counter())
```

Per-commit overrides and a default fallback are also available:

```python
s.commit(merge_fns={"hits": counter()})
s.set_default_merge(last_writer_wins())
```

## Built-in Merge Functions

### `counter()`

An integer counter. Merge strategy: `ours + theirs - old`. Both sides' increments are preserved.

```python
from kvit import counter
from kvit.kv.memory import Memory

store = Memory()

import kvit
s1 = kvit.store()
s1["hits"] = 100
s1.commit()

s2 = kvit.store()
s2.set_merge_fn("hits", counter())

s1["hits"] = 115         # +15 on main
s1.commit()

s2["hits"] = 120         # +20 on s2, triggers three-way merge
s2.commit()

print(s2["hits"])        # 135 (115 + 120 - 100)
```

### `last_writer_wins()`

Merge always returns `theirs` (the HEAD value).

```python
from kvit import last_writer_wins

fn = last_writer_wins()
fn("old", "ours", "theirs")  # "theirs"
```

## Custom Merge Functions

Any callable matching the `MergeFn` signature works:

```python
def merge_lists(old, ours, theirs):
    base = set(old or [])
    return sorted(base | set(ours or []) | set(theirs or []))

s.set_merge_fn("tags", merge_lists)
```

## BytesMergeFn (advanced)

For power users working directly with `Versioned` (bytes-level API):

```python
from kvit import BytesMergeFn

BytesMergeFn = Callable[[bytes | None, bytes | None, bytes | None], bytes]
```

`Versioned.set_merge_fn()` and `Versioned.commit(merge_fns=...)` accept `BytesMergeFn`. Staged wraps user-level `MergeFn` into `BytesMergeFn` automatically at commit time.
