# Garbage Collection: GCVersioned

`GCVersioned` extends `Versioned` with automatic garbage collection via rebase. It tracks total persisted size and drops cold (least-recently-accessed) keys when a threshold is exceeded.

## Construction

```python
from kvit import GCVersioned

v = GCVersioned(high_water_bytes=10_000)

# Custom low water (default: 80% of high)
v = GCVersioned(high_water_bytes=10_000, low_water_bytes=5_000)

# With a shared store and branch
from kvit.kv.memory import Memory
store = Memory()
v = GCVersioned(store, branch="main", high_water_bytes=50_000)
```

### `GCVersioned(store=None, *, commit_hash=None, branch="main", high_water_bytes, low_water_bytes=None)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store` | `KVStore \| None` | `None` | Backend store. |
| `commit_hash` | `str \| None` | `None` | Resume from this commit. |
| `branch` | `str` | `"main"` | Branch name. |
| `high_water_bytes` | `int` | (required) | Rebase triggers when total size exceeds this. |
| `low_water_bytes` | `int \| None` | `None` | Rebase drops keys until total is at or below this. Defaults to 80% of high water. |

## How It Works

1. Every `commit()` call checks total persisted user-data size
2. If total exceeds `high_water_bytes`, a rebase is triggered
3. Rebase sorts user keys by access recency (coldest first, then largest)
4. Keys are dropped until total is at or below `low_water_bytes`
5. A fresh root commit is created with only the retained keys
6. Orphaned commits are cleaned up

System keys (prefixed with `__`) are always retained.

## Methods

### `commit(updates=None, removals=None, *, on_conflict="raise", merge_fns=None, default_merge=None, info=None) -> MergeResult`

Same as `Versioned.commit()`, but automatically runs GC afterward if above high water.

### `maybe_rebase() -> RebaseResult`

Check if total size exceeds high water. If so, run rebase. Otherwise return a no-op result.

```python
result = v.maybe_rebase()
if result.performed:
    print(f"Dropped {len(result.dropped_keys)} keys")
```

### `rebase(keep_keys=None, *, info=None) -> RebaseResult`

Force a rebase. Creates a fresh root commit with retained keys.

```python
# Use high/low water strategy
result = v.rebase()

# Explicit keep set (plus system keys)
result = v.rebase(keep_keys={"important_key", "config"})

# With commit info
result = v.rebase(info={"reason": "manual gc"})
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `keep_keys` | `set[str] \| None` | `None` | If provided, retain exactly these keys (plus system keys). Otherwise use high/low water strategy. |
| `info` | `dict \| None` | `None` | Metadata for the rebase commit. |

Raises `ConcurrencyError` if another writer advanced HEAD during the rebase.

### `clean_orphans(min_age=3600) -> int`

Remove orphaned commits unreachable from any branch HEAD. Only deletes commits older than `min_age` seconds (default: 1 hour). Returns the number of cleaned orphans.

Walks all branch histories to build the reachable set, so orphans from any branch are cleaned.

```python
cleaned = v.clean_orphans(min_age=0)  # clean all orphans regardless of age
```

## RebaseResult

Frozen dataclass returned by `rebase()` and `maybe_rebase()`.

| Field | Type | Description |
|-------|------|-------------|
| `performed` | `bool` | Whether a rebase was actually performed |
| `new_commit` | `str \| None` | New root commit hash (None if not performed) |
| `dropped_keys` | `tuple[str, ...]` | Keys that were dropped |
| `kept_keys` | `tuple[str, ...]` | Keys that were retained |
| `total_size_before` | `int` | Total user-data size before rebase |
| `total_size_after` | `int` | Total user-data size after rebase |
| `orphans_cleaned` | `int` | Number of orphaned commits deleted |

## Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `high_water` | `int` | High water threshold in bytes |
| `low_water` | `int` | Low water threshold in bytes |
| `last_rebase_result` | `RebaseResult \| None` | Result of the last rebase |

## Example

```python
from kvit import GCVersioned

v = GCVersioned(high_water_bytes=200, low_water_bytes=100)

# Write some data
v.commit({"a": b"x" * 40})
v.commit({"b": b"y" * 40})

# This commit pushes total to 120 bytes, triggers rebase
# Oldest key ("a") gets dropped
v.commit({"c": b"z" * 40})

result = v.last_rebase_result
print(result.performed)       # True
print(result.dropped_keys)    # ("a",)
print(v.get("a"))             # None (dropped)
print(v.get("c"))             # b"zzz..." (retained)
```

## Using with Staged

For the `MutableMapping[str, Any]` interface, wrap GCVersioned in Staged:

```python
from kvit import GCVersioned, Staged

v = GCVersioned(high_water_bytes=10_000)
s = Staged(v)

s["key"] = "value"
s.commit()
```

Or use the factory:

```python
import kvit

s = kvit.store(high_water_bytes=10_000)
```
