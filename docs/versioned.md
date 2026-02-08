# Core API: Versioned

`Versioned` is the central class. It provides a commit log over any `KVStore` backend, with reads, atomic commit+merge, branching, and history traversal.

Most users should use `Staged` (via `vkv.store()`) for the set/remove/commit pattern. `Versioned` is the lower-level engine that `Staged` wraps.

## Construction

```python
from vkv import Versioned

# Default: in-memory store, "main" branch, new empty commit
v = Versioned()

# Shared store, specific branch
from vkv.kv.memory import Memory
store = Memory()
v1 = Versioned(store, branch="main")
v2 = Versioned(store, branch="dev")

# Resume from a specific commit
v = Versioned(store, commit_hash="a1b2c3d4e5f67890")
```

### `Versioned(store=None, *, commit_hash=None, branch="main")`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store` | `KVStore \| None` | `None` | Backend store. Creates an in-memory store if None. |
| `commit_hash` | `str \| None` | `None` | Resume from this commit. Reads HEAD if None. |
| `branch` | `str` | `"main"` | Branch name for this instance. |

## Reading

### `get(key) -> bytes | None`

Read a value from the current commit. Returns `None` if the key doesn't exist. Updates touch metadata (used by GC to track access recency).

```python
value = v.get("my_key")
```

### `get_many(*keys) -> dict[str, bytes]`

Read multiple values. Only includes keys that exist.

```python
data = v.get_many("a", "b", "c")  # {"a": b"...", "c": b"..."} if "b" is missing
```

### `keys() -> Iterable[str]`

All keys in the current commit.

```python
for key in v.keys():
    print(key)
```

### `key in v`

Check if a key exists.

```python
if "config" in v:
    ...
```

## Writing

### `commit(updates=None, removals=None, *, on_conflict="raise", merge_fns=None, default_merge=None, info=None) -> MergeResult`

Atomically commit changes and advance HEAD. This is the primary write operation.

Values must be `bytes`. If HEAD has diverged since this instance was created, a three-way merge is performed automatically.

```python
# Add/update keys
result = v.commit({"name": b"alice", "age": b"30"})

# Remove keys
v.commit(removals={"old_key"})

# Both at once
v.commit({"new": b"value"}, removals={"old"})

# Attach metadata to the commit
v.commit({"k": b"v"}, info={"author": "alice", "message": "init"})

# Info-only commit (no data changes)
v.commit(info={"checkpoint": True})
```

#### Three-way merge behavior

When HEAD has diverged, vkv computes the LCA and diffs both sides:

- Keys changed only by us: taken
- Keys changed only by them: taken
- Both sides removed same key: removed
- Both sides made identical change: taken
- **Contested** (both changed differently): resolved by merge function, or raises `MergeConflict`

#### Per-key merge functions

```python
# Instance-level registration
v.set_merge_fn("counter", lambda old, ours, theirs: ours)

# Per-call override
v.commit({"counter": b"5"}, merge_fns={"counter": my_merge_fn})

# Default fallback for any unregistered key
v.set_default_merge(lambda old, ours, theirs: theirs)
```

Merge functions receive `(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes`. Any argument can be `None` (key absent or removed on that side).

#### `on_conflict`

| Value | Behavior |
|-------|----------|
| `"raise"` (default) | Raises `ConcurrencyError` on CAS failure, `MergeConflict` on unresolvable keys |
| `"abandon"` | Returns a falsy `MergeResult` instead of raising |

### `refresh() -> None`

Reload state from HEAD. Use this to see changes made by other writers.

```python
v.refresh()  # now v reflects HEAD
```

## Branching

These methods are also available on `Staged` (returning `Staged` instances) and on the `Store` protocol. Most users should use `Staged.create_branch()` / `Staged.checkout()` instead of calling these directly.

### `create_branch(name) -> Versioned`

Fork the current commit onto a new branch. Returns a new `Versioned` on that branch. Raises `ValueError` if the branch already exists.

```python
dev = v.create_branch("dev")
dev.commit({"feature": b"wip"})  # commits to "dev" HEAD, not "main"
```

### `checkout(commit_hash, *, branch=None) -> Versioned | None`

Create a new `Versioned` at a specific commit. Returns `None` if the commit doesn't exist. Defaults to the same branch unless `branch` is specified.

```python
old = v.checkout(some_hash)
old = v.checkout(some_hash, branch="review")
```

### `reset_to(commit_hash) -> bool`

Force HEAD to a specific commit. Returns `False` if the commit doesn't exist.

### `list_branches() -> list[str]`

List all branch names in the store.

```python
v.list_branches()  # ["dev", "main", "staging"]
```

Also available as a static method: `Versioned.branches(store)`.

## History

### `history(commit_hash=None, *, all_parents=False) -> Iterable[str]`

Walk the commit chain from newest to oldest.

```python
# Linear history (first parent only)
for commit in v.history():
    print(commit)

# Full DAG (all parents, BFS)
for commit in v.history(all_parents=True):
    print(commit)
```

### `diff(commit_a, commit_b) -> DiffResult`

Key-level differences between two commits (by comparing keysets, no blob reads).

```python
d = v.diff(old_hash, new_hash)
print(d.added)     # frozenset of added keys
print(d.removed)   # frozenset of removed keys
print(d.modified)  # frozenset of modified keys
```

### `commit_info(commit_hash=None) -> dict | None`

Retrieve the info dict attached to a commit, or `None` if none was stored.

```python
info = v.commit_info()  # current commit
info = v.commit_info(some_hash)  # specific commit
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `current_commit` | `str` | Current commit hash |
| `base_commit` | `str` | Commit hash at branch creation (merge base) |
| `latest_head` | `str \| None` | HEAD from the store (reflects other writers) |
| `initial_commit` | `str` | Root commit (oldest in linear history) |
| `last_merge_result` | `MergeResult \| None` | Result of the last `commit()` call |

## Types

### `MergeResult`

Frozen dataclass returned by `commit()`. Truthy when merge succeeded.

| Field | Type | Description |
|-------|------|-------------|
| `merged` | `bool` | Whether the commit succeeded |
| `commit` | `str \| None` | New commit hash (None if not merged) |
| `strategy` | `str` | `"no_op"`, `"fast_forward"`, or `"three_way"` |
| `auto_merged_keys` | `tuple[str, ...]` | Keys resolved by merge functions |
| `carried_keys` | `tuple[str, ...]` | Keys carried forward unchanged |

### `DiffResult`

Frozen dataclass returned by `diff()`.

| Field | Type | Description |
|-------|------|-------------|
| `added` | `frozenset[str]` | Keys in commit_b but not commit_a |
| `removed` | `frozenset[str]` | Keys in commit_a but not commit_b |
| `modified` | `frozenset[str]` | Keys in both with different values |

### `MergeFn`

Type alias for merge functions:

```python
MergeFn = Callable[[bytes | None, bytes | None, bytes | None], bytes]
# (old_value, our_value, their_value) -> merged_value
```

### `MetaEntry`

Per-key metadata used by GC.

| Field | Type | Description |
|-------|------|-------------|
| `last_touch` | `int` | Touch counter (higher = more recently accessed) |
| `size` | `int \| None` | Value size in bytes |
| `created_at` | `float` | Creation timestamp |
