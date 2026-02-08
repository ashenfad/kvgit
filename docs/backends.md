# Storage Backends, Store Protocol & Namespaces

## Store Protocol

`Store` is the primary user-facing interface. It provides set/get/commit semantics. All high-level types implement it.

```python
from vkv import Store
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key: str) -> bytes \| None` | Get value or None |
| `get_many` | `(*keys: str) -> dict[str, bytes]` | Get multiple, only existing keys |
| `keys` | `() -> Iterable[str]` | Iterate over all keys |
| `__contains__` | `(key: str) -> bool` | Check key existence |
| `set` | `(key: str, value: bytes) -> None` | Set a value |
| `remove` | `(key: str) -> None` | Remove a key |
| `commit` | `(**kwargs) -> MergeResult` | Flush changes to storage |
| `reset` | `() -> None` | Discard pending changes |
| `create_branch` | `(name: str) -> Store` | Fork current commit onto a new branch |
| `checkout` | `(commit_hash: str, *, branch=None) -> Store \| None` | Open a specific commit |
| `list_branches` | `() -> list[str]` | List all branch names |

**Implementations:** `Staged`, `Live`, `Namespaced`

`Live` raises `NotImplementedError` for `commit`, `reset`, `create_branch`, `checkout`, and `list_branches`.

## Factory: `vkv.store()`

The simplest way to create a Store:

```python
import vkv

# Default: Staged backed by in-memory Versioned
s = vkv.store()

# Live store (immediate writes, no versioning)
s = vkv.store(type="live")

# With disk persistence
s = vkv.store(storage="disk", path="/path/to/db")

# With garbage collection
s = vkv.store(high_water_bytes=10_000)

# Custom branch
s = vkv.store(branch="dev")
```

### `vkv.store(type="versioned", storage="memory", *, path=None, branch="main", high_water_bytes=None, low_water_bytes=None)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `type` | `str` | `"versioned"` | `"versioned"` for Staged or `"live"` for Live |
| `storage` | `str` | `"memory"` | `"memory"` or `"disk"` |
| `path` | `str \| None` | `None` | Required when `storage="disk"` |
| `branch` | `str` | `"main"` | Branch name (versioned only) |
| `high_water_bytes` | `int \| None` | `None` | Enable GC (versioned only) |
| `low_water_bytes` | `int \| None` | `None` | GC low-water (defaults to 80% of high) |

---

## Staged

`Staged` wraps a `Versioned` instance. Individual `set()` / `remove()` calls are buffered in memory. `commit()` flushes them as a single atomic commit.

```python
from vkv import Staged, Versioned

v = Versioned()
s = Staged(v)

s.set("name", b"alice")
s.set("age", b"30")
s.commit()

s.get("name")  # b"alice"
```

### Construction

```python
Staged(versioned: Versioned)
```

### Methods

| Method | Description |
|--------|-------------|
| `set(key, value)` | Stage a key-value pair |
| `remove(key)` | Stage a removal |
| `get(key)` | Check staged first, then committed |
| `commit(**kwargs)` | Flush to Versioned, returns MergeResult |
| `reset()` | Discard staged changes |
| `refresh()` | Reload from HEAD and discard staged changes |
| `create_branch(name)` | Fork current commit, returns a new `Staged` |
| `checkout(hash, *, branch=None)` | Open a specific commit, returns `Staged` or `None` |
| `list_branches()` | List all branch names |
| `has_changes` | Property: whether staging buffer is non-empty |

### Properties

| Property | Description |
|----------|-------------|
| `versioned` | The underlying Versioned instance |
| `current_commit` | Delegates to Versioned |
| `base_commit` | Delegates to Versioned |
| `last_merge_result` | Delegates to Versioned |

### Merge & Content Types

```python
s.set_merge_fn("counter", fn)        # delegates to Versioned
s.set_content_type("hits", ct)       # delegates to Versioned
s.get_content_type("hits")           # delegates to Versioned
s.set_default_merge(fn)              # delegates to Versioned
```

---

## Live

`Live` wraps a `KVStore` directly. Writes take effect immediately. Versioning operations (`commit`, `reset`, `create_branch`, `checkout`, `list_branches`) raise `NotImplementedError`.

```python
from vkv import Live

s = Live()
s.set("k", b"v")
s.get("k")  # b"v" (immediately available)
```

### Construction

```python
Live(backend: KVStore | None = None)
```

Creates an in-memory backend if None.

---

## Namespaced

`Namespaced` provides a key-prefixed view over any `Store`. All keys are transparently prefixed with `namespace/`.

```python
from vkv import Namespaced
import vkv

s = vkv.store()
agent = Namespaced(s, "agent")
config = Namespaced(s, "config")

agent.set("state", b"running")
config.set("timeout", b"30")

agent.get("state")   # b"running"
config.get("state")  # None (isolated)
s.get("agent/state") # b"running" (prefixed in base store)
```

### Construction

```python
Namespaced(store: Store, namespace: str)
```

Namespace names must not contain `/`. Nesting is supported by wrapping another `Namespaced`:

```python
ns1 = Namespaced(s, "agent")
ns2 = Namespaced(ns1, "worker")
ns2.namespace  # "agent/worker"
ns2.get("task")  # reads "agent/worker/task" from base store
```

### Reading

| Method | Description |
|--------|-------------|
| `get(key)` | Get from namespaced view |
| `get_many(*keys)` | Batch get, returns unprefixed keys |
| `keys()` | Direct child keys only (not nested) |
| `descendant_keys()` | All keys including nested paths |
| `key in ns` | Check existence |

### Writing

All write methods auto-prefix keys:

```python
ns.set("k", b"v")              # writes "myns/k" in base store
ns.remove("k")                 # removes "myns/k"
result = ns.commit()           # delegates to underlying store
```

### Merge & Content Types

```python
ns.set_merge_fn("counter", fn)        # registers for "myns/counter"
ns.set_content_type("hits", ct)       # registers for "myns/hits"
ns.get_content_type("hits")           # retrieves the ContentType
ns.set_default_merge(fn)              # store-wide default (not prefixed)
```

### Properties

| Property | Description |
|----------|-------------|
| `namespace` | Full namespace path (e.g., `"agent/worker"`) |
| `current_commit` | Delegates to underlying store |
| `base_commit` | Delegates to underlying store |
| `last_merge_result` | Delegates to underlying store |

### Branching

All branching methods delegate to the underlying store:

```python
ns.create_branch("dev")   # delegates to underlying store
ns.checkout(some_hash)    # delegates to underlying store
ns.list_branches()        # delegates to underlying store
```

---

## KVStore Interface

All backends implement the `KVStore` abstract base class. Values are bytes-only; serialization is handled by higher layers.

```python
from vkv import KVStore
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key: str) -> bytes \| None` | Get value or None |
| `set` | `(key: str, value: bytes) -> None` | Set a value |
| `get_many` | `(*keys: str) -> Mapping[str, bytes]` | Get multiple, only existing keys |
| `set_many` | `(**kwargs: bytes) -> None` | Set multiple key-value pairs |
| `keys` | `() -> Iterable[str]` | Iterate over all keys |
| `items` | `() -> Iterable[tuple[str, bytes]]` | Iterate over all pairs |
| `__contains__` | `(key: str) -> bool` | Check key existence |
| `remove` | `(key: str) -> None` | Remove a key (no-op if missing) |
| `remove_many` | `(*keys: str) -> None` | Remove multiple keys |
| `cas` | `(key: str, value: bytes, expected: bytes \| None) -> bool` | Atomic compare-and-swap |
| `clear` | `() -> None` | Remove all entries |

### Compare-and-Swap (CAS)

`cas(key, value, expected)` sets `key` to `value` only if the current value equals `expected`. Pass `expected=None` to require the key not exist. Returns `True` on success. This is the foundation of vkv's optimistic concurrency.

## Memory

In-memory backend. Fast, no dependencies, no persistence.

```python
from vkv.kv.memory import Memory

store = Memory()
```

CAS operations are thread-safe (locked). Other operations are not synchronized -- use a single writer or external locking for concurrent access.

The underlying dict is accessible as `store.memory` for debugging.

## Disk

Persistent backend via [diskcache](https://pypi.org/project/diskcache/). Requires the `disk` extra.

```bash
pip install vkv[disk]
```

```python
from vkv.kv.disk import Disk

store = Disk("/path/to/db")
```

## Custom Backends

Implement `KVStore` to use any storage:

```python
from vkv import KVStore

class RedisStore(KVStore):
    def get(self, key):
        return self.client.get(key)
    def set(self, key, value):
        self.client.set(key, value)
    def cas(self, key, value, expected):
        # Use Redis WATCH/MULTI for atomicity
        ...
    # ... implement remaining methods
```

---

## Errors

### `ConcurrencyError`

Raised when a CAS operation fails during `commit()` or `rebase()`. Another writer updated HEAD between when this branch started and when the commit was attempted.

```python
from vkv import ConcurrencyError

try:
    v.commit({"k": b"v"})
except ConcurrencyError:
    v.refresh()
    # re-apply changes and retry
```

### `MergeConflict`

Raised when a three-way merge encounters keys that both sides changed and no merge function resolves them.

```python
from vkv import MergeConflict

try:
    v.commit({"k": b"v"})
except MergeConflict as e:
    print(e.conflicting_keys)  # {"key_a", "key_b"}
    print(e.merge_errors)      # {"key_a": ValueError("...")} if a merge fn raised
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `conflicting_keys` | `set[str]` | Keys that could not be resolved |
| `merge_errors` | `dict[str, Exception]` | Per-key exceptions from merge functions that raised |
