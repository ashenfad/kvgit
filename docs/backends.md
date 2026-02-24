# Storage Backends, Store Protocol & Namespaces

## Store Protocol

`Store` is the base key-value interface. It implements `MutableMapping[str, Any]` semantics.

```python
from kvgit import Store
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key: str, default=None) -> Any` | Get value or default |
| `get_many` | `(*keys: str) -> dict[str, Any]` | Get multiple, only existing keys |
| `keys` | `() -> Iterable[str]` | Iterate over all keys |
| `__contains__` | `(key: object) -> bool` | Check key existence |
| `__getitem__` | `(key: str) -> Any` | Get value, raise KeyError if missing |
| `__setitem__` | `(key: str, value: Any) -> None` | Set a value |
| `__delitem__` | `(key: str) -> None` | Remove a key, raise KeyError if missing |
| `__iter__` | `() -> Iterator[str]` | Iterate over keys |
| `__len__` | `() -> int` | Number of keys |
| `set` | `(key: str, value: Any) -> None` | Set a value |
| `remove` | `(key: str) -> None` | Remove a key |

**Implementations:** `Staged`, `Live`, `Namespaced`

## VersionedStore Protocol

`VersionedStore` extends `Store` with commit semantics and branching.

```python
from kvgit import VersionedStore
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `commit` | `(**kwargs) -> MergeResult` | Flush changes to storage |
| `reset` | `() -> None` | Discard pending changes |
| `create_branch` | `(name: str) -> VersionedStore` | Fork current commit onto a new branch |
| `checkout` | `(commit_hash: str, *, branch=None) -> VersionedStore \| None` | Open a specific commit |
| `list_branches` | `() -> list[str]` | List all branch names |

**Implementations:** `Staged`

## Factory: `kvgit.store()`

The simplest way to create a store:

```python
import kvgit

# Default: Staged backed by in-memory Versioned
s = kvgit.store()

# With disk persistence
s = kvgit.store(kind="disk", path="/path/to/db")

# With garbage collection
s = kvgit.store(high_water_bytes=10_000)

# Custom branch
s = kvgit.store(branch="dev")

# Custom encoder/decoder (default is pickle)
import json
s = kvgit.store(
    encoder=lambda v: json.dumps(v).encode(),
    decoder=lambda b: json.loads(b),
)
```

### `kvgit.store(kind="memory", *, path=None, branch="main", encoder=pickle.dumps, decoder=pickle.loads, high_water_bytes=None, low_water_bytes=None, is_protected=None)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kind` | `Literal["memory", "disk"]` | `"memory"` | Backend type |
| `path` | `str \| None` | `None` | Required when `kind="disk"` |
| `branch` | `str` | `"main"` | Branch name |
| `encoder` | `Callable[[Any], bytes]` | `pickle.dumps` | Value encoder |
| `decoder` | `Callable[[bytes], Any]` | `pickle.loads` | Value decoder |
| `high_water_bytes` | `int \| None` | `None` | Enable GC |
| `low_water_bytes` | `int \| None` | `None` | GC low-water (defaults to 80% of high) |
| `is_protected` | `Callable[[str], bool] \| None` | `None` | Keys GC should never drop. Only used when `high_water_bytes` is set. Defaults to protecting keys starting with `__`. |

---

## Staged

`Staged` wraps a `Versioned` instance and implements `MutableMapping[str, Any]`. Individual `set()` / `__setitem__()` calls are buffered in memory. `commit()` encodes values to bytes and flushes them as a single atomic commit.

```python
from kvgit import Staged, Versioned

s = Staged(Versioned())

s["name"] = "alice"
s["age"] = 30
s.commit()

s["name"]  # "alice"
```

### Construction

```python
Staged(versioned: Versioned, *, encoder=pickle.dumps, decoder=pickle.loads)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `versioned` | `Versioned` | (required) | The underlying versioned store |
| `encoder` | `Callable[[Any], bytes]` | `pickle.dumps` | Serializes values to bytes on commit |
| `decoder` | `Callable[[bytes], Any]` | `pickle.loads` | Deserializes bytes to values on read |

> **Note:** The default encoder/decoder uses `pickle`, which can execute arbitrary code during deserialization. This is safe when the store backend is local and under your control (memory, local disk). If your backend crosses a trust boundary (e.g., a shared remote store), switch to a safe serializer like JSON.

### Methods

| Method | Description |
|--------|-------------|
| `set(key, value)` | Stage a key-value pair |
| `remove(key)` | Stage a removal |
| `get(key, default=None)` | Check staged first, then committed |
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

### Merge Functions

Merge functions on Staged operate on **decoded values** (not bytes):

```python
s.set_merge_fn("counter", fn)        # user-level MergeFn
s.set_default_merge(fn)              # fallback for unregistered keys
```

At commit time, Staged wraps these into bytes-level merge functions automatically.

---

## Live

`Live` is an in-memory store with no versioning. Writes take effect immediately. Backed by a plain `dict[str, Any]`. Satisfies the `Store` protocol (not `VersionedStore`).

```python
from kvgit import Live

s = Live()
s["k"] = "v"
s["k"]  # "v" (immediately available)
```

### Construction

```python
Live()
```

No parameters. Memory-only.

---

## Namespaced

`Namespaced` provides a key-prefixed view over any `Store`. All keys are transparently prefixed with `namespace/`. Implements `MutableMapping[str, Any]` and satisfies the `Store` protocol.

Versioning operations (commit, reset, branching) are performed on the underlying store directly:

```python
from kvgit import Namespaced
import kvgit

s = kvgit.store()
agent = Namespaced(s, "agent")
config = Namespaced(s, "config")

agent["state"] = "running"
config["timeout"] = 30

agent["state"]       # "running"
config.get("state")  # None (isolated)
s.get("agent/state") # "running" (prefixed in base store)

s.commit()           # commit is a store-level operation
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
| `get(key, default=None)` | Get from namespaced view |
| `get_many(*keys)` | Batch get, returns unprefixed keys |
| `keys()` | Direct child keys only (not nested) |
| `descendant_keys()` | All keys including nested paths |
| `key in ns` | Check existence |

### Writing

All write methods auto-prefix keys:

```python
ns["k"] = "v"                  # writes "myns/k" in base store
ns.set("k", "v")               # same as above
ns.remove("k")                 # removes "myns/k"
```

### Merge Functions

Register merge functions on the underlying store with the full prefixed key:

```python
s.set_merge_fn("myns/counter", fn)   # register on the store, not the namespace
```

### Properties

| Property | Description |
|----------|-------------|
| `namespace` | Full namespace path (e.g., `"agent/worker"`) |

---

## KVStore Interface

All backends implement the `KVStore` abstract base class. Values are bytes-only; serialization is handled by higher layers (Staged).

```python
from kvgit.kv.base import KVStore
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

`cas(key, value, expected)` sets `key` to `value` only if the current value equals `expected`. Pass `expected=None` to require the key not exist. Returns `True` on success. This is the foundation of kvgit's optimistic concurrency.

## Memory

In-memory backend. Fast, no dependencies, no persistence.

```python
from kvgit.kv.memory import Memory

store = Memory()
```

All operations are thread-safe (locked), including under free-threaded Python 3.14+.

The underlying dict is accessible as `store.memory` for debugging.

## Disk

Persistent backend via [diskcache](https://pypi.org/project/diskcache/). Requires the `disk` extra.

```bash
pip install kvgit[disk]
```

```python
from kvgit.kv.disk import Disk

store = Disk("/path/to/db")
```

CAS and transactional operations are safe across multiple processes sharing the same directory, backed by SQLite's file-level locking. This makes Disk suitable for multi-process agent coordination.

## Custom Backends

Implement `KVStore` to use any storage:

```python
from kvgit.kv.base import KVStore

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
from kvgit import ConcurrencyError

try:
    s.commit()
except ConcurrencyError:
    s.refresh()
    # re-apply changes and retry
```

### `MergeConflict`

Raised when a three-way merge encounters keys that both sides changed and no merge function resolves them.

```python
from kvgit import MergeConflict

try:
    s.commit()
except MergeConflict as e:
    print(e.conflicting_keys)  # {"key_a", "key_b"}
    print(e.merge_errors)      # {"key_a": ValueError("...")} if a merge fn raised
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `conflicting_keys` | `set[str]` | Keys that could not be resolved |
| `merge_errors` | `dict[str, Exception]` | Per-key exceptions from merge functions that raised |
