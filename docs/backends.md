# Storage Backends & Namespaces

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

## Namespaced

`Namespaced` provides a key-prefixed view over a `Versioned` store. All keys are transparently prefixed with `namespace/`.

```python
from vkv import Versioned, Namespaced

v = Versioned()
agent = Namespaced(v, "agent")
config = Namespaced(v, "config")

agent.snapshot({"state": b"running"})
config.snapshot({"timeout": b"30"})

agent.get("state")   # b"running"
config.get("state")  # None (isolated)
v.get("agent/state") # b"running" (prefixed in base store)
```

### Construction

```python
Namespaced(store: Versioned | Namespaced, namespace: str)
```

Namespace names must not contain `/`. Nesting is supported by wrapping another `Namespaced`:

```python
ns1 = Namespaced(v, "agent")
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
ns.snapshot({"k": b"v"})          # writes "myns/k" in base store
ns.snapshot(removals={"k"})       # removes "myns/k"
result = ns.merge()               # merges the underlying branch
```

Merge functions passed via `merge(merge_fns={"k": fn})` are auto-prefixed too.

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
| `base_store` | The underlying `Versioned` instance (unwraps nesting) |
| `current_commit` | Delegates to base store |
| `base_commit` | Delegates to base store |
| `last_merge_result` | Delegates to base store |

### Other

```python
ns.list_branches()  # delegates to base store
```

---

## Errors

### `ConcurrencyError`

Raised when a CAS operation fails during `merge()` or `rebase()`. Another writer updated HEAD between when this branch started and when the merge was attempted.

```python
from vkv import ConcurrencyError

try:
    v.merge()
except ConcurrencyError:
    v.reset()
    # re-apply changes and retry
```

### `MergeConflict`

Raised when a three-way merge encounters keys that both sides changed and no merge function resolves them.

```python
from vkv import MergeConflict

try:
    v.merge()
except MergeConflict as e:
    print(e.conflicting_keys)  # {"key_a", "key_b"}
    print(e.merge_errors)      # {"key_a": ValueError("...")} if a merge fn raised
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `conflicting_keys` | `set[str]` | Keys that could not be resolved |
| `merge_errors` | `dict[str, Exception]` | Per-key exceptions from merge functions that raised |
