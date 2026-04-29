# API Reference

## `kvgit.store()`

Factory function that returns a configured `Staged` instance.

```python
kvgit.store(
    kind="memory",       # "memory", "disk", "git", or "indexeddb"
    *,
    path=None,           # required for "disk" and "git"
    db_name="kvgit",     # IndexedDB database name (only for "indexeddb")
    branch="main",
    encoder=pickle.dumps,
    decoder=pickle.loads,
    codecs=None,         # named codec preset (mutually exclusive with encoder/decoder)
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kind` | `Literal["memory", "disk", "git", "indexeddb"]` | `"memory"` | Backend type |
| `path` | `str \| None` | `None` | Required for `"disk"` and `"git"` |
| `db_name` | `str` | `"kvgit"` | IndexedDB database name. Only used with `"indexeddb"`. |
| `branch` | `str` | `"main"` | Branch name |
| `encoder` | `Callable[..., bytes]` | `pickle.dumps` | Value encoder. Pass a `compose()` pair to enable [chunked codecs](#chunked-codecs). |
| `decoder` | `Callable[..., Any]` | `pickle.loads` | Value decoder. |
| `codecs` | `str \| None` | `None` | Named codec preset shortcut. Currently `"scientific"` (numpy + pandas chunked codecs). Mutually exclusive with explicit `encoder` / `decoder`. |

**Named codec presets** (passed via `codecs="..."`):

| Name | Codecs included | Required dependency |
|------|-----------------|---------------------|
| `"scientific"` | `NumpyCodec()` (catches pandas DataFrame block buffers too) | `pip install kvgit[scientific]` |

---

## Staged

`Staged` wraps a `Versioned` implementation and provides a `MutableMapping[str, Any]` interface with buffered writes. Individual `set()` / `__setitem__()` calls are held in memory; `commit()` encodes and flushes them atomically.

### Construction

```python
from kvgit import Staged, VersionedKV

s = Staged(VersionedKV(), encoder=pickle.dumps, decoder=pickle.loads)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `versioned` | `Versioned` | (required) | Any `Versioned` implementation |
| `encoder` | `Callable[..., bytes]` | `pickle.dumps` | Serializes values to bytes on commit |
| `decoder` | `Callable[..., Any]` | `pickle.loads` | Deserializes bytes to values on read |

#### Chunked encoder/decoder

`Staged` autodetects the encoder/decoder shape by signature:

* **1-arg** -- `encoder(value) -> bytes` and `decoder(bytes) -> value` use the legacy in-blob serialization. The store layout stays v2-compatible.
* **2-arg with required second parameter** -- `encoder(value, sink) -> bytes` and `decoder(blob, reader) -> value` route through a `ChunkSink` / `ChunkReader`, enabling content-addressed chunk dedup. The first chunked write upgrades the store to v3.

The arity check is "second positional parameter has no default" -- so `pickle.dumps` (whose `protocol` arg has a default) stays 1-arg, and the encoders returned by `kvgit.codecs.compose(...)` are detected as 2-arg automatically. See [Chunked codecs](#chunked-codecs).

### Reading

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key, default=None) -> Any` | Check staged buffer first, then committed state |
| `get_many` | `(*keys) -> dict[str, Any]` | Batch get; only includes existing keys |
| `keys` | `() -> set[str]` | All keys (staged + committed, minus staged removals) |
| `__getitem__` | `(key) -> Any` | Raises `KeyError` if missing |
| `__contains__` | `(key) -> bool` | Check existence |
| `__iter__` | `() -> Iterator[str]` | Iterate over keys |
| `__len__` | `() -> int` | Number of keys |
| `is_staged` | `(key) -> bool` | Whether this key has uncommitted changes |

### Writing

| Method | Signature | Description |
|--------|-----------|-------------|
| `__setitem__` | `(key, value) -> None` | Stage a value |
| `__delitem__` | `(key) -> None` | Stage a removal. Raises `KeyError` if missing. |
| `set` | `(key, value) -> None` | Same as `__setitem__` |
| `remove` | `(key) -> None` | Same as `__delitem__` |

### Committing

#### `commit(*, keys=None, on_conflict="raise", merge_fns=None, default_merge=None, info=None) -> MergeResult`

Encode staged changes and flush as a single atomic commit. If HEAD has diverged, a three-way merge is performed.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `keys` | `set[str] \| None` | `None` | If provided, only commit these keys. Uncommitted keys remain staged. |
| `on_conflict` | `str` | `"raise"` | `"raise"` or `"abandon"` |
| `merge_fns` | `dict[str, MergeFn] \| None` | `None` | Per-key merge functions for this commit |
| `default_merge` | `MergeFn \| None` | `None` | Fallback merge function for this commit |
| `info` | `dict \| None` | `None` | Metadata attached to the commit |

**Partial commits:** Pass `keys` to commit only a subset of staged changes. Keys not in `_updates` or `_removals` are silently ignored. Uncommitted keys remain staged for a future `commit()`.

```python
s["a"] = b"alpha"
s["b"] = b"beta"
s.commit(keys={"a"}, info={"message": "just a"})
# "a" is committed; "b" remains staged
```

#### `reset() -> None`

Discard all staged (uncommitted) changes.

#### `refresh() -> None`

Reload from HEAD and discard staged changes. Use this to see writes from other branches or processes.

### Merge functions

#### `set_merge_fn(key, fn) -> None`

Register a persistent merge function for a key. `fn` receives decoded values: `(old, ours, theirs) -> merged`.

#### `set_default_merge(fn) -> None`

Register a fallback merge function for any key without a specific registration.

### Branching

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_branch` | `(name, *, at=None) -> Staged` | Fork onto a new branch. Returns a new `Staged`. |
| `checkout` | `(commit_hash, *, branch=None) -> Staged \| None` | Open a specific commit. Returns `None` if not found. |
| `switch_branch` | `(name) -> None` | Switch to an existing branch (clears staged buffer). |
| `delete_branch` | `(name) -> None` | Delete a branch and clean up orphaned commits. Cannot delete the current branch. |
| `list_branches` | `() -> list[str]` | All branch names in the store. |
| `peek` | `(key, *, branch) -> Any \| None` | Read a decoded value from another branch's HEAD. |
| `reset_to` | `(commit_hash) -> bool` | Force HEAD to a specific commit. Returns `False` if not found. |

### History

| Method | Signature | Description |
|--------|-----------|-------------|
| `history` | `(commit_hash=None, *, all_parents=False) -> Iterable[str]` | Walk commit chain from newest to oldest. `all_parents=True` for full DAG (BFS). |

Access `commit_info()` and `diff()` via `s.versioned`:

```python
s.versioned.commit_info()              # info dict for current commit
s.versioned.commit_info(some_hash)     # info dict for specific commit
s.versioned.diff(hash_a, hash_b)       # DiffResult between two commits
s.versioned.parents()                  # parent hashes of current commit
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `versioned` | `Versioned` | The underlying versioned engine |
| `current_commit` | `str` | Current commit hash |
| `base_commit` | `str` | Commit hash at branch creation |
| `current_branch` | `str` | Name of the current branch |
| `initial_commit` | `str` | Root commit (oldest in linear history) |
| `last_merge_result` | `MergeResult \| None` | Result of the last `commit()` |
| `has_changes` | `bool` | Whether the staging buffer is non-empty |

---

## Namespaced

Key-prefixed view over any `MutableMapping[str, Any]`. All keys are transparently prefixed with `namespace/`.

### Construction

```python
from kvgit import Namespaced

ns = Namespaced(store, "myns")
```

Raises `ValueError` if namespace contains `/`. Nesting is supported:

```python
inner = Namespaced(ns, "sub")
inner.namespace  # "myns/sub"
```

### Reading

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key, default=None) -> Any` | Get from namespaced view |
| `get_many` | `(*keys) -> dict[str, Any]` | Batch get; returns unprefixed keys |
| `keys` | `() -> set[str]` | Direct child keys only |
| `descendant_keys` | `() -> Iterable[str]` | All keys including nested namespace paths |
| `__getitem__` | `(key) -> Any` | Raises `KeyError` if missing |
| `__contains__` | `(key) -> bool` | Check existence |
| `__iter__` | `() -> Iterator[str]` | Iterate over direct child keys |
| `__len__` | `() -> int` | Number of direct child keys |

### Writing

| Method | Signature | Description |
|--------|-----------|-------------|
| `__setitem__` | `(key, value) -> None` | Set (auto-prefixed) |
| `__delitem__` | `(key) -> None` | Remove (auto-prefixed) |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `namespace` | `str` | Full namespace path (e.g., `"agent/worker"`) |

### Merge functions

Register merge functions on the underlying store with the full prefixed key:

```python
s.set_merge_fn("myns/counter", fn)
```

---

## Types

### MergeResult

Frozen dataclass returned by `commit()`. Truthy when merge succeeded.

| Field | Type | Description |
|-------|------|-------------|
| `merged` | `bool` | Whether the commit succeeded |
| `commit` | `str \| None` | New commit hash |
| `strategy` | `str` | `"no_op"`, `"fast_forward"`, or `"three_way"` |
| `auto_merged_keys` | `tuple[str, ...]` | Keys resolved by merge functions |
| `carried_keys` | `tuple[str, ...]` | Keys carried forward from the other side |

### DiffResult

Frozen dataclass returned by `diff()`.

| Field | Type | Description |
|-------|------|-------------|
| `added` | `frozenset[str]` | Keys in commit_b but not commit_a |
| `removed` | `frozenset[str]` | Keys in commit_a but not commit_b |
| `modified` | `frozenset[str]` | Keys in both with different blob hashes |

### MergeFn

User-level merge function type (decoded values), used by `Staged`:

```python
MergeFn = Callable[[Any | None, Any, Any], Any]
# (old_value, our_value, their_value) -> merged_value
```

### BytesMergeFn

Bytes-level merge function type, used by `VersionedKV` / `VersionedGP`:

```python
BytesMergeFn = Callable[[bytes | None, bytes | None, bytes | None], bytes]
```

---

## Built-in merge functions

### `counter() -> MergeFn`

Integer counter merge: `ours + theirs - old`. Both sides' increments are preserved.

### `last_writer_wins() -> MergeFn`

Always returns `theirs` (the HEAD value).

---

## Errors

### ConcurrencyError

Raised when a CAS operation fails during `commit()`. Another writer updated HEAD between when this instance last read it and when the commit was attempted.

### MergeConflict

Raised when a three-way merge encounters keys changed by both sides with no merge function to resolve them.

| Attribute | Type | Description |
|-----------|------|-------------|
| `conflicting_keys` | `set[str]` | Keys that could not be resolved |
| `merge_errors` | `dict[str, Exception]` | Per-key exceptions from merge functions that raised |

---

## Chunked codecs

`kvgit.codecs` is an opt-in layer that externalizes large sub-values (numpy buffers, pandas DataFrames, ...) as content-addressed chunks. Equal buffers are stored once across keys, commits, and branches. Pass the resulting `(encoder, decoder)` pair to `Staged` (or `kvgit.store(...)`) to enable.

Install with `pip install kvgit[numpy]` or `kvgit[scientific]`.

### `compose(*codecs) -> (encoder, decoder)`

Build the encoder/decoder pair from a list of codecs. Codecs are tried in order during encoding -- the first to claim an object wins. Plain pickling handles anything no codec claims; there is no need to register a "pickle codec".

```python
from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec

encoder, decoder = compose(NumpyCodec())
```

Order matters when codecs claim overlapping types. Put the more specific codec first.

### `scientific() -> (encoder, decoder)`

One-liner shortcut: compose the numpy codec (which transparently handles pandas DataFrames via their pickle path). Equivalent to `compose(NumpyCodec())`. Raises `ImportError` if numpy is not installed.

```python
from kvgit.codecs import scientific

encoder, decoder = scientific()
```

The same shortcut is exposed on the factory as `kvgit.store(codecs="scientific")` -- prefer that when you don't need to tune codec parameters.

### `NumpyCodec(min_bytes=1024)`

Externalizes `numpy.ndarray` instances. Built-in dedup behaviors:

| Case | What happens |
|------|--------------|
| Same buffer (Python `is`) | One chunk; `id()` memo skips the second hash |
| Two arrays with identical bytes | One chunk via content-addressed hash |
| `arr2 = arr[i:j]` (view of a parent) | Chunk hashes the **root** buffer; both arrays share it |
| `arr.dtype.hasobject` (object dtype) | Pass through to pickle (elements may be intercepted by other codecs) |
| `arr.nbytes < min_bytes` and not a view | Pass through to pickle (chunk overhead exceeds savings) |

Materialized arrays are **read-only**. Slices reconstruct via `numpy.lib.stride_tricks.as_strided` from the shared chunk bytes; making them writeable would risk silent cross-key corruption. Call `.copy()` to mutate.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_bytes` | `int` | `1024` | Below this size, standalone arrays inline rather than chunk. Tunable per backend (IndexedDB has higher per-entry overhead, so a higher threshold may be appropriate). |

### `PandasCodec`

Currently an alias for `NumpyCodec`. Pickling a DataFrame visits its block ndarrays as Python objects, which the numpy codec catches before reduction -- so DataFrame block buffers chunk for free, including `iloc` row-slice views that share blocks with their parent. Extension dtypes whose pickle path doesn't expose ndarrays uniformly (some `ArrowDtype` / `MaskedArray` cases) fall back to opaque pickle without chunking.

```python
from kvgit.codecs.pandas import PandasCodec  # alias of NumpyCodec
```

### Codec protocol

Custom codecs implement two methods. Each codec must declare a unique short `name` (used as the persistent-id tag inside encoded blobs).

```python
class Codec(Protocol):
    name: str

    def try_externalize(self, obj, sink: ChunkSink) -> Any | None:
        """Return a picklable token, or None to pass."""

    def materialize(self, token, reader: ChunkReader) -> Any:
        """Reconstruct the value from the token."""
```

`ChunkSink.put(data) -> str` registers a chunk and returns its content-addressed reference. `ChunkReader.get(ref) -> bytes` and `get_many(refs)` fetch chunks during decode.

### Storage layout (v3)

The first chunked write lazily upgrades a store from v2 to v3:

| Key pattern | Contents |
|-------------|----------|
| `kvgit:chunk:<hash>` | Content-addressed chunk bytes |
| `MetaEntry.chunks` (per key) | List of chunk hashes referenced by that key's blob |

`clean_orphans` traces `MetaEntry.chunks` from every reachable commit, plus from any commit younger than `min_age` (in-flight writer protection), and sweeps unreferenced entries under `kvgit:chunk:`. Stores that never use chunks stay byte-identical to v2.

### v2 ↔ v3 compatibility

* **v3 code reading a v2 store**: works transparently; the store's `__kvgit_version__` stamp is left as-is until the first chunked write.
* **v2 code reading a v3 store**: refused on open with a clear error. Once a chunk has been written, the store is v3-only.
* **Mixed entries**: a single store can hold both plain-pickle and chunked entries; dispatch is per-entry based on whether `MetaEntry.chunks` is populated.
* **Migration**: import values from a v2 source into a fresh v3 target (`new[k] = old[k]; new.commit()`). Equal buffers across the v2 source's keys collapse into one chunk in the target -- you get retroactive dedup as a side effect of the copy.

### Limitations

* **Merge results are not chunked.** When `Staged`'s wrapped merge function re-encodes a merged value, it always falls back to plain `pickle.dumps` (the bytes-level merge protocol has no place to land chunks). Subsequent commits that overwrite the merged key go through the chunked path normally. In single-writer use cases (e.g., one agent per branch), merges are rare and this rarely matters.
* **Materialized arrays are read-only.** Trades one allocation against safe sharing. Call `.copy()` for mutation.
* **Chunk dedup is a disk/storage optimization, not an in-memory one.** While values are sitting in the staging buffer, they're still distinct Python objects. Dedup happens at encode time.

---

## Versioned protocol

The `Versioned` protocol defines the shared interface implemented by all versioned backends. Most users interact with it through `Staged`, but it's useful for type annotations and custom backends.

```python
from kvgit import Versioned
```

See `kvgit/versioned/protocol.py` for the full protocol definition.

---

## VersionedKV

KV-backed implementation of `Versioned`. Operates on raw `bytes`. Most users should use `Staged` instead.

```python
from kvgit import VersionedKV

v = VersionedKV()                                       # in-memory
v = VersionedKV(store, branch="dev")                    # shared store
v = VersionedKV(store, commit_hash="a1b2c3...")         # resume
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store` | `KVStore \| None` | `None` | Backend. Creates `Memory()` if None. |
| `commit_hash` | `str \| None` | `None` | Resume from this commit. Reads HEAD if None. |
| `branch` | `str` | `"main"` | Branch name. |

All methods from the `Versioned` protocol are implemented. Additional:

| Method / Attribute | Description |
|--------------------|-------------|
| `store` | Direct access to the underlying `KVStore` |
| `branches(store)` | Static method: list branch names for a store |
| `clean_orphans(min_age=3600)` | Remove orphaned commits unreachable from any branch HEAD. Returns count of cleaned orphans. Only deletes commits older than `min_age` seconds. Also sweeps unreferenced chunks (v3 stores). |

### Orphan Cleanup

When branches are deleted, the commits they referenced may become unreachable ("orphaned"). `delete_branch()` automatically calls `clean_orphans()` after removing the branch HEAD. The default `min_age=3600` (1 hour) guards against concurrent writes: recently-created commits are left alone so that a commit from another thread can't be mistaken for an orphan mid-sweep. Orphaned commits from deleted branches are cleaned up by subsequent `clean_orphans()` calls once they age past the guard.

For v3 stores (those using [chunked codecs](#chunked-codecs)), the same sweep also reclaims unreferenced `kvgit:chunk:*` entries. Chunks reachable from any live branch survive; chunks reachable only from young orphan commits (within the `min_age` window) also survive, protecting in-flight writers from premature collection.

You can also call `clean_orphans()` manually:

```python
v = VersionedKV(store)
cleaned = v.clean_orphans()            # default: only orphans older than 1 hour
cleaned = v.clean_orphans(min_age=0)   # immediate (only safe without concurrent writers)
```

The cleanup is safe for shared commit histories (e.g., forked branches). Blobs referenced by any reachable commit are never deleted.

---

## VersionedGP

GitPython-backed implementation of `Versioned`. Stores data as blobs in a real git repository.

```python
from kvgit import VersionedGP

v = VersionedGP("/path/to/repo")
v = VersionedGP("/path/to/repo", branch="dev")
v = VersionedGP("/path/to/repo", commit_hash="abc123...")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `repo_path` | `str` | (required) | Path to git repository (created if missing) |
| `commit_hash` | `str \| None` | `None` | Resume from this commit. |
| `branch` | `str` | `"main"` | Branch name. |

All methods from the `Versioned` protocol are implemented. Additional:

| Attribute | Type | Description |
|-----------|------|-------------|
| `repo` | `git.Repo` | The underlying GitPython `Repo` object |
| `repo_path` | `str` | Path to the repository |

---

## KVStore

Abstract base class for storage backends. All values are `bytes`.

```python
from kvgit.kv.base import KVStore
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `(key) -> bytes \| None` | Get value or None |
| `set` | `(key, value) -> None` | Set a value |
| `get_many` | `(*keys) -> Mapping[str, bytes]` | Batch get; only existing keys |
| `set_many` | `(**kwargs) -> None` | Batch set |
| `keys` | `() -> Iterable[str]` | All keys |
| `items` | `() -> Iterable[tuple[str, bytes]]` | All key-value pairs |
| `__contains__` | `(key) -> bool` | Check existence |
| `remove` | `(key) -> None` | Remove (no-op if missing) |
| `remove_many` | `(*keys) -> None` | Batch remove |
| `cas` | `(key, value, expected) -> bool` | Atomic compare-and-swap |
| `clear` | `() -> None` | Remove all entries |

### Compare-and-swap

`cas(key, value, expected)` sets `key` to `value` only if the current value equals `expected`. Pass `expected=None` to require the key not exist. Returns `True` on success. This is the foundation of kvgit's optimistic concurrency.

---

## Memory

In-memory `KVStore`. Thread-safe. No dependencies.

```python
from kvgit.kv.memory import Memory

store = Memory()
store.memory  # underlying dict, for debugging
```

---

## Disk

Persistent `KVStore` via [diskcache](https://pypi.org/project/diskcache/). Requires `pip install kvgit[disk]`.

```python
from kvgit.kv.disk import Disk

store = Disk("/path/to/db")                      # default: unbounded
store = Disk("/path/to/db", size_limit=10 * 1024**3)  # explicit 10 GiB cap
store = Disk("/path/to/db", size_limit=None)     # also unbounded (explicit)
```

By default the store has no practical size cap. Pass `size_limit` (in bytes) to enable diskcache's eviction policy. CAS and transactional operations are safe across multiple processes (backed by SQLite file locking).

---

## IndexedDB

Browser-persistent `KVStore` via IndexedDB. Available automatically in [Pyodide](https://pyodide.org/) environments (no extra install needed).

```python
from kvgit.kv.indexeddb import IndexedDB

store = IndexedDB()
store = IndexedDB(db_name="myapp", store_name="state")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `db_name` | `str` | `"kvgit"` | IndexedDB database name. Each name is an independent store, persisted across page reloads. |
| `store_name` | `str` | `"kv"` | Object store name within the database. |

Requires JSPI (JavaScript Promise Integration). CAS is atomic across Web Workers sharing the same database.
