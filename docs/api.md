# API Reference

## `kvgit.store()`

Factory function that returns a configured `Staged` instance.

```python
kvgit.store(
    kind="memory",       # "memory", "disk", or "indexeddb"
    *,
    path=None,           # required for "disk"
    db_name="kvgit",     # IndexedDB database name (only for "indexeddb")
    branch="main",
    encoder=pickle.dumps,
    decoder=pickle.loads,
    codecs=None,         # named codec preset (mutually exclusive with encoder/decoder)
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kind` | `Literal["memory", "disk", "indexeddb"]` | `"memory"` | Backend type |
| `path` | `str \| None` | `None` | Required for `"disk"` |
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

## `kvgit.delete_branches()`

Admin teardown: delete one or more branches directly on the backend, with no branch anchor, then sweep orphaned commits.

```python
kvgit.delete_branches(
    names,               # one branch name, or an iterable of them
    *,
    kind="disk",         # "memory", "disk", or "indexeddb"
    path=None,           # required for "disk"
    db_name="kvgit",     # IndexedDB database name (only for "indexeddb")
    min_age=3600,        # sweep guard: commits younger than this survive
)
```

`Staged.delete_branch` / `VersionedKV.delete_branch` refuse to delete the branch the handle is anchored on, and a handle always has a current branch — so when the doomed branch is the store's only branch, there is nothing safe to anchor on. `delete_branches` opens the raw backend (no `VersionedKV`, hence no current branch) and edits the branch keys itself, so any branch — including the last one — can be removed.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `names` | `str \| Iterable[str]` | — | Branch names to delete. Missing names are no-ops (idempotent teardown). A bare string is one name, not iterated per character. |
| `kind` | `Literal["memory", "disk", "indexeddb"]` | `"disk"` | Backend type |
| `path` | `str \| None` | `None` | Required for `"disk"` |
| `db_name` | `str` | `"kvgit"` | IndexedDB database name. Only used with `"indexeddb"`. |
| `min_age` | `float` | `3600` | Passed to the orphan sweep — commits younger than this many seconds survive. `0` reclaims immediately (only when no concurrent writers). |

Each name's `__branch_head__` and its `__branch_head_prev__` recovery backup are removed (the backup too, so a later same-named branch can't resurrect the deleted state), then a single [`clean_orphans`](#orphan-cleanup) sweep — at `min_age` (default: the one-hour concurrent-writer guard) — reclaims commits only the deleted branches referenced. That sweep does not reclaim chunks (see [Orphan Cleanup](#orphan-cleanup)); follow with `deep_clean` on a quiescent store if the store uses chunked codecs. Deleting every branch is legal; the store mints a fresh empty `main` on next open.

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

Bytes-level merge function type, used by `VersionedKV`:

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

Materialized arrays are independent, writable copies. Reads allocate a fresh array (one memcpy per key, equivalent to plain `pickle.loads`); the dedup story is purely at the storage layer. Mutating an array returned from one key has no effect on any other key, even when they share the same chunk on disk.

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

Chunk reclamation belongs to [`deep_clean`](#orphan-cleanup) alone. Because a chunk key is a bare content hash, an orphan's chunk may be the very key a concurrent writer's new commit just deduped onto, so `clean_orphans` leaves chunks in place — see [Chunks are not reclaimed by `clean_orphans()`](#chunks-are-not-reclaimed-by-clean_orphans). `deep_clean` marks `MetaEntry.chunks` from every reachable commit plus any commit younger than `min_age` (in-flight writer protection), then sweeps the rest. Stores that never use chunks stay byte-identical to v2.

### v2 ↔ v3 compatibility

* **v3 code reading a v2 store**: works transparently; the store's `__kvgit_version__` stamp is left as-is until the first chunked write.
* **v2 code reading a v3 store**: refused on open with a clear error. Once a chunk has been written, the store is v3-only.
* **Mixed entries**: a single store can hold both plain-pickle and chunked entries; dispatch is per-entry based on whether `MetaEntry.chunks` is populated.
* **Migration**: import values from a v2 source into a fresh v3 target (`new[k] = old[k]; new.commit()`). Equal buffers across the v2 source's keys collapse into one chunk in the target -- you get retroactive dedup as a side effect of the copy.

### Limitations

* **Merge results are not chunked.** When `Staged`'s wrapped merge function re-encodes a merged value, it always falls back to plain `pickle.dumps` (the bytes-level merge protocol has no place to land chunks). Subsequent commits that overwrite the merged key go through the chunked path normally. In single-writer use cases (e.g., one agent per branch), merges are rare and this rarely matters.
* **Decode allocates per key.** The codec is a storage-layer optimization — every read materializes a fresh, writable array (one memcpy, same cost shape as plain `pickle.loads`). It saves disk and quota; it doesn't reduce in-process RAM after read.
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
| `recover_from_corrupt_head` | `CorruptHeadRecoverer \| None` | `None` | Last-resort HEAD recovery, applied to every resolve this handle makes. `None` means a HEAD that is corrupt with no usable backup is unrecoverable. See [HEAD Recovery](#head-recovery). |

All methods from the `Versioned` protocol are implemented. Additional:

| Method / Attribute | Description |
|--------------------|-------------|
| `store` | Direct access to the underlying `KVStore` |
| `branches(store)` | Static method: list branch names for a store |
| `clean_orphans(min_age=3600)` | Remove orphaned commits unreachable from any branch HEAD, along with the blobs and HAMT nodes they uniquely owned. **Does not reclaim chunks** — see below. Returns count of cleaned orphans. Only deletes commits older than `min_age` seconds. Safe under concurrent writers. |
| `deep_clean(min_age=3600)` | `clean_orphans` plus a full scan of the `kvgit:keyset:` and `kvgit:chunk:` namespaces. The only pass that reclaims chunks, orphan-owned ones included. **Requires a quiescent store** — see below. |
| `repair_head()` | Persist a recovered HEAD for this branch. Reads recover a damaged HEAD in memory without writing it back; this is the explicit call that makes the recovery durable. Returns the commit HEAD now names, or `None` if nothing was recoverable. See [HEAD Recovery](#head-recovery). |

### HEAD Recovery

A branch HEAD lives in one key, `__branch_head__<branch>`, and a backup of the value it held before its current one lives in `__branch_head_prev__<branch>`. If HEAD is unreadable — truncated bytes, a hash whose commit metadata is gone — head resolution tries the backup, and if that does not resolve either, reports `None`: the branch is unrecoverable.

There is a third tier below the backup, and it is **off by default**. When HEAD is unresolvable *and* the backup is missing or equally broken, the information needed is no longer in the store, so nothing kvgit can do is correct — only lucky. `recover_from_corrupt_head` is the seam for a caller who decides a guess beats losing the branch:

```python
from kvgit.versioned.kv import recover_by_commit_scan

v = VersionedKV(store, recover_from_corrupt_head=recover_by_commit_scan)
```

The recoverer is `(store, branch) -> str | None`, fired only when HEAD is present and both tiers above have failed. It applies to every resolve the handle makes — opening, `latest_head`, `refresh`, `switch_branch`, `peek`, `repair_head` — and is inherited by handles from `checkout()` and `create_branch()`. The module-level `repair_head(store, branch, recover_from_corrupt_head=...)` takes the same argument.

`recover_by_commit_scan` is the implementation kvgit used to run by default, kept and exported. It scans every `__commit_root__` and returns the newest tip not claimed by a healthy branch. Know what you are buying:

* **It can serve another branch's deleted data.** "Unclaimed" is its only signal for whose commit a commit is, and a deleted branch's commits are unclaimed until `clean_orphans()` collects them. Delete a branch, damage an unrelated branch's HEAD, lose its backup, and the survivor resolves onto the deleted branch's tip.
* **It is O(store)**, per unresolved read, until `repair_head()` runs.

It is a reasonable trade on a single-branch store, or one where branches are never deleted — neither hazard is in play there.

`clean_orphans()` and `deep_clean()` never use a recoverer, even one your handle carries. GC must not decide reachability from a guess: a wrong answer marks the wrong commits live, so real garbage survives and another branch's ancestry gets pinned into this one's mark set. The sweep marks only from branches whose HEAD actually resolves.

Two further rules govern this.

**Reads never write.** Resolving a damaged branch on a read path — opening a handle, `peek`, `switch_branch`, `refresh`, the mark phase of a sweep — recovers in memory and leaves the store exactly as it found it. A read-only consumer can therefore read a damaged store, two concurrent readers cannot race each other repairing the same branch to different answers, and the damage stays visible instead of being quietly papered over. The cost is that the fallback runs on each read until someone repairs it: the backup tier is a couple of extra `get` calls and is flat in store size, and an injected recoverer costs whatever it costs.

Two things persist a recovery:

* `repair_head()` — the explicit maintenance call, and the one to reach for.
* A successful write. A CAS against a damaged HEAD always fails, which would leave the branch permanently unwritable, so a writer that finds HEAD unresolvable replaces it with the recovered commit and retries once. The replacement is itself a CAS against the exact damaged bytes, so two writers racing it cannot both win, and a HEAD that merely *moved* — an ordinary lost race — is never touched.

```python
v = VersionedKV(store, branch="main")
v.repair_head()                                  # or:
kvgit.versioned.kv.repair_head(store, "main")    # no handle needed
```

**The backup only ever names a commit HEAD really held.** `__branch_head_prev__` is written after a HEAD swap succeeds, never before. Written first it would land whether or not the swap did, so a writer losing a race would leave its own stale value as the branch's recovery target — and where that value came from a recoverer, a commit that was never HEAD at all, which recovery would then graft onto the branch as a lineage it never had. It does **not** guarantee a backup exactly one commit back. The swap and the backup write are two steps, and anything that separates them — a crash, or simply losing the CPU while another writer completes both of its own — lets the older writer's backup land last, leaving HEAD two or more commits ahead of it. Recovery then skips whatever came between. That is the deliberate trade: recovering to an older real HEAD loses commits, recovering to a commit that was never HEAD loses the branch. No backend offers a CAS spanning two keys, so the two writes cannot be made one.

### Orphan Cleanup

When branches are deleted, the commits they referenced may become unreachable ("orphaned"). `delete_branch()` automatically calls `clean_orphans()` after removing the branch HEAD. The default `min_age=3600` (1 hour) decides which unreachable commits are old enough to delete; orphans from deleted branches are cleaned up by subsequent `clean_orphans()` calls once they age past the guard.

`clean_orphans()` finds everything it deletes by walking the keyset of each orphan commit it is removing. Blobs and HAMT nodes that the orphan owned are reclaimed; anything shared with a reachable commit — or with a young orphan inside the `min_age` window, which protects in-flight writers — is left alone. Because candidates come only from orphan keysets and never from a namespace scan, a commit made by another writer *while the sweep is running* can never contribute a deletion candidate.

#### A lost CAS leaves garbage, and that is the safe outcome

A commit writes its blobs, HAMT nodes, chunks and metadata *before* it attempts the CAS that advances HEAD. A writer that loses that race leaves all of it behind, and nothing deletes it inline. That is deliberate, not an oversight: the loser's nodes and chunks are content-addressed, so the winning commit may legitimately share them, and deleting what a loser wrote is the same resurrection hazard that keeps `clean_orphans()` off chunks entirely.

The leftovers are ordinary orphans and are collected on the ordinary path. Commit metadata, blobs and HAMT nodes go once the commit ages past `min_age`; chunks wait for `deep_clean()`, like every other chunk. There is no retry loop and no inline cleanup, so a store under heavy CAS contention accumulates orphan commits between sweeps.

You can call it manually:

```python
v = VersionedKV(store)
cleaned = v.clean_orphans()            # default: only orphans older than 1 hour
cleaned = v.clean_orphans(min_age=0)   # delete unreachable commits immediately
```

The cleanup is safe for shared commit histories (e.g., forked branches). Blobs referenced by any reachable commit are never deleted.

#### Chunks are not reclaimed by `clean_orphans()`

Keyed on content and nothing else, `kvgit:chunk:<content_hash>` is the one class in the [storage layout](#chunked-codecs) that two unrelated commits can share by accident. Blob keys are `<commit_hash>:<key>` and HAMT nodes embed that blob pointer, so both are commit-scoped: identical data in unrelated commits still lands under distinct keys, and "in the orphan's tree" really does mean "the orphan's to delete". A chunk breaks that. An orphan's chunk and a chunk written by a commit made one microsecond ago are the *same key*, and the sweep never scanned that commit — scoping the walk to orphan keysets cannot help, because the key genuinely is in the orphan's tree.

So `clean_orphans()` deletes no chunks at all. This is correctness by construction rather than by narrowing a window: re-validating just before the delete would shrink the race to microseconds without closing it, and locking chunk deletion would close it at the cost of stalling writers.

The cost is real. Chunks are the large objects — the numpy and pandas buffers — so on a store using chunked codecs, deleted branches leave their unique buffers on disk and routine GC accumulates them. Two mitigations:

* Chunks only exist when a chunked codec is in use. A store on plain pickle has none and gives up nothing.
* `deep_clean()` reclaims them, on a quiescent store. Schedule one if you store large arrays.

#### `deep_clean()` — reclaiming commit-less artifacts

`deep_clean()` does everything `clean_orphans()` does and then scans the whole `kvgit:keyset:` and `kvgit:chunk:` namespaces, deleting anything not reachable from a live branch head or a young orphan. That scan reaches two things the incremental sweep cannot: **every chunk**, per the section above, and any HAMT node or chunk that *no* commit references — leftovers from interrupted writes, from crashes between a write and its CAS, and from stores swept by an earlier kvgit, which have no keyset to be found through.

```python
v = VersionedKV(store)
v.deep_clean()   # or kvgit.versioned.kv.deep_clean(store)
```

**`deep_clean()` is not safe against concurrent writers.** The namespace scan runs after the mark phase, so it sees — and deletes — nodes and chunks written by any commit that landed in between, including one that has since become a live branch HEAD. The result is a HEAD whose keyset cannot be loaded. Run it only on a quiescent store: no other process or thread writing, for the whole call. `min_age` does not help here; it governs commit deletion, not the namespace scan.

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
