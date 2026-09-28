# API Reference

kvgit's public API is three objects over one backend:

| Object | What it is |
|--------|------------|
| [`Repo`](#repo) | The repository: owns the `KVStore`, and everything store-wide — branches, tags, history, snapshots, garbage collection. Safe to share across threads and processes. |
| [`Worktree`](#worktree) | One branch checked out for work: a `MutableMapping[str, Any]` whose writes stay pending until `commit()`. Belongs to one thread at a time. |
| [`Snapshot`](#snapshot) | A read-only `Mapping[str, Any]` pinned to one commit. |

```python
from kvgit import Repo
from kvgit.kv.disk import Disk

with Repo(Disk("/tmp/db")) as repo:
    wt = repo.worktree("main", create=True)
    wt["k"] = "v"
    wt.commit(info={"msg": "first"})
    repo.create_tag("v1", wt.head)
    repo.snapshot(tag="v1")["k"]  # "v"
```

Commit hashes are plain `str`s; a `Commit` record describes one. Every ref argument that names a starting point — `snapshot`, `log`, `Worktree.merge` — takes exactly one of `commit=` / `branch=` / `tag=`.

---

## `kvgit.store()`

The one-line happy path: build a `Repo` over a backend and open (or create) one branch.

```python
kvgit.store(
    kind="memory",       # "memory", "disk", or "indexeddb"
    *,
    path=None,           # required for "disk"
    db_name="kvgit",     # IndexedDB database name (only for "indexeddb")
    branch="main",
    codec="pickle",
) -> Worktree
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kind` | `Literal["memory", "disk", "indexeddb"]` | `"memory"` | Backend type |
| `path` | `str \| None` | `None` | Required for `"disk"` |
| `db_name` | `str` | `"kvgit"` | IndexedDB database name. Only used with `"indexeddb"`. |
| `branch` | `str` | `"main"` | Branch to open; created at the empty root commit if missing |
| `codec` | see [Codecs](#codecs) | `"pickle"` | How values become stored bytes |

The returned worktree's `repo` is the repository; close it with `wt.repo.close()`. For any other backend, or repo-wide options, construct a [`Repo`](#repo).

---

## Repo

```python
from kvgit import Repo

repo = Repo(
    backend,                          # any KVStore
    *,
    codec="pickle",
    merge_fns=None,
    merge_prefixes=None,
    default_merge=None,
    recover_from_corrupt_head=None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backend` | `KVStore` | (required) | `Memory()`, `Disk(path)`, `Postgres(dsn)`, `IndexedDB(...)`, a `Composite`, or your own |
| `codec` | `str \| (encoder, decoder)` | `"pickle"` | See [Codecs](#codecs) |
| `merge_fns` | `dict[str, MergeFn \| MergeChoice] \| None` | `None` | Merge rules by key, for every worktree this repo opens |
| `merge_prefixes` | `dict[str, MergeFn \| MergeChoice] \| None` | `None` | Merge rules by key prefix, likewise |
| `default_merge` | `MergeFn \| MergeChoice \| None` | `None` | The rule for keys no other rule covers |
| `recover_from_corrupt_head` | `CorruptHeadRecoverer \| None` | `None` | Last-resort HEAD recovery; see [HEAD Recovery](#head-recovery) |

None of these settings is written to the store: they belong to the process that opens it. Opening reads the store's version stamp once and raises [`StorageVersionError`](#errors) for a layout this code does not read; it never writes.

A `Repo` is a context manager; `close()` closes the backend.

### Branches

| Method | Returns | Description |
|--------|---------|-------------|
| `worktree(name, *, create=False)` | `Worktree` | Check out a branch. Raises `UnknownBranchError` if missing — unless `create=True`, which creates it at the empty root commit first. `CorruptHeadError` if its HEAD is damaged beyond recovery. |
| `create_branch(name, *, at=None)` | `str` | Create a branch at commit `at` (default: the empty root commit); returns that commit. `BranchExistsError` if taken, `UnknownCommitError` if `at` is not in the store. |
| `delete_branch(name)` | `None` | Delete any branch, including the last one and one with open worktrees. Removes its HEAD and HEAD backup; its commits become collectable at the next [`gc()`](#garbage-collection). `UnknownBranchError` if missing. |
| `branches()` | `list[str]` | Every branch name, sorted. Tags are not included. |
| `has_branch(name)` | `bool` | Whether a branch exists. Never writes. |
| `head(name)` | `str` | The commit a branch points at. `UnknownBranchError`, `CorruptHeadError`. |
| `repair_head(name)` | `str \| None` | Persist a recovered HEAD; see [HEAD Recovery](#head-recovery). |

Branch names are any non-empty string without `%`, `/` included, except the reserved `refs/tags/` prefix, which every branch method refuses with `ValueError`.

A worktree on a deleted branch still reads from its `head`; its next `commit()` raises `UnknownBranchError`.

### Tags

| Method | Returns | Description |
|--------|---------|-------------|
| `create_tag(name, commit, *, info=None)` | `None` | Name a commit permanently. `TagExistsError` if taken, `UnknownCommitError` if the commit is not in the store. `info` must be JSON-serializable. |
| `delete_tag(name)` | `None` | Remove a tag. A commit only it kept alive becomes collectable at the next `gc()`. `UnknownTagError` if missing. |
| `tags()` | `dict[str, str]` | Every tag, name → commit |
| `tag_info(name)` | `TagInfo \| None` | Details for one tag, or `None` |

See [Tags](#tags) for the semantics.

### Commits and history

| Method | Returns | Description |
|--------|---------|-------------|
| `get_commit(commit)` | `Commit` | One commit's record. `UnknownCommitError` if missing. |
| `log(*, commit=None, branch=None, tag=None, limit=None, first_parent=False)` | `Iterator[Commit]` | Commits reachable from the one starting point named, newest first. Follows every parent of a merge; `first_parent=True` follows only the line made on the branch itself. |
| `diff(a, b)` | `DiffResult` | Keys added, removed and modified going from commit `a` to `b` |
| `merge_base(a, b)` | `str \| None` | Lowest common ancestor, or `None` if the two share no history. Criss-cross ties go to the smallest hash. `UnknownCommitError` if either is not in the store. |
| `snapshot(*, commit=None, branch=None, tag=None)` | `Snapshot` | Read-only view of the commit the ref resolves to now |

History ends at the empty root commit every branch starts from (`ROOT_COMMIT`, the same hash in every store).

### Garbage collection

| Method | Returns | Description |
|--------|---------|-------------|
| `gc(*, min_age=3600, deep=False, wait=True, lease_ttl=600)` | `int` | Reclaim what no branch, tag or in-flight commit reaches; returns how many commits were removed |

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_age` | `3600` | Orphans younger than this many seconds survive. Policy only: any value, `0` included, is safe beside concurrent writers. |
| `deep` | `False` | Also scan the content namespaces for anything no commit references — crash leftovers. See [`deep=True`](#deeptrue--reclaiming-commit-less-artifacts). |
| `wait` | `True` | Wait for another sweep's lease; `False` raises [`GcBusy`](#errors) instead |
| `lease_ttl` | `600` | Seconds a crashed sweep can hold writers up. See [The GC lease](#the-gc-lease). |

Nothing sweeps implicitly — deleting a branch or tag doesn't. Run `gc()` when it suits the deployment: a scheduled job for a shared Postgres store, a quiet moment in the embedding process for a local one. See [Orphan Cleanup](#orphan-cleanup).

### Other members

| Member | Description |
|--------|-------------|
| `store` | The backend `KVStore` |
| `close()` | Close the backend; also `__enter__` / `__exit__` |

---

## Worktree

A branch checked out for work: a `MutableMapping[str, Any]` bound to one branch for its whole life. Writes are pending until `commit()` — there is no separate index to stage into. Several worktrees may hold the same branch, in one process or many; a commit that finds the branch moved merges automatically.

Get one from `repo.worktree(name)` or `kvgit.store()`.

### Reading and writing

| Member | Description |
|--------|-------------|
| `wt[key]`, `get(key, default=None)` | A value, pending changes included. `wt[key]` raises `KeyError` if absent. |
| `get_many(*keys)` | `dict` of the keys that exist |
| `keys()`, `iter(wt)`, `len(wt)`, `key in wt` | Committed keys plus pending writes, minus pending deletions |
| `wt[key] = value` | A pending write |
| `del wt[key]` | A pending deletion; `KeyError` if absent. Deleting a key that exists only as a pending write just drops the write. |
| `status()` | `Status(updated, removed)`: the pending keys, as `frozenset`s. Falsy when nothing is pending. |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `repo` | `Repo` | The repository |
| `branch` | `str` | The branch this worktree holds |
| `head` | `str` | The commit this worktree is based on. The branch tip may have moved since: `repo.head(wt.branch)`. |

### Committing

#### `commit(*, keys=None, info=None, on_conflict="raise", merge_fns=None, merge_prefixes=None, default_merge=None) -> MergeResult`

Encode pending changes and write them as one atomic commit. If the branch has moved past `head`, a three-way merge is performed.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `keys` | `set[str] \| None` | `None` | Commit only these pending keys; the rest stay pending. Keys with nothing pending are ignored. |
| `info` | `dict \| None` | `None` | Metadata attached to the commit (JSON-serializable) |
| `on_conflict` | `str` | `"raise"` | `"raise"` or `"abandon"` (a falsy result, nothing written) |
| `merge_fns` | `dict[str, MergeFn \| MergeChoice] \| None` | `None` | Per-key rules for this call |
| `merge_prefixes` | `dict[str, MergeFn \| MergeChoice] \| None` | `None` | Rules by key prefix for this call |
| `default_merge` | `MergeFn \| MergeChoice \| None` | `None` | Fallback rule for this call |

Raises `MergeConflict` on a conflict in `"raise"` mode, `ConcurrencyError` if the commit keeps losing the race to publish, and `UnknownBranchError` if the branch was deleted.

```python
wt["a"] = b"alpha"
wt["b"] = b"beta"
wt.commit(keys={"a"}, info={"message": "just a"})
# "a" is committed; "b" is still pending
```

### Merging and applying changes

Each of these refuses with `ValueError` while changes are pending — commit or `discard()` first — and takes the same options:

| Option | Default | Description |
|--------|---------|-------------|
| `info` | `None` | Metadata attached to the new commit |
| `on_conflict` | `"raise"` | `"raise"` or `"abandon"` (leaves the branch untouched) |
| `merge_fns`, `merge_prefixes`, `default_merge` | `None` | Rules for this call, layered over the registered ones |
| `post_check` | `None` | `(key, merged_bytes) -> bool` over each merge-produced value; `False` files that key as conflicted. A key resolved by a [`MergeChoice`](#mergechoice) produces no new value, so nothing is checked for it. |

#### `merge(*, commit=None, branch=None, tag=None, **options) -> MergeResult`

Merge another branch, tag or commit into this worktree's branch: lowest common ancestor (criss-cross ties go to the smallest hash), three-way resolve, and a two-parent merge commit whose first parent is this branch's head.

| Case | Result |
|------|--------|
| The branch already contains theirs | `strategy="no_op"`; nothing written |
| The branch has not moved since the fork, `fast_forward=True` (default) | `strategy="fast_forward"`: HEAD moves to theirs, no commit is written, so `info` is not recorded |
| The branch has not moved since the fork, `fast_forward=False` | A merge commit, as below |
| Both sides moved | `strategy="three_way"`: a merge commit |

Finding the common ancestor costs a few batched reads however long the history: see [Generations](#generations).

#### `apply(base, target, **options) -> MergeResult`

Apply the change from commit `base` to commit `target` onto this branch, as an ordinary single-parent commit. `base` stands in for the common ancestor in a three-way merge between this branch's head and `target`, so keys the change did not touch are left as they are. A change already present is a no-op.

#### `cherry_pick(commit, **options) -> MergeResult`

`apply(first parent of commit, commit)`: the change one commit made.

#### `revert(commit, **options) -> MergeResult`

`apply(commit, first parent of commit)`: undo the change one commit made.

### Moving

| Method | Description |
|--------|-------------|
| `discard()` | Drop pending changes (`git restore .`) |
| `reset(commit)` | Move the branch to `commit` and drop pending changes (`git reset --hard`). `UnknownCommitError` if not in the store; `UnknownBranchError` if the branch was deleted, which it does not recreate. |
| `refresh()` | Move to the branch's current tip, dropping pending changes. `UnknownBranchError` if the branch was deleted. |

### Merge rules

Each rule is either a merge function — `fn(old, ours, theirs) -> merged` — or a [`MergeChoice`](#mergechoice). See [MergePolicy](#mergepolicy) for how far each kind reaches.

A merge function receives **decoded values**, not bytes, so pick one written for that level: `text_merge()` rather than `kvgit.merges.text` for a key holding `str`. A merged value is encoded with the repo's codec. See [Built-in merge functions](#built-in-merge-functions).

| Method | Description |
|--------|-------------|
| `set_merge_fn(key, fn)` | Register a rule for one key |
| `set_merge_prefix(prefix, fn)` | Register a rule for every key starting with `prefix` — keys whose names are not known when the policy is set (`"runs/"` covering `runs/<id>`) |
| `set_default_merge(fn)` | Register the rule for keys no exact-key or prefix rule covers |

**Layering.** The repository's rules (`Repo(merge_fns=...)`), then this worktree's registrations, then a call's `merge_fns=` / `merge_prefixes=` / `default_merge=`, each overriding the one before for the same key or prefix.

**Resolution order.** A key takes the most specific rule that applies: its exact key, else the longest registered prefix it starts with, else the default. Merge functions and `MergeChoice` policies compete in that one order, so a longer prefix wins whichever kind each holds. A contested key no rule covers is a conflict.

```python
wt.set_merge_prefix("runs/", MergeChoice.OURS)  # this branch owns runs/
wt.set_merge_prefix("runs/counts/", counter())  # except these
wt.set_merge_fn("runs/counts/total", theirs)    # and this one exactly
```

---

## Snapshot

A read-only `Mapping[str, Any]` pinned to one commit, from `repo.snapshot(commit=... | branch=... | tag=...)`. A branch that moves later does not move the snapshot.

| Member | Description |
|--------|-------------|
| `snap[key]`, `get`, `keys`, `items`, `values`, `in`, `len`, `iter` | The usual `Mapping` reads, decoded with the repo's codec |
| `get_many(*keys)` | `dict` of the keys that exist |
| `commit` | The commit hash |
| `raw` | The same snapshot as a read-only `Mapping[str, bytes]` of stored bytes, whatever the codec (`RawSnapshot`, with `get_many` too) |

`raw` reads bytes without decoding them — what a caller that encodes its own values, or does not trust a store's pickles, reads through. It exists only on snapshots: a worktree's pending values are not encoded until commit, so read its committed state as `repo.snapshot(commit=wt.head).raw`. Under the `"scientific"` codec the stored bytes are an envelope that refers to chunks elsewhere in the store, so they are not self-contained outside it; `"pickle"` and `"bytes"` values are.

---

## Commit and Status

```python
@dataclass(frozen=True)
class Commit:
    hash: str
    parents: tuple[str, ...]  # first parent is the branch's own line
    time: float | None
    info: dict | None         # what commit(info=...) attached
    root: str                 # keyset root: equal roots mean equal contents

@dataclass(frozen=True)
class Status:
    updated: frozenset[str]   # keys with pending writes
    removed: frozenset[str]   # keys with pending deletions
```

---

## Codecs

A repo's codec turns values into stored bytes and back. It is chosen when the repo is opened (`Repo(..., codec=)` or `kvgit.store(codec=)`):

| `codec=` | Values | Stored as |
|----------|--------|-----------|
| `"pickle"` (default) | Anything picklable | `pickle.dumps(value)` |
| `"scientific"` | Anything picklable | Pickle, with large numpy / pandas buffers stored once as [chunks](#chunked-codecs). Requires `pip install kvgit[scientific]`. |
| `"bytes"` | `bytes` only (`TypeError` otherwise, at commit) | The bytes as they are; nothing is ever decoded |
| `(encoder, decoder)` | Whatever the pair handles | `encoder(value)` |

A pair is detected by signature: a one-argument `encoder(value) -> bytes` / `decoder(bytes) -> value`, or a chunk-aware `encoder(value, sink)` / `decoder(bytes, reader)` pair as returned by `kvgit.codecs.compose(...)`. The check is "second positional parameter has no default", so `pickle.dumps` (whose `protocol` has a default) is one-argument.

### Codecs and trust

Pickle is what makes a worktree a dict of anything. But unpickling can execute code, so anyone who can write a pickle-codec store — a shared Postgres table, a disk directory — can run code in every process that reads it, `"scientific"` included. For a store more than one party can write, use `codec="bytes"`: kvgit then never decodes anything, and the embedder encodes its values itself, choosing where, if anywhere, untrusted pickles are loaded — in a sandbox, say, rather than the host process.

The codec is fixed per store in practice. Nothing records it in the store, and values written under one read back as that codec's bytes under another — `snapshot.raw` reads them under any — so switching an existing store means rewriting its values.

---

## Tags

A tag is an immutable name for a commit.

```python
repo.create_tag("v1", wt.head)
repo.create_tag("v1-reviewed", wt.head, info={"by": "ann"})
repo.tags()                          # {"v1": "a1b2c3...", "v1-reviewed": "a1b2c3..."}
repo.tag_info("v1-reviewed")         # TagInfo(name=..., commit=..., info={"by": "ann"}, ...)
repo.snapshot(tag="v1")["config"]    # read the tagged state
repo.delete_tag("v1")
```

**Tags never move.** Creating one over an existing name raises `TagExistsError`; pointing a name somewhere else is `delete_tag` then `create_tag`, so the move is visible in the calling code. Tags and branches are separate namespaces — `release` can be both, and the two are unrelated.

**Names** follow branch names: any non-empty string, `/` included, so embedders can namespace their own (`pub/v1`). The single exclusion is `%`, since tag keys are built by `%`-formatting a template. `info` must be JSON-serializable, like commit info.

**A tag is a garbage collection root**, and it is one by construction: a tag is stored as a *branch head* under the reserved name `refs/tags/<name>`, hidden from the branch API. Every sweep walks every branch head, so a tag's commit — and everything it descends from — stays alive with no tag-specific rule anywhere in the sweep. Tagging a commit and then deleting every branch that reached it leaves the commit alive until the tag goes. `delete_tag` removes the reserved head, its `__branch_head_prev__` backup and the `__tag_info__` record; the next `gc()` takes what only the tag kept alive.

A tag whose commit is not in the store (`dangling=True`) marks **nothing** — it is a head that does not resolve, and the sweep already treats those as roots pointing nowhere. That is damage rather than an ordinary state: a tag cannot be created for a commit that does not exist, and `snapshot(tag=...)` / `log(tag=...)` on one raise `UnknownCommitError`.

Tags do not get the prev-HEAD recovery tiers a branch gets. kvgit never writes a backup for a tag, because it never moves one; reading through a tag reads the reserved head key and nothing else.

To work from a tagged commit, branch from it: `repo.create_branch("hotfix", at=repo.tags()["v1"])`.

**One race, and it is narrow.** Tagging a commit that is *already* an orphan older than `min_age` can lose to a sweep running concurrently, which was free to collect that commit before the tag existed. In practice callers tag a commit they are holding — a head, or something a head descends from — and a commit a branch reaches is never a sweep candidate.

### Compatibility across kvgit versions

Storing a tag as a reserved branch head, rather than as a key kind of its own, is what makes tags safe in a store that other kvgit versions also open — **no storage version change shipped with tags**, and a tagged store still opens under versions that predate them.

The reason is that a version stamp cannot protect anything from code that already shipped. kvgit 0.3.4's anchor-free `delete_branches` opens a backend directly and sweeps without consulting the stamp at all, so a new key kind holding tag pointers would have been invisible to it and every tag-only commit would have been collected. Reachability, in every version, is "walk the branch heads" — so a tag that *is* a branch head is honoured by all of them, including ones written before tags existed.

What an older version sees is a branch named `refs/tags/<name>`. It will list it among the branches, and it can delete that branch by name or switch to it and commit — deleting or moving the tag. Both are deliberate acts naming a path that says what it is. Current code refuses reserved names everywhere in the branch API (`worktree`, `create_branch`, `delete_branch`, `head`, `repair_head`, and `branch=` in `snapshot` / `log` / `merge`) and hides them from `branches()`.

The `__tag_info__<name>` record is a separate key kind, and nothing collects it: every sweep, this version's and older ones', deletes only commit metadata keyed by commit hash, orphan-owned blobs and HAMT nodes, and — in a deep sweep — the `kvgit:keyset:` and `kvgit:chunk:` namespaces.

Storage version checks are what lock older kvgit out of a [v4 store](#storage-versions): `Repo` refuses a store stamped above what it reads when it is opened, and `gc()` checks again before removing anything. They are not what protects tags.

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

Register merge rules on the underlying worktree (or repo) with the full prefixed key:

```python
wt.set_merge_fn("myns/counter", fn)
wt.set_merge_prefix("myns/", fn)  # the whole namespace
```

---

## Types

### MergeResult

Frozen dataclass returned by `commit()`, `merge()`, `apply()`, `cherry_pick()` and `revert()`. Truthy when the call succeeded.

| Field | Type | Description |
|-------|------|-------------|
| `merged` | `bool` | Whether the commit succeeded |
| `commit` | `str \| None` | New commit hash |
| `strategy` | `str` | `"no_op"`, `"fast_forward"`, `"three_way"`, or `"apply"` (a change applied as a single-parent commit) |
| `auto_merged_keys` | `tuple[str, ...]` | Keys resolved by merge functions |
| `carried_keys` | `tuple[str, ...]` | Keys carried forward from the other side |

### TagInfo

Frozen dataclass returned by `Repo.tag_info()`.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | The tag name |
| `commit` | `str` | Commit the tag names |
| `time` | `float \| None` | When the tag was created. `None` if the tag's info record is missing. |
| `info` | `dict \| None` | Caller metadata passed to `create_tag()`, if any |
| `dangling` | `bool` | Whether the tagged commit is absent from the store — damage, not an ordinary state |

### DiffResult

Frozen dataclass returned by `Repo.diff()`.

| Field | Type | Description |
|-------|------|-------------|
| `added` | `frozenset[str]` | Keys in commit_b but not commit_a |
| `removed` | `frozenset[str]` | Keys in commit_a but not commit_b |
| `modified` | `frozenset[str]` | Keys in both with different blob hashes |

### MergeFn

Merge function type, over decoded values — whatever you stored:

```python
MergeFn = Callable[[Any | None, Any, Any], Any]
# (old_value, our_value, their_value) -> merged_value
```

### BytesMergeFn

Bytes-level merge function type. Under `codec="bytes"` the decoded values *are* bytes, so these fit as they are:

```python
BytesMergeFn = Callable[
    [bytes | None, bytes | None, bytes | None], bytes | MergeChoice
]
```

### MergeChoice

One side of a merge, named as the answer for a key.

| Member | Meaning |
|--------|---------|
| `MergeChoice.OURS` | Our side's state stands |
| `MergeChoice.THEIRS` | Their side's state stands |

Either way the merge carries that side's existing blob pointer, so no new blob is written for the key, and if that side does not have the key the merge does not either. The key counts as auto-merged.

It has two uses, with deliberately different reach:

**Returned by a merge function**, in place of bytes — the merged value for that *contested* key is that side's stored value, unchanged:

```python
def keep_the_longer(old, ours, theirs):
    return MergeChoice.OURS if len(ours) >= len(theirs) else MergeChoice.THEIRS
```

`kvgit.merges.ours` and `kvgit.merges.theirs` are the two constant cases. A key resolved this way produces no new value, so `post_check` does not run for it.

**Registered in place of a merge function** — a standing policy, described under [MergePolicy](#mergepolicy).

### MergePolicy

What a merge registration holds, at the bytes level:

```python
MergePolicy = BytesMergeFn | MergeChoice
```

The two kinds differ in how far a registration reaches:

| Registration | Applies to |
|--------------|-----------|
| merge function | Keys **both** sides changed. A change only one side made is applied untouched, as with no registration at all. |
| `MergeChoice` | **Every** key either side changed under it. Nothing is read or decoded for those keys. |

That difference is the point of the `MergeChoice` form: a merge function cannot express "this branch owns this namespace", because a key the other branch merely *added* is not contested and never reaches a function. Under `MergeChoice.OURS`, a key the other side added is dropped, a key it removed survives, a key it modified keeps our value, and a key we removed stays removed. `MergeChoice.THEIRS` is the mirror image, discarding our-only changes under the prefix.

```python
# The conversation on this branch is never overwritten by a merge, and a
# merged-in branch's new __agno__/runs/<id> keys do not come along.
wt.set_merge_prefix("__agno__/", MergeChoice.OURS)
```

---

## Built-in merge functions

They come at two levels, and the level decides where a function fits. A worktree decodes every side before calling a merge function, so it receives your values as you stored them — a `str` key arrives as `str`, not as UTF-8 bytes. The bytes-level functions read their arguments as bytes, so they fit keys whose values are `bytes`, which under `codec="bytes"` is every key.

### Value-level, from `kvgit.content_types`

Factories, for values of any type.

#### `counter() -> MergeFn`

Integer counter merge: `ours + theirs - old`. Both sides' increments are preserved.

#### `last_writer_wins() -> MergeFn`

Always returns `theirs` (the HEAD value), re-encoded as the merged value. `kvgit.merges.theirs` does the same thing without rewriting the value.

#### `text_merge(*, ours_label="ours", theirs_label="theirs", strict=False) -> MergeFn`

Marker merge for keys holding `str` or `bytes`: disjoint line changes merge cleanly, overlapping ones come back with git-style `<<<<<<<` markers under the given labels, which may not contain line breaks. `str` sides are encoded as UTF-8 for the merge and the result comes back as `str` when any side was `str`; a key whose values are `bytes` merges as bytes. With `strict=True` a conflict raises `CantMark` instead of marking, so the merge aborts rather than landing hunks. A value that is neither, and anything unmarkable — non-UTF-8 bytes, NUL bytes, inputs over the 1 MiB cap — raises `CantMark`, which the merge machinery files as an ordinary conflict.

This is the one to register for text. `kvgit.merges.text` below is the same merge over raw bytes.

### Bytes-level, from `kvgit.merges`

The difference is worth stating exactly: `ours` and `theirs` never look at the values they are handed, so they fit any key; `text` and `make_text_merge` do read their arguments as bytes, so they fit only keys whose values are `bytes`. Registered on a key holding `str`, `text` receives a `str` and raises, and the key is filed as a `MergeConflict` — use `text_merge()` above instead.

#### `text(old, ours, theirs) -> bytes`

Marker merge for line-oriented text over bytes: disjoint line changes merge cleanly, overlapping ones come back with git-style `<<<<<<<` markers. `make_text_merge(*, ours_label=, theirs_label=, strict=)` builds one with custom labels (no line breaks allowed in labels); `strict=True` raises `CantMark` instead of marking, so a true conflict aborts the commit rather than landing hunks. `text_merge_result(old, ours, theirs, *, ours_label=, theirs_label=)` runs the same merge and returns `(merged_bytes, conflicted)`, reporting reliably whether hunks were introduced even when the inputs contain marker-like lines. Anything unmarkable — non-UTF-8 bytes, NUL bytes, inputs over the 1 MiB cap — raises `CantMark`, which the merge machinery files as an ordinary conflict.

#### `ours(old, our, their) -> MergeChoice`

Take our side on a *contested* key. Returns [`MergeChoice.OURS`](#mergechoice): our stored value is kept as it stands, so no new blob is written, and if we removed the key the merge removes it. Being a merge function, it is consulted only where both sides changed the key — register `MergeChoice.OURS` itself for a policy that also governs one-sided changes.

#### `theirs(old, our, their) -> MergeChoice`

Take their side, on the same terms.

---

## Errors

Every error kvgit raises about the state of a store derives from `KvgitError`, so one `except KvgitError` catches them all. None subclasses `ValueError`: argument validation — a bad name, two refs where one is expected, an unknown `on_conflict` — raises `ValueError`, and that is a bug in the call rather than a state to handle.

| Error | Raised when |
|-------|-------------|
| `ConcurrencyError` | A commit keeps losing the race to publish (below) |
| `MergeConflict` | A merge leaves keys no rule resolves (below) |
| `UnknownBranchError` | A branch does not exist — `worktree`, `head`, `delete_branch`, `snapshot(branch=)`, a commit, `reset` or `refresh` on a deleted branch |
| `UnknownTagError` | A tag does not exist — `delete_tag`, `snapshot(tag=)`, `log(tag=)` |
| `UnknownCommitError` | A commit is not in the store — `get_commit`, `create_branch(at=)`, `create_tag`, `reset`, `diff`, `merge_base`, a dangling tag |
| `BranchExistsError` | `create_branch` over a taken name |
| `TagExistsError` | `create_tag` over a taken name |
| `CorruptHeadError` | A branch HEAD is damaged and nothing recovers it; see [HEAD Recovery](#head-recovery) |
| `StorageVersionError` | The store is stamped with a layout this code does not read; see [Storage versions](#storage-versions) |
| `GcBusy` | `gc(wait=False)` found another sweep holding an unexpired lease |

All are importable from `kvgit` (and `kvgit.errors`).

### ConcurrencyError

Raised when a commit cannot publish and the failure is not mergeable. A lost race is retried through the three-way merge path instead of raising, so this surfaces only when that cannot resolve — no common ancestor, a second lost race — while a true conflict raises `MergeConflict` in `raise` mode (or abandons, leaving the branch untouched, in `abandon` mode). `refresh()` moves the worktree to the branch tip.

### MergeConflict

Raised when a three-way merge encounters keys changed by both sides with no merge function to resolve them. Nothing is written.

Both sides writing the *same* bytes to a key is not a conflict, and needs no merge function: the merge compares the bytes and carries the key through. A key one side removed and the other modified stays a conflict.

| Attribute | Type | Description |
|-----------|------|-------------|
| `conflicting_keys` | `set[str]` | Keys that could not be resolved |
| `merge_errors` | `dict[str, Exception]` | Per-key exceptions from merge functions that raised |

### GcBusy

Raised by `gc(wait=False)` when another sweep holds an unexpired lease on the store. Nothing is swept and the holder's lease is left alone. Retry later; the lease carries an expiry, so a holder that dies without releasing it blocks nothing past that point. See [Orphan Cleanup](#orphan-cleanup).

---

## Chunked codecs

`kvgit.codecs` is an opt-in layer that externalizes large sub-values (numpy buffers, pandas DataFrames, ...) as content-addressed chunks. Equal buffers are stored once across keys, commits, and branches. `codec="scientific"` enables the numpy / pandas codec; pass a `compose(...)` pair as the codec to tune or extend it.

Install with `pip install kvgit[numpy]` or `kvgit[scientific]`.

### `compose(*codecs) -> (encoder, decoder)`

Build the encoder/decoder pair from a list of codecs. Codecs are tried in order during encoding -- the first to claim an object wins. Plain pickling handles anything no codec claims; there is no need to register a "pickle codec".

```python
from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec

repo = Repo(backend, codec=compose(NumpyCodec()))
```

Order matters when codecs claim overlapping types. Put the more specific codec first.

### `scientific() -> (encoder, decoder)`

One-liner shortcut: compose the numpy codec (which transparently handles pandas DataFrames via their pickle path). Equivalent to `compose(NumpyCodec())`. Raises `ImportError` if numpy is not installed.

```python
from kvgit.codecs import scientific

codec = scientific()
```

The same shortcut is `codec="scientific"` -- prefer that when you don't need to tune codec parameters.

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

Chunks are reclaimed like all content: a sweep marks `MetaEntry.chunks` from every live commit — reachable, in flight, or younger than `min_age` — and takes an orphan's chunks that nothing live references. See [Orphan Cleanup](#orphan-cleanup).

* **Mixed entries**: a single store can hold both plain-pickle and chunked entries; dispatch is per-entry based on whether `MetaEntry.chunks` is populated.
* **Migration**: import values from a store without chunks into a fresh chunked target (`new[k] = old[k]; new.commit()`). Equal buffers across the source's keys collapse into one chunk in the target -- you get retroactive dedup as a side effect of the copy.

### Storage versions

The `__kvgit_version__` key records the newest layout a store holds. Every layout reads the ones before it, and a store is stamped up only when something newer is actually written — opening a store never changes its stamp.

| Version | Stamped by | What it added |
|---------|------------|---------------|
| 2 | — | The HAMT keyset layout. |
| 3 | the first chunked write | `kvgit:chunk:<hash>` and `MetaEntry.chunks`. |
| 4 | the first commit written by this code | Blobs keyed by content (`kvgit:blob:<sha256>`), keyset entries without a timestamp, a commit hash over the parents, keyset root, time and info, and each commit's parents' [generations](#generations) (`__parent_gens__<commit>`). |

v4 changes how *new* objects are keyed; nothing already stored is rewritten. Every existing commit hash, branch head and tag stays valid, a keyset may hold blobs of both kinds (`<commit_hash>:<key>` from before v4, `kvgit:blob:<sha256>` after), and an untouched entry keeps the bytes it was stored as. Two consequences follow from content keys:

* **Equal bytes are one blob**, across keys, commits and branches, and the two sides of a merge agree about a key exactly when they point at the same blob. Keys that both sides changed to equal bytes still merge cleanly when one side's blob predates v4.
* **A commit hash names one root.** Because the time is part of the hash, two writers making the same change mint two commits rather than one hash over two different keysets, and every key under `__commit_*__<hash>` is written once and never rewritten.

A stamp locks older code out, deliberately: an older sweep deletes by rules that are wrong for content it did not write. `Repo` and `gc()` refuse a store stamped above what they read with `StorageVersionError`, before writing anything. kvgit releases whose admin paths predate that check refuse to open such a store, and their sweeps fail on the first v4 entry they decode.

### Generations

A commit's generation is one more than its highest parent's, and 0 for a commit with no parents. Every commit sits above all of its ancestors, so a search for the common ancestor of two commits can visit commits highest generation first and stop as soon as it has found it — rather than reading both histories to the root.

Each commit stores its parents' generations, in parent order, under `__parent_gens__<commit>`, beside `__parent_commit__<commit>`. Keeping the parents' generations with the child means one read tells the search both where a commit's parents are and where they fall, and every commit at the same generation is read in one `get_many`. Two tips that forked recently resolve in two or three reads, however long the history below the fork.

Commits written before generations were stored have no `__parent_gens__` key. A search that reaches one falls back to walking both whole histories, which is exact but reads one commit at a time. A new commit whose parent is such a commit works out its parent's generation from history once, when it is written, so the history above it is searched quickly from then on. Garbage collection removes the key with the rest of a commit's metadata.

### Limitations

* **Merge results are not chunked.** A value a merge function produces is encoded with plain `pickle.dumps` under a chunked codec (the bytes-level merge protocol has no place to land chunks). Subsequent commits that overwrite the merged key go through the chunked path normally. In single-writer use cases (e.g., one agent per branch), merges are rare and this rarely matters.
* **Decode allocates per key.** The codec is a storage-layer optimization — every read materializes a fresh, writable array (one memcpy, same cost shape as plain `pickle.loads`). It saves disk and quota; it doesn't reduce in-process RAM after read.
* **Chunk dedup is a disk/storage optimization, not an in-memory one.** While values are pending in a worktree, they're still distinct Python objects. Dedup happens at encode time.

---

## HEAD Recovery

A branch HEAD lives in one key, `__branch_head__<branch>`, and a backup of the value it held before its current one lives in `__branch_head_prev__<branch>`. If HEAD is unreadable — truncated bytes, a hash whose commit metadata is gone — head resolution tries the backup, and if that does not resolve either, reports `None`: the branch is unrecoverable.

There is a third tier below the backup, and it is **off by default**. When HEAD is unresolvable *and* the backup is missing or equally broken, the information needed is no longer in the store, so nothing kvgit can do is correct — only lucky. `recover_from_corrupt_head` is the seam for a caller who decides a guess beats losing the branch:

```python
from kvgit import recover_by_commit_scan

repo = Repo(backend, recover_from_corrupt_head=recover_by_commit_scan)
```

The recoverer is `(store, branch) -> str | None`, fired only when HEAD is present and both tiers above have failed. It applies to every resolve the repo makes — `worktree()`, `head()`, `snapshot(branch=)`, `log(branch=)`, a worktree's `refresh()` and commits, `repair_head()`. Without one, those raise `CorruptHeadError` for a branch nothing recovers.

`recover_by_commit_scan` is the implementation kvgit used to run by default, kept and exported. It scans every `__commit_root__` and returns the newest tip not claimed by a healthy branch. Know what you are buying:

* **It can serve another branch's deleted data.** "Unclaimed" is its only signal for whose commit a commit is, and a deleted branch's commits are unclaimed until `gc()` collects them. Delete a branch, damage an unrelated branch's HEAD, lose its backup, and the survivor resolves onto the deleted branch's tip.
* **It is O(store)**, per unresolved read, until `repair_head()` runs.

It is a reasonable trade on a single-branch store, or one where branches are never deleted — neither hazard is in play there.

`gc()` never uses a recoverer, even one the repo carries. GC must not decide reachability from a guess: a wrong answer marks the wrong commits live, so real garbage survives and another branch's ancestry gets pinned into this one's mark set. The sweep marks only from branches whose HEAD actually resolves.

Two further rules govern this.

**Reads never write.** Resolving a damaged branch on a read path — opening a worktree, `head`, `snapshot`, `refresh`, the mark phase of a sweep — recovers in memory and leaves the store exactly as it found it. A read-only consumer can therefore read a damaged store, two concurrent readers cannot race each other repairing the same branch to different answers, and the damage stays visible instead of being quietly papered over. The cost is that the fallback runs on each read until someone repairs it: the backup tier is a couple of extra `get` calls and is flat in store size, and an injected recoverer costs whatever it costs.

Two things persist a recovery:

* `repo.repair_head(name)` — the explicit maintenance call, and the one to reach for. It returns the commit HEAD now names, or `None` if nothing was recoverable.
* A successful write. A CAS against a damaged HEAD always fails, which would leave the branch permanently unwritable, so a writer that finds HEAD unresolvable replaces it with the recovered commit and retries once. The replacement is itself a CAS against the exact damaged bytes, so two writers racing it cannot both win, and a HEAD that merely *moved* — an ordinary lost race — is never touched.

```python
repo.repair_head("main")
```

**The backup is exactly the previous HEAD.** `__branch_head_prev__` is written in the same atomic `cas_many` that moves HEAD, conditioned on HEAD holding the value being backed up. So the backup always names the commit HEAD held immediately before its current one — never a losing writer's stale value, never a commit that was never HEAD — and a crash cannot leave one moved without the other. Deleting a branch removes both keys in one call, so a backup cannot outlive its branch either. (Stores written by older kvgit, which wrote the two separately, can still hold a backup older than one commit back, or one with no HEAD; resolution never serves a backup whose HEAD is absent.)

## Orphan Cleanup

When branches or tags are deleted, the commits they referenced may become unreachable ("orphaned"). Nothing is swept as part of a delete: `repo.gc()` is its own step. The default `min_age=3600` (1 hour) decides which unreachable commits are old enough to delete; younger orphans are taken by a later `gc()` once they age past it.

Reachability is decided by walking live branch heads. [Tags](#tags) need no special case: a tag is a branch head under a reserved name, so it keeps its commit's whole ancestry alive by being walked with everything else.

`gc()` finds everything it deletes by walking the keyset of each orphan commit it is removing, and deletes the orphan's commit metadata and whatever in its keyset — blobs, HAMT nodes, chunks — nothing live shares. "Live" is every commit the mark phase saw: reachable from a branch head or tag, in flight (below), or an orphan younger than `min_age`. Content is keyed by what it holds, so an orphan's blob may be the very key a live commit uses; what makes deleting it safe is that the sweep runs under the [GC lease](#the-gc-lease), which no commit batch can land beside, so the mark phase has seen every commit that could point at it.

`min_age` is policy alone — how long abandoned work lingers before it is taken — and any value is safe beside concurrent writers, `0` included.

```python
removed = repo.gc()            # default: only orphans older than 1 hour
removed = repo.gc(min_age=0)   # delete unreachable commits immediately
```

### A lost CAS leaves garbage, and that is the safe outcome

A commit writes its blobs, HAMT nodes, chunks and metadata *before* it publishes the HEAD that makes it reachable. A writer that loses the race to publish leaves its commit behind — kept as one side of the merge it retries through, or, if it gives up, withdrawn from flight and left as an ordinary orphan. Nothing deletes it inline: its content is shared by key with whatever else holds the same bytes, so only a sweep, which sees every live commit, can tell what is safe to take.

### `deep=True` — reclaiming commit-less artifacts

`gc(deep=True)` does everything a routine sweep does and then scans the whole `kvgit:blob:`, `kvgit:keyset:` and `kvgit:chunk:` namespaces, deleting anything not reachable from a live commit. That scan reaches what no orphan keyset points at — leftovers from crashes and interrupted writes, and from stores swept by an earlier kvgit. Run it as an occasional maintenance pass; the routine sweep is the everyday one.

```python
repo.gc(deep=True)
```

Either kind waits for another sweep's lease by default; `wait=False` raises [`GcBusy`](#gcbusy) instead.

### The GC lease

A sweep deletes whatever its mark phase did not see, so it is only correct if no commit it should have seen can appear while it runs. Every sweep establishes that itself rather than asking the caller to promise it.

The lease lives in one reserved key, `__gc_lease__`, holding `{"owner": <opaque id>, "expires": <unix time>}`. Absent, expired, or bytes that do not decode all mean "no live lease"; any of those may be taken over by a CAS against exactly those bytes, so two sweeps racing the same dead lease cannot both win.

| Step | What happens |
|------|--------------|
| Version check | Refuse a store stamped above the layout this code reads, *before* touching the lease key, so such a store comes out of the call with nothing written to it. |
| Acquire | CAS the lease key. On a live lease held by someone else, wait it out — or, with `wait=False`, raise [`GcBusy`](#gcbusy). |
| Sweep | Mark from in-flight markers, then branch heads, then young orphans; delete the rest. |
| Release | In a `finally`: CAS our own bytes to an expired record carrying our owner id. A failed release means the lease was already reclaimed by someone else, and theirs is left alone. |

Writers hold up their end inside the store's own atomicity. Every path that writes something a sweep could delete — or that makes a commit reachable — reads the lease, waits while a live one is held, and then writes with a [`cas_many`](#compare-and-swap) that expects the lease key to still hold the bytes it read:

| Path | What it writes |
|------|----------------|
| `commit()` fast-forward and merge batches (and those of `merge()` / `apply()`) | Nodes, blobs, chunks, commit metadata, in-flight marker |
| the HEAD advance that publishes one | `__branch_head__<branch>`, its backup, and the in-flight markers' removal |
| `create_branch(name, at=...)` | `__branch_head__<name>` |
| `Worktree.reset(commit)` | `__branch_head__<branch>` and its backup |
| `create_tag(name, commit)` | `__branch_head__refs/tags/<name>` and `__tag_info__<name>` |
| corrupt-HEAD repair (`repair_head()`, and the retry inside a losing publish) | `__branch_head__<branch>` |

Every acquisition writes a fresh owner id and release writes an expired record rather than deleting the key, so bytes a writer read before a sweep can never match again: a sweep that starts after the read makes the write fail, and the writer waits and tries again. The head writes check their target commit exists and write the head against the same lease record, so `create_branch(at=...)`, `reset` and `create_tag` aimed at a commit a concurrent sweep collects report it gone rather than installing a head that names nothing.

`lease_ttl` (default 600 seconds) bounds what a crashed holder costs: writers wait out a lease's remaining term and no longer. A sweep that outlives its own lease is **not** extended silently — it finishes, logs a warning at `kvgit.orphans` naming the overrun, and during that window writers are free to write. Set `lease_ttl` above the longest sweep this store has taken.

The price is that writers wait while a sweep runs, including the mark phase, which walks every live commit's keyset once; on a large store, schedule sweeps for quiet moments.

### Commits between their write batch and their HEAD advance

A commit lands in two steps: the batch that writes its nodes, blobs, chunks and metadata, and — later — the write that publishes it as a branch HEAD, or the three-way merge that folds it into one. In between it is fully written and unreachable from every head.

So the batch also writes an in-flight marker, `__inflight__<commit>`, holding the time its protection lapses (`IN_FLIGHT_TTL`, 600 seconds), and the publishing write removes it in the same atomic step that moves HEAD. A sweep reads the markers *before* the branch heads: a commit published after its marker was read is under a head read later, and one published before has no marker to miss. So a commit in flight is marked live whatever `min_age` says, and a sweep never deletes a commit whose writer is about to publish it — or whose writer is still reading it back, as the merge path does.

A writer that abandons its attempt withdraws its markers; one that dies leaves them, and they lapse, after which the next sweep takes the commit like any other orphan. The publishing write also expects the lease record — the one its latest batch landed against, so the common case costs no extra read — which keeps a publish from landing between a sweep's scans and its removals. And it expects each marker to hold the bytes its writer wrote, so a writer that took longer than `IN_FLIGHT_TTL` to publish — whose marker lapsed and may have been reaped along with its commit — gets an error instead of a head over a commit that is gone.

**A writer that bypasses kvgit is still exposed**: a process editing the backend directly, or an older kvgit (which the storage version stamp locks out), can land writes a sweep never saw.

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
| `keys` | `(prefix="") -> Iterable[str]` | All keys, or those starting with `prefix` |
| `items` | `() -> Iterable[tuple[str, bytes]]` | All key-value pairs |
| `__contains__` | `(key) -> bool` | Check existence |
| `remove` | `(key) -> None` | Remove (no-op if missing) |
| `remove_many` | `(*keys) -> None` | Batch remove |
| `cas_many` | `(expected, writes, removes=()) -> bool` | Atomic conditional batch |
| `cas` | `(key, value, expected) -> bool` | One-key compare-and-swap (provided) |
| `clear` | `() -> None` | Remove all entries |

### Compare-and-swap

`cas_many(expected, writes, removes=())` applies `writes` and `removes` atomically if and only if every key in `expected` currently holds its value — `None` meaning "must not exist" — and returns `True`; otherwise it changes nothing and returns `False`. No other writer's change may land between the check and the write. This is the foundation of kvgit's concurrency: every commit batch expects the GC lease record, and every publish expects the branch HEAD while moving it, writing its backup and removing the commit's in-flight marker in one step.

`cas(key, value, expected)` is the one-key case, provided by the base class on top of `cas_many`.

A backend implements `cas_many` with whatever multi-key atomicity it has: a lock (`Memory`), a SQLite transaction (`Disk`), one `readwrite` transaction (`IndexedDB`), or — outside kvgit — a Postgres transaction, a Redis `MULTI`/`WATCH`, a DynamoDB transactional write. `keys(prefix)` lets a backend with an ordered index answer the sweep's and the branch listing's prefix scans without reading every key; `Memory` and `Disk` filter, `IndexedDB` asks for a key range.

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

## Postgres

`KVStore` in one PostgreSQL table, for a store shared by processes on several machines. Requires `pip install kvgit[postgres]` (psycopg 3 and psycopg-pool) and PostgreSQL 11 or later.

```python
from kvgit import Repo
from kvgit.kv.postgres import Postgres

backend = Postgres("postgresql://app@db.internal/kvgit")        # table "kvgit"
backend = Postgres("dbname=kvgit", table="sessions", max_size=16)
repo = Repo(backend)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `conninfo` | `""` | libpq connection string or URL (libpq environment variables apply). Ignored when `pool` is given. |
| `table` | `"kvgit"` | Table holding this store — lowercase letters, digits, underscores. Several stores may share a database. |
| `pool` | `None` | An existing `psycopg_pool.ConnectionPool` to draw from; its connections should be in autocommit mode. |
| `create` | `True` | Create the table if it does not exist. |
| `max_size` | `8` | Size of the pool the store opens when `pool` is not given. |

Keys are a `text COLLATE "C"` primary key, so `keys(prefix)` is an index range scan; values are `bytea`. Every method but `cas_many` is one statement on an autocommit connection — one round trip, atomic on its own — and batch writes go in key order, so concurrent batches upserting overlapping keys cannot deadlock.

`cas_many` is one transaction that locks every expected key with Postgres's own row locking, so no write to it — by any method, `set` and `remove` included — can land between the check and the batch. A key expected to hold a value is read `FOR UPDATE`; a key expected absent gets a placeholder row inserted, which a concurrent insert of the same key must wait on (and if the row already exists, the check fails). Then come the writes, and the removal of placeholders the batch does not write. The statements are pipelined, so a batch costs two round trips. A write Postgres aborts to break a deadlock is retried; one that fails otherwise is rolled back.

`close()` closes the pool if the store opened it; `drop()` drops the table.

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

---

## Composite

N-tier cache composing any number of `KVStore`s, ordered fastest first. The last tier is authoritative.

```python
from kvgit.kv.composite import Composite
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory

store = Composite([Memory(), Disk("/path/to/db")])
```

| Operation | Behaviour |
|-----------|-----------|
| `get` / `get_many` / `__contains__` | Check L1, L2, ..., Ln in order; a hit at tier *i* populates L1..L(i-1). **Except for `__`-prefixed keys**, which are read from Ln only and never cached. |
| `set` / `set_many` / `remove` / `remove_many` / `clear` | Ln first (its failures propagate — durability is the contract), then the cache tiers. |
| `cas` | Delegated to Ln. On success the new value is written into the cache tiers, unless the key is `__`-prefixed. |
| `keys` / `items` | Ln only. |

Tier failures that look operational (`OSError`, network errors, a Pyodide `JsException`) are logged at WARNING and the next tier is tried. `TypeError`, `AttributeError` and `AssertionError` are treated as programming bugs and propagate.

### Cache tiers serve only immutable, content-derived keys

A key starting with `__` names a value that changes under a fixed key — a branch head, its `__branch_head_prev__` backup, the `__kvgit_version__` stamp, the `__gc_lease__` record. Cached, those let a process keep serving state another process has already replaced: the worktree takes a `ConcurrencyError` on commit, calls `refresh()`, and reads the same stale head back out of L1, forever. So they are read from the authoritative tier alone.

Everything else is keyed by its own content — `kvgit:blob:<hash>`, `kvgit:keyset:<hash>`, `kvgit:chunk:<hash>`, and `<commit>:<key>` blobs from before v4 — so the same key always holds the same bytes and a hit at any tier is the right answer. Those are what the cache tiers are for, and they are the bulk of the reads. Commit metadata (`__commit_root__`, `__parent_commit__`, `__parent_gens__`, `__commit_time__`, `__info__`) is immutable too, but it is small and `__`-prefixed, so it rides the same read-through rule rather than earning an exception.
