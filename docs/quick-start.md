# Quick Start

## Create a store

```python
import kvgit

wt = kvgit.store()
```

That's it: a `Worktree` on branch `main` of an in-memory `Repo`. For persistence, pass a backend:

```python
wt = kvgit.store(kind="disk", path="/tmp/mydb")       # SQLite-backed via diskcache
wt = kvgit.store(kind="indexeddb", db_name="myapp")    # browser-persistent via IndexedDB
```

`kind="disk"` requires `pip install kvgit[disk]`. `kind="indexeddb"` is available in Pyodide (browser) environments but has portability and durability tradeoffs — see [Browser persistence in Pyodide](pyodide.md) for the full picture and the recommended cross-browser alternative.

`kvgit.store()` is sugar. It builds a `Repo` over the backend and opens (or creates) one branch. Build the `Repo` yourself for any other backend, or to set repo-wide options:

```python
from kvgit import Repo
from kvgit.kv.postgres import Postgres

repo = Repo(Postgres("postgresql://app@db.internal/kvgit"))
wt = repo.worktree("main", create=True)
```

The PostgreSQL backend (`pip install kvgit[postgres]`) suits a store shared by processes on several machines. Any number of processes may commit to the same store, and garbage collection runs beside them — see [Postgres](api.md#postgres) in the API reference.

Three objects make up the API:

* **`Repo`** owns the backend and everything store-wide: branches, tags, history, snapshots, garbage collection. It is safe to share across threads.
* **`Worktree`** is one branch checked out for work — a dict whose writes stay pending until `commit()`. It belongs to one thread at a time.
* **`Snapshot`** is a read-only dict pinned to one commit.

---

## Basic reads and writes

A worktree is a `MutableMapping[str, Any]`. Values are pickle-serialized by default (see [Codecs and trust](#codecs-and-trust)).

```python
wt = kvgit.store()
wt["user"] = "alice"
wt["score"] = 42
wt["tags"] = ["admin", "active"]

print(wt["user"])      # "alice"
print(wt.get("nope"))  # None
print(len(wt))         # 3
print(sorted(wt))      # ["score", "tags", "user"]

del wt["tags"]
```

Nothing is persisted until you commit. There is no index to stage into: every change is pending until then, and `status()` lists them:

```python
wt.status()  # Status(updated=frozenset({'user', 'score'}), removed=frozenset())
wt.commit()
wt.status()  # falsy: nothing pending
```

---

## Commits and rollback

Every `commit()` creates an immutable snapshot, and `wt.head` names the commit the worktree is on.

```python
wt["x"] = 1
wt.commit()

first = wt.head

wt["x"] = 2
wt.commit()

print(wt["x"])  # 2

wt.reset(first)  # move the branch back (git reset --hard)
print(wt["x"])   # 1
```

Drop pending changes with `discard()`:

```python
wt["x"] = 999
wt.discard()
print(wt["x"])  # 1: back to the last committed value
```

Attach metadata to commits and read it back from the repo:

```python
repo = wt.repo
wt["x"] = 10
wt.commit(info={"author": "alice", "message": "bump x"})

repo.get_commit(wt.head).info  # {"author": "alice", "message": "bump x"}
```

Commit only specific keys — the rest stay pending:

```python
wt["a"] = 1
wt["b"] = 2
wt.commit(keys={"a"}, info={"message": "just a"})
wt.status().updated  # frozenset({"b"}): still pending
wt.discard()
```

---

## Branching

Branches are cheap. A branch is created on the repo, and a worktree checks one out:

```python
wt = kvgit.store()
repo = wt.repo
wt["shared"] = "hello"
wt.commit()

repo.create_branch("dev", at=wt.head)
dev = repo.worktree("dev")
dev["feature"] = True
dev.commit()

print("feature" in wt)   # False (main is unchanged)
print("feature" in dev)  # True

repo.branches()          # ["dev", "main"]
repo.delete_branch("dev")
```

A worktree stays on its branch for its whole life; to work on another branch, open another worktree. Without `at`, a branch starts at the empty root commit:

```python
repo.create_branch("clean")
print(len(repo.worktree("clean")))  # 0
```

---

## Reading other branches and commits

A snapshot reads any branch, tag or commit without a worktree, and stays pinned to the commit it resolved:

```python
wt = kvgit.store()
repo = wt.repo
wt["config"] = "v1"
wt.commit()

repo.create_branch("dev", at=wt.head)
dev = repo.worktree("dev")
dev["config"] = "v2"
dev.commit()

repo.snapshot(branch="dev")["config"]  # "v2"
wt["config"]                           # "v1" (still on main)
repo.snapshot(commit=wt.head)["config"]  # "v1"
```

---

## Tags

A tag is an immutable name for a commit. Unlike a branch head, it never moves:

```python
wt = kvgit.store()
repo = wt.repo
wt["config"] = "v1"
wt.commit()

repo.create_tag("release-1", wt.head, info={"by": "ann"})

wt["config"] = "v2"
wt.commit()

wt["config"]                                # "v2" (the branch moved on)
repo.snapshot(tag="release-1")["config"]    # "v1" (the tag did not)

repo.tags()                                 # {"release-1": "a1b2c3..."}
repo.tag_info("release-1").info             # {"by": "ann"}
repo.delete_tag("release-1")
```

Creating a tag under a name already taken raises `TagExistsError` — moving a tag is `delete_tag` then `create_tag`, spelled out. Tags and branches are separate namespaces, so the same name can be both.

A tag is also a garbage collection root: the tagged commit and everything it descends from survive [garbage collection](#garbage-collection) for as long as the tag exists, even after every branch that reached them is gone. That is what makes a tag a safe place to leave a release, an experiment worth keeping, or a checkpoint an agent may want to come back to.

To work from a tagged commit, branch from it: `repo.create_branch("hotfix", at=repo.tags()["release-1"])`.

Under the hood a tag is a branch head under the reserved name `refs/tags/<name>`, hidden from `branches()` and refused by the branch API. That is deliberate: reachability is decided by walking branch heads in *every* kvgit version, so a tagged commit is kept alive even by versions written before tags existed. See [Compatibility across kvgit versions](api.md#compatibility-across-kvgit-versions).

---

## Concurrent commits merge

Several worktrees may hold the same branch — in one process or many. When a commit finds the branch has moved since the worktree's `head`, kvgit performs a three-way merge automatically:

```python
wt = kvgit.store()
repo = wt.repo
wt["a"] = 1
wt["b"] = 1
wt.commit()

w1 = repo.worktree("main")
w2 = repo.worktree("main")

w1["a"] = 2         # w1 changes "a"
w1.commit()

w2["b"] = 2         # w2 changes "b"
w2.commit()         # auto-merges: keeps w1's "a" and adds w2's "b"

print(w2["a"])      # 2 (from w1)
print(w2["b"])      # 2 (from w2)
```

If both sides change the same key differently, you get a `MergeConflict`, and nothing is written:

```python
from kvgit import MergeConflict

w1["x"] = "from_w1"
w1.commit()

w2["x"] = "from_w2"
try:
    w2.commit()
except MergeConflict as e:
    print(e.conflicting_keys)  # {"x"}
w2.refresh()  # drop the pending change and move to the branch tip
```

---

## Merging branches

`merge()` brings another branch, tag or commit into the worktree's branch with a two-parent merge commit. It refuses while changes are pending. When the worktree's branch has not moved since the two forked, it fast-forwards instead, as git does — the branch simply moves to theirs — and `fast_forward=False` writes a merge commit regardless. Merging something the branch already contains is a no-op.

```python
wt = kvgit.store()
repo = wt.repo
wt["a"] = 1
wt.commit()

repo.create_branch("feature", at=wt.head)
feature = repo.worktree("feature")
feature["b"] = 2
feature.commit()

wt["c"] = 3
wt.commit()

wt.merge(branch="feature")
print(wt["b"], wt["c"])  # 2 3
```

`cherry_pick(c)` applies the change one commit made, `revert(c)` undoes it, and `apply(base, target)` applies the change between any two commits — each as an ordinary single-parent commit on the worktree's branch:

```python
feature["d"] = 4
feature.commit()
wt.cherry_pick(feature.head)  # just that commit's change
print(wt["d"])                # 4
wt.revert(wt.head)            # and undo it again
print(wt.get("d"))            # None
```

---

## Merge functions

Register a merge function to resolve conflicts automatically.

```python
from kvgit import counter, last_writer_wins

wt = kvgit.store()
repo = wt.repo
wt["hits"] = 100
wt.commit()

w1 = repo.worktree("main")
w2 = repo.worktree("main")

# counter() merges as: ours + theirs - old
w2.set_merge_fn("hits", counter())

w1["hits"] = 115     # +15
w1.commit()

w2["hits"] = 120     # +20
w2.commit()

print(w2["hits"])    # 135 (115 + 120 - 100)
```

`last_writer_wins()` always takes the HEAD value, and `text_merge()`
resolves line-oriented text -- disjoint line edits merge cleanly,
overlapping ones come back with git-style `<<<<<<<` markers:

```python
from kvgit import text_merge

wt.set_merge_fn("notes", text_merge())
```

Custom merge functions work too:

```python
def merge_lists(old, ours, theirs):
    """Union of both sides' changes."""
    base = set(old or [])
    return sorted(base | set(ours or []) | set(theirs or []))

wt.set_merge_fn("tags", merge_lists)
```

A merge function receives `(old_value, our_value, their_value)` and returns the merged value. Any argument can be `None` (key absent on that side).

Cover a whole family of keys with one registration, for names you cannot
know in advance:

```python
from kvgit import MergeChoice

wt.set_merge_prefix("counts/", counter())        # every key under counts/
wt.set_merge_prefix("notes/", MergeChoice.OURS)  # this branch owns notes/
```

Registering a `MergeChoice` rather than a function is a policy, not a
conflict resolver: it hands that side *every* key either side changed
under the prefix, so `OURS` also drops a key the other side added and
keeps one the other side removed. A merge function -- including
`kvgit.merges.ours` -- only ever sees keys both sides changed.

Set a default fallback for any key without a registered function:

```python
wt.set_default_merge(last_writer_wins())
```

A contested key takes the most specific registration that applies: its
exact key, else the longest matching prefix, else the default.

Rules registered on a worktree apply to it alone. For rules every worktree should share, set them once on the repo; a worktree's registrations layer over the repo's, and a call's `merge_fns=` / `merge_prefixes=` / `default_merge=` over both:

```python
from kvgit import Repo
from kvgit.kv.memory import Memory

shared_rules = Repo(Memory(), merge_prefixes={"counts/": counter()})
```

---

## History and diffs

Walk the commit chain, newest first:

```python
history = list(repo.log(branch="main", limit=10))
for c in history:
    print(c.hash, c.time, c.info)
```

`log` follows every parent of a merge commit; `first_parent=True` follows only the branch's own line. Compare two commits:

```python
d = repo.diff(history[-1].hash, history[0].hash)  # oldest to newest
print(d.added)     # frozenset of added keys
print(d.removed)   # frozenset of removed keys
print(d.modified)  # frozenset of modified keys
```

---

## Namespaces

`Namespaced` gives you an isolated key-prefixed view over a shared store. Useful for multi-agent setups where each agent owns a slice of state.

```python
from kvgit import Namespaced

wt = kvgit.store()
agent = Namespaced(wt, "agent")
config = Namespaced(wt, "config")

agent["state"] = "running"
config["timeout"] = 30

agent["state"]         # "running"
config.get("state")    # None (isolated)
wt.get("agent/state")  # "running" (prefixed in the worktree)

wt.commit()            # one commit covers all namespaces
```

Nesting works:

```python
worker = Namespaced(agent, "worker")
worker["task"] = "fetch"
wt.get("agent/worker/task")  # "fetch"
```

---

## Garbage collection

Committing creates history. When a branch is deleted, the commits it referenced may become unreachable -- no branch HEAD and no [tag](#tags) can walk to them anymore -- but they still occupy storage along with any blobs, keyset nodes and chunks they uniquely owned. `repo.gc()` reclaims them. This is reachability-based collection, not LRU eviction: nothing is ever removed just because it's old or infrequently accessed.

Deleting a branch does not sweep; collection is its own step, run when it suits you:

```python
wt = kvgit.store(kind="disk", path="/tmp/mydb")
repo = wt.repo
repo.create_branch("experiment", at=wt.head)
# ... work on the branch ...
repo.delete_branch("experiment")

repo.gc()           # default: skip orphans younger than 1 hour
repo.gc(min_age=0)  # sweep unreachable commits immediately
```

For a long-lived store, run `gc()` periodically — from a scheduled job for a shared Postgres store, or at a quiet moment in an embedding process for a local one. It is safe to run while other writers are committing, at any `min_age` -- `0` included. `min_age` is purely your policy on how long abandoned work lingers before it is taken. Content referenced by any live commit is never deleted.

### How a sweep runs beside writers

Blobs, keyset nodes and chunks are keyed by what they hold, so an orphan's blob and a blob a *brand-new* commit just wrote are literally the same key whenever the bytes match. A sweep may take an orphan's content only if it has seen every commit that could point at it, and kvgit makes that true rather than asking you to quiesce the store:

* Every sweep takes a lease under the reserved key `__gc_lease__`, and every commit batch is written with a `cas_many` that expects the lease record its writer read. No batch lands while a sweep runs; a writer that tries waits, and lands after.
* Every batch carries an `__inflight__` marker for its commit, removed by the write that publishes it. A sweep marks from those markers as well as from branch heads, so a commit written but not yet published is live, not garbage.

Writers do wait while a sweep runs, so on a large store, sweep at quiet moments. By default `gc()` waits for another sweep's lease; `wait=False` raises `kvgit.GcBusy` instead:

```python
try:
    repo.gc(wait=False)
except kvgit.GcBusy:
    pass   # someone else is already sweeping
```

### `deep=True` -- the maintenance pass

`gc(deep=True)` does everything a routine sweep does, then scans the `kvgit:blob:`, `kvgit:keyset:` and `kvgit:chunk:` namespaces directly for content *no* commit references -- left behind by a crash or an interrupted write, or by a store swept by an earlier kvgit -- since those have no orphan to be found through. Run it occasionally.

```python
repo.gc(deep=True)
```

`lease_ttl` (default 600 seconds) bounds the damage from a sweep that crashes holding the lease: writers wait out its remaining term and no longer. Raise it above the longest sweep this store has taken -- an overrun is not extended silently, it logs a warning and leaves writers free during the overrun.

A writer that bypasses kvgit is still exposed -- a process editing the backend directly. Older kvgit is locked out of a v4 store by its version stamp.

A commit that loses a race to publish leaves its commit behind too -- nothing deletes it inline, because the winner may share its content. It is an ordinary orphan and the ordinary sweep collects it.

See [Orphan Cleanup in the API reference](api.md#orphan-cleanup) for details.

## Recovering a damaged HEAD

If a branch's HEAD key is unreadable, kvgit falls back to a backup of the previous HEAD. Reads use that fallback but never write it back: opening a worktree on a damaged store gets you the recovered state without mutating anything, which is what a read-only consumer needs and what keeps two readers from racing each other.

If the backup is gone too, the branch is unrecoverable, and opening a worktree on it raises `CorruptHeadError`. At that point the store no longer holds the answer, so any recovery is a guess. You can opt into one for the whole repo:

```python
from kvgit import recover_by_commit_scan

repo = Repo(backend, recover_from_corrupt_head=recover_by_commit_scan)
```

`recover_by_commit_scan` is what kvgit ran by default through v0.3.3: the newest commit no healthy branch claims. It is a heuristic, and on a store where branches get deleted it can hand one branch another branch's deleted data — a deleted branch's commits are unclaimed until GC collects them. Fine on a single-branch store; think twice elsewhere. See [HEAD Recovery in the API reference](api.md#head-recovery).

Making the recovery durable is a separate, explicit step:

```python
repo.repair_head("main")   # writes the recovered commit back to HEAD
```

Writes heal it on their own, since a CAS against a damaged HEAD would otherwise fail forever. So in practice a damaged branch that anyone still commits to fixes itself, and `repair_head()` is for the read-only case and for maintenance.

See [HEAD Recovery in the API reference](api.md#head-recovery) for the full contract.

---

## Codecs and trust

A repo's codec turns values into stored bytes, and is set when the repo is opened:

| `codec=` | Values | Stored as |
|---|---|---|
| `"pickle"` (default) | anything picklable | `pickle.dumps(value)` |
| `"scientific"` | anything picklable | pickle, with large numpy / pandas buffers stored once as [chunks](#storing-scientific-data-efficiently-chunked-codecs) |
| `"bytes"` | `bytes` only | the bytes themselves; kvgit never decodes anything |
| `(encoder, decoder)` | whatever your pair handles | `encoder(value)` |

Pickle is what makes a worktree a dict of anything. But unpickling can execute code, so anyone who can write a pickle-codec store -- a shared Postgres table, a disk directory -- can run code in every process that reads it. For a store more than one party can write, use `codec="bytes"` and encode values yourself, choosing where (if anywhere) untrusted pickles are loaded:

```python
import json

wt = kvgit.store(codec="bytes")
wt["config"] = json.dumps({"retries": 3}).encode()
wt.commit()
json.loads(wt["config"])  # {"retries": 3}
```

A pair of your own works the same way — JSON throughout, say:

```python
as_json = kvgit.store(codec=(lambda v: json.dumps(v).encode(), json.loads))
```

Whatever the codec, a snapshot's `.raw` view reads the stored bytes without decoding them:

```python
snap = wt.repo.snapshot(commit=wt.head)
snap.raw["config"]  # b'{"retries": 3}'
```

The codec is fixed per store in practice: values written under one read back as that codec's bytes under another, so switching an existing store means rewriting its values. Merge functions see decoded values, and a merged value is encoded with the repo's codec.

---

## Storing scientific data efficiently (chunked codecs)

A common pain point: an agent or notebook holds a 10 MB DataFrame and slices it into half a dozen derived variables. With plain pickle, every commit re-serializes each derived value in full -- 10 MB times the number of slices, every commit. The store fills up fast, especially against IndexedDB or other quota-bound backends.

The `kvgit.codecs` package solves this by externalizing large numpy buffers as content-addressed chunks. Equal buffers (across keys, across commits, across branches) are stored exactly once.

```python
import numpy as np
import kvgit

wt = kvgit.store(codec="scientific")  # numpy + pandas

big = np.arange(1_000_000, dtype="float64")  # ~8 MB

wt["full"]  = big
wt["head"]  = big[:100_000]
wt["tail"]  = big[-100_000:]
wt["copy"]  = np.arange(1_000_000, dtype="float64")  # different ndarray, same content
wt.commit()
# Storage cost: ~8 MB, not ~32 MB. All four keys reference one chunk.
```

The `codec="scientific"` shortcut is equivalent to building the encoder/decoder pair by hand — useful when you want to tune codec parameters:

```python
from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec

codec = compose(NumpyCodec(min_bytes=4096))  # higher threshold
wt = kvgit.store(codec=codec)
```

Pandas DataFrames work without a separate codec -- their underlying block ndarrays are visible to the numpy codec during pickling:

```python
import pandas as pd

df = pd.DataFrame({"x": np.arange(100_000), "y": np.random.normal(size=100_000)})
wt["df"]    = df
wt["head"]  = df.iloc[:1000]      # row-slice view
wt["tail"]  = df.iloc[-1000:]
wt.commit()
# Block buffers shared across all three.
```

### Migrating an existing store

Current kvgit reads every older store in place; see [Storage versions](api.md#storage-versions). Chunked codecs need no migration either -- a store takes chunked writes as it is. To reclaim disk from arrays an older store pickled once per key, import its values into a fresh chunked store -- the dedup happens during the copy:

```python
old = kvgit.store(kind="disk", path="/old/store")  # plain pickle
new = kvgit.store(kind="disk", path="/new/store", codec=codec)
for k in old.keys():
    new[k] = old[k]
new.commit()
```

If `old` happened to hold five separate copies of the same array under five keys, `new` ends up with one chunk and five small manifests.

### What's chunked, what isn't

| Type | Behavior |
|------|----------|
| `numpy.ndarray` (>= 1 KiB) | Externalized; views dedup against parent buffer |
| `numpy.ndarray` (< 1 KiB) | Inlined into the value blob (chunk overhead would exceed savings) |
| Object-dtype ndarrays | Pass through to pickle (their elements may still be intercepted by other codecs) |
| `pandas.DataFrame` / `Series` | Block ndarrays externalize via the numpy codec |
| Containers (`dict`, `list`, dataclass) holding ndarrays | The container pickles normally; nested arrays still externalize |
| Anything else | Plain pickle, unchanged |

Materialized arrays are independent, writable copies — same semantics as a value coming back from `pickle.loads`. Mutating one key's array doesn't affect any other key. The dedup happens at the storage layer; reads always allocate a fresh array.

### Custom codecs

`Codec` is a small protocol. Provide your own for non-numpy types:

```python
from kvgit.codecs import compose

class MyCodec:
    name = "my"          # short tag, must be unique within compose()

    def try_externalize(self, obj, sink):
        if not isinstance(obj, MyBigThing):
            return None
        ref = sink.put(obj.payload)
        return {"ref": ref, "label": obj.label}

    def materialize(self, token, reader):
        return MyBigThing(label=token["label"], payload=reader.get(token["ref"]))

codec = compose(MyCodec(), NumpyCodec())  # order = priority; pass as codec=
```

See [the API reference](api.md#chunked-codecs) for the full protocol and the storage layout.

### Reclaiming chunk space

Chunks are collected like all other content: `repo.gc()` takes an orphan's chunks that nothing live references. `gc(deep=True)` also reaches chunks left behind by a crash, or by a store swept by an earlier kvgit. See [Garbage collection](#garbage-collection).

---

## Concurrency

Multiple writers sharing the same backend coordinate via optimistic concurrency (compare-and-swap). If a CAS fails during commit, kvgit retries with a three-way merge (see [Concurrent commits merge](#concurrent-commits-merge)). If it keeps losing the race, you get a `ConcurrencyError`:

```python
from kvgit import ConcurrencyError

try:
    wt.commit()
except ConcurrencyError:
    wt.refresh()  # move to the branch tip, dropping pending changes
    # re-apply changes and retry
```

The `Disk` backend is safe across multiple processes (backed by SQLite file locking), and `Postgres` across machines. Share one `Repo` between threads; give each thread its own worktree.

---

## Checking commit results

`commit()` returns a `MergeResult` with details about what happened:

```python
result = wt.commit()

result.merged            # True if commit succeeded
result.commit            # new commit hash
result.strategy          # "no_op", "fast_forward", "three_way", or "apply"
result.auto_merged_keys  # keys a merge rule decided
result.carried_keys      # keys the other side changed, taken as they were
```

Use `on_conflict="abandon"` to get a falsy result instead of an exception:

```python
result = wt.commit(on_conflict="abandon")
if not result:
    print("commit failed, no exception raised")
```
