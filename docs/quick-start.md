# Quick Start

## Create a store

```python
import kvgit

s = kvgit.store()
```

That's it. You have a versioned key-value store backed by in-memory storage. For persistence, pass a backend:

```python
s = kvgit.store(kind="disk", path="/tmp/mydb")       # SQLite-backed via diskcache
s = kvgit.store(kind="indexeddb", db_name="myapp")    # browser-persistent via IndexedDB
```

`kind="disk"` requires `pip install kvgit[disk]`. `kind="indexeddb"` is available automatically in Pyodide (browser) environments.

---

## Basic reads and writes

A store is a `MutableMapping[str, Any]`. Values are pickle-serialized by default.

```python
s["user"] = "alice"
s["score"] = 42
s["tags"] = ["admin", "active"]

print(s["user"])     # "alice"
print(s.get("nope")) # None
print(len(s))        # 3
print(list(s.keys()))# ["user", "score", "tags"]

del s["tags"]
```

Nothing is persisted until you commit:

```python
s.commit()
```

---

## Commits and rollback

Every `commit()` creates an immutable snapshot.

```python
s["x"] = 1
s.commit()

first = s.current_commit

s["x"] = 2
s.commit()

print(s["x"])  # 2

s.reset_to(first)
print(s["x"])  # 1
```

Discard uncommitted changes with `reset()`:

```python
s["x"] = 999
s.reset()
print(s["x"])  # back to last committed value
```

Attach metadata to commits and retrieve it later:

```python
s["x"] = 10
s.commit(info={"author": "alice", "message": "bump x"})

s.versioned.commit_info()  # {"author": "alice", "message": "bump x"}
```

Commit only specific keys — the rest stay staged:

```python
s["a"] = 1
s["b"] = 2
s.commit(keys={"a"}, info={"message": "just a"})
# "a" is committed; "b" remains staged for a future commit
```

---

## Branching

Branches are cheap. Each branch has its own HEAD and commits independently.

```python
s = kvgit.store()
s["shared"] = "hello"
s.commit()

dev = s.create_branch("dev")
dev["feature"] = True
dev.commit()

print("feature" in s)    # False (main is unchanged)
print("feature" in dev)  # True

# Switch in-place
s.switch_branch("dev")
print(s["feature"])       # True

# List and delete branches
s.list_branches()         # ["dev", "main"]
s.switch_branch("main")
s.delete_branch("dev")
```

Fork from a specific commit with `at`:

```python
clean = s.create_branch("clean", at=s.initial_commit)
print(len(clean))  # 0 (forked from empty root)
```

---

## Merging

When you commit on a branch that's behind HEAD (because another branch or writer committed first), kvgit performs a three-way merge automatically.

```python
s = kvgit.store()
s["a"] = 1
s["b"] = 1
s.commit()

# Fork two branches from the same point
b1 = s.create_branch("b1")
b2 = s.create_branch("b2")

b1["a"] = 2         # b1 changes "a"
b1.commit()

b2["b"] = 2          # b2 changes "b"
b2.commit()          # auto-merges: takes b1's "a" and b2's "b"

print(b2["a"])       # 2 (from b1)
print(b2["b"])       # 2 (from b2)
```

If both sides change the same key differently, you get a `MergeConflict`:

```python
from kvgit import MergeConflict

b1["x"] = "from_b1"
b1.commit()

b2["x"] = "from_b2"
try:
    b2.commit()
except MergeConflict as e:
    print(e.conflicting_keys)  # {"x"}
```

---

## Merge functions

Register a merge function to resolve conflicts automatically.

```python
from kvgit import counter, last_writer_wins

s = kvgit.store()
s["hits"] = 100
s.commit()

b1 = s.create_branch("b1")
b2 = s.create_branch("b2")

# counter() merges as: ours + theirs - old
b2.set_merge_fn("hits", counter())

b1["hits"] = 115     # +15
b1.commit()

b2["hits"] = 120     # +20
b2.commit()

print(b2["hits"])    # 135 (115 + 120 - 100)
```

`last_writer_wins()` always takes the HEAD value. Custom merge functions work too:

```python
def merge_lists(old, ours, theirs):
    """Union of both sides' changes."""
    base = set(old or [])
    return sorted(base | set(ours or []) | set(theirs or []))

s.set_merge_fn("tags", merge_lists)
```

A merge function receives `(old_value, our_value, their_value)` and returns the merged value. Any argument can be `None` (key absent on that side).

Set a default fallback for any key without a registered function:

```python
s.set_default_merge(last_writer_wins())
```

---

## Peeking across branches

Read a key from another branch without switching:

```python
s = kvgit.store()
s["config"] = "v1"
s.commit()

dev = s.create_branch("dev")
dev["config"] = "v2"
dev.commit()

s.peek("config", branch="dev")  # "v2"
s["config"]                      # "v1" (still on main)
```

---

## History and diffs

Walk the commit chain:

```python
for commit_hash in s.history():
    print(commit_hash)
```

Compare two commits:

```python
d = s.versioned.diff(old_hash, new_hash)
print(d.added)     # frozenset of added keys
print(d.removed)   # frozenset of removed keys
print(d.modified)  # frozenset of modified keys
```

---

## Namespaces

`Namespaced` gives you an isolated key-prefixed view over a shared store. Useful for multi-agent setups where each agent owns a slice of state.

```python
from kvgit import Namespaced

s = kvgit.store()
agent = Namespaced(s, "agent")
config = Namespaced(s, "config")

agent["state"] = "running"
config["timeout"] = 30

agent["state"]         # "running"
config.get("state")    # None (isolated)
s.get("agent/state")   # "running" (prefixed in base store)

s.commit()             # one commit covers all namespaces
```

Nesting works:

```python
worker = Namespaced(agent, "worker")
worker["task"] = "fetch"
s.get("agent/worker/task")  # "fetch"
```

---

## Cleaning up unreachable commits

Committing creates history. When a branch is deleted, the commits it referenced may become unreachable -- no branch HEAD can walk to them anymore -- but they still occupy storage along with any blobs and keyset nodes they uniquely owned. kvgit reclaims this with reachability-based garbage collection via `clean_orphans()`. This is not LRU eviction: nothing is ever removed just because it's old or infrequently accessed. Only truly unreachable commits are swept.

`delete_branch()` calls `clean_orphans()` automatically, so in the common case you don't need to think about it:

```python
s = kvgit.store(kind="disk", path="/tmp/mydb")
worker = s.create_branch("experiment")
# ... work on the branch ...
s.delete_branch("experiment")   # calls clean_orphans() for you
```

For periodic background cleanup -- or after a batch of deletions -- call it directly via the underlying `Versioned`:

```python
s.versioned.clean_orphans()           # default: skip commits younger than 1 hour
s.versioned.clean_orphans(min_age=0)  # immediate (only safe without concurrent writers)
```

The default `min_age=3600` guards against concurrent writers: a commit created by another thread while the sweep is running could look like an orphan if you don't give it a chance to settle. Leave the default unless you're certain no one else is committing.

Cleanup is safe for shared commit histories -- blobs and keyset nodes referenced by any reachable commit are never deleted. See [Orphan Cleanup in the API reference](api.md#orphan-cleanup) for details.

---

## Custom serialization

The default encoder/decoder is pickle. Switch to JSON for human-readable storage:

```python
import json

s = kvgit.store(
    encoder=lambda v: json.dumps(v).encode(),
    decoder=lambda b: json.loads(b),
)
```

---

## Storing scientific data efficiently (chunked codecs)

A common pain point: an agent or notebook holds a 10 MB DataFrame and slices it into half a dozen derived variables. With plain pickle, every commit re-serializes each derived value in full -- 10 MB times the number of slices, every commit. The store fills up fast, especially against IndexedDB or other quota-bound backends.

The `kvgit.codecs` package solves this by externalizing large numpy buffers as content-addressed chunks. Equal buffers (across keys, across commits, across branches) are stored exactly once.

```python
import numpy as np
import kvgit

s = kvgit.store(codecs="scientific")  # numpy + pandas

big = np.arange(1_000_000, dtype="float64")  # ~8 MB

s["full"]  = big
s["head"]  = big[:100_000]
s["tail"]  = big[-100_000:]
s["copy"]  = np.arange(1_000_000, dtype="float64")  # different ndarray, same content
s.commit()
# Storage cost: ~8 MB, not ~32 MB. All four keys reference one chunk.
```

The `codecs="scientific"` shortcut is equivalent to building the encoder/decoder pair by hand — useful when you want to tune codec parameters:

```python
from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec

encoder, decoder = compose(NumpyCodec(min_bytes=4096))  # higher threshold
s = kvgit.store(encoder=encoder, decoder=decoder)
```

Pandas DataFrames work without a separate codec -- their underlying block ndarrays are visible to the numpy codec during pickling:

```python
import pandas as pd

df = pd.DataFrame({"x": np.arange(100_000), "y": np.random.normal(size=100_000)})
s["df"]    = df
s["head"]  = df.iloc[:1000]      # row-slice view
s["tail"]  = df.iloc[-1000:]
s.commit()
# Block buffers shared across all three.
```

### Migrating an existing store

A v3 store (one that has ever held a chunked write) is a strict superset of a v2 store. Opening a v2 store with chunked-codec code is allowed; the upgrade only happens on the first chunked write. To migrate an existing v2 store and reclaim disk from accidental duplicates, just import its values into a fresh chunked store -- the dedup happens during the copy:

```python
old = kvgit.store(kind="disk", path="/old/v2/store")        # plain pickle, v2
new = kvgit.store(
    kind="disk", path="/new/v3/store",
    encoder=encoder, decoder=decoder,
)
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

encoder, decoder = compose(MyCodec(), NumpyCodec())  # order = priority
```

See [the API reference](api.md#chunked-codecs) for the full protocol and the storage layout.

---

## Concurrency

Multiple writers sharing the same backend coordinate via optimistic concurrency (compare-and-swap). If a CAS fails during commit, kvgit retries with a three-way merge. If the merge itself can't resolve, you get a `ConcurrencyError`:

```python
from kvgit import ConcurrencyError

try:
    s.commit()
except ConcurrencyError:
    s.refresh()  # reload from HEAD
    # re-apply changes and retry
```

The `Disk` backend is safe across multiple processes (backed by SQLite file locking).

---

## Checking commit results

`commit()` returns a `MergeResult` with details about what happened:

```python
result = s.commit()

result.merged            # True if commit succeeded
result.commit            # new commit hash
result.strategy          # "no_op", "fast_forward", or "three_way"
result.auto_merged_keys  # keys resolved by merge functions
result.carried_keys      # keys carried from the other side
```

Use `on_conflict="abandon"` to get a falsy result instead of an exception:

```python
result = s.commit(on_conflict="abandon")
if not result:
    print("commit failed, no exception raised")
```
