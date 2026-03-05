# Quick Start

## Create a store

```python
import kvgit

s = kvgit.store()
```

That's it. You have a versioned key-value store backed by in-memory storage. For persistence, pass a backend:

```python
s = kvgit.store(kind="disk", path="/tmp/mydb")       # SQLite-backed via diskcache
s = kvgit.store(kind="git", path="/tmp/myrepo")       # real git repo via GitPython
```

`kind="disk"` requires `pip install kvgit[disk]`. `kind="git"` requires `pip install kvgit[git]` and git on PATH.

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

## Garbage collection

Bound the store's size with high/low water marks. When total serialized value size exceeds the threshold, the coldest (least-recently-accessed) keys are dropped automatically.

```python
s = kvgit.store(high_water_bytes=10_000)

# GC runs automatically on commit() when above high water
s["big"] = "x" * 5000
s.commit()
```

Customize the low-water target (defaults to 80% of high water):

```python
s = kvgit.store(high_water_bytes=10_000, low_water_bytes=5_000)
```

Keys starting with `__` are protected from GC by default. Customize with `is_protected`:

```python
s = kvgit.store(
    high_water_bytes=10_000,
    is_protected=lambda key: key.startswith("config/"),
)
```

GC is not supported with the git backend.

---

## Custom serialization

The default encoder/decoder is pickle. Switch to JSON for human-readable storage (especially useful with the git backend):

```python
import json

s = kvgit.store(
    encoder=lambda v: json.dumps(v).encode(),
    decoder=lambda b: json.loads(b),
)
```

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
