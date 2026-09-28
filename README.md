# kvgit 🔀

Git-style versioning for your data. Commits, branches, and merges -- backed by a dict-like `MutableMapping`.

| Features | Description |
|---|---|
| **Dict interface** | `MutableMapping[str, Any]` -- reads and writes work like a dict |
| **Commits** | Immutable, content-addressable snapshots with rollback |
| **Branches** | Cheap forks with CAS-based optimistic concurrency |
| **Tags** | Immutable names for commits; a tagged commit outlives every branch that reached it, in every kvgit version |
| **Three-way merge** | Auto-merges non-overlapping changes; pluggable merge fns for conflicts |
| **Pluggable backends** | In-memory, disk (diskcache), PostgreSQL, IndexedDB (Pyodide/browser), or bring your own `KVStore` -- any store with an atomic conditional batch |
| **Concurrent GC** | Garbage collection runs beside live writers, from any process sharing the store |
| **Content-addressed storage** | Equal values are stored once across keys, commits, and branches |
| **Chunked codecs** | Optional dedup *inside* large numpy arrays and pandas DataFrames -- equal buffers (slices included) stored once |

## Install

```bash
pip install kvgit              # in-memory only
pip install kvgit[disk]        # adds disk backend via diskcache
pip install kvgit[postgres]    # adds PostgreSQL backend via psycopg
pip install kvgit[scientific]  # adds chunked codecs for numpy / pandas
# IndexedDB backend is available automatically in Pyodide (browser) environments
```

## Quick example

```python
import kvgit

main = kvgit.open()  # a Worktree on branch "main" of an in-memory Repo
repo = main.repo

main["user"] = "alice"
main["score"] = 0
main.commit()

# Branch and diverge
repo.create_branch("dev", at=main.head)
dev = repo.worktree("dev")
dev["score"] = 999
dev.commit()

print(main["score"])  # 0   (main unchanged)
print(dev["score"])   # 999 (dev branch)

# Tag a commit by name -- immutable, and safe from garbage collection
repo.create_tag("v1", main.head)
print(repo.snapshot(tag="v1")["score"])  # 0
```

A `Repo` owns the backend and everything store-wide: branches, tags,
history, snapshots, garbage collection. A `Worktree` is one branch
checked out for work -- a dict whose writes stay pending until
`commit()`. A `Snapshot` is a read-only dict pinned to one commit.

```python
from kvgit import Repo
from kvgit.kv.disk import Disk

with Repo(Disk("/tmp/kvgit-demo")) as on_disk:
    wt = on_disk.worktree("main", create=True)  # open, or create at the empty root
    wt["k"] = "v"
    print(wt.status())  # Status(updated=frozenset({'k'}), removed=frozenset())
    wt.commit(info={"msg": "first"})
    for c in on_disk.log(branch="main"):
        print(c.hash[:8], c.info)  # the commit, then the empty root commit
```

## For git users

| git | kvgit |
|---|---|
| `git worktree add` / `git checkout s1` | `repo.worktree("s1")` |
| edit, `git add` | `wt[k] = v` -- no index; everything is pending until commit |
| `git status` | `wt.status()` |
| `git commit [paths]` | `wt.commit([keys=...])` |
| `git restore .` | `wt.discard()` |
| `git reset --hard c` | `wt.reset(c)` |
| `git merge x` | `wt.merge(branch="x")` |
| `git cherry-pick c` / `git revert c` | `wt.cherry_pick(c)` / `wt.revert(c)` |
| `git branch [-D] x` | `repo.create_branch("x")` / `repo.delete_branch("x")` |
| `git tag [-d] v1` | `repo.create_tag("v1", c)` / `repo.delete_tag("v1")` |
| `git log`, `git show c`, `git diff a b`, `git merge-base a b` | `repo.log(...)`, `repo.get_commit(c)`, `repo.diff(a, b)`, `repo.merge_base(a, b)` |
| `git show v1:path` | `repo.snapshot(tag="v1")[key]` |
| `git gc [--prune=now]` | `repo.gc([min_age=0])` |

Where it differs: there is no index; several worktrees can hold one
branch, and their commits race and merge rather than being refused; a
conflicted merge raises `MergeConflict` and changes nothing (unless a
merge function such as `text_merge` writes markers into the value);
commit hashes are not git's, and there are no remotes.

## Merging

`Worktree.merge()` merges a branch, tag or commit into the worktree's
branch: lowest common ancestor, three-way resolve, and a two-parent
merge commit guarded on your own head. As in git, a branch that has not
moved since the fork fast-forwards instead, and `fast_forward=False`
writes the merge commit anyway:

```python
dev["score"] = 500
dev.commit()

result = main.merge(branch="dev")  # truthy when merged
print(result.strategy)  # "fast_forward": main hadn't moved since dev forked
print(main["score"])    # 500
```

Overlapping changes need a merge function per key, or a `default_merge`
fallback. `text_merge()` resolves line-oriented text with git-style
`<<<<<<<` markers (its `ours_label` / `theirs_label` arguments name
them); anything it cannot mark -- binary, non-UTF-8, oversized -- raises
`CantMark`, filed as an ordinary conflict:

```python
from kvgit import text_merge

main["notes"] = "alpha\nbeta\n"
main.commit()

repo.create_branch("edits", at=main.head)
edits = repo.worktree("edits")
edits["notes"] = "alpha\nBETA\n"
edits.commit()

main["notes"] = "ALPHA\nbeta\n"
main.commit()

main.merge(branch="edits", default_merge=text_merge())
print(main["notes"])  # "ALPHA\nBETA\n" -- both edits kept
```

Merge functions see decoded values, so `text_merge()` works on `str`
values under the default codec. `kvgit.merges.text` is the same merge
over raw bytes, for keys whose values already are `bytes`.

Registrations resolve most-specific-first: a key takes its exact-key
registration, else the longest registered prefix it starts with, else
`default_merge`. Prefixes cover keys whose names are not known when the
policy is set:

```python
from kvgit import MergeChoice, text_merge

main.set_merge_prefix("runs/", MergeChoice.OURS)   # this branch owns runs/
main.set_merge_fn("runs/index", text_merge())      # except this one key
```

Rules can live on the `Repo` too -- `Repo(backend, merge_fns=...,
merge_prefixes=..., default_merge=...)` -- for every worktree it opens.
A worktree's own registrations layer over the repo's, and a call's
`merge_fns=` / `merge_prefixes=` / `default_merge=` over both.

A registration holds either a merge function or a `MergeChoice`, and the
two reach differently:

* A **merge function** is consulted only where both sides changed a key.
  A change only one side made is applied as it always was.
* A **`MergeChoice`** is a standing policy: it hands that side every key
  either side changed under it. So `OURS` also drops a key the other
  side added and keeps one the other side removed -- what a branch that
  owns a whole namespace needs. Nothing is read or decoded for those
  keys.

`kvgit.merges.ours` and `kvgit.merges.theirs` are the merge-function
form, for keys where one side wins only when both sides collide. They
keep that side's stored value as it stands rather than rewriting it, so
the merge writes no new blob -- and if the chosen side removed the key,
the merge removes it.

Keys both sides changed to the same bytes merge cleanly with no merge
function, even though each side wrote its own copy.

A `post_check(key, merged_bytes)` predicate runs over every
merge-produced value; returning `False` files that key as conflicted.
`on_conflict="abandon"` leaves the branch untouched instead of raising.
Merging refuses with `ValueError` while the worktree has pending
changes -- commit or `discard()` first.

`cherry_pick(c)`, `revert(c)` and the general `apply(base, target)`
replay one change onto the worktree's branch as an ordinary
single-parent commit, with the same merge options.

## Codecs and trust

A repo's codec turns values into stored bytes. `codec="pickle"` is the
default: it is what makes a worktree a dict of anything. But unpickling
can execute code, so anyone who can write a pickle-codec store -- a
shared Postgres table, a disk directory -- can run code in every process
that reads it. For a store more than one party can write, use
`codec="bytes"`: kvgit then never decodes anything, values must be
`bytes`, and you choose where (if anywhere) untrusted pickles are
loaded. `snapshot.raw` reads the stored bytes under any codec.

The codec is fixed per store in practice: values written under one read
back as that codec's bytes under another, so switching an existing
store means rewriting its values.

## Chunked codecs (numpy / pandas)

Large numpy arrays and pandas DataFrames -- and any sliced views of them -- can be stored once and shared across keys, commits, and branches:

```python
import kvgit
import numpy as np

s = kvgit.open(codec="scientific")

big = np.arange(1_000_000, dtype="float64")  # ~8 MB
s["full"] = big
s["head"] = big[:100_000]
s["tail"] = big[-100_000:]
s.commit()
# All three keys reference the same chunk on disk -- ~8 MB total, not ~24 MB.
```

Pandas DataFrames piggyback on the numpy codec via their underlying block ndarrays. See [`docs/quick-start.md`](docs/quick-start.md#storing-scientific-data-efficiently-chunked-codecs) and the [API reference](docs/api.md#chunked-codecs).

## Part of the agex stack

kvgit provides versioned agent memory in [agex](https://github.com/ashenfad/agex) with branching and rollback. It also works as a versioned backing store for [monkeyfs](https://github.com/ashenfad/monkeyfs) virtual filesystems -- pass a `Worktree` anywhere a dict is expected.

## Development

```bash
uv sync --extra dev
uv run pytest
```

## Documentation

See [`docs/`](docs/) for detailed documentation:

- [Quick Start](docs/quick-start.md) -- common patterns with runnable examples
- [API Reference](docs/api.md) -- full reference for all classes, methods, and types
- [Browser persistence (Pyodide)](docs/pyodide.md) -- choosing between the IndexedDB and OPFS-mounted-disk backends, plus the syncfs flush requirement and recommended host-side patterns
