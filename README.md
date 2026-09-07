# kvgit 🔀

Git-style versioning for your data. Commits, branches, and merges -- backed by a dict-like `MutableMapping`.

| Features | Description |
|---|---|
| **Dict interface** | `MutableMapping[str, Any]` -- reads and writes work like a dict |
| **Commits** | Immutable, content-addressable snapshots with rollback |
| **Branches** | Cheap forks with CAS-based optimistic concurrency |
| **Tags** | Immutable names for commits; a tagged commit outlives every branch that reached it, in every kvgit version |
| **Three-way merge** | Auto-merges non-overlapping changes; pluggable merge fns for conflicts |
| **Pluggable backends** | In-memory, disk (diskcache), IndexedDB (Pyodide/browser), or bring your own `KVStore` |
| **Chunked codecs** | Optional content-addressed dedup for large numpy arrays and pandas DataFrames -- equal buffers stored once across keys, commits, and branches |

## Install

```bash
pip install kvgit              # in-memory only
pip install kvgit[disk]        # adds disk backend via diskcache
pip install kvgit[scientific]  # adds chunked codecs for numpy / pandas
# IndexedDB backend is available automatically in Pyodide (browser) environments
```

## Quick example

```python
import kvgit

main = kvgit.store()

main["user"] = "alice"
main["score"] = 0
main.commit()

# Branch and diverge
dev = main.create_branch("dev")
dev["score"] = 999
dev.commit()

print(main["score"])  # 0   (main unchanged)
print(dev["score"])   # 999 (dev branch)

# Tag a commit by name -- immutable, and safe from garbage collection
main.tag("v1")
print(main.peek("score", tag="v1"))  # 0
```

## Merging

`Staged.merge()` merges another head -- usually another branch's HEAD --
into the current branch: lowest common ancestor, three-way resolve, and
a two-parent merge commit guarded on your own head:

```python
dev["score"] = 500
dev.commit()

result = main.merge(dev.current_commit)  # True when merged
print(main["score"])  # 500 (fast-forward: main hadn't diverged)
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

edits = main.create_branch("edits")
edits["notes"] = "alpha\nBETA\n"
edits.commit()

main["notes"] = "ALPHA\nbeta\n"
main.commit()

main.merge(edits.current_commit, default_merge=text_merge())
print(main["notes"])  # "ALPHA\nBETA\n" -- both edits kept
```

`kvgit.merges.text` is the same merge one level down, over raw bytes,
for `VersionedKV`. A `Staged` decodes each side before calling a merge
function, so register `text_merge()` there unless the key's values are
already `bytes`.

Registrations resolve most-specific-first: a key takes its exact-key
registration, else the longest registered prefix it starts with, else
`default_merge`. Prefixes cover keys whose names are not known when the
policy is set:

```python
from kvgit import MergeChoice, text_merge

main.set_merge_prefix("runs/", MergeChoice.OURS)   # this branch owns runs/
main.set_merge_fn("runs/index", text_merge())      # except this one key
```

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

`commit()` and `merge()` take `merge_fns=` / `merge_prefixes=` /
`default_merge=` for one call, layered over what is registered.

Keys both sides changed to the same bytes merge cleanly with no merge
function, even though each side wrote its own copy.

A `post_check(key, merged_bytes)` predicate runs over every
merge-produced value; returning `False` files that key as conflicted.
`on_conflict="abandon"` leaves the branch untouched instead of raising.
Merging refuses with `ValueError` when the staging buffer holds
uncommitted changes -- commit or reset first.

## Chunked codecs (numpy / pandas)

Large numpy arrays and pandas DataFrames -- and any sliced views of them -- can be stored once and shared across keys, commits, and branches:

```python
import kvgit
import numpy as np

s = kvgit.store(codecs="scientific")

big = np.arange(1_000_000, dtype="float64")  # ~8 MB
s["full"] = big
s["head"] = big[:100_000]
s["tail"] = big[-100_000:]
s.commit()
# All three keys reference the same chunk on disk -- ~8 MB total, not ~24 MB.
```

Pandas DataFrames piggyback on the numpy codec via their underlying block ndarrays. See [`docs/quick-start.md`](docs/quick-start.md#storing-scientific-data-efficiently-chunked-codecs) and the [API reference](docs/api.md#chunked-codecs).

## Part of the agex stack

kvgit provides versioned agent memory in [agex](https://github.com/ashenfad/agex) with branching and rollback. It also works as a versioned backing store for [monkeyfs](https://github.com/ashenfad/monkeyfs) virtual filesystems -- pass a `Staged` instance anywhere a dict is expected.

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
