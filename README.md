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
fallback. `kvgit.merges.text` resolves line-oriented text with git-style
`<<<<<<<` markers (see `make_text_merge` for custom labels); anything it
cannot mark -- binary, non-UTF-8, oversized -- raises `CantMark`, filed
as an ordinary conflict:

```python
from kvgit.merges import text

result = main.merge(dev.current_commit, default_merge=text)
```

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
