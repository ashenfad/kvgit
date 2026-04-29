# kvgit 🔀

Git-style versioning for your data. Commits, branches, and merges -- backed by a dict-like `MutableMapping`.

| Features | Description |
|---|---|
| **Dict interface** | `MutableMapping[str, Any]` -- reads and writes work like a dict |
| **Commits** | Immutable, content-addressable snapshots with rollback |
| **Branches** | Cheap forks with CAS-based optimistic concurrency |
| **Three-way merge** | Auto-merges non-overlapping changes; pluggable merge fns for conflicts |
| **Pluggable backends** | In-memory, disk (diskcache), git (GitPython), IndexedDB (Pyodide/browser), or bring your own `KVStore` |
| **Chunked codecs** | Optional content-addressed dedup for large numpy arrays and pandas DataFrames -- equal buffers stored once across keys, commits, and branches |

## Install

```bash
pip install kvgit              # in-memory only
pip install kvgit[disk]        # adds disk backend via diskcache
pip install kvgit[git]         # adds git backend via GitPython (requires git on PATH)
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
```

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
