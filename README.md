# kvgit 🔀

Git-style versioning for your data. Commits, branches, and merges -- backed by a dict-like `MutableMapping`.

| Features | Description |
|---|---|
| **Dict interface** | `MutableMapping[str, Any]` -- reads and writes work like a dict |
| **Commits** | Immutable, content-addressable snapshots with rollback |
| **Branches** | Cheap forks with CAS-based optimistic concurrency |
| **Three-way merge** | Auto-merges non-overlapping changes; pluggable merge fns for conflicts |
| **Eviction** | High/low water rebase drops least-recently-used keys automatically |
| **Pluggable backends** | In-memory, disk (diskcache), git (GitPython), IndexedDB (Pyodide/browser), or bring your own `KVStore` |

## Install

```bash
pip install kvgit            # in-memory only
pip install kvgit[disk]      # adds disk backend via diskcache
pip install kvgit[git]       # adds git backend via GitPython (requires git on PATH)
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
