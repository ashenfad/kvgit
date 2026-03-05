# kvgit 🔀

Git-like versioning for your data. Commits, branches, and three-way merges -- backed by a `MutableMapping[str, Any]` you can use like a dict.

Built for applications that need rollback, branching, or multi-writer coordination on shared state -- from agent orchestration to stateful workflows.

## Features

- **Dict interface** -- `MutableMapping[str, Any]`, reads and writes work like a dict
- **Commits** -- immutable, content-addressable snapshots with rollback
- **Branches** -- cheap forks with CAS-based optimistic concurrency
- **Three-way merge** -- auto-merges non-overlapping changes; pluggable per-key merge functions (counters, last-writer-wins, or custom) for conflicts
- **Garbage collection** -- high/low water rebase drops cold keys automatically
- **Namespaces** -- key-prefixed views for isolating state across components
- **Pluggable backends** -- in-memory, disk (via diskcache), git (via GitPython), or bring your own `KVStore`

## Install

```bash
pip install kvgit            # in-memory only
pip install kvgit[disk]      # adds disk backend via diskcache
pip install kvgit[git]       # adds git backend via GitPython (requires git on PATH)
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

## Development

```bash
uv sync --extra dev
uv run pytest
```

## Documentation

See [`docs/`](docs/) for detailed documentation:

- [Quick Start](docs/quick-start.md) -- common patterns with runnable examples
- [API Reference](docs/api.md) -- full reference for all classes, methods, and types
