# kvgit 🔀

Versioned key-value store with git-like commit, branch, and merge semantics.

Values are `Any` (serialized via pickle by default). Commits are content-addressable. Branches are cheap. Merges are three-way with pluggable per-key conflict resolution.

## Features

- **Commits** -- immutable, content-addressable snapshots
- **Branches** -- named branch heads with CAS-based concurrency
- **Three-way merge** -- auto-merges non-overlapping changes, pluggable merge functions for conflicts
- **Merge functions** -- counters, last-writer-wins, or custom per-key merge logic
- **Garbage collection** -- high/low water rebase drops cold keys automatically
- **Namespaces** -- key-prefixed views with full read/write support
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

# Create a store -- values are Any (pickle-serialized by default)
s = kvgit.store()

s["user"] = "alice"
s["score"] = 0
s.commit()

first = s.current_commit

# Update and commit again
s["score"] = 100
s.commit()
print(s["score"])              # 100

# Rollback to the first commit
s.reset_to(first)
print(s["score"])              # 0

# Branching
s["score"] = 50
s.commit()

dev = s.create_branch("dev")
dev["score"] = 999
dev.commit()

print(s["score"])              # 50  (main unchanged)
print(dev["score"])            # 999 (dev branch)
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
