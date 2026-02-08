# kvit

Versioned key-value store with git-like commit, branch, and merge semantics.

Values are bytes. Commits are content-addressable. Branches are cheap. Merges are three-way with pluggable per-key conflict resolution.

## Features

- **Commits** -- immutable, content-addressable snapshots
- **Branches** -- named branch heads with CAS-based concurrency
- **Three-way merge** -- auto-merges non-overlapping changes, pluggable merge functions for conflicts
- **Content types** -- typed encode/decode/merge for counters, JSON, last-writer-wins, or custom types
- **Garbage collection** -- high/low water rebase drops cold keys automatically
- **Namespaces** -- key-prefixed views with full read/write support
- **Pluggable backends** -- in-memory, disk (via diskcache), or bring your own `KVStore`

## Install

```bash
pip install kvit            # in-memory only
pip install kvit[disk]      # adds disk backend via diskcache
```

## Quick example

```python
import kvit

# Create a store (Staged backed by in-memory Versioned)
s = kvit.store()

# Write and commit
s.set("user", b"alice")
s.set("score", b"\x00" * 8)
s.commit()

# Content types handle typed merge logic
from kvit import counter

ct = counter()
s2 = kvit.store()
s2.set("hits", ct.encode(100))
s2.commit()

# Branching
worker = s2.create_branch("worker")
worker.set_content_type("hits", ct)

s2.set("hits", ct.encode(115))       # +15 on main
s2.commit()

worker.set("hits", ct.encode(120))   # +20 on worker
worker.commit()

print(ct.decode(worker.get("hits")))  # 135 (115 + 120 - 100)
```

## Development

```bash
uv sync --extra dev
uv run pytest
```

## Documentation

See [`docs/`](docs/) for detailed API documentation:

- [Core API (Versioned)](docs/versioned.md) -- commits, reads, writes, merging, branching, history
- [Content Types](docs/content-types.md) -- typed values with automatic merge
- [Garbage Collection](docs/gc.md) -- GCVersioned, rebase, orphan cleanup
- [Backends & Namespaces](docs/backends.md) -- KVStore interface, Memory, Disk, Store, Staged, Live, Namespaced
