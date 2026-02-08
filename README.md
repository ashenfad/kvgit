# vkv

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
pip install vkv            # in-memory only
pip install vkv[disk]      # adds disk backend via diskcache
```

## Quick example

```python
from vkv import Versioned, counter

# Create a store and write some data
v = Versioned()
v.snapshot({"user": b"alice", "score": b"\x00" * 8})
v.merge()

# Fork a branch
branch = v.create_branch("feature")

# Both branches update different keys
v.snapshot({"user": b"bob"})
v.merge()

branch.snapshot({"score": b"\x00\x00\x00\x00\x00\x00\x00\x05"})

# Merge auto-resolves (non-overlapping changes)
result = branch.merge()
print(result.strategy)  # "three_way"
print(branch.get("user"))   # b"bob"
print(branch.get("score"))  # b"\x00\x00\x00\x00\x00\x00\x00\x05"

# Content types handle typed merge logic
ct = counter()
v2 = Versioned()
v2.snapshot({"hits": ct.encode(100)})
v2.merge()

fork = v2.create_branch("worker")
fork.set_content_type("hits", ct)

v2.snapshot({"hits": ct.encode(115)})   # +15
v2.merge()
fork.snapshot({"hits": ct.encode(120)}) # +20

fork.merge()
print(ct.decode(fork.get("hits")))  # 135 (115 + 120 - 100)
```

## Documentation

See [`docs/`](docs/) for detailed API documentation:

- [Core API (Versioned)](docs/versioned.md) -- commits, reads, writes, merging, branching, history
- [Content Types](docs/content-types.md) -- typed values with automatic merge
- [Garbage Collection](docs/gc.md) -- GCVersioned, rebase, orphan cleanup
- [Backends & Namespaces](docs/backends.md) -- KVStore interface, Memory, Disk, Namespaced
