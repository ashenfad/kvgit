# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-04-28

Adds an opt-in `kvgit.codecs` package for content-addressed chunk dedup of large numpy / pandas values. Storage format bumps to v3, but v2 stores remain readable by v3 code and are only stamped as v3 on the first chunked write — the on-disk layout is unchanged for plain-pickle workloads.

### Added

- **`kvgit.codecs` package** — pluggable chunked-codec layer over the existing `Staged` encoder/decoder slot. Externalizes large sub-values as content-addressed chunks (under a new `kvgit:chunk:<hash>` namespace) and emits a small token via pickle's `persistent_id`. Composes with arbitrary container nesting; equal buffers are stored exactly once across keys, commits, and branches.
  - `compose(*codecs) -> (encoder, decoder)` — public entry point.
  - `Codec` / `ChunkSink` / `ChunkReader` protocols (`kvgit.codecs.base`).
  - `NumpyCodec(min_bytes=1024)` (`kvgit.codecs.numpy`) — externalizes ndarrays; chases `.base` so view slices share the parent's chunk; passes through object-dtype and below-threshold standalone arrays.
  - `PandasCodec` (`kvgit.codecs.pandas`) — alias of `NumpyCodec`. DataFrame block buffers are reachable through pandas' pickle path, so the numpy codec catches them automatically (including `iloc` row-slice views that share blocks with their parent).
- **`MetaEntry.chunks`** — optional list of chunk-reference hashes per key. Omitted from the on-disk encoding when empty, so v2-format entries stay byte-identical and remain readable by older code.
- **`Versioned.commit(chunks=..., chunk_refs=...)`** — new keyword args carrying the per-commit chunk batch through to `_create_commit`. Backends that don't support chunks ignore them.
- **`Staged` autodetects chunked encoder/decoder** by signature (second positional parameter has no default). Default `pickle.dumps` / `pickle.loads` continue to behave as before.
- **`pyproject.toml` extras**: `numpy`, `pandas`, `scientific`.
- **`kvgit.codecs.scientific()`** — one-liner returning a pre-composed `(encoder, decoder)` pair using whichever scientific codecs are installed. Exposed as a named preset on the factory: `kvgit.store(codecs="scientific")`. The `codecs=` argument is mutually exclusive with explicit `encoder` / `decoder`.
- **75 new tests** under `tests/codecs/` covering hash determinism, the pickler glue (composition, dedup-by-identity vs by-content, codec ordering, error paths), numpy round-trips across dtypes/shapes/views, pandas DataFrame/Series including row-slice dedup, end-to-end via `Staged`, GC reachability and young-orphan protection, v2 ↔ v3 import-as-migration dedup, and the `codecs="scientific"` factory shortcut.

### Changed

- **`STORAGE_VERSION` 2 → 3.** Lazy upgrade: `_check_storage_version` accepts `{2, 3}` on open; the v3 stamp is written by `_create_commit` only when the commit includes chunks. A v3-aware reader can open a v2 store transparently; a v2-only reader still refuses v3 stores (intentional — it can't decode chunked blobs).
- **`clean_orphans` traces chunk reachability.** The mark phase accumulates `MetaEntry.chunks` from every reachable commit. Young orphan commits (younger than `min_age`) also contribute chunks to the reachable set, protecting in-flight writers. The sweep phase removes unreferenced `kvgit:chunk:*` entries alongside unreferenced HAMT nodes.

### Limitations

- **Merge results don't chunk.** When `Staged`'s wrapped merge function re-encodes a merged value, it falls back to plain `pickle.dumps`. The bytes-level merge protocol has no place to land chunks. Subsequent commits that overwrite the merged key go through the chunked path normally.
- **Materialized arrays are read-only.** Mutating one slice would silently affect every other key sharing the same chunk; `.copy()` is the explicit escape hatch.

## [0.2.2] - 2026-04-28

### Added

- **`Hamt.walk(skip_nodes=...)` / `Keyset.walk(skip_nodes=...)`** — optional cumulative seen-set parameter. Subtrees whose root hash is in `skip_nodes` are not fetched, not recursed into, and not included in the returned `nodes` set; items beneath them are also omitted. Pass an accumulating set across multiple walks (e.g. across the commits of a branch's history) so structurally-shared HAMT subtrees are visited only once. Turns N walks over a shared tree from `O(N · subtree)` into `O(unique nodes)`.

### Changed

- **`clean_orphans` mark phase shares walk work across commits and branches** — the cumulative `reachable_nodes` set is now passed as `skip_nodes` to each per-commit `Keyset.walk()`. On long histories with heavy structural sharing (e.g. ~600 commits where each commit changes a few keys against a large keyset), this collapses redundant subtree traversal: the mark phase becomes proportional to unique HAMT nodes instead of `commits × subtree-size`.

### Fixed

- **`IndexedDB` open no longer hangs forever when the database is blocked by another connection.** The `_idb_open` executor now wires `onblocked` alongside `onsuccess` / `onerror`, rejecting with an actionable error message ("close other tabs holding the database open and reload, or restart the browser") instead of leaving `run_sync` suspended indefinitely. Previously a zombie connection from a closed tab could wedge `IndexedDB.__init__` until the browser was restarted.

## [0.2.1] - 2026-04-18

### Added
- **`Staged.commit(keys=...)`** — partial commits. Pass a set of keys to flush only those entries from the staging buffer; uncommitted keys remain staged for a future commit. Enables selective version control workflows.

## [0.2.0] - 2026-04-10

Per-commit keysets are now stored as a content-addressable HAMT, so
single-key commits write O(log N) new nodes instead of rewriting the
full keyset. Cold loads and orphan cleanup use batched BFS for
O(log N) round-trips on high-latency backends. **Storage format is
not backward compatible with v0.1.x** -- pre-v2 stores raise on open.

### Breaking Changes

- **Storage format v2** -- `__commit_keyset__` / `__meta__` / `__total_var_size__` replaced by `__commit_root__` + `kvgit:keyset:<hash>` HAMT nodes. New `__kvgit_version__` sentinel raises on pre-v2 stores.
- **`MetaEntry.last_touch` removed** along with `_touch()` / `_touch_counter`. Persisting touch counts would rewrite every leaf on every commit, defeating structural sharing. `MetaEntry` is now `(size, created_at)`.
- **`Disk` default `size_limit` is now unbounded.** Previously defaulted to 1 GiB, silently evicting past the cap. `None` also accepted as an explicit "no limit".
- **`Hamt.commit` / `Keyset.commit` renamed to `.persist`** -- avoids name collision with `Versioned.commit`.
- **`kvgit.encoding` helpers renamed**: `to_bytes` → `dumps`, `from_bytes` → `loads`, plus new `safe_loads`. Matches the `json` module's convention.
- **`MetaEntry` relocated** from `kvgit.encoding` to `kvgit.versioned.keyset`.

### Added

- **`kvgit.hamt.Hamt`** -- generic content-addressable HAMT over a `KVStore`. Branching factor 16, configurable `bucket_max` (default 8), canonical form preserved across insert/delete patterns.
- **`kvgit.versioned.keyset.Keyset`** -- thin kvgit-specific wrapper that decodes HAMT values into `KeysetEntry(blob, meta)`.
- **`Hamt.materialize()` / `Keyset.materialize()`** -- batched BFS returning the full map as a dict in O(log_branching N) round-trips instead of one read per node.
- **`Hamt.walk()` / `Keyset.walk()`** -- single batched BFS returning `(items, node_hashes)` for GC mark phases.
- **`KVStore` bulk methods accept `Mapping` / `Iterable` forms** (non-breaking): `set_many({"a": b"1"})`, `get_many(["a", "b"])`, `remove_many(["a", "b"])`. Existing `**kwargs` / `*args` callers continue to work.
- **`Keyset.diff()`** -- structural diff that skips identical subtrees by hash equality.
- **`tests/benchmark_storage.py`** -- storage-growth and cold-load benchmarks, including a `--latency-ms` mode that simulates network-attached backends.
- **`tests/stress_kill.py`** -- concurrent commit + branch-delete stress test.

### Changed

- **`clean_orphans` uses batched walks** -- mark phase uses `Keyset.walk()` (one BFS per commit instead of separate `items()` + `reachable_nodes()` passes); sweep phase uses `Keyset.materialize()` for orphan blob enumeration.
- **`_three_way_merge` dedupes `_load_keyset` calls** -- loads each unique commit's keyset once per merge instead of up to three times.

### Fixed

- **`clean_orphans` could corrupt live branches under concurrent writes.** `delete_branch` previously called `clean_orphans(min_age=0)`, bypassing the age guard. The sweep is now also batched into a single `remove_many` so it's atomic at the store level.
- **`kvgit.store(kind="disk")` was producing zero-byte caches.** The factory passed `size_limit=0`, which `diskcache` 5.6.3 interprets as "zero bytes allowed". Every write was evicted immediately. Adds disk-factory round-trip regression tests.
- **`VersionedGP` now raises `ImportError` instead of `NameError`** when GitPython is missing. `tests/versioned/test_gp.py` uses `pytest.importorskip("git")` so the full suite runs cleanly without GitPython installed.

### Performance

Measured against the new benchmarks (1000 keys, 1000 single-key commits for storage; 1000-key cold load and 5-branch / 20-commits-per-branch `delete_branch` for round-trips):

| Metric | v0.1.x | v0.2.0 | Speedup |
|---|---|---|---|
| Per-commit storage growth | ~195 KB | ~6.2 KB | **~30x** |
| Cold load (1 ms latency) | ~400 ms | ~10 ms | **~38x** |
| Cold load (5 ms latency) | ~1.9 s | ~40 ms | **~47x** |
| `delete_branch` (1 ms latency) | ~2 min | ~1.2 s | **~100x** |
| `delete_branch` (5 ms latency) | ~9 min | ~4.7 s | **~115x** |

Local backends (Memory, Disk) are unchanged -- the batching wins apply only where per-call latency dominates.

### Removed

- **`__commit_keyset__` / `__meta__` / `__total_var_size__`** storage keys (`__total_var_size__` was dead weight; the others are replaced by the HAMT).
- **`kvgit.encoding.meta_to_bytes` / `meta_from_bytes`**.
- **`MetaEntry.last_touch`**, **`VersionedKV._touch_counter`**, **`VersionedKV._touch()`**.

## [0.1.11] - 2026-04-08

### Fixed
- **IndexedDB binary data corruption** -- write path now uses `.slice()` to copy `to_js(bytes)` WASM memory views into standalone ArrayBuffers before storing, preventing pickle stream corruption through IndexedDB's structured clone (manifested as `UnpicklingError: invalid load key, '\x0a'`)
- **IndexedDB read path** -- removed `Uint8Array.new()` constructor from `_to_bytes`, calling `.to_py().tobytes()` directly to avoid constructor failures in some Pyodide environments

### Added
- **Binary round-trip test** -- `test_binary_roundtrip` covers all 256 byte values and realistic pickle payloads across `set`, `set_many`, and `cas`

## [0.1.9] - 2026-04-01

### Added
- **Backup HEAD** (`__branch_head_prev__`) -- each HEAD update now saves the previous HEAD, providing a one-commit-behind fallback if a write is interrupted (e.g. browser tab closed mid-commit)
- **Automatic HEAD recovery** -- all HEAD resolution goes through a three-tier fallback: current HEAD, prev HEAD, full commit scan. Corrupt HEADs are auto-repaired and logged as warnings. Covers `__init__`, `peek`, `switch_branch`, `refresh`, and `clean_orphans`

### Fixed
- **IndexedDB byte conversion** -- `_to_bytes` now handles corrupted or unexpected JS values gracefully instead of crashing
- **`latest_head` property** is now side-effect free (reads but never writes to the store); repair only happens in explicit methods

## [0.1.8] - 2026-03-18

### Fixed
- **IndexedDB byte conversion** -- replaced slow `bytes(js_proxy)` with `Uint8Array.to_py().tobytes()` for fast memcpy across the JS/WASM boundary (76MB: ~10s → ~14ms)
- **`items()` cursor** -- updated to use the same fast byte conversion

## [0.1.7] - 2026-03-12

### Added
- **`clean_orphans(min_age=3600)`** on `VersionedKV` -- mark-and-sweep cleanup of commits unreachable from any branch HEAD, safe for shared blob histories
- **Automatic orphan cleanup** -- `delete_branch()` now calls `clean_orphans(min_age=0)` after removing the branch HEAD

### Removed
- **`GCVersionedKV`** -- destructive LRU eviction class removed entirely. Use `delete_branch()` (which auto-cleans orphans) or call `clean_orphans()` directly.
- **`RebaseResult`** and **`MetaEntry`** removed from public API docs

## [0.1.6] - 2026-03-08

### Added
- **IndexedDB backend** (`kvgit.kv.indexeddb.IndexedDB`) -- browser-persistent KV store for Pyodide environments using IndexedDB and JSPI
- **`kind="indexeddb"`** option in `kvgit.store()` factory with `db_name` parameter
- **Pyodide CI job** -- IndexedDB tests run in Chrome via pytest-pyodide

## [0.1.5] - 2026-03-05

### Added
- **`Versioned` protocol** in `kvgit.protocol` -- shared interface for all versioned backends
- **`VersionedGP`** (`kvgit.versioned_gp`) -- GitPython-backed versioned store
- **`kind="git"`** option in `kvgit.store()` factory

### Changed
- **`Versioned`** (class) renamed to **`VersionedKV`** and moved to `kvgit.versioned_kv`
- **`GCVersioned`** renamed to **`GCVersionedKV`** and moved to `kvgit.gc_kv`
- **`GitVersioned`** renamed to **`VersionedGP`** and moved to `kvgit.versioned_gp`
- **`Staged`** now typed against the `Versioned` protocol, accepting any implementation
- Shared types (`MergeResult`, `DiffResult`, `BytesMergeFn`, `MetaEntry`) moved to `kvgit.protocol`

### Removed
- Old modules: `kvgit.versioned`, `kvgit.git_versioned`, `kvgit.gc`

## [0.1.4] - 2026-03-01

### Added
- **`current_branch`** property on Versioned and Staged -- returns the name of the active branch
- **`switch_branch(name)`** on Versioned and Staged -- switch to an existing branch in-place (Staged clears its staging buffer)
- **`delete_branch(name)`** on Versioned and Staged -- delete a branch by name
- **`peek(key, *, branch)`** on Versioned and Staged -- read a key from another branch's HEAD without switching
- **`create_branch(name, *, at=None)`** -- optional `at` parameter to fork from a specific commit instead of current HEAD

## [0.1.3] - 2026-02-28

### Fixed
- **ConcurrencyError state recovery**: Store state is now restored after CAS failures during commit, preventing stale in-memory state
- **Bare assert in versioned.py**: Replaced with proper ValueError guard

### Changed
- **GC docs**: Clarified that size tracking covers serialized value bytes only
- **backends.md**: Updated to reference concrete classes instead of removed protocol types
