# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
