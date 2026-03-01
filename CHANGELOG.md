# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
