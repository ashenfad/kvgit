# kvgit 🔀

Git-style versioning for key-value data: commits, branches, tags and
three-way merges over any store with an atomic conditional batch, with
garbage collection that runs beside live writers.

kvgit comes as two packages that share one store format and one engine:

| Package | | README |
|---|---|---|
| **Python**, `kvgit` on PyPI | A dict-like `Worktree`; memory, disk and PostgreSQL backends; codecs for pickle, bytes and chunked numpy / pandas | [`python/README.md`](python/README.md) |
| **TypeScript**, `@agex-ts/kvgit` on npm | An async `Worktree` for Node and the browser; memory, IndexedDB and SQLite backends; sync over GitHub | [`ts/README.md`](ts/README.md) |

A store either package writes, the other reads, and a commit either
makes gets the same hash in both.

## How it is built

The logic is a Rust core, [`crates/core`](crates/core), that never touches
a store or a clock: it asks its host for what it needs and is handed the
answers. Python hosts it through a native extension
([`crates/py`](crates/py)), TypeScript through WebAssembly
([`crates/wasm`](crates/wasm)). Each package keeps its own backends,
codecs and API, and runs every commit, merge, sweep and sync in the
core.

- [Storage format](docs/storage.md): every key kvgit writes, and how
  commits are named.
- [The request protocol](docs/protocol.md): how a host drives the core.
- [The corpus](corpus/README.md): scenarios both packages, and the core,
  run and must agree on, byte for byte.
- [Benchmarks](bench/README.md).

## Development

```bash
cargo test                          # the core
uv sync --extra dev && uv run pytest   # Python, building the extension
(cd ts && pnpm install && pnpm test)   # TypeScript, building the wasm
```

Building either package needs a Rust toolchain (`rustup`); the
TypeScript build also needs the `wasm32-unknown-unknown` target and
`wasm-bindgen-cli` at the version the `wasm-bindgen` crate pins. [`CHANGELOG.md`](CHANGELOG.md) lists every change.
