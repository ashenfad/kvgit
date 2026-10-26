# kvgit core: design

One Rust core under both kvgit implementations: the Python package
(`kvgit` on PyPI) and the TypeScript one (`@agex-ts/kvgit` on npm, now
`packages/kvgit-ts` in agex-ts). Both move into this repository and
release together. The work happens on the `core` branch, in PRs, and
`core` merges to `main` once, when both hosts run on the core and pass
everything they pass today.

`plan.md` breaks the work into stages. This document holds what the
core is, where its edges are, and why.

## Why

The two implementations already share a shape: storage v4 byte for
byte, the same API (one sync, one async), the same merge rules, the
same GC lease protocol. Keeping them in step costs every change twice,
and the changes that recur are the subtle ones — conditional deletes,
`on_moved`, the lease, head recovery, publishing a head with its backup.
Each has been designed in one language, ported to the other, and tested
twice. Golden fixtures pin the bytes; nothing pins the protocol except
two test suites written separately.

A shared core makes the protocol one piece of code. It also makes the
Python side faster where it is slowest: building and walking the HAMT,
and loading a whole keyset into memory on every worktree open.

## Scope

In:

- A Rust crate holding everything that is bytes or protocol (below).
- A Python host package and a TypeScript host package over it, each
  keeping an idiomatic public API: Python synchronous and dict-like,
  TypeScript async.
- `@agex-ts/kvgit` moves here from agex-ts, with its history.
- A language-neutral scenario corpus that all three (core, Python
  host, TS host) run.
- Dropping surface nobody should build on: `VersionedBase`, `Staged`,
  and `VersionedKV` as public API.
- Dropping the fallbacks for history written before v4, once a
  backfill has brought existing stores up to date.

Out:

- Any change to the storage format. Stores stay v4, and a commit made
  by either host keeps the hash it has today.
- An async Python API. The core makes one possible later; nothing here
  needs it.
- A Pyodide build of the Python package. Python code in a browser
  reaches versioned state through the TypeScript host, not by opening
  a store itself.
- A Python sync transport. The wire format and its walk and replay
  move into the core, so Python gets them; a GitHub transport for
  Python is not part of this.

## Repository layout

```
Cargo.toml            workspace
crates/core/          kvgit-core: no I/O, no host dependencies
crates/py/            PyO3 bindings, built as kvgit._core
crates/wasm/          wasm-bindgen bindings
python/kvgit/         the Python host package
python/tests/
pyproject.toml        maturin backend, python-source = "python"
ts/                   @agex-ts/kvgit: src/, tests/, the built wasm
corpus/               scenarios every implementation runs
docs/
```

`pyproject.toml` stays at the root so `uv sync` and `uv run pytest`
work from the root as they do now. uv rebuilds the extension when Rust
sources change if the crates are listed in `[tool.uv] cache-keys`.

`ts/` is a standalone pnpm package with its own lockfile. It is
imported from agex-ts with its history (a `filter-repo` of
`packages/kvgit-ts`, merged in), so `git log ts/` reads back through
the work done there.

## The core and the hosts

The core owns:

- **Encoding.** Canonical JSON as Python's `json.dumps` writes it
  (ASCII escapes, Python's float repr), SHA-256, layout keys.
- **The tree.** HAMT nodes, keyset entries, lookups, updates, the
  structural diff, the walk GC marks with.
- **Commits.** Records, the commit hash, generations, merge base,
  ancestry (what one tip has that another lacks).
- **Refs.** Branch and tag names and their validation, creation,
  conditional deletion, tag records.
- **Head resolution.** The current HEAD, its backup, the caller's
  recoverer, `repair_head`, and the commit-scan recoverer (Python has
  one, TypeScript does not; both get it).
- **Merging.** Policy picking (exact key, longest prefix, default),
  three-way resolution, fast-forward, `apply` and its cherry-pick and
  revert forms, `post_check`, the line-oriented text merge with
  conflict markers, and `SequenceMatcher`.
- **The write protocol.** Waiting out a live sweep, landing a commit's
  batch under the lease record with its in-flight marker, publishing
  the head with its backup and the marker's removal in one write, the
  lost-race merge, `on_moved="raise"`.
- **GC.** Acquiring and releasing the lease, mark and sweep, the deep
  scan.
- **The content cache.** Memory of write-once keys (nodes, commit
  records), byte-bounded, emptied when the lease record changes.
- **Sync primitives.** The wire form of a commit, the walk that
  produces it, the replay that checks every hash before writing,
  publishing a synced head, sync heads and marks.

Each host owns:

- **Backends.** Python: Memory, Disk, Postgres, Composite, Namespaced.
  TypeScript: Memory, IndexedDB, SQLite.
- **Values.** A blob's bytes, and hashing them to the blob's key (see
  "Values" below).
- **Codecs.** Python: pickle, scientific (chunked numpy and pandas),
  bytes. TypeScript: bytes, JSON, CBOR. The core sees only bytes and,
  for chunked values, the chunk references a codec reports.
- **Merge functions over decoded values.** A rule registered on a host
  is lowered to bytes in the host and called back by the core.
- **The value cache.** Decoded values a worktree has read, bounded in
  bytes in both hosts (TypeScript's is an unbounded `Map` today).
- **The public API.** `Repo`, `Worktree`, `Snapshot`, `Branches`,
  `Tags`, errors, `open()`.
- **Transport.** TypeScript's `PackRemote` over GitHub, its pack
  format, `FakeGithub`.
- **Time and waiting.** The clock, sleeping, abort signals.

## Sans-IO

The core never touches a store. An operation is a machine that the host
drives: the core hands back a request, the host performs it and hands
back the answer, until the operation finishes.

```
op = core.commit(worktree, changes, options)
reply = None
while True:
    request = op.resume(reply)
    match request:
        case Get(keys):                  reply = store.get_many(keys)
        case Scan(prefix):               reply = list(store.keys(prefix))
        case Cas(expect, writes, drops): reply = store.cas_many(expect, writes, drops)
        case Wait(seconds):              sleep(seconds); reply = None
        case Merge(rule, key, old, ours, theirs):
                                         reply = merge(rule, old, ours, theirs)
        case Recover(branch):            reply = recoverer(store, branch)
        case Done(result):               return result
        case Failed(error):              raise host_error(error)
```

Every reply also carries the host's clock reading. Commit and tag times
come from a clock the caller may replace (TypeScript's `now` option,
which Python gains); lease and in-flight expiries always use wall time.
The lease owner id is host-supplied entropy, so the core needs no
random source.

Why requests, rather than the core calling a store trait:

- **One core serves a synchronous and an asynchronous host.** The
  Python driver blocks on each request; the TypeScript driver awaits
  it. Nothing blocks across the wasm boundary and the core needs no
  async runtime.
- **The host keeps control of I/O:** batching, transactions,
  cancellation, and what a backend does with a request.
- **Interleavings become testable.** A test drives two machines over
  one in-memory store and chooses which advances next, so a race that
  needs threads and luck to hit today is one scenario in the corpus.

The cost is one boundary crossing per request. Requests are batched — a
tree level per `Get`, one `Cas` per write — so a commit makes a handful
of crossings, about the tree's depth plus the write protocol's steps.

Inside the core, operations are written as ordinary sequential code
(`async fn` against a request channel) and polled by a small executor
that surfaces each pending request to the binding. That keeps the
protocol readable as straight-line code, as it is in Python today,
rather than as hand-written state tables. No threads and no runtime are
involved, so it runs the same in wasm.

## Values

Values do not cross the boundary. The core deals in blob keys, sizes
and chunk references; the host reads and writes the bytes.

- **Writing.** The host encodes a value, hashes it (`hashlib` in
  Python, WebCrypto in TypeScript) and hands the core the blob key and
  size. When the core's batch names that blob, the host supplies the
  bytes it is holding into the `Cas`.
- **Reading.** The core resolves a key to its entry; the host fetches
  the blob in its own batched read.
- **Merging.** A `Merge` request names the three sides by blob key.
  The host reads and decodes them, runs the rule, encodes and hashes
  the result, and replies with its key and size. The built-in text
  merge is the one place value bytes enter the core, and only for the
  keys it merges.

This matters for large values: kvsql stores its database snapshots as
8 MB chunks, and copying each one into wasm memory and out again would
cost more than the commit it belongs to.

## Worktree state

A worktree today holds its whole keyset in memory: always in Python,
and in TypeScript unless opened `lazy`. TypeScript's lazy mode instead
holds the keyset root and reads the tree on demand through a bounded
node cache.

Lazy mode was built for a design of the app database that stored one
SQLite page per key: about 51,000 keys for a 200 MB database, 262,000
for 1 GB. Its measurements (headless Chromium, IndexedDB) put an eager
open at 0.56 s for 50,000 keys, 3.0 s for 262,000 and 50 s for a
million, and a lazy open under a millisecond at every size. The
database shipped instead as kvsql, a changeset log with snapshot
chunks, whose trees hold about a hundred keys, and nothing uses lazy
mode today. The large tree that remains is the world's, one key per
file.

One model: a worktree is a root plus its pending changes; reads go
through the repository's content cache, which already holds tree
nodes. Opening costs one read whatever the keyset's size, and a commit
rewrites only the paths it changes.

Walking the whole tree for every `keys()` call is too slow on the
world's shape. Measured in TypeScript on 100,000 files, against a
materialized worktree:

| | materialized ms | root plus cache ms |
|---|---|---|
| open | 318.4 | 0.009 |
| `keys()` | 42.8 | 251.4 |
| one directory's listing | 45.5 | 250.1 |
| reading every file | 521.8 | 1,034.2 |
| 10,000 reads of 1,000 files | 314.6 | 102.0 |

So a worktree also keeps a key index, as a cache over the same model:

- The first `keys()`, listing or iteration builds it with one walk;
  nothing builds it at open, so a worktree that never lists pays
  nothing.
- It holds the sorted keys and nothing else. Pending changes are
  values, so they stay with the host, which lays them over the index.
- A commit or merge carries it to the new root with its own changes.
  Moving the worktree to a root it did not write (a checkout, a
  refresh, a reset) updates it by a structural diff of the two roots,
  which reads only the subtrees that differ.
- A lookup is a tree path through the cache. The index answers only
  that a key is absent, without a read.

Worktree state lives in the core, owned by the host object. TypeScript
objects holding core state are freed by a `FinalizationRegistry`
(wasm-bindgen's weak references), with an explicit `close()` for
callers that want it deterministic.

## Storage

The format is storage v4 as both implementations write it today, and
the corpus pins it byte for byte.

Moving a store to v4 only raised its stamp; nothing already written was
rewritten, and untouched subtrees are carried forward as stored. A
store stamped v4 can therefore still hold objects from before v4. The
core treats them this way:

- **Blob pointers of the form `<commit>:<key>`** are read like any
  other pointer: an entry names a key and the blob is that key's value.
  There is no separate read path for them.
- **Entries carrying `created_at`** are read and written back with it,
  in its stored position, so an untouched subtree re-encodes to the
  bytes it was stored as. The core never writes it on a new entry.
  TypeScript's reader drops it today, which makes the two
  implementations write different nodes for the same change on such a
  store; the corpus covers it and the fix lands on `main` first.
- **Commits without stored generations** are not read: there is no
  whole-history walk for merge base. The backfill below writes their
  generations first.
- **Parent records stored as a bare string** are not read; the
  backfill rewrites them as lists, which changes no hash.
- **Stores stamped v2 or v3** are refused; the backfill raises the
  stamp.

Chunks stay: entries keep their chunk references, GC marks and sweeps
chunks, and the scientific codec keeps writing them from Python.

## Public API

Each host keeps the API it has, minus the engine layer, and the two
align where they differ without reason.

Python drops:

- `kvgit.versioned` as a public package: `VersionedKV`,
  `VersionedBase`, the protocol module. `MergeChoice`, `MergeResult`,
  `DiffResult`, `TagInfo` and `MergePolicy` are already exported from
  `kvgit`.
- `kvgit.hamt` and `kvgit.versioned.keyset` as public modules.
- The IndexedDB backend and `docs/pyodide.md`.

`CorruptHeadRecoverer` and `recover_by_commit_scan` stay, exported from
`kvgit` as now.

Python adds what nontainer reaches into internals for today:

- a three-way resolve over caller-named values (nontainer's
  `apply` runs `resolve_merge` and `pick_merge_policy` with its own
  base);
- `EMPTY_ROOT`, the root of an empty keyset (nontainer checks
  `commit.root == EMPTY_HASH`).

TypeScript drops `VersionedKV`, `VersionedBase`, `Staged`,
`jsonEncoder`/`jsonDecoder`, and the tree classes (`Hamt`, `Keyset`,
`NodeCache`, entry encoding) from its exports. `SequenceMatcher` stays
exported (the world uses it), now from the core. The sync API, the
`github` subpath and the backends stay.

Aligned in both:

- `now`, the replaceable clock, on `Repo`.
- The commit-scan recoverer.
- A byte-bounded value cache.
- One worktree model (above), which retires TypeScript's `lazy` option
  or makes it a no-op.

Every public change is listed in the CHANGELOG under a Breaking prefix
when it lands.

## Errors

The core reports errors as kinds with a message and fields
(`BranchMoved { branch, base, head }`, `MergeConflict { keys, errors }`,
`GcBusy { owner, expires_in }`, ...). Each host maps them to the
exception classes it exports today, with the same names in both
languages. An exception raised by a host callback — a merge function, a
recoverer — is carried through the core and re-raised in the host as
the error the API documents (a merge function's failure files its key as
a conflict, as now).

## Toolchain and distribution

- **Rust:** stable, edition 2024. Dependencies kept small: `sha2`, and
  a JSON reader; the canonical writer is our own, since it must match
  Python byte for byte (as `pyjson.ts` does today).
- **Python:** maturin, `abi3` wheels for CPython 3.10 and later, built
  in CI for Linux (x86_64 and aarch64, manylinux and musllinux), macOS
  (arm64 and x86_64) and Windows, plus an sdist that needs a Rust
  toolchain to build. `pip install kvgit` stays dependency-free on every
  platform with a wheel.
- **TypeScript:** wasm-bindgen, with the wasm shipped inside the npm
  package. It must load under Node 22 and in Vite builds, in the main
  thread and in workers. Initialization happens inside `Repo.open`,
  which is already async, so callers see no new step. The module's
  bytes are inlined in the package as base64, so no bundler or server
  has to resolve a `.wasm` URL; the release build is 97 KB gzipped.
- **Versions:** both packages release together under one version
  number, starting at 0.5.0. `core` merges to `main` as that release.
- **Changelog:** one `CHANGELOG.md` at the root, entries marked by
  package where they differ.

## Testing

- **The corpus** (`corpus/`) is the shared specification: scenarios as
  data, each an initial store, a script of operations by named actors,
  and the expected results and final store bytes. The core runs it
  with an in-memory store and full control of interleaving; each host
  runs it through its public API. The existing goldens (`golden-v4`,
  `merge-golden`) become corpus scenarios; codec goldens
  (`golden-cbor`) stay with the TypeScript host.
- **The current suites** keep testing each host's public API. Tests of
  internals (the HAMT, the keyset, the commit engine's private state)
  move to Rust unit tests or become corpus scenarios.
- **Benchmarks** recorded before the swap (Python and TypeScript, on
  the current code) are the baseline the core is measured against.

## Migration

For stores the core would refuse, the last pure-Python release on
`main` (a 0.4.x) ships the tooling:

- **An inventory** that reports, per store, its stamp, how many commits
  lack generations, how many parent records are bare strings, and, for
  information, how many entries carry `created_at` and how many
  reachable blobs use commit-keyed pointers.
- **A backfill** that writes each commit's parents' generations,
  rewrites bare-string parent records as lists, and raises the stamp
  to 4. None of these is part of a commit's hash, so no commit changes
  name and commit hashes recorded elsewhere stay valid. It only adds
  keys or rewrites records to an equal value, so it is safe on a live
  store and safe to re-run.

Entries and blobs are not rewritten: the core reads both as stored.

## Consumers

- **agex-ts** (`world`, `kvsql`, `agex-ts`) and **agex-studio** depend
  on the published `@agex-ts/kvgit` rather than a workspace package.
  Development across both repositories uses a pnpm `link:` to a local
  checkout. A change that spans kvgit and the world becomes two PRs.
- **nontainer** moves off `kvgit.versioned.merge`,
  `kvgit.versioned.keyset` and `kvgit.hamt` to the public replacements
  above, and raises its floor to 0.5.0.

## Decisions

Made 2026-10-23:

- One Rust core under both hosts, as a sans-IO library.
- This repository is the home of the core and of both hosts;
  `@agex-ts/kvgit` moves here with its history.
- All or nothing: `core` merges to `main` only when both hosts run on
  the core.
- The public APIs stay mostly as they are; changes are made where they
  pay and listed as breaking.
- `VersionedBase`, `Staged` and `VersionedKV` leave the public API.
- The fallbacks for pre-v4 history go (the merge-base walk, bare-string
  parents, v2 and v3 stamps), after a hash-preserving backfill. Pre-v4
  entries and blob pointers stay readable as stored; nothing is
  rewritten.
- No Pyodide build of the Python package.

## Open questions

- **Python sync API:** expose the wire walk and replay publicly in
  0.5.0, or keep them internal until a transport wants them.
- **Python's Composite backend** caches mutable keys per tier and has
  been a source of stale-HEAD bugs; it stays a host backend, but whether
  the content cache now covers its purpose is worth asking.
