# kvgit core: plan

The stages that take `design.md` from a branch to a release. Each stage
is one or more PRs into `core`; each ends at an exit check, and two of
them are gates where the work can stop with nothing lost but the
branch. `main` keeps taking 0.4.x fixes meanwhile, and `core` merges
`main` in after each one. A fix on `main` brings a corpus scenario with
it once the corpus exists, so the branch has to pass it too.

## C0. Branch and design

`core` cut from `main` at 0.4.3, with `design.md` and this plan at the
root for markup.

**Exit:** both documents agreed.

## C1. One repository

Bring the TypeScript host in and set up the three builds, before any
code changes behaviour.

- Move the Python package to `python/kvgit` and its tests to
  `python/tests`; switch `pyproject.toml` to maturin with an empty
  `crates/py` extension (`kvgit._core`, one version function) so the
  wheel path is real from the start.
- Import `packages/kvgit-ts` from agex-ts with its history into `ts/`;
  make it build and test standalone (its own pnpm lockfile, no
  workspace links).
- `crates/core` and `crates/wasm` as empty crates; the wasm build
  loads in a Node test and in a Vite smoke page, in the main thread and
  in a worker.
- CI: `cargo test` and clippy; pytest on 3.10–3.14 against the built
  extension; vitest on Node and in a browser.
- Wheels built in CI for every target in the design (not published).

**Exit:** both existing suites pass unchanged in the new layout; wheels
and the npm tarball build; the wasm stub loads everywhere it must.

## C2. Corpus

The parity baseline, run against the two implementations as they are.

- The scenario format: initial store, operations by named actors,
  expected results, expected final store bytes. JSON, with bytes as
  base64.
- A runner in each host, driving its public API.
- Deterministic inputs. A scenario fixes the commit clock, the wall
  clock (lease and in-flight expiries) and the lease owner, so final
  store bytes compare exactly with nothing normalized. The runners set
  them through test seams: TypeScript's `now` option and its lease
  clock and owner, and patching `time.time` and the owner in Python.
  Python's public `now` still arrives in C7.
- Seed it: the v4 golden history, a v4 store carrying pre-v4 entries
  and blob pointers, the merge goldens, text-merge cases,
  merge-base and ancestry cases, conditional branch and tag deletes,
  `on_moved`, lost-race merges (two worktrees on one stale base), head
  recovery, GC (orphans, tags, in-flight markers, `min_age`).
- Sync scenarios (the wire walk and the checked replay) are written
  now and run by the TypeScript host only; Python has no sync
  primitives until C6.
- Races that need a true interleaving (a sweep starting between a
  commit's batch and its publish) are written now and run first by the
  core in C4; the hosts run them where a pausable test store can stage
  them.

**Exit:** both current implementations pass every scenario their API
can run (all but sync for Python), and any scenario on which they
disagreed is fixed on `main` first.

## C3. Kernel and toolchain (gate 1)

The pure parts in Rust, behind both hosts.

- Canonical JSON (Python's `json.dumps`, floats included), SHA-256,
  layout keys, keyset entries, HAMT nodes, lookups, updates, the
  structural diff, the commit hash.
- Both hosts call the core for these; the hosts still run their own
  I/O loops around them.
- Benchmarks before and after, in both hosts: open, one-key commit,
  bulk commit, merge, GC, over two shapes of store:
  - kvsql's: a log of a few thousand commits on a tree of about a
    hundred keys, with 8 MB snapshot chunks; writing a snapshot and
    replaying the log;
  - the world's: 100,000 files, listing, iteration and repeated reads.
- Values stay out of the core from the first kernel PR: the hosts hash
  blobs, and the benchmarks confirm no value is copied into wasm.
- The worktree-model measurement: a root-plus-cache worktree against a
  materialized one on the world's shape, for `keys()`, listing and
  repeated reads, starting from the numbers lazy mode recorded.
- Wasm loading settled (URL or inline) and the gzipped size recorded.

**Gate 1:** the wheels and the wasm build hold up in day-to-day use
(the `uv` and `pnpm` dev loops included), and the numbers are no worse
anywhere that matters. If not, stop: keep the corpus, drop the branch.

## C4. The write protocol (gate 2)

The sans-IO design proven on the hardest path.

- The request type, the executor, and a driver in each host.
- The commit path: waiting out a live sweep, the batch with its
  in-flight marker, the publish with its backup, the lost-race merge,
  `on_moved="raise"`, rollback of a failed attempt.
- Merge resolution and the text merge in the core, merge functions as
  host callbacks.
- The interleaving scenarios from C2, run by the core. The two that
  need a sweep (`sweep-before-publish`, and `lost-at-publish`, which
  ends with one) join when GC moves into the core in C5; until then
  the core's own tests stage the sweep by taking and giving back the
  lease between a commit's batch and its publish.

**Gate 2:** the protocol reads as straight-line code, each host's
driver stays small (on the order of a hundred lines), and the corpus
passes. If the request protocol turns out tangled, stop here and decide
whether the C3 kernel alone is worth keeping.

## C5. The rest of the engine

- Refs: branch and tag creation and conditional deletion, tag records.
- Head resolution, `repair_head`, the commit-scan recoverer.
- Merge, fast-forward, `apply`, cherry-pick, revert, `post_check`.
- Merge base, ancestry, log, diff, snapshots.
- GC: the lease, mark and sweep, the deep scan.
- The content cache, and the worktree model C3 chose: root plus
  cache, with the key index built on the first listing.

**Exit:** both hosts run entirely on the core; neither keeps any of its
own engine code; both suites and the corpus pass.

## C6. Sync

- The wire walk, the checked replay, publishing a synced head, sync
  heads and marks, `ancestryDelta`, `isAncestor` — in the core.
- `PackRemote`, the pack format and `FakeGithub` stay in the TypeScript
  host, on the core's primitives.
- Each pull's and push's decisions (fast-forward, created, diverged,
  gone) are in the core too, as steps the host calls the remote
  between.
- Python gets the primitives, a reference remote and pull and push,
  internal in 0.5.0 (`kvgit._sync`).
- What TypeScript's sync still does itself goes with it: the lease
  helpers (`tryLand`, `waitForGc`, `inFlightMarker`), the ancestry
  walk, and the entry encoding its replay builds trees with.

**Exit:** the TypeScript sync suites and the corpus's sync scenarios
pass, the latter in both hosts, and the live GitHub test against the
scratch repository passes.

Left for C7: TypeScript's `Keyset` and `Hamt` classes, with the entry
encoding, are reached now only through `VersionedKV`, and go with it.

## C7. Public surface

- Python: `kvgit.versioned`, `kvgit.hamt` and the keyset module leave
  the public API; the IndexedDB backend and the Pyodide guide go; the
  three-way resolve over caller-named values and `EMPTY_ROOT` arrive,
  replacing `resolve_merge` and `pick_merge_policy`; `now` on `Repo`;
  the value cache stays byte-bounded. The code only `VersionedKV` and
  `VersionedBase` reach (the history walk, `commit_changes`) goes with
  them.
- TypeScript: `VersionedKV`, `VersionedBase`, `Staged`, the JSON
  encoder pair, the tree classes, `encodeEntry`/`decodeEntry`,
  `diffKeysets` and `walkHistory` leave the exports, and the code only
  they reach goes with them; `SequenceMatcher` stays, as TypeScript's
  own; the value cache becomes byte-bounded; `lazy` becomes a no-op.
- Docs: a README per host, the storage format and the request protocol
  documented once, the CHANGELOG with every breaking change.

**Exit:** the consumers' uses of kvgit are covered: nontainer `main`,
agex-ts (`world`, `kvsql`, `agex-ts`) and agex-studio pass against the
branch's packages, linked locally.

Met 2026-10-27 against two consumer branches, which land with C8:
nontainer `main` and agex-ts's termish reach API this stage removes,
so they pass only with these changes, and C8 merges them before the
release.

- nontainer: `apply` resolves through `Repo.resolve` and
  `Repo.merge_rule`, and the empty-branch check compares against
  `EMPTY_ROOT` (branch `kvgit-05`); its kvgit cap rises to `<0.6` with
  the 0.5.0 floor.
- agex-ts: termish's `KvgitFS` runs on a `Worktree`, with
  `fileRecordCodec` and `polymorphicCodec` in place of the encoder
  pairs (branch `kvgit-05`), since `Staged` is gone. World, kvsql and
  agex-ts need no change.

Against the branch: nontainer 3,413 passed; termish 1,216, world 1,338,
kvsql 313 and agex-ts 222 passed, with agex-ts's own copy of the package
replaced by this one; agex-studio's 765 passed, its typecheck and build
clean, with the four packages linked.

## C8. Migration and release

- Check the stamps of the stores worth keeping; a store stamped v2 or
  v3 takes one commit under kvgit 0.4.x first.
- The docs brought in line with the code, in both packages.
- Python's `Disk` backend on one SQLite table, with
  `Disk.import_diskcache` for existing stores and a disk shape in the
  benchmarks.
- Publish workflows: wheels and sdist to PyPI, the npm package with its
  wasm, one version (npm has `@agex-ts/kvgit` 0.2.1, so the TypeScript
  package moves from 0.2 to 0.5).
- `core` merges to `main`; 0.5.0 releases both packages.
- agex-ts drops `packages/kvgit-ts` and depends on the published
  package; agex-studio and nontainer raise their floors.
- nontainer's 0.5 branch moves its disk stores with a one-off command,
  in a module of its own, that runs `Disk.import_diskcache` on a
  store's `kvgit` directory; opening a store not yet moved fails with
  the command's name. Both leave nontainer in a later release, once
  the stores worth keeping have moved.
- `design.md` and this plan condense into `docs/design.md`, the
  decisions and their reasons, and leave the root.

**Exit:** 0.5.0 published, and every consumer on it.
