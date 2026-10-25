# Benchmarks

The same workloads in both packages, through their public APIs, on the
in-memory backend. Numbers from the code before the core are the
baseline (`baseline/`) the core is measured against; the gate after the
kernel stage asks that nothing that matters gets slower. `kernel/`
holds the numbers once both packages ran the kernel.

The in-memory backend keeps storage out of the numbers, so they show
what the engine costs: encoding, hashing, building and walking the
tree, the commit protocol.

## Running

From the repository root:

```bash
uv run --extra dev python bench/kvgit_bench.py --json python.json
(cd ts && pnpm build) && node bench/kvgit_bench.mjs --json ts.json
python bench/table.py bench/baseline/python.json python.json
```

`--only world` or `--only kvsql` runs one shape; `--quick` shrinks
every workload to check a script runs.

## Workloads

Times are medians over the runs shown in `n`, in milliseconds; a
workload with ten or more runs also gives its p90.

**The world's shape:** one key per file, 100,000 files of about 300
bytes in 100 directories.

| workload | measures |
|---|---|
| `world.bulk_import` | One commit of all 100,000 files. |
| `world.open` | Opening a repository on the store and a worktree on the branch, cold. |
| `world.keys_cold` | Opening, then listing every key. |
| `world.keys_warm` | Listing every key again on an open worktree. |
| `world.list_dir` | One directory's files, from the key listing. |
| `world.read_all` | Opening, then reading every file, a thousand at a time. |
| `world.reads_10k` | Opening, then 10,000 reads of 1,000 files. |
| `world.commit_one` | A commit changing one file. |
| `world.commit_1k` | A commit changing 1,000 files. |
| `world.merge` | A three-way merge of a branch where each side changed 100 files. |
| `world.gc` | A sweep reclaiming a deleted branch's 20 commits. |

TypeScript also runs the reads with `lazy` worktrees (`world.lazy.*`),
which hold the keyset's root and read the tree through a node cache:
the worktree model the core proposes, against the materialized one.

**kvsql's shape:** a changeset log on a branch whose tree holds about a
hundred keys, beside a database snapshot in 8 MB chunks.

| workload | measures |
|---|---|
| `kvsql.snapshot` | Writing a 64 MB snapshot, two chunks to a commit. |
| `kvsql.append` | A log commit: one 2 KB entry and its info. 3,000 of them. |
| `kvsql.open` | Opening a repository and a worktree on the log, cold. |
| `kvsql.replay` | Reading every entry back along the log's first parents. |
| `kvsql.merge` | A three-way merge of a branch that changed one key. |
| `kvsql.gc` | A sweep reclaiming the deleted log: 3,000 commits and the snapshot. |

## Baseline

Recorded on `core` at c40337a, on an Apple M4 (Darwin 25.5.0), with
CPython 3.13.5 and Node 25.9.0. The JSON files in `baseline/` hold the
same numbers with each workload's run count.

| workload | Python ms | TypeScript ms | TypeScript ÷ Python |
|---|---|---|---|
| `world.bulk_import` | 3,490.962 | 17,739.836 | 5.08× |
| `world.open` | 341.588 | 274.790 | 0.80× |
| `world.keys_cold` | 346.239 | 349.098 | 1.01× |
| `world.keys_warm` | 3.402 | 62.654 | 18.42× |
| `world.list_dir` | 6.147 | 63.038 | 10.26× |
| `world.read_all` | 567.317 | 519.187 | 0.92× |
| `world.reads_10k` | 363.738 | 286.700 | 0.79× |
| `world.commit_one` | 0.099 (p90 0.106) | 0.247 (p90 0.295) | 2.49× |
| `world.commit_1k` | 46.807 | 200.110 | 4.28× |
| `world.merge` | 9.183 | 20.918 | 2.28× |
| `world.gc` | 775.444 | 368.922 | 0.48× |
| `kvsql.snapshot` | 22.263 | 36.836 | 1.65× |
| `kvsql.append` | 0.063 (p90 0.067) | 0.137 (p90 0.146) | 2.17× |
| `kvsql.open` | 0.240 | 0.184 | 0.77× |
| `kvsql.replay` | 91.599 | 86.220 | 0.94× |
| `kvsql.merge` | 0.148 | 0.209 | 1.41× |
| `kvsql.gc` | 701.358 | 38.986 | 0.06× |
| `world.lazy.open` |  | 0.011 |  |
| `world.lazy.keys_cold` |  | 204.184 |  |
| `world.lazy.keys_warm` |  | 203.651 |  |
| `world.lazy.list_dir` |  | 209.159 |  |
| `world.lazy.read_all` |  | 1,240.038 |  |
| `world.lazy.reads_10k` |  | 173.682 |  |

What stands out, before any of the core:

- TypeScript is slower at building trees: the 100,000-file import takes
  five times as long, and commits two to four times.
- TypeScript lists keys twenty times slower on an open worktree.
- Python's sweep is slower everywhere, and seventeen times slower over
  kvsql's 3,000-commit log.
- A lazy worktree opens in microseconds, but `keys()` walks the tree on
  every call, three or four times the cost of a materialized listing,
  and reading every file costs two and a half times as much. Repeated
  reads of a working set come out ahead once the open is counted.

## The kernel

Recorded on `core` with the kernel stage's last PR, on the same
machine: both packages building, reading and diffing keyset trees, and
hashing commits, in the core. Each package still runs its own engine
around it.

**Python** (`kernel/python.json`):

| workload | before ms | after ms | after ÷ before |
|---|---|---|---|
| `world.bulk_import` | 3,490.962 | 1,995.403 | 0.57× |
| `world.open` | 341.588 | 238.596 | 0.70× |
| `world.keys_cold` | 346.239 | 251.419 | 0.73× |
| `world.keys_warm` | 3.402 | 4.070 | 1.20× |
| `world.list_dir` | 6.147 | 8.865 | 1.44× |
| `world.read_all` | 567.317 | 450.877 | 0.79× |
| `world.reads_10k` | 363.738 | 252.100 | 0.69× |
| `world.commit_one` | 0.099 (p90 0.106) | 0.068 (p90 0.076) | 0.69× |
| `world.commit_1k` | 46.807 | 29.032 | 0.62× |
| `world.merge` | 9.183 | 8.190 | 0.89× |
| `world.gc` | 775.444 | 595.303 | 0.77× |
| `kvsql.snapshot` | 22.263 | 21.024 | 0.94× |
| `kvsql.append` | 0.063 (p90 0.067) | 0.043 (p90 0.046) | 0.68× |
| `kvsql.open` | 0.240 | 0.171 | 0.71× |
| `kvsql.replay` | 91.599 | 77.746 | 0.85× |
| `kvsql.merge` | 0.148 | 0.136 | 0.92× |
| `kvsql.gc` | 701.358 | 457.397 | 0.65× |

**TypeScript** (`kernel/ts.json`):

| workload | before ms | after ms | after ÷ before |
|---|---|---|---|
| `world.bulk_import` | 17,739.836 | 4,815.519 | 0.27× |
| `world.open` | 274.790 | 318.396 | 1.16× |
| `world.keys_cold` | 349.098 | 363.051 | 1.04× |
| `world.keys_warm` | 62.654 | 42.829 | 0.68× |
| `world.list_dir` | 63.038 | 45.486 | 0.72× |
| `world.read_all` | 519.187 | 521.785 | 1.01× |
| `world.reads_10k` | 286.700 | 314.622 | 1.10× |
| `world.lazy.open` | 0.011 | 0.009 | 0.82× |
| `world.lazy.keys_cold` | 204.184 | 250.496 | 1.23× |
| `world.lazy.keys_warm` | 203.651 | 251.419 | 1.23× |
| `world.lazy.list_dir` | 209.159 | 250.144 | 1.20× |
| `world.lazy.read_all` | 1,240.038 | 1,034.178 | 0.83× |
| `world.lazy.reads_10k` | 173.682 | 102.020 | 0.59× |
| `world.commit_one` | 0.247 (p90 0.295) | 0.164 (p90 0.224) | 0.66× |
| `world.commit_1k` | 200.110 | 72.655 | 0.36× |
| `world.merge` | 20.918 | 16.976 | 0.81× |
| `world.gc` | 368.922 | 501.860 | 1.36× |
| `kvsql.snapshot` | 36.836 | 34.091 | 0.93× |
| `kvsql.append` | 0.137 (p90 0.146) | 0.089 (p90 0.100) | 0.65× |
| `kvsql.open` | 0.184 | 0.239 | 1.30× |
| `kvsql.replay` | 86.220 | 103.036 | 1.20× |
| `kvsql.merge` | 0.209 | 0.332 | 1.59× |
| `kvsql.gc` | 38.986 | 107.664 | 2.76× |

Python is faster everywhere the core is used: 30% faster opening and
reading a large worktree, 40% faster building trees, and a quarter to a
third faster sweeping. Its listing rows never reach the core; they pass
over a set of keys the worktree already holds, and are a millisecond or
three slower over 100,000 keys the binding made.

TypeScript builds trees two to four times as fast, and a lazy worktree's
repeated reads take 40% less time, but some reads are slower:

- **GC**, by 130 ms over 100,000 files and 70 ms over kvsql's log. A
  sweep walks each orphan's keyset as an operation of its own, and each
  pays a fixed cost crossing into wasm; 3,000 small walks add up. A
  sweep run by the core is one operation whose walks batch across
  commits.
- **Small operations** pay the same fixed cost: kvsql's replay, 3,000
  reads of a hundred-key tree, by 17 ms, and its open and merge by a
  fraction of a millisecond.
- **Opening a large worktree**, by 40 ms: materializing 100,000 entries
  as JS objects. Whether a worktree materializes at all is the question
  below.
- **Lazy listings**, by 40 to 50 ms: each walks the whole tree, and the
  core parses every node where V8's `JSON.parse` did before.

### The worktree model

A worktree that holds its root and reads through a node cache (`lazy`),
against one that materializes its keyset, on the world's 100,000 files,
in TypeScript on the core:

| | materialized ms | root and cache ms |
|---|---|---|
| open | 318.4 | 0.009 |
| `keys()`, after opening | 42.8 | 251.4 |
| one directory's files | 45.5 | 250.1 |
| read every file, opening included | 521.8 | 1,034.2 |
| 10,000 reads of 1,000 files, opening included | 314.6 | 102.0 |

Opening is free and a working set reads three times faster, but every
listing walks the whole tree: six times a materialized listing, a
quarter of a second at this size. One model with nothing materialized
fits a worktree that is opened often and listed rarely; a world-sized
tree listed on every directory read wants a key index cached beside the
root.

### Values stay out

The core sees keyset entries and tree nodes, never a value: each host
encodes a value and hashes it itself (`hashlib`, WebCrypto), and hands
the core the blob key in an entry. kvsql's 64 MB snapshot is written
faster than before in both packages.

### Loading the wasm

The module's bytes ship inside the npm package and are instantiated on
first use, the same way under Node, Vite and workers, with no URL to
resolve. Built with `[profile.wasm]` (`opt-level = "z"`, LTO,
`panic = "abort"`), it is 251,542 bytes, 97 KB gzipped, and about
400 KB of the built package as base64.

