# Benchmarks

The same workloads in both packages, through their public APIs, on the
in-memory backend. Numbers from the code before the core are the
baseline (`baseline/`) the core is measured against; the gate after the
kernel stage asks that nothing that matters gets slower.

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
