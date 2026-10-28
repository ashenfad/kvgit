"""The kvgit benchmarks (``bench/README.md``), run through the Python
package's public API on the in-memory backend.

Usage, from the repository root:

    uv run --extra dev python bench/kvgit_bench.py [--quick] [--json PATH] [--only SHAPE] [--store disk]

``--quick`` shrinks every workload to check the script runs; its
numbers mean nothing. ``--store disk`` runs every workload on the
``Disk`` backend, in a temporary directory, instead of in memory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from typing import Any

from kvgit import Repo
from kvgit.kv.base import KVStore
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory

HOST = "python"


# -- Measuring --


class Results:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def add(self, name: str, times_ms: list[float], **extra: Any) -> None:
        ordered = sorted(times_ms)
        row: dict[str, Any] = {
            "median_ms": round(statistics.median(ordered), 3),
            "n": len(ordered),
        }
        if len(ordered) >= 10:
            # Nearest rank: the smallest sample at or above 90% of them.
            row["p90_ms"] = round(ordered[math.ceil(len(ordered) * 0.9) - 1], 3)
        row.update(extra)
        self.rows[name] = row
        shown = ", ".join(f"{k} {v}" for k, v in row.items())
        print(f"{name:<24} {shown}", file=sys.stderr, flush=True)


def timed(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[float, Any]:
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return (time.perf_counter() - t0) * 1000, out


def repeat(fn: Callable[[], Any], times: int) -> list[float]:
    return [timed(fn)[0] for _ in range(times)]


# -- Stores --


class Stores:
    """Makes each workload's store: in memory, or on disk under one
    temporary directory removed when the run ends."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.root = tempfile.TemporaryDirectory(prefix="kvgit-bench-")
        self.made = 0

    def new(self) -> KVStore:
        if self.kind == "memory":
            return Memory()
        self.made += 1
        return Disk(f"{self.root.name}/{self.made}")


# -- The world's shape: one key per file --


def file_path(i: int) -> str:
    return f"files/d{i % 100:02d}/s{(i // 100) % 10}/f{i}.txt"


def file_body(i: int, version: int = 0) -> bytes:
    return (f"file {i} version {version}\n" * 12).encode()


def world(results: Results, stores: Stores, files: int, rounds: int) -> None:
    imports = []
    for _ in range(3):
        store = stores.new()
        repo = Repo(store, codec="bytes")
        wt = repo.worktree("main", create=True)
        for i in range(files):
            wt[file_path(i)] = file_body(i)
        imports.append(timed(wt.commit, info={"message": "import"})[0])
    results.add("world.bulk_import", imports, keys=files)

    def fresh() -> Any:
        return Repo(store, codec="bytes").worktree("main")

    results.add("world.open", repeat(fresh, rounds))

    results.add("world.keys_cold", repeat(lambda: fresh().keys(), rounds))
    w = fresh()
    w.keys()
    results.add("world.keys_warm", repeat(w.keys, rounds))

    def list_dir() -> Any:
        return sorted(k for k in w.keys() if k.startswith("files/d07/"))

    results.add("world.list_dir", repeat(list_dir, rounds))

    def iterate() -> None:
        r = fresh()
        names = sorted(r.keys())
        for start in range(0, len(names), 1000):
            r.get_many(*names[start : start + 1000])

    results.add("world.read_all", repeat(iterate, max(1, rounds // 2)))

    rng = random.Random(7)
    hot = [file_path(rng.randrange(files)) for _ in range(1000)]
    picks = [hot[rng.randrange(len(hot))] for _ in range(10_000)]

    def reads() -> None:
        r = fresh()
        for k in picks:
            r[k]

    results.add("world.reads_10k", repeat(reads, rounds))

    one = []
    for n in range(rounds * 10):
        wt[file_path(n)] = file_body(n, 1 + n)
        one.append(timed(wt.commit)[0])
    results.add("world.commit_one", one)

    bulk = []
    for n in range(rounds):
        for i in range(1000):
            wt[file_path((n * 1000 + i) * 7 % files)] = file_body(i, 100 + n)
        bulk.append(timed(wt.commit)[0])
    results.add("world.commit_1k", bulk)

    merges = []
    for n in range(rounds):
        repo.branches.create(f"side/{n}", wt.head)
        side = repo.worktree(f"side/{n}")
        for i in range(100):
            side[file_path(i * 13 + 1)] = file_body(i, 1000 + n)
            wt[file_path(i * 13 + 5)] = file_body(i, 2000 + n)
        side.commit()
        wt.commit()
        merges.append(timed(wt.merge, branch=f"side/{n}")[0])
    results.add("world.merge", merges)

    repo.branches.create("churn", wt.head)
    churn = repo.worktree("churn")
    for n in range(20):
        for i in range(100):
            churn[file_path(i * 31 + n)] = file_body(i, 3000 + n)
        churn.commit()
    repo.branches.delete("churn")
    for n in range(rounds):
        repo.branches.delete(f"side/{n}")
    time.sleep(0.01)
    ms, removed = timed(repo.gc, min_age=0)
    results.add("world.gc", [ms], removed=removed)


# -- kvsql's shape: a changeset log beside a chunked snapshot --


CHUNK = 8 << 20


def kvsql(
    results: Results, stores: Stores, entries: int, chunks: int, rounds: int
) -> None:
    rng = random.Random(11)
    store = stores.new()
    repo = Repo(store, codec="bytes")
    wt = repo.worktree("db/1", create=True)
    for i in range(90):
        wt[f"checkpoint/{i}"] = f"checkpoint {i}".encode()
    wt.commit()

    payload = [rng.randbytes(CHUNK) for _ in range(chunks)]

    def snapshot() -> None:
        for i in range(0, chunks, 2):
            for j in range(i, min(i + 2, chunks)):
                wt[f"snapshot/{j}"] = payload[j]
            wt.commit(info={"v": 1, "snapshot": {"seq": 0, "last": i + 2 >= chunks}})
        wt["epoch"] = b'{"last":0}'
        wt.commit()

    ms, _ = timed(snapshot)
    results.add("kvsql.snapshot", [ms], chunks=chunks, chunk_bytes=CHUNK)

    appends = []
    for seq in range(1, entries + 1):
        body = rng.randbytes(2048)
        wt["entry"] = body
        info = {
            "v": 1,
            "range": [seq, seq],
            "length": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "schema": 0,
        }
        appends.append(timed(wt.commit, info=info)[0])
    results.add("kvsql.append", appends, total_ms=round(sum(appends), 1))

    def fresh() -> Any:
        return Repo(store, codec="bytes").worktree("db/1")

    results.add("kvsql.open", repeat(fresh, rounds))

    def replay() -> int:
        r = Repo(store, codec="bytes")
        count = 0
        for commit in r.log(branch="db/1", first_parent=True):
            if "range" in (commit.info or {}):
                r.snapshot(commit=commit.hash)["entry"]
                count += 1
        return count

    results.add("kvsql.replay", repeat(replay, 3), entries=replay())

    merges = []
    for n in range(rounds):
        repo.branches.create(f"db/1-side/{n}", wt.head)
        side = repo.worktree(f"db/1-side/{n}")
        side[f"checkpoint/{n}"] = f"moved {n}".encode()
        wt["entry"] = rng.randbytes(2048)
        side.commit()
        wt.commit()
        merges.append(timed(wt.merge, branch=f"db/1-side/{n}")[0])
    results.add("kvsql.merge", merges)

    for n in range(rounds):
        repo.branches.delete(f"db/1-side/{n}")
    repo.branches.delete("db/1")
    time.sleep(0.01)
    ms, removed = timed(repo.gc, min_age=0)
    results.add("kvsql.gc", [ms], removed=removed)


# -- The run --


def machine() -> str:
    try:
        cpu = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        cpu = platform.processor() or platform.machine()
    return f"{cpu}, {platform.system()} {platform.release()}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--json", metavar="PATH")
    parser.add_argument("--only", choices=["world", "kvsql"])
    parser.add_argument("--store", choices=["memory", "disk"], default="memory")
    args = parser.parse_args()

    results = Results()
    stores = Stores(args.store)
    rounds = 2 if args.quick else 5
    if args.only in (None, "world"):
        world(results, stores, files=2_000 if args.quick else 100_000, rounds=rounds)
    if args.only in (None, "kvsql"):
        kvsql(
            results,
            stores,
            entries=50 if args.quick else 3_000,
            chunks=2 if args.quick else 8,
            rounds=rounds,
        )
    report = {
        "host": HOST,
        "runtime": f"CPython {platform.python_version()}",
        "machine": machine(),
        "quick": args.quick,
        "store": args.store,
        "results": results.rows,
    }
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2)
            f.write("\n")


if __name__ == "__main__":
    main()
