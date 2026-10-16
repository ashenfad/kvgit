"""Benchmark: commit time against branch size.

Makes one-key commits on a branch holding 1k, 10k and 100k keys, on the
in-memory backend, and prints the median and p90 time per commit. Not
collected by pytest.

Usage:
    uv run --extra dev python tests/benchmark_commit.py [sizes...]
"""

import os
import sys
import time

# Make `kvgit` importable from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kvgit.kv.memory import Memory
from kvgit.versioned.kv import VersionedKV

COMMITS = 200
WARMUP = 20


def bench(n: int) -> None:
    vk = VersionedKV(Memory())
    vk.commit({f"key/{i}": f"value {i}".encode() for i in range(n)})

    times = []
    for i in range(WARMUP + COMMITS):
        updates = {f"key/{i % n}": f"changed {i}".encode()}
        t0 = time.perf_counter()
        vk.commit(updates)
        if i >= WARMUP:
            times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    median = times[len(times) // 2]
    p90 = times[int(len(times) * 0.9)]
    print(f"{n:>7} keys: median {median:.3f} ms, p90 {p90:.3f} ms per one-key commit")


if __name__ == "__main__":
    sizes = [int(a) for a in sys.argv[1:]] or [1_000, 10_000, 100_000]
    for n in sizes:
        bench(n)
