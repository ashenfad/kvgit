"""Benchmark for the v2 storage layout.

Simulates a session-storage workload: seed a store with N keys, then
make N single-key commits. Measures wall-clock time, total disk size,
total store key count, and cold-load time.

Usage:
    python tests/benchmark_storage.py [--keys 1000] [--commits 1000]

Useful for tracking storage-growth regressions over time. The headline
numbers from this benchmark are also what justified the HAMT migration:
without structural sharing, each commit would have rewritten the full
keyset and meta, growing the store quadratically.
"""

import argparse
import os
import sys
import tempfile
import time

# Make `kvgit` importable from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kvgit.kv.disk import Disk  # noqa: E402
from kvgit.versioned.kv import VersionedKV  # noqa: E402


def _store_size(directory: str) -> int:
    """Total bytes used by the disk store."""
    total = 0
    for entry in os.scandir(directory):
        if entry.is_file():
            total += entry.stat().st_size
    return total


def run(n_keys: int, n_commits: int) -> None:
    print("kvgit storage benchmark")
    print(f"  keys={n_keys}  commits={n_commits}")
    print()

    with tempfile.TemporaryDirectory(prefix="kvgit_bench_") as d:
        store_dir = os.path.join(d, "store")
        backend = Disk(store_dir)
        v = VersionedKV(backend)

        # ---- Phase 1: seed
        print(f"Seeding {n_keys} keys...")
        t0 = time.perf_counter()
        v.commit({f"k{i:06d}": f"value-{i}".encode() * 4 for i in range(n_keys)})
        t_seed = time.perf_counter() - t0

        seed_bytes = _store_size(store_dir)
        seed_kv_keys = sum(1 for _ in backend.keys())
        print(f"  wall: {t_seed * 1000:6.0f} ms")
        print(f"  size: {seed_bytes // 1024:6d} KB")
        print(f"  keys: {seed_kv_keys:6d} store entries")
        print()

        # ---- Phase 2: many single-key commits
        print(f"Committing {n_commits} single-key changes...")
        t0 = time.perf_counter()
        for i in range(n_commits):
            target = f"k{(i * 7) % n_keys:06d}"
            v.commit({target: f"changed-{i}".encode()})
        t_commits = time.perf_counter() - t0

        final_bytes = _store_size(store_dir)
        final_kv_keys = sum(1 for _ in backend.keys())
        delta_bytes = final_bytes - seed_bytes
        delta_keys = final_kv_keys - seed_kv_keys
        print(
            f"  wall:        {t_commits * 1000:6.0f} ms total "
            f"({t_commits * 1000 / n_commits:5.2f} ms/commit avg)"
        )
        print(f"  size:        {final_bytes // 1024:6d} KB total")
        print(
            f"  size delta:  {delta_bytes // 1024:6d} KB "
            f"({delta_bytes / n_commits:6.0f} bytes/commit avg)"
        )
        print(
            f"  keys delta:  {delta_keys:6d} entries "
            f"({delta_keys / n_commits:5.1f} per commit avg)"
        )
        print()

        # For reference: what v1 would have written per commit.
        # v1 stored a full keyset dict (~80 bytes/entry × N) plus a full
        # meta dict (~120 bytes/entry × N) per commit, regardless of how
        # many keys actually changed.
        v1_per_commit = n_keys * 200
        v1_estimated = v1_per_commit * n_commits
        print("For reference, v1 would have written approximately:")
        print(
            f"  {v1_per_commit // 1024} KB per commit "
            f"({v1_estimated // 1024 // 1024} MB across {n_commits} commits)"
        )
        if delta_bytes > 0:
            print(f"  v2 reduction: ~{v1_estimated / delta_bytes:.0f}x smaller")
        print()

        # ---- Phase 3: cold load
        print("Cold load (fresh process would re-open the store):")
        del v, backend
        t0 = time.perf_counter()
        backend2 = Disk(store_dir)
        v2 = VersionedKV(backend2)
        t_load = time.perf_counter() - t0
        print(f"  wall: {t_load * 1000:6.1f} ms")
        print(f"  recovered: {len(v2._commit_keys)} keys")

        # Sanity-check that mutations actually persisted
        sample_key = "k000500"
        sample = v2.get(sample_key)
        assert sample is not None, f"sample key {sample_key} missing after reload"
        print(f"  sample {sample_key}: {sample!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--commits", type=int, default=1000)
    args = parser.parse_args()
    run(args.keys, args.commits)


if __name__ == "__main__":
    main()
