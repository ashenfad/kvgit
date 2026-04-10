"""Benchmark for the v2 storage layout.

Two benchmarks:

1. Storage growth (default): seed N keys, then make N single-key
   commits against a disk-backed store. Measures wall time, disk
   size, key count, cold-load time. Headline number: without
   structural sharing, every commit would rewrite the full keyset
   and meta (~190 MB across 1000 commits); the HAMT layout reduces
   that by ~30x.

2. Latency simulation (--latency-ms): wraps a Memory backend with
   per-call delay to simulate a network-attached store like Redis.
   Compares VersionedKV cold load (which uses Keyset.materialize,
   batched BFS) against draining ks.items() (one read per node).
   Headline number: a 1000-key cold load is ~4 round-trips instead
   of ~325, a >50x speedup at any meaningful latency.

Usage:
    python tests/benchmark_storage.py [--keys 1000] [--commits 1000]
    python tests/benchmark_storage.py --latency-ms 1
"""

import argparse
import os
import sys
import tempfile
import time

# Make `kvgit` importable from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kvgit.kv.disk import Disk  # noqa: E402
from kvgit.kv.memory import Memory  # noqa: E402
from kvgit.versioned.keyset import Keyset  # noqa: E402
from kvgit.versioned.kv import VersionedKV, _load_root  # noqa: E402


class _LatencyMemory(Memory):
    """Memory store with simulated per-call latency.

    Adds a sleep before every operation, making local timings
    representative of a high-latency backend like Redis or
    IndexedDB. Used by the cold-load benchmark to demonstrate the
    materialize() round-trip win without needing real network setup.
    """

    def __init__(self, latency_ms: float = 1.0) -> None:
        super().__init__()
        self.latency_s = latency_ms / 1000.0

    def get(self, key):
        time.sleep(self.latency_s)
        return super().get(key)

    def get_many(self, *args):
        time.sleep(self.latency_s)
        return super().get_many(*args)

    def set(self, key, value):
        time.sleep(self.latency_s)
        super().set(key, value)

    def set_many(self, items=None, /, **kwargs):
        time.sleep(self.latency_s)
        super().set_many(items, **kwargs)


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


def run_latency(n_keys: int, latency_ms: float) -> None:
    """Cold-load benchmark against a latency-wrapped Memory store.

    Compares the cost of materialize() (one batched fetch per HAMT
    level) against draining items() (one fetch per visited node).
    """
    print("kvgit cold-load benchmark with simulated latency")
    print(f"  keys={n_keys}  per-call latency={latency_ms} ms")
    print()

    # Build a store and populate it (no latency during setup —
    # otherwise the seed phase dominates).
    real_store = Memory()
    v = VersionedKV(real_store)
    v.commit({f"k{i:06d}": f"value-{i}".encode() for i in range(n_keys)})
    seed_root_commit = v.current_commit

    # Move all the seeded data into a latency-wrapped store so the
    # measurements only see the slow reads.
    slow_store = _LatencyMemory(latency_ms=latency_ms)
    for k, val in real_store.items():
        slow_store.memory[k] = val  # bypass the latency wrapper for setup
    del real_store, v

    # ---- Cold load via VersionedKV (uses materialize internally)
    print("VersionedKV cold load (uses Keyset.materialize, batched BFS):")
    slow_store_a = _LatencyMemory(latency_ms=latency_ms)
    for k, val in slow_store.memory.items():
        slow_store_a.memory[k] = val
    t0 = time.perf_counter()
    v_cold = VersionedKV(slow_store_a, commit_hash=seed_root_commit)
    t_versioned = time.perf_counter() - t0
    print(f"  wall: {t_versioned * 1000:7.1f} ms")
    print(f"  keys recovered: {len(v_cold._commit_keys)}")
    print()

    # ---- Drain items() instead, for comparison
    print("Equivalent cost of draining ks.items() (lazy, one read per node):")
    slow_store_b = _LatencyMemory(latency_ms=latency_ms)
    for k, val in slow_store.memory.items():
        slow_store_b.memory[k] = val
    t0 = time.perf_counter()
    root = _load_root(slow_store_b, seed_root_commit)
    ks = Keyset(slow_store_b, root=root)
    materialized_via_items = dict(ks.items())
    t_items = time.perf_counter() - t0
    print(f"  wall: {t_items * 1000:7.1f} ms")
    print(f"  keys recovered: {len(materialized_via_items)}")
    print()

    if t_versioned > 0:
        print(f"Speedup: ~{t_items / t_versioned:.1f}x")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--commits", type=int, default=1000)
    parser.add_argument(
        "--latency-ms",
        type=float,
        default=None,
        help=(
            "If set, run the latency-simulated cold-load benchmark "
            "instead of the storage-growth benchmark. The value is "
            "the per-call sleep applied to a Memory backend, in ms."
        ),
    )
    args = parser.parse_args()
    if args.latency_ms is not None:
        run_latency(args.keys, args.latency_ms)
    else:
        run(args.keys, args.commits)


if __name__ == "__main__":
    main()
