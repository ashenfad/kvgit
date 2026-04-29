"""Benchmark: chunked codec vs plain pickle on a realistic agent-style
workload — one DataFrame plus several derived slices.

Run with: ``uv run python tests/benchmark_codecs.py``

Reports total store size, encode time, decode time, and chunk counts
so it's easy to see whether the dedup story is paying off and whether
the codec adds non-trivial latency on top of plain pickle.
"""

from __future__ import annotations

import statistics
import time
from typing import Callable

import numpy as np
import pandas as pd

import kvgit
from kvgit.versioned.kv import CHUNK_PREFIX


# ---------- helpers ----------


def time_it(fn: Callable, repeat: int = 5) -> tuple[float, float]:
    """Run ``fn`` ``repeat`` times; return (median_seconds, stdev_seconds)."""
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples), (
        statistics.stdev(samples) if len(samples) > 1 else 0.0
    )


def store_bytes(s: kvgit.Staged) -> int:
    """Sum every entry in the underlying KV. Closest thing to 'on-disk size'."""
    total = 0
    for _, v in s.versioned.store.items():
        total += len(v)
    return total


def chunk_count(s: kvgit.Staged) -> int:
    return sum(1 for k in s.versioned.store.keys() if k.startswith(CHUNK_PREFIX))


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:7.2f} {unit}"
        n /= 1024
    return f"{n} B"


def fmt_ms(s: float) -> str:
    return f"{s * 1000:7.2f} ms"


# ---------- workload ----------


def base_df(rows: int = 1_000_000) -> "pd.DataFrame":
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        {
            "x": rng.normal(size=rows),
            "y": rng.normal(size=rows),
            "z": rng.normal(size=rows),
        }
    )


def workload_slicing_only(df: "pd.DataFrame") -> dict[str, object]:
    """Pure agent-style derivation: every variable is a view of df."""
    n = len(df)
    return {
        "full": df,
        "head": df.iloc[: n // 10],
        "tail": df.iloc[-n // 10 :],
        "middle": df.iloc[n // 4 : n // 2],
        "first_half": df.iloc[: n // 2],
    }


def workload_column_slicing(df: "pd.DataFrame") -> dict[str, object]:
    """Column subsets. For a single-block float DataFrame, contiguous
    column selections like ``df[['x','y']]`` produce C-contig views of
    the parent block — they dedup just like row slices. Single-column
    Series ``df['x']`` is a 1-D contig view of one block row."""
    return {
        "full": df,
        "x": df["x"],
        "y": df["y"],
        "xy": df[["x", "y"]],
        "yz": df[["y", "z"]],
    }


def workload_mixed(df: "pd.DataFrame") -> dict[str, object]:
    """Realistic mix: some views, some copies. The boolean filter and
    column-subset copies are independent buffers that don't dedup."""
    n = len(df)
    return {
        "full": df,
        "head": df.iloc[: n // 10],
        "tail": df.iloc[-n // 10 :],
        "middle": df.iloc[n // 4 : n // 2],
        "boolean_filter": df[df["x"] > 0].copy(),
        "x_only": df[["x"]].copy(),
    }


def workload_duplicates(df: "pd.DataFrame") -> dict[str, object]:
    """Pathological: the same DataFrame written N times under different
    keys. Plain pickle pays N× storage; chunked codec dedups to 1×."""
    return {f"copy_{i}": df for i in range(5)}


# ---------- runs ----------


def measure(
    label: str,
    data: dict,
    store_factory: Callable[[], kvgit.Staged],
) -> dict[str, float | int | str]:
    def write():
        s = store_factory()
        for k, v in data.items():
            s[k] = v
        s.commit()
        return s

    enc_med, _ = time_it(write, repeat=5)
    s = write()

    def read():
        s.reset()
        s._cache.clear()
        for k in data:
            _ = s[k]

    dec_med, _ = time_it(read, repeat=5)

    # Verify round-trip equality
    s.reset()
    s._cache.clear()
    for k, v_orig in data.items():
        v_round = s[k]
        if isinstance(v_orig, pd.DataFrame):
            pd.testing.assert_frame_equal(v_round, v_orig)
        elif isinstance(v_orig, pd.Series):
            pd.testing.assert_series_equal(v_round, v_orig)
        else:
            assert v_round.equals(v_orig)

    return {
        "label": label,
        "encode_ms": enc_med * 1000,
        "decode_ms": dec_med * 1000,
        "store_bytes": store_bytes(s),
        "chunks": chunk_count(s),
    }


def run_workload(name: str, build_data: Callable[[], dict]) -> None:
    data = build_data()

    def _bytes(v) -> int:
        if isinstance(v, pd.DataFrame):
            return int(v.memory_usage(deep=True).sum())
        if isinstance(v, pd.Series):
            return int(v.memory_usage(deep=True))
        return 0

    in_mem = sum(_bytes(v) for v in data.values())

    print(f"\n--- {name} ---")
    print(f"workload keys:           {list(data.keys())}")
    print(f"in-memory footprint:     {fmt_bytes(int(in_mem))}")

    plain = measure("plain pickle", data, lambda: kvgit.store())
    chunked = measure(
        "chunked (scientific)", data, lambda: kvgit.store(codecs="scientific")
    )

    header = f"{'codec':<22}{'on-disk':>14}{'encode':>14}{'decode':>14}{'chunks':>10}"
    print(header)
    print("-" * len(header))
    for r in (plain, chunked):
        print(
            f"{r['label']:<22}"
            f"{fmt_bytes(r['store_bytes']):>14}"
            f"{fmt_ms(r['encode_ms'] / 1000):>14}"
            f"{fmt_ms(r['decode_ms'] / 1000):>14}"
            f"{r['chunks']:>10}"
        )

    savings = 1 - chunked["store_bytes"] / plain["store_bytes"]
    enc_overhead = chunked["encode_ms"] / plain["encode_ms"] - 1
    dec_speedup = plain["decode_ms"] / chunked["decode_ms"]
    print(
        f"→ chunked saves {savings * 100:.1f}% of disk; "
        f"encode {enc_overhead * 100:+.0f}%; "
        f"decode {dec_speedup:.1f}x faster"
    )


def main() -> None:
    print("=" * 70)
    print("kvgit codec benchmark — DataFrame workloads")
    print("=" * 70)

    df = base_df()
    run_workload(
        "Pure row slicing (iloc views)",
        lambda: workload_slicing_only(df),
    )
    run_workload(
        "Pure column slicing (df[col], df[[cols]])",
        lambda: workload_column_slicing(df),
    )
    run_workload(
        "Mixed (some views, some independent copies)", lambda: workload_mixed(df)
    )
    run_workload(
        "Pathological duplicates (same df under N keys)",
        lambda: workload_duplicates(df),
    )


if __name__ == "__main__":
    main()
