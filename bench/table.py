"""Render benchmark results (``kvgit_bench`` JSON) as a Markdown table.

Usage:
    python bench/table.py BASELINE.json [OTHER.json]

With one file, the table lists its medians; with two, it lists both
and the second as a multiple of the first.
"""

from __future__ import annotations

import json
import sys


def load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def cell(row: dict | None) -> str:
    if row is None:
        return ""
    text = f"{row['median_ms']:,.3f}"
    if "p90_ms" in row:
        text += f" (p90 {row['p90_ms']:,.3f})"
    return text


def main(paths: list[str]) -> None:
    runs = [load(p) for p in paths]
    names = list(runs[0]["results"])
    for run in runs[1:]:
        names += [n for n in run["results"] if n not in names]
    head = ["workload"] + [f"{r['host']} ms" for r in runs]
    if len(runs) == 2:
        head.append("ratio")
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for name in names:
        rows = [r["results"].get(name) for r in runs]
        line = [f"`{name}`"] + [cell(row) for row in rows]
        if len(runs) == 2:
            a, b = rows
            line.append(
                f"{b['median_ms'] / a['median_ms']:.2f}×"
                if a and b and a["median_ms"]
                else ""
            )
        print("| " + " | ".join(line) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
