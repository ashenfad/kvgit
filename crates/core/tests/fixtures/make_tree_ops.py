"""Write tree-ops.json: batches of sets and removals applied in turn to
a tree by Python kvgit's HAMT, with the root after each batch.

Run with kvgit 0.4.3, the last release with a HAMT of its own, from
outside the repository so its source can't shadow the release:

    cd /tmp && uv run --isolated --no-project --with 'kvgit==0.4.3' \
        python "$REPO/crates/core/tests/fixtures/make_tree_ops.py"
"""

import json
import random
from pathlib import Path

from kvgit.hamt import EMPTY_HASH, Hamt

from kvgit.kv.memory import Memory


def run(bucket_max: int, seed: int, batches: int, pool: int) -> dict:
    rng = random.Random(seed)
    tree = Hamt(Memory(), prefix="t:", bucket_max=bucket_max)
    out = []
    for _ in range(batches):
        keys = [f"k{rng.randrange(pool)}" for _ in range(rng.randrange(1, 12))]
        sets = {k: f"v{rng.randrange(4)}" for k in keys[: len(keys) // 2 + 1]}
        removals = [k for k in keys[len(keys) // 2 + 1 :] if k not in sets]
        tree = tree.persist({k: v.encode() for k, v in sets.items()}, removals)
        out.append({"set": sets, "remove": removals, "root": tree.root})
    return {"bucket_max": bucket_max, "batches": out}


runs = [run(2, 1, 300, 40), run(8, 2, 300, 200), run(1, 3, 100, 12)]
assert runs[0]["batches"][0]["root"] != EMPTY_HASH
Path(__file__).with_name("tree-ops.json").write_text(json.dumps(runs, indent=1) + "\n")
