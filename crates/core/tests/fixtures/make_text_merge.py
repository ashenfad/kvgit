"""Write text-merge.json: three-way text merges and what Python kvgit's
text merge made of each.

    uv run --extra dev python crates/core/tests/fixtures/make_text_merge.py
"""

import json
import random
from pathlib import Path

from kvgit.merges import CantMark, text_merge_result

ENDINGS = ["\n", "\n", "\n", "\r\n", "\r", "\x0c", " ", "\x85"]
WORDS = ["alpha", "beta", "gamma", "delta", "", "é", "<<<<<<< ours"]


def line(rng: random.Random) -> str:
    return rng.choice(WORDS) + rng.choice(ENDINGS)


def edit(rng: random.Random, lines: list[str]) -> list[str]:
    out = list(lines)
    for _ in range(rng.randrange(4)):
        at = rng.randrange(len(out) + 1)
        match rng.randrange(3):
            case 0:
                out.insert(at, line(rng))
            case 1 if out:
                del out[min(at, len(out) - 1)]
            case _ if out:
                out[min(at, len(out) - 1)] = line(rng)
    return out


def side(rng: random.Random, lines: list[str]) -> str | None:
    if rng.random() < 0.08:
        return None
    text = "".join(edit(rng, lines))
    # Sometimes the last line has no break.
    if text and rng.random() < 0.2:
        text = text.rstrip("\n")
    return text


def case(rng: random.Random) -> dict:
    base = [line(rng) for _ in range(rng.randrange(8))]
    old = None if rng.random() < 0.08 else "".join(base)
    ours, theirs = side(rng, base), side(rng, base)
    labels = rng.choice([("ours", "theirs"), ("main", "feature/x")])
    sides = [None if s is None else s.encode() for s in (old, ours, theirs)]
    merged, conflicted = text_merge_result(
        *sides, ours_label=labels[0], theirs_label=labels[1]
    )
    return {
        "old": old,
        "ours": ours,
        "theirs": theirs,
        "labels": list(labels),
        "merged": merged.decode(),
        "conflicted": conflicted,
    }


rng = random.Random(4)
cases = [case(rng) for _ in range(1500)]
for bad in (b"a\x00b", b"\xff\xfe"):
    try:
        text_merge_result(bad, b"x\n", b"y\n")
    except CantMark:
        pass
    else:
        raise AssertionError(bad)
assert any(c["conflicted"] for c in cases) and not all(c["conflicted"] for c in cases)
Path(__file__).with_name("text-merge.json").write_text(
    json.dumps(cases, indent=0, ensure_ascii=False) + "\n"
)
