"""Generate tests/fixtures/golden-v4.json from Python kvgit.

The fixture is what kvgit-ts must reproduce byte for byte:

- ``json``: Python's ``json.dumps`` output for values kvgit hashes and
  stores (strings that need escaping, floats in every format Python
  writes, dicts sorted by key), so the canonical encoder is checked
  directly.
- ``scenario``: a scripted history run on a fresh in-memory store at
  fixed clock times, and the whole store after each step, every key and
  every byte. kvgit-ts replays the same steps at the same times and
  must hold the same store. A tag (with a non-ASCII name and info) is
  one of the steps, and so are a branch merge (a merge function, a
  prefix ``MergeChoice``), a fast-forward, a cherry-pick and a revert.

Regenerate (kvgit 0.4.x):

    uv run --project ~/git/kvgit python scripts/golden-v4.py > tests/fixtures/golden-v4.json
"""

import base64
import json
import math
import sys
from importlib.metadata import version

import kvgit.versioned.kv as kv_module
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import VersionedKV, create_tag
from kvgit.versioned.protocol import MergeChoice


class Clock:
    """``time.time`` for kvgit, set by the scenario before each step."""

    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now


def keys_of(result) -> dict[str, list[str]]:
    """What a merge result says it decided and carried."""
    return {"auto": list(result.auto_merged_keys), "carried": list(result.carried_keys)}


def dump_store(store: Memory) -> dict[str, str]:
    return {
        key: base64.b64encode(value).decode("ascii")
        for key, value in sorted(store.items())
    }


def json_cases() -> list[dict]:
    strings = [
        "plain",
        'quote " and backslash \\',
        "tab\tnewline\nreturn\rbell\x07nul\x00del\x7f",
        "slash / stays",
        "é ü ñ",
        "日本語",
        "emoji 😀 and 𝄞",
        "line sep   para sep  ",
        "",
    ]
    floats = [
        0.0,
        -0.0,
        1.0,
        -1.5,
        0.5,
        0.1 + 0.2,
        1 / 3,
        1e15,
        1e16,
        1.5e16,
        123456789012345.6,
        1e-4,
        1e-5,
        1.5e-7,
        1e21,
        1e22,
        5e-324,
        1.7976931348623157e308,
        1760000001.5,
        1760000002.25,
        1760000000.0,
        2.0**53,
        2.0**60,
        math.inf,
        -math.inf,
        math.nan,
    ]
    dicts = [
        {"b": 1, "a": 2, "B": 3, "é": 4, "z": 5, "😀": 6, "￿": 7, "": 8},
        {"nested": {"y": [1, 2.0, "x"], "x": None}, "flag": True, "off": False},
    ]
    cases = []
    for s in strings:
        cases.append({"kind": "string", "value": s, "dumps": json.dumps(s)})
    for f in floats:
        cases.append({"kind": "float", "value": repr(f), "dumps": json.dumps(f)})
    for d in dicts:
        cases.append(
            {
                "kind": "dict",
                "value": d,
                "dumps": json.dumps(d, separators=(",", ":")),
                "sorted": json.dumps(d, sort_keys=True, separators=(",", ":")),
            }
        )
    return cases


def scenario() -> list[dict]:
    clock = Clock()
    kv_module.time.time = clock.time  # kvgit reads the clock through this module
    store = Memory()
    steps: list[dict] = []

    def record(name: str, at: float, **extra) -> None:
        steps.append({"step": name, "time": at, "store": dump_store(store), **extra})

    clock.now = 1760000000.0
    main = VersionedKV(store)  # stamps the version, mints the root commit
    record("open", clock.now)

    # Enough keys that the HAMT splits into branch nodes, one non-ASCII
    # key, binary values, and info with every JSON kind.
    clock.now = 1760000001.5
    updates = {f"k/{i:02d}": f"value {i}".encode() for i in range(12)}
    updates["a"] = b"1"
    updates["b"] = b"hello"
    updates["ünï/ключ"] = b"\xff\x00\x01"
    info = {
        "msg": "first",
        "n": 2,
        "f": 0.5,
        "whole": 2.0,
        "é": "ü",
        "list": [1, None, True],
    }
    result = main.commit(updates=updates, info=info)
    record("first", clock.now, commit=result.commit)

    clock.now = 1760000002.25
    result = main.commit(updates={"a": b"2", "k/03": b"value 1"}, removals={"b"})
    record("second", clock.now, commit=result.commit)

    clock.now = 1760000003.0
    feature = main.create_branch("feat")
    result = feature.commit(updates={"feat-only": b"f"}, info={"branch": "feat"})
    record("branch", clock.now, commit=result.commit)

    clock.now = 1760000003.5
    create_tag(store, "v1/ünï", result.commit, {"note": "tagged", "ratio": 0.25})
    record("tag", clock.now, commit=result.commit)

    # Two writers at the same head: the first fast-forwards, the second
    # loses the race and merges (its own commit, then a merge commit).
    clock.now = 1760000004.0
    w1 = VersionedKV(store)
    w2 = VersionedKV(store)
    result = w1.commit(updates={"x": b"from w1"})
    record("writer one", clock.now, commit=result.commit)
    clock.now = 1760000005.0
    result = w2.commit(updates={"y": b"from w2"}, info={"merge": True})
    record(
        "writer two",
        clock.now,
        commit=result.commit,
        strategy=result.strategy,
        keys=keys_of(result),
    )

    # A branch merged back with both sides moved: a key both added (a
    # merge function joins them), a prefix a MergeChoice hands to theirs,
    # and a key only they changed.
    clock.now = 1760000006.0
    main = VersionedKV(store)
    side = main.create_branch("side")
    main.commit(updates={"n": b"main", "cfg/a": b"main", "cfg/b": b"main"})
    clock.now = 1760000006.5
    side.commit(updates={"n": b"side", "cfg/a": b"side", "s": b"side"})
    record("diverge", clock.now)

    clock.now = 1760000007.0
    result = main.merge_heads(
        side.current_commit,
        merge_fns={"n": lambda old, ours, theirs: theirs + b"+" + ours},
        merge_prefixes={"cfg/": MergeChoice.THEIRS},
        info={"merge": "side"},
    )
    record(
        "merge",
        clock.now,
        commit=result.commit,
        strategy=result.strategy,
        keys=keys_of(result),
    )

    # A branch whose base is main's head merges as a fast-forward.
    clock.now = 1760000008.0
    ff = main.create_branch("ff")
    ff.commit(updates={"ff": b"1"})
    result = main.merge_heads(ff.current_commit)
    record(
        "fast forward",
        clock.now,
        commit=result.commit,
        strategy=result.strategy,
        keys=keys_of(result),
    )

    # One change from side, picked onto main, then undone.
    clock.now = 1760000009.0
    side.commit(updates={"pick": b"picked"}, removals={"s"})
    picked = side.current_commit
    parent = side.parents(picked)[0]
    clock.now = 1760000009.5
    result = main.apply_change(parent, picked, info={"pick": True})
    record(
        "cherry-pick",
        clock.now,
        commit=result.commit,
        strategy=result.strategy,
        keys=keys_of(result),
    )

    clock.now = 1760000010.0
    result = main.apply_change(picked, parent)
    record(
        "revert",
        clock.now,
        commit=result.commit,
        strategy=result.strategy,
        keys=keys_of(result),
    )
    return steps


if __name__ == "__main__":
    json.dump(
        {
            "generator": f"kvgit {version('kvgit')}, scripts/golden-v4.py",
            "json": json_cases(),
            "scenario": scenario(),
        },
        sys.stdout,
        indent=1,
        ensure_ascii=False,
    )
    sys.stdout.write("\n")
