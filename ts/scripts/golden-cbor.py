"""Generate tests/fixtures/golden-cbor.json from Python's cbor2.

The values a Python kvgit store holds under a CBOR codec, as cbor2
writes them, so kvgit-ts checks it reads each back with the type a JS
caller expects. Each case carries the encoded bytes (hex) and a
description of the value in JSON terms the TS test compares against.

Regenerate:

    uv run --with cbor2==5.6.5 python scripts/golden-cbor.py > tests/fixtures/golden-cbor.json
"""

import json
import sys
from importlib.metadata import version

import cbor2

CASES = [
    ("small int", 5, {"number": 5}),
    ("negative int", -17, {"number": -17}),
    ("int past 32 bits", 2**40, {"bigint": str(2**40)}),
    ("int past 64 bits", 2**70, {"bigint": str(2**70)}),
    ("float", 1.5, {"number": 1.5}),
    ("string", "é ünï 😀", {"string": "é ünï 😀"}),
    ("bytes", b"\xff\x00\x01", {"bytes": "ff0001"}),
    ("none", None, {"null": True}),
    ("bool", True, {"boolean": True}),
    (
        "list",
        [1, "two", None],
        {"array": [{"number": 1}, {"string": "two"}, {"null": True}]},
    ),
    (
        "dict with string keys",
        {"a": 1, "b": [b"x"]},
        {"object": {"a": {"number": 1}, "b": {"array": [{"bytes": "78"}]}}},
    ),
    ("set", {1, 2}, {"set": [{"number": 1}, {"number": 2}]}),
]


def main() -> None:
    out = {
        "generator": f"cbor2 {version('cbor2')}, scripts/golden-cbor.py",
        "cases": [
            {"name": name, "hex": cbor2.dumps(value).hex(), "expect": expect}
            for name, value, expect in CASES
        ],
    }
    json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
