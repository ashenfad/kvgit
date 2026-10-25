"""Write float-repr.txt: doubles and the text CPython's ``repr`` gives
each, one per line as ``<16 hex digits of the bits> <repr>``.

    python crates/core/tests/fixtures/make_float_repr.py
"""

import random
import struct
from pathlib import Path


def bits(x: float) -> str:
    return struct.pack(">d", x).hex()


def doubles() -> list[float]:
    rng = random.Random(4)
    out: list[float] = []
    # Any bit pattern: mostly huge and tiny exponents, some subnormals.
    while len(out) < 1500:
        (x,) = struct.unpack(">d", rng.randbytes(8))
        if x == x and abs(x) != float("inf"):
            out.append(x)
    # Where repr switches between fixed and exponent notation.
    for e in range(-7, 19):
        for _ in range(20):
            out.append(rng.uniform(1, 10) * 10.0**e)
            out.append(float(rng.randrange(1, 10)) * 10.0**e)
    # Times as a clock gives them, and amounts as people write them.
    for _ in range(300):
        out.append(rng.uniform(1.6e9, 1.9e9))
        out.append(round(rng.uniform(-1e6, 1e6), rng.randrange(0, 8)))
        out.append(float(rng.randrange(-(2**60), 2**60)))
    return out


lines = [f"{bits(x)} {x!r}" for x in doubles()]
Path(__file__).with_name("float-repr.txt").write_text("\n".join(lines) + "\n")
