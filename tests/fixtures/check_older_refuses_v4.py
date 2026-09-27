"""Run a released kvgit against a v4 store and report what it does.

Build a v4 disk store with this checkout, then point an older release
at it, from outside the repository so the checkout cannot shadow it:

    uv run python tests/fixtures/check_older_refuses_v4.py build /tmp/v4store
    cd /tmp && uv run --isolated --no-project --with 'kvgit[disk]==0.3.9' \\
        python "$REPO/tests/fixtures/check_older_refuses_v4.py" probe /tmp/v4store

``probe`` tries every entry point that opens or sweeps a store and
reports whether each refused, then whether the store is byte-identical.
Probe a copy: an older release may do part of what it is asked.
"""

import base64
import hashlib
import json
import sys
from importlib.metadata import version
from pathlib import Path

import kvgit
from kvgit.kv.disk import Disk
from kvgit.versioned.kv import clean_orphans, deep_clean


def build(path: str) -> None:
    fixture = Path(__file__).parent / "v3_store.json"
    data = json.loads(fixture.read_text())
    disk = Disk(path)
    disk.set_many({k: base64.b64decode(v) for k, v in data["store"].items()})
    s = kvgit.Staged(kvgit.VersionedKV(disk))
    s["count"] = 3
    s.commit()
    print("stamp", disk.get("__kvgit_version__"), "keys", len(list(disk.keys())))
    disk.close()


def digest(path: str) -> tuple[str, int]:
    disk = Disk(path)
    h = hashlib.sha256()
    keys = sorted(disk.keys())
    for k in keys:
        h.update(k.encode())
        h.update(disk.get(k))
    disk.close()
    return h.hexdigest()[:12], len(keys)


def probe(path: str) -> None:
    print("kvgit", version("kvgit"), "from", kvgit.__file__)
    before = digest(path)
    attempts = {
        "open main": lambda: kvgit.store(kind="disk", path=path),
        "open dev": lambda: kvgit.store(kind="disk", path=path, branch="dev"),
        "clean_orphans": lambda: clean_orphans(Disk(path), min_age=0),
        "deep_clean": lambda: deep_clean(Disk(path), min_age=0, grace=0),
        "delete_branches": lambda: kvgit.delete_branches(
            "dev", kind="disk", path=path, min_age=0
        ),
        "delete_tags": lambda: kvgit.delete_tags("v1", kind="disk", path=path),
    }
    for name, attempt in attempts.items():
        try:
            attempt()
        except Exception as e:  # noqa: BLE001 — reporting is the point
            print(f"  {name}: refused ({type(e).__name__}: {str(e)[:60]})")
        else:
            print(f"  {name}: DID NOT REFUSE")
    print("store unchanged:", digest(path) == before)


if __name__ == "__main__":
    {"build": build, "probe": probe}[sys.argv[1]](sys.argv[2])
