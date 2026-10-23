"""Build the v3 store the compatibility tests read.

Run with a released kvgit that writes v3, never this checkout. From
outside the repository, so neither the project's environment nor its
source directory can shadow the released package:

    cd /tmp && uv run --isolated --no-project \
        --with 'kvgit[scientific]==0.3.9' \
        python "$REPO/tests/fixtures/make_v3_store.py" \
        "$REPO/tests/fixtures/v3_store.json"

It prints the path kvgit was imported from; check that it is not this
checkout.

The output holds every key of an in-memory store (base64 values) and a
manifest of what an older kvgit saw: branch heads, tags, each branch's
history and the values at its head.
"""

import base64
import json
import sys
from importlib.metadata import version

import numpy as np

import kvgit
from kvgit import Staged, VersionedKV, text_merge
from kvgit.kv.memory import Memory


def main(out_path: str) -> None:
    backend = Memory()
    main = Staged(VersionedKV(backend))
    main["greeting"] = "hello"
    main["count"] = 1
    main["notes"] = "alpha\nbeta\ngamma\n"
    main["shared"] = "same bytes on both sides"
    main["doomed"] = "removed on main"
    main.commit(info={"step": "seed"})

    dev = main.create_branch("dev")
    dev["dev_only"] = "only on dev"
    dev["notes"] = "alpha\nBETA\ngamma\n"
    dev.commit(info={"step": "dev edit"})

    main["count"] = 2
    del main["doomed"]
    main.commit(info={"step": "main edit"})
    main.tag("v1", info={"why": "fixture"})

    merged = main.create_branch("merged")
    merged.merge(dev.current_commit, default_merge=text_merge())

    # A branch that is deleted: its commits stay behind as orphans.
    gone = main.create_branch("gone")
    gone["gone_only"] = "orphaned payload"
    gone.commit()
    gone_head = gone.current_commit
    main.delete_branch("gone")

    sci = Staged(VersionedKV(backend, branch="sci"), **_chunked())
    sci["arr"] = np.arange(4096, dtype="float64")
    sci["arr_copy"] = np.arange(4096, dtype="float64")
    sci.commit()

    manifest: dict = {"kvgit": version("kvgit"), "branches": {}, "tags": {}}
    for name in ("main", "dev", "merged", "sci"):
        v = VersionedKV(backend, branch=name)
        manifest["branches"][name] = {
            "head": v.current_commit,
            "history": list(v.history(all_parents=True)),
        }
        if name != "sci":
            s = Staged(v)
            manifest["branches"][name]["values"] = {k: s[k] for k in sorted(s.keys())}
    manifest["tags"] = main.tags()
    manifest["orphan"] = gone_head

    dump = {k: base64.b64encode(v).decode() for k, v in backend.items()}
    with open(out_path, "w") as f:
        json.dump({"manifest": manifest, "store": dump}, f, indent=1, sort_keys=True)
    print(f"kvgit {manifest['kvgit']} ({kvgit.__file__}): {len(dump)} keys")


def _chunked():
    from kvgit.codecs import compose
    from kvgit.codecs.numpy import NumpyCodec

    encoder, decoder = compose(NumpyCodec(min_bytes=64))
    return {"encoder": encoder, "decoder": decoder}


if __name__ == "__main__":
    main(sys.argv[1])
