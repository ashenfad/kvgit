"""Merges over structural diffs: the HAMT diff, the change-based
resolver, and what a merge reports."""

import random

import pytest
from old_resolver import diff_keysets, old_resolve_merge
from support import fork, pointers, worktree

from kvgit import MergeChoice, MergeConflict
from kvgit.hamt import Hamt
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import KeysetEntry, MetaEntry
from kvgit.versioned.merge import Change, resolve_merge


def dict_diff(a: dict, b: dict):
    added = {k: v for k, v in b.items() if k not in a}
    removed = {k: v for k, v in a.items() if k not in b}
    modified = {k: (a[k], b[k]) for k in a.keys() & b.keys() if a[k] != b[k]}
    return added, removed, modified


class TestHamtDiff:
    @pytest.mark.parametrize("seed", range(40))
    def test_matches_a_diff_of_the_contents(self, seed):
        rng = random.Random(seed)
        store = Memory()
        bucket_max = rng.choice([1, 2, 3, 8])
        base = {f"k{i}": b"%d" % rng.randrange(5) for i in range(rng.randrange(0, 120))}
        a = Hamt(store, bucket_max=bucket_max).persist(base)
        contents = dict(base)
        for _ in range(rng.randrange(0, 40)):
            op = rng.random()
            key = f"k{rng.randrange(0, 160)}"
            if op < 0.4 and contents:
                contents.pop(rng.choice(sorted(contents)))
            else:
                contents[key] = b"%d" % rng.randrange(5)
        updates = {k: v for k, v in contents.items() if base.get(k) != v}
        removals = [k for k in base if k not in contents]
        b, _ = a.updated(updates, removals)
        if rng.random() < 0.5:
            b = b.flush()  # sometimes read from the store, sometimes pending

        diff = a.diff(b)
        assert (diff.added, diff.removed, diff.modified) == dict_diff(base, contents)
        back = b.diff(a)
        assert (back.added, back.removed, back.modified) == dict_diff(contents, base)
        assert not any(a.diff(a))

    def test_a_missing_node_reads_as_empty(self):
        store = Memory()
        a = Hamt(store, bucket_max=2).persist({f"k{i}": b"1" for i in range(20)})
        b = a.persist({"k0": b"2"})
        for key in list(store.keys("hamt:")):
            if key[len("hamt:") :] == a.root:
                store.remove(key)
        diff = a.diff(b)
        assert set(diff.added) == set(b.materialize())
        assert not diff.removed and not diff.modified


# -- The resolver against the one it replaced --


def rules(rng: random.Random):
    """Random registrations over keys named p<0-2>/k<n>."""

    def concat(old, ours, theirs):
        return b"|".join(v or b"-" for v in (old, ours, theirs))

    def pick(old, ours, theirs):
        return (
            MergeChoice.OURS if (ours or b"") >= (theirs or b"") else MergeChoice.THEIRS
        )

    def boom(old, ours, theirs):
        raise RuntimeError("no")

    fns = [concat, pick, boom]
    policies = [concat, pick, boom, MergeChoice.OURS, MergeChoice.THEIRS]
    merge_fns = {
        f"p{rng.randrange(3)}/k{rng.randrange(12)}": rng.choice(policies)
        for _ in range(rng.randrange(0, 4))
    }
    merge_prefixes = {
        f"p{rng.randrange(3)}/": rng.choice(policies)
        for _ in range(rng.randrange(0, 3))
    }
    if rng.random() < 0.3:
        merge_prefixes["p1/k1"] = rng.choice(policies)  # a longer prefix
    default = rng.choice([None, None, *fns])
    return merge_fns, merge_prefixes, default


def mutate(rng, keyset: dict, fresh):
    out = dict(keyset)
    for _ in range(rng.randrange(0, 8)):
        key = f"p{rng.randrange(3)}/k{rng.randrange(12)}"
        if key in out and rng.random() < 0.3:
            del out[key]
        else:
            out[key] = fresh()
    return out


def changes(a: dict, b: dict, entry) -> dict[str, Change]:
    return {
        k: Change(entry(a[k]) if k in a else None, entry(b[k]) if k in b else None)
        for k in a.keys() | b.keys()
        if a.get(k) != b.get(k)
    }


@pytest.mark.parametrize("seed", range(300))
def test_resolves_as_the_full_keyset_resolver_did(seed):
    rng = random.Random(seed)
    blobs: dict[str, bytes] = {}
    counter = iter(range(10**6))

    def fresh() -> str:
        pointer = f"ptr{next(counter)}"
        # A few distinct values, so distinct pointers often hold equal
        # bytes, as blobs from before content keys can.
        blobs[pointer] = b"v%d" % rng.randrange(4)
        return pointer

    def entry(pointer: str) -> KeysetEntry:
        return KeysetEntry(blob=pointer, meta=MetaEntry(size=len(blobs[pointer])))

    lca = {f"p{rng.randrange(3)}/k{i}": fresh() for i in range(rng.randrange(0, 10))}
    ours = mutate(rng, lca, fresh)
    theirs = mutate(rng, lca, fresh)
    if rng.random() < 0.2:  # the same change on both sides
        key = f"p0/k{rng.randrange(12)}"
        ours[key] = theirs[key] = fresh()
    if lca and rng.random() < 0.3:  # the same key removed on both sides
        key = rng.choice(sorted(lca))
        ours.pop(key, None)
        theirs.pop(key, None)
    merge_fns, merge_prefixes, default = rules(rng)

    try:
        old_keyset, old_values = old_resolve_merge(
            lca,
            ours,
            theirs,
            diff_keysets(lca, ours),
            diff_keysets(lca, theirs),
            blobs.get,
            merge_fns,
            default,
            merge_prefixes,
        )
        old_conflicts = None
    except MergeConflict as e:
        old_conflicts = (set(e.conflicting_keys), set(e.merge_errors))

    try:
        resolution = resolve_merge(
            changes(lca, ours, entry),
            changes(lca, theirs, entry),
            blobs.get,
            merge_fns,
            default,
            merge_prefixes,
        )
        new_conflicts = None
    except MergeConflict as e:
        new_conflicts = (set(e.conflicting_keys), set(e.merge_errors))

    assert new_conflicts == old_conflicts
    if old_conflicts is not None:
        return

    merged = dict(ours)
    for key, e in resolution.updates.items():
        merged[key] = e.blob
    for key in resolution.removals:
        merged.pop(key)
    for key in resolution.merged_values:
        merged[key] = "merged-value"
    old_state = {**old_keyset, **dict.fromkeys(old_values, "merged-value")}
    assert merged == old_state
    assert resolution.merged_values == old_values
    # Minimal: nothing restates what ours already holds.
    assert all(ours.get(k) != e.blob for k, e in resolution.updates.items())
    assert all(k in ours for k in resolution.removals)


# -- What a merge reports --


class TestReportedKeys:
    def _diverged(self):
        wt = worktree()
        wt["both"] = 0
        wt["mine"] = 0
        wt["yours"] = 0
        wt["owned/x"] = 0
        wt["same"] = 0
        wt.commit()
        dev = fork(wt, "dev")
        dev["both"] = 2
        dev["yours"] = 2
        dev["owned/x"] = 2
        dev["same"] = 9
        dev["new"] = 2
        dev.commit()
        wt["both"] = 1
        wt["mine"] = 1
        wt["same"] = 9
        wt.commit()
        return wt, dev

    def test_merge_reports_rule_resolved_and_carried_keys(self):
        wt, _ = self._diverged()
        wt.set_merge_prefix("owned/", MergeChoice.OURS)
        result = wt.merge(branch="dev", merge_fns={"both": lambda o, a, b: a + b})
        assert result.strategy == "three_way"
        assert set(result.auto_merged_keys) == {"both", "owned/x"}
        assert set(result.carried_keys) == {"yours", "new"}
        assert (wt["both"], wt["mine"], wt["yours"], wt["new"]) == (3, 1, 2, 2)
        assert (wt["owned/x"], wt["same"]) == (0, 9)

    def test_a_plain_commit_carries_nothing(self):
        wt = worktree()
        wt["k"] = 1
        result = wt.commit()
        assert result.strategy == "fast_forward"
        assert result.carried_keys == () and result.auto_merged_keys == ()

    def test_a_fast_forward_merge_carries_what_their_side_changed(self):
        wt = worktree()
        wt["a"] = 1
        wt["b"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        dev["b"] = 2
        dev["c"] = 3
        del dev["a"]
        dev.commit()
        result = wt.merge(branch="dev")
        assert result.strategy == "fast_forward"
        assert result.carried_keys == ("a", "b", "c")
        assert result.auto_merged_keys == ()

    def test_a_cherry_pick_carries_the_picked_change(self):
        wt = worktree()
        wt["a"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        dev["b"] = 2
        dev.commit()
        wt["c"] = 3
        wt.commit()
        result = wt.cherry_pick(dev.head)
        assert result.strategy == "apply"
        assert result.carried_keys == ("b",)
        assert (wt["a"], wt["b"], wt["c"]) == (1, 2, 3)


class TestRepoDiff:
    @pytest.mark.parametrize("seed", range(5))
    def test_matches_a_diff_of_the_full_keysets(self, seed):
        rng = random.Random(seed)
        wt = worktree()
        heads = []
        for _ in range(12):
            for _ in range(rng.randrange(1, 30)):
                key = f"k{rng.randrange(200)}"
                if key in wt and rng.random() < 0.3:
                    del wt[key]
                else:
                    wt[key] = rng.randrange(4)
            wt.commit()
            heads.append(wt.head)
        store = wt.repo.store
        for a in heads:
            for b in rng.sample(heads, 4):
                expected = diff_keysets(pointers(store, a), pointers(store, b))
                assert wt.repo.diff(a, b) == expected
