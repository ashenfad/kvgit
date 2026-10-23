"""The common-ancestor search: generations, batching, and the fallback."""

import itertools
import random

import pytest

from kvgit.encoding import dumps, loads
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    COMMIT_ROOT,
    PARENT_COMMIT,
    PARENT_GENS,
    ROOT_COMMIT,
    VersionedKV,
    _generations,
    _merge_base_by_generation,
    _merge_base_by_walk,
    _NoGenerations,
    clean_orphans,
    load_parents,
    merge_base,
)


class CountingMemory(Memory):
    """Counts the batched reads of parent records."""

    def __init__(self):
        super().__init__()
        self.record_reads = 0

    def get_many(self, *args):
        keys = self._normalize_keys(args)
        if any(k.startswith("__parent_commit__") for k in keys):
            self.record_reads += 1
        return super().get_many(*args)


def commits(store):
    prefix = COMMIT_ROOT.replace("%s", "")
    return sorted(k[len(prefix) :] for k in store.keys(prefix))


def longest_paths(store):
    """Each commit's generation, worked out from parents alone."""
    memo = {}

    def gen(c):
        if c not in memo:
            parents = load_parents(store, c)
            memo[c] = 1 + max(gen(p) for p in parents) if parents else 0
        return memo[c]

    return {c: gen(c) for c in commits(store)}


def random_history(seed, steps=120, branch_count=4):
    """A DAG from random commits and merges across a few branches,
    criss-crosses included. Every commit writes its own key, so no merge
    ever conflicts."""
    rng = random.Random(seed)
    store = Memory()
    main = VersionedKV(store)
    main.commit({"seed": b"0"})
    handles = [main] + [main.create_branch(f"b{i}") for i in range(1, branch_count)]
    counter = itertools.count()
    for _ in range(steps):
        h = rng.choice(handles)
        if rng.random() < 0.3:
            other = rng.choice(handles)
            if other is not h:
                h.merge_heads(other.current_commit, fast_forward=rng.random() < 0.5)
                continue
        n = next(counter)
        h.commit({f"k{n}": str(n).encode()})
    return store


class TestGenerations:
    def test_every_new_commit_stores_its_parents_generations(self):
        store = random_history(1)
        truth = longest_paths(store)
        for c in commits(store):
            parents = load_parents(store, c)
            if not parents:
                assert store.get(PARENT_GENS % c) is None
                continue
            assert loads(store.get(PARENT_GENS % c)) == [truth[p] for p in parents]

    def test_the_root_commit_is_generation_zero(self):
        store = Memory()
        VersionedKV(store)
        assert _generations(store, [ROOT_COMMIT]) == {ROOT_COMMIT: 0}


class TestSearch:
    @pytest.mark.parametrize("seed", range(8))
    def test_agrees_with_the_full_walk_on_random_histories(self, seed):
        store = random_history(seed)
        every = commits(store)
        rng = random.Random(seed)
        pairs = [tuple(rng.sample(every, 2)) for _ in range(150)]
        for a, b in pairs:
            expected = _merge_base_by_walk(store, a, b)
            assert _merge_base_by_generation(store, a, b, None) == expected
            assert merge_base(store, a, b) == expected

    def test_criss_cross_ties_go_to_the_smallest_hash(self):
        store = Memory()
        main = VersionedKV(store)
        main.commit({"base": b"0"})
        dev = main.create_branch("dev")
        main.commit({"m": b"1"})
        dev.commit({"d": b"1"})
        m1, d1 = main.current_commit, dev.current_commit
        main.merge_heads(d1, fast_forward=False)
        dev.merge_heads(m1, fast_forward=False)
        a, b = main.current_commit, dev.current_commit
        assert merge_base(store, a, b) == min(m1, d1)
        assert _merge_base_by_walk(store, a, b) == min(m1, d1)

    def test_unrelated_histories_have_no_common_ancestor(self):
        store = Memory()
        one = VersionedKV(store)
        one.commit({"a": b"1"})
        store.set_many(**{PARENT_COMMIT % "island": dumps([])})
        assert merge_base(store, one.current_commit, "island") is None

    def test_a_lost_race_reads_the_same_whatever_the_history(self):
        def reads_for(history):
            store = CountingMemory()
            v = VersionedKV(store)
            for i in range(history):
                v.commit({f"k{i % 5}": str(i).encode()})
            a = VersionedKV(store)
            b = VersionedKV(store)
            a.commit({"x": b"1"})
            store.record_reads = 0
            b.commit({"y": b"2"})
            return store.record_reads

        assert reads_for(10) == reads_for(300) <= 3


class TestHistoryWithoutGenerations:
    def _strip(self, store, keep):
        """Make every commit outside ``keep`` look written before
        generations were stored."""
        for c in commits(store):
            if c not in keep:
                store.remove(PARENT_GENS % c)

    def test_search_falls_back_and_stays_correct(self):
        store = random_history(3)
        self._strip(store, keep=set())
        every = commits(store)
        rng = random.Random(3)
        for a, b in (tuple(rng.sample(every, 2)) for _ in range(100)):
            with pytest.raises(_NoGenerations):
                _merge_base_by_generation(store, a, b, None)
            assert merge_base(store, a, b) == _merge_base_by_walk(store, a, b)

    def test_a_commit_over_old_history_stores_true_generations(self):
        store = Memory()
        v = VersionedKV(store)
        for i in range(6):
            v.commit({f"k{i}": b"1"})
        dev = v.create_branch("dev")
        dev.commit({"d": b"1"})
        v.merge_heads(dev.current_commit, fast_forward=False)
        self._strip(store, keep=set())

        fresh = VersionedKV(store)  # knows nothing about generations
        fresh.commit({"new": b"1"})
        truth = longest_paths(store)
        new = fresh.current_commit
        parents = load_parents(store, new)
        assert loads(store.get(PARENT_GENS % new)) == [truth[p] for p in parents]

    def test_mixed_history_is_searched_correctly(self):
        store = random_history(5, steps=60)
        old = set(commits(store))
        self._strip(store, keep=set())
        # New history on top, with generations.
        main = VersionedKV(store)
        dev = VersionedKV(store, branch="b1")
        for i in range(10):
            main.commit({f"new-m{i}": b"1"})
            dev.commit({f"new-d{i}": b"1"})
        new = [c for c in commits(store) if c not in old]
        for a, b in itertools.combinations(new, 2):
            assert merge_base(store, a, b) == _merge_base_by_walk(store, a, b)


class TestCollection:
    def test_gc_removes_an_orphans_generations(self):
        store = Memory()
        main = VersionedKV(store)
        dev = main.create_branch("dev")
        dev.commit({"k": b"v"})
        orphan = dev.current_commit
        assert store.get(PARENT_GENS % orphan) is not None
        main.delete_branch("dev")
        clean_orphans(store, min_age=0)
        assert store.get(PARENT_GENS % orphan) is None
