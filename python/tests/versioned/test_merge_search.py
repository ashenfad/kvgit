"""The common-ancestor search: generations, batching, and the fallback."""

import itertools
import random

import pytest
from support import clean_orphans, fork

from kvgit import Repo
from kvgit._store import COMMIT_ROOT, EMPTY_HASH, PARENT_COMMIT, PARENT_GENS
from kvgit.encoding import dumps, loads
from kvgit.kv.memory import Memory


def open_repo(store) -> Repo:
    """A raw-bytes repository over ``store`` with no content cache, so
    every record a search needs is read from the store."""
    return Repo(store, codec="bytes", cache_bytes=0)


def handle(store, branch: str = "main"):
    """A worktree on ``branch`` from its own repository handle."""
    return open_repo(store).worktree(branch, create=True)


def put(wt, updates: dict[str, bytes]) -> None:
    """Stage ``updates`` on ``wt``, then commit them."""
    wt.update(updates)
    wt.commit()


def load_parents(store, commit: str) -> tuple[str, ...]:
    return open_repo(store).get_commit(commit).parents


def merge_base(store, a: str, b: str) -> str | None:
    return open_repo(store).merge_base(a, b)


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


def walk_merge_base(store, a, b):
    """The lowest common ancestor from both whole histories: of the
    commits both reach, the ones no other such commit descends from,
    the smallest hash breaking a tie."""

    def ancestors(start):
        seen, stack = set(), [start]
        while stack:
            c = stack.pop()
            if c not in seen:
                seen.add(c)
                stack.extend(load_parents(store, c))
        return seen

    common = ancestors(a) & ancestors(b)
    below = set()
    for c in common:
        below |= ancestors(c) - {c}
    best = common - below
    return min(best) if best else None


def random_history(seed, steps=120, branch_count=4):
    """A DAG from random commits and merges across a few branches,
    criss-crosses included. Every commit writes its own key, so no merge
    ever conflicts."""
    rng = random.Random(seed)
    store = Memory()
    main = handle(store)
    put(main, {"seed": b"0"})
    handles = [main] + [fork(main, f"b{i}") for i in range(1, branch_count)]
    counter = itertools.count()
    for _ in range(steps):
        h = rng.choice(handles)
        if rng.random() < 0.3:
            other = rng.choice(handles)
            if other is not h:
                h.merge(commit=other.head, fast_forward=rng.random() < 0.5)
                continue
        n = next(counter)
        put(h, {f"k{n}": str(n).encode()})
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


class TestSearch:
    @pytest.mark.parametrize("seed", range(8))
    def test_agrees_with_the_full_walk_on_random_histories(self, seed):
        store = random_history(seed)
        every = commits(store)
        rng = random.Random(seed)
        pairs = [tuple(rng.sample(every, 2)) for _ in range(150)]
        for a, b in pairs:
            assert merge_base(store, a, b) == walk_merge_base(store, a, b)

    def test_criss_cross_ties_go_to_the_smallest_hash(self):
        store = Memory()
        main = handle(store)
        put(main, {"base": b"0"})
        dev = fork(main, "dev")
        put(main, {"m": b"1"})
        put(dev, {"d": b"1"})
        m1, d1 = main.head, dev.head
        main.merge(commit=d1, fast_forward=False)
        dev.merge(commit=m1, fast_forward=False)
        a, b = main.head, dev.head
        assert merge_base(store, a, b) == min(m1, d1)
        assert walk_merge_base(store, a, b) == min(m1, d1)

    def test_unrelated_histories_have_no_common_ancestor(self):
        store = Memory()
        one = handle(store)
        put(one, {"a": b"1"})
        # A root commit outside every branch's history: no parents, and a
        # keyset root so the store holds it as a commit.
        store.set_many(
            **{
                PARENT_COMMIT % "island": dumps([]),
                COMMIT_ROOT % "island": dumps(EMPTY_HASH),
            }
        )
        assert merge_base(store, one.head, "island") is None

    def test_a_lost_race_reads_the_same_whatever_the_history(self):
        def reads_for(history):
            store = CountingMemory()
            v = handle(store)
            for i in range(history):
                put(v, {f"k{i % 5}": str(i).encode()})
            a = handle(store)
            b = handle(store)
            put(a, {"x": b"1"})
            store.record_reads = 0
            put(b, {"y": b"2"})
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
            assert merge_base(store, a, b) == walk_merge_base(store, a, b)

    def test_a_commit_over_old_history_stores_true_generations(self):
        store = Memory()
        v = handle(store)
        for i in range(6):
            put(v, {f"k{i}": b"1"})
        dev = fork(v, "dev")
        put(dev, {"d": b"1"})
        v.merge(commit=dev.head, fast_forward=False)
        self._strip(store, keep=set())

        fresh = handle(store)  # knows nothing about generations
        put(fresh, {"new": b"1"})
        truth = longest_paths(store)
        new = fresh.head
        parents = load_parents(store, new)
        assert loads(store.get(PARENT_GENS % new)) == [truth[p] for p in parents]

    def test_mixed_history_is_searched_correctly(self):
        store = random_history(5, steps=60)
        old = set(commits(store))
        self._strip(store, keep=set())
        # New history on top, with generations.
        main = handle(store)
        dev = handle(store, "b1")
        for i in range(10):
            put(main, {f"new-m{i}": b"1"})
            put(dev, {f"new-d{i}": b"1"})
        new = [c for c in commits(store) if c not in old]
        for a, b in itertools.combinations(new, 2):
            assert merge_base(store, a, b) == walk_merge_base(store, a, b)


class TestCollection:
    def test_gc_removes_an_orphans_generations(self):
        store = Memory()
        main = handle(store)
        dev = fork(main, "dev")
        put(dev, {"k": b"v"})
        orphan = dev.head
        assert store.get(PARENT_GENS % orphan) is not None
        main.repo.branches.delete("dev")
        clean_orphans(store, min_age=0)
        assert store.get(PARENT_GENS % orphan) is None
