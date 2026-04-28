"""Tests for the content-addressable HAMT."""

import hashlib
import random

import pytest

from kvgit.hamt import EMPTY_HASH, Hamt, HamtDiff
from kvgit.kv.memory import Memory


# ---- helpers ----


def _store():
    return Memory()


def _all_node_keys(store: Memory, prefix: str = "hamt:") -> set[str]:
    return {k for k in store.keys() if isinstance(k, str) and k.startswith(prefix)}


def _flush_and_count(h: Hamt) -> int:
    """Flush a Hamt and return the number of HAMT nodes in the store."""
    h.flush()
    return len(_all_node_keys(h.store, h.prefix))


class _CountingMemory(Memory):
    """Memory store that counts get / get_many calls.

    Used by tests that verify batching behavior — e.g.,
    ``materialize()`` should issue a small number of ``get_many``
    calls and zero per-key ``get`` calls.
    """

    def __init__(self) -> None:
        super().__init__()
        self.get_calls = 0
        self.get_many_calls = 0

    def get(self, key):
        self.get_calls += 1
        return super().get(key)

    def get_many(self, *args):
        self.get_many_calls += 1
        return super().get_many(*args)

    def reset_counts(self) -> None:
        self.get_calls = 0
        self.get_many_calls = 0


# ---- empty ----


def test_empty_hamt_get_returns_none():
    h = Hamt(_store())
    assert h.get("anything") is None
    assert "x" not in h
    assert list(h.items()) == []
    assert list(h.keys()) == []
    assert len(h) == 0
    assert h.root == EMPTY_HASH


def test_empty_hamt_does_not_write_to_store():
    store = _store()
    h = Hamt(store)
    h.flush()
    assert _all_node_keys(store) == set()


def test_empty_hash_is_deterministic():
    # Two independent stores should produce the same EMPTY_HASH constant
    h1 = Hamt(_store())
    h2 = Hamt(_store())
    assert h1.root == h2.root == EMPTY_HASH


# ---- single insert ----


def test_single_insert_get():
    h = Hamt(_store()).persist({"a": b"1"})
    assert h.get("a") == b"1"
    assert "a" in h
    assert h.get("nope") is None


def test_single_insert_persists_to_store():
    store = _store()
    Hamt(store).persist({"a": b"1"})
    # One leaf node should be in the store
    assert len(_all_node_keys(store)) == 1


def test_insert_then_read_through_pending_before_flush():
    store = _store()
    h0 = Hamt(store)
    new_h, pending = h0.updated({"a": b"1"})

    # Reads on the new Hamt see the new value before any flush
    assert new_h.get("a") == b"1"
    # The store itself doesn't have the data yet
    assert len(_all_node_keys(store)) == 0
    # Pending has exactly one node
    assert len(pending) == 1

    # After flushing, the store has the data
    new_h.flush()
    assert len(_all_node_keys(store)) == 1


# ---- multiple inserts: bucket fill ----


def test_inserts_within_bucket_max_stay_in_one_leaf():
    store = _store()
    h = Hamt(store, bucket_max=8).persist({f"k{i}": f"v{i}".encode() for i in range(8)})
    assert len(_all_node_keys(store)) == 1
    for i in range(8):
        assert h.get(f"k{i}") == f"v{i}".encode()


def test_overflow_triggers_split():
    store = _store()
    items = {f"k{i}": f"v{i}".encode() for i in range(20)}
    h = Hamt(store, bucket_max=4).persist(items)

    # All keys must still be retrievable
    for k, v in items.items():
        assert h.get(k) == v

    # The store must contain more than one node now (a split happened)
    assert len(_all_node_keys(store)) > 1


# ---- canonical form: same content -> same root hash ----


def test_same_content_different_insertion_order_same_hash():
    items = {f"k{i}": f"v{i}".encode() for i in range(50)}

    h1 = Hamt(_store(), bucket_max=4).persist(items)

    shuffled = list(items.items())
    random.Random(42).shuffle(shuffled)
    h2 = Hamt(_store(), bucket_max=4)
    for k, v in shuffled:
        h2 = h2.persist({k: v})

    assert h1.root == h2.root


def test_same_content_after_inserts_and_deletes_same_hash():
    """Insert + delete patterns must produce the canonical form."""
    keep = {f"k{i}": f"v{i}".encode() for i in range(20)}
    extra = {f"e{i}": f"x{i}".encode() for i in range(10)}

    # Path A: insert only the keepers
    h_only = Hamt(_store(), bucket_max=3).persist(keep)

    # Path B: insert keepers + extras, then delete the extras
    h_mixed = Hamt(_store(), bucket_max=3).persist({**keep, **extra})
    h_mixed = h_mixed.persist(removals=list(extra.keys()))

    assert h_only.root == h_mixed.root


def test_collapse_after_delete_returns_to_empty():
    items = {f"k{i}": f"v{i}".encode() for i in range(15)}
    h = Hamt(_store(), bucket_max=2).persist(items)
    assert h.root != EMPTY_HASH

    h = h.persist(removals=list(items.keys()))
    assert h.root == EMPTY_HASH
    assert len(h) == 0


def test_canonical_form_under_random_operations():
    """Random insert/delete sequences should converge on the same hash
    when applied to two independent paths reaching the same logical state."""
    rng = random.Random(7)
    keys = [f"key-{i}" for i in range(40)]

    # Pick a target subset
    target = {k: rng.randbytes(16) for k in rng.sample(keys, 25)}

    # Path A: build target directly
    h_a = Hamt(_store(), bucket_max=3).persist(target)

    # Path B: insert ALL keys with random values, then delete the
    # non-target ones, then overwrite the target ones with target values
    all_initial = {k: rng.randbytes(16) for k in keys}
    h_b = Hamt(_store(), bucket_max=3).persist(all_initial)
    h_b = h_b.persist(removals=[k for k in keys if k not in target])
    h_b = h_b.persist(target)  # overwrite to match

    assert h_a.root == h_b.root


# ---- update existing key ----


def test_update_existing_key():
    h = Hamt(_store()).persist({"a": b"1"})
    h2 = h.persist({"a": b"2"})
    assert h2.get("a") == b"2"
    assert h.get("a") == b"1"  # original is unchanged


def test_setting_same_value_is_noop():
    h = Hamt(_store()).persist({"a": b"1"})
    new_h, pending = h.updated({"a": b"1"})
    assert new_h.root == h.root
    assert pending == {}


# ---- remove ----


def test_remove_key():
    h = Hamt(_store()).persist({"a": b"1", "b": b"2"})
    h2 = h.persist(removals=["a"])
    assert h2.get("a") is None
    assert h2.get("b") == b"2"
    # Original is unchanged
    assert h.get("a") == b"1"


def test_remove_missing_key_is_noop():
    h = Hamt(_store()).persist({"a": b"1"})
    new_h, pending = h.updated(removals=["nope"])
    assert new_h.root == h.root
    assert pending == {}


def test_remove_all_keys_returns_empty():
    items = {f"k{i}": f"v{i}".encode() for i in range(10)}
    h = Hamt(_store(), bucket_max=3).persist(items)
    h = h.persist(removals=list(items.keys()))
    assert h.root == EMPTY_HASH


# ---- forced deep collisions (force splits all the way down) ----


def test_deep_collisions_via_tiny_bucket():
    """With bucket_max=1, every collision forces a split. Use many keys
    so we exercise the recursive split path."""
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(100)}
    h = Hamt(_store(), bucket_max=1).persist(items)
    for k, v in items.items():
        assert h.get(k) == v
    assert len(list(h.items())) == 100


def test_split_recursion_when_all_share_next_nibble():
    """Construct a case where forcing a split needs to recurse because
    all entries share the next nibble too."""
    # Find two distinct keys whose SHA-256 hashes share their first
    # several nibbles. We just brute-force search.
    target_prefix_len = 3  # 12 bits of shared prefix
    found = []
    i = 0
    seen_prefixes: dict[str, str] = {}
    while len(found) < 2:
        k = f"x{i}"
        h = hashlib.sha256(k.encode()).hexdigest()[:target_prefix_len]
        if h in seen_prefixes:
            found = [seen_prefixes[h], k]
            break
        seen_prefixes[h] = k
        i += 1

    h = Hamt(_store(), bucket_max=1).persist({found[0]: b"a", found[1]: b"b"})
    assert h.get(found[0]) == b"a"
    assert h.get(found[1]) == b"b"


# ---- structural sharing ----


def test_structural_sharing_one_changed_key():
    """Modifying one key should write only O(log N) new nodes, not the
    whole tree. Verify by counting nodes added to the store."""
    store = _store()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h = Hamt(store, bucket_max=4).persist(items)
    nodes_before = len(_all_node_keys(store))

    h2 = h.persist({"k0050": b"changed"})
    nodes_after = len(_all_node_keys(store))
    new_nodes = nodes_after - nodes_before

    # Should be a small number of new nodes (the path from root to leaf),
    # not anywhere near the total tree size.
    assert 0 < new_nodes < 10, (
        f"expected ~few new nodes, got {new_nodes} (total tree {nodes_before})"
    )

    # Both versions still readable
    assert h.get("k0050") == b"v50"
    assert h2.get("k0050") == b"changed"
    assert h.get("k0001") == h2.get("k0001") == b"v1"


# ---- batch updated() filters intermediate orphans ----


def test_batch_update_filters_orphan_intermediates():
    """A batched update of N keys to the same leaf should not write
    superseded intermediate leaves to pending."""
    store = _store()
    h = Hamt(store, bucket_max=8)
    new_h, pending = h.updated({f"k{i}": b"v" for i in range(5)})

    # Only the final leaf should be in pending — no orphans from intermediate steps
    assert len(pending) == 1


# ---- iteration ----


def test_iteration_yields_all_entries():
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(100)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    yielded = dict(h.items())
    assert yielded == items


def test_keys_values_iteration():
    items = {f"k{i}": f"v{i}".encode() for i in range(20)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    assert set(h.keys()) == set(items.keys())
    assert set(h.values()) == set(items.values())
    assert set(iter(h)) == set(items.keys())


def test_len():
    items = {f"k{i}": b"x" for i in range(50)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    assert len(h) == 50


# ---- value encoding ----


def test_value_with_arbitrary_bytes():
    """Values containing nulls, high bytes, etc. must round-trip."""
    nasty = bytes(range(256))
    h = Hamt(_store()).persist({"k": nasty})
    assert h.get("k") == nasty


def test_value_empty_bytes():
    h = Hamt(_store()).persist({"k": b""})
    assert h.get("k") == b""
    assert "k" in h


def test_key_with_special_chars():
    keys = ["with space", 'with "quote"', "with\nnewline", "unicode-café-🦀"]
    h = Hamt(_store()).persist({k: k.encode() for k in keys})
    for k in keys:
        assert h.get(k) == k.encode()


# ---- prefix isolation ----


def test_two_hamts_in_same_store_with_different_prefixes():
    store = _store()
    h_a = Hamt(store, prefix="a:").persist({"k": b"a-val"})
    h_b = Hamt(store, prefix="b:").persist({"k": b"b-val"})

    # Both readable, no interference
    assert h_a.get("k") == b"a-val"
    assert h_b.get("k") == b"b-val"

    # Storage is partitioned
    a_keys = {k for k in store.keys() if k.startswith("a:")}
    b_keys = {k for k in store.keys() if k.startswith("b:")}
    assert a_keys and b_keys
    assert not (a_keys & b_keys)


# ---- reachable_nodes ----


def test_reachable_nodes_empty():
    h = Hamt(_store())
    assert list(h.reachable_nodes()) == []


def test_reachable_nodes_single_leaf():
    h = Hamt(_store()).persist({"a": b"1"})
    nodes = list(h.reachable_nodes())
    assert len(nodes) == 1
    assert nodes[0] == h.root


def test_reachable_nodes_covers_full_tree():
    store = _store()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(100)}
    h = Hamt(store, bucket_max=4).persist(items)

    reachable = set(h.reachable_nodes())
    all_nodes = {k[len(h.prefix) :] for k in _all_node_keys(store, h.prefix)}
    assert reachable == all_nodes


def test_reachable_nodes_works_with_pending():
    store = _store()
    h = Hamt(store, bucket_max=2)
    items = {f"k{i}": b"v" for i in range(10)}
    new_h, pending = h.updated(items)

    # All reachable nodes are in pending (nothing flushed yet)
    reachable = set(new_h.reachable_nodes())
    pending_hashes = {k[len(new_h.prefix) :] for k in pending}
    assert reachable == pending_hashes


# ---- diff ----


def test_diff_empty_vs_empty():
    h1 = Hamt(_store())
    h2 = Hamt(_store())
    d = h1.diff(h2)
    assert d == HamtDiff(added={}, removed={}, modified={})


def test_diff_empty_vs_populated():
    h1 = Hamt(_store())
    h2 = Hamt(_store()).persist({"a": b"1", "b": b"2"})
    d = h1.diff(h2)
    assert d.added == {"a": b"1", "b": b"2"}
    assert d.removed == {}
    assert d.modified == {}


def test_diff_populated_vs_empty():
    h1 = Hamt(_store()).persist({"a": b"1", "b": b"2"})
    h2 = Hamt(_store())
    d = h1.diff(h2)
    assert d.added == {}
    assert d.removed == {"a": b"1", "b": b"2"}
    assert d.modified == {}


def test_diff_added_removed_modified():
    h1 = Hamt(_store(), bucket_max=4).persist({"a": b"1", "b": b"2", "c": b"3"})
    h2 = h1.persist({"b": b"22", "d": b"4"})  # modify b, add d
    h3 = h2.persist(removals=["a"])  # remove a

    d = h1.diff(h3)
    assert d.added == {"d": b"4"}
    assert d.removed == {"a": b"1"}
    assert d.modified == {"b": (b"2", b"22")}


def test_diff_skips_identical_subtrees():
    """Diff should be cheap when most of the tree is shared.
    We verify this by ensuring it returns the right thing for a tree with
    many shared subtrees and one changed key."""
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h1 = Hamt(_store(), bucket_max=4).persist(items)
    h2 = h1.persist({"k0050": b"changed"})

    d = h1.diff(h2)
    assert d.added == {}
    assert d.removed == {}
    assert d.modified == {"k0050": (b"v50", b"changed")}


# ---- bucket_max parameter ----


def test_bucket_max_affects_shape_but_not_contents():
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(50)}
    h_small = Hamt(_store(), bucket_max=2).persist(items)
    h_large = Hamt(_store(), bucket_max=16).persist(items)

    # Both contain the same logical data
    assert dict(h_small.items()) == dict(h_large.items()) == items
    # But different shapes
    assert h_small.root != h_large.root


def test_bucket_max_one_is_valid():
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(20)}
    h = Hamt(_store(), bucket_max=1).persist(items)
    for k, v in items.items():
        assert h.get(k) == v


def test_bucket_max_zero_rejected():
    with pytest.raises(ValueError, match="bucket_max"):
        Hamt(_store(), bucket_max=0)


# ---- large random workload ----


def test_large_random_workload():
    rng = random.Random(123)
    n = 1000
    items = {f"k-{rng.randint(0, 10**9)}": rng.randbytes(32) for _ in range(n)}

    h = Hamt(_store(), bucket_max=4).persist(items)

    # Every key readable
    for k, v in items.items():
        assert h.get(k) == v

    # Length matches
    assert len(h) == len(items)

    # Iteration yields everything exactly once
    yielded = dict(h.items())
    assert yielded == items


def test_large_random_with_deletes():
    rng = random.Random(456)
    items = {f"k-{i}": rng.randbytes(16) for i in range(500)}
    to_delete = set(rng.sample(list(items.keys()), 200))

    h = Hamt(_store(), bucket_max=4).persist(items)
    h = h.persist(removals=list(to_delete))

    survivors = {k: v for k, v in items.items() if k not in to_delete}
    assert dict(h.items()) == survivors
    for k in to_delete:
        assert h.get(k) is None


# ---- updated()/commit() interaction with existing pending ----


def test_chained_updated_calls_accumulate_pending():
    store = _store()
    h0 = Hamt(store, bucket_max=4)
    h1, pending1 = h0.updated({f"k{i}": b"v" for i in range(3)})
    h2, pending2 = h1.updated({f"k{i}": b"w" for i in range(3, 6)})

    # Nothing in store yet
    assert len(_all_node_keys(store)) == 0

    # h2 sees both batches of inserts
    assert h2.get("k0") == b"v"
    assert h2.get("k4") == b"w"

    # Final pending contains the nodes reachable from h2's root
    assert len(pending2) > 0
    assert len(pending2) == len(set(h2.reachable_nodes()))

    # Flushing h2 writes all pending nodes to the store
    h2.flush()
    h2_clean = Hamt(store, h2.root, bucket_max=4)
    assert h2_clean.get("k0") == b"v"
    assert h2_clean.get("k5") == b"w"


def test_persist_clears_pending():
    h0 = Hamt(_store(), bucket_max=4)
    h1 = h0.persist({"a": b"1"})
    assert h1.pending == {}


def test_flush_clears_pending():
    h0 = Hamt(_store(), bucket_max=4)
    h1, _ = h0.updated({"a": b"1"})
    assert h1.pending != {}
    h2 = h1.flush()
    assert h2.pending == {}
    assert h2.get("a") == b"1"


# ---- pending isolation between Hamt instances ----


def test_pending_not_visible_to_separate_hamt_instance():
    store = _store()
    h0 = Hamt(store, bucket_max=4)
    new_h, pending = h0.updated({"a": b"1"})

    # A fresh Hamt opened on the same store at the new root cannot see
    # the value, because the underlying nodes haven't been flushed.
    fresh = Hamt(store, new_h.root, bucket_max=4)
    assert fresh.get("a") is None

    # After flushing the original, the fresh view sees it.
    new_h.flush()
    assert fresh.get("a") == b"1"


# ---- materialize ----


def test_materialize_empty():
    h = Hamt(_store())
    assert h.materialize() == {}


def test_materialize_single():
    h = Hamt(_store()).persist({"a": b"1"})
    assert h.materialize() == {"a": b"1"}


def test_materialize_matches_items():
    rng = random.Random(99)
    items = {f"k{i}": rng.randbytes(16) for i in range(150)}
    h = Hamt(_store(), bucket_max=4).persist(items)

    via_items = dict(h.items())
    via_materialize = h.materialize()
    assert via_items == via_materialize == items


def test_materialize_includes_pending_writes():
    """A non-flushed Hamt's materialize() must see its pending nodes."""
    store = _store()
    h0 = Hamt(store, bucket_max=4)
    new_h, _ = h0.updated({f"k{i}": f"v{i}".encode() for i in range(20)})

    # Nothing flushed yet
    assert len(_all_node_keys(store)) == 0
    # But materialize through the pending Hamt sees everything
    materialized = new_h.materialize()
    assert len(materialized) == 20
    assert materialized["k5"] == b"v5"


def test_materialize_uses_batched_reads():
    """materialize() should issue O(depth) get_many calls and zero per-key gets."""
    store = _CountingMemory()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h = Hamt(store, bucket_max=4).persist(items)

    store.reset_counts()
    result = h.materialize()
    assert result == items

    # No per-key reads at all
    assert store.get_calls == 0, (
        f"materialize used {store.get_calls} per-key get() calls; expected 0"
    )
    # Bounded number of batched reads (one per tree level).
    # 200 keys with bucket_max=4 fits in ~4 levels at branching factor 16,
    # so we expect at most that many round-trips.
    assert 0 < store.get_many_calls <= 6, (
        f"unexpected get_many call count: {store.get_many_calls}"
    )


def test_items_uses_per_node_reads():
    """Sanity check the contrast: items() does one get per visited node."""
    store = _CountingMemory()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h = Hamt(store, bucket_max=4).persist(items)

    store.reset_counts()
    list(h.items())

    # items() doesn't batch — it's many per-key gets, no get_many calls.
    assert store.get_calls > 50, (
        f"items() should issue many per-node get calls, got {store.get_calls}"
    )
    assert store.get_many_calls == 0


def test_materialize_round_trip_through_store():
    """A Hamt rebuilt from a fresh view should materialize to the same dict."""
    store = _store()
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(50)}
    h = Hamt(store, bucket_max=4).persist(items)
    root = h.root

    # Open a fresh Hamt at the same root and materialize.
    fresh = Hamt(store, root, bucket_max=4)
    assert fresh.materialize() == items


# ---- walk (combined items + node hashes) ----


def test_walk_empty():
    h = Hamt(_store())
    items, nodes = h.walk()
    assert items == {}
    assert nodes == set()


def test_walk_returns_both_items_and_nodes():
    items = {f"k{i}": f"v{i}".encode() for i in range(20)}
    h = Hamt(_store(), bucket_max=4).persist(items)

    walked_items, walked_nodes = h.walk()
    assert walked_items == items
    assert len(walked_nodes) > 0
    # Root must be in the node set
    assert h.root in walked_nodes


def test_walk_node_set_matches_reachable_nodes():
    """walk() should return the same node set as reachable_nodes()."""
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(150)}
    h = Hamt(_store(), bucket_max=4).persist(items)

    _, walked_nodes = h.walk()
    via_reachable = set(h.reachable_nodes())
    assert walked_nodes == via_reachable


def test_walk_uses_batched_reads():
    """walk() should issue O(depth) get_many calls and zero per-key gets."""
    store = _CountingMemory()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h = Hamt(store, bucket_max=4).persist(items)

    store.reset_counts()
    walked_items, walked_nodes = h.walk()
    assert walked_items == items
    assert len(walked_nodes) > 0

    assert store.get_calls == 0, (
        f"walk used {store.get_calls} per-key get() calls; expected 0"
    )
    assert 0 < store.get_many_calls <= 6, (
        f"unexpected get_many call count: {store.get_many_calls}"
    )


def test_walk_is_one_pass_compared_to_separate_walks():
    """walk() should make ~half the round-trips of items() + reachable_nodes()."""
    store = _CountingMemory()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h = Hamt(store, bucket_max=4).persist(items)

    # walk() — single batched BFS
    store.reset_counts()
    h.walk()
    walk_calls = store.get_many_calls + store.get_calls

    # items() + reachable_nodes() — two separate per-node walks
    store.reset_counts()
    list(h.items())
    list(h.reachable_nodes())
    separate_calls = store.get_many_calls + store.get_calls

    # walk() should be drastically cheaper
    assert walk_calls * 10 < separate_calls, (
        f"walk={walk_calls}, separate={separate_calls} — "
        f"expected walk to be at least 10x cheaper"
    )


def test_walk_includes_pending_writes():
    store = _store()
    h0 = Hamt(store, bucket_max=4)
    new_h, _ = h0.updated({f"k{i}": f"v{i}".encode() for i in range(15)})

    # Nothing flushed yet
    assert len(_all_node_keys(store)) == 0
    # walk() through pending sees everything
    walked_items, walked_nodes = new_h.walk()
    assert len(walked_items) == 15
    assert len(walked_nodes) > 0
    assert new_h.root in walked_nodes


def test_materialize_is_walk_dot_zero():
    """materialize() should return exactly walk()[0]."""
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(50)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    assert h.materialize() == h.walk()[0]


# ---- walk(skip_nodes=...) cumulative seen-set ----


def test_walk_skip_nodes_root_short_circuits():
    """Skipping the root yields nothing — entire tree pruned."""
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(20)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    walked_items, walked_nodes = h.walk(skip_nodes={h.root})
    assert walked_items == {}
    assert walked_nodes == set()


def test_walk_skip_nodes_excludes_skipped_from_returned_set():
    """Returned ``nodes`` excludes anything in ``skip_nodes``."""
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(60)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    full_nodes = h.walk()[1]
    # Pick an arbitrary non-root node to skip.
    interior = next(iter(full_nodes - {h.root}))
    _, walked_nodes = h.walk(skip_nodes={interior})
    assert interior not in walked_nodes
    # Other nodes are still visited (root at minimum).
    assert h.root in walked_nodes


def test_walk_skip_nodes_skips_subtree_items():
    """Items beneath a skipped subtree are not returned."""
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(80)}
    h = Hamt(_store(), bucket_max=4).persist(items)

    # Find a non-root branch node and the items reachable beneath it,
    # then verify those items are absent when we skip that node.
    full_items, full_nodes = h.walk()
    interior_candidates = full_nodes - {h.root}
    # Walk only the subtree rooted at the candidate to see what's under it.
    for candidate in interior_candidates:
        sub_items = Hamt(h.store, root=candidate, bucket_max=4).walk()[0]
        if sub_items and len(sub_items) < len(full_items):
            walked_items, _ = h.walk(skip_nodes={candidate})
            for k in sub_items:
                assert k not in walked_items
            # And the items not in that subtree are still present.
            for k in full_items:
                if k not in sub_items:
                    assert walked_items[k] == full_items[k]
            return
    pytest.fail("expected to find at least one non-root subtree to skip")


def test_walk_skip_nodes_does_not_fetch_skipped():
    """Skipped nodes should not be fetched from the store at all."""
    store = _CountingMemory()
    items = {f"k{i:04d}": f"v{i}".encode() for i in range(200)}
    h = Hamt(store, bucket_max=4).persist(items)
    full_nodes = h.walk()[1]

    # Skip every node — the only fetches should be ones forced by
    # the level-batched fetch *before* we filter (we filter first
    # in the implementation, so even those should be zero).
    store.reset_counts()
    walked_items, walked_nodes = h.walk(skip_nodes=full_nodes)
    assert walked_items == {}
    assert walked_nodes == set()
    assert store.get_calls == 0
    assert store.get_many_calls == 0, (
        f"expected zero fetches when skipping the whole tree, "
        f"got {store.get_many_calls}"
    )


def test_walk_skip_nodes_cumulative_across_shared_subtree():
    """Two HAMTs sharing structure: walking the second with the
    first's nodes as skip_nodes should fetch only what's new."""
    base_items = {f"k{i:04d}": f"v{i}".encode() for i in range(120)}
    store = _CountingMemory()
    h1 = Hamt(store, bucket_max=4).persist(base_items)

    # h2 shares the bulk of h1's structure — single-key delta.
    h2 = h1.persist({"new-key": b"new-value"})
    assert h2.root != h1.root

    # First walk seeds the seen-set.
    seen: set[str] = set()
    items1, nodes1 = h1.walk(skip_nodes=seen)
    seen |= nodes1

    # Second walk: only the path from h2.root down to the changed
    # leaf should be visited; the rest of the tree is in ``seen``.
    store.reset_counts()
    items2, nodes2 = h2.walk(skip_nodes=seen)
    # New items: just the added key (and only items along visited
    # paths — which by definition contain the new key).
    assert "new-key" in items2
    # Visited node count should be tiny relative to the full tree.
    assert len(nodes2) < len(nodes1) // 4, (
        f"expected shared-structure walk to revisit << {len(nodes1) // 4} "
        f"nodes; got {len(nodes2)}"
    )
    # And no overlap between the two walks' returned node sets.
    assert nodes1.isdisjoint(nodes2)


def test_walk_skip_nodes_none_matches_no_arg():
    """Passing skip_nodes=None is identical to omitting it."""
    items = {f"k{i:03d}": f"v{i}".encode() for i in range(40)}
    h = Hamt(_store(), bucket_max=4).persist(items)
    a_items, a_nodes = h.walk()
    b_items, b_nodes = h.walk(skip_nodes=None)
    c_items, c_nodes = h.walk(skip_nodes=set())
    assert a_items == b_items == c_items
    assert a_nodes == b_nodes == c_nodes
