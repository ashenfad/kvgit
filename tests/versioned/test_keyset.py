"""Tests for the Keyset wrapper around HAMT."""

import pytest

from kvgit.hamt import EMPTY_HASH
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import (
    Keyset,
    KeysetDiff,
    KeysetEntry,
    MetaEntry,
    decode_entry,
    encode_entry,
)


def _meta(size=10, created=1000.0) -> MetaEntry:
    return MetaEntry(size=size, created_at=created)


def _entry(blob="abc:k", size=10, created=1000.0) -> KeysetEntry:
    return KeysetEntry(blob=blob, meta=_meta(size, created))


# ---- entry encoding ----


def test_encode_decode_round_trip():
    e = _entry(blob="commit-hash:user-key", size=100, created=1234.5)
    raw = encode_entry(e)
    assert isinstance(raw, bytes)
    decoded = decode_entry(raw)
    assert decoded == e


def test_encode_is_deterministic():
    e = _entry()
    assert encode_entry(e) == encode_entry(e)


def test_encode_handles_none_size():
    e = KeysetEntry(blob="x", meta=MetaEntry(size=None, created_at=0.0))
    decoded = decode_entry(encode_entry(e))
    assert decoded == e
    assert decoded.meta.size is None


def test_keyset_entry_is_frozen():
    e = _entry()
    with pytest.raises((AttributeError, Exception)):
        e.blob = "different"  # type: ignore[misc]


# ---- empty keyset ----


def test_empty_keyset():
    ks = Keyset(Memory())
    assert ks.root == EMPTY_HASH
    assert ks.get("anything") is None
    assert "x" not in ks
    assert list(ks.items()) == []
    assert len(ks) == 0


def test_empty_keyset_does_not_write():
    store = Memory()
    Keyset(store).flush()
    assert list(store.keys()) == []


# ---- single entry ----


def test_set_and_get():
    e = _entry(blob="commit1:foo", size=100)
    ks = Keyset(Memory()).persist({"foo": e})
    assert ks.get("foo") == e
    assert "foo" in ks


def test_get_blob_shortcut():
    e = _entry(blob="commit1:foo")
    ks = Keyset(Memory()).persist({"foo": e})
    assert ks.get_blob("foo") == "commit1:foo"
    assert ks.get_blob("missing") is None


def test_get_missing_returns_none():
    ks = Keyset(Memory()).persist({"a": _entry()})
    assert ks.get("b") is None


# ---- multiple entries ----


def test_multiple_entries():
    entries = {
        f"key-{i}": _entry(blob=f"commit:key-{i}", size=i * 10) for i in range(20)
    }
    ks = Keyset(Memory(), bucket_max=4).persist(entries)
    for k, e in entries.items():
        assert ks.get(k) == e
    assert len(ks) == 20


def test_iteration_yields_decoded_entries():
    entries = {f"k{i}": _entry(blob=f"b{i}", size=i) for i in range(15)}
    ks = Keyset(Memory(), bucket_max=4).persist(entries)
    yielded = dict(ks.items())
    assert yielded == entries


def test_keys_iteration():
    entries = {f"k{i}": _entry() for i in range(10)}
    ks = Keyset(Memory(), bucket_max=4).persist(entries)
    assert set(ks.keys()) == set(entries.keys())
    assert set(iter(ks)) == set(entries.keys())


def test_values_iteration():
    entries = {f"k{i}": _entry(blob=f"b{i}") for i in range(10)}
    ks = Keyset(Memory(), bucket_max=4).persist(entries)
    blobs = {v.blob for v in ks.values()}
    assert blobs == {f"b{i}" for i in range(10)}


# ---- updates ----


def test_update_existing_entry():
    ks = Keyset(Memory()).persist({"a": _entry(blob="old", size=1)})
    ks2 = ks.persist({"a": _entry(blob="new", size=2)})
    assert ks2.get("a") == _entry(blob="new", size=2)
    # Original is unchanged
    assert ks.get("a") == _entry(blob="old", size=1)


def test_setting_same_entry_is_noop():
    e = _entry(blob="x", size=1)
    ks = Keyset(Memory()).persist({"a": e})
    new_ks, pending = ks.updated({"a": e})
    assert new_ks.root == ks.root
    assert pending == {}


# ---- removes ----


def test_remove_entry():
    ks = Keyset(Memory()).persist({"a": _entry(), "b": _entry()})
    ks2 = ks.persist(removals=["a"])
    assert ks2.get("a") is None
    assert ks2.get("b") is not None
    assert "a" not in ks2
    assert "b" in ks2


def test_remove_all_returns_to_empty():
    entries = {f"k{i}": _entry() for i in range(10)}
    ks = Keyset(Memory(), bucket_max=3).persist(entries)
    ks = ks.persist(removals=list(entries.keys()))
    assert ks.root == EMPTY_HASH
    assert len(ks) == 0


# ---- canonical form propagates from HAMT ----


def test_canonical_form_inherited_from_hamt():
    """Two Keysets with the same logical content must share the same root."""
    entries = {f"k{i}": _entry(blob=f"b{i}") for i in range(20)}

    ks_a = Keyset(Memory(), bucket_max=4).persist(entries)

    ks_b = Keyset(Memory(), bucket_max=4)
    for k, e in entries.items():
        ks_b = ks_b.persist({k: e})

    assert ks_a.root == ks_b.root


# ---- pending / flush / commit ----


def test_pending_writes_returned_for_batching():
    store = Memory()
    ks0 = Keyset(store)
    new_ks, pending = ks0.updated({"a": _entry()})
    # Nothing in store yet
    assert list(store.keys()) == []
    # New keyset reads through pending
    assert new_ks.get("a") is not None
    # Pending has at least one entry, ready to merge
    assert len(pending) > 0


def test_flush_persists():
    store = Memory()
    ks0 = Keyset(store)
    new_ks, _ = ks0.updated({"a": _entry(blob="x")})
    new_ks.flush()
    # Now a fresh Keyset on the same root sees the data
    fresh = Keyset(store, new_ks.root)
    assert fresh.get_blob("a") == "x"


def test_persist_writes_immediately():
    store = Memory()
    ks = Keyset(store).persist({"a": _entry(blob="x")})
    # Can read from a fresh Keyset on the same store + root
    fresh = Keyset(store, ks.root)
    assert fresh.get_blob("a") == "x"


# ---- prefix isolation ----


def test_keyset_uses_distinct_prefix_from_default_hamt():
    """The Keyset's default prefix shouldn't collide with a generic
    Hamt sharing the same store."""
    from kvgit.hamt import Hamt

    store = Memory()
    ks = Keyset(store).persist({"a": _entry(blob="ksval")})
    h = Hamt(store).persist({"a": b"hamtval"})

    # Both have data, no interference
    assert ks.get_blob("a") == "ksval"
    assert h.get("a") == b"hamtval"

    # Storage is partitioned by prefix
    ks_keys = {k for k in store.keys() if k.startswith(ks.prefix)}
    h_keys = {k for k in store.keys() if k.startswith(h.prefix)}
    assert ks_keys
    assert h_keys
    assert not (ks_keys & h_keys)


# ---- structural sharing ----


def test_structural_sharing_via_keyset():
    """Modifying one entry should add only a handful of new HAMT nodes."""
    store = Memory()
    entries = {f"k{i:04d}": _entry(blob=f"b{i}", size=i) for i in range(200)}
    ks = Keyset(store, bucket_max=4).persist(entries)
    nodes_before = sum(1 for k in store.keys() if k.startswith(ks.prefix))

    ks2 = ks.persist({"k0050": _entry(blob="changed", size=999)})
    nodes_after = sum(1 for k in store.keys() if k.startswith(ks.prefix))
    new_nodes = nodes_after - nodes_before

    assert 0 < new_nodes < 10, f"too many new nodes: {new_nodes}"
    assert ks2.get_blob("k0050") == "changed"
    assert ks.get_blob("k0050") == "b50"


# ---- reachable_nodes ----


def test_reachable_nodes_empty():
    ks = Keyset(Memory())
    assert list(ks.reachable_nodes()) == []


def test_reachable_nodes_covers_full_keyset():
    store = Memory()
    entries = {f"k{i:04d}": _entry(blob=f"b{i}") for i in range(100)}
    ks = Keyset(store, bucket_max=4).persist(entries)

    reachable = set(ks.reachable_nodes())
    all_nodes = {k[len(ks.prefix) :] for k in store.keys() if k.startswith(ks.prefix)}
    assert reachable == all_nodes


# ---- diff ----


def test_diff_empty_vs_empty():
    a = Keyset(Memory())
    b = Keyset(Memory())
    d = a.diff(b)
    assert d == KeysetDiff(added={}, removed={}, modified={})


def test_diff_added_removed_modified():
    e1 = _entry(blob="b1", size=1)
    e2 = _entry(blob="b2", size=2)
    e2_new = _entry(blob="b2-new", size=22)
    e3 = _entry(blob="b3", size=3)

    a = Keyset(Memory(), bucket_max=4).persist({"k1": e1, "k2": e2})
    b = a.persist({"k2": e2_new, "k3": e3}).persist(removals=["k1"])

    d = a.diff(b)
    assert d.added == {"k3": e3}
    assert d.removed == {"k1": e1}
    assert d.modified == {"k2": (e2, e2_new)}


def test_diff_returns_decoded_entries():
    """Diff results should be KeysetEntry objects, not raw bytes."""
    a = Keyset(Memory()).persist({"k": _entry(blob="old")})
    b = a.persist({"k": _entry(blob="new")})
    d = a.diff(b)
    assert isinstance(d.modified["k"][0], KeysetEntry)
    assert isinstance(d.modified["k"][1], KeysetEntry)


# ---- bucket_max ----


def test_bucket_max_configurable():
    entries = {f"k{i:03d}": _entry() for i in range(30)}
    ks_small = Keyset(Memory(), bucket_max=2).persist(entries)
    ks_large = Keyset(Memory(), bucket_max=16).persist(entries)
    assert dict(ks_small.items()) == dict(ks_large.items()) == entries
    assert ks_small.root != ks_large.root


# ---- meta semantics survive round-trip ----


def test_meta_field_round_trip():
    """All MetaEntry fields must round-trip exactly through encode/decode/HAMT."""
    e = KeysetEntry(
        blob="commit-abc:my-key",
        meta=MetaEntry(size=98765, created_at=1234567890.5),
    )
    ks = Keyset(Memory()).persist({"k": e})
    got = ks.get("k")
    assert got == e
    assert got.meta.size == 98765
    assert got.meta.created_at == 1234567890.5
