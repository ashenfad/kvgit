"""Tests for the Namespaced wrapper."""

import pytest

from vkv import MergeResult, Namespaced, Versioned, counter
from vkv.kv.memory import Memory


class TestNamespacedBasic:
    def test_get_set(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        v.snapshot({"app/greeting": b"hello"})
        assert ns.get("greeting") == b"hello"

    def test_get_missing(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        assert ns.get("nope") is None

    def test_contains(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        v.snapshot({"app/k": b"v"})
        assert "k" in ns
        assert "nope" not in ns

    def test_get_many(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        v.snapshot({"app/a": b"1", "app/b": b"2", "other/c": b"3"})
        result = ns.get_many("a", "b", "c")
        assert result == {"a": b"1", "b": b"2"}


class TestNamespacedIsolation:
    def test_two_namespaces_isolated(self):
        v = Versioned()
        ns1 = Namespaced(v, "one")
        ns2 = Namespaced(v, "two")
        v.snapshot({"one/k": b"from-one", "two/k": b"from-two"})
        assert ns1.get("k") == b"from-one"
        assert ns2.get("k") == b"from-two"

    def test_keys_only_direct_children(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        v.snapshot({
            "app/a": b"1",
            "app/b": b"2",
            "app/sub/c": b"3",  # nested — not a direct child
            "other/d": b"4",  # different namespace
        })
        keys = set(ns.keys())
        assert keys == {"a", "b"}

    def test_descendant_keys(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        v.snapshot({
            "app/a": b"1",
            "app/sub/b": b"2",
            "app/sub/deep/c": b"3",
            "other/d": b"4",
        })
        descendants = set(ns.descendant_keys())
        assert descendants == {"a", "sub/b", "sub/deep/c"}


class TestNamespacedNested:
    def test_nested_namespace(self):
        v = Versioned()
        ns1 = Namespaced(v, "agent")
        ns2 = Namespaced(ns1, "worker")
        assert ns2.namespace == "agent/worker"

        v.snapshot({"agent/worker/task": b"data"})
        assert ns2.get("task") == b"data"

    def test_deeply_nested(self):
        v = Versioned()
        ns = Namespaced(Namespaced(Namespaced(v, "a"), "b"), "c")
        assert ns.namespace == "a/b/c"

        v.snapshot({"a/b/c/key": b"deep"})
        assert ns.get("key") == b"deep"

    def test_base_store(self):
        v = Versioned()
        ns1 = Namespaced(v, "level1")
        ns2 = Namespaced(ns1, "level2")
        assert ns2.base_store is v
        assert ns1.base_store is v


class TestNamespacedValidation:
    def test_slash_in_namespace_rejected(self):
        v = Versioned()
        with pytest.raises(ValueError, match="cannot contain '/'"):
            Namespaced(v, "bad/name")

    def test_invalid_store_type_rejected(self):
        with pytest.raises(TypeError, match="not dict"):
            Namespaced({}, "ns")  # type: ignore


class TestNamespacedWrite:
    def test_snapshot_auto_prefixes(self):
        v = Versioned()
        ns = Namespaced(v, "myns")
        ns.snapshot({"k": b"v"})
        assert v.get("myns/k") == b"v"

    def test_snapshot_removals_prefixed(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        ns.snapshot({"x": b"1", "y": b"2"})
        ns.snapshot(removals={"x"})
        assert ns.get("x") is None
        assert ns.get("y") == b"2"

    def test_snapshot_with_info(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        ns.snapshot({"k": b"v"}, info={"author": "test"})
        assert v.commit_info() == {"author": "test"}

    def test_merge_delegates(self):
        store = Memory()
        v1 = Versioned(store)
        ns1 = Namespaced(v1, "app")
        ns1.snapshot({"k": b"1"})
        ns1.merge()

        v2 = Versioned(store)
        ns2 = Namespaced(v2, "app")
        ns2.snapshot({"k": b"2"})

        result = ns2.merge()
        assert isinstance(result, MergeResult)
        assert result.merged

    def test_merge_fns_auto_prefixed(self):
        """merge_fns keys are auto-prefixed with namespace."""
        store = Memory()
        v1 = Versioned(store)
        ns1 = Namespaced(v1, "ns")
        ns1.snapshot({"k": b"base"})
        ns1.merge()

        v2 = Versioned(store)
        ns2 = Namespaced(v2, "ns")

        ns1.snapshot({"k": b"v1_update"})
        ns1.merge()
        ns2.snapshot({"k": b"v2_update"})

        # Merge fn concatenates both values
        concat = lambda old, ours, theirs: ours + b"+" + theirs
        result = ns2.merge(merge_fns={"k": concat})
        assert result.merged
        # ours=ns2's value, theirs=HEAD (v1's value)
        assert ns2.get("k") == b"v2_update+v1_update"

    def test_set_content_type_prefixed(self):
        """set_content_type registers with the prefixed key."""
        store = Memory()
        ct = counter()

        v1 = Versioned(store)
        ns1 = Namespaced(v1, "stats")
        ns1.snapshot({"hits": ct.encode(10)})
        ns1.merge()

        v2 = Versioned(store)
        ns2 = Namespaced(v2, "stats")
        ns2.set_content_type("hits", ct)

        ns1.snapshot({"hits": ct.encode(15)})
        ns1.merge()
        ns2.snapshot({"hits": ct.encode(20)})

        assert ns2.merge()
        assert ct.decode(ns2.get("hits")) == 25  # 15 + 20 - 10

    def test_two_namespaces_independent_writes(self):
        v = Versioned()
        ns1 = Namespaced(v, "one")
        ns2 = Namespaced(v, "two")
        ns1.snapshot({"k": b"from-one"})
        ns2.snapshot({"k": b"from-two"})
        assert ns1.get("k") == b"from-one"
        assert ns2.get("k") == b"from-two"


class TestNamespacedProperties:
    def test_current_commit_delegates(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        assert ns.current_commit == v.current_commit
        ns.snapshot({"k": b"v"})
        assert ns.current_commit == v.current_commit

    def test_base_commit_delegates(self):
        v = Versioned()
        ns = Namespaced(v, "app")
        assert ns.base_commit == v.base_commit

    def test_last_merge_result_delegates(self):
        store = Memory()
        v = Versioned(store)
        ns = Namespaced(v, "app")
        ns.snapshot({"k": b"v"})
        ns.merge()
        assert ns.last_merge_result is v.last_merge_result
        assert ns.last_merge_result.merged
