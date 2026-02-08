"""Tests for the Namespaced wrapper."""

import pytest

from vkv import MergeResult, Namespaced, Staged, Versioned, counter
from vkv.kv.memory import Memory


def _staged(store=None, **kwargs):
    """Helper to create a Staged store."""
    return Staged(Versioned(store, **kwargs))


class TestNamespacedBasic:
    def test_get_set(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("greeting", b"hello")
        assert ns.get("greeting") == b"hello"

    def test_get_missing(self):
        s = _staged()
        ns = Namespaced(s, "app")
        assert ns.get("nope") is None

    def test_contains(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("k", b"v")
        assert "k" in ns
        assert "nope" not in ns

    def test_get_many(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("a", b"1")
        ns.set("b", b"2")
        s.set("other/c", b"3")
        result = ns.get_many("a", "b", "c")
        assert result == {"a": b"1", "b": b"2"}


class TestNamespacedIsolation:
    def test_two_namespaces_isolated(self):
        s = _staged()
        ns1 = Namespaced(s, "one")
        ns2 = Namespaced(s, "two")
        ns1.set("k", b"from-one")
        ns2.set("k", b"from-two")
        assert ns1.get("k") == b"from-one"
        assert ns2.get("k") == b"from-two"

    def test_keys_only_direct_children(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("a", b"1")
        ns.set("b", b"2")
        s.set("app/sub/c", b"3")  # nested — not a direct child
        s.set("other/d", b"4")  # different namespace
        keys = set(ns.keys())
        assert keys == {"a", "b"}

    def test_descendant_keys(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("a", b"1")
        s.set("app/sub/b", b"2")
        s.set("app/sub/deep/c", b"3")
        s.set("other/d", b"4")
        descendants = set(ns.descendant_keys())
        assert descendants == {"a", "sub/b", "sub/deep/c"}


class TestNamespacedNested:
    def test_nested_namespace(self):
        s = _staged()
        ns1 = Namespaced(s, "agent")
        ns2 = Namespaced(ns1, "worker")
        assert ns2.namespace == "agent/worker"

        ns2.set("task", b"data")
        assert ns2.get("task") == b"data"

    def test_deeply_nested(self):
        s = _staged()
        ns = Namespaced(Namespaced(Namespaced(s, "a"), "b"), "c")
        assert ns.namespace == "a/b/c"

        ns.set("key", b"deep")
        assert ns.get("key") == b"deep"


class TestNamespacedValidation:
    def test_slash_in_namespace_rejected(self):
        s = _staged()
        with pytest.raises(ValueError, match="cannot contain '/'"):
            Namespaced(s, "bad/name")

    def test_invalid_store_type_rejected(self):
        with pytest.raises(TypeError, match="not dict"):
            Namespaced({}, "ns")  # type: ignore


class TestNamespacedWrite:
    def test_set_prefixes_key(self):
        s = _staged()
        ns = Namespaced(s, "myns")
        ns.set("k", b"v")
        assert s.get("myns/k") == b"v"

    def test_remove_prefixed(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("x", b"1")
        ns.set("y", b"2")
        ns.remove("x")
        assert ns.get("x") is None
        assert ns.get("y") == b"2"

    def test_commit_delegates(self):
        store = Memory()
        s1 = _staged(store)
        ns1 = Namespaced(s1, "app")
        ns1.set("k", b"1")
        result = ns1.commit()
        assert isinstance(result, MergeResult)
        assert result.merged

        s2 = _staged(store)
        ns2 = Namespaced(s2, "app")
        assert ns2.get("k") == b"1"

    def test_set_content_type_prefixed(self):
        """set_content_type registers with the prefixed key."""
        store = Memory()
        ct = counter()

        s1 = _staged(store)
        ns1 = Namespaced(s1, "stats")
        ns1.set("hits", ct.encode(10))
        ns1.commit()

        s2 = _staged(store)
        ns2 = Namespaced(s2, "stats")
        ns2.set_content_type("hits", ct)

        # Diverge: s1 writes 15, s2 writes 20
        ns1.set("hits", ct.encode(15))
        ns1.commit()
        ns2.set("hits", ct.encode(20))

        assert ns2.commit()
        assert ct.decode(ns2.get("hits")) == 25  # 15 + 20 - 10

    def test_two_namespaces_independent_writes(self):
        s = _staged()
        ns1 = Namespaced(s, "one")
        ns2 = Namespaced(s, "two")
        ns1.set("k", b"from-one")
        ns2.set("k", b"from-two")
        assert ns1.get("k") == b"from-one"
        assert ns2.get("k") == b"from-two"


class TestNamespacedProperties:
    def test_current_commit_delegates(self):
        s = _staged()
        ns = Namespaced(s, "app")
        assert ns.current_commit == s.current_commit
        ns.set("k", b"v")
        ns.commit()
        assert ns.current_commit == s.current_commit

    def test_base_commit_delegates(self):
        s = _staged()
        ns = Namespaced(s, "app")
        assert ns.base_commit == s.base_commit

    def test_last_merge_result_delegates(self):
        store = Memory()
        s = _staged(store)
        ns = Namespaced(s, "app")
        ns.set("k", b"v")
        ns.commit()
        assert ns.last_merge_result is s.last_merge_result
        assert ns.last_merge_result.merged
