"""Tests for the Namespaced wrapper."""

import pytest

from vkv import Namespaced, Versioned


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
