"""Tests for the Namespaced wrapper."""

import pytest

from kvit import Live, MergeResult, Namespaced, Staged, Versioned, counter
from kvit.kv.memory import Memory


def _staged(store=None, **kwargs):
    """Helper to create a Staged store."""
    return Staged(Versioned(store, **kwargs))


class TestNamespacedBasic:
    def test_get_set(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("greeting", "hello")
        assert ns.get("greeting") == "hello"

    def test_get_missing(self):
        s = _staged()
        ns = Namespaced(s, "app")
        assert ns.get("nope") is None

    def test_get_default(self):
        s = _staged()
        ns = Namespaced(s, "app")
        assert ns.get("nope", "fallback") == "fallback"

    def test_contains(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("k", "v")
        assert "k" in ns
        assert "nope" not in ns

    def test_get_many(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("a", 1)
        ns.set("b", 2)
        s.set("other/c", 3)
        result = ns.get_many("a", "b", "c")
        assert result == {"a": 1, "b": 2}


class TestNamespacedMutableMapping:
    def test_getitem(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns["k"] = "v"
        assert ns["k"] == "v"

    def test_getitem_missing_raises(self):
        s = _staged()
        ns = Namespaced(s, "app")
        with pytest.raises(KeyError):
            ns["nope"]

    def test_delitem(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns["k"] = "v"
        del ns["k"]
        assert ns.get("k") is None

    def test_iter(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns["a"] = 1
        ns["b"] = 2
        assert set(ns) == {"a", "b"}

    def test_len(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns["a"] = 1
        ns["b"] = 2
        assert len(ns) == 2


class TestNamespacedIsolation:
    def test_two_namespaces_isolated(self):
        s = _staged()
        ns1 = Namespaced(s, "one")
        ns2 = Namespaced(s, "two")
        ns1.set("k", "from-one")
        ns2.set("k", "from-two")
        assert ns1.get("k") == "from-one"
        assert ns2.get("k") == "from-two"

    def test_keys_only_direct_children(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("a", 1)
        ns.set("b", 2)
        s.set("app/sub/c", 3)  # nested — not a direct child
        s.set("other/d", 4)  # different namespace
        keys = set(ns.keys())
        assert keys == {"a", "b"}

    def test_descendant_keys(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("a", 1)
        s.set("app/sub/b", 2)
        s.set("app/sub/deep/c", 3)
        s.set("other/d", 4)
        descendants = set(ns.descendant_keys())
        assert descendants == {"a", "sub/b", "sub/deep/c"}


class TestNamespacedNested:
    def test_nested_namespace(self):
        s = _staged()
        ns1 = Namespaced(s, "agent")
        ns2 = Namespaced(ns1, "worker")
        assert ns2.namespace == "agent/worker"

        ns2.set("task", "data")
        assert ns2.get("task") == "data"

    def test_deeply_nested(self):
        s = _staged()
        ns = Namespaced(Namespaced(Namespaced(s, "a"), "b"), "c")
        assert ns.namespace == "a/b/c"

        ns.set("key", "deep")
        assert ns.get("key") == "deep"

    def test_nested_keys_correct(self):
        """Nested namespace keys() works correctly (unwrap fix)."""
        s = _staged()
        ns1 = Namespaced(s, "agent")
        ns2 = Namespaced(ns1, "worker")

        ns2.set("task", "data")
        assert set(ns2.keys()) == {"task"}
        assert s.get("agent/worker/task") == "data"

    def test_nested_stores_at_root(self):
        """Nested namespace stores keys at the correct path in root store."""
        s = _staged()
        ns = Namespaced(Namespaced(Namespaced(s, "a"), "b"), "c")
        ns["key"] = "deep"
        assert s.get("a/b/c/key") == "deep"


class TestNamespacedValidation:
    def test_slash_in_namespace_rejected(self):
        s = _staged()
        with pytest.raises(ValueError, match="cannot contain '/'"):
            Namespaced(s, "bad/name")

    def test_invalid_store_type_rejected(self):
        with pytest.raises(TypeError, match="not dict"):
            Namespaced({}, "ns")  # type: ignore[arg-type]


class TestNamespacedWrite:
    def test_set_prefixes_key(self):
        s = _staged()
        ns = Namespaced(s, "myns")
        ns.set("k", "v")
        assert s.get("myns/k") == "v"

    def test_remove_prefixed(self):
        s = _staged()
        ns = Namespaced(s, "app")
        ns.set("x", 1)
        ns.set("y", 2)
        ns.remove("x")
        assert ns.get("x") is None
        assert ns.get("y") == 2

    def test_commit_through_store(self):
        """Commit is done on the store, not the namespace."""
        store = Memory()
        s1 = _staged(store)
        ns1 = Namespaced(s1, "app")
        ns1.set("k", 1)
        result = s1.commit()
        assert isinstance(result, MergeResult)
        assert result.merged

        s2 = _staged(store)
        ns2 = Namespaced(s2, "app")
        assert ns2.get("k") == 1

    def test_merge_fn_with_namespace(self):
        """Merge functions are registered on Staged with full prefixed key."""
        store = Memory()

        s1 = _staged(store)
        ns1 = Namespaced(s1, "stats")
        ns1.set("hits", 10)
        s1.commit()

        s2 = _staged(store)
        s2.set_merge_fn("stats/hits", counter())

        # Diverge: s1 writes 15, s2 writes 20
        ns1.set("hits", 15)
        s1.commit()

        ns2 = Namespaced(s2, "stats")
        ns2.set("hits", 20)

        assert s2.commit()
        assert ns2.get("hits") == 25  # 15 + 20 - 10

    def test_two_namespaces_independent_writes(self):
        s = _staged()
        ns1 = Namespaced(s, "one")
        ns2 = Namespaced(s, "two")
        ns1.set("k", "from-one")
        ns2.set("k", "from-two")
        assert ns1.get("k") == "from-one"
        assert ns2.get("k") == "from-two"


class TestNamespacedProtocol:
    def test_satisfies_store(self):
        from kvit import Store

        s = _staged()
        ns = Namespaced(s, "app")
        assert isinstance(ns, Store)

    def test_wraps_live(self):
        """Namespaced can wrap a Live store."""
        live = Live()
        ns = Namespaced(live, "app")
        ns["k"] = "v"
        assert ns["k"] == "v"
        assert live.get("app/k") == "v"
