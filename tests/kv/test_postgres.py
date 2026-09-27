"""Tests for the Postgres KV store.

Needs a reachable server: KVGIT_POSTGRES_DSN (default ``dbname=kvgit_test``).
Skipped when psycopg is missing or the server does not answer.
"""

import os
import threading
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from kvgit.kv.postgres import Postgres  # noqa: E402

DSN = os.environ.get("KVGIT_POSTGRES_DSN", "dbname=kvgit_test")

try:
    psycopg.connect(DSN, connect_timeout=2).close()
except psycopg.OperationalError:
    pytest.skip(f"no Postgres at {DSN!r}", allow_module_level=True)


@pytest.fixture
def table():
    return "t_" + uuid.uuid4().hex[:12]


@pytest.fixture
def store(table):
    s = Postgres(DSN, table=table)
    yield s
    s.drop()
    s.close()


class TestBasic:
    def test_set_get_missing(self, store):
        store.set("k", b"v")
        assert store.get("k") == b"v"
        assert store.get("nope") is None
        assert "k" in store and "nope" not in store

    def test_overwrite(self, store):
        store.set("k", b"1")
        store.set("k", b"2")
        assert store.get("k") == b"2"

    def test_binary_and_unicode(self, store):
        store.set("ключ/🔑", b"\x00\xff\x00")
        assert store.get("ключ/🔑") == b"\x00\xff\x00"

    def test_bulk(self, store):
        store.set_many({"a": b"1", "b": b"2"}, c=b"3")
        assert store.get_many("a", "c", "missing") == {"a": b"1", "c": b"3"}
        store.remove_many(["a", "b"])
        assert sorted(store.keys()) == ["c"]
        assert list(store.items()) == [("c", b"3")]

    def test_type_error(self, store):
        with pytest.raises(TypeError, match="Expected bytes"):
            store.set("k", "str")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected bytes"):
            store.set_many(k="str")  # type: ignore[arg-type]

    def test_clear(self, store):
        store.set_many(a=b"1", b=b"2")
        store.clear()
        assert list(store.keys()) == []


class TestPrefixKeys:
    def test_prefix_range(self, store):
        store.set_many(
            {
                "__branch_head__main": b"",
                "__branch_head__dev": b"",
                "__branch_head_prev__main": b"",
                "__branch_heae": b"",
                "x": b"",
            }
        )
        assert sorted(store.keys("__branch_head__")) == [
            "__branch_head__dev",
            "__branch_head__main",
        ]
        assert len(list(store.keys())) == 5

    def test_prefix_non_ascii(self, store):
        store.set_many({"é/a": b"", "é/b": b"", "ê": b"", "e": b""})
        assert sorted(store.keys("é/")) == ["é/a", "é/b"]


class TestCAS:
    def test_cas(self, store):
        assert store.cas("k", b"1", None)
        assert not store.cas("k", b"2", None)
        assert not store.cas("k", b"2", b"0")
        assert store.cas("k", b"2", b"1")
        assert store.get("k") == b"2"

    def test_cas_across_pools(self, table):
        """Writers on separate pools (separate processes, in effect) race
        one counter; every increment must land exactly once."""
        stores = [Postgres(DSN, table=table, max_size=2) for _ in range(4)]
        stores[0].set("n", b"0")

        def bump(s):
            done = 0
            while done < 50:
                cur = s.get("n")
                if s.cas("n", str(int(cur) + 1).encode(), cur):
                    done += 1

        threads = [threading.Thread(target=bump, args=(s,)) for s in stores]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert stores[0].get("n") == b"200"
        stores[0].drop()
        for s in stores:
            s.close()


def test_persists_across_instances(table):
    a = Postgres(DSN, table=table)
    a.set("k", b"v")
    a.close()
    b = Postgres(DSN, table=table)
    assert b.get("k") == b"v"
    b.drop()
    b.close()


def test_rejects_bad_table_name():
    with pytest.raises(ValueError, match="invalid table name"):
        Postgres(DSN, table="x; drop table y")


def test_a_failed_batch_leaves_nothing_and_the_connection_usable(table):
    """A statement that fails mid-transaction rolls the batch back and
    returns the connection to the pool clean."""
    s = Postgres(DSN, table=table, max_size=1)
    s.set("keep", b"1")
    with pytest.raises(psycopg.DataError):
        # Postgres text cannot hold NUL, so the write statement fails
        # after the check and before the commit.
        s.cas_many({"keep": b"1"}, {"ok": b"x", "bad\x00key": b"y"})
    assert s.get("ok") is None
    assert s.cas_many({"keep": b"1"}, {"ok": b"x"})
    assert s.get("ok") == b"x"
    s.drop()
    s.close()
