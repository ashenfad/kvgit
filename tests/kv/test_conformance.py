"""What every ``KVStore`` owes kvgit beyond reads and writes.

``cas_many`` and ``keys(prefix)`` are run against each backend that can
run here — Postgres when a server answers at ``KVGIT_POSTGRES_DSN``
(default ``dbname=kvgit_test``); the IndexedDB backend has the same
cases in ``test_indexeddb.py``, run in a browser.
"""

import os
import threading
import uuid

import pytest

from kvgit.kv.composite import Composite
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory

POSTGRES_DSN = os.environ.get("KVGIT_POSTGRES_DSN", "dbname=kvgit_test")


def _postgres_unavailable() -> str | None:
    """Why the Postgres cases cannot run here, or None if they can."""
    try:
        import psycopg
    except ImportError:
        return "psycopg is not installed (kvgit[postgres])"
    try:
        psycopg.connect(POSTGRES_DSN, connect_timeout=2).close()
    except psycopg.OperationalError:
        return f"no Postgres at {POSTGRES_DSN!r}"
    return None


_WHY_NOT_POSTGRES = _postgres_unavailable()
needs_postgres = pytest.mark.skipif(
    _WHY_NOT_POSTGRES is not None, reason=_WHY_NOT_POSTGRES or ""
)


@pytest.fixture(
    params=[
        "memory",
        "disk",
        "composite-memory",
        "composite-disk",
        pytest.param("postgres", marks=needs_postgres),
    ]
)
def store(request, tmp_path):
    kind = request.param
    if kind == "postgres":
        from kvgit.kv.postgres import Postgres

        pg = Postgres(POSTGRES_DSN, table="t_" + uuid.uuid4().hex[:12])
        yield pg
        pg.drop()
        pg.close()
    elif kind == "memory":
        yield Memory()
    elif kind == "disk":
        disk = Disk(str(tmp_path / "store"))
        yield disk
        disk.close()
    elif kind == "composite-memory":
        yield Composite([Memory(), Memory()])
    else:
        disk = Disk(str(tmp_path / "store"))
        yield Composite([Memory(), disk])
        disk.close()


class TestCasMany:
    def test_applies_when_every_expectation_holds(self, store):
        store.set_many({"a": b"1", "gone": b"x"})
        assert store.cas_many(
            {"a": b"1", "absent": None}, {"b": b"2", "c": b"3"}, ["gone"]
        )
        assert store.get_many("a", "b", "c", "gone") == {
            "a": b"1",
            "b": b"2",
            "c": b"3",
        }

    def test_applies_nothing_when_any_expectation_fails(self, store):
        store.set_many({"a": b"1", "keep": b"x"})
        assert not store.cas_many({"a": b"1", "keep": b"other"}, {"b": b"2"}, ["a"])
        assert not store.cas_many({"a": b"1", "absent": b"y"}, {"b": b"2"}, ["a"])
        assert not store.cas_many({"a": None}, {"b": b"2"})
        assert store.get_many("a", "b", "keep") == {"a": b"1", "keep": b"x"}

    def test_an_expected_key_may_be_rewritten(self, store):
        assert store.cas_many({"k": None}, {"k": b"1"})
        assert store.cas_many({"k": b"1"}, {"k": b"2"})
        assert not store.cas_many({"k": b"1"}, {"k": b"3"})
        assert store.get("k") == b"2"

    def test_no_expectations_is_an_atomic_batch(self, store):
        assert store.cas_many({}, {"a": b"1"}, ["missing"])
        assert store.get("a") == b"1"

    def test_rejects_non_bytes_and_a_key_both_written_and_removed(self, store):
        with pytest.raises(TypeError, match="Expected bytes"):
            store.cas_many({}, {"a": "str"})  # type: ignore[dict-item]
        with pytest.raises(ValueError, match="both written and removed"):
            store.cas_many({}, {"a": b"1"}, ["a"])
        assert store.get("a") is None

    def test_cas_is_the_one_key_case(self, store):
        assert store.cas("k", b"1", None)
        assert not store.cas("k", b"2", None)
        assert store.cas("k", b"2", b"1")
        assert store.get("k") == b"2"

    def test_concurrent_writers_each_land_exactly_once(self, store):
        store.set("n", b"0")

        def bump():
            done = 0
            while done < 50:
                cur = store.get("n")
                nxt = str(int(cur) + 1).encode()
                if store.cas_many({"n": cur}, {"n": nxt, f"seen/{nxt.decode()}": b""}):
                    done += 1

        threads = [threading.Thread(target=bump) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert store.get("n") == b"200"
        assert len(list(store.keys("seen/"))) == 200


class TestKeysByPrefix:
    def test_prefix_selects_exactly_the_keys_under_it(self, store):
        store.set_many(
            {
                "__branch_head__main": b"",
                "__branch_head__dev": b"",
                "__branch_head_prev__main": b"",
                "__branch_heae": b"",
                "kvgit:blob:ab": b"",
                "x": b"",
            }
        )
        assert sorted(store.keys("__branch_head__")) == [
            "__branch_head__dev",
            "__branch_head__main",
        ]
        assert list(store.keys("kvgit:blob:")) == ["kvgit:blob:ab"]
        assert list(store.keys("nothing-here")) == []
        assert len(list(store.keys())) == 6
        assert sorted(store.keys("")) == sorted(store.keys())

    def test_non_ascii_prefixes(self, store):
        store.set_many({"é/a": b"", "é/b": b"", "ê": b"", "e": b"", "🔑/x": b""})
        assert sorted(store.keys("é/")) == ["é/a", "é/b"]
        assert list(store.keys("🔑")) == ["🔑/x"]

    def test_prefixes_ending_at_the_last_code_point(self, store):
        top = chr(0x10FFFF)
        store.set_many(
            {f"a{top}": b"", f"a{top}x": b"", "b": b"", top: b"", f"{top}y": b""}
        )
        assert sorted(store.keys(f"a{top}")) == [f"a{top}", f"a{top}x"]
        assert sorted(store.keys(top)) == [top, f"{top}y"]
