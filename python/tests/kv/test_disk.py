"""Tests for the Disk KV store."""

import multiprocessing
import os
import shutil
import signal
import sqlite3
import tempfile
import threading
from collections.abc import Iterable, Mapping

import diskcache
import pytest

from kvgit import Repo, StorageVersionError
from kvgit.kv.base import KVStore
from kvgit.kv.disk import DB_NAME, Disk


@pytest.fixture
def disk_store():
    tmpdir = tempfile.mkdtemp()
    store = Disk(tmpdir)
    yield store, tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


class TestDiskBasic:
    def test_set_get(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        assert store.get("k") == b"v"

    def test_get_missing(self, disk_store):
        store, _ = disk_store
        assert store.get("nope") is None

    def test_contains(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        assert "k" in store
        assert "nope" not in store

    def test_set_many_get_many(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2", c=b"3")
        result = store.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_type_error_on_non_bytes(self, disk_store):
        store, _ = disk_store
        with pytest.raises(TypeError, match="Expected bytes"):
            store.set("k", "not bytes")  # type: ignore

    def test_clear(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2")
        store.clear()
        assert store.get("a") is None


class TestDiskPersistence:
    def test_survives_reload(self, disk_store):
        store, tmpdir = disk_store
        store.set("k", b"persistent")
        del store
        store2 = Disk(tmpdir)
        assert store2.get("k") == b"persistent"

    def test_cas_persists(self, disk_store):
        store, tmpdir = disk_store
        store.set("k", b"old")
        assert store.cas("k", b"new", expected=b"old")
        del store
        store2 = Disk(tmpdir)
        assert store2.get("k") == b"new"


class TestDiskRemove:
    def test_remove(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        store.remove("k")
        assert store.get("k") is None

    def test_remove_many(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2", c=b"3")
        store.remove_many("a", "c")
        assert store.get("a") is None
        assert store.get("b") == b"2"


class TestDiskCAS:
    def test_cas_success(self, disk_store):
        store, _ = disk_store
        store.set("k", b"old")
        assert store.cas("k", b"new", expected=b"old")
        assert store.get("k") == b"new"

    def test_cas_failure(self, disk_store):
        store, _ = disk_store
        store.set("k", b"old")
        assert not store.cas("k", b"new", expected=b"wrong")
        assert store.get("k") == b"old"


class TestDiskBulkCallForms:
    """Disk backend supports both variadic and container call forms."""

    def test_set_many_mapping_form(self, disk_store):
        store, _ = disk_store
        store.set_many({"a": b"1", "b": b"2"})
        assert store.get("a") == b"1"
        assert store.get("b") == b"2"

    def test_set_many_kwargs_form(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2")
        assert store.get("a") == b"1"
        assert store.get("b") == b"2"

    def test_get_many_iterable_form(self, disk_store):
        store, _ = disk_store
        store.set_many({"a": b"1", "b": b"2"})
        assert store.get_many(["a", "b"]) == {"a": b"1", "b": b"2"}

    def test_remove_many_iterable_form(self, disk_store):
        store, _ = disk_store
        store.set_many({"a": b"1", "b": b"2", "c": b"3"})
        store.remove_many(["a", "c"])
        assert store.get("a") is None
        assert store.get("b") == b"2"


class TestDiskFiles:
    def test_one_database_file(self, disk_store):
        store, tmpdir = disk_store
        store.set("big", b"x" * (1 << 20))
        store.close()
        assert sorted(
            n for n in os.listdir(tmpdir) if not n.endswith(("-wal", "-shm"))
        ) == [DB_NAME]

    def test_keys_by_prefix_reads_the_index(self, disk_store):
        store, _ = disk_store
        plan = (
            store._db()
            .execute(
                "EXPLAIN QUERY PLAN SELECT k FROM kv WHERE k >= ? AND k < ?", ("a", "b")
            )
            .fetchall()
        )
        assert "SEARCH" in str(plan)

    def test_a_refused_commit_leaves_the_connection_usable(self, disk_store):
        store, _ = disk_store
        store.set("k", b"old")
        real = store._db()

        class RefusesCommit:
            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, sql, *args):
                if sql == "COMMIT":
                    raise sqlite3.OperationalError("disk I/O error")
                return real.execute(sql, *args)

        store._local.db = RefusesCommit()
        with pytest.raises(sqlite3.OperationalError):
            store.set("k", b"new")
        store._local.db = real
        assert not real.in_transaction
        assert store.get("k") == b"old"
        store.set("k", b"newer")
        assert store.get("k") == b"newer"

    def test_a_relative_directory_survives_a_change_of_directory(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        store = Disk("store")
        store.set("k", b"v")
        (tmp_path / "elsewhere").mkdir()
        monkeypatch.chdir(tmp_path / "elsewhere")
        seen = []
        worker = threading.Thread(target=lambda: seen.append(store.get("k")))
        worker.start()
        worker.join()
        assert seen == [b"v"]
        assert not os.path.exists(tmp_path / "elsewhere" / "store")

    def test_ended_threads_connections_are_closed(self, disk_store):
        store, _ = disk_store
        store.get("k")
        for _ in range(20):
            worker = threading.Thread(target=store.get, args=("k",))
            worker.start()
            worker.join()
        assert len(store._connections) <= 2

    def test_close_then_reuse(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        store.close()
        assert store.get("k") == b"v"


def _bump(directory: str, times: int) -> None:
    store = Disk(directory)
    for _ in range(times):
        while True:
            raw = store.get("n")
            if store.cas("n", str(int(raw or b"0") + 1).encode(), expected=raw):
                break
    store.close()


class TestDiskProcesses:
    def test_cas_across_processes(self, disk_store):
        store, tmpdir = disk_store
        ctx = multiprocessing.get_context("spawn")
        procs = [ctx.Process(target=_bump, args=(tmpdir, 50)) for _ in range(4)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        assert all(p.exitcode == 0 for p in procs)
        assert store.get("n") == b"200"


class _DiskcacheStore(KVStore):
    """A store in diskcache's layout, as kvgit's disk backend kept one."""

    def __init__(self, cache: diskcache.Cache) -> None:
        self.cache = cache

    def get(self, key: str) -> bytes | None:
        return self.cache.get(key)

    def set(self, key: str, value: bytes) -> None:
        self.cache[key] = value

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = self._normalize_keys(args)
        return {k: v for k in keys if (v := self.cache.get(k)) is not None}

    def set_many(self, items=None, /, **kwargs) -> None:
        with self.cache.transact():
            for k, v in self._normalize_items(items, kwargs).items():
                self.cache[k] = v

    def items(self) -> Iterable[tuple[str, bytes]]:
        return [(k, self.cache[k]) for k in self.cache.iterkeys()]

    def keys(self, prefix: str = "") -> Iterable[str]:
        return [k for k in self.cache.iterkeys() if k.startswith(prefix)]

    def __contains__(self, key: str) -> bool:
        return key in self.cache

    def remove(self, key: str) -> None:
        self.cache.delete(key)

    def remove_many(self, *args) -> None:
        with self.cache.transact():
            for k in self._normalize_keys(args):
                self.cache.delete(k)

    def cas_many(self, expected, writes, removes=()) -> bool:
        removes = self._check_batch(writes, removes)
        with self.cache.transact():
            if any(self.cache.get(k) != v for k, v in expected.items()):
                return False
            for k, v in writes.items():
                self.cache[k] = v
            for k in removes:
                self.cache.delete(k)
            return True

    def clear(self) -> None:
        self.cache.clear()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
class TestDiskFork:
    def test_a_child_forked_while_the_lock_is_held_reads_on(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        with store._lock:
            pid = os.fork()
            if pid == 0:
                signal.alarm(10)  # a hang fails the child rather than the run
                ok = store.get("k") == b"v" and store.cas("n", b"1", expected=None)
                os._exit(0 if ok else 1)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert store.get("n") == b"1"


def _diskcache_store(directory: str, items: dict) -> None:
    cache = diskcache.Cache(directory)
    for key, value in items.items():
        cache[key] = value
    cache.close()


class TestImportDiskcache:
    def test_refuses_a_diskcache_directory(self, tmp_path):
        _diskcache_store(str(tmp_path), {"k": b"v"})
        with pytest.raises(StorageVersionError, match="import_diskcache"):
            Disk(str(tmp_path))

    def test_copies_in_place(self, tmp_path):
        big = os.urandom(1 << 16)
        _diskcache_store(str(tmp_path), {"small": b"v", "big": big, "empty": b""})
        store = Disk.import_diskcache(str(tmp_path))
        assert store.get_many("small", "big", "empty") == {
            "small": b"v",
            "big": big,
            "empty": b"",
        }
        assert os.path.exists(tmp_path / "cache.db")
        assert Disk(str(tmp_path)).get("big") == big

    def test_a_path_with_uri_characters(self, tmp_path):
        odd = str(tmp_path / "a?b#c%20d")
        _diskcache_store(odd, {"k": b"v"})
        assert Disk.import_diskcache(odd).get("k") == b"v"

    def test_copies_to_another_directory(self, tmp_path):
        _diskcache_store(str(tmp_path / "old"), {"k": b"v"})
        store = Disk.import_diskcache(str(tmp_path / "old"), str(tmp_path / "new"))
        assert store.directory == str(tmp_path / "new")
        assert dict(store.items()) == {"k": b"v"}

    def test_a_repository_reads_on(self, tmp_path):
        old = str(tmp_path / "old")
        _diskcache_store(old, {})
        cache = diskcache.Cache(old)
        repo = Repo(_DiskcacheStore(cache), codec="bytes")
        wt = repo.worktree("main", create=True)
        wt["a"] = b"1"
        wt["b"] = os.urandom(1 << 16)
        wt.commit()
        head, b = wt.head, wt["b"]
        cache.close()
        repo = Repo(Disk.import_diskcache(old, str(tmp_path / "new")), codec="bytes")
        wt = repo.worktree("main")
        assert wt.head == head and wt["a"] == b"1" and wt["b"] == b
        assert repo.gc(min_age=0) == 0

    def test_refuses_a_pickled_value_without_unpickling(self, tmp_path):
        _diskcache_store(str(tmp_path), {"k": b"v", "obj": {"not": "bytes"}})
        with pytest.raises(ValueError, match="obj"):
            Disk.import_diskcache(str(tmp_path))
        assert not [n for n in os.listdir(tmp_path) if n.startswith(DB_NAME)]

    def test_refuses_a_key_that_is_not_text(self, tmp_path):
        _diskcache_store(str(tmp_path), {b"bytes-key": b"v"})
        with pytest.raises(ValueError, match="not plain text"):
            Disk.import_diskcache(str(tmp_path))

    def test_refuses_a_destination_holding_a_store(self, tmp_path):
        _diskcache_store(str(tmp_path / "old"), {"k": b"v"})
        Disk(str(tmp_path / "new")).set("x", b"y")
        with pytest.raises(FileExistsError):
            Disk.import_diskcache(str(tmp_path / "old"), str(tmp_path / "new"))
        assert dict(Disk(str(tmp_path / "new")).items()) == {"x": b"y"}

    def test_leaves_a_store_made_at_the_destination_meanwhile(
        self, tmp_path, monkeypatch
    ):
        import kvgit.kv.disk as disk

        _diskcache_store(str(tmp_path / "old"), {"k": b"v"})
        rows = disk._diskcache_rows

        def racing(db, directory):
            Disk(str(tmp_path / "new")).set("x", b"y")
            yield from rows(db, directory)

        monkeypatch.setattr(disk, "_diskcache_rows", racing)
        with pytest.raises(FileExistsError):
            Disk.import_diskcache(str(tmp_path / "old"), str(tmp_path / "new"))
        assert dict(Disk(str(tmp_path / "new")).items()) == {"x": b"y"}
        assert not [n for n in os.listdir(tmp_path / "new") if ".import-" in n]

    def test_needs_a_diskcache_store(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Disk.import_diskcache(str(tmp_path))
