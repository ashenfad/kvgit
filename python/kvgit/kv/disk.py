"""Disk-backed KV store: one SQLite table in a directory of its own."""

import os
import sqlite3
import threading
import uuid
import weakref
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from pathlib import Path

from ..errors import StorageVersionError
from .base import KVStore, prefix_upper_bound

DB_NAME = "kvgit.sqlite3"
"""The database file inside a ``Disk`` directory."""

_DISKCACHE_DB = "cache.db"

# Keys per statement in a batch read or check: well under SQLite's
# smallest host-parameter limit (999).
_BATCH = 500

# How long a statement waits for another connection's lock, in seconds.
_BUSY_TIMEOUT = 60.0

# The page size of a new database: large enough that a chunk of several
# megabytes spans few overflow pages, small enough that a commit's small
# records don't rewrite much around them.
_PAGE_SIZE = 16384

# diskcache's value modes: the bytes in the row, or in a file beside it.
_MODE_RAW = 1
_MODE_BINARY = 2

# A rowid table: its rows hold values of any size, which a WITHOUT ROWID
# table's index pages carry badly, and the primary key's index answers
# key lookups and prefix ranges.
_SCHEMA = "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v BLOB NOT NULL)"

# Every open store, so a forked child can drop what it inherited.
_STORES: "weakref.WeakSet[Disk]" = weakref.WeakSet()


def _after_fork_in_child() -> None:
    """A forked child starts each store afresh: a new lock, since the
    parent's may have been held by a thread the child doesn't have, and
    no connections, since SQLite's must not cross a fork. The parent's
    are left unclosed; it still uses them."""
    for store in list(_STORES):
        store._reset()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


class Disk(KVStore):
    """A KV store in one SQLite table, in a directory of its own.

    Keys are a ``TEXT`` primary key, which SQLite compares as UTF-8
    bytes, so ``keys(prefix)`` is an index range scan; values are
    ``BLOB``s, however large. The database runs in WAL mode, so readers
    in other processes never block a writer, and each thread uses its
    own connection. Every write is one transaction, and ``cas_many``
    opens with ``BEGIN IMMEDIATE``: it holds the write lock from its
    first read, so no other process's write can land between the check
    and the batch.

    A directory holding a diskcache store and no ``kvgit.sqlite3`` is
    refused with ``StorageVersionError``; :meth:`import_diskcache` copies
    such a store.

    Args:
        directory: The store's directory, created if missing.
    """

    def __init__(self, directory: str) -> None:
        # Absolute, so a later change of working directory can't point a
        # new connection at another database.
        directory = os.path.abspath(directory)
        path = os.path.join(directory, DB_NAME)
        if not os.path.exists(path) and os.path.exists(
            os.path.join(directory, _DISKCACHE_DB)
        ):
            raise StorageVersionError(
                f"{directory!r} holds a diskcache store; copy it into this "
                "backend's format with Disk.import_diskcache(directory)"
            )
        os.makedirs(directory, exist_ok=True)
        self.directory = directory
        self.path = path
        self._reset()
        _STORES.add(self)
        with self._write() as db:
            db.execute(_SCHEMA)

    # ---- connections ----

    def _reset(self) -> None:
        """No connections yet, and a lock guarding the list of them."""
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connections: list[tuple[threading.Thread, sqlite3.Connection]] = []

    def _db(self) -> sqlite3.Connection:
        """This thread's connection, opened on first use. Opening one
        closes those of threads that have ended, so the store holds one
        per live thread however many come and go."""
        db = getattr(self._local, "db", None)
        if db is None:
            # Only this thread runs statements on it; ``close`` may close
            # it from another.
            db = sqlite3.connect(
                self.path,
                timeout=_BUSY_TIMEOUT,
                isolation_level=None,
                check_same_thread=False,
            )
            db.execute(f"PRAGMA page_size={_PAGE_SIZE}")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            self._local.db = db
            with self._lock:
                ended = [c for t, c in self._connections if not t.is_alive()]
                self._connections = [
                    (t, c) for t, c in self._connections if t.is_alive()
                ]
                self._connections.append((threading.current_thread(), db))
            for c in ended:
                c.close()
        return db

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """One write transaction, holding the write lock from its start."""
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            yield db
            db.execute("COMMIT")
        except BaseException:
            # A COMMIT SQLite refused can leave the transaction open.
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise

    def _rows(self, sql: str, keys: list[str]) -> Iterator[tuple[str, bytes]]:
        """``sql``'s rows for ``keys``, a batch of them per statement;
        ``sql`` holds one ``{}`` for the batch's placeholders."""
        db = self._db()
        for start in range(0, len(keys), _BATCH):
            batch = keys[start : start + _BATCH]
            yield from db.execute(sql.format(",".join("?" * len(batch))), batch)

    # ---- reads ----

    def get(self, key: str) -> bytes | None:
        row = self._db().execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
        return None if row is None else row[0]

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = list(dict.fromkeys(self._normalize_keys(args)))
        return dict(self._rows("SELECT k, v FROM kv WHERE k IN ({})", keys))

    def items(self) -> Iterable[tuple[str, bytes]]:
        return self._db().execute("SELECT k, v FROM kv ORDER BY k").fetchall()

    def keys(self, prefix: str = "") -> Iterable[str]:
        upper = prefix_upper_bound(prefix)
        if upper is None:
            rows = self._db().execute("SELECT k FROM kv WHERE k >= ?", (prefix,))
        else:
            rows = self._db().execute(
                "SELECT k FROM kv WHERE k >= ? AND k < ?", (prefix, upper)
            )
        return [k for (k,) in rows.fetchall()]

    def __contains__(self, key: str) -> bool:
        return (
            self._db().execute("SELECT 1 FROM kv WHERE k = ?", (key,)).fetchone()
            is not None
        )

    # ---- writes ----

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        with self._write() as db:
            db.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", (key, value))

    def set_many(
        self,
        items: Mapping[str, bytes] | None = None,
        /,
        **kwargs: bytes,
    ) -> None:
        items = self._normalize_items(items, kwargs)
        for key, value in items.items():
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for {key}, got {type(value).__name__}")
        with self._write() as db:
            db.executemany(
                "INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", items.items()
            )

    def remove(self, key: str) -> None:
        with self._write() as db:
            db.execute("DELETE FROM kv WHERE k = ?", (key,))

    def remove_many(self, *args) -> None:
        keys = [(k,) for k in self._normalize_keys(args)]
        with self._write() as db:
            db.executemany("DELETE FROM kv WHERE k = ?", keys)

    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        removes = self._check_batch(writes, removes)
        with self._write() as db:
            found = dict(
                self._rows("SELECT k, v FROM kv WHERE k IN ({})", list(expected))
            )
            if any(found.get(k) != v for k, v in expected.items()):
                return False
            db.executemany(
                "INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", writes.items()
            )
            db.executemany("DELETE FROM kv WHERE k = ?", [(k,) for k in removes])
            return True

    def clear(self) -> None:
        with self._write() as db:
            db.execute("DELETE FROM kv")

    def close(self) -> None:
        """Close every connection this store opened. A short-lived store
        (a one-off sweep, a teardown) closes so its files are released."""
        with self._lock:
            connections, self._connections = self._connections, []
            self._local = threading.local()
        for _, db in connections:
            db.close()

    # ---- importing ----

    @classmethod
    def import_diskcache(cls, source: str, destination: str | None = None) -> "Disk":
        """Copy the diskcache store in ``source`` into a new ``Disk``.

        ``destination`` defaults to ``source``: the new database is
        written beside diskcache's files, which stay until their owner
        removes them, and the directory opens as a ``Disk`` from then
        on. Every key is copied with its bytes, the values diskcache kept
        in files beside its database included; nothing is unpickled, and
        a key or value that isn't plain text and bytes, which kvgit never
        writes, refuses the import with ``ValueError``. The database is
        written under a temporary name and linked into place whole, so an
        import that fails leaves nothing that opens, and one that finds a
        store made at the destination meanwhile leaves it be.

        Run it with no writer on ``source``. Raises ``FileExistsError``
        if ``destination`` already holds a ``Disk`` store.
        """
        destination = source if destination is None else destination
        cache_db = os.path.join(source, _DISKCACHE_DB)
        if not os.path.exists(cache_db):
            raise FileNotFoundError(f"no diskcache store in {source!r}")
        path = os.path.join(destination, DB_NAME)
        if os.path.exists(path):
            raise FileExistsError(f"{destination!r} already holds a Disk store")
        os.makedirs(destination, exist_ok=True)
        partial = f"{path}.import-{uuid.uuid4().hex}"
        # A file URI escapes what a path may hold and a URI reserves (`?`, `#`).
        src = sqlite3.connect(Path(cache_db).resolve().as_uri() + "?mode=ro", uri=True)
        try:
            out = sqlite3.connect(partial, isolation_level=None)
            try:
                out.execute(f"PRAGMA page_size={_PAGE_SIZE}")
                out.execute(_SCHEMA)
                out.execute("BEGIN")
                out.executemany(
                    "INSERT INTO kv (k, v) VALUES (?, ?)",
                    _diskcache_rows(src, source),
                )
                out.execute("COMMIT")
            finally:
                out.close()
        except BaseException:
            with suppress(FileNotFoundError):
                os.remove(partial)
            raise
        finally:
            src.close()
        # A hard link installs the database whole and, unlike a rename,
        # refuses to replace one another process made meanwhile.
        try:
            os.link(partial, path)
        except FileExistsError:
            raise FileExistsError(
                f"{destination!r} already holds a Disk store"
            ) from None
        finally:
            os.remove(partial)
        return cls(destination)


def _diskcache_rows(
    db: sqlite3.Connection, directory: str
) -> Iterator[tuple[str, bytes]]:
    """Every key in a diskcache database and its bytes, read in one
    transaction."""
    db.execute("BEGIN")
    rows = db.execute("SELECT key, raw, mode, filename, value FROM Cache")
    for key, raw, mode, filename, value in rows:
        if not raw or not isinstance(key, str):
            raise ValueError(f"diskcache key {key!r} is not plain text")
        if mode == _MODE_RAW and isinstance(value, bytes):
            yield key, value
        elif mode == _MODE_BINARY and filename:
            with open(os.path.join(directory, filename), "rb") as f:
                yield key, f.read()
        else:
            raise ValueError(f"diskcache value of {key!r} is not plain bytes")
