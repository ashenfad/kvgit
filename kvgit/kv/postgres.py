"""PostgreSQL KV store.

Requires ``pip install kvgit[postgres]`` (psycopg 3 and its pool).
"""

import re
from collections.abc import Iterable, Mapping

from .base import KVStore

try:
    from psycopg import errors, sql
    from psycopg.pq import TransactionStatus
    from psycopg_pool import ConnectionPool
except ImportError as e:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "kvgit's Postgres backend needs psycopg; install kvgit[postgres]"
    ) from e

_TABLE_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_DEADLOCK_RETRIES = 5
_MAX_CODE_POINT = 0x10FFFF


def _prefix_upper_bound(prefix: str) -> str | None:
    """The least string greater than every string starting with ``prefix``.

    Under the C collation Postgres compares UTF-8 bytes, which order the
    same as code points, so it is ``prefix`` with its last character
    bumped by one — carried leftwards past characters already at
    U+10FFFF, and stepped over the surrogates, which UTF-8 cannot hold.
    None when every character is U+10FFFF: nothing is greater, and the
    range is open above.
    """
    stem = prefix.rstrip(chr(_MAX_CODE_POINT))
    if not stem:
        return None
    bumped = ord(stem[-1]) + 1
    if 0xD800 <= bumped <= 0xDFFF:
        bumped = 0xE000
    return stem[:-1] + chr(bumped)


class Postgres(KVStore):
    """A KV store in one PostgreSQL table.

    Keys live in a ``text COLLATE "C"`` primary key, so ``keys(prefix)``
    is an index range scan; values are ``bytea``. Every method but
    ``cas_many`` is a single statement on an autocommit connection — one
    round trip, atomic on its own — and ``set_many`` writes in key order,
    so two batches upserting overlapping keys lock their rows in the same
    order and cannot deadlock.

    ``cas_many`` is one transaction that locks every expected key with
    Postgres's own row locking, so that no write to it — by any method —
    can land between the check and the batch: a key expected to hold a
    value is read ``FOR UPDATE``, and a key expected absent gets a
    placeholder row that a concurrent insert must wait on. Then the
    writes, and the removal of placeholders the batch does not write. The
    statements are pipelined, so a batch costs two round trips. A batch
    Postgres aborts to break a deadlock is retried; one that fails
    otherwise is rolled back.

    Several stores may share a database under different table names.

    Args:
        conninfo: libpq connection string (``"dbname=kvgit"``, a URL).
            Ignored when ``pool`` is given.
        table: Table holding this store: lowercase letters, digits and
            underscores.
        pool: An existing ``psycopg_pool.ConnectionPool`` to draw from.
            Its connections should be in autocommit mode; otherwise each
            call pays for an implicit ``BEGIN`` and ``COMMIT``.
        create: Create the table if it does not exist.
        max_size: Pool size when this store opens its own pool.
    """

    def __init__(
        self,
        conninfo: str = "",
        *,
        table: str = "kvgit",
        pool: "ConnectionPool | None" = None,
        create: bool = True,
        max_size: int = 8,
    ) -> None:
        if not _TABLE_NAME.match(table):
            raise ValueError(f"invalid table name: {table!r}")
        self._owns_pool = pool is None
        self._pool = pool or ConnectionPool(
            conninfo,
            min_size=1,
            max_size=max_size,
            kwargs={"autocommit": True},
            open=True,
        )
        self._table = table
        self._t = sql.Identifier(table)
        if create:
            self._exec(
                'CREATE TABLE IF NOT EXISTS {t} (k text COLLATE "C" PRIMARY KEY, '
                "v bytea NOT NULL)"
            )

    # ---- plumbing ----

    def _q(self, text: str) -> sql.Composed:
        return sql.SQL(text).format(t=self._t)

    def _exec(self, text: str, params: tuple = ()) -> None:
        with self._pool.connection() as conn:
            conn.execute(self._q(text), params)

    def _fetch(self, text: str, params: tuple = ()) -> list[tuple]:
        with self._pool.connection() as conn:
            return conn.execute(self._q(text), params).fetchall()

    _UPSERT = (
        "INSERT INTO {t} (k, v) SELECT * FROM unnest(%s::text[], %s::bytea[]) "
        "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
    )

    @staticmethod
    def _sorted_columns(items: Mapping[str, bytes]) -> tuple[list[str], list[bytes]]:
        keys = sorted(items)
        return keys, [items[k] for k in keys]

    # ---- KVStore ----

    def get(self, key: str) -> bytes | None:
        rows = self._fetch("SELECT v FROM {t} WHERE k = %s", (key,))
        return bytes(rows[0][0]) if rows else None

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        self._exec(
            "INSERT INTO {t} (k, v) VALUES (%s, %s) "
            "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            (key, value),
        )

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = list(self._normalize_keys(args))
        if not keys:
            return {}
        rows = self._fetch("SELECT k, v FROM {t} WHERE k = ANY(%s)", (keys,))
        return {k: bytes(v) for k, v in rows}

    def set_many(
        self,
        items: Mapping[str, bytes] | None = None,
        /,
        **kwargs: bytes,
    ) -> None:
        items = self._normalize_items(items, kwargs)
        self._check_batch(items, ())
        if items:
            columns = self._sorted_columns(items)
            self._retrying(lambda: self._exec(self._UPSERT, columns))

    def items(self) -> Iterable[tuple[str, bytes]]:
        return [(k, bytes(v)) for k, v in self._fetch("SELECT k, v FROM {t}")]

    def keys(self, prefix: str = "") -> Iterable[str]:
        """Every key, or only those starting with ``prefix``.

        The prefix form is a range scan on the primary key, from the
        prefix up to :func:`_prefix_upper_bound`.
        """
        if not prefix:
            return [r[0] for r in self._fetch("SELECT k FROM {t}")]
        upper = _prefix_upper_bound(prefix)
        if upper is None:
            rows = self._fetch("SELECT k FROM {t} WHERE k >= %s", (prefix,))
        else:
            rows = self._fetch(
                "SELECT k FROM {t} WHERE k >= %s AND k < %s", (prefix, upper)
            )
        return [r[0] for r in rows]

    def __contains__(self, key: str) -> bool:
        return bool(self._fetch("SELECT 1 FROM {t} WHERE k = %s", (key,)))

    def remove(self, key: str) -> None:
        self._exec("DELETE FROM {t} WHERE k = %s", (key,))

    def remove_many(self, *args) -> None:
        keys = sorted(set(self._normalize_keys(args)))
        if keys:
            self._retrying(
                lambda: self._exec("DELETE FROM {t} WHERE k = ANY(%s)", (keys,))
            )

    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        removes = self._check_batch(writes, removes)
        return self._retrying(lambda: self._cas_many_once(expected, writes, removes))

    def _cas_many_once(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: list[str],
    ) -> bool:
        with self._pool.connection() as conn:
            try:
                # Explicit BEGIN/COMMIT rather than ``conn.transaction()``,
                # which syncs the pipeline at every step: this way the
                # claim and the check travel in one round trip, and the
                # writes and the commit in a second.
                with conn.pipeline() as pipeline:
                    conn.execute("BEGIN")
                    claimed = self._claim_and_check(conn, pipeline, expected)
                    if claimed is None:
                        conn.execute("ROLLBACK")
                        return False
                    if writes:
                        conn.execute(
                            self._q(self._UPSERT), self._sorted_columns(writes)
                        )
                    # A placeholder claimed for a key expected absent goes
                    # again unless this batch writes that key.
                    removals = sorted(
                        {*removes, *(k for k in claimed if k not in writes)}
                    )
                    if removals:
                        conn.execute(
                            self._q("DELETE FROM {t} WHERE k = ANY(%s)"), (removals,)
                        )
                    conn.execute("COMMIT")
                return True
            except BaseException:
                # A failed statement leaves the transaction aborted; end it
                # before the connection goes back to the pool.
                if conn.info.transaction_status != TransactionStatus.IDLE:
                    conn.execute("ROLLBACK")
                raise

    def _claim_and_check(
        self, conn, pipeline, expected: Mapping[str, bytes | None]
    ) -> list[str] | None:
        """Lock every expected key against every other writer, then check.

        A key expected to hold a value is read ``FOR UPDATE``: any other
        write or delete of that row waits for this transaction. A key
        expected absent has a placeholder row inserted for it: a
        concurrent insert of the same key waits on the unique index, and
        if the row already exists nothing is inserted and the check
        fails. So no change to an expected key can land between the check
        and the write, whichever method makes it.

        Returns the keys given placeholders, or None if an expectation
        does not hold.
        """
        absent = sorted(k for k, v in expected.items() if v is None)
        present = sorted(k for k, v in expected.items() if v is not None)
        claim = lock = None
        if absent:
            claim = conn.execute(
                self._q(
                    "INSERT INTO {t} (k, v) SELECT x, ''::bytea "
                    "FROM unnest(%s::text[]) AS x ON CONFLICT (k) DO NOTHING RETURNING k"
                ),
                (absent,),
            )
        if present:
            lock = conn.execute(
                self._q("SELECT k, v FROM {t} WHERE k = ANY(%s) ORDER BY k FOR UPDATE"),
                (present,),
            )
        pipeline.sync()
        claimed = [row[0] for row in claim.fetchall()] if claim else []
        if len(claimed) != len(absent):
            return None
        current = {k: bytes(v) for k, v in lock.fetchall()} if lock else {}
        if any(current.get(k) != expected[k] for k in present):
            return None
        return claimed

    def _retrying(self, attempt):
        """Run a write, retrying one Postgres chose as a deadlock victim.

        Batches lock their rows in key order, but a batch's claimed and
        locked keys are two ordered runs, so two batches over overlapping
        keys can still meet in opposite orders. Postgres breaks such a
        cycle by aborting one side, which then changed nothing; running
        it again is safe.
        """
        for remaining in range(_DEADLOCK_RETRIES, 0, -1):
            try:
                return attempt()
            except errors.DeadlockDetected:
                if remaining == 1:
                    raise
        raise AssertionError("unreachable")

    def clear(self) -> None:
        self._exec("DELETE FROM {t}")

    # ---- lifecycle ----

    def drop(self) -> None:
        """Drop this store's table."""
        self._exec("DROP TABLE IF EXISTS {t}")

    def close(self) -> None:
        """Close the connection pool, if this store opened it."""
        if self._owns_pool:
            self._pool.close()
