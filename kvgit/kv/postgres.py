"""PostgreSQL KV store.

Requires ``pip install kvgit[postgres]`` (psycopg 3 and its pool).
"""

import re
from collections.abc import Iterable, Mapping

from .base import KVStore

try:
    from psycopg import sql
    from psycopg.pq import TransactionStatus
    from psycopg_pool import ConnectionPool
except ImportError as e:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "kvgit's Postgres backend needs psycopg; install kvgit[postgres]"
    ) from e

_TABLE_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class Postgres(KVStore):
    """A KV store in one PostgreSQL table.

    Keys live in a ``text COLLATE "C"`` primary key, so ``keys(prefix)``
    is an index range scan; values are ``bytea``. Every method but
    ``cas_many`` is a single statement on an autocommit connection — one
    round trip, atomic on its own — and ``set_many`` writes in key order,
    so two batches upserting overlapping keys lock their rows in the same
    order and cannot deadlock.

    ``cas_many`` is one transaction: a transaction-scoped advisory lock
    per expected key, taken in a fixed order, then the check, then the
    writes. The locks serialize every batch that expects the same key —
    one expecting it absent included, which no row lock could cover — and
    under READ COMMITTED the check, a statement of its own after the
    locks, reads the latest committed values. The statements are
    pipelined, so a batch costs two round trips.

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
            self._exec(self._UPSERT, self._sorted_columns(items))

    def items(self) -> Iterable[tuple[str, bytes]]:
        return [(k, bytes(v)) for k, v in self._fetch("SELECT k, v FROM {t}")]

    def keys(self, prefix: str = "") -> Iterable[str]:
        """Every key, or only those starting with ``prefix``.

        The prefix form is a range scan on the primary key: under the C
        collation keys sort by code point, so the keys starting with
        ``p`` are exactly those in ``[p, p')`` where ``p'`` bumps the
        last character of ``p``.
        """
        if not prefix:
            return [r[0] for r in self._fetch("SELECT k FROM {t}")]
        upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        rows = self._fetch(
            "SELECT k FROM {t} WHERE k >= %s AND k < %s", (prefix, upper)
        )
        return [r[0] for r in rows]

    def __contains__(self, key: str) -> bool:
        return bool(self._fetch("SELECT 1 FROM {t} WHERE k = %s", (key,)))

    def remove(self, key: str) -> None:
        self._exec("DELETE FROM {t} WHERE k = %s", (key,))

    def remove_many(self, *args) -> None:
        keys = list(self._normalize_keys(args))
        if keys:
            self._exec("DELETE FROM {t} WHERE k = ANY(%s)", (keys,))

    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        removes = self._check_batch(writes, removes)
        # The lock key names the table as well as the key, so stores
        # sharing a database do not serialize each other.
        lock_keys = [f"{self._table}|{key}" for key in expected]
        with self._pool.connection() as conn:
            try:
                # Explicit BEGIN/COMMIT rather than ``conn.transaction()``,
                # which syncs the pipeline at every step: this way the
                # locks and the check travel in one round trip, and the
                # writes and the commit in a second.
                with conn.pipeline() as pipeline:
                    conn.execute("BEGIN")
                    if expected:
                        # One statement takes every lock, in hash order, so
                        # two batches expecting overlapping keys cannot
                        # deadlock.
                        conn.execute(
                            "SELECT pg_advisory_xact_lock(h) FROM ("
                            "SELECT hashtextextended(x, 0) AS h "
                            "FROM unnest(%s::text[]) AS x ORDER BY h) AS locks",
                            (lock_keys,),
                        )
                        check = conn.execute(
                            self._q("SELECT k, v FROM {t} WHERE k = ANY(%s)"),
                            (list(expected),),
                        )
                        pipeline.sync()
                        current = {k: bytes(v) for k, v in check.fetchall()}
                        if any(current.get(k) != v for k, v in expected.items()):
                            conn.execute("ROLLBACK")
                            return False
                    if writes:
                        conn.execute(
                            self._q(self._UPSERT), self._sorted_columns(writes)
                        )
                    if removes:
                        conn.execute(
                            self._q("DELETE FROM {t} WHERE k = ANY(%s)"), (removes,)
                        )
                    conn.execute("COMMIT")
                return True
            except BaseException:
                # A failed statement leaves the transaction aborted; end it
                # before the connection goes back to the pool.
                if conn.info.transaction_status != TransactionStatus.IDLE:
                    conn.execute("ROLLBACK")
                raise

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
