import os

# KVGIT_TEST_BACKEND=postgres reruns the whole suite with every in-memory
# store swapped for a fresh Postgres table, so the tests
# double as a conformance run for the backend. KVGIT_POSTGRES_DSN names the
# database (default: dbname=kvgit_test); it is dropped and recreated.
if os.environ.get("KVGIT_TEST_BACKEND") == "postgres":
    import itertools

    import psycopg
    from psycopg_pool import ConnectionPool

    import kvgit._open
    import kvgit.kv
    import kvgit.kv.memory
    from kvgit.kv.postgres import Postgres

    _dsn = os.environ.get("KVGIT_POSTGRES_DSN", "dbname=kvgit_test")
    _dbname = psycopg.conninfo.conninfo_to_dict(_dsn)["dbname"]
    _admin = psycopg.conninfo.make_conninfo(_dsn, dbname="postgres")
    with psycopg.connect(_admin, autocommit=True) as _c:
        _c.execute(f'DROP DATABASE IF EXISTS "{_dbname}" WITH (FORCE)')
        _c.execute(f'CREATE DATABASE "{_dbname}"')
    _pool = ConnectionPool(
        _dsn, min_size=2, max_size=32, kwargs={"autocommit": True}, open=True
    )
    _ids = itertools.count()

    class _PostgresAsMemory(Postgres):
        def __init__(self) -> None:
            super().__init__(pool=_pool, table=f"t{next(_ids)}")

    for _mod in (kvgit.kv, kvgit.kv.memory, kvgit._open):
        _mod.Memory = _PostgresAsMemory

    def pytest_unconfigure(config):
        _pool.close()
