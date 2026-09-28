import glob
import os

collect_ignore_glob = []

# Pyodide-hosted tests require: --runtime chrome --dist-dir ./pyodide
# Skip them during normal `uv run pytest` runs.
if os.environ.get("KVGIT_PYODIDE_TESTS") != "1":
    collect_ignore_glob.append("**/test_indexeddb.py")
    collect_ignore_glob.append("**/test_pyodide_fs.py")


def pytest_configure(config):
    """Write the kvgit wheel filename to dist-dir so pyodide tests can find it."""
    dist_dir = getattr(config.option, "dist_dir", None)
    if dist_dir and os.path.isdir(dist_dir):
        wheels = glob.glob(os.path.join(dist_dir, "kvgit-*.whl"))
        if wheels:
            whl_name = os.path.basename(wheels[0])
            with open(os.path.join(dist_dir, "_kvgit_whl.txt"), "w") as f:
                f.write(whl_name)


# KVGIT_TEST_BACKEND=postgres reruns the whole suite with every in-memory
# store swapped for a fresh Postgres table, so the versioned layer's tests
# double as a conformance run for the backend. KVGIT_POSTGRES_DSN names the
# database (default: dbname=kvgit_test); it is dropped and recreated.
if os.environ.get("KVGIT_TEST_BACKEND") == "postgres":
    import itertools
    import sys

    import psycopg
    from psycopg_pool import ConnectionPool

    import kvgit.kv
    import kvgit.kv.memory
    import kvgit.versioned.kv
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

    # ``kvgit.store`` names the factory function, which shadows its module.
    _store_module = sys.modules["kvgit.store"]
    for _mod in (kvgit.kv, kvgit.kv.memory, _store_module, kvgit.versioned.kv):
        _mod.Memory = _PostgresAsMemory

    def pytest_unconfigure(config):
        _pool.close()
