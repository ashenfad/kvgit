"""Tests for the IndexedDB KV store.

These tests run inside a real browser via pytest-pyodide and are excluded
from the default ``uv run pytest`` run. To execute them:

1. Download the Pyodide distribution (once)::

    mkdir -p pyodide
    curl -L https://github.com/pyodide/pyodide/releases/download/0.27.7/pyodide-0.27.7.tar.bz2 \
        | tar -xj -C pyodide --strip-components=1

2. Build the wheel and copy it into the Pyodide directory::

    uv build
    cp dist/kvgit-*.whl pyodide/

3. Run the tests::

    KVGIT_PYODIDE_TESTS=1 uv run pytest tests/kv/test_indexeddb.py \
        --runtime chrome --dist-dir ./pyodide -x -v

Requires:
    - pytest-pyodide (``uv pip install pytest-pyodide``)
    - Chrome with JSPI support
    - A matching chromedriver on PATH (Selenium will auto-download one if needed)
"""

import pytest

try:
    from pytest_pyodide import run_in_pyodide
except ImportError:
    pytest.skip("pytest-pyodide not installed", allow_module_level=True)


@run_in_pyodide(packages=["micropip"])
async def test_set_get(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_set_get")
    store.set("k", b"v")
    assert store.get("k") == b"v"


@run_in_pyodide(packages=["micropip"])
async def test_get_missing(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_get_missing")
    assert store.get("nope") is None


@run_in_pyodide(packages=["micropip"])
async def test_contains(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_contains")
    store.set("k", b"v")
    assert "k" in store
    assert "nope" not in store


@run_in_pyodide(packages=["micropip"])
async def test_keys(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_keys")
    store.set("a", b"1")
    store.set("b", b"2")
    assert set(store.keys()) == {"a", "b"}


@run_in_pyodide(packages=["micropip"])
async def test_items(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_items")
    store.set("a", b"1")
    store.set("b", b"2")
    assert dict(store.items()) == {"a": b"1", "b": b"2"}


@run_in_pyodide(packages=["micropip"])
async def test_set_many_get_many(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_set_many_get_many")
    store.set_many(a=b"1", b=b"2", c=b"3")
    result = store.get_many("a", "c", "missing")
    assert result == {"a": b"1", "c": b"3"}


@run_in_pyodide(packages=["micropip"])
async def test_overwrite(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_overwrite")
    store.set("k", b"old")
    store.set("k", b"new")
    assert store.get("k") == b"new"


@run_in_pyodide(packages=["micropip"])
async def test_remove(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_remove")
    store.set("k", b"v")
    store.remove("k")
    assert store.get("k") is None


@run_in_pyodide(packages=["micropip"])
async def test_remove_missing(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_remove_missing")
    store.remove("nope")  # should not raise


@run_in_pyodide(packages=["micropip"])
async def test_remove_many(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_remove_many")
    store.set_many(a=b"1", b=b"2", c=b"3")
    store.remove_many("a", "c", "missing")
    assert store.get("a") is None
    assert store.get("b") == b"2"
    assert store.get("c") is None


@run_in_pyodide(packages=["micropip"])
async def test_cas_success(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_cas_success")
    store.set("k", b"old")
    assert store.cas("k", b"new", expected=b"old")
    assert store.get("k") == b"new"


@run_in_pyodide(packages=["micropip"])
async def test_cas_failure(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_cas_failure")
    store.set("k", b"old")
    assert not store.cas("k", b"new", expected=b"wrong")
    assert store.get("k") == b"old"


@run_in_pyodide(packages=["micropip"])
async def test_cas_create(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_cas_create")
    assert store.cas("k", b"val", expected=None)
    assert store.get("k") == b"val"


@run_in_pyodide(packages=["micropip"])
async def test_cas_create_fails_if_exists(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_cas_create_fails")
    store.set("k", b"existing")
    assert not store.cas("k", b"new", expected=None)
    assert store.get("k") == b"existing"


@run_in_pyodide(packages=["micropip"])
async def test_clear(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_clear")
    store.set_many(a=b"1", b=b"2")
    store.clear()
    assert store.get("a") is None
    assert list(store.keys()) == []


@run_in_pyodide(packages=["micropip"])
async def test_type_error_on_non_bytes(selenium_jspi):
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB

    store = IndexedDB(db_name="test_type_error")
    try:
        store.set("k", "not bytes")
        assert False, "should have raised"
    except TypeError:
        pass


@run_in_pyodide(packages=["micropip"])
async def test_versioned_integration(selenium_jspi):
    """Full integration: Staged -> VersionedKV -> IndexedDB."""
    import micropip
    from pyodide.http import pyfetch

    resp = await pyfetch("./_kvgit_whl.txt")
    whl = (await resp.string()).strip()
    await micropip.install(f"./{whl}", deps=False)

    from kvgit.kv.indexeddb import IndexedDB
    from kvgit.staged import Staged
    from kvgit.versioned.kv import VersionedKV

    backend = IndexedDB(db_name="test_versioned")
    versioned = VersionedKV(backend)
    staged = Staged(versioned)

    staged["greeting"] = "hello"
    staged.commit()

    staged["greeting"] = "updated"
    staged.commit()

    # Verify current value
    assert staged["greeting"] == "updated"

    # Undo: reset to first commit
    history = list(versioned.history())
    versioned.reset_to(history[1])
    staged.refresh()
    assert staged["greeting"] == "hello"
