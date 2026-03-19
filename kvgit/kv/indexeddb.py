"""IndexedDB-backed KV store for Pyodide/browser environments.

Provides persistent key-value storage in the browser using IndexedDB,
presenting the same synchronous ``KVStore`` interface as ``Memory`` and
``Disk``.

Platform requirements
---------------------
This module is **Pyodide-only**. It will fail to import outside of a
Pyodide runtime (the import is guarded in ``kvgit.kv.__init__``).

Within Pyodide, two conditions must hold:

1. **JSPI enabled** — The browser must support JavaScript Promise
   Integration. JSPI allows ``run_sync()`` to block on JS promises
   without blocking the browser event loop.

2. **Async entry point** — The Python call stack must have been entered
   via ``pyodide.runPythonAsync()``, an async Python function, or
   ``callPromising()``.  This is always the case when using agex's
   async agent loop, PyScript, or JupyterLite.  Only
   ``pyodide.runPython()`` (the synchronous entry) would violate this,
   and that entry point cannot use any browser async API anyway.

These are platform constraints (like ``Disk`` requiring a writable
filesystem), not runtime surprises — if you can import this module
you are already in an environment where both conditions hold.

Concurrency
-----------
IndexedDB is shared across Web Workers within the same origin.
Write operations (``set``, ``set_many``, ``remove``, ``cas``, ``clear``)
use ``readwrite`` transactions, which IndexedDB serializes against other
``readwrite`` transactions on the same object store.  ``cas`` performs
its read and write within a single ``readwrite`` transaction, making it
safe across workers.

Implementation note
-------------------
All IDB operations (transaction creation, requests, and completion
waiting) must happen within a single async function passed to
``run_sync()``.  Creating an IDB request *outside* ``run_sync`` and
attaching handlers *inside* causes a race: the JSPI suspension between
the two gives the event loop a chance to complete the request before
the handler is set, causing a deadlock.
"""

from typing import Iterable, Mapping

from pyodide.ffi import create_proxy, run_sync, to_js  # type: ignore[import-not-found]
from js import Promise, indexedDB, undefined  # type: ignore[import-not-found]

from .base import KVStore


def _promise(executor):
    """Create a JS Promise, destroying the executor proxy after use."""
    proxy = create_proxy(executor)
    try:
        return Promise.new(proxy)
    finally:
        proxy.destroy()


async def _idb_open(db_name: str, store_name: str):
    """Open (or create) an IndexedDB database, returning the IDBDatabase."""

    def _executor(resolve, reject):
        request = indexedDB.open(db_name, 1)

        def on_upgrade(event):
            db = event.target.result
            if not db.objectStoreNames.contains(store_name):
                db.createObjectStore(store_name)

        def on_success(event):
            resolve(event.target.result)

        def on_error(event):
            reject(event.target.error)

        request.onupgradeneeded = on_upgrade
        request.onsuccess = on_success
        request.onerror = on_error

    return await _promise(_executor)


def _idb_request_promise(request):
    """Create a JS Promise for an IDBRequest (synchronous — no await).

    Attaches onsuccess/onerror immediately so the handler is set before
    any JSPI suspension can let the event loop complete the request.
    """

    def _executor(resolve, reject):
        request.onsuccess = lambda e: resolve(e.target.result)
        request.onerror = lambda e: reject(e.target.error)

    return _promise(_executor)


async def _idb_request(request):
    """Await an IDBRequest and return its result."""
    return await _idb_request_promise(request)


async def _idb_tx_complete(tx):
    """Wait for an IDBTransaction to complete."""

    def _executor(resolve, reject):
        tx.oncomplete = lambda e: resolve(None)
        tx.onerror = lambda e: reject(e.target.error)
        tx.onabort = lambda e: reject(e.target.error)

    await _promise(_executor)


def _to_bytes(js_value) -> bytes | None:
    """Convert a JS result to bytes, or None if absent.

    Uses Uint8Array.to_py() for fast memcpy from JS to WASM memory,
    avoiding the slow element-wise iteration of bytes(js_proxy).
    """
    if js_value is None or js_value is undefined:
        return None
    from js import Uint8Array  # type: ignore[import-not-found]

    arr = Uint8Array.new(js_value)
    return arr.to_py().tobytes()


class IndexedDB(KVStore):
    """KV store backed by browser IndexedDB.

    Presents the same synchronous interface as ``Memory`` and ``Disk``.
    Internally uses ``pyodide.ffi.run_sync`` to block on IndexedDB
    promises.  See module docstring for platform requirements and
    concurrency guarantees.

    Args:
        db_name: IndexedDB database name.  Each name is an independent
            store, persisted across page reloads.
        store_name: Object store name within the database.
    """

    def __init__(self, db_name: str = "kvgit", store_name: str = "kv") -> None:
        self._db_name = db_name
        self._store_name = store_name
        self._db = run_sync(_idb_open(db_name, store_name))

    def _object_store(self, mode: str = "readonly"):
        tx = self._db.transaction(self._store_name, mode)
        return tx.objectStore(self._store_name), tx

    def get(self, key: str) -> bytes | None:
        async def _op():
            store, _tx = self._object_store("readonly")
            return _to_bytes(await _idb_request(store.get(key)))

        return run_sync(_op())

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")

        async def _op():
            store, tx = self._object_store("readwrite")
            store.put(to_js(value), key)
            await _idb_tx_complete(tx)

        run_sync(_op())

    def get_many(self, *args: str) -> Mapping[str, bytes]:
        async def _op():
            store, _tx = self._object_store("readonly")
            # Attach all handlers synchronously before any await to prevent
            # the transaction from auto-committing between suspensions.
            promises = []
            for key in args:
                req = store.get(key)
                promises.append((key, _idb_request_promise(req)))
            result = {}
            for key, p in promises:
                val = _to_bytes(await p)
                if val is not None:
                    result[key] = val
            return result

        return run_sync(_op())

    def set_many(self, **kwargs: bytes) -> None:
        for key, value in kwargs.items():
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for {key}, got {type(value).__name__}")

        async def _op():
            store, tx = self._object_store("readwrite")
            for key, value in kwargs.items():
                store.put(to_js(value), key)
            await _idb_tx_complete(tx)

        run_sync(_op())

    def items(self) -> Iterable[tuple[str, bytes]]:
        results: list[tuple[str, bytes]] = []

        async def _op():
            store, _tx = self._object_store("readonly")
            cursor_request = store.openCursor()

            def _executor(resolve, reject):
                def on_success(event):
                    cursor = event.target.result
                    if cursor:
                        results.append((str(cursor.key), bytes(cursor.value)))
                        cursor.continue_()
                    else:
                        resolve(None)

                def on_error(event):
                    reject(event.target.error)

                cursor_request.onsuccess = on_success
                cursor_request.onerror = on_error

            await _promise(_executor)

        run_sync(_op())
        return results

    def keys(self) -> Iterable[str]:
        async def _op():
            store, _tx = self._object_store("readonly")
            return await _idb_request(store.getAllKeys())

        result = run_sync(_op())
        return [str(k) for k in result]

    def __contains__(self, key: str) -> bool:
        async def _op():
            store, _tx = self._object_store("readonly")
            return await _idb_request(store.count(key))

        result = run_sync(_op())
        return int(result) > 0

    def remove(self, key: str) -> None:
        async def _op():
            store, tx = self._object_store("readwrite")
            store.delete(key)
            await _idb_tx_complete(tx)

        run_sync(_op())

    def remove_many(self, *keys: str) -> None:
        async def _op():
            store, tx = self._object_store("readwrite")
            for key in keys:
                store.delete(key)
            await _idb_tx_complete(tx)

        run_sync(_op())

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        """Atomic compare-and-swap.

        Read and write happen in a single ``readwrite`` transaction.
        IndexedDB serializes ``readwrite`` transactions on the same
        object store, so this is safe across Web Workers sharing the
        same database.
        """
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")

        cas_result = [False]

        async def _op():
            store, tx = self._object_store("readwrite")
            read_req = store.get(key)

            # Do the read and conditional write entirely within callbacks
            # to keep the transaction alive (no await between read and write).
            def _executor(resolve, reject):
                def on_read_success(event):
                    current = _to_bytes(event.target.result)
                    if current != expected:
                        return  # tx will auto-commit empty
                    store.put(to_js(value), key)
                    cas_result[0] = True

                read_req.onsuccess = on_read_success
                read_req.onerror = lambda e: reject(e.target.error)
                tx.oncomplete = lambda e: resolve(None)
                tx.onerror = lambda e: reject(e.target.error)
                tx.onabort = lambda e: reject(e.target.error)

            await _promise(_executor)

        run_sync(_op())
        return cas_result[0]

    def clear(self) -> None:
        async def _op():
            store, tx = self._object_store("readwrite")
            store.clear()
            await _idb_tx_complete(tx)

        run_sync(_op())
