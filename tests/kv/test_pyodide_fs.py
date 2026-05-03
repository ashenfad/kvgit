"""Capability probes for Pyodide filesystem persistence.

These tests don't fully exercise candidate backends — they probe whether
the underlying APIs are present and round-trip data, so we can choose
between purpose-built KV stores (e.g. an OpfsKV) and reusing
``kvgit[disk]`` (diskcache + sqlite) on top of a mounted persistent FS
without flying blind.

Each probe is best-effort: a failure means "this option isn't viable
in this Pyodide / browser combo," not a regression. The probes print
their findings via stdout — run with ``-s`` to see them.

Run on Chrome:

    KVGIT_PYODIDE_TESTS=1 pytest tests/kv/test_pyodide_fs.py \\
        --runtime chrome --dist-dir ./pyodide -v -s

Or on Firefox (these probes don't need JSPI — they target the
non-JSPI persistence paths, which is the whole point):

    KVGIT_PYODIDE_TESTS=1 pytest tests/kv/test_pyodide_fs.py \\
        --runtime firefox --dist-dir ./pyodide -v -s

Implementation notes:

* ``@run_in_pyodide`` ships only the function body across the JS
  bridge — it does NOT capture module-level globals. JS snippets are
  therefore inlined inside each test function rather than factored to
  module-level constants.
* The probes use the plain ``selenium`` fixture, not ``selenium_jspi``.
  Their entire purpose is to validate paths that don't need JSPI, so
  enabling JSPI flags would muddy the test signal.
"""

import pytest

try:
    from pytest_pyodide import run_in_pyodide
except ImportError:
    pytest.skip("pytest-pyodide not installed", allow_module_level=True)


@run_in_pyodide()
async def test_probe_apis(selenium):
    """Inventory which Pyodide / browser FS APIs are present."""
    from pyodide.code import run_js

    inventory_js = """
    ({
      pyodideVersion: pyodide?.version ?? null,

      // Pyodide JS bindings
      hasFS: typeof pyodide?.FS !== 'undefined',
      hasMountNativeFS: typeof pyodide?.mountNativeFS === 'function',

      // Emscripten FS surface
      hasFSMount: typeof pyodide?.FS?.mount === 'function',
      hasFSSyncfs: typeof pyodide?.FS?.syncfs === 'function',
      hasIDBFS: typeof pyodide?.FS?.filesystems?.IDBFS !== 'undefined',
      hasMEMFS: typeof pyodide?.FS?.filesystems?.MEMFS !== 'undefined',

      // Browser-level OPFS root (no user gesture required)
      hasGetDirectory: typeof navigator?.storage?.getDirectory === 'function',

      // Sync OPFS access handles only work in workers; pytest-pyodide
      // runs Pyodide on the main thread, so this should report false
      // here even if the production environment (agex worker) reports
      // true.
      isInWorker: typeof WorkerGlobalScope !== 'undefined',

      // JSPI presence (just for completeness — current IndexedDB
      // backend depends on this being true).
      hasJSPI: typeof WebAssembly?.Suspending === 'function',
    })
    """

    findings = run_js(inventory_js).to_py()

    print("\n=== Pyodide FS capability inventory ===")
    print(f"  pyodide version: {findings.pop('pyodideVersion')}")
    for name, present in findings.items():
        mark = "YES" if present else "no "
        print(f"  {mark}  {name}")
    print()

    # The bare minimum — if pyodide.FS is missing, Pyodide didn't load.
    assert findings["hasFS"], "pyodide.FS missing — Pyodide didn't load right"


@run_in_pyodide()
async def test_probe_idbfs_roundtrip(selenium):
    """Mount IDBFS, write a file via Python, syncfs to IDB, read it back."""
    from pyodide.code import run_js

    mount_path = "/probe_idbfs"

    # Mount IDBFS at mount_path. Returns {ok: bool, error?: str}.
    setup_js = """
    (async (mountPath) => {
      const fs = pyodide.FS;
      try {
        try { fs.mkdir(mountPath); }
        catch(e) { if (!String(e).includes('EEXIST')) throw e; }
        fs.mount(fs.filesystems.IDBFS, {}, mountPath);
        return { ok: true };
      } catch(e) {
        return { ok: false, error: String(e) };
      }
    })
    """

    # Push memory -> IDB. syncfs is callback-style; wrap as Promise so
    # it can be awaited from async Python.
    syncfs_js = """
    (async () => {
      await new Promise((resolve, reject) => {
        pyodide.FS.syncfs(false, (err) => err ? reject(err) : resolve());
      });
      return { ok: true };
    })()
    """

    print("\n=== IDBFS round-trip probe ===")

    setup = (await run_js(setup_js)(mount_path)).to_py()
    if not setup["ok"]:
        print(f"  mount IDBFS at {mount_path}: FAILED — {setup['error']}")
        pytest.skip(f"IDBFS unavailable: {setup['error']}")
    print(f"  mount IDBFS at {mount_path}: ok")

    # Python file I/O against the mount — this is what diskcache does.
    file_path = f"{mount_path}/probe.txt"
    payload = b"hello from python via idbfs"
    with open(file_path, "wb") as f:
        f.write(payload)
    print("  Python open()/write(): ok")

    with open(file_path, "rb") as f:
        assert f.read() == payload
    print("  Python open()/read() (in-memory mirror): ok")

    sync_result = (await run_js(syncfs_js)).to_py()
    assert sync_result["ok"]
    print("  FS.syncfs(false) (memory -> IDB): ok")

    with open(file_path, "rb") as f:
        assert f.read() == payload
    print("  post-syncfs readback: ok")
    print()


@run_in_pyodide()
async def test_probe_opfs_native_mount(selenium):
    """Try mounting OPFS via pyodide.mountNativeFS, then write a file.

    OPFS is the modern Origin Private File System (distinct from the
    legacy IndexedDB-backed IDBFS). ``pyodide.mountNativeFS`` uses
    async file APIs — or sync access handles in workers — and persists
    automatically with no syncfs needed. Cross-browser support is
    broader than JSPI (Safari 17+, Firefox 111+).

    Note: pytest-pyodide runs Pyodide on the main thread, so even if
    the API is present here, sync access handles won't be — only the
    async path is exercised. The production target (agex worker) is a
    different story.
    """
    from pyodide.code import run_js

    mount_path = "/probe_opfs"

    mount_js = """
    (async (mountPath) => {
      if (typeof pyodide.mountNativeFS !== 'function') {
        return { ok: false, error: 'pyodide.mountNativeFS not available' };
      }
      if (typeof navigator?.storage?.getDirectory !== 'function') {
        return { ok: false, error: 'navigator.storage.getDirectory not available' };
      }
      try {
        try { pyodide.FS.mkdir(mountPath); }
        catch(e) { if (!String(e).includes('EEXIST')) throw e; }
        const root = await navigator.storage.getDirectory();
        await pyodide.mountNativeFS(mountPath, root);
        return { ok: true };
      } catch(e) {
        return { ok: false, error: `${e?.name || 'Error'}: ${e?.message || e}` };
      }
    })
    """

    print("\n=== OPFS (mountNativeFS) probe ===")

    setup = (await run_js(mount_js)(mount_path)).to_py()
    if not setup["ok"]:
        print(f"  mountNativeFS at {mount_path}: FAILED — {setup['error']}")
        pytest.skip(f"OPFS mount failed: {setup['error']}")
    print(f"  mountNativeFS at {mount_path}: ok")

    file_path = f"{mount_path}/probe.txt"
    payload = b"hello from python via opfs"
    with open(file_path, "wb") as f:
        f.write(payload)
    print("  Python open()/write(): ok")

    with open(file_path, "rb") as f:
        assert f.read() == payload
    print("  Python open()/read(): ok")
    print()


@run_in_pyodide(packages=["micropip", "sqlite3"])
async def test_probe_diskcache_on_idbfs(selenium):
    """Install diskcache, point it at an IDBFS mount, round-trip a value.

    The actual reuse hypothesis: if this works, the entire
    purpose-built-OpfsKV direction becomes unnecessary — we just ship
    instructions for mounting persistent FS under kvgit[disk].

    Pyodide doesn't ship ``sqlite3`` in its core stdlib — it's a
    separately-loadable Pyodide package. ``packages=`` triggers
    ``loadPackage`` before the test body runs.
    """
    import micropip
    from pyodide.code import run_js

    mount_path = "/probe_diskcache_idbfs"

    setup_js = """
    (async (mountPath) => {
      const fs = pyodide.FS;
      try {
        try { fs.mkdir(mountPath); }
        catch(e) { if (!String(e).includes('EEXIST')) throw e; }
        fs.mount(fs.filesystems.IDBFS, {}, mountPath);
        return { ok: true };
      } catch(e) {
        return { ok: false, error: String(e) };
      }
    })
    """

    syncfs_js = """
    (async () => {
      await new Promise((resolve, reject) => {
        pyodide.FS.syncfs(false, (err) => err ? reject(err) : resolve());
      });
      return { ok: true };
    })()
    """

    print("\n=== diskcache + IDBFS probe ===")

    setup = (await run_js(setup_js)(mount_path)).to_py()
    if not setup["ok"]:
        print(f"  mount IDBFS at {mount_path}: FAILED — {setup['error']}")
        pytest.skip(f"IDBFS unavailable: {setup['error']}")
    print(f"  mount IDBFS at {mount_path}: ok")

    try:
        await micropip.install("diskcache")
    except Exception as e:
        print(f"  micropip install diskcache: FAILED — {e}")
        pytest.skip(f"diskcache install failed: {e}")
    print("  micropip install diskcache: ok")

    try:
        from diskcache import Cache  # type: ignore[import-not-found]

        cache_dir = f"{mount_path}/cache"
        cache = Cache(cache_dir)
        cache.set("k", b"hello from diskcache on idbfs")
        assert cache.get("k") == b"hello from diskcache on idbfs"
        print("  diskcache set/get round-trip: ok")
        cache.close()
    except Exception as e:
        print(f"  diskcache round-trip: FAILED — {type(e).__name__}: {e}")
        raise

    sync_result = (await run_js(syncfs_js)).to_py()
    assert sync_result["ok"]
    print("  FS.syncfs(false) after diskcache writes: ok")
    print()
