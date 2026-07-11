# Browser persistence (Pyodide)

Running kvgit inside [Pyodide](https://pyodide.org/) — for example, an in-browser agent or notebook — has two viable paths to a persistent store, with materially different tradeoffs. This document covers both, when to use each, and the gotchas you'll hit if you don't pick the right one for your situation.

## TL;DR

| | `kind="indexeddb"` | `kind="disk"` over OPFS |
|---|---|---|
| Browsers | Chromium 137+ only (needs JSPI) | Chrome 102+, Safari 17+, Firefox 111+ |
| Multi-tab safe | Yes (built-in) | No (OPFS handles are exclusive) |
| Per-op durability | Yes | No — needs explicit flush |
| Host-side setup | None | Mount + flush logic in worker.js |
| Extra Python deps | None | `sqlite3` (Pyodide pkg) + `diskcache` |
| Cold load | Lazy / fast | Fast |
| Sustained write throughput | Slow (JSPI per call) | Fast (batched at FS layer) |

**Rule of thumb:** prefer `kind="disk"` over OPFS unless you specifically need multi-tab concurrent writes to the same store.

## Why this is harder than it looks

Pyodide bridges synchronous Python to a fundamentally async browser environment. kvgit's `Staged.commit()` is synchronous, but every browser persistence API is asynchronous under the hood. Closing that gap is the recurring source of complexity in everything below.

There are two ways to close it:

1. **JSPI (JavaScript Promise Integration):** lets a synchronous Python frame suspend on a JS promise. Available in Chromium 137+ (June 2025). Firefox has it behind a flag; Safari has no implementation as of late 2025.
2. **Mount-and-flush:** mount a synchronous-looking filesystem inside Pyodide (in-memory mirror of OPFS), let kvgit do its synchronous file I/O against it, then push the mirror out to OPFS asynchronously from the JS host.

Option 1 is cleaner but Chromium-only. Option 2 is portable but requires explicit flush coordination.

## Approach 1: `kind="indexeddb"` (Chromium-only, multi-tab safe)

The built-in `IndexedDB` backend talks to IndexedDB's async API directly and bridges it to a synchronous Python interface using JSPI. Per-operation durability comes for free — every write commits a real IndexedDB transaction before the function returns.

```python
import kvgit
s = kvgit.store(kind="indexeddb", db_name="myapp")
s["key"] = "value"
s.commit()  # durable on return — no host-side flush needed
```

**Use this when:**

- You can constrain users to Chromium-based browsers (Chrome, Edge, Brave, Arc, etc.)
- Multiple tabs may concurrently write to the same store. IndexedDB transactions plus kvgit's compare-and-swap commit layer let two tabs write to the same store without corrupting each other — neither of the alternatives below offers this.
- You want the simplest Python-side story: zero host-side bootstrap, zero flushing.

**Don't use this when:**

- You need Safari or Firefox support without telling users to flip flags.
- Your workload involves many small writes per kvgit commit (chunked codecs, etc.). Each write pays a JSPI stack-switch + IndexedDB transaction round-trip; Approach 2 batches at the FS layer and is materially faster.

## Approach 2: `kind="disk"` over OPFS via `mountNativeFS`

Pyodide can mount the browser's [Origin Private File System (OPFS)](https://web.dev/articles/origin-private-file-system) as a regular filesystem path inside its virtual FS. Once mounted, kvgit's existing `Disk` backend works against it unchanged — the underlying SQLite via diskcache just does ordinary file I/O against the mount.

This is the cross-browser path. OPFS is available in Chrome 102+, Safari 17+, and Firefox 111+ — no JSPI dependency.

### Bootstrap (host side, e.g. worker.js)

```javascript
const pyodide = await loadPyodide();

// sqlite3 isn't in Pyodide's core stdlib — load as a Pyodide package.
// Required by diskcache, which backs kvgit's Disk store.
await pyodide.loadPackage(["sqlite3"]);

// diskcache itself is pure-Python; install via micropip if your
// transitive deps don't already pull it.
await pyodide.runPythonAsync(`
    import micropip
    await micropip.install("diskcache")
`);

// Mount OPFS at /persist. GB-scale quota, broad browser support.
const opfsRoot = await navigator.storage.getDirectory();
const persistDir = await opfsRoot.getDirectoryHandle("kvgit", { create: true });
await pyodide.mountNativeFS("/persist", persistDir);
```

### Python side

```python
import kvgit
s = kvgit.store(kind="disk", path="/persist/mydb")
s["key"] = "value"
s.commit()  # writes hit the in-memory mirror; see flush requirement below
```

### The flush requirement (this is the important part)

`mountNativeFS` buffers writes in an in-memory mirror of the OPFS directory and only pushes them to OPFS when you explicitly flush via `pyodide.FS.syncfs(false, callback)`. Without that call, every kvgit commit lives in RAM and is lost on tab close or reload — the OPFS directory stays empty.

One trap: `FS.syncfs` is Emscripten's **callback-style** API — it does not return a Promise, so `await pyodide.FS.syncfs(false)` compiles, runs, and awaits `undefined` while the flush proceeds unobserved (and its errors vanish). Wrap it once:

```javascript
const syncfs = () => new Promise((resolve, reject) => {
    pyodide.FS.syncfs(false, (err) => err ? reject(err) : resolve());
});
```

This isn't a kvgit limitation: `Staged.commit()` is synchronous, `syncfs` is asynchronous, and there's no way to truly await durability from synchronous Python without JSPI (the very thing this approach exists to avoid). The flush has to live in the JS host, where async actually works.

A robust pattern combines two flush triggers:

```javascript
// 1. Per-call flush at clean run() boundaries — guaranteed sync
//    after every Python turn that completes normally.
async function runPython(code) {
    try {
        const result = await pyodide.runPythonAsync(code);
        await syncfs();
        return result;
    } catch (e) {
        await syncfs();  // flush partial commits too
        throw e;
    }
}

// 2. Periodic flush on a timer — protects against mid-call reload
//    or crash. Why this works during a long runPythonAsync: the JS
//    event loop processes setInterval callbacks at every await
//    boundary, including the awaits inside Pyodide's asyncio loop
//    (LLM calls, sleeps, tool round-trips). So this fires *during*
//    long agent runs, not just between them.
setInterval(async () => {
    try {
        await syncfs();
    } catch (e) {
        console.error("[worker] syncfs failed:", e);
    }
}, 1000);
```

The two compose: per-call flush handles clean turn completion; periodic flush bounds mid-call data loss to ~1 interval on a surprise reload. Pick the interval based on how much loss you can tolerate vs. how much overhead you want when idle (1s on a clean mirror is cheap; tighter intervals are fine).

### Use this when

- You need to support Safari or Firefox (or any non-Chromium browser).
- Each session is one tab — the typical agent / notebook shape.
- You can stomach the host-side bootstrap and flush logic.

### Don't use this when

- Multiple tabs may write to the same store. OPFS sync access handles are exclusive: opening the same store from a second tab will conflict (mount fails, or sqlite gets corrupted depending on timing). Use Approach 1 for this case.
- You can't add JS code on the host side. The flush requirement is non-negotiable for durability.

### Web Worker recommended

OPFS sync access handles — the fast path — only work in Web Worker contexts. On the main thread, `mountNativeFS` falls back to the async file API, which still works but is slower for sustained writes. If your Pyodide is on the main thread (e.g., direct `loadPyodide()` from a `<script>` tag), consider moving it into a Worker for performance. The flush logic is identical either way.

## Migrating between approaches

There's no automatic migration path. The two backends store data in completely different shapes:

- `kind="indexeddb"` puts kvgit-format records directly in IndexedDB under `db_name`.
- `kind="disk"` over OPFS puts a SQLite database file inside the OPFS mount.

If you're switching, plan for a one-time export/import — read all keys from the old backend in Python, write them to the new one. Or, if you're pre-launch, just do a clean break.

## Verifying the recipe

The kvgit test suite includes capability probes (`tests/kv/test_pyodide_fs.py`) that exercise these paths in real browsers via pytest-pyodide. They're gated behind `KVGIT_PYODIDE_TESTS=1` and run in CI on Chrome and Firefox. If you're integrating kvgit into a new Pyodide environment, those probes are a useful "does my setup actually work" sanity check.
