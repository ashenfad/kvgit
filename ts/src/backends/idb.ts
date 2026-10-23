/**
 * IndexedDB-backed `KVStore` for browsers (and Node with a shim).
 *
 * Two pieces of discipline matter, both ported from kvgit-py's IDB
 * backend:
 *
 * 1. **Handle `onblocked` explicitly.** When another connection holds
 *    the database open with an incompatible version, IDB fires
 *    `onblocked` instead of resolving. Without an explicit handler the
 *    open promise never settles. We reject with an actionable message.
 *
 * 2. **Attach IDB request handlers synchronously, before any `await`.**
 *    The browser's microtask queue can complete an IDB request between
 *    request creation and handler attachment if you `await` in the
 *    middle, silently losing the result. All `IDBRequest` ops in this
 *    file create the request, attach `onsuccess`/`onerror`, and only
 *    *then* await — usually wrapped in a Promise that ties resolution
 *    to the transaction's `oncomplete` event.
 *
 * Beyond those, this backend is a thin shim over IDB. Values are
 * stored as `Uint8Array` directly (IDB's structured-clone handles
 * binary natively — no base64), each in a buffer of exactly its own
 * bytes: structured clone keeps a view's whole `ArrayBuffer`, so a
 * view handed in as it is would store every byte of the buffer under it.
 */

import type { KVStore } from '../types'
import { bytesEqual, checkBatch } from './_batch'

const DEFAULT_DB_NAME = 'kvgit'
const DEFAULT_STORE_NAME = 'kv'
const SCHEMA_VERSION = 1

export interface IndexedDBOptions {
  /** IndexedDB database name. Each name is an independent persistent store. */
  dbName?: string
  /** Object store name within the database. */
  storeName?: string
}

/**
 * IndexedDB-backed KV store.
 *
 * Construct via the async `IndexedDB.open(opts?)` factory. The
 * underlying `IDBDatabase` is opened (and the object store created if
 * needed) before the instance is returned, so subsequent operations
 * can be synchronous up to the IDB boundary.
 */
export class IndexedDB implements KVStore {
  readonly #db: IDBDatabase
  readonly #storeName: string

  private constructor(db: IDBDatabase, storeName: string) {
    this.#db = db
    this.#storeName = storeName
  }

  static async open(opts: IndexedDBOptions = {}): Promise<IndexedDB> {
    const dbName = opts.dbName ?? DEFAULT_DB_NAME
    const storeName = opts.storeName ?? DEFAULT_STORE_NAME

    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const req = globalThis.indexedDB.open(dbName, SCHEMA_VERSION)
      req.onupgradeneeded = () => {
        const upgradedDb = req.result
        if (!upgradedDb.objectStoreNames.contains(storeName)) {
          upgradedDb.createObjectStore(storeName)
        }
      }
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error ?? new Error('IndexedDB open failed'))
      req.onblocked = () =>
        reject(
          new Error(
            `IndexedDB open of '${dbName}' is blocked. Close other tabs / windows holding the database open and reload, or restart the browser.`,
          ),
        )
    })

    return new IndexedDB(db, storeName)
  }

  /** Close the underlying IDB connection. */
  close(): void {
    this.#db.close()
  }

  /**
   * Delete an IndexedDB database entirely. Returns when the deletion
   * succeeds.
   *
   * A connection still open is given `blockedMs` (default 5000) to
   * close. IndexedDB fires `blocked` at once, even for a connection
   * `close()`d just before whose last transaction is still finishing:
   * that one closes when the transaction does, and the deletion goes
   * ahead. Rejects if a connection is still open after `blockedMs`
   * (another tab, say); the deletion then happens once it closes.
   */
  static async deleteDatabase(dbName: string, opts: { blockedMs?: number } = {}): Promise<void> {
    const blockedMs = opts.blockedMs ?? 5000
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined
      const req = globalThis.indexedDB.deleteDatabase(dbName)
      req.onsuccess = () => {
        clearTimeout(timer)
        resolve()
      }
      req.onerror = () => {
        clearTimeout(timer)
        reject(req.error ?? new Error('IndexedDB deleteDatabase failed'))
      }
      req.onblocked = () => {
        timer ??= setTimeout(
          () =>
            reject(
              new Error(
                `IndexedDB delete of '${dbName}' is blocked: another connection has kept it open for ${blockedMs} ms. Close other tabs / connections.`,
              ),
            ),
          blockedMs,
        )
      }
    })
  }

  // ---------- Internal: tx helper ----------

  #tx(mode: IDBTransactionMode): { store: IDBObjectStore; tx: IDBTransaction } {
    const tx = this.#db.transaction(this.#storeName, mode)
    return { store: tx.objectStore(this.#storeName), tx }
  }

  // ---------- Reads ----------

  async get(key: string): Promise<Uint8Array | null> {
    const { store, tx } = this.#tx('readonly')
    return new Promise((resolve, reject) => {
      const req = store.get(key)
      req.onsuccess = () => {
        const r = req.result
        resolve(r === undefined ? null : (r as Uint8Array))
      }
      req.onerror = () => reject(req.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  async has(key: string): Promise<boolean> {
    const { store, tx } = this.#tx('readonly')
    return new Promise((resolve, reject) => {
      const req = store.count(key)
      req.onsuccess = () => resolve(req.result > 0)
      req.onerror = () => reject(req.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const keyArr = [...keys]
    if (keyArr.length === 0) return new Map()
    const { store, tx } = this.#tx('readonly')
    return new Promise((resolve, reject) => {
      const result = new Map<string, Uint8Array>()
      // Attach all handlers synchronously before any await — otherwise
      // the tx could auto-commit between requests on a microtask tick.
      for (const key of keyArr) {
        const req = store.get(key)
        req.onsuccess = () => {
          if (req.result !== undefined) result.set(key, req.result as Uint8Array)
        }
        req.onerror = () => reject(req.error)
      }
      tx.oncomplete = () => resolve(result)
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  // ---------- Writes ----------

  async set(key: string, value: Uint8Array): Promise<void> {
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      const req = store.put(ownBuffer(value), key)
      req.onerror = () => reject(req.error)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  async setMany(items: Iterable<readonly [string, Uint8Array]>): Promise<void> {
    const itemArr = [...items]
    if (itemArr.length === 0) return
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      for (const [k, v] of itemArr) {
        const req = store.put(ownBuffer(v), k)
        req.onerror = () => reject(req.error)
      }
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  async remove(key: string): Promise<void> {
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      const req = store.delete(key)
      req.onerror = () => reject(req.error)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  async removeMany(keys: Iterable<string>): Promise<void> {
    const keyArr = [...keys]
    if (keyArr.length === 0) return
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      for (const k of keyArr) {
        const req = store.delete(k)
        req.onerror = () => reject(req.error)
      }
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  // ---------- CAS ----------

  /**
   * Atomic compare-and-swap.
   *
   * Read + conditional write happen in a single `readwrite`
   * transaction. IDB serializes `readwrite` transactions on the same
   * object store, so concurrent CAS calls (even from other workers
   * sharing the database) are safely linearized.
   */
  /** Every read, the check and every write run in one readwrite
   *  transaction, which IndexedDB linearizes against every other. */
  async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const removeList = checkBatch(writes, removes)
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      let landed = false
      const apply = (): void => {
        landed = true
        for (const [key, value] of writes) {
          const req = store.put(ownBuffer(value), key)
          req.onerror = () => reject(req.error)
        }
        for (const key of removeList) {
          const req = store.delete(key)
          req.onerror = () => reject(req.error)
        }
      }
      const checks = [...expected]
      if (checks.length === 0) apply()
      let pending = checks.length
      let failed = false
      for (const [key, value] of checks) {
        const req = store.get(key)
        req.onsuccess = () => {
          const current = req.result === undefined ? null : (req.result as Uint8Array)
          if (!bytesEqual(current, value)) failed = true
          pending--
          if (pending === 0 && !failed) apply()
        }
        req.onerror = () => reject(req.error)
      }
      tx.oncomplete = () => resolve(landed)
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  async cas(key: string, value: Uint8Array, expected: Uint8Array | null): Promise<boolean> {
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      let casResult = false
      const readReq = store.get(key)
      // Do the read and conditional write entirely within callbacks
      // so the transaction stays alive between them.
      readReq.onsuccess = () => {
        const current = readReq.result === undefined ? null : (readReq.result as Uint8Array)
        if (bytesEqual(current, expected)) {
          const writeReq = store.put(ownBuffer(value), key)
          writeReq.onerror = () => reject(writeReq.error)
          casResult = true
        }
        // else: no write; tx will auto-commit empty.
      }
      readReq.onerror = () => reject(readReq.error)
      tx.oncomplete = () => resolve(casResult)
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }

  // ---------- Iteration ----------

  // ----- Iteration design note -----
  //
  // Both `keys()` and `items()` materialize the result set inside the
  // IDB transaction's lifetime, then yield from memory. True
  // cursor-streaming (yield each row from inside the cursor callback)
  // is possible but conflicts with two of this file's discipline rules
  // (handler-attach-synchronously, no-await-in-tx) — the consumer's
  // pull rate would dictate when `cursor.continue()` runs, and any
  // delay risks the tx auto-committing mid-iteration.
  //
  // For our usage patterns — bounded HAMT walks, prefix-scoped GC
  // sweeps — the consumer always drains the iterator anyway, so
  // streaming's memory benefit doesn't materialize. We pay the
  // materialization cost knowingly. Revisit if a real consumer
  // genuinely benefits from backpressure-friendly streaming.

  async *keys(prefix?: string): AsyncIterable<string> {
    const { store, tx } = this.#tx('readonly')
    const range = prefix !== undefined ? prefixRange(prefix) : null
    const collected = await new Promise<IDBValidKey[]>((resolve, reject) => {
      const req = range !== null ? store.getAllKeys(range) : store.getAllKeys()
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
    for (const k of collected) yield String(k)
  }

  async *items(prefix?: string): AsyncIterable<readonly [string, Uint8Array]> {
    const { store, tx } = this.#tx('readonly')
    const range = prefix !== undefined ? prefixRange(prefix) : null
    const collected = await new Promise<Array<[string, Uint8Array]>>((resolve, reject) => {
      const items: Array<[string, Uint8Array]> = []
      const req = range !== null ? store.openCursor(range) : store.openCursor()
      req.onsuccess = () => {
        const cursor = req.result
        if (cursor) {
          items.push([String(cursor.key), cursor.value as Uint8Array])
          cursor.continue()
        } else {
          resolve(items)
        }
      }
      req.onerror = () => reject(req.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
    for (const item of collected) yield item
  }

  async clear(): Promise<void> {
    const { store, tx } = this.#tx('readwrite')
    return new Promise((resolve, reject) => {
      const req = store.clear()
      req.onerror = () => reject(req.error)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
      tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'))
    })
  }
}

/**
 * The bytes in a buffer of their own size. A view into a larger buffer
 * (an encoder's scratch space, a `subarray`) is copied, since IDB would
 * store the whole buffer; a value that already fills its buffer goes
 * through as it is. `new Uint8Array` copies where `slice` would not:
 * Node's `Buffer#slice` is a view.
 */
function ownBuffer(value: Uint8Array): Uint8Array {
  if (value.byteOffset === 0 && value.byteLength === value.buffer.byteLength) return value
  return new Uint8Array(value)
}

/**
 * Build an `IDBKeyRange` covering all keys with the given prefix.
 * `'￿'` is the highest BMP code point — well beyond any sensible
 * suffix character.
 */
function prefixRange(prefix: string): IDBKeyRange {
  return IDBKeyRange.bound(prefix, `${prefix}￿`, false, false)
}
