/**
 * SQLite-backed `KVStore` for Node, using the built-in `node:sqlite`
 * module.
 *
 * **Requires Node 22.5+** — `node:sqlite` is stable from Node 24+; on
 * 22.5/23 it works but ships behind the `--experimental-sqlite` flag.
 * No native dependencies needed (no `better-sqlite3`, no postinstall
 * compilation).
 *
 * Storage layout: a single `kv` table with `(key TEXT PRIMARY KEY,
 * value BLOB NOT NULL) WITHOUT ROWID`. WITHOUT ROWID makes lookups
 * by primary key slightly faster and cuts space overhead — exactly
 * what a KV store wants.
 *
 * WAL journal mode is enabled by default for file-backed databases:
 * concurrent readers don't block, and writes are durable. Set
 * `wal: false` to fall back to the default rollback journal.
 *
 * CAS is implemented as two atomic single-statement forms — no
 * explicit transactions needed:
 * - `expected === null`: `INSERT OR IGNORE`. The single statement is
 *   atomic; succeeds iff the row was inserted (changes === 1).
 * - `expected` is bytes: `UPDATE WHERE key = ? AND value = ?`. The
 *   row is updated iff the current value matches; changes === 1
 *   signals success.
 *
 * Both forms are race-free against concurrent writers because SQLite
 * serializes writes within a database (single-writer model).
 */

import { createRequire } from 'node:module'
import type { DatabaseSync as DatabaseSyncT, StatementSync } from 'node:sqlite'
import type { KVStore } from '../types'
import { bytesEqual, checkBatch } from './_batch'

// Vite's 5.x dep optimizer doesn't recognize `node:sqlite` as a Node
// built-in and strips the `node:` prefix, breaking resolution — even
// with @vite-ignore on a dynamic import. Bypass Vite's module system
// entirely by going through Node's require, which has the correct
// builtin resolver. The type-only import above is erased at build time.
const localRequire = createRequire(import.meta.url)
let _DatabaseSync: typeof DatabaseSyncT | null = null
function loadDatabaseSync(): typeof DatabaseSyncT {
  if (_DatabaseSync === null) {
    const mod = localRequire('node:sqlite') as typeof import('node:sqlite')
    _DatabaseSync = mod.DatabaseSync
  }
  return _DatabaseSync
}

export interface SqliteOptions {
  /** Database path. `:memory:` (default) for in-memory, file path
   *  otherwise. File paths persist across handles. */
  path?: string
  /** Enable WAL journal mode for file-backed databases. Default true.
   *  Ignored for `:memory:`. WAL gives concurrent readers + one writer;
   *  the rollback-journal default serializes all access. */
  wal?: boolean
}

const SCHEMA_DDL = `
  CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
  ) WITHOUT ROWID;
`

/**
 * Construct via the async `Sqlite.open(opts?)` factory. The constructor
 * is private; the factory opens the database, applies the WAL pragma,
 * creates the schema if missing, and prepares the statements once.
 */
export class Sqlite implements KVStore {
  readonly #db: DatabaseSyncT
  readonly #getStmt: StatementSync
  readonly #setStmt: StatementSync
  readonly #removeStmt: StatementSync
  readonly #hasStmt: StatementSync
  readonly #allKeysStmt: StatementSync
  readonly #allItemsStmt: StatementSync
  readonly #prefixKeysStmt: StatementSync
  readonly #prefixItemsStmt: StatementSync
  readonly #clearStmt: StatementSync
  readonly #casUpdateStmt: StatementSync
  readonly #casInsertIfAbsentStmt: StatementSync

  private constructor(db: DatabaseSyncT) {
    this.#db = db
    // Cached prepared statements — reused for the lifetime of the
    // handle. node:sqlite cleans them up when the database is closed.
    this.#getStmt = db.prepare('SELECT value FROM kv WHERE key = ?')
    this.#setStmt = db.prepare(
      'INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
    )
    this.#removeStmt = db.prepare('DELETE FROM kv WHERE key = ?')
    this.#hasStmt = db.prepare('SELECT 1 AS present FROM kv WHERE key = ? LIMIT 1')
    this.#allKeysStmt = db.prepare('SELECT key FROM kv')
    this.#allItemsStmt = db.prepare('SELECT key, value FROM kv')
    this.#prefixKeysStmt = db.prepare('SELECT key FROM kv WHERE key >= ? AND key < ?')
    this.#prefixItemsStmt = db.prepare('SELECT key, value FROM kv WHERE key >= ? AND key < ?')
    this.#clearStmt = db.prepare('DELETE FROM kv')
    this.#casUpdateStmt = db.prepare('UPDATE kv SET value = ? WHERE key = ? AND value = ?')
    this.#casInsertIfAbsentStmt = db.prepare('INSERT OR IGNORE INTO kv (key, value) VALUES (?, ?)')
  }

  static async open(opts: SqliteOptions = {}): Promise<Sqlite> {
    const path = opts.path ?? ':memory:'
    const DatabaseSync = loadDatabaseSync()
    const db = new DatabaseSync(path)
    if ((opts.wal ?? true) && path !== ':memory:') {
      db.exec('PRAGMA journal_mode = WAL')
    }
    db.exec(SCHEMA_DDL)
    return new Sqlite(db)
  }

  close(): void {
    this.#db.close()
  }

  // ---------- Reads ----------

  async get(key: string): Promise<Uint8Array | null> {
    const row = this.#getStmt.get(key) as { value: Uint8Array } | undefined
    if (row === undefined) return null
    return toFreshUint8Array(row.value)
  }

  async has(key: string): Promise<boolean> {
    return this.#hasStmt.get(key) !== undefined
  }

  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    // Single-row lookups; cheap with prepared statement reuse. A
    // SELECT ... WHERE key IN (?, ?, ...) form would need dynamic
    // parameter binding — we'd recompile per distinct key count.
    // The point-lookup loop is fast enough for typical use.
    const out = new Map<string, Uint8Array>()
    for (const key of keys) {
      const row = this.#getStmt.get(key) as { value: Uint8Array } | undefined
      if (row !== undefined) out.set(key, toFreshUint8Array(row.value))
    }
    return out
  }

  // ---------- Writes ----------

  async set(key: string, value: Uint8Array): Promise<void> {
    this.#setStmt.run(key, value)
  }

  async setMany(items: Iterable<readonly [string, Uint8Array]>): Promise<void> {
    const arr = [...items]
    if (arr.length === 0) return
    this.#db.exec('BEGIN')
    try {
      for (const [k, v] of arr) this.#setStmt.run(k, v)
      this.#db.exec('COMMIT')
    } catch (e) {
      this.#db.exec('ROLLBACK')
      throw e
    }
  }

  async remove(key: string): Promise<void> {
    this.#removeStmt.run(key)
  }

  async removeMany(keys: Iterable<string>): Promise<void> {
    const arr = [...keys]
    if (arr.length === 0) return
    this.#db.exec('BEGIN')
    try {
      for (const k of arr) this.#removeStmt.run(k)
      this.#db.exec('COMMIT')
    } catch (e) {
      this.#db.exec('ROLLBACK')
      throw e
    }
  }

  // ---------- CAS ----------

  async cas(key: string, value: Uint8Array, expected: Uint8Array | null): Promise<boolean> {
    if (expected === null) {
      // INSERT OR IGNORE: succeeds iff the row didn't already exist.
      const r = this.#casInsertIfAbsentStmt.run(key, value)
      return Number(r.changes) === 1
    }
    // UPDATE ... WHERE key = ? AND value = ?: succeeds iff the
    // current value bytewise matches `expected`.
    const r = this.#casUpdateStmt.run(value, key, expected)
    return Number(r.changes) === 1
  }

  async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const removeList = checkBatch(writes, removes)
    // BEGIN IMMEDIATE takes the write lock before the reads, so no other
    // connection can change an expected key between check and write.
    this.#db.exec('BEGIN IMMEDIATE')
    try {
      for (const [key, value] of expected) {
        const row = this.#getStmt.get(key) as { value: Uint8Array } | undefined
        const current = row === undefined ? null : row.value
        if (!bytesEqual(current, value)) {
          this.#db.exec('ROLLBACK')
          return false
        }
      }
      for (const [key, value] of writes) this.#setStmt.run(key, value)
      for (const key of removeList) this.#removeStmt.run(key)
      this.#db.exec('COMMIT')
      return true
    } catch (e) {
      this.#db.exec('ROLLBACK')
      throw e
    }
  }

  // ---------- Iteration ----------

  async *keys(prefix?: string): AsyncIterable<string> {
    const rows = (
      prefix !== undefined
        ? this.#prefixKeysStmt.all(prefix, prefixUpperBound(prefix))
        : this.#allKeysStmt.all()
    ) as Array<{ key: string }>
    for (const row of rows) yield row.key
  }

  async *items(prefix?: string): AsyncIterable<readonly [string, Uint8Array]> {
    const rows = (
      prefix !== undefined
        ? this.#prefixItemsStmt.all(prefix, prefixUpperBound(prefix))
        : this.#allItemsStmt.all()
    ) as Array<{ key: string; value: Uint8Array }>
    for (const row of rows) yield [row.key, toFreshUint8Array(row.value)] as const
  }

  async clear(): Promise<void> {
    this.#clearStmt.run()
  }
}

/**
 * node:sqlite returns BLOB as a Uint8Array, but the underlying buffer
 * may be reused by the SQLite driver after the result is consumed.
 * Copy into a fresh Uint8Array so callers can hold the value past the
 * statement boundary safely.
 */
function toFreshUint8Array(v: Uint8Array): Uint8Array {
  return new Uint8Array(v)
}

/**
 * Compute the exclusive upper bound for a prefix range scan.
 * `'￿'` (highest BMP code point) is appended so the half-open
 * range `[prefix, upper)` covers every key starting with `prefix`.
 */
function prefixUpperBound(prefix: string): string {
  return `${prefix}￿`
}
