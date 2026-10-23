/**
 * Browser-condition stub for the sqlite backend.
 *
 * The real `@agex-ts/kvgit/backends/sqlite` module imports `node:module` to
 * load `node:sqlite` — both Node-only. When a browser bundler (Vite,
 * Rollup, esbuild, webpack) resolves this sub-path under the
 * `"browser"` exports condition, it picks this file instead, so
 * `node:module` never enters the browser import graph.
 *
 * The stub preserves the public shape (a `Sqlite` class with an async
 * `open` factory) so type-only consumers compile cleanly. Calling
 * `Sqlite.open` at runtime throws — but in practice this only fires
 * if a browser app explicitly opts into `storage: 'sqlite'`, which
 * doesn't make sense in a browser anyway.
 */

import type { KVStore } from '../types'

export interface SqliteOptions {
  path?: string
  wal?: boolean
}

export class Sqlite implements KVStore {
  private constructor() {
    throw new Error('@agex-ts/kvgit: the sqlite backend is Node-only and not available in browsers')
  }

  static async open(_opts: SqliteOptions = {}): Promise<Sqlite> {
    throw new Error('@agex-ts/kvgit: the sqlite backend is Node-only and not available in browsers')
  }

  // Method stubs — never reached because the constructor / factory throw
  // first. They exist so TypeScript sees `Sqlite implements KVStore`.
  get(_key: string): Promise<Uint8Array | null> {
    throw new Error('unreachable')
  }
  set(_key: string, _value: Uint8Array): Promise<void> {
    throw new Error('unreachable')
  }
  remove(_key: string): Promise<void> {
    throw new Error('unreachable')
  }
  has(_key: string): Promise<boolean> {
    throw new Error('unreachable')
  }
  casMany(
    _expected: ReadonlyMap<string, Uint8Array | null>,
    _writes: ReadonlyMap<string, Uint8Array>,
    _removes?: Iterable<string>,
  ): Promise<boolean> {
    throw new Error('unreachable')
  }
  getMany(_keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    throw new Error('unreachable')
  }
  setMany(_items: Iterable<readonly [string, Uint8Array]>): Promise<void> {
    throw new Error('unreachable')
  }
  removeMany(_keys: Iterable<string>): Promise<void> {
    throw new Error('unreachable')
  }
  cas(_key: string, _value: Uint8Array, _expected: Uint8Array | null): Promise<boolean> {
    throw new Error('unreachable')
  }
  keys(_prefix?: string): AsyncIterable<string> {
    throw new Error('unreachable')
  }
  items(_prefix?: string): AsyncIterable<readonly [string, Uint8Array]> {
    throw new Error('unreachable')
  }
  clear(): Promise<void> {
    throw new Error('unreachable')
  }
}
