import type { KVStore } from '../types'
import { bytesEqual, checkBatch } from './_batch'

/**
 * In-process KV store backed by a `Map`. Useful for tests, dev-mode
 * agents, and as the reference implementation against which other
 * backends are tested.
 *
 * Operations are synchronous internally but exposed as `async` to
 * match the `KVStore` interface — user code is uniform across
 * backends.
 *
 * Values are copied on the way in and on the way out, as a backend
 * that serializes them does by nature: a caller holding an array it
 * wrote, or one it read, can't change what the store holds (and with
 * it a commit whose hash covers those bytes). Each copy is a fresh buffer of
 * exactly the value's size, made with `new Uint8Array` rather than
 * `slice`: Node's `Buffer#slice` is a view, which would keep sharing
 * (and pinning) the caller's buffer, an encoder's scratch space say.
 */
export class Memory implements KVStore {
  readonly #data = new Map<string, Uint8Array>()

  async get(key: string): Promise<Uint8Array | null> {
    const v = this.#data.get(key)
    return v === undefined ? null : new Uint8Array(v)
  }

  async set(key: string, value: Uint8Array): Promise<void> {
    this.#data.set(key, new Uint8Array(value))
  }

  async remove(key: string): Promise<void> {
    this.#data.delete(key)
  }

  async has(key: string): Promise<boolean> {
    return this.#data.has(key)
  }

  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const out = new Map<string, Uint8Array>()
    for (const k of keys) {
      const v = this.#data.get(k)
      if (v !== undefined) out.set(k, new Uint8Array(v))
    }
    return out
  }

  async setMany(items: Iterable<readonly [string, Uint8Array]>): Promise<void> {
    for (const [k, v] of items) {
      this.#data.set(k, new Uint8Array(v))
    }
  }

  async removeMany(keys: Iterable<string>): Promise<void> {
    for (const k of keys) {
      this.#data.delete(k)
    }
  }

  async cas(key: string, value: Uint8Array, expected: Uint8Array | null): Promise<boolean> {
    const current = this.#data.get(key) ?? null
    if (!bytesEqual(current, expected)) return false
    this.#data.set(key, new Uint8Array(value))
    return true
  }

  async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const removeList = checkBatch(writes, removes)
    for (const [key, value] of expected) {
      if (!bytesEqual(this.#data.get(key) ?? null, value)) return false
    }
    for (const [key, value] of writes) this.#data.set(key, new Uint8Array(value))
    for (const key of removeList) this.#data.delete(key)
    return true
  }

  async *keys(prefix?: string): AsyncIterable<string> {
    if (prefix === undefined) {
      for (const k of this.#data.keys()) yield k
    } else {
      for (const k of this.#data.keys()) {
        if (k.startsWith(prefix)) yield k
      }
    }
  }

  async *items(prefix?: string): AsyncIterable<readonly [string, Uint8Array]> {
    for (const [k, v] of this.#data.entries()) {
      if (prefix === undefined || k.startsWith(prefix)) yield [k, new Uint8Array(v)] as const
    }
  }

  async clear(): Promise<void> {
    this.#data.clear()
  }
}
