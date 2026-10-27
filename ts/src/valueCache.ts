/**
 * A worktree's decoded values of committed keys, least recently used
 * first out, bounded in bytes.
 */

/** The default budget for each worktree's decoded values. */
export const DEFAULT_VALUE_CACHE_BYTES = 32 * 1024 * 1024

/** What an entry costs beyond its value's bytes: its key, the map's
 *  bookkeeping, object headers. An estimate, so the budget bounds memory
 *  rather than just payload. */
const ENTRY_OVERHEAD = 96

/**
 * It holds only what the worktree's head already stores, so anything it
 * lets go is read again from the store; pending writes are kept by the
 * worktree itself and never pass through here. A value's cost is the
 * bytes it was decoded from, and a value costing more than a quarter of
 * the budget is not held at all, so one large read cannot push out
 * everything else and the largest values, the ones a caller may want
 * only a few bytes of, are not kept alive by the cache. A budget of `0`
 * holds nothing.
 */
export class ValueCache<V = unknown> {
  readonly maxBytes: number
  /** In use order, least recent first: a `Map` keeps insertion order,
   *  and a use re-inserts. */
  readonly #entries = new Map<string, { value: V; cost: number }>()
  #bytes = 0

  constructor(maxBytes: number = DEFAULT_VALUE_CACHE_BYTES) {
    if (!Number.isInteger(maxBytes) || maxBytes < 0) {
      throw new RangeError(`maxBytes must be a whole number, 0 or more; got ${maxBytes}`)
    }
    this.maxBytes = maxBytes
  }

  /** How many values are held. */
  get size(): number {
    return this.#entries.size
  }

  /** What the values held now cost, by the budget's estimate. */
  get sizeBytes(): number {
    return this.#bytes
  }

  /** The largest cost of a value that is held. */
  get maxValueBytes(): number {
    return Math.floor(this.maxBytes / 4)
  }

  has(key: string): boolean {
    return this.#entries.has(key)
  }

  /** The value held for `key`, marked as just used; undefined if none
   *  is. */
  get(key: string): V | undefined {
    const entry = this.#entries.get(key)
    if (entry === undefined) return undefined
    this.#entries.delete(key)
    this.#entries.set(key, entry)
    return entry.value
  }

  /** Hold `value`, decoded from `nbytes` bytes, letting the least
   *  recently used go to fit. */
  remember(key: string, value: V, nbytes: number): void {
    const cost = nbytes + key.length + ENTRY_OVERHEAD
    if (cost > this.maxValueBytes) return
    const old = this.#entries.get(key)
    if (old !== undefined) {
      this.#bytes -= old.cost
      this.#entries.delete(key)
    }
    this.#entries.set(key, { value, cost })
    this.#bytes += cost
    for (const [oldest, entry] of this.#entries) {
      if (this.#bytes <= this.maxBytes) break
      this.#entries.delete(oldest)
      this.#bytes -= entry.cost
    }
  }

  clear(): void {
    this.#entries.clear()
    this.#bytes = 0
  }
}
