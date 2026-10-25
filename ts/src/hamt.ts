/**
 * Content-addressable Hash Array Mapped Trie.
 *
 * A persistent `string -> Uint8Array` map laid out in a `KVStore` so that
 * unchanged subtrees are shared across versions by hash equality. Each
 * node is JSON-serialized as Python kvgit serializes it (`json.dumps`
 * with sorted keys, no whitespace, ASCII escapes; base64-encoded values
 * inline in leaves) and stored under its SHA-256 hash, so a HAMT either
 * language builds has the same nodes and root. A HAMT is
 * identified by its root node hash; mutations produce a new root and a
 * set of pending node bytes that the caller persists (atomically, if
 * desired) by writing them through the underlying store.
 *
 * The tree itself lives in the core (`core.ts`): each operation there
 * asks for the nodes it needs, one tree level at a time, and this
 * module answers from the view's pending nodes, then its node cache,
 * then the store.
 *
 * Layering: this module knows nothing about kvgit's commit semantics —
 * it is a generic content-addressable map. The `Keyset` wrapper adds
 * blob-pointer + meta-entry semantics on top.
 */

import type { NodeMemo, TreeOp } from '../wasm/kvgit_wasm.js'
import { type Core, core } from './core'
import type { HamtDiff, KVStore } from './types'

/** A parsed HAMT node, as it is stored. */
export type HamtNode =
  | { readonly kind: 'leaf'; readonly items: Readonly<Record<string, string>> }
  | { readonly kind: 'branch'; readonly children: Readonly<Record<string, string>> }

export interface NodeCacheStats {
  hits: number
  misses: number
  evictions: number
  /** Nodes held now. */
  entries: number
  /** Serialized bytes of the nodes held now. */
  bytes: number
}

/**
 * Tree nodes held between reads, parsed, bounded by their stored bytes
 * and evicted least recently used first. The nodes live in the core; a
 * HAMT given a cache reads through it before the store.
 */
export class NodeCache {
  readonly maxBytes: number
  #memo: NodeMemo | null = null

  constructor(opts: { maxBytes?: number } = {}) {
    this.maxBytes = opts.maxBytes ?? 16 * 1024 * 1024
  }

  /** @internal The core's memo behind this cache. */
  memo(c: Core): NodeMemo {
    this.#memo ??= new c.NodeMemo(this.maxBytes)
    return this.#memo
  }

  /** Hold a node from its stored bytes, once the cache has been read
   *  through (before then it holds nothing to keep it beside). */
  setBytes(hash: string, bytes: Uint8Array): void {
    this.#memo?.hold(hash, bytes)
  }

  stats(): NodeCacheStats {
    if (this.#memo === null) return { hits: 0, misses: 0, evictions: 0, entries: 0, bytes: 0 }
    return JSON.parse(this.#memo.stats()) as NodeCacheStats
  }

  resetStats(): void {
    this.#memo?.resetStats()
  }

  clear(): void {
    this.#memo?.clear()
  }
}

let noMemo: NodeMemo | null = null

/** A memo with no room, for reads that shouldn't be cached. */
/** @internal A memo that holds nothing. */
export function uncached(c: Core): NodeMemo {
  noMemo ??= new c.NodeMemo(0)
  return noMemo
}

/**
 * Hash of the canonical empty leaf, `{"items":{},"kind":"leaf"}`, the
 * root of every empty HAMT. The empty leaf itself is never written to
 * the store; reads short-circuit on this hash.
 */
export const EMPTY_HASH = 'a01262f23a252767acf71bcd9d1ca8deca5fda1519228657ef7d44990698fd30'

// ---------------------------------------------------------------------------
// Driving the core
// ---------------------------------------------------------------------------

/** @internal A tree operation in the core. */
export type { TreeOp }

/** @internal Byte strings laid end to end, and their lengths. */
export function pack(values: Iterable<Uint8Array>): [Uint8Array, Uint32Array] {
  const list = [...values]
  const lengths = new Uint32Array(list.length)
  let total = 0
  for (let i = 0; i < list.length; i++) {
    lengths[i] = (list[i] as Uint8Array).length
    total += lengths[i] as number
  }
  const data = new Uint8Array(total)
  let at = 0
  for (const v of list) {
    data.set(v, at)
    at += v.length
  }
  return [data, lengths]
}

/** @internal The byte strings `pack` laid end to end. */
export function unpack(data: Uint8Array, lengths: Uint32Array): Uint8Array[] {
  const out: Uint8Array[] = []
  let at = 0
  for (const n of lengths) {
    out.push(data.slice(at, at + n))
    at += n
  }
  return out
}

/** @internal Run `op` to the end, answering each read with `read`. The
 *  caller reads its result, then frees it. */
export async function drive(
  op: TreeOp,
  read: (keys: string[]) => Promise<Map<string, Uint8Array>>,
): Promise<TreeOp> {
  let want = op.resume(undefined, undefined, undefined)
  while (want !== undefined) {
    const found = await read(JSON.parse(want) as string[])
    const [data, lengths] = pack(found.values())
    want = op.resume(JSON.stringify([...found.keys()]), data, lengths)
  }
  return op
}

// ---------------------------------------------------------------------------
// Hamt
// ---------------------------------------------------------------------------

export interface HamtOptions {
  /** Storage-key prefix. Two HAMTs sharing a backing store should use
   * different prefixes to avoid node collisions. */
  prefix?: string
  /** Max entries in a leaf before it splits into a branch. */
  bucketMax?: number
  /** Initial pending node bytes (for reconstructing in-progress state). */
  pending?: Map<string, Uint8Array>
  /** Nodes to read through before the store. Nodes read from the store
   *  are added to it. */
  cache?: NodeCache
}

export interface UpdatedOptions {
  updates?: Iterable<readonly [string, Uint8Array]>
  removals?: Iterable<string>
}

/**
 * Immutable, content-addressable HAMT view over a `KVStore`.
 *
 * Mutating methods (`updated`, `persist`) return a new `Hamt`. The
 * returned view's `pending` map contains any new node bytes not yet
 * flushed to the store. Reads on the new view resolve through `pending`
 * first, falling back to the store. Use `flush()` to persist all
 * pending nodes, or merge `pending` into a larger write batch.
 *
 * Two HAMTs with the same logical contents and the same `bucketMax`
 * have the same root hash, regardless of how they were constructed —
 * this invariant is what enables structural sharing across versions.
 *
 * `bucketMax` controls how many entries fit in a leaf before it splits
 * into a branch. Larger buckets mean fewer nodes but larger leaves;
 * smaller buckets mean more nodes with finer-grained sharing. A HAMT
 * built with one `bucketMax` will hash differently from the same
 * logical contents built with another.
 */
export class Hamt {
  readonly store: KVStore
  readonly root: string
  readonly prefix: string
  readonly bucketMax: number
  readonly pending: Map<string, Uint8Array>
  readonly cache: NodeCache | null

  constructor(store: KVStore, root: string, opts: HamtOptions = {}) {
    const bucketMax = opts.bucketMax ?? 8
    if (bucketMax < 1) {
      throw new RangeError(`bucketMax must be >= 1, got ${bucketMax}`)
    }
    this.store = store
    this.root = root
    this.prefix = opts.prefix ?? 'hamt:'
    this.bucketMax = bucketMax
    this.pending = opts.pending ?? new Map()
    this.cache = opts.cache ?? null
  }

  /** Options for a view derived from this one. */
  private derived(pending?: Map<string, Uint8Array>): HamtOptions {
    return {
      prefix: this.prefix,
      bucketMax: this.bucketMax,
      ...(pending !== undefined && { pending }),
      ...(this.cache !== null && { cache: this.cache }),
    }
  }

  /** Construct a fresh, empty HAMT. */
  static async empty(store: KVStore, opts: HamtOptions = {}): Promise<Hamt> {
    return new Hamt(store, EMPTY_HASH, opts)
  }

  /** The hash of the canonical empty leaf. Useful for tests. */
  static emptyHash(): Promise<string> {
    return Promise.resolve(EMPTY_HASH)
  }

  // ---------- Reading nodes for the core ----------

  /** @internal Nodes by prefixed key: pending ones, then the store's.
   *  The core asks only for nodes its memo doesn't hold. */
  async read(keys: string[]): Promise<Map<string, Uint8Array>> {
    const found = new Map<string, Uint8Array>()
    const rest: string[] = []
    for (const key of keys) {
      const pending = this.pending.get(key)
      if (pending !== undefined) found.set(key, pending)
      else rest.push(key)
    }
    if (rest.length > 0) {
      for (const [key, raw] of await this.store.getMany(rest)) found.set(key, raw)
    }
    return found
  }

  /** @internal The memo this view's operations read through. */
  memo(c: Core, useCache = true): NodeMemo {
    return useCache && this.cache !== null ? this.cache.memo(c) : uncached(c)
  }

  /** @internal Run `make(core, memo)`'s operation against this view's
   *  nodes, hand it to `use`, and free it. */
  async run<T>(
    make: (c: Core, memo: NodeMemo) => TreeOp,
    use: (op: TreeOp) => T,
    useCache = true,
  ): Promise<T> {
    const c = await core()
    const op = make(c, this.memo(c, useCache))
    try {
      await drive(op, (keys) => this.read(keys))
      return use(op)
    } finally {
      op.free()
    }
  }

  // ---------- Reads ----------

  /** Look up a key. Returns null if absent. */
  async get(key: string): Promise<Uint8Array | null> {
    return (await this.getMany([key])).get(key) ?? null
  }

  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== null
  }

  /**
   * The values of the keys that are present. Reads the paths to all of
   * them a level at a time, one batched store read per level, rather
   * than walking the tree once per key.
   */
  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const wanted = [...new Set(keys)]
    if (wanted.length === 0 || this.root === EMPTY_HASH) return new Map()
    return this.run(
      (c, memo) =>
        c.TreeOp.getMany(this.prefix, this.bucketMax, this.root, JSON.stringify(wanted), memo),
      (op) => rows(op),
    )
  }

  async *items(): AsyncIterable<readonly [string, Uint8Array]> {
    yield* (await this.walk())[0]
  }

  async *keys(): AsyncIterable<string> {
    yield* (await this.walk())[0].keys()
  }

  /**
   * Every key, one batched store read per tree level. Bypasses the node
   * cache, so a full listing doesn't evict the nodes point reads use.
   */
  async *keysBatched(chunk = 4096): AsyncIterable<string> {
    if (!Number.isInteger(chunk) || chunk < 1) {
      throw new RangeError(`chunk must be a positive integer, not ${chunk}`)
    }
    if (this.root === EMPTY_HASH) return
    const keys = await this.run(
      (c, memo) => c.TreeOp.keys(this.prefix, this.bucketMax, this.root, memo),
      (op) => JSON.parse(op.resultKeys()) as string[],
      false,
    )
    yield* keys
  }

  async *values(): AsyncIterable<Uint8Array> {
    yield* (await this.walk())[0].values()
  }

  /** The whole map, one batched store read per tree level. */
  async materialize(): Promise<Map<string, Uint8Array>> {
    return (await this.walk())[0]
  }

  /**
   * Walk the whole HAMT, returning its entries and every node hash it
   * is made of, one batched store read per level. A node in `skipNodes`
   * is taken as already walked: it and everything under it are neither
   * read nor returned, so passing a cumulative seen-set across the walks
   * of trees that share structure reads each shared node once.
   */
  async walk(skipNodes?: ReadonlySet<string>): Promise<[Map<string, Uint8Array>, Set<string>]> {
    if (this.root === EMPTY_HASH || skipNodes?.has(this.root)) return [new Map(), new Set()]
    const skip =
      skipNodes === undefined
        ? undefined
        : skipNodes instanceof Set
          ? skipNodes
          : new Set(skipNodes)
    return this.run(
      (c, memo) =>
        c.TreeOp.walk(
          this.prefix,
          this.bucketMax,
          this.root,
          skip as Set<string> | undefined,
          memo,
        ),
      (op) => [rows(op), new Set(JSON.parse(op.resultNodes()) as string[])],
    )
  }

  /** Total entry count. O(N) — walks the tree. */
  async size(): Promise<number> {
    return (await this.walk())[0].size
  }

  // ---------- Writes ----------

  /**
   * Apply updates, then removals. Returns a new `Hamt` whose `pending`
   * map contains any new node bytes not yet flushed; every path the
   * changes touch is read up front, a level at a time.
   */
  async updated(opts: UpdatedOptions = {}): Promise<Hamt> {
    const updates = [...(opts.updates ?? [])]
    const removals = [...(opts.removals ?? [])]
    const [data, lengths] = pack(updates.map(([, v]) => v))
    const n = this.prefix.length
    const unwrittenKeys = [...this.pending.keys()].map((k) => k.slice(n))
    const [unwrittenData, unwrittenLengths] = pack(this.pending.values())
    const [root, pending] = await this.run(
      (c, memo) =>
        c.TreeOp.update(
          this.prefix,
          this.bucketMax,
          this.root,
          JSON.stringify(updates.map(([k]) => k)),
          data,
          lengths,
          JSON.stringify(removals),
          JSON.stringify(unwrittenKeys),
          unwrittenData,
          unwrittenLengths,
          memo,
        ),
      (op) => {
        const nodes = new Map<string, Uint8Array>()
        for (const [hash, raw] of rows(op)) nodes.set(this.prefix + hash, raw)
        return [op.resultRoot(), nodes] as const
      },
    )
    return new Hamt(this.store, root, this.derived(pending))
  }

  /** {@link updated}: the core applies every change in one pass. */
  async updatedBatch(opts: UpdatedOptions = {}): Promise<Hamt> {
    return this.updated(opts)
  }

  /** Apply updates and removals and write the new nodes to the store. */
  async persist(opts: UpdatedOptions = {}): Promise<Hamt> {
    return (await this.updated(opts)).flush()
  }

  /** Persist any pending node writes. Returns a fresh `Hamt`. */
  async flush(): Promise<Hamt> {
    if (this.pending.size > 0) await this.store.setMany(this.pending)
    return new Hamt(this.store, this.root, this.derived())
  }

  // ---------- Structural ops ----------

  /** Yield every node hash reachable from this root, pending ones
   *  included. */
  async *reachableNodes(): AsyncIterable<string> {
    yield* (await this.walk())[1]
  }

  /**
   * Structural diff against `other`. Identical subtrees (same node hash)
   * are never read, so the cost follows the size of the change; both
   * trees are read a level at a time.
   */
  async diff(other: Hamt): Promise<HamtDiff> {
    const added = new Map<string, Uint8Array>()
    const removed = new Map<string, Uint8Array>()
    const modified = new Map<string, readonly [Uint8Array, Uint8Array]>()
    if (this.root === other.root) return { added, removed, modified }
    const c = await core()
    const op = c.TreeOp.diff(this.prefix, this.bucketMax, this.root, other.root, this.memo(c))
    try {
      await drive(op, (keys) => this.readEither(other, keys))
      const kinds = op.resultDiffKinds()
      const keys = JSON.parse(op.resultDiffKeys()) as string[]
      const values = unpack(op.resultDiffData(), op.resultDiffLengths())
      let v = 0
      kinds.forEach((kind, i) => {
        const key = keys[i] as string
        if (kind === 0) added.set(key, values[v++] as Uint8Array)
        else if (kind === 1) removed.set(key, values[v++] as Uint8Array)
        else {
          modified.set(key, [values[v] as Uint8Array, values[v + 1] as Uint8Array] as const)
          v += 2
        }
      })
    } finally {
      op.free()
    }
    return { added, removed, modified }
  }

  /** @internal Nodes from this view, then from `other`'s: content
   *  addressed, so either tree's copy will do. */
  async readEither(other: Hamt, keys: string[]): Promise<Map<string, Uint8Array>> {
    const found = await this.read(keys)
    const rest = keys.filter((k) => !found.has(k))
    if (rest.length > 0) {
      const n = this.prefix.length
      const theirs = await other.read(rest.map((k) => other.prefix + k.slice(n)))
      const m = other.prefix.length
      for (const [k, v] of theirs) found.set(this.prefix + k.slice(m), v)
    }
    return found
  }
}

/** An operation's result rows: key to value. */
function rows(op: TreeOp): Map<string, Uint8Array> {
  const keys = JSON.parse(op.resultKeys()) as string[]
  const values = unpack(op.resultData(), op.resultLengths())
  const out = new Map<string, Uint8Array>()
  keys.forEach((k, i) => out.set(k, values[i] as Uint8Array))
  return out
}
