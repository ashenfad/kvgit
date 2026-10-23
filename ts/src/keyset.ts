/**
 * kvgit-specific wrapper around the generic HAMT.
 *
 * A `Keyset` is a content-addressable map from user keys to
 * `KeysetEntry` values, where each entry holds a blob's storage key
 * and per-key metadata. This is what `VersionedKV` (TBD)
 * uses to represent the state of a single commit.
 *
 * The wrapper is thin: encode/decode entries and delegate everything
 * else to `Hamt`. The HAMT does the structural-sharing work; the
 * `Keyset` just gives the API a kvgit-friendly shape.
 */

import { Hamt, type HamtOptions, type NodeCache } from './hamt'
import { pyDumpsBytes } from './pyjson'
import type { KVStore, KeysetDiff, KeysetEntry, MetaEntry } from './types'

const _decoder = new TextDecoder()

/**
 * Encode a `KeysetEntry` to bytes, as Python kvgit's `encode_entry`
 * does: `[blob, {"size": n}]`, with `"chunks"` after `size` only when
 * there are chunks, compact and in that order (not sorted). Same entry,
 * same bytes, same HAMT leaf hash.
 */
export function encodeEntry(entry: KeysetEntry): Uint8Array {
  const meta: Record<string, unknown> = { size: entry.meta.size }
  if (entry.meta.chunks !== undefined && entry.meta.chunks.length > 0) {
    meta.chunks = [...entry.meta.chunks]
  }
  return pyDumpsBytes([entry.blob, meta])
}

export function decodeEntry(raw: Uint8Array): KeysetEntry {
  const [blob, meta] = JSON.parse(_decoder.decode(raw)) as [
    string,
    { size: number; chunks?: string[] },
  ]
  const out: MetaEntry =
    meta.chunks !== undefined && meta.chunks.length > 0
      ? { size: meta.size, chunks: meta.chunks }
      : { size: meta.size }
  return { blob, meta: out }
}

export interface KeysetOptions {
  prefix?: string
  bucketMax?: number
  pending?: Map<string, Uint8Array>
  /** Parsed HAMT nodes to read through before the store. */
  cache?: NodeCache
}

export interface KeysetUpdatedOptions {
  updates?: Iterable<readonly [string, KeysetEntry]>
  removals?: Iterable<string>
}

/**
 * Immutable view of a kvgit keyset, backed by a HAMT.
 *
 * Mutations return a new `Keyset` whose `pending` map carries any new
 * HAMT node bytes not yet flushed to the store. Use `flush()` to
 * persist, or merge `pending` into a larger write batch.
 */
export class Keyset {
  /** Default storage-key prefix for HAMT nodes belonging to a Keyset.
   *  Used by the GC layer to identify keyset nodes via prefix scan. */
  static readonly DEFAULT_PREFIX = 'kvgit:keyset:'

  readonly #hamt: Hamt

  private constructor(hamt: Hamt) {
    this.#hamt = hamt
  }

  /** Construct a fresh, empty Keyset. */
  static async empty(store: KVStore, opts: KeysetOptions = {}): Promise<Keyset> {
    return new Keyset(await Hamt.empty(store, hamtOpts(opts)))
  }

  /** Construct a Keyset from a known root hash. */
  static fromRoot(store: KVStore, root: string, opts: KeysetOptions = {}): Keyset {
    return new Keyset(new Hamt(store, root, hamtOpts(opts)))
  }

  // ---------- Properties ----------

  get store(): KVStore {
    return this.#hamt.store
  }

  get root(): string {
    return this.#hamt.root
  }

  get prefix(): string {
    return this.#hamt.prefix
  }

  get bucketMax(): number {
    return this.#hamt.bucketMax
  }

  get pending(): Map<string, Uint8Array> {
    return this.#hamt.pending
  }

  // ---------- Reads ----------

  async get(key: string): Promise<KeysetEntry | null> {
    const raw = await this.#hamt.get(key)
    return raw === null ? null : decodeEntry(raw)
  }

  /** The entries of the keys that are present, one batched store read
   *  per tree level. */
  async getMany(keys: Iterable<string>): Promise<Map<string, KeysetEntry>> {
    const out = new Map<string, KeysetEntry>()
    for (const [k, raw] of await this.#hamt.getMany(keys)) out.set(k, decodeEntry(raw))
    return out
  }

  /** Shortcut: just the blob pointer, skipping a meta decode. */
  async getBlob(key: string): Promise<string | null> {
    const entry = await this.get(key)
    return entry === null ? null : entry.blob
  }

  async has(key: string): Promise<boolean> {
    return this.#hamt.has(key)
  }

  /**
   * Iterate over all `(key, entry)` pairs lazily. One store read per
   * visited HAMT node. See `materialize()` for a batched alternative.
   */
  async *items(): AsyncIterable<readonly [string, KeysetEntry]> {
    for await (const [k, v] of this.#hamt.items()) {
      yield [k, decodeEntry(v)] as const
    }
  }

  /** Walk the entire keyset using batched store reads. */
  async materialize(): Promise<Map<string, KeysetEntry>> {
    const raw = await this.#hamt.materialize()
    const out = new Map<string, KeysetEntry>()
    for (const [k, v] of raw) out.set(k, decodeEntry(v))
    return out
  }

  /**
   * Single batched walk returning `[entries, hamtNodeHashes]`.
   *
   * Equivalent to `materialize()` plus collecting every visited HAMT
   * node hash, in one tree traversal. Used by GC mark phases. See
   * `Hamt.walk` for `skipNodes` cumulative seen-set semantics.
   */
  async walk(skipNodes?: ReadonlySet<string>): Promise<[Map<string, KeysetEntry>, Set<string>]> {
    const [raw, nodes] = await this.#hamt.walk(skipNodes)
    const entries = new Map<string, KeysetEntry>()
    for (const [k, v] of raw) entries.set(k, decodeEntry(v))
    return [entries, nodes]
  }

  async *keys(): AsyncIterable<string> {
    yield* this.#hamt.keys()
  }

  /** Every key, one batched store read per tree level; see
   *  `Hamt.keysBatched`. */
  keysBatched(chunk?: number): AsyncIterable<string> {
    return this.#hamt.keysBatched(chunk)
  }

  async *values(): AsyncIterable<KeysetEntry> {
    for await (const [, entry] of this.items()) yield entry
  }

  /** Total entry count. O(N) — walks the tree. */
  async size(): Promise<number> {
    return this.#hamt.size()
  }

  // ---------- Writes ----------

  async updated(opts: KeysetUpdatedOptions = {}): Promise<Keyset> {
    const encodedUpdates: Array<readonly [string, Uint8Array]> = []
    if (opts.updates) {
      for (const [k, entry] of opts.updates) {
        encodedUpdates.push([k, encodeEntry(entry)] as const)
      }
    }
    const newHamt = await this.#hamt.updated({
      updates: encodedUpdates,
      ...(opts.removals !== undefined && { removals: opts.removals }),
    })
    return new Keyset(newHamt)
  }

  /** {@link updated} in one pass down the tree; see `Hamt.updatedBatch`. */
  async updatedBatch(opts: KeysetUpdatedOptions = {}): Promise<Keyset> {
    const encodedUpdates: Array<readonly [string, Uint8Array]> = []
    for (const [k, entry] of opts.updates ?? [])
      encodedUpdates.push([k, encodeEntry(entry)] as const)
    const newHamt = await this.#hamt.updatedBatch({
      updates: encodedUpdates,
      ...(opts.removals !== undefined && { removals: opts.removals }),
    })
    return new Keyset(newHamt)
  }

  async persist(opts: KeysetUpdatedOptions = {}): Promise<Keyset> {
    const next = await this.updated(opts)
    return new Keyset(await next.#hamt.flush())
  }

  async flush(): Promise<Keyset> {
    return new Keyset(await this.#hamt.flush())
  }

  // ---------- Structural ops ----------

  /** Yield every HAMT node hash reachable from this root. */
  reachableNodes(): AsyncIterable<string> {
    return this.#hamt.reachableNodes()
  }

  /**
   * Structural diff against `other`. Skips identical subtrees by hash
   * equality, so cost is proportional to the number of changed entries.
   */
  async diff(other: Keyset): Promise<KeysetDiff> {
    const raw = await this.#hamt.diff(other.#hamt)
    const added = new Map<string, KeysetEntry>()
    const removed = new Map<string, KeysetEntry>()
    const modified = new Map<string, readonly [KeysetEntry, KeysetEntry]>()
    for (const [k, v] of raw.added) added.set(k, decodeEntry(v))
    for (const [k, v] of raw.removed) removed.set(k, decodeEntry(v))
    for (const [k, [oldRaw, newRaw]] of raw.modified) {
      modified.set(k, [decodeEntry(oldRaw), decodeEntry(newRaw)] as const)
    }
    return { added, removed, modified }
  }
}

/** Build a `HamtOptions` from `KeysetOptions`, defaulting the prefix. */
function hamtOpts(opts: KeysetOptions): HamtOptions {
  return {
    prefix: opts.prefix ?? Keyset.DEFAULT_PREFIX,
    ...(opts.bucketMax !== undefined && { bucketMax: opts.bucketMax }),
    ...(opts.pending !== undefined && { pending: opts.pending }),
    ...(opts.cache !== undefined && { cache: opts.cache }),
  }
}
