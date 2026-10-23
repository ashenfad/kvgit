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
 * Layering: this module knows nothing about kvgit's commit semantics —
 * it is a generic content-addressable map. The `Keyset` wrapper (TBD)
 * adds blob-pointer + meta-entry semantics on top.
 */

import { pyDumpsBytes } from './pyjson'
import type { HamtDiff, KVStore } from './types'

/** SHA-256 hex digest length. Bounds the maximum trie depth. */
const HASH_LEN = 64

interface LeafNode {
  readonly kind: 'leaf'
  /** user key -> base64-encoded value */
  readonly items: Record<string, string>
}

interface BranchNode {
  readonly kind: 'branch'
  /** hex nibble (`'0'..'f'`) -> child node hash */
  readonly children: Record<string, string>
}

type Node = LeafNode | BranchNode

/** A parsed HAMT node, as {@link NodeCache} holds it. */
export type HamtNode = Node

/** Counters a {@link NodeCache} keeps. */
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
 * A bounded LRU of parsed HAMT nodes, by node hash.
 *
 * A node is named by the hash of its bytes, so an entry can never go
 * stale: one cache can serve every keyset in a store, across commits,
 * branches and handles. The bound is on the nodes' serialized size,
 * a rough proxy for what they hold in memory.
 */
export class NodeCache {
  readonly maxBytes: number
  readonly #map = new Map<string, { node: Node; size: number }>()
  #bytes = 0
  #hits = 0
  #misses = 0
  #evictions = 0

  constructor(opts: { maxBytes?: number } = {}) {
    this.maxBytes = opts.maxBytes ?? 16 * 1024 * 1024
  }

  /** The node, marked most recently used; undefined (a miss) if absent. */
  get(hash: string): Node | undefined {
    const hit = this.#map.get(hash)
    if (hit === undefined) {
      this.#misses++
      return undefined
    }
    this.#hits++
    this.#map.delete(hash)
    this.#map.set(hash, hit)
    return hit.node
  }

  /** Hold `node` as most recently used, evicting the least recently used
   *  nodes past the bound. */
  set(hash: string, node: Node, size: number): void {
    const prior = this.#map.get(hash)
    if (prior !== undefined) {
      this.#map.delete(hash)
      this.#bytes -= prior.size
    }
    if (size > this.maxBytes) return
    this.#map.set(hash, { node, size })
    this.#bytes += size
    while (this.#bytes > this.maxBytes) {
      const [oldest, entry] = this.#map.entries().next().value as [string, { size: number }]
      this.#map.delete(oldest)
      this.#bytes -= entry.size
      this.#evictions++
    }
  }

  /** Parse and hold a node from its stored bytes. */
  setBytes(hash: string, bytes: Uint8Array): void {
    this.set(hash, parseNode(bytes), bytes.length)
  }

  stats(): NodeCacheStats {
    return {
      hits: this.#hits,
      misses: this.#misses,
      evictions: this.#evictions,
      entries: this.#map.size,
      bytes: this.#bytes,
    }
  }

  resetStats(): void {
    this.#hits = 0
    this.#misses = 0
    this.#evictions = 0
  }

  clear(): void {
    this.#map.clear()
    this.#bytes = 0
  }
}

/** One key's change in a batched update: its new encoded value, or
 *  null to remove it. */
interface KeyChange {
  readonly key: string
  readonly kh: string
  readonly value: string | null
}

const EMPTY_LEAF: LeafNode = { items: {}, kind: 'leaf' }

/**
 * Hash of the canonical empty leaf, `{"items":{},"kind":"leaf"}`, the
 * root of every empty HAMT. The empty leaf itself is never written to
 * the store; reads short-circuit on this hash.
 */
export const EMPTY_HASH = 'a01262f23a252767acf71bcd9d1ca8deca5fda1519228657ef7d44990698fd30'

function emptyHash(): Promise<string> {
  return Promise.resolve(EMPTY_HASH)
}

// ---------------------------------------------------------------------------
// Serialization & hashing
// ---------------------------------------------------------------------------

const _encoder = new TextEncoder()
const _decoder = new TextDecoder()

/**
 * Serialize a node deterministically, as Python's `json.dumps(node,
 * sort_keys=True, separators=(",", ":"))` does: keys sorted by code
 * point, no whitespace, non-ASCII escaped. Two HAMTs with the same
 * logical contents produce byte-for-byte identical nodes, which is what
 * makes content addressing work, across languages too.
 */
function nodeBytes(node: Node): Uint8Array {
  return pyDumpsBytes(node, { sortKeys: true })
}

/** An empty record with no prototype, so every key, `__proto__`
 *  included, is an own property when assigned. */
function dict(): Record<string, string> {
  return Object.create(null) as Record<string, string>
}

/** A leaf's encoded value for `key`, if it holds one: an own property,
 *  never one inherited from `Object.prototype`. */
function itemOf(items: Record<string, string>, key: string): string | undefined {
  return Object.hasOwn(items, key) ? items[key] : undefined
}

function parseNode(bytes: Uint8Array): Node {
  return JSON.parse(_decoder.decode(bytes)) as Node
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  // SubtleCrypto's `BufferSource` excludes `SharedArrayBuffer`-backed views.
  // All our inputs are `ArrayBuffer`-backed in practice; narrow the cast.
  const buf = await globalThis.crypto.subtle.digest('SHA-256', data as Uint8Array<ArrayBuffer>)
  const bytes = new Uint8Array(buf)
  let hex = ''
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i] as number
    hex += (b < 16 ? '0' : '') + b.toString(16)
  }
  return hex
}

function keyHash(key: string): Promise<string> {
  return sha256Hex(_encoder.encode(key))
}

// ---------------------------------------------------------------------------
// Base64 codec for leaf values
// ---------------------------------------------------------------------------

function encodeValue(value: Uint8Array): string {
  let binary = ''
  // Chunked to avoid exceeding the apply-args limit on very large values.
  const chunkSize = 0x8000
  for (let i = 0; i < value.length; i += chunkSize) {
    const slice = value.subarray(i, i + chunkSize)
    binary += String.fromCharCode(...slice)
  }
  return btoa(binary)
}

function decodeValue(s: string): Uint8Array {
  const binary = atob(s)
  const out = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    out[i] = binary.charCodeAt(i)
  }
  return out
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
  /** Parsed nodes to read through before the store. Nodes read from the
   *  store are added to it. */
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
    return new Hamt(store, await emptyHash(), opts)
  }

  /** The hash of the canonical empty leaf. Useful for tests. */
  static emptyHash(): Promise<string> {
    return emptyHash()
  }

  // ---------- Internal load / store ----------

  /**
   * Load a node by hash. Checks the supplied transient pending dict
   * first (used during in-progress batch updates), then `this.pending`,
   * then the store. Returns null if not found.
   */
  private async load(nodeHash: string, pending?: Map<string, Uint8Array>): Promise<Node | null> {
    if (nodeHash === (await emptyHash())) return EMPTY_LEAF
    const prefixed = this.prefix + nodeHash
    const fromPending = pending?.get(prefixed) ?? this.pending.get(prefixed)
    if (fromPending !== undefined) return parseNode(fromPending)
    const cached = this.cache?.get(nodeHash)
    if (cached !== undefined) return cached
    const raw = await this.store.get(prefixed)
    if (raw === null) return null
    const node = parseNode(raw)
    this.cache?.set(nodeHash, node, raw.length)
    return node
  }

  private async storeLeaf(
    items: Record<string, string>,
    pending: Map<string, Uint8Array>,
  ): Promise<string> {
    const node: LeafNode = { items, kind: 'leaf' }
    const bytes = nodeBytes(node)
    const hash = await sha256Hex(bytes)
    pending.set(this.prefix + hash, bytes)
    return hash
  }

  private async storeBranch(
    children: Record<string, string>,
    pending: Map<string, Uint8Array>,
  ): Promise<string> {
    const node: BranchNode = { children, kind: 'branch' }
    const bytes = nodeBytes(node)
    const hash = await sha256Hex(bytes)
    pending.set(this.prefix + hash, bytes)
    return hash
  }

  // ---------- Reads ----------

  /** Look up a key. Returns null if absent. */
  async get(key: string): Promise<Uint8Array | null> {
    const empty = await emptyHash()
    if (this.root === empty) return null
    const kh = await keyHash(key)
    let nodeHash = this.root
    let depth = 0
    while (true) {
      const node = await this.load(nodeHash)
      if (node === null) return null
      if (node.kind === 'leaf') {
        const encoded = itemOf(node.items, key)
        return encoded === undefined ? null : decodeValue(encoded)
      }
      const chunk = kh[depth] as string
      const nextHash = node.children[chunk]
      if (nextHash === undefined) return null
      nodeHash = nextHash
      depth++
    }
  }

  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== null
  }

  /**
   * The values of the keys that are present. Walks the paths to all of
   * them level by level, with one batched store read per level rather
   * than one walk per key, so the cost follows the tree's depth.
   */
  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const found = new Map<string, Uint8Array>()
    if (this.root === (await emptyHash())) return found
    let frontier = new Map<string, Array<{ key: string; kh: string }>>()
    const unique = [...new Set(keys)]
    const hashes = await Promise.all(unique.map(keyHash))
    if (unique.length > 0) {
      frontier.set(
        this.root,
        unique.map((key, i) => ({ key, kh: hashes[i] as string })),
      )
    }
    for (let depth = 0; frontier.size > 0; depth++) {
      const nodes = await this.loadMany([...frontier.keys()])
      const next = new Map<string, Array<{ key: string; kh: string }>>()
      for (const [nodeHash, waiting] of frontier) {
        const node = nodes.get(nodeHash)
        if (node === undefined) continue
        for (const item of waiting) {
          if (node.kind === 'leaf') {
            const encoded = itemOf(node.items, item.key)
            if (encoded !== undefined) found.set(item.key, decodeValue(encoded))
            continue
          }
          const child = node.children[item.kh[depth] as string]
          if (child === undefined) continue
          const at = next.get(child) ?? []
          at.push(item)
          next.set(child, at)
        }
      }
      frontier = next
    }
    return found
  }

  /** Load several nodes in one store read (pending nodes first);
   *  absent ones are left out. */
  private async loadMany(nodeHashes: readonly string[]): Promise<Map<string, Node>> {
    const out = new Map<string, Node>()
    const empty = await emptyHash()
    const fetch: string[] = []
    for (const h of nodeHashes) {
      if (h === empty) {
        out.set(h, EMPTY_LEAF)
        continue
      }
      const pending = this.pending.get(this.prefix + h)
      if (pending !== undefined) {
        out.set(h, parseNode(pending))
        continue
      }
      const cached = this.cache?.get(h)
      if (cached !== undefined) out.set(h, cached)
      else fetch.push(this.prefix + h)
    }
    if (fetch.length > 0) {
      for (const [k, raw] of await this.store.getMany(fetch)) {
        const h = k.slice(this.prefix.length)
        const node = parseNode(raw)
        this.cache?.set(h, node, raw.length)
        out.set(h, node)
      }
    }
    return out
  }

  /**
   * Iterate over all `(key, value)` pairs lazily. One store read per
   * visited node. Use `materialize()` if you want the whole map and the
   * underlying store has non-trivial per-call latency.
   */
  async *items(): AsyncIterable<readonly [string, Uint8Array]> {
    const empty = await emptyHash()
    if (this.root === empty) return
    yield* this.itemsFrom(this.root)
  }

  private async *itemsFrom(nodeHash: string): AsyncIterable<readonly [string, Uint8Array]> {
    const node = await this.load(nodeHash)
    if (node === null) return
    if (node.kind === 'leaf') {
      for (const [k, v] of Object.entries(node.items)) {
        yield [k, decodeValue(v)] as const
      }
    } else {
      for (const childHash of Object.values(node.children)) {
        yield* this.itemsFrom(childHash)
      }
    }
  }

  async *keys(): AsyncIterable<string> {
    for await (const [k] of this.items()) yield k
  }

  /**
   * Every key, a tree level at a time: one batched store read per
   * level (in chunks of `chunk` nodes), holding no more than one level's
   * node hashes. Bypasses the node cache, so a full listing doesn't
   * evict the nodes point reads use.
   */
  async *keysBatched(chunk = 4096): AsyncIterable<string> {
    if (!Number.isInteger(chunk) || chunk < 1) {
      throw new RangeError(`chunk must be a positive integer, not ${chunk}`)
    }
    const empty = await emptyHash()
    if (this.root === empty) return
    let level: string[] = [this.root]
    while (level.length > 0) {
      const next: string[] = []
      for (let i = 0; i < level.length; i += chunk) {
        const slice = level.slice(i, i + chunk)
        const fetch: string[] = []
        const nodes = new Map<string, Node>()
        for (const h of slice) {
          const pending = this.pending.get(this.prefix + h)
          if (pending !== undefined) nodes.set(h, parseNode(pending))
          else fetch.push(this.prefix + h)
        }
        if (fetch.length > 0) {
          for (const [k, raw] of await this.store.getMany(fetch)) {
            nodes.set(k.slice(this.prefix.length), parseNode(raw))
          }
        }
        for (const h of slice) {
          const node = nodes.get(h)
          if (node === undefined) continue
          if (node.kind === 'leaf') yield* Object.keys(node.items)
          else next.push(...Object.values(node.children))
        }
      }
      level = next
    }
  }

  async *values(): AsyncIterable<Uint8Array> {
    for await (const [, v] of this.items()) yield v
  }

  /** Walk the entire HAMT and return its contents as a Map. */
  async materialize(): Promise<Map<string, Uint8Array>> {
    const [items] = await this.walk()
    return items
  }

  /**
   * Walk the entire HAMT, returning `[items, nodeHashes]`.
   *
   * Single batched BFS that collects both the key→value entries and
   * the set of every visited node hash. Used by GC mark phases that
   * want both, like `cleanOrphans`.
   *
   * `skipNodes` is an optional set of node hashes to treat as
   * already-visited. Skipped subtrees are not fetched, not recursed
   * into, and not included in the returned `nodes` set. Items beneath
   * skipped subtrees are also omitted. Pass a cumulative seen-set
   * across multiple `walk()` calls (e.g. across the commits of a
   * branch's history) to share work where the underlying HAMTs share
   * structure.
   */
  async walk(skipNodes?: ReadonlySet<string>): Promise<[Map<string, Uint8Array>, Set<string>]> {
    const empty = await emptyHash()
    const items = new Map<string, Uint8Array>()
    const nodes = new Set<string>()
    if (this.root === empty || skipNodes?.has(this.root)) return [items, nodes]

    let currentLevel: string[] = [this.root]

    while (currentLevel.length > 0) {
      // Partition: cached in pending vs needs fetch. Drop skipped.
      const cachedNodes = new Map<string, Node>()
      const toFetch: string[] = []
      for (const h of currentLevel) {
        if (h === empty || skipNodes?.has(h)) continue
        const prefixed = this.prefix + h
        const fromPending = this.pending.get(prefixed)
        if (fromPending !== undefined) {
          cachedNodes.set(h, parseNode(fromPending))
        } else {
          toFetch.push(prefixed)
        }
      }

      const fetched = toFetch.length > 0 ? await this.store.getMany(toFetch) : new Map()

      const nextLevel: string[] = []
      for (const h of currentLevel) {
        if (h === empty || skipNodes?.has(h)) continue
        const cached = cachedNodes.get(h)
        let node: Node
        if (cached !== undefined) {
          node = cached
        } else {
          const raw = fetched.get(this.prefix + h)
          if (raw === undefined) continue // missing — skip rather than crash
          node = parseNode(raw)
        }
        nodes.add(h)
        if (node.kind === 'leaf') {
          for (const [k, v] of Object.entries(node.items)) {
            items.set(k, decodeValue(v))
          }
        } else {
          nextLevel.push(...Object.values(node.children))
        }
      }
      currentLevel = nextLevel
    }
    return [items, nodes]
  }

  /** Total entry count. O(N) — walks the tree. */
  async size(): Promise<number> {
    let n = 0
    for await (const _ of this.items()) n++
    return n
  }

  // ---------- Writes ----------

  /**
   * Apply updates and removals. Returns a new `Hamt` whose `pending`
   * map contains any new node bytes not yet flushed.
   */
  async updated(opts: UpdatedOptions = {}): Promise<Hamt> {
    const pending = new Map(this.pending)
    let currentRoot = this.root

    for (const [key, value] of opts.updates ?? []) {
      currentRoot = await this.insert(currentRoot, key, value, pending)
    }
    for (const key of opts.removals ?? []) {
      currentRoot = await this.delete(currentRoot, key, pending)
    }

    // Drop any pending node no longer reachable from the new root
    // (intermediate nodes superseded by later updates).
    const reachablePending = await this.filterPending(currentRoot, pending)

    return new Hamt(this.store, currentRoot, this.derived(reachablePending))
  }

  /**
   * {@link updated}, with every change applied in one pass down the
   * tree: the changes are grouped by the nibble they take at each
   * branch, and each node on their paths is rewritten once, however
   * many of them pass through it. The result is the same tree, so the
   * same root, as applying them one at a time; as there, a key both
   * updated and removed ends up removed.
   *
   * Pair it with {@link getMany} over the changed keys first to load
   * their paths a level at a time; this pass then reads them from the
   * node cache.
   */
  async updatedBatch(opts: UpdatedOptions = {}): Promise<Hamt> {
    const ops = new Map<string, string | null>()
    for (const [key, value] of opts.updates ?? []) ops.set(key, encodeValue(value))
    for (const key of opts.removals ?? []) ops.set(key, null)
    if (ops.size === 0) return new Hamt(this.store, this.root, this.derived(new Map(this.pending)))
    const entries = [...ops]
    const hashes = await Promise.all(entries.map(([key]) => keyHash(key)))
    const changes: KeyChange[] = entries.map(([key, value], i) => ({
      key,
      kh: hashes[i] as string,
      value,
    }))
    const pending = new Map(this.pending)
    const root = (await this.applyAt(this.root, 0, changes, pending)) ?? (await emptyHash())
    const reachablePending = await this.filterPending(root, pending)
    return new Hamt(this.store, root, this.derived(reachablePending))
  }

  /** Apply `changes` (all under this node) to the subtree at `nodeHash`;
   *  return its new hash, or null if it is now empty. */
  private async applyAt(
    nodeHash: string,
    depth: number,
    changes: readonly KeyChange[],
    pending: Map<string, Uint8Array>,
  ): Promise<string | null> {
    const node = await this.load(nodeHash, pending)
    if (node === null || node.kind === 'leaf') {
      // A dangling reference reads as an empty leaf, as in `insertAt`.
      const items = Object.assign(dict(), node?.items)
      let changed = node === null
      for (const c of changes) {
        if (c.value === null) {
          if (c.key in items) {
            delete items[c.key]
            changed = true
          }
        } else if (items[c.key] !== c.value) {
          items[c.key] = c.value
          changed = true
        }
      }
      if (!changed) return nodeHash
      return this.subtreeOf(items, depth, pending)
    }

    const groups = new Map<string, KeyChange[]>()
    for (const c of changes) {
      const nibble = c.kh[depth] as string
      const group = groups.get(nibble)
      if (group === undefined) groups.set(nibble, [c])
      else group.push(c)
    }
    const children: Record<string, string> = { ...node.children }
    let changed = false
    let removed = false
    for (const [nibble, group] of groups) {
      const child = children[nibble]
      let next: string | null
      if (child === undefined) {
        const items = dict()
        for (const c of group) if (c.value !== null) items[c.key] = c.value
        next = await this.subtreeOf(items, depth + 1, pending)
        if (next === null) continue
      } else {
        next = await this.applyAt(child, depth + 1, group, pending)
        if (next === child) continue
      }
      changed = true
      if (group.some((c) => c.value === null)) removed = true
      if (next === null) delete children[nibble]
      else children[nibble] = next
    }
    if (!changed) return nodeHash
    if (Object.keys(children).length === 0) return null
    if (removed) {
      // A subtree that shrank to one bucket is a leaf, whatever path
      // led there.
      const collapsed = await this.tryCollapse(children, pending)
      if (collapsed !== null) return collapsed
    }
    return this.storeBranch(children, pending)
  }

  /** The canonical subtree holding exactly `items` at `depth`: a leaf
   *  if they fit one bucket, else split by nibble. Null if empty. */
  private async subtreeOf(
    items: Record<string, string>,
    depth: number,
    pending: Map<string, Uint8Array>,
  ): Promise<string | null> {
    const n = Object.keys(items).length
    if (n === 0) return null
    if (n <= this.bucketMax) return this.storeLeaf(items, pending)
    return this.splitLeaf(items, depth, pending)
  }

  /**
   * Apply updates and write any new nodes to the store immediately.
   * Returns a fresh `Hamt` with empty pending.
   */
  async persist(opts: UpdatedOptions = {}): Promise<Hamt> {
    const next = await this.updated(opts)
    if (next.pending.size > 0) {
      await this.store.setMany(next.pending)
    }
    return new Hamt(this.store, next.root, this.derived())
  }

  /** Persist any pending node writes. Returns a fresh `Hamt`. */
  async flush(): Promise<Hamt> {
    if (this.pending.size > 0) {
      await this.store.setMany(this.pending)
    }
    return new Hamt(this.store, this.root, this.derived())
  }

  // ---------- Insert ----------

  private async insert(
    rootHash: string,
    key: string,
    value: Uint8Array,
    pending: Map<string, Uint8Array>,
  ): Promise<string> {
    const empty = await emptyHash()
    if (rootHash === empty) {
      return this.storeLeaf({ [key]: encodeValue(value) }, pending)
    }
    const kh = await keyHash(key)
    return this.insertAt(rootHash, 0, kh, key, value, pending)
  }

  private async insertAt(
    nodeHash: string,
    depth: number,
    kh: string,
    key: string,
    value: Uint8Array,
    pending: Map<string, Uint8Array>,
  ): Promise<string> {
    const node = await this.load(nodeHash, pending)
    if (node === null) {
      // Dangling reference — treat as missing and materialize a leaf.
      return this.storeLeaf({ [key]: encodeValue(value) }, pending)
    }

    if (node.kind === 'leaf') {
      const encoded = encodeValue(value)
      const existing = itemOf(node.items, key)
      if (existing === encoded) return nodeHash // no-op
      const newItems: Record<string, string> = { ...node.items, [key]: encoded }
      if (Object.keys(newItems).length <= this.bucketMax) {
        return this.storeLeaf(newItems, pending)
      }
      return this.splitLeaf(newItems, depth, pending)
    }

    // Branch
    const chunk = kh[depth] as string
    const existingChildren = node.children
    const existingChildHash = existingChildren[chunk]
    if (existingChildHash !== undefined) {
      const newChildHash = await this.insertAt(
        existingChildHash,
        depth + 1,
        kh,
        key,
        value,
        pending,
      )
      if (newChildHash === existingChildHash) return nodeHash
      const newChildren = { ...existingChildren, [chunk]: newChildHash }
      return this.storeBranch(newChildren, pending)
    }
    const newLeafHash = await this.storeLeaf({ [key]: encodeValue(value) }, pending)
    const newChildren = { ...existingChildren, [chunk]: newLeafHash }
    return this.storeBranch(newChildren, pending)
  }

  /** Convert an overflowing leaf at `depth` into a branch. */
  private async splitLeaf(
    encodedItems: Record<string, string>,
    depth: number,
    pending: Map<string, Uint8Array>,
  ): Promise<string> {
    if (depth >= HASH_LEN) {
      // Hash exhausted — full SHA-256 collision. Astronomically rare;
      // keep them in one (over-sized) leaf to avoid recursing forever.
      return this.storeLeaf(encodedItems, pending)
    }

    const groups = new Map<string, Record<string, string>>()
    for (const [k, v] of Object.entries(encodedItems)) {
      const nibble = (await keyHash(k))[depth] as string
      let group = groups.get(nibble)
      if (group === undefined) {
        group = dict()
        groups.set(nibble, group)
      }
      group[k] = v
    }

    if (groups.size === 1) {
      // All entries share the next nibble too — recurse deeper, then
      // wrap in a single-child branch at this depth.
      const [nibble, groupItems] = groups.entries().next().value as [string, Record<string, string>]
      const childHash = await this.splitLeaf(groupItems, depth + 1, pending)
      return this.storeBranch({ [nibble]: childHash }, pending)
    }

    const children: Record<string, string> = {}
    for (const [nibble, groupItems] of groups) {
      if (Object.keys(groupItems).length <= this.bucketMax) {
        children[nibble] = await this.storeLeaf(groupItems, pending)
      } else {
        children[nibble] = await this.splitLeaf(groupItems, depth + 1, pending)
      }
    }
    return this.storeBranch(children, pending)
  }

  // ---------- Delete ----------

  private async delete(
    rootHash: string,
    key: string,
    pending: Map<string, Uint8Array>,
  ): Promise<string> {
    const empty = await emptyHash()
    if (rootHash === empty) return empty
    const kh = await keyHash(key)
    const result = await this.deleteAt(rootHash, 0, kh, key, pending)
    return result === null ? empty : result
  }

  /** Returns the new node hash, or null if the subtree is now empty. */
  private async deleteAt(
    nodeHash: string,
    depth: number,
    kh: string,
    key: string,
    pending: Map<string, Uint8Array>,
  ): Promise<string | null> {
    const node = await this.load(nodeHash, pending)
    if (node === null) return nodeHash

    if (node.kind === 'leaf') {
      if (!Object.hasOwn(node.items, key)) return nodeHash
      const newItems = dict()
      for (const [k, v] of Object.entries(node.items)) {
        if (k !== key) newItems[k] = v
      }
      if (Object.keys(newItems).length === 0) return null
      return this.storeLeaf(newItems, pending)
    }

    // Branch
    const chunk = kh[depth] as string
    const existingChildren = node.children
    const existingChildHash = existingChildren[chunk]
    if (existingChildHash === undefined) return nodeHash

    const newChildHash = await this.deleteAt(existingChildHash, depth + 1, kh, key, pending)
    if (newChildHash === existingChildHash) return nodeHash

    const newChildren: Record<string, string> = { ...existingChildren }
    if (newChildHash === null) {
      delete newChildren[chunk]
    } else {
      newChildren[chunk] = newChildHash
    }

    if (Object.keys(newChildren).length === 0) return null

    // Canonicalization: if all children are leaves and their combined
    // entries fit in a single bucket, collapse the whole branch into one
    // leaf. This preserves the invariant that the same logical contents
    // always produce the same root hash.
    const collapsed = await this.tryCollapse(newChildren, pending)
    if (collapsed !== null) return collapsed

    return this.storeBranch(newChildren, pending)
  }

  /**
   * If every child is a leaf and the union of their entries fits in
   * `bucketMax`, return the merged leaf hash. Otherwise null.
   */
  private async tryCollapse(
    children: Record<string, string>,
    pending: Map<string, Uint8Array>,
  ): Promise<string | null> {
    const merged = dict()
    let count = 0
    for (const childHash of Object.values(children)) {
      const child = await this.load(childHash, pending)
      if (child === null || child.kind !== 'leaf') return null
      for (const [k, v] of Object.entries(child.items)) {
        if (!(k in merged)) {
          merged[k] = v
          count++
        }
        if (count > this.bucketMax) return null
      }
    }
    return this.storeLeaf(merged, pending)
  }

  // ---------- Pending management ----------

  /**
   * Walk from `root`, returning only pending entries actually reachable.
   * Drops orphans created by superseded inserts.
   */
  private async filterPending(
    root: string,
    pending: Map<string, Uint8Array>,
  ): Promise<Map<string, Uint8Array>> {
    const empty = await emptyHash()
    const result = new Map<string, Uint8Array>()
    if (root === empty) return result
    const queue = [root]
    while (queue.length > 0) {
      const h = queue.pop() as string
      const prefixed = this.prefix + h
      if (result.has(prefixed) || !pending.has(prefixed)) continue
      const bytes = pending.get(prefixed) as Uint8Array
      result.set(prefixed, bytes)
      const node = parseNode(bytes)
      if (node.kind === 'branch') {
        queue.push(...Object.values(node.children))
      }
    }
    return result
  }

  // ---------- Structural ops ----------

  /**
   * Yield every node hash reachable from this root. Used by GC layers
   * to mark live nodes. Includes pending nodes — works on a Hamt that
   * hasn't been flushed.
   */
  async *reachableNodes(): AsyncIterable<string> {
    const empty = await emptyHash()
    if (this.root === empty) return
    const seen = new Set<string>()
    const queue = [this.root]
    while (queue.length > 0) {
      const h = queue.pop() as string
      if (seen.has(h)) continue
      seen.add(h)
      yield h
      const node = await this.load(h)
      if (node === null) continue
      if (node.kind === 'branch') {
        queue.push(...Object.values(node.children))
      }
    }
  }

  /**
   * Structural diff against `other`. Cost is O(changes + log N), not
   * O(N), because identical subtrees (same hash) are skipped wholesale.
   * The primary payoff of structural sharing.
   */
  async diff(other: Hamt): Promise<HamtDiff> {
    const added = new Map<string, Uint8Array>()
    const removed = new Map<string, Uint8Array>()
    const modified = new Map<string, readonly [Uint8Array, Uint8Array]>()
    await this.diffWalk(this.root, other.root, other, added, removed, modified)
    return { added, removed, modified }
  }

  private async diffWalk(
    aHash: string,
    bHash: string,
    other: Hamt,
    added: Map<string, Uint8Array>,
    removed: Map<string, Uint8Array>,
    modified: Map<string, readonly [Uint8Array, Uint8Array]>,
  ): Promise<void> {
    if (aHash === bHash) return // identical subtrees

    const empty = await emptyHash()

    if (aHash === empty) {
      for await (const [k, v] of other.itemsFrom(bHash)) added.set(k, v)
      return
    }
    if (bHash === empty) {
      for await (const [k, v] of this.itemsFrom(aHash)) removed.set(k, v)
      return
    }

    const aNode = await this.load(aHash)
    const bNode = await other.load(bHash)
    if (aNode === null || bNode === null) {
      // Missing node — fall back to full walk for whichever side is intact.
      if (aNode !== null) {
        for await (const [k, v] of this.itemsFrom(aHash)) removed.set(k, v)
      }
      if (bNode !== null) {
        for await (const [k, v] of other.itemsFrom(bHash)) added.set(k, v)
      }
      return
    }

    if (aNode.kind === 'leaf' && bNode.kind === 'leaf') {
      const aItems = aNode.items
      const bItems = bNode.items
      for (const [k, v] of Object.entries(aItems)) {
        const bv = itemOf(bItems, k)
        if (bv === undefined) {
          removed.set(k, decodeValue(v))
        } else if (bv !== v) {
          modified.set(k, [decodeValue(v), decodeValue(bv)] as const)
        }
      }
      for (const [k, v] of Object.entries(bItems)) {
        if (itemOf(aItems, k) === undefined) added.set(k, decodeValue(v))
      }
      return
    }

    if (aNode.kind === 'branch' && bNode.kind === 'branch') {
      const chunks = new Set([...Object.keys(aNode.children), ...Object.keys(bNode.children)])
      for (const chunk of chunks) {
        const aChild = aNode.children[chunk] ?? empty
        const bChild = bNode.children[chunk] ?? empty
        await this.diffWalk(aChild, bChild, other, added, removed, modified)
      }
      return
    }

    // Mixed kinds. Walk both fully and reconcile.
    const aItems = new Map<string, Uint8Array>()
    const bItems = new Map<string, Uint8Array>()
    for await (const [k, v] of this.itemsFrom(aHash)) aItems.set(k, v)
    for await (const [k, v] of other.itemsFrom(bHash)) bItems.set(k, v)
    for (const [k, v] of aItems) {
      const bv = bItems.get(k)
      if (bv === undefined) {
        removed.set(k, v)
      } else if (!bytesEqual(v, bv)) {
        modified.set(k, [v, bv] as const)
      }
    }
    for (const [k, v] of bItems) {
      if (!aItems.has(k)) added.set(k, v)
    }
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a === b) return true
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}
