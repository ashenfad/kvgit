/**
 * Staged: a buffered, Map-shaped layer over a `Versioned`.
 *
 * Writes (`set` / `delete`) accumulate in memory; nothing reaches the
 * underlying store until `commit()` flushes them as a single atomic
 * commit (with optional three-way merge if HEAD has moved).
 *
 * Reads check the staging buffer first, then a per-instance read cache,
 * then the underlying store. Decoded values are cached so repeat reads
 * don't re-decode.
 *
 * The encoder/decoder convert user values to/from bytes for storage.
 * The default is JSON over UTF-8; pass custom codecs for richer types
 * (cbor for Map/Set/Date/typed arrays, msgpack for compactness, etc.).
 */

import {
  type BytesMergeFn,
  type CommitInfo,
  type ConflictDisposition,
  type Decoder,
  type Encoder,
  MergeChoice,
  type MergeFn,
  type MergeResult,
  type Versioned,
} from './types'

const _encoder = new TextEncoder()
const _decoder = new TextDecoder()

/** Default encoder: JSON via UTF-8. JSON-serializable values only. */
export const jsonEncoder: Encoder = (value) => _encoder.encode(JSON.stringify(value))

/** Default decoder: JSON via UTF-8. Mirrors `jsonEncoder`. */
export const jsonDecoder: Decoder = (bytes) => JSON.parse(_decoder.decode(bytes))

export interface StagedOptions {
  encoder?: Encoder
  decoder?: Decoder
}

export interface StagedCommitOptions {
  /** If provided, only these keys are flushed; others remain staged. */
  keys?: Set<string>
  onConflict?: ConflictDisposition
  /** Per-key merge fns added on top of the registered ones for this commit. */
  mergeFns?: Map<string, MergeFn>
  defaultMerge?: MergeFn
  info?: CommitInfo
}

/** Options for `Staged.mergeHeads()`. */
export interface StagedMergeHeadsOptions {
  onConflict?: ConflictDisposition
  /** Per-key merge fns added on top of the registered ones for this merge. */
  mergeFns?: Map<string, MergeFn>
  defaultMerge?: MergeFn
  info?: CommitInfo
}

/**
 * Buffered writes over a `Versioned`. Implements a Map-shaped surface;
 * staged changes flush atomically via `commit()`.
 *
 * Per-call generics on `get` / `set` give the call site typed access
 * (`staged.get<Model>('model')`); without a generic the value type is
 * `unknown` and the caller narrows.
 */
export class Staged {
  readonly versioned: Versioned
  private readonly encoder: Encoder
  private readonly decoder: Decoder

  // Buffered state. `_updates` holds new/replaced values; `_removals`
  // holds keys to delete. A key in `_updates` overrides any prior
  // removal and vice versa.
  private updates = new Map<string, unknown>()
  private removals = new Set<string>()
  private cache = new Map<string, unknown>()

  // User-level merge fns (decoded values cross the boundary).
  private userMergeFns = new Map<string, MergeFn>()
  private userDefaultMerge: MergeFn | null = null

  constructor(versioned: Versioned, opts: StagedOptions = {}) {
    this.versioned = versioned
    this.encoder = opts.encoder ?? jsonEncoder
    this.decoder = opts.decoder ?? jsonDecoder
  }

  // --- Versioned pass-through (read-only) ---

  get currentCommit(): string {
    return this.versioned.currentCommit
  }

  get baseCommit(): string {
    return this.versioned.baseCommit
  }

  get currentBranch(): string {
    return this.versioned.currentBranch
  }

  get initialCommit(): string {
    return this.versioned.initialCommit
  }

  get lastMergeResult(): MergeResult | null {
    return this.versioned.lastMergeResult
  }

  // --- Reads ---

  async get<T = unknown>(key: string): Promise<T | undefined> {
    if (this.removals.has(key)) return undefined
    if (this.updates.has(key)) return this.updates.get(key) as T
    if (this.cache.has(key)) return this.cache.get(key) as T
    const raw = await this.versioned.get(key)
    if (raw === null) return undefined
    const value = this.decoder(raw) as T
    this.cache.set(key, value)
    return value
  }

  async has(key: string): Promise<boolean> {
    if (this.removals.has(key)) return false
    if (this.updates.has(key)) return true
    return this.versioned.has(key)
  }

  async *keys(): AsyncIterable<string> {
    const seen = new Set<string>()
    for await (const k of this.versioned.keys()) {
      if (this.removals.has(k)) continue
      seen.add(k)
      yield k
    }
    for (const k of this.updates.keys()) {
      if (!seen.has(k)) yield k
    }
  }

  // --- Writes (in-memory buffer; no IO) ---

  set<T = unknown>(key: string, value: T): void {
    this.removals.delete(key)
    this.updates.set(key, value)
  }

  delete(key: string): void {
    this.updates.delete(key)
    this.removals.add(key)
  }

  /** Whether there are any staged changes. */
  get hasChanges(): boolean {
    return this.updates.size > 0 || this.removals.size > 0
  }

  /** Whether a specific key has a pending update or removal. */
  isStaged(key: string): boolean {
    return this.updates.has(key) || this.removals.has(key)
  }

  /** Discard all staged changes and the read cache. */
  reset(): void {
    this.updates.clear()
    this.removals.clear()
    this.cache.clear()
  }

  // --- Navigation + inspection (canonical wrappers around `Versioned`) ---
  //
  // The full Versioned navigation surface is mirrored on `Staged` so
  // callers don't need to reach through `staged.versioned.*` for any
  // common operation. Two reasons:
  //
  // 1. Operations that move HEAD (switchBranch / resetTo / refresh)
  //    must clear `Staged`'s read cache, otherwise post-move reads
  //    return stale values. Reaching through `versioned` skips that.
  // 2. Operations that fork a new HEAD (createBranch / checkout)
  //    return a new `Versioned`. Wrapping it in `Staged` here keeps
  //    the encoder/decoder aligned — callers stay in `Staged`-land
  //    instead of constructing fresh `Staged`s by hand.
  //
  // Same shape as kvgit-py's `Staged` API. The `versioned` property
  // remains exposed for raw bytes-level access, but for branch / commit
  // navigation use these wrappers.

  /**
   * Switch to a different branch in-place. **Discards staged changes**
   * — `updates`, `removals`, and the read cache are all cleared.
   *
   * Carrying uncommitted writes across a branch switch is a three-way-
   * merge problem in disguise; the kvgit contract is to drop them
   * rather than silently fold them into the new branch.
   */
  async switchBranch(name: string): Promise<void> {
    await this.versioned.switchBranch(name)
    this.updates.clear()
    this.removals.clear()
    this.cache.clear()
  }

  /**
   * Reset HEAD to `commitHash` and discard staged changes.
   *
   * Returns `true` if the commit exists and the reset landed; `false`
   * leaves staged state untouched. Mirrors kvgit-py's `reset_to` —
   * cleanup only fires on success so a failed reset (unknown hash)
   * doesn't silently throw away the caller's work.
   */
  async resetTo(commitHash: string): Promise<boolean> {
    const ok = await this.versioned.resetTo(commitHash)
    if (ok) {
      this.updates.clear()
      this.removals.clear()
      this.cache.clear()
    }
    return ok
  }

  /**
   * Reload from HEAD (picks up writes from other producers on the
   * same branch). **Discards staged changes** — same reasoning as
   * `switchBranch`: a refresh that landed concurrent commits can
   * leave staged work unable to merge cleanly.
   */
  async refresh(): Promise<void> {
    await this.versioned.refresh()
    this.updates.clear()
    this.removals.clear()
    this.cache.clear()
  }

  /**
   * Fork a new branch off `at` (defaults to current HEAD). Returns a
   * fresh `Staged` wrapping the new branch's `Versioned`, with the
   * same encoder/decoder as this one. User merge fns are NOT
   * propagated — register them on the returned instance if needed.
   */
  async createBranch(name: string, opts: { at?: string } = {}): Promise<Staged> {
    const v = await this.versioned.createBranch(name, opts)
    return new Staged(v, { encoder: this.encoder, decoder: this.decoder })
  }

  /**
   * Open a `Staged` view at a specific commit (read-only timeline
   * navigation). Returns `null` if the commit doesn't exist. Optional
   * `branch` follows the underlying `Versioned.checkout` semantics.
   */
  async checkout(commitHash: string, opts: { branch?: string } = {}): Promise<Staged | null> {
    const v = await this.versioned.checkout(commitHash, opts)
    if (v === null) return null
    return new Staged(v, { encoder: this.encoder, decoder: this.decoder })
  }

  /** List all branch names in the underlying store. */
  async listBranches(): Promise<string[]> {
    return this.versioned.listBranches()
  }

  /** Lowest common ancestor of two commits, or null if unrelated. */
  async mergeBase(commitA: string, commitB: string): Promise<string | null> {
    return this.versioned.mergeBase(commitA, commitB)
  }

  /** Whether a branch exists in the underlying store. Never writes. */
  async branchExists(name: string): Promise<boolean> {
    return this.versioned.branchExists(name)
  }

  /**
   * Delete a branch by name. Cannot delete the current branch — the
   * underlying `Versioned` enforces this and throws.
   */
  async deleteBranch(name: string): Promise<void> {
    return this.versioned.deleteBranch(name)
  }

  /**
   * Read a key from another branch's HEAD without switching to it.
   * Returns the decoded value, or `undefined` if the key is absent.
   *
   * Doesn't touch the read cache (the cache is keyed by *this* branch).
   */
  async peek<T = unknown>(key: string, opts: { branch: string }): Promise<T | undefined> {
    const raw = await this.versioned.peek(key, opts)
    if (raw === null) return undefined
    return this.decoder(raw) as T
  }

  /**
   * Walk the commit chain from `commitHash` (or current HEAD) backward
   * through history. With `allParents: true`, also walks merge
   * second-parents. Pure pass-through to the underlying `Versioned`.
   */
  history(commitHash?: string, opts: { allParents?: boolean } = {}): AsyncIterable<string> {
    return this.versioned.history(commitHash, opts)
  }

  // --- Merge fn registry (user-level: decoded values) ---

  setMergeFn<T = unknown>(key: string, fn: MergeFn<T>): void {
    this.userMergeFns.set(key, fn as MergeFn)
  }

  setDefaultMerge<T = unknown>(fn: MergeFn<T>): void {
    this.userDefaultMerge = fn as MergeFn
  }

  // --- Commit ---

  async commit(opts: StagedCommitOptions = {}): Promise<MergeResult> {
    const { keys: filterKeys } = opts

    // Encode the staged updates targeted by this commit.
    let encodedUpdates: Map<string, Uint8Array> | null = null
    let effectiveRemovals: Set<string> | null = null

    if (filterKeys !== undefined) {
      // Targeted commit: only flush keys in the filter that are also staged.
      const matchedUpdates = new Map<string, unknown>()
      for (const k of filterKeys) {
        if (this.updates.has(k)) matchedUpdates.set(k, this.updates.get(k))
      }
      if (matchedUpdates.size > 0) {
        encodedUpdates = new Map()
        for (const [k, v] of matchedUpdates) {
          encodedUpdates.set(k, this.encoder(v))
        }
      }
      const matchedRemovals = new Set<string>()
      for (const k of filterKeys) {
        if (this.removals.has(k)) matchedRemovals.add(k)
      }
      if (matchedRemovals.size > 0) effectiveRemovals = matchedRemovals
    } else {
      if (this.updates.size > 0) {
        encodedUpdates = new Map()
        for (const [k, v] of this.updates) {
          encodedUpdates.set(k, this.encoder(v))
        }
      }
      if (this.removals.size > 0) effectiveRemovals = new Set(this.removals)
    }

    // Build effective merge fns and wrap user-level → bytes-level.
    const effectiveFns = new Map(this.userMergeFns)
    if (opts.mergeFns) {
      for (const [k, fn] of opts.mergeFns) effectiveFns.set(k, fn)
    }
    const effectiveDefault = opts.defaultMerge ?? this.userDefaultMerge

    let bytesMergeFns: Map<string, BytesMergeFn> | null = null
    if (effectiveFns.size > 0) {
      bytesMergeFns = new Map()
      for (const [k, fn] of effectiveFns) {
        bytesMergeFns.set(k, this.wrapMergeFn(fn))
      }
    }
    const bytesDefault: BytesMergeFn | null =
      effectiveDefault !== null && effectiveDefault !== undefined
        ? this.wrapMergeFn(effectiveDefault)
        : null

    const result = await this.versioned.commit({
      ...(encodedUpdates !== null && { updates: encodedUpdates }),
      ...(effectiveRemovals !== null && { removals: effectiveRemovals }),
      ...(opts.onConflict !== undefined && { onConflict: opts.onConflict }),
      ...(bytesMergeFns !== null && { mergeFns: bytesMergeFns }),
      ...(bytesDefault !== null && { defaultMerge: bytesDefault }),
      ...(opts.info !== undefined && { info: opts.info }),
    })

    if (result.merged) {
      if (filterKeys !== undefined) {
        for (const k of filterKeys) {
          this.updates.delete(k)
          this.removals.delete(k)
        }
      } else {
        this.updates.clear()
        this.removals.clear()
      }
      // The full read cache must be cleared on any successful merge:
      // a three-way merge may have introduced changes from the other
      // side under keys we haven't touched, leaving cached entries
      // stale.
      this.cache.clear()
    }
    return result
  }

  /**
   * Merge another head (usually another branch's HEAD) into this branch.
   *
   * Lowest common ancestor + three-way resolve + a two-parent merge
   * commit, CAS-guarded on our own head. Refuses when the staging
   * buffer holds uncommitted changes — commit or reset first, so the
   * merge reads committed heads on both sides.
   */
  async mergeHeads(theirHead: string, opts: StagedMergeHeadsOptions = {}): Promise<MergeResult> {
    if (this.updates.size > 0 || this.removals.size > 0) {
      throw new Error('cannot merge with staged changes; commit or reset first')
    }
    // Same assembly as commit(): registered fns first, per-call
    // overrides on top.
    const effectiveFns = new Map(this.userMergeFns)
    if (opts.mergeFns) {
      for (const [k, fn] of opts.mergeFns) effectiveFns.set(k, fn)
    }
    const effectiveDefault = opts.defaultMerge ?? this.userDefaultMerge

    let bytesMergeFns: Map<string, BytesMergeFn> | null = null
    if (effectiveFns.size > 0) {
      bytesMergeFns = new Map()
      for (const [k, fn] of effectiveFns) {
        bytesMergeFns.set(k, this.wrapMergeFn(fn))
      }
    }
    const bytesDefault: BytesMergeFn | null =
      effectiveDefault !== null && effectiveDefault !== undefined
        ? this.wrapMergeFn(effectiveDefault)
        : null

    const result = await this.versioned.mergeHeads(theirHead, {
      ...(opts.onConflict !== undefined && { onConflict: opts.onConflict }),
      ...(bytesMergeFns !== null && { mergeFns: bytesMergeFns }),
      ...(bytesDefault !== null && { defaultMerge: bytesDefault }),
      ...(opts.info !== undefined && { info: opts.info }),
    })
    if (result.merged) {
      // HEAD moved: cached values may be stale after a merge.
      this.cache.clear()
    }
    return result
  }

  /**
   * Wrap a user-level merge fn (decoded values) into a bytes-level fn
   * the `Versioned` layer can call. Encodes the merge result with the
   * configured encoder.
   */
  private wrapMergeFn(fn: MergeFn): BytesMergeFn {
    const encoder = this.encoder
    const decoder = this.decoder
    return (oldB, oursB, theirsB) => {
      const oldV = oldB !== null ? decoder(oldB) : null
      const ours = oursB !== null ? decoder(oursB) : null
      const theirs = theirsB !== null ? decoder(theirsB) : null
      const merged = fn(oldV, ours, theirs)
      return merged instanceof MergeChoice ? merged : encoder(merged)
    }
  }
}
