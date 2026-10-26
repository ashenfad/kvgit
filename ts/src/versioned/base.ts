/**
 * Shared commit / merge orchestration for versioned stores.
 *
 * `VersionedBase` provides:
 * - The `commit()` flow: fast-forward + CAS, three-way merge fallback
 *   on HEAD divergence, snapshot/restore on failed CAS.
 * - Merging another head (`mergeHeads`) and applying one change
 *   (`applyChange`, under cherry-pick and revert).
 * - History walking, key-level diff, parent lookup, merge registry.
 *
 * Subclasses provide storage-specific operations (CAS, commit creation,
 * blob reading, LCA finding). Anything that touches storage layout is
 * abstract; everything that orchestrates the protocol is concrete.
 */

import type {
  ApplyChangeOptions,
  CommitInfo,
  ConflictDisposition,
  DiffResult,
  MergeHeadsOptions,
  MergePolicy,
  MergeResult,
  MovedDisposition,
  Versioned,
  VersionedCommitOptions,
} from '../types'
import { walkHistory } from './helpers'
import type { Change } from './merge'

/**
 * Abstract base for versioned KV stores.
 *
 * Manages the in-memory snapshot of the current commit's flat keyset
 * and orchestrates `commit()`. Subclasses fill in the abstract methods
 * to bind the orchestration to specific storage.
 */
export abstract class VersionedBase implements Versioned {
  protected branch: string
  protected currentCommitHash: string
  protected baseCommitHash: string
  protected commitKeys: Map<string, string>
  protected mergeFns: Map<string, MergePolicy>
  protected mergePrefixes: Map<string, MergePolicy>
  protected defaultMergeFn: MergePolicy | null
  lastMergeResult: MergeResult | null

  /** Cached on first access; the root commit walking back from HEAD. */
  private cachedInitialCommit: string | null = null
  /** Iterations still reading the current `commitKeys` map. A change
   *  made while one runs goes to a copy, so each iteration sees one
   *  commit's keys from start to finish. */
  private keyReaders = 0
  /** Settles when the latest state-changing call on this handle does. */
  private opTail: Promise<void> = Promise.resolve()

  protected constructor(opts: { branch: string; commitHash: string }) {
    this.branch = opts.branch
    this.currentCommitHash = opts.commitHash
    this.baseCommitHash = opts.commitHash
    this.commitKeys = new Map()
    this.mergeFns = new Map()
    this.mergePrefixes = new Map()
    this.defaultMergeFn = null
    this.lastMergeResult = null
  }

  // --- Properties ---

  get currentCommit(): string {
    return this.currentCommitHash
  }

  get baseCommit(): string {
    return this.baseCommitHash
  }

  get currentBranch(): string {
    return this.branch
  }

  get initialCommit(): string {
    if (this.cachedInitialCommit !== null) return this.cachedInitialCommit
    // Synchronous accessor over an async walk: we kick off a sync best-
    // effort walk by re-using the in-memory state. The first call after
    // construction is async-only, so callers should `await initial()`
    // when they need it. The property here is a convenience for the
    // already-resolved case.
    throw new Error('initialCommit not yet resolved; call await initial() first')
  }

  /** Resolve the root commit by walking the parent chain. Caches the result. */
  async initial(): Promise<string> {
    if (this.cachedInitialCommit !== null) return this.cachedInitialCommit
    let last = this.currentCommitHash
    for await (const c of this.history()) last = c
    this.cachedInitialCommit = last
    return last
  }

  // --- Reads (in-memory keyset) ---

  async get(key: string): Promise<Uint8Array | null> {
    const blob = this.commitKeys.get(key)
    if (blob === undefined) return null
    return this.readBlob(blob)
  }

  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const out = new Map<string, Uint8Array>()
    for (const key of keys) {
      const v = await this.get(key)
      if (v !== null) out.set(key, v)
    }
    return out
  }

  async has(key: string): Promise<boolean> {
    return this.commitKeys.has(key)
  }

  async *keys(): AsyncIterable<string> {
    const map = this.commitKeys
    this.keyReaders++
    try {
      for (const k of map.keys()) yield k
    } finally {
      if (this.commitKeys === map) this.keyReaders--
    }
  }

  /**
   * `commitKeys`, ready to change in place. A commit changes the map
   * rather than rebuilding it, so its cost follows the size of the
   * change; when an iteration is reading the map, the change goes to a
   * copy instead and the iteration keeps the map it started on.
   */
  protected ownedKeys(): Map<string, string> {
    if (this.keyReaders > 0) {
      this.commitKeys = new Map(this.commitKeys)
      this.keyReaders = 0
    }
    return this.commitKeys
  }

  /** Replace `commitKeys` wholesale, as when loading another commit. */
  protected replaceKeys(map: Map<string, string>): void {
    this.commitKeys = map
    this.keyReaders = 0
  }

  // --- Merge registry ---

  /** Register a merge function, or a `MergeChoice`, for one key. */
  setMergeFn(key: string, fn: MergePolicy): void {
    this.mergeFns.set(key, fn)
  }

  /**
   * Register a merge function, or a `MergeChoice`, under a key prefix,
   * for keys whose names aren't known up front (`runs/` for
   * `runs/<id>`). A key takes the most specific registration: its exact
   * key, else the longest prefix it starts with, else the default.
   *
   * A merge function is consulted only where both sides changed a key.
   * A `MergeChoice` is a standing policy over the whole prefix: it gives
   * that side every key either side changed under it.
   */
  setMergePrefix(prefix: string, fn: MergePolicy): void {
    this.mergePrefixes.set(prefix, fn)
  }

  /** Register the merge function, or `MergeChoice`, for keys no other
   *  registration covers. */
  setDefaultMerge(fn: MergePolicy): void {
    this.defaultMergeFn = fn
  }

  // --- History and diff ---

  /** Keys added, removed and modified going from `commitA` to
   *  `commitB`. */
  async diff(commitA: string, commitB: string): Promise<DiffResult> {
    const added = new Set<string>()
    const removed = new Set<string>()
    const modified = new Set<string>()
    for (const [key, change] of await this.changes(commitA, commitB)) {
      if (change.old === null) added.add(key)
      else if (change.new === null) removed.add(key)
      else modified.add(key)
    }
    return { added, removed, modified }
  }

  history(commitHash?: string, opts: { allParents?: boolean } = {}): AsyncIterable<string> {
    const start = commitHash ?? this.currentCommitHash
    return walkHistory(start, (h) => this.loadParents(h), opts)
  }

  async parents(commitHash?: string): Promise<readonly string[]> {
    return this.loadParents(commitHash ?? this.currentCommitHash)
  }

  /**
   * Lowest common ancestor of two commits, or null if unrelated.
   * On a criss-cross tie the smallest hash wins (deterministic but
   * arbitrary) — exactly the base a merge of the two would use.
   */
  async mergeBase(commitA: string, commitB: string): Promise<string | null> {
    return this.findLca(commitA, commitB)
  }

  // --- Commit orchestration ---

  /**
   * Commit changes and advance HEAD.
   *
   * Fast-forwards when HEAD hasn't moved since this handle's base;
   * otherwise three-way merges onto it. A HEAD move landing inside the
   * fast-forward's CAS window goes through that same merge, so a lost
   * race is never mistaken for a conflict.
   *
   * With `onMoved: 'raise'` a moved HEAD throws `BranchMovedError`
   * instead of merging, whether it moved before the commit started or
   * inside the CAS window: the only commit that can land is the
   * fast-forward, published by the one CAS that expects HEAD at the
   * base. A commit that loses withdraws its in-flight marker, leaving
   * nothing reachable.
   */
  async commit(opts: VersionedCommitOptions = {}): Promise<MergeResult> {
    return this.exclusive(() => this.commitOnce(opts))
  }

  /** One commit, run once every earlier state-changing call has
   *  settled. */
  protected abstract commitOnce(opts: VersionedCommitOptions): Promise<MergeResult>

  /**
   * Merge another head (usually another branch's) into this branch.
   *
   * A head our history already contains merges as a no-op, provided the
   * branch is still at our head. When ours is an ancestor of theirs,
   * the branch fast-forwards: HEAD moves to `theirHead` and no commit is
   * written, so `info` isn't recorded, unless `fastForward: false`.
   * Otherwise the lowest common ancestor, a three-way resolve, and a
   * two-parent merge commit `[ours, theirs]`, CAS-guarded on our head
   * (a race throws `ConcurrencyError` and changes nothing). No common
   * ancestor throws `ConcurrencyError` likewise.
   */
  async mergeHeads(theirHead: string, opts: MergeHeadsOptions = {}): Promise<MergeResult> {
    return this.exclusive(() => this.mergeOnce(theirHead, null, opts))
  }

  /**
   * Apply the change from `base` to `target` as one commit: a three-way
   * merge with `base` in place of the common ancestor. What `target`
   * changed relative to `base` lands on our head, our own changes since
   * are kept, and conflicts resolve (or throw) as in a merge. The result
   * is an ordinary single-parent commit, CAS-guarded on our head; a
   * change already present commits nothing (`no_op`). A cherry-pick is
   * `applyChange(parent, commit)` and a revert
   * `applyChange(commit, parent)`.
   */
  async applyChange(
    base: string,
    target: string,
    opts: ApplyChangeOptions = {},
  ): Promise<MergeResult> {
    return this.exclusive(() => this.mergeOnce(target, base, opts))
  }

  /** A merge of `theirs`, or with `from` an apply of the change from
   *  it, run once every earlier state-changing call has settled. */
  protected abstract mergeOnce(
    theirs: string,
    from: string | null,
    opts: MergeHeadsOptions,
  ): Promise<MergeResult>

  /**
   * Run `op` once every earlier state-changing call on this handle has
   * settled, so two never interleave: each builds on the state the
   * last one left.
   */
  protected async exclusive<T>(op: () => Promise<T>): Promise<T> {
    const prior = this.opTail
    let release = (): void => {}
    this.opTail = new Promise<void>((resolve) => {
      release = resolve
    })
    await prior
    try {
      return await op()
    } finally {
      release()
    }
  }

  protected finish(result: MergeResult): MergeResult {
    this.lastMergeResult = result
    return result
  }

  // --- Abstract methods (concrete subclasses provide) ---

  abstract latestHead(): Promise<string | null>
  abstract peek(key: string, opts: { branch: string }): Promise<Uint8Array | null>
  abstract refresh(): Promise<void>
  abstract checkout(commitHash: string, opts?: { branch?: string }): Promise<Versioned | null>
  abstract createBranch(name: string, opts?: { at?: string }): Promise<Versioned>
  abstract deleteBranch(name: string): Promise<void>
  abstract switchBranch(name: string): Promise<void>
  abstract resetTo(commitHash: string): Promise<boolean>
  abstract listBranches(): Promise<string[]>
  abstract commitInfo(commitHash?: string): Promise<CommitInfo | null>
  abstract branchExists(name: string): Promise<boolean>

  /** Each key whose value differs from `commitA` to `commitB`; a null
   *  or missing `commitA` reads as empty. */
  protected abstract changes(commitA: string | null, commitB: string): Promise<Map<string, Change>>
  protected abstract loadParents(commitHash: string): Promise<readonly string[]>
  protected abstract findLca(commitA: string, commitB: string): Promise<string | null>
  protected abstract readBlob(blobId: string): Promise<Uint8Array | null>
}

export function checkDisposition(value: ConflictDisposition | undefined): ConflictDisposition {
  const onConflict = value ?? 'raise'
  if (onConflict !== 'raise' && onConflict !== 'abandon') {
    throw new TypeError(`onConflict must be 'raise' or 'abandon', got ${String(onConflict)}`)
  }
  return onConflict
}

export function checkMoved(value: MovedDisposition | undefined): MovedDisposition {
  const onMoved = value ?? 'merge'
  if (onMoved !== 'merge' && onMoved !== 'raise') {
    throw new TypeError(`onMoved must be 'merge' or 'raise', got ${String(onMoved)}`)
  }
  return onMoved
}

export function noOp(commit: string): MergeResult {
  return { merged: true, commit, strategy: 'no_op', autoMergedKeys: [], carriedKeys: [] }
}

export function abandoned(strategy: MergeResult['strategy']): MergeResult {
  return { merged: false, commit: null, strategy, autoMergedKeys: [], carriedKeys: [] }
}

export function landed(
  commit: string,
  strategy: MergeResult['strategy'],
  autoMergedKeys: readonly string[],
  carriedKeys: readonly string[],
): MergeResult {
  return {
    merged: true,
    commit,
    strategy,
    autoMergedKeys: [...autoMergedKeys],
    carriedKeys: [...carriedKeys],
  }
}
