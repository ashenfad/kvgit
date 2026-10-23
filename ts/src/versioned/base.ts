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

import {
  type ApplyChangeOptions,
  BranchMovedError,
  type CommitInfo,
  ConcurrencyError,
  type ConflictDisposition,
  type DiffResult,
  MergeConflict,
  type MergeHeadsOptions,
  type MergePolicy,
  type MergeResult,
  type MergeRuleOptions,
  type MovedDisposition,
  type PostCheck,
  UnknownBranchError,
  UnknownCommitError,
  type Versioned,
  type VersionedCommitOptions,
} from '../types'
import { walkHistory } from './helpers'
import { type Change, type MergeResolution, changesOurs, resolveMerge } from './merge'

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

  private async commitOnce(opts: VersionedCommitOptions): Promise<MergeResult> {
    const updates = opts.updates ?? null
    const removals = opts.removals ?? null
    const onConflict = checkDisposition(opts.onConflict)
    const onMoved = checkMoved(opts.onMoved)
    const info = opts.info ?? null

    // Nothing to do: report the current HEAD and write nothing. With
    // `onMoved: 'raise'` a moved branch still throws, since the caller's
    // base is out of date whether or not it had anything to write.
    if (
      (updates === null || updates.size === 0) &&
      (removals === null || removals.size === 0) &&
      info === null
    ) {
      if (onMoved === 'raise') {
        const head = await this.latestHead()
        if (head === null) throw new UnknownBranchError(this.branch)
        if (head !== this.baseCommitHash) {
          throw new BranchMovedError(this.branch, this.baseCommitHash, head)
        }
      }
      return this.finish(noOp(this.currentCommitHash))
    }

    let currentHead = await this.latestHead()
    let oursBuilt = false
    let saved: unknown = null

    if (onMoved === 'raise' && currentHead !== this.baseCommitHash) {
      if (currentHead === null) throw new UnknownBranchError(this.branch)
      throw new BranchMovedError(this.branch, this.baseCommitHash, currentHead)
    }

    // Fast-forward path: HEAD hasn't moved.
    if (currentHead === this.baseCommitHash) {
      saved = this.snapshotState()
      await this.createCommit({
        ...(updates !== null && { updates }),
        ...(removals !== null && { removals }),
        ...(info !== null && { info }),
      })
      if (await this.casHead(this.baseCommitHash, this.currentCommitHash)) {
        this.baseCommitHash = this.currentCommitHash
        return this.finish(landed(this.currentCommitHash, 'fast_forward', [], []))
      }
      if (onMoved === 'raise') {
        this.restoreState(saved)
        const head = await this.latestHead()
        if (head === null) throw new UnknownBranchError(this.branch)
        if (head !== this.baseCommitHash) {
          throw new BranchMovedError(this.branch, this.baseCommitHash, head)
        }
        // HEAD reads as the base, yet the publish failed: it moved and
        // came back, or the commit's in-flight marker lapsed and a sweep
        // may have taken the commit.
        throw new ConcurrencyError('Commit could not be published. Retry.')
      }
      // Lost the fast-forward race: another writer advanced HEAD between
      // our read and our CAS. Re-read HEAD and merge, as when the base
      // was already behind, so the caller never has to refresh (and drop
      // pending work) just to replay a mergeable commit.
      try {
        currentHead = await this.latestHead()
      } catch (e) {
        // A failing re-read must not leave phantom state behind either.
        this.restoreState(saved)
        throw e
      }
      // Keep the commit just built as our side of the merge: a rebuild
      // would mint a second commit for the same change (the hash covers
      // the time) and orphan the first.
      oursBuilt = true
    }

    // Three-way merge path: HEAD has moved.
    if (currentHead === null) {
      // A retry that finds its branch gone restores the pre-commit
      // snapshot. A null first read built nothing to restore.
      if (oursBuilt) this.restoreState(saved)
      throw new UnknownBranchError(this.branch)
    }
    if (!oursBuilt) {
      saved = this.snapshotState()
      await this.createCommit({
        ...(updates !== null && { updates }),
        ...(removals !== null && { removals }),
      })
    }
    return this.threeWayMerge(currentHead, {
      onConflict,
      rules: opts,
      ...(info !== null && { info }),
      savedState: saved,
    })
  }

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
    return this.exclusive(() => this.mergeHeadsOnce(theirHead, opts))
  }

  private async mergeHeadsOnce(theirHead: string, opts: MergeHeadsOptions): Promise<MergeResult> {
    const onConflict = checkDisposition(opts.onConflict)
    await this.requireCommit(theirHead)
    const ourHead = this.currentCommitHash
    const lca = await this.findLca(ourHead, theirHead)
    if (lca === theirHead) return this.finish(await this.liveNoOp(ourHead, onConflict))
    if (lca === ourHead && opts.fastForward !== false) {
      return this.finish(await this.fastForward(theirHead, onConflict))
    }
    return this.threeWayMerge(theirHead, {
      onConflict,
      rules: opts,
      ...(opts.info !== null && opts.info !== undefined && { info: opts.info }),
      ...(opts.postCheck !== null && opts.postCheck !== undefined && { postCheck: opts.postCheck }),
      savedState: this.snapshotState(),
      casFrom: ourHead,
      parents: [ourHead, theirHead],
      ancestor: lca,
    })
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
    return this.exclusive(() => this.applyChangeOnce(base, target, opts))
  }

  private async applyChangeOnce(
    base: string,
    target: string,
    opts: ApplyChangeOptions,
  ): Promise<MergeResult> {
    const onConflict = checkDisposition(opts.onConflict)
    // A missing commit would read as an empty tree, and applying "to
    // empty" removes every key.
    await this.requireCommit(base)
    await this.requireCommit(target)
    const ourHead = this.currentCommitHash
    return this.threeWayMerge(target, {
      onConflict,
      rules: opts,
      ...(opts.info !== null && opts.info !== undefined && { info: opts.info }),
      ...(opts.postCheck !== null && opts.postCheck !== undefined && { postCheck: opts.postCheck }),
      savedState: this.snapshotState(),
      casFrom: ourHead,
      parents: [ourHead],
      base,
      strategy: 'apply',
    })
  }

  /**
   * Three-way merge `theirHead` into our current commit.
   *
   * `theirHead` is any commit: the moved HEAD of our own branch (the
   * lost-race path) or another branch's head. `casFrom` names the
   * commit the merge publishes over (default `theirHead`, the lost-race
   * case). `base` replaces the common ancestor, which is how a change
   * is applied rather than a history merged; `ancestor` passes in one
   * already found (null meaning none).
   */
  private async threeWayMerge(
    theirHead: string,
    opts: {
      onConflict: ConflictDisposition
      rules: MergeRuleOptions
      info?: CommitInfo
      postCheck?: PostCheck
      savedState: unknown
      casFrom?: string
      parents?: readonly string[]
      base?: string
      ancestor?: string | null
      strategy?: 'three_way' | 'apply'
    },
  ): Promise<MergeResult> {
    const strategy = opts.strategy ?? 'three_way'
    const lca =
      opts.base !== undefined
        ? opts.base
        : opts.ancestor !== undefined
          ? opts.ancestor
          : await this.findLca(this.currentCommitHash, theirHead)
    if (lca === null) {
      this.restoreState(opts.savedState)
      if (opts.onConflict === 'abandon') return this.finish(abandoned(strategy))
      throw new ConcurrencyError('No common ancestor found between current commit and HEAD.')
    }

    // Each side's changes since the ancestor, as structural diffs:
    // subtrees the two commits share are never read.
    const ourChanges = await this.changes(lca, this.currentCommitHash)
    const theirChanges = await this.changes(lca, theirHead)

    const rules = opts.rules
    const mergeFns = new Map([...this.mergeFns, ...(rules.mergeFns ?? [])])
    const mergePrefixes = new Map([...this.mergePrefixes, ...(rules.mergePrefixes ?? [])])
    const defaultMerge = rules.defaultMerge ?? this.defaultMergeFn

    let resolution: MergeResolution
    try {
      resolution = await resolveMerge({
        ours: ourChanges,
        theirs: theirChanges,
        blobReader: (id) => this.readBlob(id),
        mergeFns,
        mergePrefixes,
        defaultMerge,
      })
      if (opts.postCheck !== undefined) {
        const refused = new Set<string>()
        for (const [key, value] of resolution.mergedValues) {
          if (!opts.postCheck(key, value)) refused.add(key)
        }
        if (refused.size > 0) throw new MergeConflict(refused)
      }
    } catch (e) {
      if (e instanceof MergeConflict) {
        this.restoreState(opts.savedState)
        if (opts.onConflict === 'abandon') return this.finish(abandoned(strategy))
      }
      throw e
    }

    if (opts.base !== undefined && !changesOurs(resolution)) {
      // The change is already in our state, or changes nothing: as
      // long as our state is still the branch's.
      return this.finish(await this.liveNoOp(this.currentCommitHash, opts.onConflict))
    }

    // Lost-race default: follow the moved HEAD. Cross-branch callers
    // pass [ours, theirs] so linear history stays on the merging branch.
    const parents = opts.parents ?? [theirHead, this.currentCommitHash]
    await this.createMergeCommit(resolution, parents, opts.info ?? null)
    const mergeHash = this.currentCommitHash

    // Publish over theirHead on the lost-race path, our own head
    // otherwise.
    if (await this.casHead(opts.casFrom ?? theirHead, mergeHash)) {
      this.baseCommitHash = mergeHash
      return this.finish(
        landed(mergeHash, strategy, resolution.autoMergedKeys, resolution.carriedKeys),
      )
    }

    this.restoreState(opts.savedState)
    if (opts.onConflict === 'abandon') return this.finish(abandoned(strategy))
    throw new ConcurrencyError('HEAD changed during three-way merge. Refresh and retry.')
  }

  /**
   * A merge or apply that changes nothing, at `ourHead`. Nothing is
   * written, so no publish guards the answer the way the other outcomes'
   * CAS on our head does: it holds only if the branch is still where
   * this handle left it.
   */
  private async liveNoOp(ourHead: string, onConflict: ConflictDisposition): Promise<MergeResult> {
    const live = await this.latestHead()
    if (live === null) throw new UnknownBranchError(this.branch)
    if (live !== ourHead) {
      if (onConflict === 'abandon') return abandoned('no_op')
      throw new ConcurrencyError('HEAD changed during merge. Refresh and retry.')
    }
    return noOp(ourHead)
  }

  private async requireCommit(commit: string): Promise<void> {
    if (!(await this.hasCommit(commit))) throw new UnknownCommitError(commit)
  }

  /**
   * Run `op` once every earlier state-changing call on this handle has
   * settled, so two never interleave: each builds on the state the
   * last one left. One that throws partway is rolled back, leaving the
   * handle as it found it.
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
    } catch (e) {
      this.rollBackAttempt()
      throw e
    } finally {
      this.endAttempt()
      release()
    }
  }

  private finish(result: MergeResult): MergeResult {
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

  protected abstract snapshotState(): unknown
  protected abstract restoreState(saved: unknown): void
  /** Restore the state an attempt opened by `snapshotState` started
   *  from, if one is still open: the operation running it threw. */
  protected rollBackAttempt(): void {}
  /** Forget the open attempt, if any: the operation running it is over. */
  protected endAttempt(): void {}
  protected abstract createCommit(opts: {
    updates?: Map<string, Uint8Array>
    removals?: Set<string>
    info?: CommitInfo
  }): Promise<string>
  /** Make a commit from a resolved merge, on `parents`: our state with
   *  the resolution's changes applied. */
  protected abstract createMergeCommit(
    resolution: MergeResolution,
    parents: readonly string[],
    info: CommitInfo | null,
  ): Promise<string>
  /** Move HEAD from our head to `theirHead`, a descendant of it,
   *  writing no commit. */
  protected abstract fastForward(
    theirHead: string,
    onConflict: ConflictDisposition,
  ): Promise<MergeResult>
  protected abstract casHead(expected: string, newHead: string): Promise<boolean>
  /** Each key whose value differs from `commitA` to `commitB`; a null
   *  or missing `commitA` reads as empty. */
  protected abstract changes(commitA: string | null, commitB: string): Promise<Map<string, Change>>
  protected abstract loadParents(commitHash: string): Promise<readonly string[]>
  protected abstract findLca(commitA: string, commitB: string): Promise<string | null>
  protected abstract readBlob(blobId: string): Promise<Uint8Array | null>
  /** Whether a commit is in the store. */
  protected abstract hasCommit(commit: string): Promise<boolean>
}

function checkDisposition(value: ConflictDisposition | undefined): ConflictDisposition {
  const onConflict = value ?? 'raise'
  if (onConflict !== 'raise' && onConflict !== 'abandon') {
    throw new TypeError(`onConflict must be 'raise' or 'abandon', got ${String(onConflict)}`)
  }
  return onConflict
}

function checkMoved(value: MovedDisposition | undefined): MovedDisposition {
  const onMoved = value ?? 'merge'
  if (onMoved !== 'merge' && onMoved !== 'raise') {
    throw new TypeError(`onMoved must be 'merge' or 'raise', got ${String(onMoved)}`)
  }
  return onMoved
}

function noOp(commit: string): MergeResult {
  return { merged: true, commit, strategy: 'no_op', autoMergedKeys: [], carriedKeys: [] }
}

function abandoned(strategy: MergeResult['strategy']): MergeResult {
  return { merged: false, commit: null, strategy, autoMergedKeys: [], carriedKeys: [] }
}

function landed(
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
