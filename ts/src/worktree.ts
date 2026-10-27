/**
 * Worktree: a branch checked out for work, as Python kvgit 0.4 shapes
 * it.
 */

import type { Engine } from './engine'
import { UnknownCommitError } from './errors'
import type { Ref, Repo } from './repo'
import {
  type CommitInfo,
  MergeChoice,
  type MergeFn,
  type MergePolicy,
  type MergeResult,
  type PostCheck,
} from './types'
import { ValueCache } from './valueCache'
import { ROOT_COMMIT } from './versioned/layout'

/** What a merge registration holds at the value level: a merge
 *  function over decoded values, or a {@link MergeChoice}. */
export type MergeRule<V> = MergeFn<V> | MergeChoice

/** A worktree's pending changes: keys written and keys deleted since
 *  its last commit. */
export class Status {
  constructor(
    readonly updated: ReadonlySet<string>,
    readonly removed: ReadonlySet<string>,
  ) {}

  /** No pending changes. */
  get clean(): boolean {
    return this.updated.size === 0 && this.removed.size === 0
  }
}

/** Merge rules a single call layers over the worktree's and the
 *  repository's. */
export interface WorktreeMergeRules<V> {
  /** Rules by key. */
  mergeFns?: ReadonlyMap<string, MergeRule<V>>
  /** Rules by key prefix; a key takes the longest one it starts with,
   *  after an exact key match. */
  mergePrefixes?: ReadonlyMap<string, MergeRule<V>>
  /** The rule for keys nothing else covers. */
  defaultMerge?: MergeRule<V>
  /** What a conflict the rules can't resolve does: `'raise'` (default)
   *  throws `MergeConflict`; `'abandon'` returns a result with
   *  `merged: false` and changes nothing. */
  onConflict?: 'raise' | 'abandon'
}

export interface WorktreeCommitOptions<V> extends WorktreeMergeRules<V> {
  /** Commit only these pending keys and leave the rest pending. */
  keys?: Iterable<string>
  info?: CommitInfo
  /** What a commit does when the branch has moved since this
   *  worktree's head: `'merge'` (default) three-way merges onto the
   *  tip; `'raise'` throws `BranchMovedError`, writes nothing and keeps
   *  the pending changes. */
  onMoved?: 'merge' | 'raise'
}

export interface WorktreeApplyOptions<V> extends WorktreeMergeRules<V> {
  info?: CommitInfo
  /** Accept or refuse each value a merge function produced, as stored
   *  bytes: `(key, merged)`; false files the key as a conflict. */
  postCheck?: PostCheck
}

export interface WorktreeMergeOptions<V> extends WorktreeApplyOptions<V> {
  /** Fast-forward when this branch hasn't moved since the two forked
   *  (default true); false writes a merge commit regardless. */
  fastForward?: boolean
}

/**
 * A branch checked out for work: a map whose writes stay pending until
 * {@link commit}.
 *
 * Bound to one branch for its whole life. Reads see pending changes on
 * top of the commit the worktree is based on ({@link head}). Several
 * worktrees, in one process or many, may hold the same branch; a
 * commit that loses the race to another merges automatically, and
 * throws only when the two changed the same key in ways no merge rule
 * resolves. `commit({ onMoved: 'raise' })` refuses to merge instead.
 *
 * Obtain one from `Repo.worktree`.
 */
export class Worktree<V = Uint8Array> {
  readonly repo: Repo<V>
  readonly #engine: Engine
  readonly #updates = new Map<string, V>()
  readonly #removals = new Set<string>()
  readonly #cache: ValueCache<V>
  readonly #mergeFns = new Map<string, MergeRule<V>>()
  readonly #mergePrefixes = new Map<string, MergeRule<V>>()
  #defaultMerge: MergeRule<V> | undefined

  /** @internal Made by `Repo.worktree`. */
  constructor(repo: Repo<V>, engine: Engine) {
    this.repo = repo
    this.#engine = engine
    this.#cache = new ValueCache<V>(repo.valueCacheBytes)
  }

  /**
   * The values read at {@link head} that this worktree remembers, up
   * to the repository's `valueCacheBytes`. Emptied whenever the head
   * moves and by {@link discard}; `cache.clear()` lets them go without
   * either. Pending changes are never held here.
   */
  get cache(): ValueCache<V> {
    return this.#cache
  }

  /** Every worktree reads its keyset on demand. */
  get lazy(): boolean {
    return true
  }

  /** The branch this worktree is bound to. */
  get branch(): string {
    return this.#engine.branch
  }

  /**
   * The commit this worktree is based on. The branch itself may have
   * moved on since, if another worktree committed to it;
   * `repo.branches.get(wt.branch)` reads the branch's tip.
   */
  get head(): string {
    return this.#engine.head
  }

  // --- Reads ---

  /** A value, pending changes included; undefined if absent. */
  async get(key: string): Promise<V | undefined> {
    if (this.#removals.has(key)) return undefined
    if (this.#updates.has(key)) return this.#updates.get(key)
    if (this.#cache.has(key)) return this.#cache.get(key)
    const raw = (await this.#engine.getMany([key])).get(key)
    if (raw === undefined) return undefined
    const value = this.repo.codec.decode(raw)
    this.#cache.remember(key, value, raw.length)
    return value
  }

  /** The values of the given keys that exist, pending changes included. */
  async getMany(keys: Iterable<string>): Promise<Map<string, V>> {
    const out = new Map<string, V>()
    const fetch: string[] = []
    for (const key of keys) {
      if (this.#removals.has(key)) continue
      if (this.#updates.has(key)) out.set(key, this.#updates.get(key) as V)
      else if (this.#cache.has(key)) out.set(key, this.#cache.get(key) as V)
      else fetch.push(key)
    }
    if (fetch.length > 0) {
      for (const [key, raw] of await this.#engine.getMany(fetch)) {
        const value = this.repo.codec.decode(raw)
        this.#cache.remember(key, value, raw.length)
        out.set(key, value)
      }
    }
    return out
  }

  async has(key: string): Promise<boolean> {
    if (this.#removals.has(key)) return false
    if (this.#updates.has(key)) return true
    return this.#engine.has(key)
  }

  /** Every key visible here, sorted: committed, plus pending, minus
   *  deleted. */
  async keys(): Promise<string[]> {
    const seen = new Set<string>()
    for (const key of await this.#engine.keys()) {
      if (!this.#removals.has(key)) seen.add(key)
    }
    for (const key of this.#updates.keys()) seen.add(key)
    return [...seen].sort()
  }

  // --- Writes ---

  /** Write a value; it stays pending until {@link commit}. */
  set(key: string, value: V): void {
    this.#removals.delete(key)
    this.#updates.set(key, value)
  }

  /**
   * Delete a key; the deletion stays pending until {@link commit}.
   *
   * @returns false if the key was not there.
   */
  async delete(key: string): Promise<boolean> {
    if (!(await this.has(key))) return false
    this.#updates.delete(key)
    // A key written only in pending changes has nothing committed to
    // remove: dropping the pending write is the whole deletion.
    if (await this.#engine.has(key)) this.#removals.add(key)
    return true
  }

  /** The changes pending since this worktree's last commit. */
  status(): Status {
    return new Status(new Set(this.#updates.keys()), new Set(this.#removals))
  }

  // --- Merge rules ---

  /** Register a merge function, or a `MergeChoice`, for one key. A
   *  worktree's rules sit over the repository's and under a single
   *  call's. */
  setMergeFn(key: string, fn: MergeRule<V>): void {
    this.#mergeFns.set(key, fn)
  }

  /**
   * Register a merge function, or a `MergeChoice`, under a prefix, for
   * keys whose names aren't known up front (`runs/` for `runs/<id>`). A
   * key takes the most specific registration: its exact key, else the
   * longest prefix it starts with, else the default. A merge function
   * is consulted only where both sides changed a key; a `MergeChoice`
   * is a standing policy over the whole prefix, so `OURS` also drops a
   * key the other side added and keeps one it removed.
   */
  setMergePrefix(prefix: string, fn: MergeRule<V>): void {
    this.#mergePrefixes.set(prefix, fn)
  }

  /** Register the merge function, or `MergeChoice`, for keys no other
   *  rule covers. */
  setDefaultMerge(fn: MergeRule<V>): void {
    this.#defaultMerge = fn
  }

  // --- Commit ---

  /**
   * Commit the pending changes to the branch.
   *
   * Fast-forwards when the branch has not moved since this worktree's
   * head; otherwise three-way merges onto its tip with the effective
   * merge rules. With `onMoved: 'raise'` a moved branch is never
   * merged: the commit lands only as a fast-forward from this
   * worktree's head, decided by the same compare-and-swap that
   * publishes it, and otherwise throws.
   *
   * @throws BranchMovedError with `onMoved: 'raise'`, if the branch has
   *   moved since this worktree's head, even with nothing to commit; its
   *   `head` is the tip found.
   *   Nothing is written and the pending changes stay readable; then
   *   `refresh()` moves to the tip (dropping them) to redo the work.
   * @throws MergeConflict for keys both sides changed that no rule
   *   resolves (unless `onConflict: 'abandon'`).
   * @throws ConcurrencyError if the branch moved again mid-merge.
   * @throws UnknownBranchError if the branch was deleted.
   */
  async commit(opts: WorktreeCommitOptions<V> = {}): Promise<MergeResult> {
    const only = opts.keys === undefined ? null : new Set(opts.keys)
    const updates = new Map<string, Uint8Array>()
    for (const [key, value] of this.#updates) {
      if (only === null || only.has(key)) updates.set(key, this.repo.codec.encode(value))
    }
    const removals = new Set<string>()
    for (const key of this.#removals) {
      if (only === null || only.has(key)) removals.add(key)
    }

    const result = await this.#engine.commit({
      ...(updates.size > 0 && { updates }),
      ...(removals.size > 0 && { removals }),
      ...this.rules(opts),
      ...(opts.info !== undefined && { info: opts.info }),
      ...(opts.onMoved !== undefined && { onMoved: opts.onMoved }),
    })

    if (result.merged) {
      if (only === null) {
        this.#updates.clear()
        this.#removals.clear()
      } else {
        for (const key of only) {
          this.#updates.delete(key)
          this.#removals.delete(key)
        }
      }
      // The head moved, and a merge may have changed other keys.
      this.#cache.clear()
    }
    return result
  }

  // --- Merge and apply ---

  /**
   * Merge a commit, branch or tag into this worktree's branch.
   *
   * The lowest common ancestor, a three-way resolve, and a two-parent
   * merge commit `[ours, theirs]` published on this worktree's head.
   * When this branch hasn't moved since the two forked, it
   * fast-forwards instead, as git does: HEAD moves to theirs and no
   * commit is written, so `info` isn't recorded (`fastForward: false`
   * writes the merge commit regardless). A commit this branch already
   * contains merges as a no-op.
   *
   * @throws Error with pending changes: commit or discard them first.
   * @throws MergeConflict for keys both sides changed that no rule
   *   resolves (unless `onConflict: 'abandon'`).
   * @throws ConcurrencyError if the branch moved during the merge, or
   *   the two share no history.
   */
  async merge(ref: Ref, opts: WorktreeMergeOptions<V> = {}): Promise<MergeResult> {
    this.requireClean('merge')
    const theirHead = await this.repo.resolveRef(ref)
    const result = await this.#engine.merge(theirHead, null, {
      ...this.rules(opts),
      ...(opts.info !== undefined && { info: opts.info }),
      ...(opts.postCheck !== undefined && { postCheck: opts.postCheck }),
      ...(opts.fastForward !== undefined && { fastForward: opts.fastForward }),
    })
    if (result.merged) this.#cache.clear()
    return result
  }

  /**
   * Commit the change from `base` to `target` onto this branch: a
   * three-way merge with `base` standing in for the common ancestor.
   * What `target` changed relative to `base` lands on this worktree's
   * head as one ordinary commit, and changes made here since are kept.
   * A change already present commits nothing (`no_op`). `cherryPick`
   * and `revert` are the common cases.
   *
   * @throws Error with pending changes: commit or discard them first.
   * @throws UnknownCommitError if `base` or `target` is not in the store.
   * @throws MergeConflict for keys both changed that no rule resolves.
   * @throws ConcurrencyError if the branch moved during the apply.
   */
  async apply(
    base: string,
    target: string,
    opts: WorktreeApplyOptions<V> = {},
  ): Promise<MergeResult> {
    this.requireClean('apply a change')
    await this.repo.getCommit(base)
    await this.repo.getCommit(target)
    const result = await this.#engine.merge(target, base, {
      ...this.rules(opts),
      ...(opts.info !== undefined && { info: opts.info }),
      ...(opts.postCheck !== undefined && { postCheck: opts.postCheck }),
    })
    if (result.merged) this.#cache.clear()
    return result
  }

  /** Commit the change `commit` made (relative to its first parent)
   *  onto this branch. Takes {@link apply}'s options. */
  async cherryPick(commit: string, opts: WorktreeApplyOptions<V> = {}): Promise<MergeResult> {
    const parents = (await this.repo.getCommit(commit)).parents
    return this.apply(parents[0] ?? ROOT_COMMIT, commit, opts)
  }

  /** Commit the undoing of the change `commit` made (relative to its
   *  first parent) onto this branch. Takes {@link apply}'s options. */
  async revert(commit: string, opts: WorktreeApplyOptions<V> = {}): Promise<MergeResult> {
    const parents = (await this.repo.getCommit(commit)).parents
    return this.apply(commit, parents[0] ?? ROOT_COMMIT, opts)
  }

  private requireClean(verb: string): void {
    if (this.#updates.size > 0 || this.#removals.size > 0) {
      throw new Error(`cannot ${verb} with pending changes; commit or discard them first`)
    }
  }

  /** The effective rules, lowered to bytes: the repository's, then this
   *  worktree's, then the call's. */
  private rules(opts: WorktreeMergeRules<V>): {
    onConflict: 'raise' | 'abandon'
    mergeFns?: Map<string, MergePolicy>
    mergePrefixes?: Map<string, MergePolicy>
    defaultMerge?: MergePolicy
  } {
    const lowerAll = (...maps: Array<ReadonlyMap<string, MergeRule<V>> | undefined>) => {
      const out = new Map<string, MergePolicy>()
      for (const map of maps) for (const [k, rule] of map ?? []) out.set(k, this.lower(rule))
      return out
    }
    const fns = lowerAll(this.repo.mergeFns, this.#mergeFns, opts.mergeFns)
    const prefixes = lowerAll(this.repo.mergePrefixes, this.#mergePrefixes, opts.mergePrefixes)
    const fallback = opts.defaultMerge ?? this.#defaultMerge ?? this.repo.defaultMerge
    return {
      onConflict: opts.onConflict ?? 'raise',
      ...(fns.size > 0 && { mergeFns: fns }),
      ...(prefixes.size > 0 && { mergePrefixes: prefixes }),
      ...(fallback !== undefined && { defaultMerge: this.lower(fallback) }),
    }
  }

  /**
   * A merge rule, as the bytes-level merge takes it. A `MergeChoice`
   * names a side rather than computing a value, so it goes down
   * untouched. A merge function is wrapped: each side decoded with the
   * repository's codec, the result encoded back with it (or passed
   * through, if it is a `MergeChoice`).
   */
  private lower(rule: MergeRule<V>): MergePolicy {
    if (rule instanceof MergeChoice) return rule
    const { codec } = this.repo
    return (old, ours, theirs) => {
      const result = rule(
        old === null ? null : codec.decode(old),
        ours === null ? null : codec.decode(ours),
        theirs === null ? null : codec.decode(theirs),
      )
      return result instanceof MergeChoice ? result : codec.encode(result)
    }
  }

  // --- Moving ---

  /** Drop every pending change. */
  discard(): void {
    this.#updates.clear()
    this.#removals.clear()
    this.#cache.clear()
  }

  /**
   * Move the branch to `commit` and drop pending changes. The commit
   * the branch left stays in its history only if something else
   * reaches it.
   *
   * @throws UnknownCommitError if `commit` is not in the store.
   * @throws UnknownBranchError if the branch was deleted.
   */
  async reset(commit: string): Promise<void> {
    if (!(await this.#engine.resetTo(commit))) throw new UnknownCommitError(commit)
    this.discard()
  }

  /**
   * Move to the branch's current tip, dropping pending changes.
   *
   * @throws UnknownBranchError if the branch was deleted.
   * @throws CorruptHeadError if its HEAD is damaged beyond recovery.
   */
  async refresh(): Promise<void> {
    await this.#engine.refresh()
    this.discard()
  }
}
