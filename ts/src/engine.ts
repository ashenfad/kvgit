/**
 * A worktree's engine: the commit its branch is checked out at, read
 * through the core, and the commits, merges and resets that move it.
 *
 * @internal
 */

import type { NodeMemo, Op, WorktreeState } from '../wasm/kvgit_wasm.js'
import { type Core, core } from './core'
import { CorruptHeadError, UnknownBranchError } from './errors'
import { compareCodePoints, pyDumps } from './pyjson'
import {
  type BytesMergeFn,
  type CommitInfo,
  type KVStore,
  MergeChoice,
  type MergePolicy,
  type MergeResult,
  type PostCheck,
} from './types'
import { type Committed, callCore, driveCommit } from './versioned/drive'
import { type CorruptHeadRecoverer, resolveHead } from './versioned/kv'
import { BRANCH_HEAD, EMPTY_HASH, blobKey } from './versioned/layout'
import { rejectReservedBranch } from './versioned/refs'

/** Merge rules at the bytes level, as a commit or merge takes them. */
export interface EngineRules {
  mergeFns?: ReadonlyMap<string, MergePolicy>
  mergePrefixes?: ReadonlyMap<string, MergePolicy>
  defaultMerge?: MergePolicy | null
}

export interface EngineCommitOptions extends EngineRules {
  updates?: ReadonlyMap<string, Uint8Array>
  removals?: ReadonlySet<string>
  info?: CommitInfo
  onConflict?: 'raise' | 'abandon'
  onMoved?: 'merge' | 'raise'
}

export interface EngineMergeOptions extends EngineRules {
  info?: CommitInfo
  onConflict?: 'raise' | 'abandon'
  postCheck?: PostCheck
  fastForward?: boolean
}

let noMemo: NodeMemo | null = null

/** A memo with no room: the engine reads its tree through the
 *  repository's content cache instead. */
function uncached(c: Core): NodeMemo {
  noMemo ??= new c.NodeMemo(0)
  return noMemo
}

/**
 * The state a worktree reads and the operations that move it. It holds
 * the keyset root of the commit its branch is checked out at (its
 * head), and the key index the core builds on the first listing.
 * Commits, merges and applies run in the core and move it to what they
 * land; a refresh or reset moves it to another commit. Operations that
 * move it run one at a time, each building on what the last one left.
 */
export class Engine {
  readonly store: KVStore
  readonly branch: string
  readonly #recover: CorruptHeadRecoverer | undefined
  readonly #now: () => number
  readonly #state: WorktreeState
  /** Generations of commits this engine has written or read. A
   *  commit's generation never changes, so nothing here goes stale. */
  readonly #generations = new Map<string, number>()
  #head: string
  #tail: Promise<void> = Promise.resolve()

  private constructor(opts: {
    store: KVStore
    branch: string
    head: string
    recover: CorruptHeadRecoverer | undefined
    now: () => number
    state: WorktreeState
  }) {
    this.store = opts.store
    this.branch = opts.branch
    this.#head = opts.head
    this.#recover = opts.recover
    this.#now = opts.now
    this.#state = opts.state
  }

  /**
   * An engine on `branch` at its head.
   *
   * @throws UnknownBranchError if the branch has no head.
   * @throws CorruptHeadError if its head is damaged beyond recovery.
   */
  static async open(
    store: KVStore,
    branch: string,
    opts: { recover?: CorruptHeadRecoverer | undefined; now: () => number },
  ): Promise<Engine> {
    // A name the branch API refuses never reaches the store: the empty
    // name, or one under the tag namespace.
    await rejectReservedBranch(branch)
    const c = await core()
    const engine = new Engine({
      store,
      branch,
      head: '',
      recover: opts.recover,
      now: opts.now,
      state: new c.WorktreeState(EMPTY_HASH),
    })
    await engine.#moveTo(await engine.#resolve())
    return engine
  }

  /** The commit the engine is checked out at. */
  get head(): string {
    return this.#head
  }

  async #resolve(): Promise<string> {
    const found = await resolveHead(this.store, this.branch, {
      ...(this.#recover !== undefined && { recoverFromCorruptHead: this.#recover }),
    })
    if (found !== null) return found
    if ((await this.store.get(BRANCH_HEAD(this.branch))) !== null) {
      throw new CorruptHeadError(this.branch)
    }
    throw new UnknownBranchError(this.branch)
  }

  /** Move to a commit's keyset root, reading nothing of the tree under
   *  it. The head moves in the turn the root does, so no reader sees
   *  one commit's hash over another's keys; the read of the root brings
   *  the commit's records, so the engine knows the generation its next
   *  commit builds on. */
  async #moveTo(commit: string): Promise<void> {
    await this.#onTree('wt_checkout', { commit }, (found) => {
      const { generation } = found as { generation: number | null }
      if (generation !== null) this.#generations.set(commit, generation)
      this.#head = commit
    })
  }

  #onTree(name: string, args: object, settled?: (value: unknown) => void): Promise<unknown> {
    return callCore(name, args, { store: this.store, now: this.#now, tree: this.#state }, settled)
  }

  /** Run `op` once every earlier call that moves the engine has
   *  settled, so two never interleave. */
  async #exclusive<T>(op: () => Promise<T>): Promise<T> {
    const prior = this.#tail
    let release = (): void => {}
    this.#tail = new Promise<void>((resolve) => {
      release = resolve
    })
    await prior
    try {
      return await op()
    } finally {
      release()
    }
  }

  // --- Reads at the head ---

  /** The stored bytes of each of `keys` the head holds. The tree is
   *  walked for all of them a level at a time, through the repository's
   *  content cache, and the blobs read in one batch. */
  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const found = (await this.#onTree('wt_lookup', { keys: [...new Set(keys)] })) as Array<
      [string, [string, number, string[]]]
    >
    // Keys holding equal bytes share one blob, so a blob answers for
    // every key that points at it.
    const byBlob = new Map<string, string[]>()
    for (const [key, [blob]] of found) {
      const at = byBlob.get(blob)
      if (at === undefined) byBlob.set(blob, [key])
      else at.push(key)
    }
    const out = new Map<string, Uint8Array>()
    if (byBlob.size === 0) return out
    for (const [blob, raw] of await this.store.getMany(byBlob.keys())) {
      for (const key of byBlob.get(blob) ?? []) out.set(key, raw)
    }
    return out
  }

  async has(key: string): Promise<boolean> {
    return (await this.#onTree('wt_contains', { key })) as boolean
  }

  /** Every key at the head, sorted. The first listing builds the key
   *  index with one walk of the tree; later ones read it. */
  async keys(): Promise<string[]> {
    return (await this.#onTree('wt_keys', {})) as string[]
  }

  // --- Moving the branch ---

  /** Commit `updates` and `removals` on the head, fast-forward or
   *  merged onto a branch that moved; see `Worktree.commit`. */
  commit(opts: EngineCommitOptions): Promise<MergeResult> {
    return this.#exclusive(async () => {
      const onConflict = checkDisposition(opts.onConflict)
      const onMoved = opts.onMoved ?? 'merge'
      if (onMoved !== 'merge' && onMoved !== 'raise') {
        throw new TypeError(`onMoved must be 'merge' or 'raise', got ${String(onMoved)}`)
      }
      // The bytes the commit writes stay here; the core names them by key.
      const held = new Map<string, Uint8Array>()
      const updates: Array<[string, string, number, null]> = []
      for (const [key, value] of opts.updates ?? []) {
        const blob = await blobKey(value)
        held.set(blob, value)
        updates.push([key, blob, value.length, null])
      }
      const hosted: BytesMergeFn[] = []
      const args = {
        branch: this.branch,
        base: this.#head,
        current: this.#head,
        updates,
        removals: [...(opts.removals ?? [])].sort(compareCodePoints),
        chunks: [],
        onMoved,
        onConflict,
        ...lowered(hosted, opts),
        recover: this.#recover !== undefined,
        // Opening the repository checked the store is stamped v4, the
        // only version read.
        stamped: true,
        ...this.#known(this.#head),
      }
      const info = opts.info ?? null
      const c = await core()
      const op = c.Op.commit(
        JSON.stringify(args),
        info === null ? undefined : pyDumps(info),
        uncached(c),
      )
      return this.#land(op, held, hosted)
    })
  }

  /** Merge `theirs` into the branch, or, given `from`, apply the
   *  change from `from` to `theirs`; see `Worktree.merge` and
   *  `Worktree.apply`. */
  merge(theirs: string, from: string | null, opts: EngineMergeOptions): Promise<MergeResult> {
    return this.#exclusive(async () => {
      const onConflict = checkDisposition(opts.onConflict)
      const hosted: BytesMergeFn[] = []
      const postCheck = opts.postCheck
      const args = {
        branch: this.branch,
        ours: this.#head,
        theirs,
        from,
        fastForward: from === null && opts.fastForward !== false,
        onConflict,
        ...lowered(hosted, opts),
        postCheck: postCheck !== undefined,
        recover: this.#recover !== undefined,
        stamped: true,
        ...this.#known(this.#head),
      }
      const info = opts.info ?? null
      const c = await core()
      const op = c.Op.merge(
        JSON.stringify(args),
        info === null ? undefined : pyDumps(info),
        uncached(c),
      )
      return this.#land(op, new Map(), hosted, postCheck)
    })
  }

  /**
   * Move to the branch's head as the store holds it now.
   *
   * @throws UnknownBranchError if the branch has no head.
   * @throws CorruptHeadError if its head is damaged beyond recovery.
   */
  refresh(): Promise<void> {
    return this.#exclusive(async () => this.#moveTo(await this.#resolve()))
  }

  /**
   * Move the branch to `commit`, the head it held becoming the backup
   * in the same write; false if the store lacks `commit`.
   *
   * @throws UnknownBranchError if the branch has no head.
   */
  resetTo(commit: string): Promise<boolean> {
    return this.#exclusive(async () => {
      const args = { name: this.branch, target: commit }
      const moved = await callCore('reset_branch', args, { store: this.store, now: this.#now })
      if (moved !== true) return false
      await this.#moveTo(commit)
      return true
    })
  }

  /** The root and generation the engine knows of `commit`. */
  #known(commit: string) {
    const generation = this.#generations.get(commit)
    return {
      roots: commit === this.#head ? [[commit, this.#state.root()]] : [],
      generations: generation === undefined ? [] : [[commit, generation]],
    }
  }

  /** Run a commit or merge, and move to what it landed. */
  async #land(
    op: Op,
    held: Map<string, Uint8Array>,
    hosted: BytesMergeFn[],
    postCheck?: PostCheck,
  ): Promise<MergeResult> {
    // A commit that lands moves the state as it finishes, and the head
    // moves in the same turn.
    const moved = (done: Committed) => {
      if (!done.merged || done.strategy === 'no_op') return
      for (const [commit, gen] of done.generations) this.#generations.set(commit, gen)
      this.#head = done.commit as string
    }
    let done: Committed
    try {
      done = await driveCommit(
        op,
        {
          store: this.store,
          held,
          mergeFns: hosted,
          postCheck,
          recover: this.#recover,
          now: this.#now,
          tree: this.#state,
        },
        moved,
      )
    } finally {
      op.free()
    }
    return {
      merged: done.merged,
      commit: done.commit,
      strategy: done.strategy,
      autoMergedKeys: done.autoMerged,
      carriedKeys: done.carried,
    }
  }
}

function checkDisposition(value: 'raise' | 'abandon' | undefined): 'raise' | 'abandon' {
  const onConflict = value ?? 'raise'
  if (onConflict !== 'raise' && onConflict !== 'abandon') {
    throw new TypeError(`onConflict must be 'raise' or 'abandon', got ${String(onConflict)}`)
  }
  return onConflict
}

/** The merge rules as the core names them: a side, or the number of a
 *  function in `hosted`, which gains each function named. */
function lowered(hosted: BytesMergeFn[], rules: EngineRules) {
  const lower = (policy: MergePolicy): string | number => {
    if (policy === MergeChoice.OURS) return 'ours'
    if (policy === MergeChoice.THEIRS) return 'theirs'
    hosted.push(policy as BytesMergeFn)
    return hosted.length - 1
  }
  const lowerAll = (map: ReadonlyMap<string, MergePolicy> | undefined) =>
    [...(map ?? [])].map(([k, policy]) => [k, lower(policy)])
  const defaultMerge = rules.defaultMerge ?? null
  return {
    mergeFns: lowerAll(rules.mergeFns),
    mergePrefixes: lowerAll(rules.mergePrefixes),
    defaultMerge: defaultMerge === null ? null : lower(defaultMerge),
  }
}
