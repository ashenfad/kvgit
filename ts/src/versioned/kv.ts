/**
 * KVStore-backed versioned state.
 *
 * Storage layout:
 *
 *   `__kvgit_version__`              — storage version sentinel (1 in v1)
 *   `__branch_head__<branch>`        — current HEAD commit hash
 *   `__branch_head_prev__<branch>`   — previous HEAD (recovery backup)
 *   `__commit_root__<commit>`        — keyset HAMT root hash
 *   `__parent_commit__<commit>`      — JSON list of parent commit hashes
 *   `__commit_time__<commit>`        — wall time epoch ms
 *   `__info__<commit>`               — optional caller-supplied info dict
 *   `kvgit:keyset:<node_hash>`       — HAMT node bytes (via Keyset)
 *   `<commit_hash>:<user_key>`       — blob value bytes
 *
 * `__branch_head_prev__` is written only after a HEAD swap succeeds, so
 * it always names a commit `__branch_head__` really held. Recovery
 * reads it, so a value that was never HEAD would graft onto the branch
 * a lineage it never had. See `casHead` for what that does and does
 * not guarantee, and `repairHead` for the explicit repair path.
 *
 * The keyset is a content-addressable HAMT (`Keyset` over `Hamt`) so
 * unchanged subtrees are shared across commits by hash equality. A
 * single-key change writes O(log N) new HAMT nodes instead of
 * rewriting a full snapshot per commit.
 */

import type { Op } from '../../wasm/kvgit_wasm.js'
import { core } from '../core'
import { CorruptHeadError, UnknownBranchError, UnknownCommitError } from '../errors'
import { EMPTY_HASH, NodeCache, uncached } from '../hamt'
import { Keyset, decodeEntry } from '../keyset'
import { compareCodePoints, pyDumps, pyDumpsBytes } from '../pyjson'
import {
  type BytesMergeFn,
  type CommitInfo,
  type KVStore,
  type KeysetEntry,
  MergeChoice,
  type MergeHeadsOptions,
  type MergePolicy,
  type MergeResult,
  type MergeRuleOptions,
  type MetaEntry,
  type PostCheck,
  type Versioned,
  type VersionedCommitOptions,
} from '../types'
import { VersionedBase, checkDisposition, checkMoved } from './base'
import { type Committed, type StoredEntry, callCore, driveCommit } from './drive'
import {} from './helpers'
import {
  BRANCH_HEAD,
  COMMIT_ROOT,
  INFO_KEY,
  PARENT_COMMIT,
  PARENT_GENS,
  ROOT_COMMIT,
  assertStorageVersion,
  blobKey,
  checkStorageVersion,
  generationOf,
  loads,
  loadsInfo,
  safeLoads,
} from './layout'
import { GC_LEASE_TTL } from './lease'
import type { Change } from './merge'
import { listBranches, rejectReservedBranch } from './refs'

// ---------------------------------------------------------------------------
// Recovery layer
// ---------------------------------------------------------------------------

/**
 * Last-resort recovery for a HEAD that is present but unresolvable.
 *
 * Called with the store and the branch name; returns a commit hash to
 * treat as that branch's HEAD, or null if it cannot say.
 *
 * There is no default, and none is shipped. When HEAD is unresolvable
 * and the prev-HEAD backup is missing or equally broken, the
 * information needed is no longer in the store, so no implementation
 * can be correct — only lucky. kvgit reports null and leaves the guess
 * to a caller who has decided the trade is worth it. Unset, corrupt-
 * HEAD recovery stops at the prev-HEAD tier.
 *
 * The answer is checked before it is used: it must be a string naming
 * a commit whose `__commit_root__` is present in this store. Anything
 * else leaves the branch unrecoverable.
 */
export type CorruptHeadRecoverer = (store: KVStore, branch: string) => Promise<string | null>

/**
 * Resolve the HEAD of a branch with prev-HEAD fallback.
 *
 * Tries:
 *   1. `__branch_head__<branch>` — current pointer
 *   2. `__branch_head_prev__<branch>` — backup written after each CAS
 *   3. `recoverFromCorruptHead` — optional caller-supplied last resort
 *
 * **Never writes.** Every read path in the library goes through here,
 * so healing the damage in place would make an ordinary `get` a
 * mutation — impossible for a read-only consumer, and a race between
 * two readers repairing the same branch to different answers. The
 * recovery is returned to the caller and forgotten; {@link repairHead}
 * is the explicit call that makes it durable, and the write path heals
 * HEAD itself as part of the CAS that has to move it anyway.
 *
 * Returns null if nothing recovers.
 */
export async function resolveHead(
  store: KVStore,
  branch: string,
  opts: { recoverFromCorruptHead?: CorruptHeadRecoverer } = {},
): Promise<string | null> {
  return (await resolveWithRoot(store, branch, opts.recoverFromCorruptHead))?.commit ?? null
}

/** A head resolved: its commit, and that commit's root. */
interface Resolved {
  commit: string
  root: string | null
}

const wallClock = (): number => Date.now() / 1000

/** {@link resolveHead}, with the root of the commit it resolves to. */
async function resolveWithRoot(
  store: KVStore,
  branch: string,
  recover: CorruptHeadRecoverer | undefined,
): Promise<Resolved | null> {
  const args = { branch, recover: recover !== undefined }
  return (await callCore('resolve_head', args, {
    store,
    recover,
    now: wallClock,
  })) as Resolved | null
}

/**
 * The head of branch `name`, which must exist and resolve.
 *
 * @throws UnknownBranchError if there is no such branch.
 * @throws CorruptHeadError if its HEAD is damaged beyond recovery.
 */
export async function headOf(
  store: KVStore,
  name: string,
  recover: CorruptHeadRecoverer | undefined,
): Promise<string> {
  const args = { branch: name, recover: recover !== undefined }
  const found = (await callCore('head_of', args, { store, recover, now: wallClock })) as Resolved
  return found.commit
}

/**
 * Persist a recovered HEAD for a damaged branch.
 *
 * Read paths recover a corrupt `__branch_head__` in memory and leave
 * the store untouched, so the damage stays visible until someone
 * decides what to do about it. This is that decision: resolve the
 * branch the way a read would, and write the answer back.
 *
 * Handle-independent, like `cleanOrphans` — it takes a raw `KVStore`,
 * so it works with or without a `VersionedKV` anchored on the branch.
 *
 * Idempotent, and a no-op on a healthy branch. The write is a CAS
 * against the damaged bytes, so it cannot overwrite a HEAD another
 * writer fixed, or advanced, in the meantime. When that CAS does not
 * win, the branch is re-resolved, so the answer describes the store
 * rather than the attempt.
 *
 * @returns The commit HEAD now names, or null if the branch does not
 *   exist or nothing recoverable was found.
 */
export async function repairHead(
  store: KVStore,
  branch = 'main',
  opts: { recoverFromCorruptHead?: CorruptHeadRecoverer } = {},
): Promise<string | null> {
  const recover = opts.recoverFromCorruptHead
  const args = { branch, recover: recover !== undefined }
  return (await callCore('repair_head', args, { store, recover, now: wallClock })) as string | null
}

/**
 * Guess a corrupt branch's HEAD by scanning every commit. Opt-in: a
 * {@link CorruptHeadRecoverer} to pass as `recoverFromCorruptHead`.
 *
 * Of the commits no other branch or tag reaches, it returns the newest
 * tip by commit time. Two things to weigh before wiring it in. It can
 * serve another branch's deleted data: a deleted branch's commits are
 * unclaimed until garbage collection takes them, so a damaged branch
 * can recover to a lineage it never had. And it reads every commit in
 * the store, per unresolved read, until someone calls `repairHead`.
 * It is worth it where losing the branch outright is worse than
 * recovering it to a plausible commit.
 */
export const recoverByCommitScan: CorruptHeadRecoverer = async (store, branch) =>
  (await callCore('recover_by_commit_scan', { branch }, { store, now: wallClock })) as string | null

// ---------------------------------------------------------------------------
// VersionedKV
// ---------------------------------------------------------------------------

export interface VersionedKVOptions {
  branch?: string
  /** Pin to a specific commit instead of resolving the branch HEAD. */
  commitHash?: string
  /**
   * Mint the branch with an initial commit when it does not exist
   * (default true). Pass false to throw `UnknownBranchError` instead,
   * so a read after a delete cannot resurrect the branch and block a
   * later create.
   */
  create?: boolean
  /** Last resort for a corrupt HEAD with no usable backup (see `CorruptHeadRecoverer`). */
  recoverFromCorruptHead?: CorruptHeadRecoverer
  /** The clock commits are stamped with, in seconds. Default: wall
   *  time. A test pins it to replay a history byte for byte. */
  now?: () => number
  /**
   * Read the keyset from its HAMT on demand instead of loading all of
   * it on open. Opening reads only the commit's root; `get`, `has` and
   * `getMany` walk the tree through a bounded node cache, and a commit
   * rewrites only the paths it changes. Worth it for keysets of tens of
   * thousands of keys and up; `keys()` then reads the whole tree on
   * every call. `true` uses a private cache of the default size.
   */
  lazy?: boolean | LazyKeysetOptions
}

/** Options for a lazily read keyset. */
export interface LazyKeysetOptions {
  /** The node cache to read through. Nodes are content addressed, so
   *  one cache can be shared by every handle on a store. */
  cache?: NodeCache
}

/**
 * Create branch `name` at commit `target`, in one conditional write:
 * the head (expected absent), the root commit's records when `target`
 * is the root commit and the store lacks it, and the removal of any
 * stale backup left under the name by an earlier branch.
 *
 * @throws BranchExistsError if the branch exists.
 * @throws UnknownCommitError if `target` is not in the store.
 */
export async function createBranchHead(
  store: KVStore,
  name: string,
  target: string,
  now: () => number,
): Promise<void> {
  await callCore('create_branch', { name, target }, { store, now })
}

/**
 * Delete branch `name`: its head and its backup, in one removal. Its
 * commits become collectable.
 *
 * The removal is conditional on the head it read. If the branch moved
 * or was made again in between, nothing is removed: a delete decided
 * on one head must not take a newer one with it. A branch deleted and
 * made again at the *same* commit is indistinguishable, so the stale
 * delete removes it, as git's `update-ref -d <ref> <old-oid>` would.
 *
 * @throws UnknownBranchError if there is no such branch.
 * @throws ConcurrencyError if the branch changed while being deleted.
 */
export async function deleteBranchHead(store: KVStore, name: string): Promise<void> {
  await callCore('delete_branch', { name }, { store, now: wallClock })
}

/**
 * The lowest common ancestor of two commits, or null if they share no
 * history. On a criss-cross tie the smallest hash wins: deterministic,
 * but arbitrary, so criss-cross merges resolve cleanly rather than
 * throwing. `known` collects the generations the search works out, for
 * a caller that writes a commit over these two next.
 */
export async function mergeBase(
  store: KVStore,
  commitA: string,
  commitB: string,
  known?: Map<string, number>,
): Promise<string | null> {
  const args = { a: commitA, b: commitB, check: false }
  const found = (await callCore('merge_base', args, { store, now: wallClock })) as {
    base: string | null
    generations: Array<[string, number]>
  }
  for (const [commit, gen] of found.generations) known?.set(commit, gen)
  return found.base
}

/**
 * Each key whose value differs from commit `a` to commit `b`. A
 * structural diff of the two keysets: subtrees the commits share are
 * skipped by hash, so the cost follows the size of the change. A key
 * whose entry differs only in metadata, with the same blob, is not a
 * change. A null or missing `a` reads as empty.
 */
export async function commitChanges(
  store: KVStore,
  commitA: string | null,
  commitB: string,
  cache?: NodeCache,
): Promise<Map<string, Change>> {
  const wanted = [commitA, commitB].filter((c): c is string => c !== null).map(COMMIT_ROOT)
  const found = await store.getMany(wanted)
  const rootOf = (commit: string | null): string => {
    if (commit === null) return EMPTY_HASH
    const raw = found.get(COMMIT_ROOT(commit))
    const root = raw === undefined ? null : safeLoads(raw)
    return typeof root === 'string' ? root : EMPTY_HASH
  }
  const opts = cache === undefined ? {} : { cache }
  const diff = await Keyset.fromRoot(store, rootOf(commitA), opts).diff(
    Keyset.fromRoot(store, rootOf(commitB), opts),
  )
  const changes = new Map<string, Change>()
  for (const [key, entry] of diff.added) changes.set(key, { old: null, new: entry })
  for (const [key, entry] of diff.removed) changes.set(key, { old: entry, new: null })
  for (const [key, [oldEntry, newEntry]] of diff.modified) {
    if (oldEntry.blob !== newEntry.blob) changes.set(key, { old: oldEntry, new: newEntry })
  }
  return changes
}

export interface GcOptions {
  /** Seconds an unreachable commit is kept (default an hour): how long
   *  abandoned work lingers. Any value is safe beside writers. */
  minAge?: number
  /** Also scan the node and blob namespaces for content no commit
   *  references (crash leftovers). */
  deep?: boolean
  /** Wait out a sweep holding the lease (default true); false throws
   *  `GcBusy` instead. */
  wait?: boolean
  /** Seconds this sweep's lease stays live: what it costs writers, at
   *  most, if it dies holding it. */
  leaseTtl?: number
  now?: () => number
}

/**
 * Sweep what no live commit reaches; return how many commits went.
 *
 * A commit is live if a branch head or a tag reaches it, if it is in
 * flight (written, not yet published), or if it is younger than
 * `minAge` seconds. Everything an orphan alone held goes with it: its
 * records, blobs and HAMT nodes. `deep` adds a scan of the node and
 * blob namespaces for content no commit references at all.
 *
 * Runs under the store's GC lease: no commit batch can land while it is
 * held, and every commit written but not yet published carries an
 * in-flight marker the sweep marks from, so this is safe beside
 * concurrent writers at any `minAge`, 0 included.
 *
 * @throws GcBusy with `wait: false`, if another sweep holds the lease.
 * @throws StorageVersionError for a store of another layout; nothing is
 *   written to it, the lease included.
 */
export async function gcStore(store: KVStore, opts: GcOptions = {}): Promise<number> {
  // Before the lease, so a store this code mustn't touch comes out of
  // the call untouched.
  await assertStorageVersion(store)
  const args = {
    min_age: opts.minAge ?? 3600,
    deep: opts.deep ?? false,
    wait: opts.wait ?? true,
    lease_ttl: opts.leaseTtl ?? GC_LEASE_TTL,
  }
  return (await callCore('gc', args, { store, now: opts.now ?? wallClock })) as number
}

/** A commit's parents, in order; empty for the root commit or one not
 *  in the store. */
export async function loadParents(store: KVStore, commit: string): Promise<readonly string[]> {
  const raw = await store.get(PARENT_COMMIT(commit))
  if (raw === null) return []
  const parsed = loads(raw)
  if (typeof parsed === 'string') return [parsed]
  if (Array.isArray(parsed)) return parsed.filter((p): p is string => typeof p === 'string')
  return []
}

/**
 * A commit log over a `KVStore`.
 *
 * Construct via the async `VersionedKV.open(store, opts?)` factory —
 * the constructor itself is private because initialization needs to
 * resolve HEAD (and possibly create an initial empty commit), both of
 * which are async.
 */
export class VersionedKV extends VersionedBase {
  readonly store: KVStore
  private meta: Map<string, MetaEntry>
  private readonly recoverFromCorruptHead: CorruptHeadRecoverer | undefined
  private readonly now: () => number
  /** Generations of commits this handle has written or read. A
   *  commit's generation never changes, so nothing here goes stale. */
  private readonly generations = new Map<string, number>()
  /** A lazy handle's node cache; null for a handle that holds its
   *  whole keyset in `commitKeys` and `meta`. */
  private readonly nodeCache: NodeCache | null
  /** A lazy handle's current keyset root. */
  private root: string

  private constructor(opts: {
    store: KVStore
    branch: string
    commitHash: string
    commitKeys: Map<string, string>
    meta: Map<string, MetaEntry>
    recoverFromCorruptHead: CorruptHeadRecoverer | undefined
    now: () => number
    nodeCache: NodeCache | null
    root: string
  }) {
    super({ branch: opts.branch, commitHash: opts.commitHash })
    this.store = opts.store
    this.commitKeys = opts.commitKeys
    this.meta = opts.meta
    this.recoverFromCorruptHead = opts.recoverFromCorruptHead
    this.now = opts.now
    this.nodeCache = opts.nodeCache
    this.root = opts.root
  }

  /** Whether this handle reads its keyset on demand. */
  get isLazy(): boolean {
    return this.nodeCache !== null
  }

  /** A lazy handle's node cache (null when eager). */
  get keysetCache(): NodeCache | null {
    return this.nodeCache
  }

  /** The options a handle derived from this one opens with. */
  private inherited(): Pick<VersionedKVOptions, 'recoverFromCorruptHead' | 'now' | 'lazy'> {
    return {
      now: this.now,
      ...(this.recoverFromCorruptHead !== undefined && {
        recoverFromCorruptHead: this.recoverFromCorruptHead,
      }),
      ...(this.nodeCache !== null && { lazy: { cache: this.nodeCache } }),
    }
  }

  /** The keyset at the current commit, reading through the node cache. */
  private lazyKeyset(): Keyset {
    return Keyset.fromRoot(this.store, this.root, { cache: this.nodeCache as NodeCache })
  }

  /**
   * Open or create a versioned store on `store`.
   *
   * Resolves the branch HEAD with prev-HEAD recovery; creates an
   * initial empty commit if the branch doesn't exist yet. Validates
   * the storage version (rejects formats from other versions).
   */
  static async open(store: KVStore, opts: VersionedKVOptions = {}): Promise<VersionedKV> {
    await checkStorageVersion(store)
    const branch = opts.branch ?? 'main'
    await rejectReservedBranch(branch)
    const create = opts.create ?? true

    let commitHash = opts.commitHash
    if (commitHash === undefined) {
      const recovered = await resolveHead(store, branch, {
        ...(opts.recoverFromCorruptHead !== undefined && {
          recoverFromCorruptHead: opts.recoverFromCorruptHead,
        }),
      })
      if (recovered !== null) {
        commitHash = recovered
      } else if ((await store.get(BRANCH_HEAD(branch))) !== null) {
        throw new CorruptHeadError(branch)
      } else if (!create) {
        throw new UnknownBranchError(branch, true)
      } else {
        // A new branch starts at the empty root commit. Another writer
        // may create it first, at any commit; then the handle opens at
        // whatever head that writer gave it.
        let created = true
        try {
          await createBranchHead(store, branch, ROOT_COMMIT, opts.now ?? wallClock)
        } catch (e) {
          if ((await store.get(BRANCH_HEAD(branch))) === null) throw e
          created = false
        }
        if (created) {
          commitHash = ROOT_COMMIT
        } else {
          const winner = await resolveHead(store, branch, {
            ...(opts.recoverFromCorruptHead !== undefined && {
              recoverFromCorruptHead: opts.recoverFromCorruptHead,
            }),
          })
          if (winner === null) {
            throw new CorruptHeadError(branch)
          }
          commitHash = winner
        }
      }
    }

    const lazy = opts.lazy === undefined || opts.lazy === false ? null : opts.lazy
    if (lazy !== null) {
      return new VersionedKV({
        store,
        branch,
        commitHash,
        commitKeys: new Map(),
        meta: new Map(),
        recoverFromCorruptHead: opts.recoverFromCorruptHead,
        now: opts.now ?? wallClock,
        nodeCache: (lazy === true ? undefined : lazy.cache) ?? new NodeCache(),
        root: await rootOfCommit(store, commitHash),
      })
    }
    const { commitKeys, meta } = await populateState(store, commitHash)
    return new VersionedKV({
      store,
      branch,
      commitHash,
      commitKeys,
      meta,
      recoverFromCorruptHead: opts.recoverFromCorruptHead,
      now: opts.now ?? wallClock,
      nodeCache: null,
      root: EMPTY_HASH,
    })
  }

  // --- Reads ---

  override async get(key: string): Promise<Uint8Array | null> {
    if (this.nodeCache === null) return super.get(key)
    const entry = await this.lazyKeyset().get(key)
    return entry === null ? null : this.readBlob(entry.blob)
  }

  /** In a lazy handle, the tree is walked for all the keys a level at a
   *  time, and the blobs read in one batch. */
  override async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    if (this.nodeCache === null) return super.getMany(keys)
    const entries = await this.lazyKeyset().getMany(keys)
    const byBlob = new Map<string, string[]>()
    for (const [key, entry] of entries) {
      const at = byBlob.get(entry.blob)
      if (at === undefined) byBlob.set(entry.blob, [key])
      else at.push(key)
    }
    const out = new Map<string, Uint8Array>()
    if (byBlob.size === 0) return out
    for (const [blob, raw] of await this.store.getMany(byBlob.keys())) {
      for (const key of byBlob.get(blob) ?? []) out.set(key, raw)
    }
    return out
  }

  override async has(key: string): Promise<boolean> {
    if (this.nodeCache === null) return super.has(key)
    return this.lazyKeyset().has(key)
  }

  /** In a lazy handle, every call walks the whole tree, a level at a
   *  time, from the root current when it starts. */
  override async *keys(): AsyncIterable<string> {
    if (this.nodeCache === null) {
      yield* super.keys()
      return
    }
    yield* Keyset.fromRoot(this.store, this.root).keysBatched()
  }

  // --- VersionedBase abstract methods ---

  async latestHead(): Promise<string | null> {
    return resolveHead(this.store, this.branch, {
      ...(this.recoverFromCorruptHead !== undefined && {
        recoverFromCorruptHead: this.recoverFromCorruptHead,
      }),
    })
  }

  /**
   * One commit, run by the core: it reads the head, lands the batch,
   * publishes it or merges onto a moved head, and reports the keyset it
   * leaves as changes to this handle's. The handle changes only once a
   * commit lands, so one that fails leaves it as it was.
   */
  protected async commitOnce(opts: VersionedCommitOptions): Promise<MergeResult> {
    const onConflict = checkDisposition(opts.onConflict)
    const onMoved = checkMoved(opts.onMoved)
    // The bytes the commit writes stay here; the core names them by key.
    const held = new Map<string, Uint8Array>()
    const updates: Array<[string, string, number, null]> = []
    for (const [key, value] of opts.updates ?? []) {
      const blob = await blobKey(value)
      held.set(blob, value)
      updates.push([key, blob, value.length, null])
    }
    const hosted: BytesMergeFn[] = []
    const current = this.currentCommitHash
    const args = {
      branch: this.branch,
      base: this.baseCommitHash,
      current,
      updates,
      removals: [...(opts.removals ?? [])].sort(compareCodePoints),
      chunks: [],
      onMoved,
      onConflict,
      ...this.lowered(hosted, opts),
      recover: this.recoverFromCorruptHead !== undefined,
      // Opening checked the store is stamped v4, the only version read.
      stamped: true,
      ...this.known(current),
    }
    const info = opts.info ?? null
    const c = await core()
    const memo = this.nodeCache === null ? uncached(c) : this.nodeCache.memo(c)
    const op = c.Op.commit(JSON.stringify(args), info === null ? undefined : pyDumps(info), memo)
    return this.land(op, held, hosted)
  }

  /**
   * Merge `theirs` into this branch, or, given `from`, apply the change
   * from `from` to `theirs`: one operation of the core, which this
   * handle moves to the result of once it lands.
   */
  protected async mergeOnce(
    theirs: string,
    from: string | null,
    opts: MergeHeadsOptions,
  ): Promise<MergeResult> {
    const onConflict = checkDisposition(opts.onConflict)
    const hosted: BytesMergeFn[] = []
    const current = this.currentCommitHash
    const postCheck = opts.postCheck ?? undefined
    const args = {
      branch: this.branch,
      ours: current,
      theirs,
      from,
      fastForward: from === null && opts.fastForward !== false,
      onConflict,
      ...this.lowered(hosted, opts),
      postCheck: postCheck !== undefined,
      recover: this.recoverFromCorruptHead !== undefined,
      stamped: true,
      ...this.known(current),
    }
    const info = opts.info ?? null
    const c = await core()
    const memo = this.nodeCache === null ? uncached(c) : this.nodeCache.memo(c)
    const op = c.Op.merge(JSON.stringify(args), info === null ? undefined : pyDumps(info), memo)
    return this.land(op, new Map(), hosted, postCheck)
  }

  /** The merge rules in effect, layered over this handle's, as the core
   *  names them: a side, or the number of a function in `hosted`, which
   *  gains each function named. */
  private lowered(hosted: BytesMergeFn[], opts: MergeRuleOptions) {
    const lower = (policy: MergePolicy): string | number => {
      if (policy === MergeChoice.OURS) return 'ours'
      if (policy === MergeChoice.THEIRS) return 'theirs'
      hosted.push(policy as BytesMergeFn)
      return hosted.length - 1
    }
    const lowerAll = (rules: Map<string, MergePolicy>) =>
      [...rules].map(([k, policy]) => [k, lower(policy)])
    const defaultMerge = opts.defaultMerge ?? this.defaultMergeFn
    return {
      mergeFns: lowerAll(new Map([...this.mergeFns, ...(opts.mergeFns ?? [])])),
      mergePrefixes: lowerAll(new Map([...this.mergePrefixes, ...(opts.mergePrefixes ?? [])])),
      defaultMerge: defaultMerge === null ? null : lower(defaultMerge),
    }
  }

  /** The root and generation this handle knows of `commit`. */
  private known(commit: string) {
    const generation = this.generations.get(commit)
    return {
      roots: this.nodeCache === null ? [] : [[commit, this.root]],
      generations: generation === undefined ? [] : [[commit, generation]],
    }
  }

  /** Run a commit or merge, and move this handle to what it landed. */
  private async land(
    op: Op,
    held: Map<string, Uint8Array>,
    hosted: BytesMergeFn[],
    postCheck?: PostCheck,
  ): Promise<MergeResult> {
    let done: Committed
    try {
      done = await driveCommit(op, {
        store: this.store,
        held,
        mergeFns: hosted,
        postCheck,
        recover: this.recoverFromCorruptHead,
        now: this.now,
      })
    } finally {
      op.free()
    }
    const result: MergeResult = {
      merged: done.merged,
      commit: done.commit,
      strategy: done.strategy,
      autoMergedKeys: done.autoMerged,
      carriedKeys: done.carried,
    }
    if (!done.merged || done.strategy === 'no_op') return this.finish(result)
    // Landed: change the key maps in place, by what the commit changed.
    for (const [key, stored] of done.changes) {
      this.setEntry(key, stored === null ? null : decodeStored(stored))
    }
    for (const [commit, gen] of done.generations) this.generations.set(commit, gen)
    const commit = done.commit as string
    if (this.nodeCache !== null) {
      this.root = done.roots.find(([c]) => c === commit)?.[1] ?? this.root
    }
    this.currentCommitHash = this.baseCommitHash = commit
    return this.finish(result)
  }

  /** Point `key` at `blob`, or remove it (null), in place, once a
   *  commit or merge has landed. */
  private setEntry(key: string, entry: { blob: string; meta: MetaEntry } | null): void {
    // A lazy handle holds no key maps: the new root is the change.
    if (this.nodeCache !== null) return
    const keys = this.ownedKeys()
    if (entry === null) {
      keys.delete(key)
      this.meta.delete(key)
    } else {
      keys.set(key, entry.blob)
      this.meta.set(key, entry.meta)
    }
  }

  /** The generation of each of a new commit's parents. */
  private async parentGenerations(parents: readonly string[]): Promise<number[]> {
    const out: number[] = []
    for (const parent of parents) {
      let gen = this.generations.get(parent)
      if (gen === undefined) {
        gen = await this.generationFromStore(parent)
        this.generations.set(parent, gen)
      }
      out.push(gen)
    }
    return out
  }

  /** A commit's generation, from its own records: 0 for a commit with no
   *  parents, else one more than its highest parent's. */
  private async generationFromStore(commit: string): Promise<number> {
    const parents = await this.loadParents(commit)
    if (parents.length === 0) return 0
    const raw = await this.store.get(PARENT_GENS(commit))
    const gens = raw === null ? null : safeLoads(raw)
    if (
      Array.isArray(gens) &&
      gens.length === parents.length &&
      gens.every((g) => Number.isInteger(g) && g >= 0)
    ) {
      return generationOf(gens as number[])
    }
    // A commit with no recorded generations: count from its parents.
    let highest = 0
    for (const parent of parents) {
      highest = Math.max(highest, (await this.parentGenerations([parent]))[0] as number)
    }
    return highest + 1
  }

  protected async changes(commitA: string | null, commitB: string): Promise<Map<string, Change>> {
    return commitChanges(this.store, commitA, commitB, this.nodeCache ?? undefined)
  }

  protected async loadParents(commitHash: string): Promise<readonly string[]> {
    return loadParents(this.store, commitHash)
  }

  protected async findLca(commitA: string, commitB: string): Promise<string | null> {
    const found = new Map<string, number>()
    const base = await mergeBase(this.store, commitA, commitB, found)
    for (const [c, g] of found) this.generations.set(c, g)
    return base
  }

  protected async readBlob(blobId: string): Promise<Uint8Array | null> {
    return this.store.get(blobId)
  }

  // --- Navigation ---

  async refresh(): Promise<void> {
    return this.exclusive(() => this.refreshOnce())
  }

  private async refreshOnce(): Promise<void> {
    const head = await resolveHead(this.store, this.branch, {
      ...(this.recoverFromCorruptHead !== undefined && {
        recoverFromCorruptHead: this.recoverFromCorruptHead,
      }),
    })
    if (head === null) {
      if ((await this.store.get(BRANCH_HEAD(this.branch))) !== null) {
        throw new CorruptHeadError(this.branch)
      }
      throw new UnknownBranchError(this.branch)
    }
    await this.loadCommitInto(head, true)
  }

  async checkout(commitHash: string, opts: { branch?: string } = {}): Promise<Versioned | null> {
    if ((await this.store.get(COMMIT_ROOT(commitHash))) === null) return null
    return VersionedKV.open(this.store, {
      commitHash,
      branch: opts.branch ?? this.branch,
      ...this.inherited(),
    })
  }

  async createBranch(name: string, opts: { at?: string } = {}): Promise<Versioned> {
    const target = opts.at ?? this.currentCommitHash
    if (opts.at !== undefined && (await this.store.get(COMMIT_ROOT(opts.at))) === null) {
      throw new UnknownCommitError(opts.at)
    }
    await createBranchHead(this.store, name, target, this.now)
    return VersionedKV.open(this.store, {
      branch: name,
      commitHash: target,
      ...this.inherited(),
    })
  }

  async deleteBranch(name: string): Promise<void> {
    if (name === this.branch) {
      throw new Error('Cannot delete the current branch')
    }
    await deleteBranchHead(this.store, name)
  }

  async switchBranch(name: string): Promise<void> {
    return this.exclusive(() => this.switchBranchOnce(name))
  }

  private async switchBranchOnce(name: string): Promise<void> {
    await rejectReservedBranch(name)
    const head = await resolveHead(this.store, name, {
      ...(this.recoverFromCorruptHead !== undefined && {
        recoverFromCorruptHead: this.recoverFromCorruptHead,
      }),
    })
    if (head === null) {
      if ((await this.store.get(BRANCH_HEAD(name))) !== null) {
        throw new CorruptHeadError(name)
      }
      throw new UnknownBranchError(name)
    }
    this.branch = name
    await this.loadCommitInto(head, true)
  }

  async peek(key: string, opts: { branch: string }): Promise<Uint8Array | null> {
    const head = await resolveHead(this.store, opts.branch, {
      ...(this.recoverFromCorruptHead !== undefined && {
        recoverFromCorruptHead: this.recoverFromCorruptHead,
      }),
    })
    if (head === null) return null
    const rootBytes = await this.store.get(COMMIT_ROOT(head))
    if (rootBytes === null) return null
    const root = loads(rootBytes) as string
    const ks = Keyset.fromRoot(this.store, root)
    const entry = await ks.get(key)
    if (entry === null) return null
    return this.store.get(entry.blob)
  }

  /**
   * Point the branch at `commitHash`. Whatever HEAD held becomes the
   * backup in the same write, conditional on HEAD still holding it: a
   * writer moving HEAD in between sends the reset round again, and a
   * delete landing first makes it throw rather than recreate the
   * branch.
   *
   * @returns false if `commitHash` is not in the store.
   * @throws UnknownBranchError if the branch has no HEAD.
   */
  async resetTo(commitHash: string): Promise<boolean> {
    return this.exclusive(() => this.resetToOnce(commitHash))
  }

  private async resetToOnce(commitHash: string): Promise<boolean> {
    const args = { name: this.branch, target: commitHash }
    const moved = await callCore('reset_branch', args, { store: this.store, now: this.now })
    if (moved !== true) return false
    await this.loadCommitInto(commitHash, true)
    return true
  }

  async listBranches(): Promise<string[]> {
    return listBranches(this.store)
  }

  static async exists(store: KVStore, name: string): Promise<boolean> {
    // Same predicate listBranches lists by: the branch has a HEAD
    // entry. Never writes, so checking cannot resurrect a deleted
    // branch. A damaged HEAD still counts as existing; opening it
    // raises either way.
    return (await callCore('branch_exists', { name }, { store, now: wallClock })) as boolean
  }

  async branchExists(name: string): Promise<boolean> {
    return VersionedKV.exists(this.store, name)
  }

  async commitInfo(commitHash?: string): Promise<CommitInfo | null> {
    const target = commitHash ?? this.currentCommitHash
    const raw = await this.store.get(INFO_KEY(target))
    if (raw === null) return null
    return loadsInfo(raw) as CommitInfo
  }

  // --- Recovery ---

  /**
   * Persist a recovered HEAD for this branch.
   *
   * Thin instance wrapper over the module-level {@link repairHead}.
   * Reads recover a damaged HEAD without writing it back; this is the
   * explicit call that makes the recovery durable.
   *
   * @returns The commit HEAD now names, or null if nothing was
   *   recoverable.
   */
  async repairHead(): Promise<string | null> {
    return repairHead(this.store, this.branch, {
      ...(this.recoverFromCorruptHead !== undefined && {
        recoverFromCorruptHead: this.recoverFromCorruptHead,
      }),
    })
  }

  // --- Orphan cleanup ---

  /**
   * Reclaim commits no branch or tag reaches, with whatever only they
   * held (blobs, HAMT nodes, records). Commits younger than `minAge`
   * (milliseconds, default an hour) are kept: how long abandoned work
   * lingers.
   *
   * Safe beside concurrent writers: the sweep runs under the store's GC
   * lease, no commit batch lands while it is held, and a commit written
   * but not yet published is marked in flight, so nothing a writer is
   * using is taken. A sweep already running is waited out.
   *
   * It doesn't reclaim content no commit points at (an interrupted
   * write, a crash between a batch and its publish, the blobs of an
   * orphan whose keyset is unreadable); {@link deepClean} does.
   *
   * @returns Number of orphaned commits removed.
   */
  async cleanOrphans(opts: { minAge?: number } = {}): Promise<number> {
    return gcStore(this.store, { minAge: (opts.minAge ?? 3_600_000) / 1000, now: this.now })
  }

  /**
   * {@link cleanOrphans} plus a scan of the whole `kvgit:keyset:` and
   * `kvgit:blob:` namespaces, deleting any node or blob no live, in
   * flight or young commit reaches: the only way to reclaim content no
   * commit references any more. Runs under the same lease, so it too is
   * safe beside writers; a sweep already running makes it throw
   * `GcBusy` rather than wait.
   *
   * @param opts.minAge Milliseconds. Commits younger than this are
   *   protected from sweep, even if currently unreachable. Default: 1 hour.
   * @returns Number of orphaned commits removed.
   */
  async deepClean(opts: { minAge?: number } = {}): Promise<number> {
    return gcStore(this.store, {
      minAge: (opts.minAge ?? 3_600_000) / 1000,
      deep: true,
      wait: false,
      now: this.now,
    })
  }

  // --- Internal ---

  private async loadCommitInto(commitHash: string, updateBase: boolean): Promise<void> {
    if (this.nodeCache !== null) {
      const root = await rootOfCommit(this.store, commitHash)
      this.currentCommitHash = commitHash
      if (updateBase) this.baseCommitHash = commitHash
      this.root = root
      return
    }
    const { commitKeys, meta } = await populateState(this.store, commitHash)
    // The commit and its keys change together, after the read, so no
    // reader sees one commit's hash over another's keys.
    this.currentCommitHash = commitHash
    if (updateBase) this.baseCommitHash = commitHash
    this.replaceKeys(commitKeys)
    this.meta = meta
  }
}

/** A commit's keyset root, or the empty tree's for a commit with none. */
async function rootOfCommit(store: KVStore, commit: string): Promise<string> {
  const raw = await store.get(COMMIT_ROOT(commit))
  return raw === null ? EMPTY_HASH : (loads(raw) as string)
}

/**
 * Materialize a commit's flat keyset and meta map from its HAMT.
 *
 * Uses `Keyset.materialize()` (batched BFS, one `getMany` per HAMT
 * level) so cold loads against high-latency stores are O(log N)
 * round-trips instead of O(N).
 *
 * **Eager-materialization tradeoff.** This holds the entire commit's
 * keyset (key → blob pointer + meta) in memory. The benefit:
 * `get(key)` is one in-memory map lookup followed by a single store
 * fetch for the blob, regardless of total commit size. The cost:
 * memory scales with key count.
 *
 * Matches kvgit-py's design. For agex sessions (typically hundreds
 * to a few thousand keys) this is fine. For very large keysets, open
 * the handle with `lazy`, which walks the HAMT on demand through a
 * bounded node cache, at the cost of extra store reads per `get`.
 */
async function populateState(
  store: KVStore,
  commitHash: string,
): Promise<{ commitKeys: Map<string, string>; meta: Map<string, MetaEntry> }> {
  const rootBytes = await store.get(COMMIT_ROOT(commitHash))
  if (rootBytes === null) {
    return { commitKeys: new Map(), meta: new Map() }
  }
  const root = loads(rootBytes) as string
  const ks = Keyset.fromRoot(store, root)
  const materialized = await ks.materialize()
  const commitKeys = new Map<string, string>()
  const meta = new Map<string, MetaEntry>()
  for (const [k, entry] of materialized) {
    commitKeys.set(k, entry.blob)
    meta.set(k, entry.meta)
  }
  return { commitKeys, meta }
}

/** An entry as the core reports it, stored form parsed. */
function decodeStored(stored: StoredEntry): KeysetEntry {
  return decodeEntry(pyDumpsBytes(stored))
}
