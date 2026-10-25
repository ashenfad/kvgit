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

import { core } from '../core'
import {
  BranchExistsError,
  ConcurrencyError,
  CorruptHeadError,
  GcBusy,
  UnknownBranchError,
  UnknownCommitError,
} from '../errors'
import { EMPTY_HASH, NodeCache, uncached } from '../hamt'
import { Keyset, decodeEntry } from '../keyset'
import { PyFloat, compareCodePoints, pyDumps, pyDumpsBytes } from '../pyjson'
import {
  type BytesMergeFn,
  type CommitInfo,
  type ConflictDisposition,
  type KVStore,
  type KeysetEntry,
  MergeChoice,
  type MergePolicy,
  type MergeResult,
  type MetaEntry,
  type Versioned,
  type VersionedCommitOptions,
} from '../types'
import { type CommitRecord, NoGenerations, generationOfRecord, loadRecords } from './ancestry'
import { VersionedBase, checkDisposition, checkMoved } from './base'
import { type Committed, type StoredEntry, driveCommit } from './drive'
import { type ParentLoader, walkHistory } from './helpers'
import {
  BLOB_PREFIX,
  BRANCH_HEAD,
  BRANCH_HEAD_PREFIX,
  BRANCH_HEAD_PREV,
  COMMIT_ROOT,
  COMMIT_TIME,
  GC_LEASE_KEY,
  INFO_KEY,
  IN_FLIGHT_KEY,
  IN_FLIGHT_PREFIX,
  PARENT_COMMIT,
  PARENT_GENS,
  ROOT_COMMIT,
  TAG_BRANCH_PREFIX,
  assertStorageVersion,
  blobKey,
  checkStorageVersion,
  commitHash,
  commitRecords,
  dumps,
  generationOf,
  loads,
  loadsInfo,
  safeLoads,
} from './layout'
import {
  GC_LEASE_TTL,
  type HeldLease,
  acquireGcLease,
  inFlightMarker,
  markerLive,
  releaseGcLease,
  sameBytes,
  tryLand,
  waitForGc,
} from './lease'
import type { Change, MergeResolution } from './merge'
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
  // Try current HEAD.
  const headBytes = await store.get(BRANCH_HEAD(branch))
  if (headBytes !== null) {
    const commitHash = safeLoads(headBytes)
    if (typeof commitHash === 'string' && (await store.get(COMMIT_ROOT(commitHash))) !== null) {
      return commitHash
    }
  }

  // HEAD is present but unusable — try the backup.
  //
  // Gated on HEAD *existing*, the same condition the injected tier
  // below already applies. An absent HEAD is not damage, it means the
  // branch is gone: `deleteBranch` removes the key. A backup that
  // outlives it — a writer descheduled between its CAS and its backup
  // write, resuming after a concurrent delete and recreating only the
  // backup — must not bring the branch back through this tier.
  //
  // Nothing legitimate needs the ungated form: `casHead` writes the
  // backup only after a successful CAS, so HEAD exists whenever the
  // backup means anything.
  const prevBytes = headBytes === null ? null : await store.get(BRANCH_HEAD_PREV(branch))
  if (prevBytes !== null) {
    const commitHash = safeLoads(prevBytes)
    if (typeof commitHash === 'string' && (await store.get(COMMIT_ROOT(commitHash))) !== null) {
      console.warn(`kvgit: branch '${branch}' HEAD corrupt, recovered from prev HEAD`)
      return commitHash
    }
  }

  // HEAD existed, is corrupt, and the backup did not save it. The
  // store no longer holds the answer, so there is nothing left to read
  // — only to guess. Guessing is the caller's call, not ours.
  if (opts.recoverFromCorruptHead && headBytes !== null) {
    const recovered: unknown = await opts.recoverFromCorruptHead(store, branch)
    // A recoverer is caller-supplied, so its answer is no more
    // trustworthy than anything else read out of the store: it counts
    // only if it is a string naming a commit whose `__commit_root__`
    // is present. This function promises a valid commit or null, and
    // an unchecked answer is worse than no answer — `repairHead` makes
    // it durable, replacing obviously corrupt HEAD bytes with a
    // plausible hash that names nothing, which is harder to diagnose
    // than the damage it replaced.
    if (typeof recovered === 'string' && (await store.get(COMMIT_ROOT(recovered))) !== null) {
      console.warn(`kvgit: branch '${branch}' HEAD corrupt, recovered via injected recoverer`)
      return recovered
    }
    if (recovered !== null && recovered !== undefined) {
      console.warn(
        `kvgit: branch '${branch}' recoverer returned ${describeCandidate(recovered)}, which is not a commit in this store; treating the branch as unrecoverable`,
      )
    }
  }

  return null
}

/**
 * Render a value for a log line, showing what it actually was.
 *
 * A rejected recovery candidate is worth naming: the whole point of
 * rejecting it is that someone has to go and fix the recoverer.
 */
function describeCandidate(value: unknown): string {
  try {
    return JSON.stringify(value) ?? String(value)
  } catch {
    return Object.prototype.toString.call(value)
  }
}

/** Bytewise equality for two `Uint8Array`s. */
function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

/**
 * Atomically replace an unresolvable HEAD with a recovered value.
 *
 * Returns true only when HEAD was damaged *and* this call is the one
 * that replaced it. Three cases are deliberately left alone:
 *
 *   - HEAD resolves fine — it did not break, it *moved*, and another
 *     writer won a legitimate race. Overwriting it would destroy a
 *     good commit to make a losing CAS succeed.
 *   - HEAD already holds `recovered` — nothing to do.
 *   - HEAD is absent — the branch was deleted. Re-creating the key is
 *     exactly the resurrection `deleteBranch` drops the prev-HEAD
 *     backup to prevent.
 *
 * The replacement is a CAS against the exact damaged bytes, so two
 * writers healing the same branch cannot both win, and a HEAD that
 * someone else repaired (or advanced) in the meantime is never
 * clobbered.
 */
const wallClock = (): number => Date.now() / 1000

async function healHead(store: KVStore, branch: string, recovered: Uint8Array): Promise<boolean> {
  const branchKey = BRANCH_HEAD(branch)
  // Installing a recovered HEAD makes a commit reachable that the store
  // didn't claim a moment ago, so the checks and the write happen
  // against one lease record, and a sweep in between sends the decision
  // round again.
  for (;;) {
    const lease = await waitForGc(store)
    const raw = await store.get(branchKey)
    if (raw === null || bytesEqual(raw, recovered)) return false
    const commitHash = safeLoads(raw)
    if (typeof commitHash === 'string' && (await store.get(COMMIT_ROOT(commitHash))) !== null) {
      return false
    }
    const landed = await tryLand(
      store,
      lease,
      new Map([[branchKey, raw]]),
      new Map([[branchKey, recovered]]),
    )
    if (landed === null) continue
    if (!landed) return false
    console.warn(`kvgit: branch '${branch}' corrupt HEAD replaced with recovered commit`)
    return true
  }
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
 * writer fixed, or advanced, in the meantime.
 *
 * When that CAS does not win, the recovery candidate is stale and
 * returning it would name an older commit than HEAD actually holds.
 * The branch is re-resolved instead, so the answer describes the store
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
  const commitHash = await resolveHead(store, branch, opts)
  if (commitHash === null) return null
  if (await healHead(store, branch, dumps(commitHash))) return commitHash
  // The heal declined: another writer repaired, advanced or deleted
  // the branch in between, so the candidate is stale and returning it
  // would name an older commit than HEAD actually holds.
  return resolveHead(store, branch, opts)
}

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
  rejectReservedBranch(name)
  const branchKey = BRANCH_HEAD(name)
  // "The commit is here" and "the head names it" are decided against
  // one lease record: a sweep starting between the check and the write
  // makes the write fail, and the check runs again.
  for (;;) {
    const lease = await waitForGc(store)
    const expected = new Map<string, Uint8Array | null>([[branchKey, null]])
    const writes = new Map<string, Uint8Array>([[branchKey, dumps(target)]])
    if ((await store.get(COMMIT_ROOT(target))) === null) {
      if (target !== ROOT_COMMIT) throw new UnknownCommitError(target)
      // Minted with the head, and only if nobody minted it first.
      expected.set(COMMIT_ROOT(ROOT_COMMIT), null)
      const records = commitRecords({
        commit: ROOT_COMMIT,
        root: EMPTY_HASH,
        parents: [],
        parentGens: null,
        created: now(),
        info: null,
      })
      for (const [k, v] of records) writes.set(k, v)
    }
    const landed = await tryLand(store, lease, expected, writes, [BRANCH_HEAD_PREV(name)])
    if (landed === true) return
    if (landed === false && (await store.get(branchKey)) !== null) {
      throw new BranchExistsError(name)
    }
    // A sweep moved the lease, or another writer minted the root commit
    // first: decide again.
  }
}

/**
 * Delete branch `name`: its head and its backup, in one removal. Its
 * commits become collectable.
 *
 * The removal is conditional on the head it read. If the branch moved
 * or was made again in between, nothing is removed: a delete decided
 * on one head must not take a newer one with it. Read it again and
 * retry if the newer head should go too. A branch deleted and made
 * again at the *same* commit is indistinguishable, so the stale delete
 * removes it, as git's `update-ref -d <ref> <old-oid>` would; the state
 * it names is the one the delete saw.
 *
 * @throws UnknownBranchError if there is no such branch.
 * @throws ConcurrencyError if the branch changed while being deleted.
 */
export async function deleteBranchHead(store: KVStore, name: string): Promise<void> {
  rejectReservedBranch(name)
  const head = BRANCH_HEAD(name)
  const current = await store.get(head)
  if (current === null) throw new UnknownBranchError(name)
  const removed = await store.casMany(new Map([[head, current]]), new Map(), [
    head,
    BRANCH_HEAD_PREV(name),
  ])
  if (!removed) {
    throw new ConcurrencyError(`Branch '${name}' changed while being deleted; nothing was removed`)
  }
}

/**
 * The lowest common ancestor, from the two commits' whole histories:
 * for history written before generations were stored. On a criss-cross
 * tie the smallest hash wins: deterministic, but arbitrary.
 */
async function mergeBaseByWalk(
  commitA: string,
  commitB: string,
  loadParents: ParentLoader,
): Promise<string | null> {
  // Ancestor-set intersection with non-minimal candidates dropped
  // (a candidate that is itself an ancestor of another candidate is
  // not lowest). When several commits tie for lowest — criss-cross
  // histories — the smallest hash wins: deterministic, but
  // arbitrary, so criss-cross merges resolve cleanly rather than
  // raising.
  if (commitA === commitB) return commitA

  const parents = new Map<string, readonly string[]>()
  const ancestorsA = await walkAncestors(commitA, parents, loadParents)
  // Fast path: B inside A's history (or vice versa) names the
  // lowest directly — every other common ancestor sits above it.
  if (ancestorsA.has(commitB)) return commitB
  const ancestorsB = await walkAncestors(commitB, parents, loadParents)
  if (ancestorsB.has(commitA)) return commitA

  const common = new Set<string>()
  for (const c of ancestorsA) {
    if (ancestorsB.has(c)) common.add(c)
  }
  if (common.size === 0) return null
  // Minimality in one bottom-up pass: a candidate is lowest when no
  // other candidate sits below it. Propagate "a candidate is
  // at-or-below here" from tips to roots over the in-memory parent
  // map — no further store reads, linear in the history.
  const children = new Map<string, string[]>()
  for (const node of parents.keys()) children.set(node, [])
  for (const [node, nodeParents] of parents) {
    for (const parent of nodeParents) {
      const kids = children.get(parent)
      if (kids !== undefined) kids.push(node)
    }
  }
  const below = new Map<string, boolean>()
  const remaining = new Map<string, number>()
  for (const [node, kids] of children) {
    below.set(node, false)
    remaining.set(node, kids.length)
  }
  const queue: string[] = []
  for (const [node, kids] of children) {
    if (kids.length === 0) queue.push(node)
  }
  while (queue.length > 0) {
    const node = queue.shift() as string
    const nodeParents = parents.get(node) ?? []
    for (const parent of nodeParents) {
      if (common.has(node) || below.get(node) === true) below.set(parent, true)
      const left = (remaining.get(parent) ?? 1) - 1
      remaining.set(parent, left)
      if (left === 0) queue.push(parent)
    }
  }
  let best: string | null = null
  for (const c of common) {
    if (below.get(c) !== true && (best === null || c < best)) best = c
  }
  return best
}

async function walkAncestors(
  start: string,
  parents: Map<string, readonly string[]>,
  loadParents: ParentLoader,
): Promise<Set<string>> {
  // All ancestors of `start` (itself included), recording each
  // visited commit's parents in `parents` for later passes.
  const ancestors = new Set<string>([start])
  const stack = [start]
  while (stack.length > 0) {
    const current = stack.pop() as string
    if (parents.has(current)) continue
    const nodeParents = await loadParents(current)
    parents.set(current, nodeParents)
    for (const parent of nodeParents) {
      if (!ancestors.has(parent)) {
        ancestors.add(parent)
        stack.push(parent)
      }
    }
  }
  return ancestors
}

const FROM_A = 1
const FROM_B = 2
const STALE = 4
const FROM_BOTH = FROM_A | FROM_B

/**
 * The lowest common ancestor, searching down from both commits at once.
 *
 * Commits are visited highest generation first, every commit of one
 * generation in one batched read. A commit is visited only after every
 * descendant the search reached, so the sides it was reached from are
 * final when it is popped. Reached from both, it is a common ancestor,
 * and a lowest one unless it was reached through a common ancestor
 * already found: finding one marks everything below it stale. The
 * search stops once every commit still queued is stale; for two tips
 * that forked recently, that is a few reads whatever the length of the
 * history below.
 *
 * @throws NoGenerations if it reaches a commit that stores no
 *   generations for its parents.
 */
async function mergeBaseByGeneration(
  store: KVStore,
  commitA: string,
  commitB: string,
  known: Map<string, number> | undefined,
): Promise<string | null> {
  const records = await loadRecords(store, [commitA, commitB])
  const gens = new Map<string, number>()
  for (const c of [commitA, commitB]) {
    const g = generationOfRecord(records.get(c) as CommitRecord)
    if (g === null) throw new NoGenerations()
    gens.set(c, g)
  }
  if (known !== undefined) for (const [c, g] of gens) known.set(c, g)
  const flags = new Map<string, number>([
    [commitA, FROM_A],
    [commitB, FROM_B],
  ])
  const levels = new Map<number, Set<string>>()
  const enqueue = (c: string, g: number): void => {
    const level = levels.get(g)
    if (level === undefined) levels.set(g, new Set([c]))
    else level.add(c)
  }
  enqueue(commitA, gens.get(commitA) as number)
  enqueue(commitB, gens.get(commitB) as number)
  const active = new Set([commitA, commitB])
  const found: string[] = []
  while (active.size > 0) {
    const levelGen = Math.max(...levels.keys())
    const level = levels.get(levelGen) as Set<string>
    levels.delete(levelGen)
    const unread = [...level].filter((c) => !records.has(c))
    if (unread.length > 0) {
      for (const [c, r] of await loadRecords(store, unread)) records.set(c, r)
    }
    for (const c of [...level].sort()) {
      active.delete(c)
      let f = flags.get(c) as number
      if ((f & FROM_BOTH) === FROM_BOTH && (f & STALE) === 0) {
        found.push(c)
        f |= STALE
        flags.set(c, f)
      }
      const { parents, gens: parentGens } = records.get(c) as CommitRecord
      if (parents.length > 0 && parentGens === null) throw new NoGenerations()
      for (let i = 0; i < parents.length; i++) {
        const parent = parents[i] as string
        const parentGen = (parentGens as readonly number[])[i] as number
        // Inconsistent data: search the slow way.
        if (parentGen >= levelGen) throw new NoGenerations()
        const before = flags.get(parent) ?? 0
        const after = before | f
        if (after === before) continue
        flags.set(parent, after)
        if (!gens.has(parent)) {
          gens.set(parent, parentGen)
          enqueue(parent, parentGen)
        }
        if ((after & STALE) !== 0) active.delete(parent)
        else active.add(parent)
      }
    }
  }
  if (known !== undefined) for (const [c, g] of gens) known.set(c, g)
  return found.length === 0 ? null : found.reduce((a, b) => (b < a ? b : a))
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
  if (commitA === commitB) return commitA
  try {
    return await mergeBaseByGeneration(store, commitA, commitB, known)
  } catch (e) {
    if (!(e instanceof NoGenerations)) throw e
    return mergeBaseByWalk(commitA, commitB, (h) => loadParents(store, h))
  }
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
  const now = opts.now ?? wallClock
  const ttl = opts.leaseTtl ?? GC_LEASE_TTL
  const wait = opts.wait ?? true
  // Before the lease, so a store this code mustn't touch comes out of
  // the call untouched.
  await assertStorageVersion(store)
  let held: HeldLease
  for (;;) {
    if (wait) await waitForGc(store)
    try {
      held = await acquireGcLease(store, ttl)
      break
    } catch (e) {
      if (wait && e instanceof GcBusy) continue
      throw e
    }
  }
  try {
    return await sweep(store, opts.minAge ?? 3600, opts.deep ?? false, now)
  } finally {
    await releaseGcLease(store, held, ttl)
  }
}

/** The mark-and-sweep behind {@link gcStore}. The caller holds the
 *  lease, so no commit batch lands while this runs. */
async function sweep(
  store: KVStore,
  minAge: number,
  deep: boolean,
  now: () => number,
): Promise<number> {
  const at = now()
  const cutoffTime = at - minAge

  const reachableCommits = new Set<string>()
  const reachableBlobs = new Set<string>()
  const reachableNodes = new Set<string>()

  // Walk one commit's keyset, accumulating reachable refs.
  const walkCommitForMarks = async (commitHash: string): Promise<void> => {
    const rootBytes = await store.get(COMMIT_ROOT(commitHash))
    if (rootBytes === null) return
    const root = loads(rootBytes) as string
    const ks = Keyset.fromRoot(store, root)
    // skipNodes is read at call time, then we add to it after — so
    // subtrees shared with already-visited commits are skipped, and
    // newly-discovered nodes become reachable for the next iteration.
    const [entries, newNodes] = await ks.walk(reachableNodes)
    for (const entry of entries.values()) reachableBlobs.add(entry.blob)
    for (const node of newNodes) reachableNodes.add(node)
  }

  const markFrom = async (tip: string): Promise<void> => {
    // allParents follows merge commits' second parents too.
    for await (const commit of walkHistory(tip, (h) => loadParents(store, h), {
      allParents: true,
    })) {
      if (reachableCommits.has(commit)) continue
      reachableCommits.add(commit)
      await walkCommitForMarks(commit)
    }
  }

  // In-flight markers first, branch heads second. Publishing a commit
  // installs its head and drops its marker in one atomic write, so a
  // commit published after its marker was read is under a head read
  // later, and one published before has no marker to miss; read the
  // other way round, a publish landing between the two reads would leave
  // the commit under neither.
  const staleMarkers: string[] = []
  const markerKeys: string[] = []
  for await (const k of store.keys(IN_FLIGHT_PREFIX)) markerKeys.push(k)
  for (const k of markerKeys) {
    if (markerLive(await store.get(k))) {
      await markFrom(k.slice(IN_FLIGHT_PREFIX.length))
    } else {
      // The writer never published or withdrew it; its commit is an
      // ordinary orphan now, governed by minAge.
      staleMarkers.push(k)
    }
  }

  // Every root is a branch head, tags included: a tag is a head under
  // the reserved `refs/tags/` name. Prefix scan returns only heads
  // (prev backups don't match).
  const headKeys: string[] = []
  for await (const k of store.keys(BRANCH_HEAD_PREFIX)) headKeys.push(k)
  for (const k of headKeys) {
    const branchName = k.slice(BRANCH_HEAD_PREFIX.length)
    // No `recoverFromCorruptHead` here, deliberately, even when this
    // handle carries one. GC must not decide reachability from a
    // guess: a wrong answer marks the wrong commits live, so real
    // garbage survives forever, and a guessed tip gets walked as
    // though it were this branch's own history, pinning another
    // branch's ancestry into this one's mark set. The sweep sees
    // only what the store actually claims — a branch whose HEAD
    // resolves is marked from its real HEAD, and one whose HEAD does
    // not resolve marks nothing and keeps its commits as young
    // orphans until `minAge` and an explicit `repairHead` settle
    // what it points at. The inconsistency with the read paths is
    // the point, not an oversight.
    const branchHead = await resolveHead(store, branchName)
    if (branchHead !== null) await markFrom(branchHead)
  }

  // Sweep phase: scan commit roots, partition into orphans (old enough
  // to delete) and young orphans (within the minAge window).
  const orphans: string[] = []
  const youngOrphanCommits: string[] = []
  const COMMIT_ROOT_PREFIX = '__commit_root__'

  for await (const k of store.keys(COMMIT_ROOT_PREFIX)) {
    const commitHash = k.slice(COMMIT_ROOT_PREFIX.length)
    if (commitHash.length === 0 || reachableCommits.has(commitHash)) continue
    const timeBytes = await store.get(COMMIT_TIME(commitHash))
    if (timeBytes === null) continue // no timestamp — be conservative, skip
    const ts = safeLoads(timeBytes)
    if (typeof ts !== 'number') continue
    if (ts < cutoffTime) {
      orphans.push(commitHash)
    } else {
      youngOrphanCommits.push(commitHash)
    }
  }

  // Young orphans are kept whole, inside the minAge window abandoned
  // work is given to linger, so everything they reference is marked too.
  for (const young of youngOrphanCommits) {
    await walkCommitForMarks(young)
  }

  // Collect everything to delete in one batch so the sweep is atomic
  // at the store level (defends against partial sweeps under crash).
  const allRemovals = new Set<string>(staleMarkers)
  const KEYSET_PREFIX = Keyset.DEFAULT_PREFIX

  // Every deletion candidate on this path comes from walking an
  // orphan's own keyset. Content is keyed by what it holds, so an
  // orphan's blob or node may be the very key a live commit uses; the
  // mark phase saw every live commit (published, in flight or young),
  // so "unmarked" is what makes a key safe to take, and no batch can
  // land to change that while the lease is held.
  //
  // `skipNodes` starts as the reachable set (subtrees shared with a
  // live commit or a young orphan must not be touched, and are the
  // bulk of the tree) and grows with each orphan's nodes, so a
  // subtree shared between two orphans is walked once and queued
  // once rather than per orphan.
  const skipNodes = new Set(reachableNodes)

  for (const orphan of orphans) {
    const orphanRootBytes = await store.get(COMMIT_ROOT(orphan))
    if (orphanRootBytes !== null) {
      try {
        const orphanRoot = loads(orphanRootBytes) as string
        const orphanKs = Keyset.fromRoot(store, orphanRoot)
        const [orphanEntries, orphanNodes] = await orphanKs.walk(skipNodes)
        for (const entry of orphanEntries.values()) {
          if (!reachableBlobs.has(entry.blob)) allRemovals.add(entry.blob)
        }
        for (const node of orphanNodes) {
          skipNodes.add(node)
          allRemovals.add(KEYSET_PREFIX + node)
        }
      } catch {
        // Deliberate catch-all: a damaged orphan must not stall the
        // sweep. We drop its payload and still reclaim its commit
        // metadata below. Narrowing this would let one corrupt
        // keyset block GC for the whole store.
        //
        // The cost is a leak: once the commit metadata is gone there
        // is no keyset left to find the orphan's nodes and blobs
        // through, so they survive until a `deepClean`.
      }
    }
    allRemovals.add(COMMIT_ROOT(orphan))
    allRemovals.add(PARENT_COMMIT(orphan))
    allRemovals.add(PARENT_GENS(orphan))
    allRemovals.add(COMMIT_TIME(orphan))
    allRemovals.add(INFO_KEY(orphan))
  }

  if (deep) {
    // Namespace scans: reclaim nodes and blobs no orphan keyset
    // points at (interrupted writes, crashes between a write and its
    // publish, a damaged orphan's content). Safe for the same reason
    // the rest of the sweep is: no batch lands while the lease is held,
    // so everything here was either marked or is garbage.
    for await (const k of store.keys(KEYSET_PREFIX)) {
      const nodeHash = k.slice(KEYSET_PREFIX.length)
      if (nodeHash.length > 0 && !reachableNodes.has(nodeHash)) {
        allRemovals.add(k)
      }
    }
    for await (const k of store.keys(BLOB_PREFIX)) {
      if (!reachableBlobs.has(k)) allRemovals.add(k)
    }
  }

  if (allRemovals.size > 0) {
    await store.removeMany(allRemovals)
  }
  return orphans.length
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
 * A commit attempt: the commit it started from, and the entry each key
 * it has changed held before (undefined where the key was absent), to
 * put back if it fails. Restoring costs what the attempt changed, not
 * the size of the keyset.
 */
interface Attempt {
  currentCommit: string
  /** The keyset root it started from (lazy handles). */
  root: string
  undo: Map<string, { blob: string | undefined; meta: MetaEntry | undefined }>
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
  /** The commit attempt in progress, if any: what to undo should it
   *  fail. */
  private attempt: Attempt | null = null
  private readonly recoverFromCorruptHead: CorruptHeadRecoverer | undefined
  private readonly now: () => number
  /** Generations of commits this handle has written or read. A
   *  commit's generation never changes, so nothing here goes stale. */
  private readonly generations = new Map<string, number>()
  /** Commits this handle has written and not yet published, with the
   *  bytes of each one's in-flight marker. */
  private inFlight = new Map<string, Uint8Array>()
  /** The lease record this handle's latest batch landed against: a
   *  publish expecting it can't land while a sweep runs. */
  private leaseSeen: Uint8Array | null = null
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
    rejectReservedBranch(branch)
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
    const lower = (policy: MergePolicy): string | number => {
      if (policy === MergeChoice.OURS) return 'ours'
      if (policy === MergeChoice.THEIRS) return 'theirs'
      hosted.push(policy as BytesMergeFn)
      return hosted.length - 1
    }
    const lowered = (rules: Map<string, MergePolicy>) =>
      [...rules].map(([k, policy]) => [k, lower(policy)])
    const defaultMerge = opts.defaultMerge ?? this.defaultMergeFn
    const current = this.currentCommitHash
    const generation = this.generations.get(current)
    const args = {
      branch: this.branch,
      base: this.baseCommitHash,
      current,
      updates,
      removals: [...(opts.removals ?? [])].sort(compareCodePoints),
      chunks: [],
      onMoved,
      onConflict,
      mergeFns: lowered(new Map([...this.mergeFns, ...(opts.mergeFns ?? [])])),
      mergePrefixes: lowered(new Map([...this.mergePrefixes, ...(opts.mergePrefixes ?? [])])),
      defaultMerge: defaultMerge === null ? null : lower(defaultMerge),
      recover: this.recoverFromCorruptHead !== undefined,
      // Opening checked the store is stamped v4, the only version read.
      stamped: true,
      roots: this.nodeCache === null ? [] : [[current, this.root]],
      generations: generation === undefined ? [] : [[current, generation]],
    }
    const info = opts.info ?? null
    const c = await core()
    const memo = this.nodeCache === null ? uncached(c) : this.nodeCache.memo(c)
    const op = new c.CommitOp(JSON.stringify(args), info === null ? undefined : pyDumps(info), memo)
    let done: Committed
    try {
      done = await driveCommit(op, {
        store: this.store,
        held,
        mergeFns: hosted,
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

  /** Open a commit attempt: from here the key maps record what they
   *  change, so a failure can put it back. */
  protected snapshotState(): Attempt {
    this.attempt = { currentCommit: this.currentCommitHash, root: this.root, undo: new Map() }
    return this.attempt
  }

  /**
   * Restore in-memory state after a failed attempt: each key the
   * attempt changed gets its old entry back. The attempt's commits are
   * abandoned, so their in-flight markers are withdrawn and they become
   * ordinary orphans. Best effort: a marker left behind lapses after
   * `IN_FLIGHT_TTL`.
   */
  protected restoreState(saved: unknown): void {
    const s = saved as Attempt
    // Only the open attempt is undone. Loading another commit closes
    // it, and that state is newer than anything the attempt saved.
    if (this.attempt === s) {
      if (this.nodeCache !== null) {
        // A lazy handle's whole state is its root.
        this.root = s.root
      } else {
        const keys = this.ownedKeys()
        for (const [key, prev] of s.undo) {
          if (prev.blob === undefined) keys.delete(key)
          else keys.set(key, prev.blob)
          if (prev.meta === undefined) this.meta.delete(key)
          else this.meta.set(key, prev.meta)
        }
      }
      this.currentCommitHash = s.currentCommit
      this.attempt = null
    }
    if (this.inFlight.size > 0) {
      // Each marker goes only if it still holds the bytes this attempt
      // wrote: another writer that produced the same commit may have
      // overwritten it, and its protection must stay.
      const withdrawn = [...this.inFlight].map(([commit, marker]) => {
        const key = IN_FLIGHT_KEY(commit)
        return this.store.casMany(new Map([[key, marker]]), new Map(), [key])
      })
      this.inFlight = new Map()
      Promise.all(withdrawn).catch(() => {})
    }
  }

  protected override rollBackAttempt(): void {
    if (this.attempt !== null) this.restoreState(this.attempt)
  }

  protected override endAttempt(): void {
    this.attempt = null
  }

  /**
   * Point `key` at `blob`, or remove it (null), in place. The open
   * attempt records the entry it held first, so a failure can restore
   * it. Called only once a commit's batch has landed, never while the
   * attempt can still fail before it.
   */
  private setEntry(key: string, entry: { blob: string; meta: MetaEntry } | null): void {
    // A lazy handle holds no key maps: the new root is the change.
    if (this.nodeCache !== null) return
    const keys = this.ownedKeys()
    if (this.attempt !== null && !this.attempt.undo.has(key)) {
      this.attempt.undo.set(key, { blob: keys.get(key), meta: this.meta.get(key) })
    }
    if (entry === null) {
      keys.delete(key)
      this.meta.delete(key)
    } else {
      keys.set(key, entry.blob)
      this.meta.set(key, entry.meta)
    }
  }

  /**
   * Write a commit's batch, marked in flight, while no sweep runs. The
   * batch is conditional on the lease record read after waiting any
   * live sweep out, so it lands before a sweep starts or after one ends,
   * never during; landing before is safe because the marker makes every
   * later sweep treat the commit as live until it is published.
   */
  private async landBatch(commit: string, writes: Map<string, Uint8Array>): Promise<void> {
    const marker = inFlightMarker()
    writes.set(IN_FLIGHT_KEY(commit), marker)
    for (;;) {
      const lease = await waitForGc(this.store)
      if (await this.store.casMany(new Map([[GC_LEASE_KEY, lease]]), writes)) {
        this.inFlight.set(commit, marker)
        this.leaseSeen = lease
        return
      }
    }
  }

  protected async createCommit(opts: {
    updates?: Map<string, Uint8Array>
    removals?: Set<string>
    info?: CommitInfo
  }): Promise<string> {
    const updates = opts.updates ?? new Map<string, Uint8Array>()
    const removals = opts.removals ?? new Set<string>()
    const info = opts.info ?? null

    // Every blob is written, even one already stored: the key is its
    // content, so the rewrite changes nothing but guarantees the batch
    // holds everything its commit needs.
    const writes = new Map<string, Uint8Array>()
    const keysetUpdates: Array<[string, KeysetEntry]> = []
    for (const [key, value] of updates) {
      const pointer = await blobKey(value)
      writes.set(pointer, value)
      keysetUpdates.push([key, { blob: pointer, meta: { size: value.length } }])
    }

    // Apply the changes to the parent's HAMT: structural sharing reuses
    // every subtree the commit didn't touch.
    const newKs = await this.applyToKeyset(keysetUpdates, removals)
    for (const [k, v] of newKs.pending) writes.set(k, v)

    // The hash is computed last, over the parents, the root, the time
    // and the info, so it names exactly one root.
    const parents = [this.currentCommitHash]
    const parentGens = await this.parentGenerations(parents)
    const created = this.now()
    const newHash = await commitHash(parents, newKs.root, created, info)
    const records = commitRecords({
      commit: newHash,
      root: newKs.root,
      parents,
      parentGens,
      created,
      info,
    })
    for (const [k, v] of records) writes.set(k, v)

    // One batch, so a crash mid-write doesn't strand a partial commit.
    await this.landBatch(newHash, writes)

    // Landed: change the key maps in place, removals first so an update
    // to a removed key wins. Nothing between here and the return awaits,
    // so no reader sees a half-applied commit.
    for (const key of removals) this.setEntry(key, null)
    for (const [key, entry] of keysetUpdates) this.setEntry(key, entry)
    this.adoptRoot(newKs)
    this.currentCommitHash = newHash
    this.generations.set(newHash, generationOf(parentGens))
    return newHash
  }

  /** A commit's keyset root, or the empty tree's for a commit with none. */
  private async rootOf(commit: string): Promise<string> {
    return rootOfCommit(this.store, commit)
  }

  /**
   * The current keyset with `updates` and `removals` applied, its new
   * nodes pending. An eager handle applies them one key at a time; a
   * lazy one loads every changed key's path a level at a time through
   * the node cache, then rewrites each node on those paths once.
   */
  private async applyToKeyset(
    updates: Iterable<readonly [string, KeysetEntry]>,
    removals: Iterable<string>,
  ): Promise<Keyset> {
    if (this.nodeCache === null) {
      const parentKs = Keyset.fromRoot(this.store, await this.rootOf(this.currentCommitHash))
      return parentKs.updated({ updates, removals })
    }
    const ks = this.lazyKeyset()
    const updateList = [...updates]
    const removalList = [...removals]
    await ks.getMany([...updateList.map(([k]) => k), ...removalList])
    return ks.updatedBatch({ updates: updateList, removals: removalList })
  }

  /** A lazy handle moves to a keyset it just landed: its root, with the
   *  new nodes in the cache, where the next reads start. Synchronous, so
   *  the root and the commit change together. */
  private adoptRoot(ks: Keyset): void {
    if (this.nodeCache === null) return
    for (const [k, bytes] of ks.pending) {
      this.nodeCache.setBytes(k.slice(Keyset.DEFAULT_PREFIX.length), bytes)
    }
    this.root = ks.root
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

  /**
   * A commit from a resolved merge: our state with the resolution's
   * changes applied. An entry carried from either side keeps its own
   * metadata, which describes its blob; a value a merge function
   * produced is a new blob, never chunked.
   */
  protected async createMergeCommit(
    resolution: MergeResolution,
    parents: readonly string[],
    info: CommitInfo | null,
  ): Promise<string> {
    const writes = new Map<string, Uint8Array>()
    const updates = new Map(resolution.updates)
    for (const [key, value] of resolution.mergedValues) {
      const pointer = await blobKey(value)
      writes.set(pointer, value)
      updates.set(key, { blob: pointer, meta: { size: value.length } })
    }

    // Apply the changes on top of our HAMT, so structural sharing keeps
    // every subtree the merge did not touch.
    const newKs = await this.applyToKeyset(updates, resolution.removals)
    for (const [k, v] of newKs.pending) writes.set(k, v)

    const parentGens = await this.parentGenerations(parents)
    const created = this.now()
    const mergeHash = await commitHash(parents, newKs.root, created, info)
    const records = commitRecords({
      commit: mergeHash,
      root: newKs.root,
      parents,
      parentGens,
      created,
      info,
    })
    for (const [k, v] of records) writes.set(k, v)
    await this.landBatch(mergeHash, writes)

    // Landed: change the key maps in place, without awaiting.
    for (const [key, entry] of updates) this.setEntry(key, entry)
    for (const key of resolution.removals) this.setEntry(key, null)
    this.adoptRoot(newKs)
    this.currentCommitHash = mergeHash
    this.generations.set(mergeHash, generationOf(parentGens))
    return mergeHash
  }

  /**
   * Move HEAD from our head to `theirHead`, a descendant of it, writing
   * no commit. The write expects HEAD to hold our head: a branch that
   * moved meanwhile fails it as a merge commit's publish would, and a
   * deleted one is not recreated.
   */
  protected async fastForward(
    theirHead: string,
    onConflict: ConflictDisposition,
  ): Promise<MergeResult> {
    const ourHead = this.currentCommitHash
    const carried = [...(await this.changes(ourHead, theirHead)).keys()].sort(compareCodePoints)
    const branchKey = BRANCH_HEAD(this.branch)
    const expected = dumps(ourHead)
    const writes = new Map([
      [branchKey, dumps(theirHead)],
      [BRANCH_HEAD_PREV(this.branch), expected],
    ])
    // The existence check and the write are decided against one lease
    // record, so a sweep can't take the commit between them.
    let moved = false
    for (;;) {
      const lease = await waitForGc(this.store)
      if ((await this.store.get(COMMIT_ROOT(theirHead))) === null) {
        throw new UnknownCommitError(theirHead)
      }
      const landed = await tryLand(this.store, lease, new Map([[branchKey, expected]]), writes)
      if (landed === null) continue
      if (landed) break
      if ((await this.store.get(branchKey)) === null) throw new UnknownBranchError(this.branch)
      if (await healHead(this.store, this.branch, expected)) continue
      moved = true
      break
    }
    if (moved) {
      if (onConflict === 'abandon') {
        return {
          merged: false,
          commit: null,
          strategy: 'fast_forward',
          autoMergedKeys: [],
          carriedKeys: [],
        }
      }
      throw new ConcurrencyError('HEAD changed during merge. Refresh and retry.')
    }
    await this.loadCommitInto(theirHead, true)
    return {
      merged: true,
      commit: theirHead,
      strategy: 'fast_forward',
      autoMergedKeys: [],
      carriedKeys: carried,
    }
  }

  /**
   * Publish `newHead` as this branch's HEAD if HEAD is `expected`.
   *
   * One atomic write moves HEAD, records `expected` as the backup, and
   * removes the in-flight markers of the commits being published, so
   * the backup always names the commit HEAD held immediately before,
   * and a published commit never keeps a marker, nor loses it before
   * its head lands.
   *
   * The write also expects the lease record, so it can't land while a
   * sweep runs (a head installed under a sweep could name a commit the
   * sweep is about to take); after a sweep it waits and tries again. And
   * it expects each marker to hold the bytes this handle wrote: a marker
   * only disappears early if it lapsed and a sweep reaped it, possibly
   * with the commit, and then the publish fails rather than naming a
   * commit that may be gone.
   *
   * A write that fails against a *damaged* HEAD is retried behind
   * {@link healHead}, which repairs it atomically.
   */
  protected async casHead(expected: string, newHead: string): Promise<boolean> {
    const branchKey = BRANCH_HEAD(this.branch)
    const expectedBytes = dumps(expected)
    const writes = new Map([
      [branchKey, dumps(newHead)],
      [BRANCH_HEAD_PREV(this.branch), expectedBytes],
    ])
    const markers = new Map([...this.inFlight].map(([c, m]) => [IN_FLIGHT_KEY(c), m] as const))
    let lease = this.inFlight.size > 0 ? this.leaseSeen : await waitForGc(this.store)
    for (;;) {
      const expect = new Map<string, Uint8Array | null>([
        [GC_LEASE_KEY, lease],
        [branchKey, expectedBytes],
        ...markers,
      ])
      if (await this.store.casMany(expect, writes, markers.keys())) break
      if (!sameBytes(await this.store.get(GC_LEASE_KEY), lease)) {
        // A sweep ran, or is running: wait it out and try again. A
        // marker it reaped fails the next attempt on its own.
        lease = await waitForGc(this.store)
        continue
      }
      const present = await this.store.getMany(markers.keys())
      for (const [key, marker] of markers) {
        if (!sameBytes(present.get(key) ?? null, marker)) return false
      }
      if (!(await healHead(this.store, this.branch, expectedBytes))) return false
    }
    this.inFlight = new Map()
    return true
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

  protected async hasCommit(commit: string): Promise<boolean> {
    return (await this.store.get(COMMIT_ROOT(commit))) !== null
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
    rejectReservedBranch(name)
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
    const branchKey = BRANCH_HEAD(this.branch)
    // The existence check and the write are decided against one lease
    // record: this either resets onto a commit that loads or reports it
    // gone.
    for (;;) {
      const lease = await waitForGc(this.store)
      if ((await this.store.get(COMMIT_ROOT(commitHash))) === null) return false
      const current = await this.store.get(branchKey)
      if (current === null) throw new UnknownBranchError(this.branch)
      const landed = await tryLand(
        this.store,
        lease,
        new Map([[branchKey, current]]),
        new Map([
          [branchKey, dumps(commitHash)],
          [BRANCH_HEAD_PREV(this.branch), current],
        ]),
      )
      if (landed === true) break
    }
    await this.loadCommitInto(commitHash, true)
    return true
  }

  async listBranches(): Promise<string[]> {
    return listBranches(this.store)
  }

  static async exists(store: KVStore, name: string): Promise<boolean> {
    // Same predicate listBranches lists by: the branch has a HEAD
    // entry. A single read that never writes, so checking cannot
    // resurrect a deleted branch. A damaged HEAD still counts as
    // existing; opening it raises either way.
    if (name.startsWith(TAG_BRANCH_PREFIX)) return false
    return (await store.get(BRANCH_HEAD(name))) !== null
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
      this.attempt = null
      return
    }
    const { commitKeys, meta } = await populateState(this.store, commitHash)
    // The commit and its keys change together, after the read, so no
    // reader sees one commit's hash over another's keys.
    this.currentCommitHash = commitHash
    if (updateBase) this.baseCommitHash = commitHash
    this.replaceKeys(commitKeys)
    this.meta = meta
    this.attempt = null
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
