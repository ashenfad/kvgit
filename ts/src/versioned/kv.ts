/**
 * Operations on a store that need no worktree: resolving and repairing
 * heads, the storage-version check, creating and deleting branch heads,
 * the merge base, and the sweep. Each runs in the core.
 */

import type { KVStore } from '../types'
import { callCore } from './drive'
import { GC_LEASE_TTL } from './lease'

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

/**
 * Refuse a store stamped with another storage layout, or one with
 * branches and no stamp, from before stamps; stamp a fresh one. Every
 * entry point that writes a store it didn't open calls this first.
 *
 * @throws StorageVersionError for a store of another layout.
 */
export async function checkStorageVersion(store: KVStore): Promise<void> {
  await callCore('check_version', { stamp: true }, { store, now: wallClock })
}

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
 * It takes a raw `KVStore`, so it works with or without a worktree
 * open on the branch.
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
  const args = {
    min_age: opts.minAge ?? 3600,
    deep: opts.deep ?? false,
    wait: opts.wait ?? true,
    lease_ttl: opts.leaseTtl ?? GC_LEASE_TTL,
  }
  return (await callCore('gc', args, { store, now: opts.now ?? wallClock })) as number
}
