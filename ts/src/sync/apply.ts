/**
 * Wire replay: apply `WireCommit`s to a local store.
 *
 * `applyWire` is the receiver side of every transfer (sync pull,
 * bundle import). Each commit's values are keyed by their bytes here,
 * and the core replays it: it rebuilds the keyset over the first
 * parent's (nodes never cross the wire), recomputes the commit hash
 * from the rebuilt root and the recorded time and info, and checks the
 * parents' generations, refusing the commit with an `IntegrityError`
 * before anything is written if either doesn't match.
 *
 * Requirements on the stream (which `walkDelta` guarantees):
 * - topological order: every parent precedes its children, except
 *   parents already present in the store (the receiver's frontier)
 * - complete deltas relative to each commit's first parent
 *
 * Idempotency: commits are content-addressed, so a commit whose
 * records already exist is skipped wholesale. Re-applying a stream is
 * a no-op apart from the optional branch creation.
 *
 * Remote-tracking state (`getSyncHead`/`setSyncHead`) lives here too:
 * the last commit a branch is known to share with a remote, kept per
 * remote. It is bookkeeping for the sync orchestration layer —
 * `applyWire` itself never touches it.
 */

import type { KVStore, WireCommit } from '../types'
import { callCore } from '../versioned/drive'
import { checkStorageVersion } from '../versioned/kv'
import { blobKey } from '../versioned/layout'

const wallClock = (): number => Date.now() / 1000

export interface ApplyWireOptions {
  /**
   * Create this branch pointing at the stream's final commit. Fails if
   * the branch already exists (import semantics — fast-forwarding an
   * existing branch is the sync orchestration layer's job, where the
   * CAS race is handled). No-op when the stream is empty.
   */
  createBranch?: string
}

export interface ApplyWireResult {
  /** Commits written by this call. */
  applied: number
  /** Commits skipped because their records already existed. */
  skipped: number
  /** The last commit in the stream (the transfer's `want`), or null
   *  for an empty stream. */
  head: string | null
  /** Commits written by this call and not yet under a head: each is
   *  marked in flight, so a sweep keeps it until the head that names it
   *  lands. Pass them to {@link publishHead}, which drops the markers in
   *  the same write; ones never published lapse. Empty when
   *  `createBranch` published them. */
  inFlight: readonly string[]
}

/**
 * Apply a stream of wire commits to `store`.
 *
 * Throws on: storage-version mismatch, a parent missing both from the
 * stream-so-far and the store, a failed integrity check, or a
 * `createBranch` collision. Already-applied commits stay applied if a
 * later commit throws — they're valid, content-addressed history.
 */
export async function applyWire(
  store: KVStore,
  commits: AsyncIterable<WireCommit> | Iterable<WireCommit>,
  opts: ApplyWireOptions = {},
): Promise<ApplyWireResult> {
  await checkStorageVersion(store)
  const inFlight: string[] = []
  let applied = 0
  let skipped = 0
  let head: string | null = null

  for await (const wc of commits) {
    head = wc.hash
    const held = new Map<string, Uint8Array>()
    const updates: Array<[string, string, number]> = []
    for (const [key, bytes] of wc.updates) {
      const blob = await blobKey(bytes)
      held.set(blob, bytes)
      updates.push([key, blob, bytes.length])
    }
    const args = {
      hash: wc.hash,
      parents: [...wc.parents],
      parent_gens: wc.parentGens === null ? null : [...wc.parentGens],
      time: wc.time,
      info: wc.info ?? null,
      updates,
      removals: [...wc.removals],
    }
    // Written or already here, it is marked in flight until the head
    // that names it is published.
    const replayed = await callCore('wire_replay', args, { store, held, now: wallClock })
    if (replayed === 'present') skipped++
    else applied++
    inFlight.push(wc.hash)
  }

  if (opts.createBranch !== undefined && head !== null) {
    if (!(await publishHead(store, opts.createBranch, null, head, { inFlight }))) {
      throw new Error(`applyWire: branch '${opts.createBranch}' already exists`)
    }
    return { applied, skipped, head, inFlight: [] }
  }

  return { applied, skipped, head, inFlight }
}

/**
 * Move `branch` from `expected` (null: create it) to `newHead`, with
 * the backup, while no sweep runs, dropping the in-flight markers of
 * the commits this publishes in the same write.
 *
 * @returns false if the branch isn't at `expected` (or exists, for a
 *   create).
 * @throws UnknownCommitError if `newHead` isn't in the store.
 */
export async function publishHead(
  store: KVStore,
  branch: string,
  expected: string | null,
  newHead: string,
  opts: { inFlight?: readonly string[] } = {},
): Promise<boolean> {
  const args = { branch, expected, head: newHead, in_flight: [...(opts.inFlight ?? [])] }
  return (await callCore('publish_head', args, { store, now: wallClock })) as boolean
}

// ---------------------------------------------------------------------------
// Remote-tracking heads
// ---------------------------------------------------------------------------

/**
 * Which remote a sync head belongs to: a `Remote` (or anything with its
 * optional `id`), or the id itself. A remote with an id keeps its own
 * record per branch, so one branch synced with two remotes, or with a
 * remote swapped for another, never takes one's head as the other's.
 * A remote without one shares the branch's single record.
 */
export type SyncHeadOwner = string | { readonly id?: string | undefined }

/** The remote a sync head belongs to, as the core takes it: null for
 *  none named. */
function remoteArg(owner: SyncHeadOwner | undefined): { id: string | null } | null {
  if (owner === undefined) return null
  return { id: (typeof owner === 'string' ? owner : owner.id) ?? null }
}

async function call(op: string, store: KVStore, args: object): Promise<unknown> {
  return callCore(op, args, { store, now: wallClock })
}

/**
 * Last commit `branch` is known to share with `remote`, or null if it
 * has never synced there.
 *
 * For a remote with an id, a branch with no record of its own reads the
 * branch's shared record: a store synced before per-remote records
 * holds its head there, and the first sync that records a head for the
 * remote retires it.
 *
 * Without `remote`, the answer is the branch's one sync head, whichever
 * remote it's with: the shared record, or else the per-remote record
 * when exactly one remote has one; null when none or several do.
 */
export async function getSyncHead(
  store: KVStore,
  branch: string,
  remote?: SyncHeadOwner,
): Promise<string | null> {
  return (await call('sync_head', store, { branch, remote: remoteArg(remote) })) as string | null
}

/** Whether `branch` has a sync head on record with any remote, or a
 *  push of it has begun: it syncs somewhere, and was not forgotten
 *  since. */
export async function hasSyncHead(store: KVStore, branch: string): Promise<boolean> {
  return (await call('has_sync_head', store, { branch })) as boolean
}

/** Mark `branch` as syncing, before a push reads it. */
export async function markSyncing(store: KVStore, branch: string): Promise<void> {
  await call('mark_syncing', store, { branch })
}

/**
 * Delete `branch` unless it syncs with some remote (`hasSyncHead`);
 * true if it went. The check and the delete are one step against a
 * sync taking the branch up: a push marks the branch before reading
 * it, so either the delete lands first and the push finds no branch,
 * or the delete sees the mark and keeps it.
 *
 * @throws UnknownBranchError if there is no such branch.
 */
export async function deleteBranchUnlessSynced(store: KVStore, branch: string): Promise<boolean> {
  return (await call('delete_branch_unless_synced', store, { name: branch })) as boolean
}

/** Record `commit` as the head `branch` last synced to on `remote`
 *  (the branch's shared record when it has no id). A remote's own
 *  record retires the shared one, so another remote can't take it up. */
export async function setSyncHead(
  store: KVStore,
  branch: string,
  commit: string,
  remote?: SyncHeadOwner,
): Promise<void> {
  await call('set_sync_head', store, { branch, commit, remote: remoteArg(remote) })
}

/**
 * The head `branch` last synced to on `remote`, as the sync layer reads
 * it: the shared record a store kept from before per-remote records is
 * taken up as this remote's own the first time it's read, so whichever
 * remote syncs first after an upgrade (the one it was kept for, in a
 * store that syncs with one) carries it on, and no other remote reads it.
 * The take is one step: of two remotes reading it at once, one has it.
 */
export async function claimSyncHead(
  store: KVStore,
  branch: string,
  remote: SyncHeadOwner,
): Promise<string | null> {
  return (await call('claim_sync_head', store, { branch, remote: remoteArg(remote) })) as
    | string
    | null
}

/**
 * Forget what `branch` last synced to on `remote` (e.g. on remote
 * detach), the shared record with it; without `remote`, everything it
 * last synced to on every remote.
 */
export async function clearSyncHead(
  store: KVStore,
  branch: string,
  remote?: SyncHeadOwner,
): Promise<void> {
  await call('clear_sync_head', store, { branch, remote: remoteArg(remote) })
}
