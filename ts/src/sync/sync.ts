/**
 * Sync orchestration: pull / push / sync a branch against a Remote.
 *
 * Policy (v1, deliberate):
 * - **Fast-forward only.** A ref moves only along its own ancestry —
 *   local on pull, remote on push. When both sides hold commits the
 *   other lacks, the result is `'diverged'`: objects may transfer
 *   (useful for a later merge), but no ref moves and nothing merges.
 *   Reconciliation is an explicit, local act the caller performs with
 *   kvgit's existing three-way machinery, then syncs again.
 * - **Lifecycle conflicts surface, never resolve silently.** A remote
 *   ref that vanishes after a prior sync (`'remote-gone'`) means
 *   someone deleted/archived the branch elsewhere; the caller decides.
 * - **CAS races retry once.** A lost push CAS means the remote moved
 *   between classification and push; one re-classification handles
 *   the benign case (someone else pushed commits we already have),
 *   anything else is `'diverged'`.
 *
 * Remote-tracking (`getSyncHead`) records the last head known shared
 * with the remote, per remote when it has an `id`. It advances on
 * successful syncs and is the `have` frontier for delta computation;
 * it never advances on divergence.
 *
 * Note: a fast-forwarded local ref does not refresh live `VersionedKV`
 * instances — callers holding one should `refresh()` after a pull
 * that reports movement.
 */

import type { KVStore, WireCommit } from '../types'
import { isAncestor } from '../versioned/ancestry'
import { COMMIT_ROOT, readBranchHead } from '../versioned/layout'
import {
  applyWire,
  claimSyncHead,
  clearSyncHead,
  markSyncing,
  publishHead,
  setSyncHead,
} from './apply'
import type { Remote } from './remote'
import { walkDelta } from './walk'

export type SyncStatus =
  | 'up-to-date'
  | 'fast-forwarded' // pull moved the local ref
  | 'pushed' // push moved the remote ref
  | 'created' // pull created the local branch / push created the remote one
  | 'diverged' // both sides hold unshared commits; no refs moved
  | 'remote-gone' // remote ref vanished after a prior sync (lifecycle conflict)

export interface SyncResult {
  status: SyncStatus
  branch: string
  /** Local branch head after the operation (null = branch absent). */
  localHead: string | null
  /** Remote branch head as last observed (null = ref absent). */
  remoteHead: string | null
  /** Wire commits transferred (applied on pull, sent on push). */
  transferred: number
}

/** Combined pull-then-push outcome from `syncBranch`. */
export interface SyncOutcome {
  /** The most significant of the two phases; `'diverged'` /
   *  `'remote-gone'` short-circuit (push is skipped, `push: null`). */
  status: SyncStatus
  pull: SyncResult
  push: SyncResult | null
}

/**
 * Pull `branch` from `remote`: fetch missing commits and fast-forward
 * (or create) the local branch ref.
 */
export async function pullBranch(
  store: KVStore,
  remote: Remote,
  branch: string,
): Promise<SyncResult> {
  const remoteHead = await readRemoteHead(remote, branch)
  const localHead = await readBranchHead(store, branch)
  const syncHead = await claimSyncHead(store, branch, remote)
  const result = (status: SyncStatus, transferred = 0): SyncResult => ({
    status,
    branch,
    localHead,
    remoteHead,
    transferred,
  })

  if (remoteHead === null) {
    // Nothing remote: a prior sync means the ref was deleted/archived
    // elsewhere; otherwise there's simply nothing to pull.
    return syncHead !== null ? result('remote-gone') : result('up-to-date')
  }
  if (remoteHead === localHead) {
    if (syncHead !== remoteHead) await setSyncHead(store, branch, remoteHead, remote)
    return result('up-to-date')
  }

  // Local strictly ahead (remote head already in our ancestry):
  // nothing to pull — advancing the remote is pushBranch's job. The
  // remote head is observably shared, so the tracking state may
  // advance to it.
  if (
    localHead !== null &&
    (await store.get(COMMIT_ROOT(remoteHead))) !== null &&
    (await isAncestor(store, remoteHead, localHead))
  ) {
    if (syncHead !== remoteHead) await setSyncHead(store, branch, remoteHead, remote)
    return result('up-to-date')
  }

  // Fetch the delta relative to everything we can claim: not the sync
  // head of a branch deleted here, once a sweep took its commits.
  const have: string[] = []
  if (localHead !== null) have.push(localHead)
  if (
    syncHead !== null &&
    syncHead !== localHead &&
    (await store.get(COMMIT_ROOT(syncHead))) !== null
  ) {
    have.push(syncHead)
  }
  const applied = await applyWire(store, remote.fetch(remoteHead, have))

  // applyWire verifies every commit IN the stream, but an empty or
  // truncated stream verifies vacuously — guard the head's presence
  // before any ref moves, or a hollow fetch would mint a branch
  // pointing at nothing (mirrors MemoryRemote.push's guard).
  if ((await store.get(COMMIT_ROOT(remoteHead))) === null) {
    throw new Error(`pullBranch: fetched head ${remoteHead} not present after apply`)
  }

  const publish = { inFlight: applied.inFlight }
  if (localHead === null) {
    const ok = await publishHead(store, branch, null, remoteHead, publish)
    if (!ok) return result('diverged', applied.applied) // local creation race
    await setSyncHead(store, branch, remoteHead, remote)
    return { ...result('created', applied.applied), localHead: remoteHead }
  }

  if (await isAncestor(store, localHead, remoteHead)) {
    // Fast-forward: HEAD and its backup in one conditional write, as
    // VersionedKV's casHead does, so a pull that loses the race to a
    // local writer changes nothing.
    const ok = await publishHead(store, branch, localHead, remoteHead, publish)
    if (!ok) return result('diverged', applied.applied) // local writer race
    await setSyncHead(store, branch, remoteHead, remote)
    return { ...result('fast-forwarded', applied.applied), localHead: remoteHead }
  }

  // Both sides advanced. Objects are stored (handy for a later
  // merge); refs and sync head stay put.
  return result('diverged', applied.applied)
}

/**
 * Push `branch` to `remote`: send missing commits and fast-forward
 * (or create) the remote branch ref via CAS.
 */
export async function pushBranch(
  store: KVStore,
  remote: Remote,
  branch: string,
): Promise<SyncResult> {
  if ((await readBranchHead(store, branch)) === null) {
    throw new Error(`pushBranch: local branch '${branch}' does not exist`)
  }
  // marked before the head is read: a delete unless synced lands before
  // (and the branch is gone) or keeps it
  await markSyncing(store, branch)
  const localHead = await readBranchHead(store, branch)
  if (localHead === null) {
    throw new Error(`pushBranch: local branch '${branch}' was deleted`)
  }
  const syncHead = await claimSyncHead(store, branch, remote)

  // One retry: a lost CAS means the remote moved under us; re-observe
  // and re-classify once before declaring divergence. Hoisted so the
  // exhausted-retries return reuses the last observation instead of a
  // third listRefs round trip (slightly stale is fine for a report).
  let remoteHead: string | null = null
  for (let attempt = 0; attempt < 2; attempt++) {
    remoteHead = await readRemoteHead(remote, branch)
    const result = (status: SyncStatus, transferred = 0): SyncResult => ({
      status,
      branch,
      localHead,
      remoteHead,
      transferred,
    })

    if (remoteHead === localHead) {
      if (syncHead !== localHead) await setSyncHead(store, branch, localHead, remote)
      return result('up-to-date')
    }

    if (remoteHead === null) {
      if (syncHead !== null) return result('remote-gone')
      const counter = { n: 0 }
      const ok = await remote.push(
        branch,
        null,
        localHead,
        counted(walkDelta(store, { want: localHead }), counter),
      )
      if (ok) {
        await setSyncHead(store, branch, localHead, remote)
        return result('created', counter.n)
      }
      continue // creation race — re-classify
    }

    // Remote moved past our tracking: safe only if its head is
    // already in our ancestry (e.g. another device pushed, we pulled,
    // then committed on top — or a sibling tab pushed for us).
    if (
      (await store.get(COMMIT_ROOT(remoteHead))) !== null &&
      (await isAncestor(store, remoteHead, localHead))
    ) {
      const counter = { n: 0 }
      const ok = await remote.push(
        branch,
        remoteHead,
        localHead,
        counted(walkDelta(store, { want: localHead, have: [remoteHead] }), counter),
      )
      if (ok) {
        await setSyncHead(store, branch, localHead, remote)
        return result('pushed', counter.n)
      }
      continue // CAS race — re-classify
    }

    return result('diverged')
  }

  return {
    status: 'diverged',
    branch,
    localHead,
    remoteHead,
    transferred: 0,
  }
}

/**
 * Pull then push. Divergence or a lifecycle conflict on the pull
 * short-circuits (push is skipped — pushing would fail the same way).
 */
export async function syncBranch(
  store: KVStore,
  remote: Remote,
  branch: string,
): Promise<SyncOutcome> {
  const pull = await pullBranch(store, remote, branch)
  if (pull.status === 'diverged' || pull.status === 'remote-gone') {
    return { status: pull.status, pull, push: null }
  }
  const push = await pushBranch(store, remote, branch)
  const status = push.status === 'up-to-date' ? pull.status : push.status
  return { status, pull, push }
}

export type DeleteStatus =
  | 'deleted'
  | 'moved' // the remote ref isn't where it was expected; nothing deleted
  | 'gone' // the remote ref was already gone

export interface DeleteResult {
  status: DeleteStatus
  branch: string
  /** Remote branch head as last observed (null = ref absent). */
  remoteHead: string | null
}

/**
 * Delete `branch` on `remote`, but only if nobody has pushed to it
 * since this store last synced it: the remote ref must be at the head
 * the branch last synced to there, or at `opts.expected`. When the ref
 * moved, nothing is deleted (`'moved'`): pull what was pushed first.
 *
 * Once the ref is gone, deleted here or before, the record of what the
 * branch last synced to there goes too, so a later push of the same
 * name creates it afresh rather than reporting `'remote-gone'`. The
 * local branch is left alone.
 *
 * @throws if the remote can't delete branches, or the branch never
 *   synced with it and no `expected` is given.
 */
export async function deleteRemoteBranch(
  store: KVStore,
  remote: Remote,
  branch: string,
  opts: { expected?: string } = {},
): Promise<DeleteResult> {
  if (remote.deleteBranch === undefined) {
    throw new Error("deleteRemoteBranch: this remote can't delete branches")
  }
  const expected = opts.expected ?? (await claimSyncHead(store, branch, remote))
  if (expected === null) {
    throw new Error(
      `deleteRemoteBranch: '${branch}' never synced with this remote; pass the head to expect`,
    )
  }
  // One retry: the ref can move without its head moving (a repack
  // writes a new git commit at the same kvgit head).
  let remoteHead: string | null = expected
  for (let attempt = 0; attempt < 2 && remoteHead === expected; attempt++) {
    if (await remote.deleteBranch(branch, expected)) {
      await clearSyncHead(store, branch, remote)
      return { status: 'deleted', branch, remoteHead: null }
    }
    remoteHead = await readRemoteHead(remote, branch)
  }
  if (remoteHead === null) {
    await clearSyncHead(store, branch, remote)
    return { status: 'gone', branch, remoteHead }
  }
  return { status: 'moved', branch, remoteHead }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function readRemoteHead(remote: Remote, branch: string): Promise<string | null> {
  const refs = await remote.listRefs()
  return refs.find((r) => r.branch === branch)?.head ?? null
}

/** Pass-through that counts yielded wire commits (for `transferred`). */
async function* counted(
  iter: AsyncIterable<WireCommit>,
  counter: { n: number },
): AsyncIterable<WireCommit> {
  for await (const wc of iter) {
    counter.n++
    yield wc
  }
}
