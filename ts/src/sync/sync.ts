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
 * Note: a fast-forwarded local ref does not refresh open worktrees —
 * callers holding one should `refresh()` after a pull that reports
 * movement.
 */

import type { KVStore, WireCommit } from '../types'
import { callCore } from '../versioned/drive'
import { rejectReservedBranch } from '../versioned/refs'
import { applyWire, claimSyncHead, clearSyncHead, setSyncHead } from './apply'
import type { Remote } from './remote'
import { walkDelta } from './walk'

const wallClock = (): number => Date.now() / 1000

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
  /** A pull's local head after it (null = branch absent); a push's,
   *  the head it read and sent, whatever a local writer did since. */
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

/** The remote a sync head belongs to, as the core takes it. */
function remoteArg(remote: Remote): { id: string | null } {
  return { id: remote.id ?? null }
}

async function call(op: string, store: KVStore, args: object): Promise<unknown> {
  return callCore(op, args, { store, now: wallClock })
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
  const plan = (await call('pull_plan', store, {
    branch,
    remote: remoteArg(remote),
    remote_head: remoteHead,
  })) as { local_head: string | null; status: SyncStatus | null; have: string[] }
  const localHead = plan.local_head
  if (plan.status !== null || remoteHead === null) {
    return { status: plan.status ?? 'up-to-date', branch, localHead, remoteHead, transferred: 0 }
  }
  const applied = await applyWire(store, remote.fetch(remoteHead, plan.have))
  const done = (await call('pull_finish', store, {
    branch,
    remote: remoteArg(remote),
    remote_head: remoteHead,
    local_head: localHead,
    in_flight: [...applied.inFlight],
  })) as { status: SyncStatus; local_head: string | null }
  return {
    status: done.status,
    branch,
    localHead: done.local_head,
    remoteHead,
    transferred: applied.applied,
  }
}

/**
 * Push `branch` to `remote`: send missing commits and fast-forward
 * (or create) the remote branch ref via CAS.
 *
 * @throws UnknownBranchError if there is no such local branch.
 */
export async function pushBranch(
  store: KVStore,
  remote: Remote,
  branch: string,
): Promise<SyncResult> {
  const start = (await call('push_start', store, { branch, remote: remoteArg(remote) })) as {
    local_head: string
    sync_head: string | null
  }
  const localHead = start.local_head
  // One retry: a lost CAS means the remote moved under us; re-observe
  // and re-classify once before declaring divergence. The last
  // observation stands in the report after that.
  let remoteHead: string | null = null
  // Commits sent, over every attempt: a remote may keep what a push
  // that lost its compare-and-swap sent.
  const counter = { n: 0 }
  for (let attempt = 0; attempt < 2; attempt++) {
    remoteHead = await readRemoteHead(remote, branch)
    const plan = (await call('push_plan', store, {
      branch,
      remote: remoteArg(remote),
      local_head: localHead,
      sync_head: start.sync_head,
      remote_head: remoteHead,
    })) as { status: SyncStatus | null; expected: string | null; have: string[] }
    if (plan.status !== null) {
      return { status: plan.status, branch, localHead, remoteHead, transferred: counter.n }
    }
    const commits = counted(walkDelta(store, { want: localHead, have: plan.have }), counter)
    if (await remote.push(branch, plan.expected, localHead, commits)) {
      await setSyncHead(store, branch, localHead, remote)
      const status = plan.expected === null ? 'created' : 'pushed'
      return { status, branch, localHead, remoteHead, transferred: counter.n }
    }
  }
  return { status: 'diverged', branch, localHead, remoteHead, transferred: counter.n }
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
 * @throws RangeError for a name a branch can't have; Error if the
 *   remote can't delete branches, or the branch never synced with it and
 *   no `expected` is given.
 */
export async function deleteRemoteBranch(
  store: KVStore,
  remote: Remote,
  branch: string,
  opts: { expected?: string } = {},
): Promise<DeleteResult> {
  await rejectReservedBranch(branch)
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
