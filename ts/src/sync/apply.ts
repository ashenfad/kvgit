/**
 * Wire replay: apply `WireCommit`s to a local store.
 *
 * `applyWire` is the receiver side of every transfer (sync pull,
 * bundle import). It reconstructs each commit's records byte-exactly:
 * blob keys are derived from the bytes, the keyset HAMT is rebuilt
 * locally over the first parent's (nodes never cross the wire), and the
 * commit hash is recomputed from the rebuilt root and the recorded
 * time and info, and compared against the claimed hash — a mismatch
 * refuses the commit before anything is written.
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

import { UnknownBranchError } from '../errors'
import { EMPTY_HASH } from '../hamt'
import { Keyset } from '../keyset'
import type { KVStore, KeysetEntry, WireCommit } from '../types'
import {
  BRANCH_HEAD,
  BRANCH_HEAD_PREV,
  COMMIT_ROOT,
  GC_LEASE_KEY,
  IN_FLIGHT_KEY,
  PARENT_COMMIT,
  PARENT_GENS,
  ROOT_COMMIT,
  SYNC_HEAD,
  SYNC_HEADS_OF,
  SYNC_HEAD_AT,
  SYNC_MARK,
  blobKey,
  checkStorageVersion,
  commitHash,
  commitRecords,
  dumps,
  generationOf,
  loads,
  safeLoads,
} from '../versioned/layout'
import { inFlightMarker, tryLand, waitForGc } from '../versioned/lease'
import { rejectReservedBranch } from '../versioned/refs'

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
  const generations = new Map<string, number>()

  for await (const wc of commits) {
    head = wc.hash
    // Content-addressed: present means identical. But present isn't
    // reachable (it may be an orphan from an earlier fetch), so it is
    // marked in flight like a commit written here, until the head that
    // names it is published.
    if (await remark(store, wc.hash)) {
      inFlight.push(wc.hash)
      skipped++
      continue
    }

    // Rebuild the keyset over the first parent's (structural sharing
    // comes back for free), with each update's blob keyed by its bytes.
    const writes = new Map<string, Uint8Array>()
    const entryUpdates: Array<readonly [string, KeysetEntry]> = []
    for (const [key, bytes] of wc.updates) {
      const pointer = await blobKey(bytes)
      writes.set(pointer, bytes)
      entryUpdates.push([key, { blob: pointer, meta: { size: bytes.length } }])
    }
    const firstParent = wc.parents[0]
    const parentKs = Keyset.fromRoot(
      store,
      firstParent !== undefined ? await rootAt(store, firstParent) : EMPTY_HASH,
    )
    const newKs = await parentKs.updated({ updates: entryUpdates, removals: wc.removals })

    // The root commit's hash is fixed rather than derived, so it is
    // checked by its shape: no parents, no info, an empty keyset.
    const recomputed =
      wc.hash === ROOT_COMMIT
        ? wc.parents.length === 0 && newKs.root === EMPTY_HASH && wc.info == null
          ? ROOT_COMMIT
          : 'a root commit with content'
        : await commitHash(wc.parents, newKs.root, wc.time, wc.info)
    if (recomputed !== wc.hash) {
      throw new Error(
        `applyWire: integrity check failed for ${wc.hash} (recomputed ${recomputed}) — refusing commit`,
      )
    }

    // The hash doesn't cover parent generations, so they are checked
    // against the parents' own records: commits made on top of this one
    // count from them.
    const expectedGens =
      wc.parents.length === 0
        ? null
        : await Promise.all(wc.parents.map((p) => generationAt(store, p, generations)))
    if (dumpsText(wc.parentGens) !== dumpsText(expectedGens)) {
      throw new Error(
        `applyWire: parent generations ${dumpsText(wc.parentGens)} for ${wc.hash} do not match its parents' ${dumpsText(expectedGens)} — refusing commit`,
      )
    }

    // One batch per commit, as a commit is written: blobs, nodes, records.
    for (const [k, v] of newKs.pending) writes.set(k, v)
    const records = commitRecords({
      commit: wc.hash,
      root: newKs.root,
      parents: wc.parents,
      parentGens: wc.parentGens,
      created: wc.time,
      info: wc.info ?? null,
    })
    for (const [k, v] of records) writes.set(k, v)
    // Marked in flight and landed while no sweep runs: a commit no head
    // names yet must not be swept before one does.
    writes.set(IN_FLIGHT_KEY(wc.hash), inFlightMarker())
    for (;;) {
      const lease = await waitForGc(store)
      if (await store.casMany(new Map([[GC_LEASE_KEY, lease]]), writes)) break
    }
    inFlight.push(wc.hash)
    applied++
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
 * Give a commit already in the store a fresh in-flight marker, while no
 * sweep runs and conditional on its root still being there.
 *
 * @returns false if the commit isn't in the store (or a sweep took it
 *   meanwhile), so the caller writes it in full.
 */
async function remark(store: KVStore, commit: string): Promise<boolean> {
  for (;;) {
    const lease = await waitForGc(store)
    const root = await store.get(COMMIT_ROOT(commit))
    if (root === null) return false
    const landed = await tryLand(
      store,
      lease,
      new Map([[COMMIT_ROOT(commit), root]]),
      new Map([[IN_FLIGHT_KEY(commit), inFlightMarker()]]),
    )
    if (landed !== null) return landed
  }
}

/**
 * Move `branch` from `expected` (null: create it) to `newHead`, with
 * the backup, while no sweep runs, dropping the in-flight markers of
 * the commits this publishes in the same write.
 *
 * @returns false if the branch isn't at `expected` (or exists, for a
 *   create).
 */
export async function publishHead(
  store: KVStore,
  branch: string,
  expected: string | null,
  newHead: string,
  opts: { inFlight?: readonly string[] } = {},
): Promise<boolean> {
  const branchKey = BRANCH_HEAD(branch)
  const expectedBytes = expected === null ? null : dumps(expected)
  const writes = new Map([[branchKey, dumps(newHead)]])
  const removes = (opts.inFlight ?? []).map(IN_FLIGHT_KEY)
  if (expectedBytes === null) removes.push(BRANCH_HEAD_PREV(branch))
  else writes.set(BRANCH_HEAD_PREV(branch), expectedBytes)
  for (;;) {
    const lease = await waitForGc(store)
    // The head must name a commit that is here when it lands.
    if ((await store.get(COMMIT_ROOT(newHead))) === null) {
      throw new Error(`publishHead: commit ${newHead} is not in the store`)
    }
    const landed = await tryLand(
      store,
      lease,
      new Map([[branchKey, expectedBytes]]),
      writes,
      removes,
    )
    if (landed !== null) return landed
  }
}

/** A stored commit's generation: 0 with no parents, else one more than
 *  its highest parent's, as its recorded parent generations say. */
async function generationAt(
  store: KVStore,
  commit: string,
  cache: Map<string, number>,
): Promise<number> {
  const cached = cache.get(commit)
  if (cached !== undefined) return cached
  const parentsBytes = await store.get(PARENT_COMMIT(commit))
  if (parentsBytes === null) {
    throw new Error(
      `applyWire: parent ${commit} is neither in the stream nor the store (stream must be parents-first)`,
    )
  }
  const parents = loads(parentsBytes) as string[]
  let gen = 0
  if (parents.length > 0) {
    const gensBytes = await store.get(PARENT_GENS(commit))
    const gens = gensBytes === null ? null : safeLoads(gensBytes)
    if (!Array.isArray(gens) || gens.length !== parents.length) {
      throw new Error(`applyWire: commit ${commit} has no parent generations recorded`)
    }
    gen = generationOf(gens as number[])
  }
  cache.set(commit, gen)
  return gen
}

function dumpsText(value: unknown): string {
  return JSON.stringify(value ?? null)
}

/** A commit's keyset root; throws if the commit is absent, which in a
 *  parents-first stream means a parent neither sent nor held. */
async function rootAt(store: KVStore, commit: string): Promise<string> {
  const rootBytes = await store.get(COMMIT_ROOT(commit))
  if (rootBytes === null) {
    throw new Error(
      `applyWire: parent ${commit} is neither in the stream nor the store (stream must be parents-first)`,
    )
  }
  return loads(rootBytes) as string
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

function ownerId(owner: SyncHeadOwner): string | undefined {
  return typeof owner === 'string' ? owner : owner.id
}

async function readHead(store: KVStore, key: string): Promise<string | null> {
  const raw = await store.get(key)
  if (raw === null) return null
  const parsed = safeLoads(raw)
  return typeof parsed === 'string' ? parsed : null
}

async function keysUnder(store: KVStore, prefix: string): Promise<string[]> {
  const out: string[] = []
  for await (const k of store.keys(prefix)) out.push(k)
  return out
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
  const id = remote === undefined ? undefined : ownerId(remote)
  if (id !== undefined) {
    return (
      (await readHead(store, SYNC_HEAD_AT(branch, id))) ??
      (await readHead(store, SYNC_HEAD(branch)))
    )
  }
  const shared = await readHead(store, SYNC_HEAD(branch))
  if (shared !== null || remote !== undefined) return shared
  const keys = await keysUnder(store, SYNC_HEADS_OF(branch))
  return keys.length === 1 ? readHead(store, keys[0] as string) : null
}

const MARKED = dumps(true)

/** Whether `branch` has a sync head on record with any remote, or a
 *  push of it has begun: it syncs somewhere, and was not forgotten
 *  since. */
export async function hasSyncHead(store: KVStore, branch: string): Promise<boolean> {
  if ((await store.get(SYNC_MARK(branch))) !== null) return true
  if ((await store.get(SYNC_HEAD(branch))) !== null) return true
  for await (const _ of store.keys(SYNC_HEADS_OF(branch))) return true
  return false
}

/** Mark `branch` as syncing, before a push reads it. */
export async function markSyncing(store: KVStore, branch: string): Promise<void> {
  if ((await store.get(SYNC_MARK(branch))) === null) await store.set(SYNC_MARK(branch), MARKED)
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
  rejectReservedBranch(branch)
  const head = BRANCH_HEAD(branch)
  for (;;) {
    const current = await store.get(head)
    if (current === null) throw new UnknownBranchError(branch)
    if (await hasSyncHead(store, branch)) return false
    const expected = new Map<string, Uint8Array | null>([
      [head, current],
      [SYNC_MARK(branch), null],
      [SYNC_HEAD(branch), null],
    ])
    if (await store.casMany(expected, new Map(), [head, BRANCH_HEAD_PREV(branch)])) return true
    // it moved, or began syncing: look again
  }
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
  const id = remote === undefined ? undefined : ownerId(remote)
  if (id === undefined) {
    await store.casMany(
      new Map(),
      new Map([
        [SYNC_HEAD(branch), dumps(commit)],
        [SYNC_MARK(branch), MARKED],
      ]),
    )
    return
  }
  await store.casMany(
    new Map(),
    new Map([
      [SYNC_HEAD_AT(branch, id), dumps(commit)],
      [SYNC_MARK(branch), MARKED],
    ]),
    [SYNC_HEAD(branch)],
  )
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
  const id = ownerId(remote)
  if (id === undefined) return readHead(store, SYNC_HEAD(branch))
  const ownKey = SYNC_HEAD_AT(branch, id)
  const sharedKey = SYNC_HEAD(branch)
  for (;;) {
    const own = await readHead(store, ownKey)
    if (own !== null) return own
    const raw = await store.get(sharedKey)
    if (raw === null) return null
    const shared = safeLoads(raw)
    if (typeof shared !== 'string') return null
    const expected = new Map<string, Uint8Array | null>([
      [sharedKey, raw],
      [ownKey, null],
    ])
    if (await store.casMany(expected, new Map([[ownKey, raw]]), [sharedKey])) return shared
  }
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
  const id = remote === undefined ? undefined : ownerId(remote)
  if (id !== undefined) await store.remove(SYNC_HEAD_AT(branch, id))
  else if (remote === undefined) {
    for (const k of await keysUnder(store, SYNC_HEADS_OF(branch))) await store.remove(k)
  }
  await store.remove(SYNC_HEAD(branch))
  // the mark goes with the last head
  if ((await keysUnder(store, SYNC_HEADS_OF(branch))).length === 0) {
    await store.remove(SYNC_MARK(branch))
  }
}
