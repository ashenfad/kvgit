/**
 * The Remote protocol: how two kvgit histories exchange commits.
 *
 * A remote is a far peer that stores history and exposes refs — NOT a
 * `KVStore` backend (that's the near-storage boundary; see the README's
 * "Sync & remotes" section). The protocol is deliberately minimal:
 * list refs, fetch a delta, push a delta with compare-and-swap ref
 * advancement, and (optionally) delete a ref by the same CAS. Merge is
 * absent by design — reconciliation always happens locally, so a
 * remote can be *passive*: any object store with CAS-able refs
 * qualifies, and no kvgit code needs to run on the far side.
 *
 * `MemoryRemote` is the reference implementation (and test double): a
 * kvgit object store plus branch-head refs, composed directly from
 * `walkDelta` + `applyWire`. Real transports (GitHub Git Data API)
 * implement the same interface over the wire; bundles are the
 * degenerate case (a one-shot `fetch` with `have = ∅`).
 */

import type { KVStore, WireCommit } from '../types'
import {
  BRANCH_HEAD,
  BRANCH_HEAD_PREFIX,
  BRANCH_HEAD_PREV,
  COMMIT_ROOT,
  readBranchHead,
  safeLoads,
} from '../versioned/layout'
import { applyWire, publishHead } from './apply'
import { walkDelta } from './walk'

/** One remote branch pointer. */
export interface RemoteRef {
  branch: string
  /** kvgit commit hash at the remote branch's tip. */
  head: string
}

export interface Remote {
  /**
   * Who this remote is, the same from one run to the next (a
   * `PackRemote`'s is its repo): the head each branch last synced to
   * here is kept under it, apart from any other remote's. A remote
   * without one shares one such record per branch with every other
   * remote that has none.
   */
  readonly id?: string | undefined

  /** Current branch → head pointers on the remote. */
  listRefs(): Promise<RemoteRef[]>

  /**
   * Commits reachable from `want` but not from any of `have`, in
   * topological order (parents first). `have` hashes the remote can't
   * resolve contribute nothing to the exclusion (conservative
   * re-send — transfers are idempotent).
   */
  fetch(want: string, have: Iterable<string>): AsyncIterable<WireCommit>

  /**
   * Store `commits` and advance the branch ref to `newHead` iff the
   * ref currently equals `expectedOld` (`null` = "branch must not
   * exist"). Returns false — with the ref unmoved — when the CAS
   * loses; stored objects may remain (content-addressed, harmless).
   */
  push(
    branch: string,
    expectedOld: string | null,
    newHead: string,
    commits: AsyncIterable<WireCommit> | Iterable<WireCommit>,
  ): Promise<boolean>

  /**
   * Delete the branch ref iff it currently equals `expectedHead`.
   * Returns false, with nothing changed, when the ref is elsewhere or
   * gone. Optional: on a remote without it, branches stay.
   */
  deleteBranch?(branch: string, expectedHead: string): Promise<boolean>
}

/**
 * In-memory reference Remote: a kvgit object store with branch-head
 * refs, on any `KVStore` (tests pass `Memory`). Verifies integrity on
 * push as a side effect of replaying via `applyWire` — a property
 * passive transports won't have (their receivers verify on fetch
 * instead).
 */
export class MemoryRemote implements Remote {
  readonly store: KVStore
  readonly id: string | undefined

  constructor(store: KVStore, opts: { id?: string } = {}) {
    this.store = store
    this.id = opts.id
  }

  async listRefs(): Promise<RemoteRef[]> {
    const out: RemoteRef[] = []
    for await (const k of this.store.keys(BRANCH_HEAD_PREFIX)) {
      const branch = k.slice(BRANCH_HEAD_PREFIX.length)
      if (branch.length === 0) continue
      const head = await readBranchHead(this.store, branch)
      if (head !== null) out.push({ branch, head })
    }
    return out.sort((a, b) => (a.branch < b.branch ? -1 : 1))
  }

  fetch(want: string, have: Iterable<string>): AsyncIterable<WireCommit> {
    return walkDelta(this.store, { want, have })
  }

  async push(
    branch: string,
    expectedOld: string | null,
    newHead: string,
    commits: AsyncIterable<WireCommit> | Iterable<WireCommit>,
  ): Promise<boolean> {
    const applied = await applyWire(this.store, commits)
    // The stream must have delivered (or the store already held) the
    // commit the ref is about to point at.
    if ((await this.store.get(COMMIT_ROOT(newHead))) === null) {
      throw new Error(`MemoryRemote: pushed head ${newHead} not present after apply`)
    }
    return publishHead(this.store, branch, expectedOld, newHead, { inFlight: applied.inFlight })
  }

  async deleteBranch(branch: string, expectedHead: string): Promise<boolean> {
    if (branch.length === 0) return false
    const key = BRANCH_HEAD(branch)
    const current = await this.store.get(key)
    if (current === null || safeLoads(current) !== expectedHead) return false
    // its objects stay, for a sweep to collect once nothing names them
    return this.store.casMany(new Map([[key, current]]), new Map(), [key, BRANCH_HEAD_PREV(branch)])
  }
}
