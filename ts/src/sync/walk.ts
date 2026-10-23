/**
 * Delta walk: translate a slice of kvgit history into wire form.
 *
 * `walkDelta` yields the commits reachable from `want` but not from
 * any commit in `have`, as `WireCommit`s in topological order (parents
 * before children). This is the sender side of every transfer:
 *
 *   - sync push: `have` = the remote-tracking head
 *   - sync fetch (served locally): `have` = the requester's heads
 *   - bundle export: `have` = ∅ (full history)
 *
 * Works directly on a `KVStore` using the v4 storage layout
 * (`versioned/layout.ts`) rather than a `Versioned` instance — the
 * walk is a pure read over committed state and shouldn't be entangled
 * with a working tree's branch/HEAD bookkeeping.
 *
 * Cost: O(delta) store reads, on history that stores generations
 * (`versioned/ancestry.ts`); older history is walked whole to find
 * what `have` holds. Per emitted commit, the keyset diff vs
 * the first parent is structural (`Keyset.diff` skips shared subtrees
 * by hash), so work is proportional to changed keys, not keyset size.
 */

import { Keyset } from '../keyset'
import { PyFloat } from '../pyjson'
import type { KVStore, KeysetEntry, WireCommit } from '../types'
import { ancestryDelta } from '../versioned/ancestry'
import {
  COMMIT_ROOT,
  COMMIT_TIME,
  INFO_KEY,
  PARENT_GENS,
  loads,
  loadsExact,
  safeLoads,
} from '../versioned/layout'

export interface WalkDeltaOptions {
  /** Commit hash to walk back from (typically a branch HEAD). */
  want: string
  /** Commits the receiver already has. Their full ancestries are
   *  excluded from the walk. Empty/omitted = full history. */
  have?: Iterable<string>
}

/** Loaded-per-commit record cache built during the reachability pass. */
interface CommitRecord {
  parents: readonly string[]
  /** As recorded, so it re-encodes to the bytes that were hashed. */
  time: PyFloat
}

/**
 * Yield `WireCommit`s for every commit reachable from `want` but not
 * from `have`, parents before children.
 *
 * Throws if `want` (or any commit record in the delta) is missing from
 * the store — a partial history is not a valid transfer source.
 */
export async function* walkDelta(
  store: KVStore,
  opts: WalkDeltaOptions,
): AsyncIterable<WireCommit> {
  const { want } = opts
  // Strict on `want`'s side: a missing record inside the delta means
  // there's no valid transfer to make.
  const parentsOf = await ancestryDelta(store, want, opts.have ?? [])
  if (parentsOf.size === 0) return
  const delta = [...parentsOf.keys()]
  const times = await store.getMany(delta.map(COMMIT_TIME))
  const missing = new Map<string, CommitRecord>()
  for (const hash of delta) {
    const timeRaw = times.get(COMMIT_TIME(hash))
    const time = timeRaw !== undefined ? loadsExact(timeRaw) : null
    missing.set(hash, {
      parents: parentsOf.get(hash) as readonly string[],
      time: time instanceof PyFloat ? time : new PyFloat(typeof time === 'number' ? time : 0),
    })
  }

  // Topological order via Kahn's algorithm. In-degree counts only
  // parents inside the delta (the rest the receiver has already).
  // Ready commits are popped oldest-first (time, then hash) so output
  // order is deterministic for a given history.
  const inDegree = new Map<string, number>()
  const children = new Map<string, string[]>()
  for (const [hash, rec] of missing) {
    let deg = 0
    for (const p of rec.parents) {
      if (!missing.has(p)) continue
      deg++
      let kids = children.get(p)
      if (kids === undefined) {
        kids = []
        children.set(p, kids)
      }
      kids.push(hash)
    }
    inDegree.set(hash, deg)
  }

  const byAge = (a: string, b: string): number => {
    const ta = (missing.get(a) as CommitRecord).time.value
    const tb = (missing.get(b) as CommitRecord).time.value
    if (ta !== tb) return ta - tb
    return a < b ? -1 : 1
  }
  const ready: string[] = []
  for (const [hash, deg] of inDegree) {
    if (deg === 0) ready.push(hash)
  }
  ready.sort(byAge)

  let emitted = 0
  while (ready.length > 0) {
    const hash = ready.shift() as string
    const rec = missing.get(hash) as CommitRecord
    yield await buildWireCommit(store, hash, rec)
    emitted++
    for (const child of children.get(hash) ?? []) {
      const deg = (inDegree.get(child) as number) - 1
      inDegree.set(child, deg)
      if (deg === 0) {
        // Insert keeping the ready list age-sorted; delta sets are
        // small enough that a linear scan beats a heap in practice.
        const at = ready.findIndex((r) => byAge(child, r) < 0)
        if (at === -1) ready.push(child)
        else ready.splice(at, 0, child)
      }
    }
  }

  if (emitted !== missing.size) {
    // Parent cycles are impossible in content-addressed history unless
    // the store is corrupt — fail loudly rather than emit a partial walk.
    throw new Error(
      `walkDelta: cycle detected in commit graph (emitted ${emitted}/${missing.size})`,
    )
  }
}

/** Diff a commit's keyset against its first parent and classify. */
async function buildWireCommit(
  store: KVStore,
  hash: string,
  rec: CommitRecord,
): Promise<WireCommit> {
  const firstParent = rec.parents[0]
  const [commitKs, parentKs] = await Promise.all([
    loadKeysetAt(store, hash),
    firstParent !== undefined ? loadKeysetAt(store, firstParent) : Keyset.empty(store),
  ])
  if (commitKs === null) {
    throw new Error(`walkDelta: commit root missing for ${hash}`)
  }
  if (parentKs === null) {
    throw new Error(`walkDelta: commit root missing for ${firstParent}`)
  }

  const diff = await parentKs.diff(commitKs)

  // Every entry that differs from the first parent's travels with its
  // bytes: written here, or adopted from a merge's other side, it is
  // the same blob either way. One getMany for all of them.
  const changed: Array<readonly [string, KeysetEntry]> = []
  for (const [key, entry] of diff.added) changed.push([key, entry])
  for (const [key, [, entry]] of diff.modified) changed.push([key, entry])
  for (const [key, entry] of changed) {
    if (entry.meta.chunks !== undefined) {
      throw new Error(`walkDelta: '${key}' at ${hash} is chunked; chunked blobs don't transfer`)
    }
  }
  const updates = new Map<string, Uint8Array>()
  if (changed.length > 0) {
    const fetched = await store.getMany(changed.map(([, entry]) => entry.blob))
    for (const [key, entry] of changed) {
      const bytes = fetched.get(entry.blob)
      if (bytes === undefined) throw new Error(`walkDelta: blob missing for ${entry.blob}`)
      updates.set(key, bytes)
    }
  }
  const removals = new Set(diff.removed.keys())

  const [infoRaw, gensRaw] = await Promise.all([
    store.get(INFO_KEY(hash)),
    store.get(PARENT_GENS(hash)),
  ])
  const gens = gensRaw !== null ? safeLoads(gensRaw) : null

  return {
    hash,
    parents: rec.parents,
    parentGens: Array.isArray(gens) ? (gens as number[]) : null,
    time: rec.time,
    info: infoRaw !== null ? loadsExact(infoRaw) : null,
    updates,
    removals,
  }
}

/** Open the keyset at a commit, or null if the commit root is absent. */
async function loadKeysetAt(store: KVStore, commit: string): Promise<Keyset | null> {
  const rootBytes = await store.get(COMMIT_ROOT(commit))
  if (rootBytes === null) return null
  return Keyset.fromRoot(store, loads(rootBytes) as string)
}
