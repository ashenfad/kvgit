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
 * The core walks the history and diffs each commit's keyset against
 * its first parent's; this reads the changed values' bytes. The walk
 * costs about the delta on history that stores generations, and the
 * diff skips the subtrees two keysets share, so each commit costs its
 * change, not its keyset's size.
 */

import { PyFloat } from '../pyjson'
import type { KVStore, WireCommit } from '../types'
import { streamCore } from '../versioned/drive'
import { loadsExact } from '../versioned/layout'

export interface WalkDeltaOptions {
  /** Commit hash to walk back from (typically a branch HEAD). */
  want: string
  /** Commits the receiver already has. Their full ancestries are
   *  excluded from the walk. Empty/omitted = full history. */
  have?: Iterable<string>
}

/** A commit as the core's walk gives it: records as their text, and
 *  each changed key with the blob its bytes are under. */
interface Walked {
  hash: string
  parents: string[]
  parent_gens: number[] | null
  time: string | null
  info: string | null
  updates: Array<[string, string]>
  removals: string[]
}

const wallClock = (): number => Date.now() / 1000
const encoder = new TextEncoder()

/**
 * Yield `WireCommit`s for every commit reachable from `want` but not
 * from `have`, parents before children; of the commits ready to go, the
 * oldest first, so a history always walks in the same order.
 *
 * Throws `UnknownCommitError` if `want`, or a commit in the delta, is
 * missing from the store: a partial history is not a valid transfer
 * source.
 */
export async function* walkDelta(
  store: KVStore,
  opts: WalkDeltaOptions,
): AsyncIterable<WireCommit> {
  const args = { want: opts.want, have: [...(opts.have ?? [])] }
  for await (const item of streamCore('wire_walk', args, { store, now: wallClock })) {
    const walked = item as Walked
    const blobs =
      walked.updates.length === 0
        ? new Map<string, Uint8Array>()
        : await store.getMany(walked.updates.map(([, blob]) => blob))
    const updates = new Map<string, Uint8Array>()
    for (const [key, blob] of walked.updates) {
      const bytes = blobs.get(blob)
      if (bytes === undefined) throw new Error(`walkDelta: blob missing for ${blob}`)
      updates.set(key, bytes)
    }
    // As recorded, so they re-encode to the bytes that were hashed.
    const time = walked.time === null ? null : loadsExact(encoder.encode(walked.time))
    yield {
      hash: walked.hash,
      parents: walked.parents,
      parentGens: walked.parent_gens,
      time: time instanceof PyFloat ? time : new PyFloat(typeof time === 'number' ? time : 0),
      info: walked.info === null ? null : loadsExact(encoder.encode(walked.info)),
      updates,
      removals: new Set(walked.removals),
    }
  }
}
