/**
 * Pure helpers shared by the versioned layer.
 *
 * `diffKeysets` is a set-based diff between two flat `key → blob_pointer`
 * maps. `walkHistory` yields commits along the parent chain via an
 * injected loader, supporting either linear (first-parent) or
 * BFS-across-all-parents traversal.
 */

import type { DiffResult } from '../types'

/**
 * Compute key-level differences between two keysets.
 *
 * Each keyset maps user keys to opaque content identifiers (versioned
 * blob pointers in `VersionedKV`). Two keys are "modified" when both
 * sides have the key but mapped to different identifiers.
 */
export function diffKeysets(
  keysetA: ReadonlyMap<string, string>,
  keysetB: ReadonlyMap<string, string>,
): DiffResult {
  const added = new Set<string>()
  const removed = new Set<string>()
  const modified = new Set<string>()

  for (const k of keysetB.keys()) {
    if (!keysetA.has(k)) added.add(k)
  }
  for (const [k, v] of keysetA) {
    if (!keysetB.has(k)) {
      removed.add(k)
    } else if (keysetB.get(k) !== v) {
      modified.add(k)
    }
  }
  return { added, removed, modified }
}

export type ParentLoader = (commitHash: string) => Promise<readonly string[]>

/**
 * Yield commit hashes from `start` along the parent chain, newest to
 * oldest.
 *
 * - `allParents = false` (default): linear walk via the first parent.
 * - `allParents = true`: BFS across all parents (visit every ancestor
 *   exactly once).
 */
export async function* walkHistory(
  start: string,
  parentLoader: ParentLoader,
  opts: { allParents?: boolean } = {},
): AsyncIterable<string> {
  if (opts.allParents) {
    const visited = new Set<string>()
    const queue: string[] = [start]
    while (queue.length > 0) {
      const current = queue.shift() as string
      if (visited.has(current)) continue
      visited.add(current)
      yield current
      const parents = await parentLoader(current)
      for (const p of parents) {
        if (!visited.has(p)) queue.push(p)
      }
    }
  } else {
    let current: string | null = start
    while (current !== null) {
      yield current
      const parents = await parentLoader(current)
      current = parents[0] ?? null
    }
  }
}
