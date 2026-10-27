/**
 * Ancestry questions over stored commits, answered by the core: what
 * one tip has that others lack, and whether a commit is in another's
 * history. Each costs about the stretch of history it is about, on
 * history that stores generations; older history is searched whole.
 */

import type { KVStore } from '../types'
import { callCore } from './drive'

const wallClock = (): number => Date.now() / 1000

/**
 * The commits reachable from `want` and not from any of `have`, each
 * with its parents. A commit of `have` the store lacks contributes
 * nothing; a missing commit on `want`'s side throws
 * `UnknownCommitError`, since a partial history can't be sent.
 */
export async function ancestryDelta(
  store: KVStore,
  want: string,
  have: Iterable<string>,
): Promise<Map<string, readonly string[]>> {
  const found = (await callCore(
    'ancestry_delta',
    { want, have: [...have] },
    { store, now: wallClock },
  )) as Array<[string, string[]]>
  return new Map(found)
}

/**
 * Whether `ancestor` is `from` or reachable from it, in this store. A
 * commit the store lacks is reachable from nothing, and nothing is
 * reachable from it.
 */
export async function isAncestor(store: KVStore, ancestor: string, from: string): Promise<boolean> {
  return (await callCore('is_ancestor', { ancestor, from }, { store, now: wallClock })) as boolean
}

/**
 * The commits reachable from `have` (each of them included) of
 * generation `floor` and above: everything a receiver at `have` holds
 * that a transfer of commits that high could repeat. On history without
 * generations, every commit reachable from `have`.
 */
export async function ancestorsAbove(
  store: KVStore,
  have: Iterable<string>,
  floor: number,
): Promise<Set<string>> {
  const found = (await callCore(
    'ancestors_above',
    { have: [...have], floor },
    { store, now: wallClock },
  )) as string[]
  return new Set(found)
}
