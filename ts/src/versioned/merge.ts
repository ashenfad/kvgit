/**
 * Three-way merge resolution, from each side's changes since the common
 * ancestor.
 *
 * `resolveMerge` is pure with respect to storage: its only IO is the
 * injected `blobReader`, so a backend of any kind calls it the same
 * way. The result is expressed as changes to our side.
 */

import { MergeConflict } from '../errors'
import { compareCodePoints } from '../pyjson'
import { type KeysetEntry, MergeChoice, type MergePolicy } from '../types'

/** Read a blob's bytes by its key. */
export type BlobReader = (id: string) => Promise<Uint8Array | null>

/** One key's change from the common ancestor to one side: its entry
 *  there and its entry here, null where the key is absent. */
export interface Change {
  readonly old: KeysetEntry | null
  readonly new: KeysetEntry | null
}

/**
 * A resolved three-way merge, as changes to make to our side.
 *
 * `updates` and `removals` turn our keyset into the merged one;
 * `mergedValues` are values merge functions produced, each still to be
 * written as a blob and set under its key. Nothing here restates a key
 * whose merged state is already ours.
 */
export interface MergeResolution {
  readonly updates: ReadonlyMap<string, KeysetEntry>
  readonly removals: ReadonlySet<string>
  readonly mergedValues: ReadonlyMap<string, Uint8Array>
  readonly autoMergedKeys: readonly string[]
  readonly carriedKeys: readonly string[]
}

/** Whether a resolution changes our side at all. */
export function changesOurs(resolution: MergeResolution): boolean {
  return (
    resolution.updates.size > 0 || resolution.removals.size > 0 || resolution.mergedValues.size > 0
  )
}

/**
 * The registration that governs one key: an exact key match first, then
 * the longest registered prefix the key starts with, then the default.
 * Merge functions and `MergeChoice` policies compete in that one order.
 * Null means nothing covers the key, which leaves a contested key a
 * conflict.
 */
export function pickMergePolicy(
  key: string,
  mergeFns: ReadonlyMap<string, MergePolicy>,
  mergePrefixes: ReadonlyMap<string, MergePolicy>,
  defaultMerge: MergePolicy | null,
): MergePolicy | null {
  const exact = mergeFns.get(key)
  if (exact !== undefined) return exact
  let best: MergePolicy | null = null
  let bestLen = -1
  for (const [prefix, candidate] of mergePrefixes) {
    if (key.startsWith(prefix) && prefix.length > bestLen) {
      best = candidate
      bestLen = prefix.length
    }
  }
  return best ?? defaultMerge
}

export interface ResolveMergeOptions {
  /** Our side's changes since the common ancestor. */
  ours: ReadonlyMap<string, Change>
  /** Their side's changes since the common ancestor. */
  theirs: ReadonlyMap<string, Change>
  blobReader: BlobReader
  mergeFns: ReadonlyMap<string, MergePolicy>
  mergePrefixes?: ReadonlyMap<string, MergePolicy>
  defaultMerge: MergePolicy | null
}

/**
 * Resolve a three-way merge from each side's changes since the common
 * ancestor. Only changed keys are looked at, so the cost follows the
 * size of the change, not of the keyset.
 *
 * A merge function is consulted only where both sides changed a key; a
 * registered `MergeChoice` gives one side every key it covers that
 * either side changed.
 *
 * @throws MergeConflict for contested keys nothing resolves.
 */
export async function resolveMerge(opts: ResolveMergeOptions): Promise<MergeResolution> {
  const { ours, theirs, blobReader, mergeFns, defaultMerge } = opts
  const prefixes = opts.mergePrefixes ?? new Map<string, MergePolicy>()
  const updates = new Map<string, KeysetEntry>()
  const removals = new Set<string>()
  const mergedValues = new Map<string, Uint8Array>()
  const autoMerged: string[] = []
  const carried: string[] = []
  const conflicts = new Set<string>()
  const mergeErrors = new Map<string, unknown>()

  /** Make `entry` the merged state of `key`, whose state on our side is
   *  `current`. */
  const take = (key: string, entry: KeysetEntry | null, current: KeysetEntry | null): void => {
    if (sameEntry(entry, current)) return
    if (entry === null) removals.add(key)
    else updates.set(key, entry)
  }

  const keys = [...new Set([...ours.keys(), ...theirs.keys()])].sort(compareCodePoints)
  for (const key of keys) {
    const ourChange = ours.get(key)
    const theirChange = theirs.get(key)
    const ancestor = ((ourChange ?? theirChange) as Change).old
    const ourEntry = ourChange !== undefined ? ourChange.new : ancestor
    const theirEntry = theirChange !== undefined ? theirChange.new : ancestor
    const policy = pickMergePolicy(key, mergeFns, prefixes, defaultMerge)

    // A MergeChoice registration: that side's state stands, whether or
    // not the other side touched the key. Nothing is read.
    if (policy instanceof MergeChoice) {
      if (policy === MergeChoice.THEIRS) take(key, theirEntry, ourEntry)
      autoMerged.push(key)
      continue
    }

    if (theirChange === undefined) continue // changed only by us: ours stands
    if (ourChange === undefined) {
      take(key, theirEntry, ourEntry) // changed only by them
      carried.push(key)
      continue
    }

    // Contested: changed by both sides. A merge function is consulted
    // here and nowhere else, so registering one never disturbs a change
    // only one side made.
    if (ourEntry === null && theirEntry === null) continue // both removed it
    if (ourEntry !== null && theirEntry !== null && ourEntry.blob === theirEntry.blob) {
      continue // the same change on both sides
    }
    if (policy === null) {
      conflicts.add(key)
      continue
    }

    const oldVal = ancestor === null ? null : await blobReader(ancestor.blob)
    const ourVal = ourEntry === null ? null : await blobReader(ourEntry.blob)
    const theirVal = theirEntry === null ? null : await blobReader(theirEntry.blob)
    let result: Uint8Array | MergeChoice
    try {
      result = policy(oldVal, ourVal, theirVal)
    } catch (e) {
      // The function is the caller's; any failure is a conflict.
      conflicts.add(key)
      mergeErrors.set(key, e)
      continue
    }

    // A MergeChoice keeps the chosen side's committed value: its
    // existing entry, so no new blob is written. A side that removed the
    // key has no entry, and choosing it removes the key.
    if (result === MergeChoice.THEIRS) take(key, theirEntry, ourEntry)
    else if (!(result instanceof MergeChoice)) mergedValues.set(key, result)
    autoMerged.push(key)
  }

  if (conflicts.size > 0) throw new MergeConflict(conflicts, mergeErrors)
  return { updates, removals, mergedValues, autoMergedKeys: autoMerged, carriedKeys: carried }
}

function sameEntry(a: KeysetEntry | null, b: KeysetEntry | null): boolean {
  if (a === null || b === null) return a === b
  if (a.blob !== b.blob || a.meta.size !== b.meta.size) return false
  const ca = a.meta.chunks ?? []
  const cb = b.meta.chunks ?? []
  return ca.length === cb.length && ca.every((c, i) => c === cb[i])
}
