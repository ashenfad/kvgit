/**
 * Branch and tag records over a raw `KVStore`, as Python kvgit writes
 * them; the core decides them, and this module runs its operations.
 *
 * A tag is a branch head under the reserved `refs/tags/` namespace plus
 * a `__tag_info__<name>` record of when it was made and what the caller
 * said about it. The branch API refuses names in that namespace, so a
 * branch can never be mistaken for a tag, or move one.
 */

import { core } from '../core'
import type { CommitInfo, KVStore } from '../types'
import { callCore } from './drive'

const wallClock = (): number => Date.now() / 1000

/** What a store records about one tag. */
export interface TagInfo {
  readonly name: string
  readonly commit: string
  /** When the tag was made, in seconds; null if its record is missing
   *  (a crash between the two writes, or a store written by hand). */
  readonly time: number | null
  readonly info: CommitInfo | null
  /** The tagged commit is not in the store. A tag can't be made for a
   *  missing commit, so this is damage, and a dangling tag keeps
   *  nothing alive. */
  readonly dangling: boolean
}

/** Refuse a branch name that can't be a branch: an empty one, which
 *  no listing would show, or one inside the reserved tag namespace,
 *  which would list as a tag and move it when moved. */
export async function rejectReservedBranch(name: string): Promise<void> {
  const problem = (await core()).branchNameProblem(name)
  if (problem !== undefined) throw new RangeError(problem)
}

/** Every branch name, sorted; tags are not branches. */
export async function listBranches(store: KVStore): Promise<string[]> {
  return (await callCore('branches', {}, { store, now: wallClock })) as string[]
}

/** The commit a tag names, or null if it doesn't resolve. A tag never
 *  moves, so there is no backup to fall back on. */
export async function resolveTag(store: KVStore, name: string): Promise<string | null> {
  return (await callCore('resolve_tag', { name }, { store, now: wallClock })) as string | null
}

/** Every tag and the commit it names, sorted by name. Dangling tags are
 *  included: leaving them out would make damage look like deletion. */
export async function listTags(store: KVStore): Promise<Map<string, string>> {
  const pairs = (await callCore('tags', {}, { store, now: wallClock })) as Array<[string, string]>
  return new Map(pairs)
}

/** Describe one tag, or null if there is no such tag. */
export async function tagInfo(store: KVStore, name: string): Promise<TagInfo | null> {
  return (await callCore('tag_info', { name }, { store, now: wallClock })) as TagInfo | null
}

/**
 * Name `commit` permanently, keeping it and its ancestry alive. The
 * head is claimed against absence, so two writers racing one name
 * can't both win; the info record lands in the same write, and any
 * backup an earlier holder of the name left goes with it.
 *
 * @throws TagExistsError if the name is taken.
 * @throws UnknownCommitError if `commit` is not in the store.
 */
export async function createTag(
  store: KVStore,
  name: string,
  commit: string,
  opts: { info?: CommitInfo | null; now: () => number },
): Promise<void> {
  const args = { name, target: commit, info: opts.info ?? null }
  await callCore('create_tag', args, { store, now: opts.now })
}

/**
 * Delete a tag: its head, any backup of it, and its record. A commit
 * the tag alone kept alive becomes collectable.
 *
 * The removal is conditional on the head and the record it read. If
 * the tag changed in between (deleted and made again, since a tag never
 * moves), the tag this call saw is already gone and the one there now
 * is someone else's, so nothing is removed. The record is what tells a
 * tag made again at the *same* commit apart: it carries the time the
 * new one landed. A tag with no record is compared by its head alone.
 *
 * @throws UnknownTagError if there is no such tag.
 * @throws ConcurrencyError if the tag changed while being deleted.
 */
export async function deleteTag(store: KVStore, name: string): Promise<void> {
  await callCore('delete_tag', { name }, { store, now: wallClock })
}
