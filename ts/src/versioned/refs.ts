/**
 * Branch and tag records over a raw `KVStore`, as Python kvgit writes
 * them.
 *
 * A tag is a branch head under the reserved `refs/tags/` namespace plus
 * a `__tag_info__<name>` record of when it was made and what the caller
 * said about it. The branch API refuses names in that namespace, so a
 * branch can never be mistaken for a tag, or move one.
 */

import { ConcurrencyError, TagExistsError, UnknownCommitError, UnknownTagError } from '../errors'
import { PyFloat } from '../pyjson'
import type { CommitInfo, KVStore } from '../types'
import {
  BRANCH_HEAD,
  BRANCH_HEAD_PREFIX,
  BRANCH_HEAD_PREV,
  COMMIT_ROOT,
  TAG_BRANCH_PREFIX,
  TAG_INFO_KEY,
  dumps,
  loadsInfo,
  safeLoads,
} from './layout'
import { tryLand, waitForGc } from './lease'

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
export function rejectReservedBranch(name: string): void {
  if (name.length === 0) throw new RangeError('Branch name must be a non-empty string')
  if (name.startsWith(TAG_BRANCH_PREFIX)) {
    throw new RangeError(
      `Branch name '${name}' is reserved for tags (the '${TAG_BRANCH_PREFIX}' namespace); use the tag API instead`,
    )
  }
}

/** Every branch name, sorted; tags are not branches. */
export async function listBranches(store: KVStore): Promise<string[]> {
  const out: string[] = []
  // `__branch_head_prev__*` doesn't start with `__branch_head__`, so
  // the prefix scan leaves the backups out.
  for await (const k of store.keys(BRANCH_HEAD_PREFIX)) {
    const name = k.slice(BRANCH_HEAD_PREFIX.length)
    if (name.length > 0 && !name.startsWith(TAG_BRANCH_PREFIX)) out.push(name)
  }
  return out.sort()
}

function validateTagName(name: string): void {
  if (typeof name !== 'string' || name.length === 0) {
    throw new TypeError('Tag name must be a non-empty string')
  }
  // Python kvgit builds keys with %-formatting, so it can't store a
  // name carrying '%'; refusing it here keeps the stores compatible.
  if (name.includes('%')) throw new RangeError(`Tag name must not contain '%': '${name}'`)
}

const tagHead = (name: string): string => BRANCH_HEAD(TAG_BRANCH_PREFIX + name)

/** The commit a tag names, or null if it doesn't resolve. A tag never
 *  moves, so there is no backup to fall back on. */
export async function resolveTag(store: KVStore, name: string): Promise<string | null> {
  const raw = await store.get(tagHead(name))
  if (raw === null) return null
  const commit = safeLoads(raw)
  return typeof commit === 'string' ? commit : null
}

/** Every tag and the commit it names, sorted by name. Dangling tags are
 *  included: leaving them out would make damage look like deletion. */
export async function listTags(store: KVStore): Promise<Map<string, string>> {
  const prefix = tagHead('')
  const keys: string[] = []
  for await (const k of store.keys(prefix)) {
    if (k.length > prefix.length) keys.push(k)
  }
  const found = keys.length > 0 ? await store.getMany(keys) : new Map<string, Uint8Array>()
  const out: Array<[string, string]> = []
  for (const [k, raw] of found) {
    const commit = safeLoads(raw)
    if (typeof commit === 'string') out.push([k.slice(prefix.length), commit])
  }
  out.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
  return new Map(out)
}

/** Describe one tag, or null if there is no such tag. */
export async function tagInfo(store: KVStore, name: string): Promise<TagInfo | null> {
  const commit = await resolveTag(store, name)
  if (commit === null) return null
  let time: number | null = null
  let info: CommitInfo | null = null
  const raw = await store.get(TAG_INFO_KEY(name))
  if (raw !== null) {
    let record: unknown = null
    try {
      record = loadsInfo(raw)
    } catch {
      record = null
    }
    if (record !== null && typeof record === 'object' && !Array.isArray(record)) {
      const r = record as Record<string, unknown>
      if (typeof r.time === 'number') time = r.time
      if (r.info !== null && typeof r.info === 'object' && !Array.isArray(r.info)) {
        info = r.info as CommitInfo
      }
    }
  }
  const dangling = (await store.get(COMMIT_ROOT(commit))) === null
  return { name, commit, time, info, dangling }
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
  validateTagName(name)
  // Encoded before anything is written, so info that can't be encoded
  // throws without leaving a tag behind.
  const info = opts.info ?? null
  dumps(info)
  const head = tagHead(name)
  // A tag is a GC root, so it mustn't be planted under a sweep that has
  // already decided what is reachable, and the existence check must
  // still hold when it lands: both are decided against one lease record.
  for (;;) {
    const lease = await waitForGc(store)
    if ((await store.get(COMMIT_ROOT(commit))) === null) throw new UnknownCommitError(commit)
    // Built after the wait, so its time is when the tag lands.
    const record = dumps(
      new Map<string, unknown>([
        ['time', new PyFloat(opts.now())],
        ['info', info],
      ]),
    )
    const landed = await tryLand(
      store,
      lease,
      new Map([[head, null]]),
      new Map([
        [head, dumps(commit)],
        [TAG_INFO_KEY(name), record],
      ]),
      [BRANCH_HEAD_PREV(TAG_BRANCH_PREFIX + name)],
    )
    if (landed === null) continue
    if (!landed) throw new TagExistsError(name)
    return
  }
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
 * new one landed. A tag with no record is compared by its head alone,
 * and one made again at the same commit, with the same info and the
 * same time stamp (as a frozen clock guarantees), is byte-for-byte the
 * tag this call saw, so it goes, as a branch would.
 *
 * The two reads needn't be atomic. A read torn across a delete and
 * recreate either sees a state some tag really held (the head bytes are
 * the same for both when the commit is), or expects a head or record
 * that is no longer there, and the conditional removal fails.
 *
 * @throws UnknownTagError if there is no such tag.
 * @throws ConcurrencyError if the tag changed while being deleted.
 */
export async function deleteTag(store: KVStore, name: string): Promise<void> {
  validateTagName(name)
  const head = tagHead(name)
  const info = TAG_INFO_KEY(name)
  const found = await store.getMany([head, info])
  const current = found.get(head)
  if (current === undefined) throw new UnknownTagError(name)
  const removed = await store.casMany(
    new Map([
      [head, current],
      [info, found.get(info) ?? null],
    ]),
    new Map(),
    [head, BRANCH_HEAD_PREV(TAG_BRANCH_PREFIX + name), info],
  )
  if (!removed) {
    throw new ConcurrencyError(`Tag '${name}' changed while being deleted; nothing was removed`)
  }
}
