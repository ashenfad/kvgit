/**
 * Ancestry questions over stored commits, answered from generation
 * numbers where the history stores them.
 *
 * A commit's generation is one more than its highest parent's, so a
 * search that visits commits highest generation first has, when it
 * reaches a generation, visited every commit above it that the search
 * can reach. That bounds each question by the stretch of history it is
 * about, not the history's length: what one tip has that another
 * lacks costs about the commits between them. History written before
 * generations were stored is searched whole, as it always was.
 */

import type { KVStore } from '../types'
import { PARENT_COMMIT, PARENT_GENS, generationOf, safeLoads } from './layout'

/** A commit's parents, and their generations if the commit stores them. */
export interface CommitRecord {
  /** Whether the store holds the commit at all; one it doesn't reads
   *  as having no parents. */
  readonly present: boolean
  readonly parents: readonly string[]
  readonly gens: readonly number[] | null
}

/** Parents and parent generations of several commits, in one read. */
export async function loadRecords(
  store: KVStore,
  commits: Iterable<string>,
): Promise<Map<string, CommitRecord>> {
  const list = [...new Set(commits)]
  const found = await store.getMany(list.flatMap((c) => [PARENT_COMMIT(c), PARENT_GENS(c)]))
  const out = new Map<string, CommitRecord>()
  for (const c of list) {
    const parentsRaw = found.get(PARENT_COMMIT(c))
    const parsed = parentsRaw === undefined ? null : safeLoads(parentsRaw)
    const parents =
      typeof parsed === 'string'
        ? [parsed]
        : Array.isArray(parsed)
          ? parsed.filter((p): p is string => typeof p === 'string')
          : []
    const gensRaw = found.get(PARENT_GENS(c))
    const gens = gensRaw === undefined ? null : safeLoads(gensRaw)
    const valid =
      Array.isArray(gens) &&
      gens.length === parents.length &&
      gens.every((g) => Number.isInteger(g) && (g as number) >= 0)
    out.set(c, {
      present: parentsRaw !== undefined,
      parents,
      gens: valid ? (gens as number[]) : null,
    })
  }
  return out
}

/** A commit's generation from its record; null if it stores none. */
export function generationOfRecord(record: CommitRecord): number | null {
  if (record.parents.length === 0) return 0
  if (record.gens === null) return null
  return generationOf(record.gens)
}

/** The search reached history written before generations were stored. */
export class NoGenerations extends Error {}

const WANT = 1
const HAVE = 2

interface Painted {
  /** Commits reachable from `want` and not from `have`, with their
   *  parents. */
  delta: Map<string, readonly string[]>
  /** Commits reachable from `have` that the search visited. */
  had: Set<string>
}

/**
 * Paint down from `want` and from `have` at once, highest generation
 * first, each commit of one generation in one batched read. When a
 * commit is visited, every descendant the search can reach has been,
 * so whether it is reachable from `have` is settled. The search stops
 * once nothing queued is reachable from `want` alone, or, with
 * `floor`, once it has also visited every commit reachable from `have`
 * down to that generation. With `first`, it stops at the first commit
 * of the delta.
 *
 * A commit the store lacks is skipped when only `have` reaches it (a
 * receiver's claim it can't check), and throws when `want` does.
 *
 * @throws NoGenerations on history that stores none, or stores
 *   inconsistent ones.
 */
async function paint(
  store: KVStore,
  want: string | null,
  have: Iterable<string>,
  opts: { first?: boolean; floor?: number } = {},
): Promise<Painted> {
  const starts = [...new Set(have)]
  const records = await loadRecords(store, want === null ? starts : [want, ...starts])
  const flags = new Map<string, number>()
  const levels = new Map<number, Set<string>>()
  const enqueue = (c: string, g: number): void => {
    const level = levels.get(g)
    if (level === undefined) levels.set(g, new Set([c]))
    else level.add(c)
  }
  // the queued commits reachable from `want` alone
  let pending = 0
  if (want !== null) {
    const record = records.get(want) as CommitRecord
    if (!record.present) throw new Error(`commit record missing for ${want}`)
    const g = generationOfRecord(record)
    if (g === null) throw new NoGenerations()
    flags.set(want, WANT)
    enqueue(want, g)
    pending++
  }
  for (const h of starts) {
    const record = records.get(h) as CommitRecord
    if (!record.present) continue
    const g = generationOfRecord(record)
    if (g === null) throw new NoGenerations()
    const before = flags.get(h) ?? 0
    if (before === WANT) pending--
    flags.set(h, before | HAVE)
    enqueue(h, g)
  }

  const delta = new Map<string, readonly string[]>()
  const had = new Set<string>()
  const floor = opts.floor
  while (levels.size > 0) {
    const levelGen = Math.max(...levels.keys())
    if (pending === 0 && (floor === undefined || levelGen < floor)) break
    const level = levels.get(levelGen) as Set<string>
    levels.delete(levelGen)
    const unread = [...level].filter((c) => !records.has(c))
    if (unread.length > 0) {
      for (const [c, r] of await loadRecords(store, unread)) records.set(c, r)
    }
    for (const c of [...level].sort()) {
      const f = flags.get(c) as number
      const record = records.get(c) as CommitRecord
      if (f === WANT) {
        pending--
        if (!record.present) throw new Error(`commit record missing for ${c}`)
        delta.set(c, record.parents)
        if (opts.first) return { delta, had }
      } else {
        if (!record.present) continue
        had.add(c)
      }
      const { parents, gens } = record
      if (parents.length > 0 && gens === null) throw new NoGenerations()
      for (let i = 0; i < parents.length; i++) {
        const parent = parents[i] as string
        const parentGen = (gens as readonly number[])[i] as number
        if (parentGen >= levelGen) throw new NoGenerations()
        const before = flags.get(parent)
        const after = (before ?? 0) | f
        if (after === before) continue
        flags.set(parent, after)
        if (before === undefined) {
          enqueue(parent, parentGen)
          if (after === WANT) pending++
        } else if (before === WANT) {
          pending--
        }
      }
    }
  }
  return { delta, had }
}

/**
 * The commits reachable from `want` and not from any of `have`, each
 * with its parents. A commit of `have` the store lacks contributes
 * nothing; a missing commit on `want`'s side throws, since a partial
 * history can't be sent.
 */
export async function ancestryDelta(
  store: KVStore,
  want: string,
  have: Iterable<string>,
): Promise<Map<string, readonly string[]>> {
  const haveList = [...have]
  try {
    return (await paint(store, want, haveList)).delta
  } catch (e) {
    if (!(e instanceof NoGenerations)) throw e
  }
  const haveSet = await ancestorsByWalk(store, haveList)
  const delta = new Map<string, readonly string[]>()
  const stack = [want]
  while (stack.length > 0) {
    const current = stack.pop() as string
    if (delta.has(current) || haveSet.has(current)) continue
    const parents = await parentsOf(store, current)
    if (parents === null) throw new Error(`commit record missing for ${current}`)
    delta.set(current, parents)
    for (const p of parents) if (!delta.has(p) && !haveSet.has(p)) stack.push(p)
  }
  return delta
}

/**
 * Whether `ancestor` is `from` or reachable from it, in this store. A
 * commit the store lacks is reachable from nothing, and nothing is
 * reachable from it.
 */
export async function isAncestor(store: KVStore, ancestor: string, from: string): Promise<boolean> {
  if ((await parentsOf(store, ancestor)) === null) return false
  if (ancestor === from) return true
  try {
    return (await paint(store, ancestor, [from], { first: true })).delta.size === 0
  } catch (e) {
    if (!(e instanceof NoGenerations)) throw e
  }
  return (await ancestorsByWalk(store, [from])).has(ancestor)
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
  const haveList = [...have]
  try {
    return (await paint(store, null, haveList, { floor })).had
  } catch (e) {
    if (!(e instanceof NoGenerations)) throw e
  }
  return ancestorsByWalk(store, haveList)
}

/** Every commit reachable from `starts` (themselves included), the
 *  whole way down; commits the store lacks are skipped. */
async function ancestorsByWalk(store: KVStore, starts: Iterable<string>): Promise<Set<string>> {
  const out = new Set<string>()
  const stack = [...starts]
  while (stack.length > 0) {
    const current = stack.pop() as string
    if (out.has(current)) continue
    const parents = await parentsOf(store, current)
    if (parents === null) continue
    out.add(current)
    for (const p of parents) if (!out.has(p)) stack.push(p)
  }
  return out
}

/** A commit's parents, or null if the store lacks it. */
async function parentsOf(store: KVStore, commit: string): Promise<readonly string[] | null> {
  const raw = await store.get(PARENT_COMMIT(commit))
  if (raw === null) return null
  const parsed = safeLoads(raw)
  if (typeof parsed === 'string') return [parsed]
  if (Array.isArray(parsed)) return parsed.filter((p): p is string => typeof p === 'string')
  return []
}
