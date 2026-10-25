/**
 * Run an operation of the core against a store.
 *
 * The core never touches a store: an operation hands back each thing it
 * needs as a request, and this performs it and hands back the answer. A
 * request that throws is answered as failed; the operation unwinds,
 * undoing what it wrote, and the error is thrown once it is done.
 *
 * @internal
 */

import type { CommitOp } from '../../wasm/kvgit_wasm.js'
import { BranchMovedError, ConcurrencyError, MergeConflict, UnknownBranchError } from '../errors'
import { pack, unpack } from '../hamt'
import { CantMark } from '../merges'
import { type BytesMergeFn, type KVStore, MergeChoice } from '../types'
import { blobKey } from './layout'
import { leaseEnv } from './lease'

/** What a landed (or abandoned) commit reports. */
export interface Committed {
  merged: boolean
  commit: string | null
  strategy: 'no_op' | 'fast_forward' | 'three_way'
  autoMerged: string[]
  carried: string[]
  /** Each changed key's stored entry, or null where it was removed. */
  changes: Array<[string, StoredEntry | null]>
  roots: Array<[string, string]>
  generations: Array<[string, number]>
  stamped: boolean
}

export type StoredEntry = [string, { size: number; created_at?: number; chunks?: string[] }]

interface Failure {
  kind: string
  message: string
  branch?: string
  base?: string
  head?: string
  keys?: string[]
  failures?: Array<[string, string]>
}

interface Note {
  kind: 'from_backup' | 'from_recoverer' | 'recoverer_missed' | 'healed'
  branch: string
  answer: string | null
}

export interface DriveEnv {
  store: KVStore
  /** The bytes the operation writes by key without reading them;
   *  values merge functions make join it. */
  held: Map<string, Uint8Array>
  /** The functions the operation names by number. */
  mergeFns: readonly BytesMergeFn[]
  recover?: ((store: KVStore, branch: string) => Promise<string | null>) | undefined
  /** The commit clock, in seconds, read once for each commit made. */
  now: () => number
}

/** Perform `op`'s requests until it is done, and return its outcome. */
export async function driveCommit(op: CommitOp, env: DriveEnv): Promise<Committed> {
  const causes = new Map<string, unknown>()
  let raised: { error: unknown } | null = null
  let kind = op.start(leaseEnv.clock())
  while (kind !== 'done') {
    let next: (wall: number) => string
    try {
      next = await perform(op, kind, env, causes)
    } catch (e) {
      raised ??= { error: e }
      next = (wall) => op.replyFailed(wall)
    }
    kind = next(leaseEnv.clock())
  }
  const outcome = JSON.parse(op.outcome()) as { ok?: Committed; error?: Failure; notes: Note[] }
  for (const note of outcome.notes) warn(note)
  if (outcome.ok !== undefined) return outcome.ok
  if (raised !== null) throw raised.error
  throw toError(outcome.error as Failure, causes)
}

/** Perform one request; the reply, to send with the clock. */
async function perform(
  op: CommitOp,
  kind: string,
  env: DriveEnv,
  causes: Map<string, unknown>,
): Promise<(wall: number) => string> {
  const { store, held } = env
  switch (kind) {
    case 'get': {
      const found = await store.getMany(JSON.parse(op.requestJson()) as string[])
      const [data, lengths] = pack(found.values())
      const keys = JSON.stringify([...found.keys()])
      return (wall) => op.replyValues(wall, keys, data, lengths)
    }
    case 'cas': {
      const cas = JSON.parse(op.requestJson()) as {
        expect: Array<[string, number | null]>
        writes: Array<[string, number | null]>
        removes: string[]
      }
      const sized = [...cas.expect, ...cas.writes].flatMap(([, n]) => (n === null ? [] : [n]))
      const bytes = unpack(op.requestData(), Uint32Array.from(sized))
      let at = 0
      const take = (n: number | null) => (n === null ? null : (bytes[at++] as Uint8Array))
      const expect = new Map(cas.expect.map(([k, n]) => [k, take(n)] as const))
      const writes = new Map(
        cas.writes.map(([k, n]) => [k, take(n) ?? (held.get(k) as Uint8Array)] as const),
      )
      const landed = await store.casMany(expect, writes, cas.removes)
      return (wall) => op.replyLanded(wall, landed)
    }
    case 'wait': {
      await leaseEnv.sleep(JSON.parse(op.requestJson()) as number)
      return (wall) => op.replyWaited(wall)
    }
    case 'merge': {
      const answers = await merge(
        JSON.parse(op.requestJson()) as Array<[number, string, ...(string | null)[]]>,
        env,
        causes,
      )
      const text = JSON.stringify(answers)
      return (wall) => op.replyMerged(wall, text)
    }
    case 'now': {
      const now = env.now()
      return (wall) => op.replyNow(wall, now)
    }
    case 'recover': {
      const branch = JSON.parse(op.requestJson()) as string
      const guess: unknown = env.recover === undefined ? null : await env.recover(store, branch)
      if (guess !== null && typeof guess !== 'string') {
        warn({ kind: 'recoverer_missed', branch, answer: String(guess) })
      }
      const commit = typeof guess === 'string' ? guess : undefined
      return (wall) => op.replyRecovered(wall, commit)
    }
    default:
      throw new Error(`the core asked for ${kind}`)
  }
}

/** Run each merge function asked for on its key's three sides. */
async function merge(
  asks: Array<[number, string, ...(string | null)[]]>,
  env: DriveEnv,
  causes: Map<string, unknown>,
): Promise<unknown[]> {
  const blobs = new Set(asks.flatMap(([, , ...sides]) => sides.filter((b) => b !== null)))
  const values = blobs.size > 0 ? await env.store.getMany([...blobs] as string[]) : new Map()
  const answers: unknown[] = []
  for (const [rule, key, ...sides] of asks) {
    const [old, ours, theirs] = sides.map((b) => (b === null ? null : (values.get(b) ?? null)))
    let result: Uint8Array | MergeChoice
    try {
      result = (env.mergeFns[rule] as BytesMergeFn)(old ?? null, ours ?? null, theirs ?? null)
    } catch (e) {
      causes.set(key, e)
      answers.push(['failed', e instanceof Error ? e.message : String(e)])
      continue
    }
    if (result instanceof MergeChoice) {
      answers.push(result === MergeChoice.THEIRS ? 'theirs' : 'ours')
    } else {
      const blob = await blobKey(result)
      env.held.set(blob, result)
      answers.push(['value', blob, result.length])
    }
  }
  return answers
}

function toError(f: Failure, causes: Map<string, unknown>): Error {
  switch (f.kind) {
    case 'branch_moved':
      return new BranchMovedError(f.branch as string, f.base as string, f.head as string)
    case 'unknown_branch':
      return new UnknownBranchError(f.branch as string)
    case 'concurrency':
      return new ConcurrencyError(f.message)
    case 'merge_conflict': {
      const all = new Map<string, unknown>()
      for (const [key, why] of f.failures ?? []) all.set(key, causes.get(key) ?? new CantMark(why))
      return new MergeConflict(f.keys ?? [], all)
    }
    default:
      return new Error(f.message)
  }
}

function warn(note: Note): void {
  const at = `kvgit: branch '${note.branch}'`
  switch (note.kind) {
    case 'from_backup':
      console.warn(`${at} HEAD corrupt, recovered from prev HEAD`)
      break
    case 'from_recoverer':
      console.warn(`${at} HEAD corrupt, recovered via injected recoverer`)
      break
    case 'recoverer_missed':
      console.warn(
        `${at} recoverer returned ${JSON.stringify(note.answer)}, which is not a commit in this store; treating the branch as unrecoverable`,
      )
      break
    case 'healed':
      console.warn(`${at} corrupt HEAD replaced with recovered commit`)
      break
  }
}
