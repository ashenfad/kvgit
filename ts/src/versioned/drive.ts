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

import type { Op } from '../../wasm/kvgit_wasm.js'
import { core, coreIfLoaded } from '../core'
import {
  BranchExistsError,
  BranchMovedError,
  ConcurrencyError,
  CorruptHeadError,
  MergeConflict,
  TagExistsError,
  UnknownBranchError,
  UnknownCommitError,
  UnknownTagError,
} from '../errors'
import { pack, unpack } from '../hamt'
import { CantMark } from '../merges'
import { pyDumps } from '../pyjson'
import { type BytesMergeFn, type KVStore, MergeChoice } from '../types'
import { blobKey, loadsInfo } from './layout'
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
  tag?: string
  commit?: string
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
  held?: Map<string, Uint8Array>
  /** The functions the operation names by number. */
  mergeFns?: readonly BytesMergeFn[]
  recover?: ((store: KVStore, branch: string) => Promise<string | null>) | undefined
  /** The clock commit and tag times are read from, in seconds. */
  now: () => number
}

const encoder = new TextEncoder()

/** An answer's JSON, integers past the safe range kept exact. The core
 *  writes JSON without spaces, so a number starts after `:`, `[` or
 *  `,`; plain `JSON.parse` serves any text without one 16 digits long. */
const parseValue = (text: string): unknown =>
  /[:,[]-?\d{16}/.test(text) ? loadsInfo(encoder.encode(text)) : JSON.parse(text)

/** Operation `name` on `args`. Arguments that carry a caller's info go
 *  through the Python-compatible encoder, which keeps its integers and
 *  floats as they are; the rest are plain strings, flags and counts. */
async function start(name: string, args: object): Promise<Op> {
  const c = coreIfLoaded() ?? (await core())
  return new c.Op(name, 'info' in args ? pyDumps(args) : JSON.stringify(args))
}

/** Run the core's operation `name` on `args` against `env.store`, and
 *  return what it answers. */
export async function callCore(name: string, args: object, env: DriveEnv): Promise<unknown> {
  const op = await start(name, args)
  try {
    return await drive(op, env, parseValue)
  } finally {
    op.free()
  }
}

/** Run the core's operation `name`, yielding each item of the parts it
 *  emits. */
export async function* streamCore(
  name: string,
  args: object,
  env: DriveEnv,
): AsyncGenerator<unknown, void, undefined> {
  const op = await start(name, args)
  try {
    const run: Run = { causes: new Map(), raised: null }
    let kind = op.start(leaseEnv.clock())
    while (kind !== 'done') {
      if (kind === 'emit') {
        yield* parseValue(op.requestJson()) as unknown[]
        kind = op.replyNext(leaseEnv.clock())
      } else {
        kind = await advance(op, kind, env, run)
      }
    }
    finish(op, parseValue, run)
  } finally {
    op.free()
  }
}

/** Perform a commit's requests until it is done, and return what it
 *  committed. */
export async function driveCommit(op: Op, env: DriveEnv): Promise<Committed> {
  return (await drive(op, env, JSON.parse)) as Committed
}

/** What an operation's driver keeps while it runs: the exceptions
 *  merge functions raised, by key, and the first request that threw. */
interface Run {
  causes: Map<string, unknown>
  raised: { error: unknown } | null
}

/** Perform `op`'s requests until it is done, and return its outcome,
 *  decoded by `parse`. */
async function drive(op: Op, env: DriveEnv, parse: (text: string) => unknown): Promise<unknown> {
  const run: Run = { causes: new Map(), raised: null }
  let kind = op.start(leaseEnv.clock())
  while (kind !== 'done') kind = await advance(op, kind, env, run)
  return finish(op, parse, run)
}

/** Perform one request, answer it, and return the kind of the next. A
 *  request that throws is answered as failed. */
async function advance(op: Op, kind: string, env: DriveEnv, run: Run): Promise<string> {
  let next: (wall: number) => string
  try {
    next = await perform(op, kind, env, run.causes)
  } catch (e) {
    run.raised ??= { error: e }
    next = (wall) => op.replyFailed(wall)
  }
  return next(leaseEnv.clock())
}

/** A finished operation's answer, decoded by `parse`, or its error. */
function finish(op: Op, parse: (text: string) => unknown, run: Run): unknown {
  const outcome = parse(op.outcome()) as { ok?: unknown; error?: Failure; notes: Note[] }
  for (const note of outcome.notes) warn(note)
  if (outcome.error === undefined) return outcome.ok
  if (run.raised !== null) throw run.raised.error
  throw toError(outcome.error, run.causes)
}

/** Perform one request; the reply, to send with the clock. */
async function perform(
  op: Op,
  kind: string,
  env: DriveEnv,
  causes: Map<string, unknown>,
): Promise<(wall: number) => string> {
  const store = env.store
  const held = env.held ?? new Map<string, Uint8Array>()
  switch (kind) {
    case 'get': {
      const found = await store.getMany(JSON.parse(op.requestJson()) as string[])
      const [data, lengths] = pack(found.values())
      const keys = JSON.stringify([...found.keys()])
      return (wall) => op.replyValues(wall, keys, data, lengths)
    }
    case 'scan': {
      const keys: string[] = []
      for await (const key of store.keys(JSON.parse(op.requestJson()) as string)) keys.push(key)
      const text = JSON.stringify(keys)
      return (wall) => op.replyKeys(wall, text)
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
      const fn = env.mergeFns?.[rule] as BytesMergeFn
      result = fn(old ?? null, ours ?? null, theirs ?? null)
    } catch (e) {
      causes.set(key, e)
      answers.push(['failed', e instanceof Error ? e.message : String(e)])
      continue
    }
    if (result instanceof MergeChoice) {
      answers.push(result === MergeChoice.THEIRS ? 'theirs' : 'ours')
    } else {
      const blob = await blobKey(result)
      env.held?.set(blob, result)
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
    case 'corrupt_head':
      return new CorruptHeadError(f.branch as string)
    case 'unknown_tag':
      return new UnknownTagError(f.tag as string)
    case 'unknown_commit':
      return new UnknownCommitError(f.commit as string)
    case 'branch_exists':
      return new BranchExistsError(f.branch as string)
    case 'tag_exists':
      return new TagExistsError(f.tag as string)
    case 'invalid':
      return new RangeError(f.message)
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
