/**
 * Runs a corpus scenario (`corpus/README.md`) through the package's
 * public API.
 *
 * The clock and lease owners are the scenario's: repositories open with
 * its clock as `now`, and `leaseEnv` is replaced while it runs (a wait
 * for a lease advances the clock by the time waited), so every run
 * writes the same bytes.
 */

import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs'
import { join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Memory } from '../../src/backends/memory'
import {
  type CommitInfo,
  MergeChoice,
  type MergeResult,
  type MergeRule,
  type Ref,
  Repo,
  type Snapshot,
  Worktree,
  makeTextMerge,
  textMergeResult,
} from '../../src/index'
import { ROOT_COMMIT } from '../../src/versioned/layout'
import { leaseEnv } from '../../src/versioned/lease'

export const CORPUS = fileURLToPath(new URL('../../../corpus/scenarios', import.meta.url))
export const HOST = 'ts'

type Json = null | boolean | number | string | Json[] | { [key: string]: Json }
type Step = { op: string; expect?: Json; [field: string]: Json | undefined }

export interface Scenario {
  description: string
  only?: string[]
  clock: number
  store?: Record<string, Json>
  steps: Step[]
  final?: Record<string, Json>
}

export function scenarioPaths(dir: string = CORPUS): string[] {
  const out: string[] = []
  for (const name of readdirSync(dir).sort()) {
    const path = join(dir, name)
    if (statSync(path).isDirectory()) out.push(...scenarioPaths(path))
    else if (name.endsWith('.json')) out.push(path)
  }
  return out
}

export function scenarioId(path: string): string {
  return relative(CORPUS, path).replace(/\.json$/, '')
}

export function load(path: string): Scenario {
  return JSON.parse(readFileSync(path, 'utf8')) as Scenario
}

/** Write `final` as the scenario's last field, leaving the text before
 *  it as it was written. */
export function saveFinal(path: string, final: Record<string, Json>): void {
  const text = readFileSync(path, 'utf8')
  const cut = text.indexOf('\n  "final": ')
  const head = cut !== -1 ? text.slice(0, cut) : text.slice(0, text.lastIndexOf('\n}'))
  const body = JSON.stringify(final, null, 2).replaceAll('\n', '\n  ')
  writeFileSync(path, `${head.trimEnd().replace(/,$/, '')},\n  "final": ${body}\n}\n`)
}

// --- Bytes ---

const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true })

export function decodeBytes(value: Json | undefined): Uint8Array {
  if (typeof value === 'string') return new TextEncoder().encode(value)
  if (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value).length === 1 &&
    typeof value.b64 === 'string'
  ) {
    return new Uint8Array(Buffer.from(value.b64, 'base64'))
  }
  throw new Error(`not a bytes value: ${JSON.stringify(value)}`)
}

export function encodeBytes(value: Uint8Array): Json {
  try {
    return utf8.decode(value)
  } catch {
    return { b64: Buffer.from(value).toString('base64') }
  }
}

// --- Merge rules ---

type Bytes = Uint8Array | null

const RULES: Record<string, MergeRule<Uint8Array>> = {
  text: makeTextMerge(),
  text_strict: makeTextMerge({ strict: true }),
  ours: MergeChoice.OURS,
  theirs: MergeChoice.THEIRS,
  pick_ours: () => MergeChoice.OURS,
  pick_theirs: () => MergeChoice.THEIRS,
  concat: (_old: Bytes, ours: Bytes, theirs: Bytes) => {
    const a = theirs ?? new Uint8Array()
    const b = ours ?? new Uint8Array()
    const out = new Uint8Array(a.length + 1 + b.length)
    out.set(a)
    out[a.length] = 0x2b // '+'
    out.set(b, a.length + 1)
    return out
  },
  fail: () => {
    throw new Error('this rule refuses every key')
  },
}

function rule(name: Json | undefined): MergeRule<Uint8Array> {
  const found = RULES[name as string]
  if (found === undefined) throw new ScenarioFailure(`unknown merge rule ${show(name)}`)
  return found
}

function ruleMap(rules: Json | undefined): ReadonlyMap<string, MergeRule<Uint8Array>> | undefined {
  if (rules === undefined) return undefined
  return new Map(Object.entries(rules as Record<string, Json>).map(([k, v]) => [k, rule(v)]))
}

/** The merge rules a step names, as options. */
function rules(step: Step) {
  const fns = ruleMap(step.merge_fns)
  const prefixes = ruleMap(step.merge_prefixes)
  return {
    ...(fns !== undefined && { mergeFns: fns }),
    ...(prefixes !== undefined && { mergePrefixes: prefixes }),
    ...(step.default_merge !== undefined && { defaultMerge: rule(step.default_merge) }),
  }
}

// --- The run ---

/** The name a scenario uses for an error: kvgit's own errors by class
 *  name, and a caller's mistake (a plain `Error`, `TypeError`,
 *  `RangeError`) as `invalid`. */
export function errorName(e: unknown): string {
  const name = (e as Error).name
  return ['Error', 'TypeError', 'RangeError'].includes(name) ? 'invalid' : name
}

export class ScenarioFailure extends Error {
  override readonly name = 'ScenarioFailure'
}

type Handle = Worktree<Uint8Array> | Snapshot<Uint8Array>

function same(a: unknown, b: unknown): boolean {
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    return a.length === b.length && a.every((x, i) => x === b[i])
  }
  return JSON.stringify(a) === JSON.stringify(b)
}

function show(value: unknown): string {
  if (value instanceof Uint8Array) return JSON.stringify(encodeBytes(value))
  return JSON.stringify(value)
}

export class Run {
  readonly store = new Memory()
  readonly repos = new Map<string, Repo<Uint8Array>>()
  readonly handles = new Map<string, Handle>()
  readonly vars = new Map<string, unknown>([['$root', ROOT_COMMIT]])
  clock: number
  #leases = 0

  constructor(
    readonly scenario: Scenario,
    readonly name: string,
  ) {
    this.clock = scenario.clock
  }

  // Variables

  arg<T = unknown>(value: Json | undefined): T {
    if (typeof value === 'string' && value.startsWith('$')) {
      if (!this.vars.has(value)) throw new ScenarioFailure(`${value} is used before it is bound`)
      return this.vars.get(value) as T
    }
    return value as T
  }

  /** `text` with each bound variable in it replaced by its value,
   *  which must be a string. */
  interpolate(text: string): string {
    return text.replace(/\$[A-Za-z_][A-Za-z0-9_]*/g, (name) => {
      if (!this.vars.has(name)) throw new ScenarioFailure(`${name} is used before it is bound`)
      const value = this.vars.get(name)
      if (typeof value !== 'string') {
        throw new ScenarioFailure(`${name} is ${show(value)}, not a string`)
      }
      return value
    })
  }

  match(what: string, expected: Json | Uint8Array | undefined, actual: unknown): void {
    let want: unknown = expected
    if (typeof expected === 'string' && expected.startsWith('$')) {
      if (!this.vars.has(expected)) {
        this.vars.set(expected, actual)
        return
      }
      want = this.vars.get(expected)
    }
    if (!same(want, actual)) {
      throw new ScenarioFailure(`${what}: expected ${show(want)}, got ${show(actual)}`)
    }
  }

  matchSorted(what: string, expected: Json | undefined, actual: Iterable<string>): void {
    const want = (expected as Json[]).map((e) => this.arg<string>(e)).sort()
    this.match(what, want, [...actual].sort())
  }

  matchMap(what: string, expected: Json | undefined, actual: Map<string, string>): void {
    const want = expected as Record<string, Json>
    const names = [...actual.keys()].sort()
    if (!same(Object.keys(want).sort(), names)) {
      throw new ScenarioFailure(
        `${what}: expected names ${show(Object.keys(want).sort())}, got ${show(names)}`,
      )
    }
    for (const [name, value] of Object.entries(want)) {
      this.match(`${what} '${name}'`, value, actual.get(name))
    }
  }

  // Handles

  repo(step: Step): Repo<Uint8Array> {
    const repo = this.repos.get(step.repo as string)
    if (repo === undefined) throw new ScenarioFailure(`no repository ${step.repo}`)
    return repo
  }

  wt(step: Step): Worktree<Uint8Array> {
    const handle = this.handles.get(step.wt as string)
    if (!(handle instanceof Worktree)) throw new ScenarioFailure(`${step.wt} is not a worktree`)
    return handle
  }

  from(step: Step): Handle {
    const handle = this.handles.get(step.from as string)
    if (handle === undefined) throw new ScenarioFailure(`no worktree or snapshot ${step.from}`)
    return handle
  }

  ref(step: Step): Ref {
    const given = (['branch', 'commit', 'tag'] as const).filter((k) => k in step)
    if (given.length !== 1) {
      throw new ScenarioFailure(`name exactly one of branch, commit, tag: ${show(step)}`)
    }
    const kind = given[0] as 'branch' | 'commit' | 'tag'
    return { [kind]: this.arg<string>(step[kind]) } as Ref
  }

  // Running

  async run(): Promise<Record<string, Json>> {
    for (const [key, value] of Object.entries(this.scenario.store ?? {})) {
      await this.store.set(key, decodeBytes(value))
    }
    const saved = { ...leaseEnv }
    leaseEnv.clock = () => this.clock
    leaseEnv.sleep = async (seconds) => {
      this.clock += seconds
    }
    leaseEnv.owner = () => `lease-${++this.#leases}`
    try {
      for (const [index, step] of this.scenario.steps.entries()) {
        try {
          await this.step(step)
        } catch (e) {
          if (e instanceof ScenarioFailure) {
            throw new ScenarioFailure(`${this.name} step ${index} (${step.op}): ${e.message}`)
          }
          throw e
        }
      }
    } finally {
      Object.assign(leaseEnv, saved)
    }
    const final: Record<string, Json> = {}
    const keys: string[] = []
    for await (const key of this.store.keys()) keys.push(key)
    // Sorted by code point, as Python sorts strings.
    keys.sort((a, b) => {
      const x = [...a].map((c) => c.codePointAt(0) as number)
      const y = [...b].map((c) => c.codePointAt(0) as number)
      for (let i = 0; i < Math.min(x.length, y.length); i++) {
        if (x[i] !== y[i]) return (x[i] as number) - (y[i] as number)
      }
      return x.length - y.length
    })
    for (const key of keys) final[key] = encodeBytes((await this.store.get(key)) as Uint8Array)
    return final
  }

  async step(step: Step): Promise<void> {
    const handler = (this as unknown as Record<string, (s: Step) => Promise<unknown>>)[
      `op_${step.op}`
    ]
    if (typeof handler !== 'function') throw new ScenarioFailure(`unknown op '${step.op}'`)
    const expect = step.expect
    if (
      expect !== null &&
      typeof expect === 'object' &&
      !Array.isArray(expect) &&
      'error' in expect
    ) {
      try {
        await handler.call(this, step)
      } catch (e) {
        if (e instanceof ScenarioFailure) throw e
        const name = errorName(e)
        if (name !== expect.error) {
          throw new ScenarioFailure(
            `expected ${expect.error}, threw ${name}: ${(e as Error).message}`,
          )
        }
        if ('keys' in expect) {
          this.matchSorted('conflicting keys', expect.keys, (e as { keys: Set<string> }).keys)
        }
        return
      }
      throw new ScenarioFailure(`expected ${expect.error}, but it succeeded`)
    }
    const result = await handler.call(this, step)
    if ('expect' in step) this.check(step.op, step.expect, result)
  }

  check(op: string, expected: Json | undefined, actual: unknown): void {
    switch (op) {
      case 'commit':
      case 'merge':
      case 'apply':
      case 'cherry_pick':
      case 'revert': {
        const result = actual as MergeResult
        const fields: Record<string, unknown> = {
          merged: result.merged,
          commit: result.commit,
          strategy: result.strategy,
          auto_merged_keys: result.autoMergedKeys,
          carried_keys: result.carriedKeys,
        }
        for (const [field, want] of Object.entries(expected as Record<string, Json>)) {
          if (!(field in fields)) throw new ScenarioFailure(`unknown result field '${field}'`)
          if (field === 'auto_merged_keys' || field === 'carried_keys') {
            this.matchSorted(field, want, fields[field] as string[])
          } else {
            this.match(field, want, fields[field])
          }
        }
        return
      }
      case 'text_merge': {
        const got = actual as { merged: Uint8Array; conflicted?: boolean }
        for (const [field, want] of Object.entries(expected as Record<string, Json>)) {
          if (field === 'merged') this.match(field, decodeBytes(want), got.merged)
          else this.match(field, want, got[field as 'conflicted'])
        }
        return
      }
      case 'get':
        this.match('value', expected === null ? null : decodeBytes(expected), actual ?? null)
        return
      case 'keys':
        this.matchSorted('keys', expected, actual as string[])
        return
      case 'log': {
        const want = expected as Json[]
        const got = actual as string[]
        if (want.length !== got.length) {
          throw new ScenarioFailure(`log: expected ${want.length} commits, got ${show(got)}`)
        }
        want.forEach((e, n) => this.match(`log[${n}]`, e, got[n]))
        return
      }
      case 'branches':
      case 'tags':
        this.matchMap(op, expected, actual as Map<string, string>)
        return
      case 'get_commit': {
        const commit = actual as Record<string, unknown>
        for (const [field, want] of Object.entries(expected as Record<string, Json>)) {
          if (field === 'parents') {
            const wantParents = want as Json[]
            const got = commit.parents as string[]
            if (wantParents.length !== got.length) {
              throw new ScenarioFailure(`parents: expected ${show(wantParents)}, got ${show(got)}`)
            }
            wantParents.forEach((e, n) => this.match(`parents[${n}]`, e, got[n]))
          } else {
            this.match(field, want, commit[field])
          }
        }
        return
      }
      case 'diff': {
        const diff = actual as Record<'added' | 'removed' | 'modified', Set<string>>
        for (const field of ['added', 'removed', 'modified'] as const) {
          const want = (expected as Record<string, Json>)[field]
          if (want !== undefined) this.matchSorted(field, want, diff[field])
        }
        return
      }
      default:
        this.match(op, expected, actual)
    }
  }

  // --- Ops ---

  async op_clock(step: Step): Promise<void> {
    if ('set' in step) this.clock = step.set as number
    else this.clock += step.advance as number
  }

  async op_open(step: Step): Promise<void> {
    if (step.recover !== undefined) {
      throw new ScenarioFailure(`no recoverer ${show(step.recover)} in this package`)
    }
    this.repos.set(
      step.as as string,
      await Repo.open(this.store, { now: () => this.clock, ...rules(step) }),
    )
  }

  async op_worktree(step: Step): Promise<void> {
    const wt = await this.repo(step).worktree(step.branch as string, {
      create: (step.create as boolean | undefined) ?? false,
    })
    this.handles.set(step.as as string, wt)
  }

  async op_snapshot(step: Step): Promise<void> {
    this.handles.set(step.as as string, await this.repo(step).snapshot(this.ref(step)))
  }

  async op_set(step: Step): Promise<void> {
    this.wt(step).set(step.key as string, decodeBytes(step.value))
  }

  async op_delete(step: Step): Promise<void> {
    await this.wt(step).delete(step.key as string)
  }

  async op_get(step: Step): Promise<Uint8Array | undefined> {
    return this.from(step).get(step.key as string)
  }

  async op_keys(step: Step): Promise<string[]> {
    return this.from(step).keys()
  }

  async op_head(step: Step): Promise<string> {
    return this.wt(step).head
  }

  async op_discard(step: Step): Promise<void> {
    this.wt(step).discard()
  }

  async op_refresh(step: Step): Promise<void> {
    await this.wt(step).refresh()
  }

  async op_reset(step: Step): Promise<void> {
    await this.wt(step).reset(this.arg<string>(step.commit))
  }

  async op_commit(step: Step): Promise<MergeResult> {
    return this.wt(step).commit({
      ...(step.info !== undefined && { info: step.info as CommitInfo }),
      ...(step.on_moved !== undefined && { onMoved: step.on_moved as 'merge' | 'raise' }),
      ...(step.on_conflict !== undefined && {
        onConflict: step.on_conflict as 'raise' | 'abandon',
      }),
      ...(step.keys !== undefined && { keys: step.keys as string[] }),
      ...rules(step),
    })
  }

  async op_merge(step: Step): Promise<MergeResult> {
    return this.wt(step).merge(this.ref(step), {
      ...(step.info !== undefined && { info: step.info as CommitInfo }),
      ...(step.on_conflict !== undefined && {
        onConflict: step.on_conflict as 'raise' | 'abandon',
      }),
      ...(step.fast_forward !== undefined && { fastForward: step.fast_forward as boolean }),
      ...rules(step),
    })
  }

  applyOptions(step: Step) {
    return {
      ...(step.info !== undefined && { info: step.info as CommitInfo }),
      ...(step.on_conflict !== undefined && {
        onConflict: step.on_conflict as 'raise' | 'abandon',
      }),
      ...rules(step),
    }
  }

  async op_apply(step: Step): Promise<MergeResult> {
    return this.wt(step).apply(
      this.arg<string>(step.base),
      this.arg<string>(step.target),
      this.applyOptions(step),
    )
  }

  async op_cherry_pick(step: Step): Promise<MergeResult> {
    return this.wt(step).cherryPick(this.arg<string>(step.commit), this.applyOptions(step))
  }

  async op_revert(step: Step): Promise<MergeResult> {
    return this.wt(step).revert(this.arg<string>(step.commit), this.applyOptions(step))
  }

  async op_text_merge(step: Step): Promise<{ merged: Uint8Array; conflicted?: boolean }> {
    const [old, ours, theirs] = (['old', 'ours', 'theirs'] as const).map((side) =>
      step[side] === null ? null : decodeBytes(step[side]),
    ) as [Bytes, Bytes, Bytes]
    const labels = {
      ...(step.ours_label !== undefined && { oursLabel: step.ours_label as string }),
      ...(step.theirs_label !== undefined && { theirsLabel: step.theirs_label as string }),
    }
    if (step.strict === true) {
      return { merged: makeTextMerge({ ...labels, strict: true })(old, ours, theirs) }
    }
    const [merged, conflicted] = textMergeResult(old, ours, theirs, labels)
    return { merged, conflicted }
  }

  async op_branch_create(step: Step): Promise<string> {
    const at = step.at === undefined ? undefined : this.arg<string>(step.at)
    return this.repo(step).branches.create(step.name as string, at)
  }

  async op_branch_delete(step: Step): Promise<void> {
    await this.repo(step).branches.delete(step.name as string)
  }

  async op_repair_head(step: Step): Promise<string | null> {
    return this.repo(step).repairHead(step.name as string)
  }

  async op_branches(step: Step): Promise<Map<string, string>> {
    const branches = this.repo(step).branches
    const out = new Map<string, string>()
    for (const name of await branches.list()) out.set(name, await branches.get(name))
    return out
  }

  async op_tag_create(step: Step): Promise<void> {
    await this.repo(step).tags.create(step.name as string, this.arg<string>(step.commit), {
      ...(step.info !== undefined && { info: step.info as CommitInfo }),
    })
  }

  async op_tag_delete(step: Step): Promise<void> {
    await this.repo(step).tags.delete(step.name as string)
  }

  async op_tags(step: Step): Promise<Map<string, string>> {
    return this.repo(step).tags.entries()
  }

  async op_get_commit(step: Step): Promise<unknown> {
    return this.repo(step).getCommit(this.arg<string>(step.commit))
  }

  async op_log(step: Step): Promise<string[]> {
    const out: string[] = []
    for await (const commit of this.repo(step).log(this.ref(step), {
      ...(step.first_parent !== undefined && { firstParent: step.first_parent as boolean }),
      ...(step.limit !== undefined && { limit: step.limit as number }),
    })) {
      out.push(commit.hash)
    }
    return out
  }

  async op_diff(step: Step): Promise<unknown> {
    return this.repo(step).diff(this.arg<string>(step.a), this.arg<string>(step.b))
  }

  async op_merge_base(step: Step): Promise<string | null> {
    return this.repo(step).mergeBase(this.arg<string>(step.a), this.arg<string>(step.b))
  }

  async op_gc(step: Step): Promise<number> {
    return this.repo(step).gc({
      minAge: (step.min_age as number | undefined) ?? 3600,
      deep: (step.deep as boolean | undefined) ?? false,
      wait: (step.wait as boolean | undefined) ?? true,
    })
  }

  async op_store_set(step: Step): Promise<void> {
    await this.store.set(this.interpolate(step.key as string), decodeBytes(step.value))
  }

  async op_store_delete(step: Step): Promise<void> {
    await this.store.remove(this.interpolate(step.key as string))
  }
}
