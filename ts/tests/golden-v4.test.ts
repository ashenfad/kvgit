/**
 * Storage v4, byte for byte as Python kvgit writes it.
 *
 * Replays the history in tests/fixtures/golden-v4.json (from
 * scripts/golden-v4.py: a fresh store, commits with non-ASCII keys,
 * binary values and every kind of info, a branch, a tag, two writers racing
 * to a three-way merge, a branch merge with a merge function and a prefix
 * policy, a fast-forward, a cherry-pick and a revert) at the same clock times, and checks the whole
 * store after every step: every key, every byte.
 */

import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { PyFloat } from '../src/pyjson'
import { Repo } from '../src/repo'
import { MergeChoice, type MergeResult } from '../src/types'
import { VersionedKV } from '../src/versioned/kv'

interface Step {
  step: string
  time: number
  store: Record<string, string>
  commit?: string
  strategy?: string
  keys?: { auto: string[]; carried: string[] }
}
const golden = JSON.parse(
  readFileSync(new URL('./fixtures/golden-v4.json', import.meta.url), 'utf8'),
) as { scenario: Step[] }
const steps = new Map(golden.scenario.map((s) => [s.step, s]))

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const b64 = (v: Uint8Array): string => Buffer.from(v).toString('base64')

async function dump(store: Memory): Promise<Record<string, string>> {
  const out: Record<string, string> = {}
  for await (const [k, v] of store.items()) out[k] = b64(v)
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => (a < b ? -1 : 1)))
}

function expectStep(name: string, actual: Record<string, string>): void {
  const want = (steps.get(name) as Step).store
  // Compare key sets first, so a mismatch names the keys rather than
  // dumping two large objects.
  expect(Object.keys(actual).sort()).toEqual(Object.keys(want).sort())
  for (const key of Object.keys(want)) {
    expect([key, actual[key]]).toEqual([key, want[key]])
  }
}

describe('storage v4 matches Python kvgit byte for byte', () => {
  it('replays the golden history', async () => {
    let clock = 0
    const now = (): number => clock
    const store = new Memory()

    clock = (steps.get('open') as Step).time
    const main = await VersionedKV.open(store, { now })
    expectStep('open', await dump(store))

    clock = (steps.get('first') as Step).time
    const updates = new Map<string, Uint8Array>()
    for (let i = 0; i < 12; i++) updates.set(`k/${String(i).padStart(2, '0')}`, bytes(`value ${i}`))
    updates.set('a', bytes('1'))
    updates.set('b', bytes('hello'))
    updates.set('ünï/ключ', Uint8Array.of(0xff, 0x00, 0x01))
    const first = await main.commit({
      updates,
      // `whole` is a float in Python (2.0); JS has one number type, so
      // a float that must be written as one is marked.
      info: { msg: 'first', n: 2, f: 0.5, whole: new PyFloat(2), é: 'ü', list: [1, null, true] },
    })
    expect(first.commit).toBe(steps.get('first')?.commit)
    expectStep('first', await dump(store))

    clock = (steps.get('second') as Step).time
    const second = await main.commit({
      updates: new Map([
        ['a', bytes('2')],
        ['k/03', bytes('value 1')],
      ]),
      removals: new Set(['b']),
    })
    expect(second.commit).toBe(steps.get('second')?.commit)
    expectStep('second', await dump(store))

    clock = (steps.get('branch') as Step).time
    const feature = await main.createBranch('feat')
    const branch = await feature.commit({
      updates: new Map([['feat-only', bytes('f')]]),
      info: { branch: 'feat' },
    })
    expect(branch.commit).toBe(steps.get('branch')?.commit)
    expectStep('branch', await dump(store))

    clock = (steps.get('tag') as Step).time
    const repo = await Repo.open(store, { now })
    await repo.tags.create('v1/ünï', branch.commit as string, {
      info: { note: 'tagged', ratio: 0.25 },
    })
    expectStep('tag', await dump(store))

    clock = (steps.get('writer one') as Step).time
    const w1 = await VersionedKV.open(store, { now })
    const w2 = await VersionedKV.open(store, { now })
    const one = await w1.commit({ updates: new Map([['x', bytes('from w1')]]) })
    expect(one.commit).toBe(steps.get('writer one')?.commit)
    expectStep('writer one', await dump(store))

    clock = (steps.get('writer two') as Step).time
    const two = await w2.commit({
      updates: new Map([['y', bytes('from w2')]]),
      info: { merge: true },
    })
    expectResult('writer two', two)
    expectStep('writer two', await dump(store))

    clock = (steps.get('diverge') as Step).time - 0.5
    const merger = await VersionedKV.open(store, { now })
    const side = (await merger.createBranch('side')) as VersionedKV
    await merger.commit({
      updates: new Map([
        ['n', bytes('main')],
        ['cfg/a', bytes('main')],
        ['cfg/b', bytes('main')],
      ]),
    })
    clock = (steps.get('diverge') as Step).time
    await side.commit({
      updates: new Map([
        ['n', bytes('side')],
        ['cfg/a', bytes('side')],
        ['s', bytes('side')],
      ]),
    })
    expectStep('diverge', await dump(store))

    clock = (steps.get('merge') as Step).time
    const merged = await merger.mergeHeads(side.currentCommit, {
      mergeFns: new Map([
        ['n', (_old, ours, theirs) => concat(theirs as Uint8Array, bytes('+'), ours as Uint8Array)],
      ]),
      mergePrefixes: new Map([['cfg/', MergeChoice.THEIRS]]),
      info: { merge: 'side' },
    })
    expectResult('merge', merged)
    expectStep('merge', await dump(store))

    clock = (steps.get('fast forward') as Step).time
    const ff = (await merger.createBranch('ff')) as VersionedKV
    await ff.commit({ updates: new Map([['ff', bytes('1')]]) })
    expectResult('fast forward', await merger.mergeHeads(ff.currentCommit))
    expectStep('fast forward', await dump(store))

    clock = (steps.get('cherry-pick') as Step).time - 0.5
    await side.commit({ updates: new Map([['pick', bytes('picked')]]), removals: new Set(['s']) })
    const picked = side.currentCommit
    const parent = (await side.parents(picked))[0] as string
    clock = (steps.get('cherry-pick') as Step).time
    expectResult('cherry-pick', await merger.applyChange(parent, picked, { info: { pick: true } }))
    expectStep('cherry-pick', await dump(store))

    clock = (steps.get('revert') as Step).time
    expectResult('revert', await merger.applyChange(picked, parent))
    expectStep('revert', await dump(store))
  })
})

function expectResult(name: string, result: MergeResult): void {
  const want = steps.get(name) as Step
  expect([name, result.strategy, result.commit]).toEqual([name, want.strategy, want.commit])
  expect({ auto: result.autoMergedKeys, carried: result.carriedKeys }).toEqual(want.keys)
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let at = 0
  for (const p of parts) {
    out.set(p, at)
    at += p.length
  }
  return out
}
