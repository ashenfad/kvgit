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
    const repo = await Repo.open(store, { now })
    const main = await repo.worktree('main', { create: true })
    expectStep('open', await dump(store))

    clock = (steps.get('first') as Step).time
    for (let i = 0; i < 12; i++) main.set(`k/${String(i).padStart(2, '0')}`, bytes(`value ${i}`))
    main.set('a', bytes('1'))
    main.set('b', bytes('hello'))
    main.set('ünï/ключ', Uint8Array.of(0xff, 0x00, 0x01))
    const first = await main.commit({
      // `whole` is a float in Python (2.0); JS has one number type, so
      // a float that must be written as one is marked.
      info: { msg: 'first', n: 2, f: 0.5, whole: new PyFloat(2), é: 'ü', list: [1, null, true] },
    })
    expect(first.commit).toBe(steps.get('first')?.commit)
    expectStep('first', await dump(store))

    clock = (steps.get('second') as Step).time
    main.set('a', bytes('2'))
    main.set('k/03', bytes('value 1'))
    await main.delete('b')
    const second = await main.commit()
    expect(second.commit).toBe(steps.get('second')?.commit)
    expectStep('second', await dump(store))

    clock = (steps.get('branch') as Step).time
    await repo.branches.create('feat', main.head)
    const feature = await repo.worktree('feat')
    feature.set('feat-only', bytes('f'))
    const branch = await feature.commit({ info: { branch: 'feat' } })
    expect(branch.commit).toBe(steps.get('branch')?.commit)
    expectStep('branch', await dump(store))

    clock = (steps.get('tag') as Step).time
    await repo.tags.create('v1/ünï', branch.commit as string, {
      info: { note: 'tagged', ratio: 0.25 },
    })
    expectStep('tag', await dump(store))

    clock = (steps.get('writer one') as Step).time
    const w1 = await repo.worktree('main')
    const w2 = await repo.worktree('main')
    w1.set('x', bytes('from w1'))
    const one = await w1.commit()
    expect(one.commit).toBe(steps.get('writer one')?.commit)
    expectStep('writer one', await dump(store))

    clock = (steps.get('writer two') as Step).time
    w2.set('y', bytes('from w2'))
    const two = await w2.commit({ info: { merge: true } })
    expectResult('writer two', two)
    expectStep('writer two', await dump(store))

    clock = (steps.get('diverge') as Step).time - 0.5
    const merger = await repo.worktree('main')
    await repo.branches.create('side', merger.head)
    const side = await repo.worktree('side')
    merger.set('n', bytes('main'))
    merger.set('cfg/a', bytes('main'))
    merger.set('cfg/b', bytes('main'))
    await merger.commit()
    clock = (steps.get('diverge') as Step).time
    side.set('n', bytes('side'))
    side.set('cfg/a', bytes('side'))
    side.set('s', bytes('side'))
    await side.commit()
    expectStep('diverge', await dump(store))

    clock = (steps.get('merge') as Step).time
    const merged = await merger.merge(
      { commit: side.head },
      {
        mergeFns: new Map([
          [
            'n',
            (_old, ours, theirs) => concat(theirs as Uint8Array, bytes('+'), ours as Uint8Array),
          ],
        ]),
        mergePrefixes: new Map([['cfg/', MergeChoice.THEIRS]]),
        info: { merge: 'side' },
      },
    )
    expectResult('merge', merged)
    expectStep('merge', await dump(store))

    clock = (steps.get('fast forward') as Step).time
    await repo.branches.create('ff', merger.head)
    const ff = await repo.worktree('ff')
    ff.set('ff', bytes('1'))
    await ff.commit()
    expectResult('fast forward', await merger.merge({ commit: ff.head }))
    expectStep('fast forward', await dump(store))

    clock = (steps.get('cherry-pick') as Step).time - 0.5
    side.set('pick', bytes('picked'))
    await side.delete('s')
    await side.commit()
    const picked = side.head
    const parent = (await repo.getCommit(picked)).parents[0] as string
    clock = (steps.get('cherry-pick') as Step).time
    expectResult('cherry-pick', await merger.apply(parent, picked, { info: { pick: true } }))
    expectStep('cherry-pick', await dump(store))

    clock = (steps.get('revert') as Step).time
    expectResult('revert', await merger.apply(picked, parent))
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
