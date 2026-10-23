/**
 * Ancestry by generation: the delta between two tips, whether one
 * commit is another's ancestor, and what a receiver holds above a
 * generation, against the answers a walk of the whole history gives;
 * and what they cost on a long history.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { MemoryRemote, VersionedKV, pullBranch, pushBranch, walkDelta } from '../src/index'
import type { KVStore } from '../src/types'
import { ancestorsAbove, ancestryDelta, isAncestor } from '../src/versioned/ancestry'
import { PARENT_COMMIT, PARENT_GENS, dumps } from '../src/versioned/layout'

/** A deterministic pseudo-random sequence in [0, 1). */
function random(seed: number): () => number {
  let s = seed >>> 0
  return () => {
    s = (s + 0x6d2b79f5) >>> 0
    let t = s
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32
  }
}

interface Dag {
  store: Memory
  commits: string[]
  parents: Map<string, string[]>
}

/**
 * A history of `n` commits as their parent records: several lines of
 * work forking off and merging back, some commits with two parents.
 * With `generations: false`, the records history written before
 * generations has.
 */
async function dag(n: number, seed: number, generations = true): Promise<Dag> {
  const rand = random(seed)
  const store = new Memory()
  const commits: string[] = []
  const parents = new Map<string, string[]>()
  const gen = new Map<string, number>()
  const tips: string[] = []
  for (let i = 0; i < n; i++) {
    const c = `c${String(i).padStart(4, '0')}`
    let ps: string[] = []
    if (i > 0) {
      const pick = (): string =>
        rand() < 0.7 && tips.length > 0
          ? (tips[Math.floor(rand() * tips.length)] as string)
          : (commits[Math.floor(rand() * commits.length)] as string)
      const first = pick()
      ps = [first]
      if (rand() < 0.25) {
        const second = pick()
        if (second !== first) ps.push(second)
      }
    }
    for (const p of ps) {
      const at = tips.indexOf(p)
      if (at !== -1) tips.splice(at, 1)
    }
    tips.push(c)
    if (tips.length > 5) tips.shift()
    commits.push(c)
    parents.set(c, ps)
    gen.set(c, ps.length === 0 ? 0 : 1 + Math.max(...ps.map((p) => gen.get(p) as number)))
    await store.set(PARENT_COMMIT(c), dumps(ps))
    if (generations && ps.length > 0) {
      await store.set(PARENT_GENS(c), dumps(ps.map((p) => gen.get(p) as number)))
    }
  }
  return { store, commits, parents }
}

/** Every commit reachable from `starts`, the slow way. */
function reach(d: Dag, starts: Iterable<string>): Set<string> {
  const out = new Set<string>()
  const stack = [...starts].filter((s) => d.parents.has(s))
  while (stack.length > 0) {
    const c = stack.pop() as string
    if (out.has(c)) continue
    out.add(c)
    stack.push(...(d.parents.get(c) as string[]))
  }
  return out
}

/** `inner`, counting the keys read through it in `reads`. */
function counting(inner: KVStore): KVStore & { reads: number } {
  const counter = { reads: 0 }
  return new Proxy(inner, {
    get(target, prop) {
      if (prop === 'reads') return counter.reads
      const value = Reflect.get(target, prop, target)
      if (typeof value !== 'function') return value
      if (prop === 'get') {
        return (key: string) => {
          counter.reads++
          return target.get(key)
        }
      }
      if (prop === 'getMany') {
        return (keys: Iterable<string>) => {
          const list = [...keys]
          counter.reads += list.length
          return target.getMany(list)
        }
      }
      return value.bind(target)
    },
    set(_target, prop, value) {
      if (prop !== 'reads') return false
      counter.reads = value as number
      return true
    },
  }) as KVStore & { reads: number }
}

describe('ancestry, against the whole-history walk', () => {
  for (const generations of [true, false]) {
    const kind = generations ? 'by generation' : 'without generations'

    it(`agrees on every delta of a forking, merging history (${kind})`, async () => {
      for (const seed of [1, 2, 3]) {
        const d = await dag(160, seed, generations)
        const rand = random(seed * 7919)
        for (let trial = 0; trial < 60; trial++) {
          const want = d.commits[Math.floor(rand() * d.commits.length)] as string
          const have = Array.from(
            { length: Math.floor(rand() * 3) },
            () => d.commits[Math.floor(rand() * d.commits.length)] as string,
          )
          if (rand() < 0.2) have.push('missing')
          const had = reach(d, have)
          const expected = [...reach(d, [want])].filter((c) => !had.has(c)).sort()
          const delta = await ancestryDelta(d.store, want, have)
          expect([...delta.keys()].sort()).toEqual(expected)
          for (const [c, ps] of delta) expect(ps).toEqual(d.parents.get(c))
        }
      }
    })

    it(`agrees on which commits are ancestors (${kind})`, async () => {
      const d = await dag(160, 4, generations)
      const rand = random(17)
      for (let trial = 0; trial < 200; trial++) {
        const a = d.commits[Math.floor(rand() * d.commits.length)] as string
        const from = d.commits[Math.floor(rand() * d.commits.length)] as string
        expect(await isAncestor(d.store, a, from)).toBe(reach(d, [from]).has(a))
      }
      expect(await isAncestor(d.store, 'missing', d.commits.at(-1) as string)).toBe(false)
      expect(await isAncestor(d.store, d.commits[0] as string, 'missing')).toBe(false)
      expect(await isAncestor(d.store, 'missing', 'missing')).toBe(false)
    })

    it(`holds at least what a receiver has above a generation (${kind})`, async () => {
      const d = await dag(160, 5, generations)
      const tip = d.commits.at(-1) as string
      const above = await ancestorsAbove(d.store, [tip], 40)
      const all = reach(d, [tip])
      for (const c of above) expect(all.has(c)).toBe(true)
      // a commit reachable from the tip at generation 40 or above is in it
      const gens = new Map<string, number>()
      for (const c of d.commits) {
        const ps = d.parents.get(c) as string[]
        gens.set(c, ps.length === 0 ? 0 : 1 + Math.max(...ps.map((p) => gens.get(p) as number)))
      }
      for (const c of all) if ((gens.get(c) as number) >= 40) expect(above.has(c)).toBe(true)
      if (generations) expect(above.size).toBeLessThan(all.size)
    })
  }

  it('throws on a commit missing from what is wanted', async () => {
    const d = await dag(20, 6)
    await d.store.remove(PARENT_COMMIT('c0010'))
    const tip = d.commits.at(-1) as string
    if (reach(d, [tip]).has('c0010')) {
      await expect(ancestryDelta(d.store, tip, [])).rejects.toThrow(/commit record missing/)
    }
    await expect(ancestryDelta(d.store, 'missing', [])).rejects.toThrow(/commit record missing/)
  })
})

describe('the cost of a sync on a long history', () => {
  const BRANCH = 'chat'
  const N = 300

  async function long(): Promise<{ store: Memory; vk: VersionedKV }> {
    const store = new Memory()
    const vk = await VersionedKV.open(store, { branch: BRANCH })
    for (let i = 0; i < N; i++) {
      await vk.commit({ updates: new Map([[`k${i % 10}`, dumps(i)]]) })
    }
    return { store, vk }
  }

  it('walks the delta, not the history', async () => {
    const { store, vk } = await long()
    const before = vk.currentCommit
    await vk.commit({ updates: new Map([['k', dumps('one more')]]) })
    const counted = counting(store)
    const sent: string[] = []
    for await (const wc of walkDelta(counted, { want: vk.currentCommit, have: [before] })) {
      sent.push(wc.hash)
    }
    expect(sent).toEqual([vk.currentCommit])
    expect(counted.reads).toBeLessThan(40)
  })

  it('pushes and pulls one commit with a few reads', async () => {
    const remoteStore = counting(new Memory())
    const remote = new MemoryRemote(remoteStore)
    const a = await long()
    await pushBranch(a.store, remote, BRANCH)
    const b = new Memory()
    await pullBranch(b, remote, BRANCH)

    await a.vk.commit({ updates: new Map([['k', dumps('one more')]]) })
    const aCounting = counting(a.store)
    remoteStore.reads = 0
    expect((await pushBranch(aCounting, remote, BRANCH)).status).toBe('pushed')
    expect(aCounting.reads).toBeLessThan(60)

    const bCounting = counting(b)
    expect((await pullBranch(bCounting, remote, BRANCH)).status).toBe('fast-forwarded')
    expect(bCounting.reads).toBeLessThan(60)
    expect(remoteStore.reads).toBeLessThan(120)
  })
})
