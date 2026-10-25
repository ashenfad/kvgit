/**
 * Lazy keysets: a handle that reads its keyset from the HAMT on demand
 * through a bounded node cache instead of loading all of it on open.
 * These pin that it sees and writes exactly what an eager handle does,
 * commit for commit, and keeps the commit protocol's guarantees: raise
 * on a moved branch, rollback of a failed attempt, reset and branch
 * copies.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  BranchMovedError,
  ConcurrencyError,
  Hamt,
  type KVStore,
  NodeCache,
  Repo,
  VersionedKV,
  jsonCodec,
} from '../src/index'
import { IN_FLIGHT_KEY, IN_FLIGHT_PREFIX } from '../src/versioned/layout'
import { racePublish } from './races'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array | null): string | null => (b === null ? null : dec.decode(b))

/** A deterministic PRNG, so a failing sequence replays. */
function rng(seed: number): () => number {
  let s = seed >>> 0
  return () => {
    s = (s + 0x6d2b79f5) >>> 0
    let t = s
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/** A memory store that counts the keys it is asked for. */
class CountingMemory extends Memory {
  reads: string[] = []
  override async get(key: string): Promise<Uint8Array | null> {
    this.reads.push(key)
    return super.get(key)
  }
  override async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const list = [...keys]
    this.reads.push(...list)
    return super.getMany(list)
  }
}

/** A memory store whose next commit batch, once armed, fails to land. */
class FailingMemory extends Memory {
  private armed = false
  arm(): void {
    this.armed = true
  }
  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const isBatch = [...writes.keys()].some((k) => k.startsWith(IN_FLIGHT_KEY('')))
    if (isBatch && this.armed) {
      this.armed = false
      throw new Error('landing write failed')
    }
    return super.casMany(expected, writes, removes)
  }
}

async function storeKeys(store: KVStore, prefix?: string): Promise<Set<string>> {
  const out = new Set<string>()
  for await (const k of store.keys(prefix)) out.add(k)
  return out
}

async function valuesOf(vk: VersionedKV): Promise<Record<string, string | null>> {
  const out: Record<string, string | null> = {}
  const keys: string[] = []
  for await (const k of vk.keys()) keys.push(k)
  for (const k of keys.sort()) out[k] = text(await vk.get(k))
  return out
}

describe('Hamt.updatedBatch', () => {
  // Forty rounds, each also building a tree from scratch: seconds on a
  // loaded runner.
  for (const bucketMax of [1, 2, 8]) {
    it(`gives the same tree as one-at-a-time updates (bucketMax ${bucketMax})`, async () => {
      const store = new Memory()
      const rand = rng(bucketMax)
      let seq = await Hamt.empty(store, { bucketMax })
      let batch = await Hamt.empty(store, { bucketMax })
      const live = new Set<string>()
      for (let round = 0; round < 40; round++) {
        const updates: Array<[string, Uint8Array]> = []
        const removals: string[] = []
        const n = 1 + Math.floor(rand() * 30)
        for (let i = 0; i < n; i++) {
          const key = `k${Math.floor(rand() * 120)}`
          if (rand() < 0.35) removals.push(key)
          else updates.push([key, bytes(`v${round}-${i}`)])
        }
        seq = await seq.persist({ updates, removals })
        batch = await (await batch.updatedBatch({ updates, removals })).flush()
        expect(batch.root).toBe(seq.root)
        for (const [k] of updates) live.add(k)
        for (const k of removals) live.delete(k)
        // And the same as the tree built from scratch over what is left.
        const scratch = await (await Hamt.empty(new Memory(), { bucketMax })).updated({
          updates: [...(await seq.materialize())],
        })
        expect(batch.root).toBe(scratch.root)
      }
    }, 30_000)
  }

  for (const bucketMax of [1, 8]) {
    it(`keeps keys named like Object.prototype members (bucketMax ${bucketMax})`, async () => {
      // `__proto__` assigned on a plain object sets its prototype
      // instead of adding a key; the tree must hold it like any other.
      const odd = ['__proto__', 'constructor', 'toString', 'hasOwnProperty']
      const store = new Memory()
      const updates: Array<[string, Uint8Array]> = [
        ...odd.map((k): [string, Uint8Array] => [k, bytes(`v-${k}`)]),
        ...Array.from({ length: 20 }, (_, i): [string, Uint8Array] => [`k${i}`, bytes(`v${i}`)]),
      ]
      const seq = await (await Hamt.empty(store, { bucketMax })).persist({ updates })
      const batch = await (
        await (await Hamt.empty(store, { bucketMax })).updatedBatch({ updates })
      ).flush()
      expect(batch.root).toBe(seq.root)
      for (const k of odd) expect(text(await batch.get(k))).toBe(`v-${k}`)

      const seqGone = await seq.persist({ removals: odd })
      const batchGone = await (await batch.updatedBatch({ removals: odd })).flush()
      expect(batchGone.root).toBe(seqGone.root)
      for (const k of odd) expect(await batchGone.get(k)).toBeNull()
      // Removing keys that aren't there changes nothing.
      expect((await batchGone.updatedBatch({ removals: odd })).root).toBe(batchGone.root)
      expect((await seqGone.updated({ removals: odd })).root).toBe(seqGone.root)
    })
  }

  it('refuses a chunk size that would never advance', async () => {
    const tree = await (await Hamt.empty(new Memory())).persist({ updates: [['a', bytes('1')]] })
    for (const chunk of [0, -1, 1.5, Number.NaN]) {
      await expect(async () => {
        for await (const _ of tree.keysBatched(chunk)) {
          // drain
        }
      }).rejects.toThrow(RangeError)
    }
  })

  it('writes each node on the changed paths once', async () => {
    const store = new Memory()
    const updates: Array<[string, Uint8Array]> = []
    for (let i = 0; i < 2000; i++) updates.push([`page/${i}`, bytes(`v${i}`)])
    const base = await (await Hamt.empty(store)).updatedBatch({ updates })
    const flushed = await base.flush()
    const change: Array<[string, Uint8Array]> = [
      ['page/1', bytes('x')],
      ['page/2', bytes('y')],
    ]
    const next = await flushed.updatedBatch({ updates: change })
    const depthOf = async (key: string): Promise<number> => {
      // Path length: count the nodes a lookup visits.
      let n = 0
      const counting = new (class extends Memory {
        override async getMany(keys: Iterable<string>) {
          const asked = [...keys]
          n += asked.length
          return store.getMany(asked)
        }
      })()
      await new Hamt(counting, flushed.root).get(key)
      return n
    }
    const most = Math.max(await depthOf('page/1'), await depthOf('page/2'))
    expect(next.pending.size).toBeLessThanOrEqual(2 * most)
  })
})

describe('NodeCache', () => {
  it('holds no more than its byte bound', async () => {
    const store = new Memory()
    const updates: Array<[string, Uint8Array]> = []
    for (let i = 0; i < 500; i++) updates.push([`page/${i}`, bytes(`v${i}`)])
    const root = (await (await Hamt.empty(store)).persist({ updates })).root
    const cache = new NodeCache({ maxBytes: 2000 })
    await new Hamt(store, root, { cache }).materialize()
    const stats = cache.stats()
    expect(stats.bytes).toBeLessThanOrEqual(2000)
    expect(stats.evictions).toBeGreaterThan(0)
    expect(stats.misses).toBeGreaterThan(stats.entries)
  })
})

describe('lazy VersionedKV', () => {
  it('opens reading the head and its root, and no keyset node', async () => {
    const store = new CountingMemory()
    const writer = await VersionedKV.open(store)
    const updates = new Map<string, Uint8Array>()
    for (let i = 0; i < 500; i++) updates.set(`page/${i}`, bytes(`v${i}`))
    await writer.commit({ updates })

    store.reads = []
    const lazy = await VersionedKV.open(store, { lazy: true })
    expect(store.reads.filter((k) => k.startsWith('kvgit:'))).toEqual([])
    expect(lazy.isLazy).toBe(true)

    store.reads = []
    await VersionedKV.open(store)
    expect(store.reads.filter((k) => k.startsWith('kvgit:keyset:')).length).toBeGreaterThan(10)
  })

  it('reads, writes and commits what an eager handle does, commit for commit', async () => {
    // Two stores, the same clock and the same changes: the commits must
    // hash the same, so the trees are the same.
    let clockA = 1_760_000_000
    let clockB = 1_760_000_000
    const eager = await VersionedKV.open(new Memory(), { now: () => clockA++ })
    const lazy = await VersionedKV.open(new Memory(), { now: () => clockB++, lazy: true })
    const rand = rng(7)
    for (let round = 0; round < 30; round++) {
      const updates = new Map<string, Uint8Array>()
      const removals = new Set<string>()
      const n = 1 + Math.floor(rand() * 40)
      for (let i = 0; i < n; i++) {
        const key = `page/${Math.floor(rand() * 200)}`
        if (rand() < 0.3) {
          removals.add(key)
          updates.delete(key)
        } else {
          updates.set(key, bytes(`r${round}-${i}`))
          removals.delete(key)
        }
      }
      const a = await eager.commit({ updates, removals })
      const b = await lazy.commit({ updates, removals })
      expect(b.commit).toBe(a.commit)
      const probe = Array.from({ length: 20 }, () => `page/${Math.floor(rand() * 200)}`)
      for (const key of probe) {
        expect(text(await lazy.get(key))).toBe(text(await eager.get(key)))
        expect(await lazy.has(key)).toBe(await eager.has(key))
      }
      const many = [...(await lazy.getMany(probe))].map(([k, v]) => [k, text(v)])
      const manyEager = [...(await eager.getMany(probe))].map(([k, v]) => [k, text(v)])
      expect(new Map(many as [string, string][])).toEqual(new Map(manyEager as [string, string][]))
    }
    expect(await valuesOf(lazy)).toEqual(await valuesOf(eager))
  })

  // A 3,000-key commit: seconds on a loaded runner.
  it('hits the node cache for repeated reads', async () => {
    const store = new Memory()
    const writer = await VersionedKV.open(store)
    const updates = new Map<string, Uint8Array>()
    for (let i = 0; i < 3000; i++) updates.set(`page/${i}`, bytes(`v${i}`))
    await writer.commit({ updates })
    const cache = new NodeCache()
    const lazy = await VersionedKV.open(store, { lazy: { cache } })
    await lazy.get('page/42')
    const cold = cache.stats()
    expect(cold.misses).toBeGreaterThan(1)
    cache.resetStats()
    expect(text(await lazy.get('page/42'))).toBe('v42')
    expect(cache.stats()).toMatchObject({ misses: 0 })
  }, 30_000)
})

describe('lazy Worktree', () => {
  async function seeded(store: KVStore = new Memory()) {
    let clock = 1_760_000_000
    const repo = await Repo.open<unknown>(store, { codec: jsonCodec, now: () => clock++ })
    const seed = await repo.worktree('main', { create: true })
    for (let i = 0; i < 300; i++) seed.set(`page/${i}`, i)
    const base = (await seed.commit()).commit as string
    return { repo, store, base }
  }

  it('reads, writes, deletes and commits', async () => {
    const { repo } = await seeded()
    const wt = await repo.worktree('main', { lazy: true })
    expect(wt.lazy).toBe(true)
    expect(await wt.get('page/7')).toBe(7)
    expect(await wt.has('page/300')).toBe(false)
    wt.set('page/7', 'seven')
    wt.set('page/300', 300)
    expect(await wt.delete('page/8')).toBe(true)
    expect(await wt.delete('page/999')).toBe(false)
    expect(await wt.get('page/7')).toBe('seven')
    await wt.commit({ onMoved: 'raise' })
    expect(wt.status().clean).toBe(true)

    const again = await repo.worktree('main', { lazy: true })
    const got = await again.getMany(['page/7', 'page/8', 'page/300', 'page/9'])
    expect(got).toEqual(
      new Map<string, unknown>([
        ['page/7', 'seven'],
        ['page/300', 300],
        ['page/9', 9],
      ]),
    )
    expect((await again.keys()).length).toBe(300)
    // An eager worktree on the same branch sees the same thing.
    const eager = await repo.worktree('main')
    expect(await eager.keys()).toEqual(await again.keys())
  })

  it('raises on a moved branch, writes nothing, keeps its changes', async () => {
    const { repo, store, base } = await seeded()
    const a = await repo.worktree('main', { lazy: true })
    const b = await repo.worktree('main', { lazy: true })
    a.set('page/1', 'from a')
    b.set('page/2', 'from b')
    const first = (await a.commit({ onMoved: 'raise' })).commit as string

    const before = await storeKeys(store)
    const err = await b.commit({ onMoved: 'raise' }).then(
      () => null,
      (e: unknown) => e,
    )
    expect(err).toBeInstanceOf(BranchMovedError)
    expect((err as BranchMovedError).base).toBe(base)
    expect((err as BranchMovedError).head).toBe(first)
    expect(await storeKeys(store)).toEqual(before)
    expect(b.head).toBe(base)
    expect(await b.get('page/2')).toBe('from b')
    expect(await b.get('page/1')).toBe(1)

    // Redo on the tip.
    await b.refresh()
    expect(await b.get('page/1')).toBe('from a')
    b.set('page/2', 'from b')
    await b.commit({ onMoved: 'raise' })
    const fresh = await repo.worktree('main')
    expect(await fresh.getMany(['page/1', 'page/2'])).toEqual(
      new Map([
        ['page/1', 'from a'],
        ['page/2', 'from b'],
      ]),
    )
  })

  it('raises when the branch moves inside the publish window, and rolls back', async () => {
    const { repo, store, base } = await seeded()
    const a = await repo.worktree('main', { lazy: true })
    const b = await repo.worktree('main', { lazy: true })
    b.set('page/2', 'from b')
    // Move the branch between b's read of HEAD and its publish.
    const realCas = Memory.prototype.casMany
    let raced = false
    store.casMany = async function (this: Memory, expected, writes, removes) {
      if (!raced && [...writes.keys()].some((k) => k.startsWith('__branch_head__main'))) {
        raced = true
        a.set('page/1', 'from a')
        await a.commit()
      }
      return realCas.call(this, expected, writes, removes)
    } as Memory['casMany']
    await expect(b.commit({ onMoved: 'raise' })).rejects.toBeInstanceOf(BranchMovedError)
    expect(b.head).toBe(base)
    expect(await b.get('page/1')).toBe(1)
    expect(await b.get('page/2')).toBe('from b')
    expect(await storeKeys(store, IN_FLIGHT_PREFIX)).toEqual(new Set())
  })

  it('merges a moved branch by default, as an eager worktree does', async () => {
    const { repo } = await seeded()
    const a = await repo.worktree('main', { lazy: true })
    const b = await repo.worktree('main', { lazy: true })
    a.set('page/1', 'from a')
    b.set('page/2', 'from b')
    await a.commit()
    const merged = await b.commit()
    expect(merged.strategy).toBe('three_way')
    expect(await b.getMany(['page/1', 'page/2', 'page/3'])).toEqual(
      new Map<string, unknown>([
        ['page/1', 'from a'],
        ['page/2', 'from b'],
        ['page/3', 3],
      ]),
    )
  })

  it('resets to a checkpoint and back', async () => {
    const { repo, base } = await seeded()
    const wt = await repo.worktree('main', { lazy: true })
    await repo.tags.create('cp', base)
    wt.set('page/5', 'changed')
    await wt.delete('page/6')
    const later = (await wt.commit()).commit as string
    await wt.reset(await repo.tags.get('cp'))
    expect(wt.head).toBe(base)
    expect(await wt.get('page/5')).toBe(5)
    expect(await wt.get('page/6')).toBe(6)
    await wt.reset(later)
    expect(await wt.get('page/5')).toBe('changed')
    expect(await wt.has('page/6')).toBe(false)
  })

  it('copies a branch for a test without touching the original', async () => {
    const { repo, base } = await seeded()
    await repo.branches.create('db/test', base)
    const copy = await repo.worktree('db/test', { lazy: true })
    copy.set('page/1', 'test only')
    await copy.commit({ onMoved: 'raise' })
    const main = await repo.worktree('main', { lazy: true })
    expect(await main.get('page/1')).toBe(1)
    expect(await copy.get('page/1')).toBe('test only')
    // The copy and main share the repository's node cache.
    expect(copy.nodeCache).toBe(main.nodeCache)
    await repo.branches.delete('db/test')
  })

  it('serializes commits on one handle', async () => {
    const { repo } = await seeded()
    const wt = await repo.worktree('main', { lazy: true })
    wt.set('page/1', 'one')
    const p1 = wt.commit({ onMoved: 'raise' })
    const p2 = wt.commit({ onMoved: 'raise' })
    await Promise.all([p1, p2])
    expect(await wt.get('page/1')).toBe('one')
    expect(wt.head).toBe(await repo.branches.get('main'))
  })
})

describe('lazy rollback', () => {
  it('a commit batch that fails to land leaves the handle where it was', async () => {
    const store = new FailingMemory()
    const writer = await VersionedKV.open(store)
    await writer.commit({ updates: new Map([['keep', bytes('k0')]]) })
    const vk = await VersionedKV.open(store, { lazy: true })
    const before = { commit: vk.currentCommit, base: vk.baseCommit }
    store.arm()
    await expect(
      vk.commit({ updates: new Map([['keep', bytes('k1')]]), removals: new Set(['x']) }),
    ).rejects.toThrow('landing write failed')
    expect({ commit: vk.currentCommit, base: vk.baseCommit }).toEqual(before)
    expect(text(await vk.get('keep'))).toBe('k0')
    // And it commits cleanly after.
    await vk.commit({ updates: new Map([['keep', bytes('k2')]]) })
    expect(text(await (await VersionedKV.open(store)).get('keep'))).toBe('k2')
  })

  it('a merge that loses its publish race leaves the handle where it was', async () => {
    const store = new Memory()
    const writer = await VersionedKV.open(store)
    await writer.commit({ updates: new Map([['keep', bytes('k0')]]) })
    const vk = await VersionedKV.open(store, { lazy: true })
    await writer.commit({ updates: new Map([['other', bytes('o0')]]) })
    const before = { commit: vk.currentCommit, base: vk.baseCommit }
    let n = 0
    racePublish(
      store,
      'main',
      () => writer.commit({ updates: new Map([[`race${n++}`, bytes('r')]]) }),
      null,
    )
    await expect(vk.commit({ updates: new Map([['mine', bytes('m')]]) })).rejects.toThrow(
      ConcurrencyError,
    )
    expect({ commit: vk.currentCommit, base: vk.baseCommit }).toEqual(before)
    expect(await vk.get('mine')).toBeNull()
    expect(await vk.get('other')).toBeNull()
  })
})
