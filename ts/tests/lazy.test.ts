/**
 * Worktrees read their keyset from the HAMT on demand: opening reads
 * the head and its root alone, and reads walk the tree through the
 * repository's content cache. These pin that a worktree over a large
 * keyset keeps the commit protocol's guarantees: raise on a moved
 * branch, rollback of a failed attempt, reset and branch copies.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  BranchMovedError,
  ConcurrencyError,
  type KVStore,
  Repo,
  type Worktree,
  jsonCodec,
} from '../src/index'
import { IN_FLIGHT_KEY, IN_FLIGHT_PREFIX } from '../src/versioned/layout'
import { racePublish } from './races'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array | undefined): string | undefined =>
  b === undefined ? undefined : dec.decode(b)

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

/** A worktree on `main`, created if the store has none. */
async function open(store: KVStore, opts: { cacheBytes?: number } = {}): Promise<Worktree> {
  const repo = await Repo.open(store, opts)
  return repo.worktree('main', { create: true })
}

/** Write `updates` and commit them. */
async function commitAll(wt: Worktree, updates: Iterable<[string, Uint8Array]>) {
  for (const [k, v] of updates) wt.set(k, v)
  return wt.commit()
}

describe('opening a worktree', () => {
  it('reads the head and its root, and no keyset node', async () => {
    // No content cache, so nothing the writer read or wrote is answered
    // from memory.
    const store = new CountingMemory()
    const writer = await open(store, { cacheBytes: 0 })
    const updates = new Map<string, Uint8Array>()
    for (let i = 0; i < 500; i++) updates.set(`page/${i}`, bytes(`v${i}`))
    await commitAll(writer, updates)

    store.reads = []
    await open(store, { cacheBytes: 0 })
    expect(store.reads.filter((k) => k.startsWith('kvgit:'))).toEqual([])
  })

  // A 3,000-key commit: seconds on a loaded runner.
  it('reads the tree a commit wrote from the content cache', async () => {
    const store = new CountingMemory()
    const repo = await Repo.open(store)
    const writer = await repo.worktree('main', { create: true })
    for (let i = 0; i < 3000; i++) writer.set(`page/${i}`, bytes(`v${i}`))
    await writer.commit()
    const reader = await repo.worktree('main')
    store.reads = []
    expect(text((await reader.get('page/42')) as Uint8Array)).toBe('v42')
    expect(store.reads.filter((k) => k.startsWith('kvgit:keyset:'))).toEqual([])
  }, 30_000)
})

describe('a worktree over a large keyset', () => {
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
    // Another worktree on the same branch sees the same thing.
    const other = await repo.worktree('main')
    expect(await other.keys()).toEqual(await again.keys())
  })

  it('raises on a moved branch, writes nothing, keeps its changes', async () => {
    const { repo, store, base } = await seeded()
    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
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
    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
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

  it('merges a moved branch by default', async () => {
    const { repo } = await seeded()
    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
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
    const wt = await repo.worktree('main')
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
    const copy = await repo.worktree('db/test')
    copy.set('page/1', 'test only')
    await copy.commit({ onMoved: 'raise' })
    const main = await repo.worktree('main')
    expect(await main.get('page/1')).toBe(1)
    expect(await copy.get('page/1')).toBe('test only')
    await repo.branches.delete('db/test')
  })

  it('serializes commits on one handle', async () => {
    const { repo } = await seeded()
    const wt = await repo.worktree('main')
    wt.set('page/1', 'one')
    const p1 = wt.commit({ onMoved: 'raise' })
    const p2 = wt.commit({ onMoved: 'raise' })
    await Promise.all([p1, p2])
    expect(await wt.get('page/1')).toBe('one')
    expect(wt.head).toBe(await repo.branches.get('main'))
  })
})

describe('rollback', () => {
  it('a commit batch that fails to land leaves the worktree where it was', async () => {
    const store = new FailingMemory()
    const writer = await open(store)
    await commitAll(writer, [['keep', bytes('k0')]])
    const wt = await open(store)
    const before = wt.head
    store.arm()
    wt.set('keep', bytes('k1'))
    await wt.delete('x')
    await expect(wt.commit()).rejects.toThrow('landing write failed')
    expect(wt.head).toBe(before)
    wt.discard()
    expect(text(await wt.get('keep'))).toBe('k0')
    // And it commits cleanly after.
    await commitAll(wt, [['keep', bytes('k2')]])
    expect(text(await (await open(store)).get('keep'))).toBe('k2')
  })

  it('a merge that loses its publish race leaves the worktree where it was', async () => {
    const store = new Memory()
    const writer = await open(store)
    await commitAll(writer, [['keep', bytes('k0')]])
    const wt = await open(store)
    await commitAll(writer, [['other', bytes('o0')]])
    const before = wt.head
    let n = 0
    racePublish(store, 'main', () => commitAll(writer, [[`race${n++}`, bytes('r')]]), null)
    await expect(commitAll(wt, [['mine', bytes('m')]])).rejects.toThrow(ConcurrencyError)
    expect(wt.head).toBe(before)
    wt.discard()
    expect(await wt.get('mine')).toBeUndefined()
    expect(await wt.get('other')).toBeUndefined()
  })
})
