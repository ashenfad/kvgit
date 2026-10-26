/**
 * The key index: the first listing builds it with one walk of the tree;
 * later listings read it; a commit carries it with its own changes, and
 * a move to a root the worktree didn't write follows by a diff.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Repo } from '../src/index'

/** A memory store that records the keys read from it. */
class Counting extends Memory {
  reads: string[] = []

  override async get(key: string): Promise<Uint8Array | null> {
    this.reads.push(key)
    return super.get(key)
  }

  override async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const wanted = [...keys]
    this.reads.push(...wanted)
    return super.getMany(wanted)
  }

  nodeReads(): string[] {
    return this.reads.filter((k) => k.startsWith('kvgit:keyset:'))
  }
}

const enc = new TextEncoder()

/** A repository whose cache is off, so every node read reaches the
 *  store, and a branch of 300 keys. */
async function seeded() {
  const store = new Counting()
  const repo = await Repo.open(store, { cacheBytes: 0 })
  const wt = await repo.worktree('main', { create: true })
  for (let i = 0; i < 300; i++) wt.set(`k/${String(i).padStart(3, '0')}`, enc.encode(`${i}`))
  await wt.commit()
  return { store, repo }
}

describe('the key index', () => {
  it('is built by the first listing and read by the next', async () => {
    const { store, repo } = await seeded()
    const wt = await repo.worktree('main')
    store.reads = []
    expect((await wt.keys()).length).toBe(300)
    expect(store.nodeReads().length).toBeGreaterThan(0)
    store.reads = []
    expect((await wt.keys()).length).toBe(300)
    expect(store.nodeReads()).toEqual([])
  })

  it('follows a commit by its own changes', async () => {
    const { store, repo } = await seeded()
    const wt = await repo.worktree('main')
    await wt.keys()
    wt.set('k/new', enc.encode('n'))
    await wt.delete('k/000')
    await wt.commit()
    store.reads = []
    const keys = await wt.keys()
    expect(store.nodeReads()).toEqual([])
    expect(keys).toContain('k/new')
    expect(keys).not.toContain('k/000')
    expect(keys.length).toBe(300)
  })

  it('follows a refresh to what another worktree committed', async () => {
    const { repo } = await seeded()
    const wt = await repo.worktree('main')
    await wt.keys()
    const other = await repo.worktree('main')
    other.set('k/theirs', enc.encode('t'))
    await other.delete('k/001')
    await other.commit()
    await wt.refresh()
    const keys = new Set(await wt.keys())
    expect(keys.has('k/theirs')).toBe(true)
    expect(keys.has('k/001')).toBe(false)
    expect(keys.size).toBe(300)
  })

  it('starts again after a refresh from a commit that was swept', async () => {
    // Its old tree can't be read, so its keys can't be diffed out.
    const store = new Memory()
    const repo = await Repo.open(store, { cacheBytes: 0 })
    const wt = await repo.worktree('main', { create: true })
    wt.set('kept', enc.encode('1'))
    await wt.commit()
    const base = wt.head
    wt.set('swept', enc.encode('2'))
    await wt.commit()
    const ours = await (await Repo.open(store, { cacheBytes: 0 })).worktree('main')
    expect((await ours.keys()).sort()).toEqual(['kept', 'swept'])
    await (await repo.worktree('main')).reset(base)
    await repo.gc({ minAge: 0 })
    await ours.refresh()
    expect(await ours.keys()).toEqual(['kept'])
    expect(await ours.has('swept')).toBe(false)
  })

  it('answers that a key is absent without reading the tree', async () => {
    const { store, repo } = await seeded()
    const wt = await repo.worktree('main')
    await wt.keys()
    store.reads = []
    expect(await wt.has('k/nothing')).toBe(false)
    expect(await wt.get('k/nothing')).toBeUndefined()
    expect(store.nodeReads()).toEqual([])
  })
})
