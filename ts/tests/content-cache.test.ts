/**
 * The content cache: what the store never rewrites (tree nodes, commit
 * records) is answered from memory, and a sweep by any writer empties
 * it before anything builds on what the sweep removed.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Repo } from '../src/index'
import { cacheOf } from '../src/versioned/drive'

/** A memory store that counts the keys read from it. */
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
}

const enc = new TextEncoder()

async function oneCommit(store: Memory) {
  const repo = await Repo.open(store)
  const wt = await repo.worktree('main', { create: true })
  wt.set('k', enc.encode('v'))
  const { commit } = await wt.commit({ info: { n: 1 } })
  return { repo, commit: commit as string }
}

describe('the content cache', () => {
  it('answers a commit record read twice from memory', async () => {
    const store = new Counting()
    const { repo, commit } = await oneCommit(store)
    await repo.getCommit(commit)
    store.reads = []
    await repo.getCommit(commit)
    const records = store.reads.filter((k) => !k.startsWith('__commit_root__'))
    expect(records, 'records were read from the store again').toEqual([])
    expect(store.reads).toContain(`__commit_root__${commit}`)
  })

  it('is off with a budget of 0', async () => {
    const store = new Memory()
    await Repo.open(store, { cacheBytes: 0 })
    expect(cacheOf(store)).toBeUndefined()
  })

  it("is emptied by another writer's sweep before the next read", async () => {
    // Two stores over one backend stand for two processes, each with its
    // own cache.
    const backend = new Counting()
    const { repo, commit } = await oneCommit(backend)
    await repo.getCommit(commit)
    const cache = cacheOf(backend)
    expect(cache).toBeDefined()
    const before = JSON.parse((cache as NonNullable<typeof cache>).stats()) as { entries: number }
    expect(before.entries).toBeGreaterThan(0)

    const other = await Repo.open(new View(backend))
    await other.gc({ minAge: 0 })
    // The next read of the lease, which any commit makes, finds the
    // sweep's record and empties the cache.
    const wt = await repo.worktree('main')
    wt.set('k', enc.encode('after the sweep'))
    await wt.commit()
    const after = JSON.parse((cache as NonNullable<typeof cache>).stats()) as { clears: number }
    expect(after.clears).toBe(1)
  })
})

/** Another handle on a store: the same keys, a different object. */
class View extends Memory {
  constructor(private readonly backend: Memory) {
    super()
  }
  override get = (key: string) => this.backend.get(key)
  override getMany = (keys: Iterable<string>) => this.backend.getMany(keys)
  override set = (key: string, value: Uint8Array) => this.backend.set(key, value)
  override remove = (key: string) => this.backend.remove(key)
  override removeMany = (keys: Iterable<string>) => this.backend.removeMany(keys)
  override cas = (key: string, value: Uint8Array, expected: Uint8Array | null) =>
    this.backend.cas(key, value, expected)
  override casMany = (
    expected: Map<string, Uint8Array | null>,
    writes: Map<string, Uint8Array>,
    removes?: Iterable<string>,
  ) => this.backend.casMany(expected, writes, removes)
  override keys = (prefix?: string) => this.backend.keys(prefix)
}
