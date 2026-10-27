/**
 * The values a worktree remembers: least recently used first out,
 * bounded in bytes, never a value over a quarter of the budget, and
 * emptied when the head moves.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { DEFAULT_VALUE_CACHE_BYTES, Repo, ValueCache } from '../src/index'

const bytes = (n: number, fill = 1): Uint8Array => new Uint8Array(n).fill(fill)

describe('ValueCache', () => {
  it('holds values up to its budget, letting the least recently used go', () => {
    // Each costs its bytes, its key and a fixed overhead: about 700 of
    // the 3000, so four fit and a fifth pushes one out.
    const cache = new ValueCache<string>(3000)
    for (const k of ['a', 'b', 'c', 'd']) cache.remember(k, k.toUpperCase(), 600)
    expect(cache.size).toBe(4)
    expect(cache.get('a')).toBe('A') // a is now the most recent
    cache.remember('e', 'E', 600)
    expect(cache.has('b')).toBe(false)
    expect(['a', 'c', 'd', 'e'].every((k) => cache.has(k))).toBe(true)
    expect(cache.sizeBytes).toBeLessThanOrEqual(3000)
  })

  it('never holds a value over a quarter of its budget', () => {
    const cache = new ValueCache<string>(4000)
    expect(cache.maxValueBytes).toBe(1000)
    cache.remember('big', 'X', 1000)
    expect(cache.has('big')).toBe(false)
    expect(cache.size).toBe(0)
  })

  it('holds nothing with a budget of 0', () => {
    const cache = new ValueCache<string>(0)
    cache.remember('k', 'v', 1)
    expect(cache.size).toBe(0)
  })

  it('counts a value again when it is remembered again', () => {
    const cache = new ValueCache<string>(4000)
    cache.remember('k', 'v', 100)
    const once = cache.sizeBytes
    cache.remember('k', 'w', 100)
    expect(cache.sizeBytes).toBe(once)
    expect(cache.get('k')).toBe('w')
  })

  it('refuses a budget that is not a whole number of bytes', () => {
    expect(() => new ValueCache(-1)).toThrow(RangeError)
    expect(() => new ValueCache(1.5)).toThrow(RangeError)
  })
})

describe('a worktree remembers what it reads', () => {
  async function seeded(opts: { valueCacheBytes?: number } = {}) {
    const store = new Memory()
    const repo = await Repo.open(store, opts)
    const wt = await repo.worktree('main', { create: true })
    wt.set('a', bytes(10, 1))
    wt.set('b', bytes(10, 2))
    await wt.commit()
    return { repo, wt }
  }

  it('by the repository budget, 32 MB unless given', async () => {
    const { wt } = await seeded()
    expect(wt.cache.maxBytes).toBe(DEFAULT_VALUE_CACHE_BYTES)
    await wt.get('a')
    await wt.getMany(['b'])
    expect(wt.cache.has('a')).toBe(true)
    expect(wt.cache.has('b')).toBe(true)
  })

  it('until its head moves', async () => {
    const { wt } = await seeded()
    await wt.get('a')
    wt.set('c', bytes(1))
    await wt.commit()
    expect(wt.cache.size).toBe(0)
  })

  it('never pending values', async () => {
    const { wt } = await seeded()
    wt.set('a', bytes(3, 9))
    expect(await wt.get('a')).toEqual(bytes(3, 9))
    expect(wt.cache.has('a')).toBe(false)
  })

  it('and nothing with a budget of 0', async () => {
    const { wt } = await seeded({ valueCacheBytes: 0 })
    expect(await wt.get('a')).toEqual(bytes(10, 1))
    expect(wt.cache.size).toBe(0)
  })

  it('refuses a negative budget', async () => {
    await expect(Repo.open(new Memory(), { valueCacheBytes: -1 })).rejects.toThrow(RangeError)
  })
})
