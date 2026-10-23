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
import { BRANCH_HEAD, IN_FLIGHT_PREFIX } from '../src/versioned/layout'

/** A Memory that runs a one-shot hook just before the next `casMany`
 *  that writes `key`: the window between a commit's read of HEAD and
 *  the compare-and-swap that publishes it. */
class RaceMemory extends Memory {
  #hooks = new Map<string, () => Promise<void>>()

  beforeWriting(key: string, hook: () => Promise<void>): void {
    this.#hooks.set(key, hook)
  }

  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    for (const key of writes.keys()) {
      const hook = this.#hooks.get(key)
      if (hook !== undefined) {
        this.#hooks.delete(key)
        await hook()
      }
    }
    return super.casMany(expected, writes, removes)
  }
}

async function storeKeys(store: KVStore, prefix?: string): Promise<Set<string>> {
  const out = new Set<string>()
  for await (const k of store.keys(prefix)) out.add(k)
  return out
}

/** Two worktrees on `main`, both based on a seed commit. */
async function twoWriters(store: Memory = new Memory()) {
  let clock = 1_760_000_000
  const repo = await Repo.open<unknown>(store, { codec: jsonCodec, now: () => clock++ })
  const seed = await repo.worktree('main', { create: true })
  seed.set('seed', 0)
  const base = (await seed.commit()).commit as string
  const a = await repo.worktree('main')
  const b = await repo.worktree('main')
  a.set('a', 'from a')
  b.set('b', 'from b')
  return { repo, store, base, a, b }
}

async function expectMoved(wt: Worktree<unknown>, base: string, head: string): Promise<void> {
  const err = await wt.commit({ onMoved: 'raise' }).then(
    () => null,
    (e: unknown) => e,
  )
  expect(err).toBeInstanceOf(BranchMovedError)
  expect(err).toBeInstanceOf(ConcurrencyError)
  const moved = err as BranchMovedError
  expect(moved.branch).toBe('main')
  expect(moved.base).toBe(base)
  expect(moved.head).toBe(head)
}

describe("Worktree.commit — onMoved: 'raise'", () => {
  it('refuses to merge disjoint keys onto a moved branch', async () => {
    const { repo, base, a, b } = await twoWriters()
    const first = (await a.commit()).commit as string

    await expectMoved(b, base, first)

    // The branch holds only the first commit.
    expect(await repo.branches.get('main')).toBe(first)
    expect((await repo.getCommit(first)).parents).toEqual([base])
    expect(await (await repo.snapshot({ branch: 'main' })).keys()).toEqual(['a', 'seed'])
  })

  it('keeps the pending changes and the head after the refusal', async () => {
    const { base, a, b } = await twoWriters()
    await a.commit()
    await b.delete('seed')

    await expect(b.commit({ onMoved: 'raise' })).rejects.toThrow(BranchMovedError)

    expect(b.head).toBe(base)
    expect(b.status().updated).toEqual(new Set(['b']))
    expect(b.status().removed).toEqual(new Set(['seed']))
    expect(await b.get('b')).toBe('from b')
    expect(await b.has('seed')).toBe(false)
  })

  it('refuses whatever onConflict says', async () => {
    const { base, a, b } = await twoWriters()
    const first = (await a.commit()).commit as string
    await expect(b.commit({ onMoved: 'raise', onConflict: 'abandon' })).rejects.toThrow(
      BranchMovedError,
    )
    await expectMoved(b, base, first)
  })

  it('still merges by default, and after a refresh commits cleanly', async () => {
    const { repo, a, b } = await twoWriters()
    const first = (await a.commit()).commit as string

    const merged = await b.commit()
    expect(merged.strategy).toBe('three_way')
    expect((await repo.getCommit(merged.commit as string)).parents).toEqual([
      first,
      expect.any(String),
    ])

    // Retry pattern: refresh to the tip, redo the work, commit.
    const c = await repo.worktree('main')
    a.set('a', 'again')
    const tip = (await a.commit()).commit as string
    c.set('c', 1)
    await expect(c.commit({ onMoved: 'raise' })).rejects.toThrow(BranchMovedError)
    await c.refresh()
    expect(c.head).toBe(tip)
    c.set('c', 1)
    const landed = await c.commit({ onMoved: 'raise' })
    expect(landed.strategy).toBe('fast_forward')
    expect((await repo.getCommit(landed.commit as string)).parents).toEqual([tip])
  })

  it('refuses a commit with nothing to write when the branch moved', async () => {
    const { repo, a, b } = await twoWriters()
    // a clean worktree on the head the branch is about to leave
    const clean = await repo.worktree('main')
    const first = (await a.commit()).commit as string
    const err = await clean.commit({ onMoved: 'raise' }).catch((e) => e)
    expect(err).toBeInstanceOf(BranchMovedError)
    expect(err.head).toBe(first)
    // pending changes, none of them selected
    await expect(b.commit({ keys: [], onMoved: 'raise' })).rejects.toThrow(BranchMovedError)
    expect(b.status().updated).toEqual(new Set(['b']))
    // by default, and on an unmoved branch, it is still a no-op
    expect((await clean.commit()).strategy).toBe('no_op')
    await clean.refresh()
    expect((await clean.commit({ onMoved: 'raise' })).strategy).toBe('no_op')
  })

  it('fast-forwards when the branch has not moved', async () => {
    const { repo, base, a } = await twoWriters()
    const result = await a.commit({ onMoved: 'raise' })
    expect(result.strategy).toBe('fast_forward')
    expect((await repo.getCommit(result.commit as string)).parents).toEqual([base])
  })

  it('writes nothing when the branch had already moved', async () => {
    const { store, a, b } = await twoWriters()
    await a.commit()
    const before = await storeKeys(store)
    await expect(b.commit({ onMoved: 'raise' })).rejects.toThrow(BranchMovedError)
    expect(await storeKeys(store)).toEqual(before)
  })

  it('refuses a move that lands inside the publishing CAS window', async () => {
    const store = new RaceMemory()
    const { repo, base, a, b } = await twoWriters(store)
    let first = ''
    store.beforeWriting(BRANCH_HEAD('main'), async () => {
      first = (await a.commit()).commit as string
    })

    const err = await b.commit({ onMoved: 'raise' }).then(
      () => null,
      (e: unknown) => e,
    )
    expect(err).toBeInstanceOf(BranchMovedError)
    expect(first).not.toBe('')
    expect((err as BranchMovedError).base).toBe(base)
    expect((err as BranchMovedError).head).toBe(first)
    expect(await repo.branches.get('main')).toBe(first)
    expect((await repo.getCommit(first)).parents).toEqual([base])
    expect(b.head).toBe(base)
    expect(b.status().updated).toEqual(new Set(['b']))
  })

  it('leaves nothing reachable behind after losing the CAS', async () => {
    const store = new RaceMemory()
    const { repo, a, b } = await twoWriters(store)
    let first = ''
    store.beforeWriting(BRANCH_HEAD('main'), async () => {
      first = (await a.commit()).commit as string
    })
    await expect(b.commit({ onMoved: 'raise' })).rejects.toThrow(BranchMovedError)
    expect(await repo.branches.get('main')).toBe(first)

    // The attempt's in-flight marker is withdrawn (asynchronously).
    await new Promise((r) => setTimeout(r, 0))
    expect(await storeKeys(store, IN_FLIGHT_PREFIX)).toEqual(new Set())

    // Its commit is an ordinary orphan: the only one a sweep takes, and
    // with it the blob only that commit held.
    const fromB = jsonCodec.encode('from b')
    const holdsFromB = async () => {
      for (const k of await storeKeys(store)) {
        const v = await store.get(k)
        if (v !== null && v.length === fromB.length && v.every((x, i) => x === fromB[i])) {
          return true
        }
      }
      return false
    }
    expect(await holdsFromB()).toBe(true)
    expect(await repo.gc({ minAge: 0 })).toBe(1)
    expect(await holdsFromB()).toBe(false)
    expect(await (await repo.snapshot({ branch: 'main' })).keys()).toEqual(['a', 'seed'])
  })

  it('rejects an unknown disposition', async () => {
    const { a } = await twoWriters()
    await expect(a.commit({ onMoved: 'fail' as 'raise' })).rejects.toThrow(TypeError)
    expect(a.status().updated).toEqual(new Set(['a']))
  })
})
