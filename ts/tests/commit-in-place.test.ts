/**
 * A commit, merge or apply that fails leaves the worktree as it found
 * it, on every failure path: its head, its pending changes and every
 * value it reads. A listing of the keys keeps what it returned across a
 * commit, and state-changing calls on one worktree are serialized.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { core } from '../src/core'
import {
  ConcurrencyError,
  MergeConflict,
  type MergeResult,
  Repo,
  type Worktree,
} from '../src/index'
import { COMMIT_ROOT, IN_FLIGHT_KEY } from '../src/versioned/layout'
import { failReads, racePublish } from './races'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array | undefined): string | null => (b === undefined ? null : dec.decode(b))

type Wt = Worktree<Uint8Array>

/** A worktree on `main` in its own repository over `store`. */
async function openMain(store: Memory): Promise<Wt> {
  return (await Repo.open(store)).worktree('main', { create: true })
}

/** Write `updates`, delete `removals`, and commit. */
async function commit(
  wt: Wt,
  updates: Record<string, string>,
  removals: string[] = [],
): Promise<MergeResult> {
  for (const [k, v] of Object.entries(updates)) wt.set(k, bytes(v))
  for (const k of removals) await wt.delete(k)
  return wt.commit()
}

/** Everything a failed attempt must leave as it was: the head, the
 *  pending changes, and every key's value. */
async function stateOf(wt: Wt): Promise<unknown> {
  return { commit: wt.head, pending: wt.status(), values: await valuesOf(wt) }
}

async function valuesOf(wt: Wt): Promise<Record<string, string | null>> {
  const out: Record<string, string | null> = {}
  for (const k of await wt.keys()) out[k] = text(await wt.get(k))
  return out
}

/** A memory store whose Nth commit batch (counting from 1 after
 *  `arm`) fails to land. */
class FailingMemory extends Memory {
  private batchesLeft: number | null = null
  arm(nth: number): void {
    this.batchesLeft = nth
  }
  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const isBatch = [...writes.keys()].some((k) => k.startsWith(IN_FLIGHT_KEY('')))
    if (isBatch && this.batchesLeft !== null) {
      this.batchesLeft--
      if (this.batchesLeft === 0) {
        this.batchesLeft = null
        throw new Error('landing write failed')
      }
    }
    return super.casMany(expected, writes, removes)
  }
}

/** Make `loser`'s next HEAD swap lose: run `advance` first. */
function raceOnce(loser: Wt, advance: () => Promise<unknown>): void {
  racePublish(loser.repo.store, loser.branch, advance)
}

/** A branch with keys to update, keep and remove, and a second
 *  worktree on it. */
async function setup(store = new FailingMemory()) {
  const writer = await openMain(store)
  await commit(writer, { keep: 'k0', change: 'c0', drop: 'd0' })
  const wt = await openMain(store)
  return { store, writer, wt }
}

/** Stage the change: update one key, add one, delete one. */
async function stage(wt: Wt): Promise<void> {
  wt.set('change', bytes('c1'))
  wt.set('add', bytes('a1'))
  await wt.delete('drop')
}

/** Stage the change and commit it. */
async function change(wt: Wt): Promise<MergeResult> {
  await stage(wt)
  return wt.commit()
}

describe('a failed commit attempt restores the key maps exactly', () => {
  it('when the fast-forward batch fails to land', async () => {
    const { store, wt } = await setup()
    await stage(wt)
    const before = await stateOf(wt)
    store.arm(1)
    await expect(wt.commit()).rejects.toThrow('landing write failed')
    expect(await stateOf(wt)).toEqual(before)
  })

  it('when the merge batch after a lost race fails to land', async () => {
    const { store, writer, wt } = await setup()
    await stage(wt)
    const before = await stateOf(wt)
    raceOnce(wt, async () => {
      await commit(writer, { other: 'o1' })
      store.arm(1)
    })
    await expect(wt.commit()).rejects.toThrow('landing write failed')
    expect(await stateOf(wt)).toEqual(before)
  })

  it('when the merge publish loses a second race', async () => {
    const { store, writer, wt } = await setup()
    await stage(wt)
    const before = await stateOf(wt)
    let n = 0
    racePublish(store, 'main', () => commit(writer, { [`other${n++}`]: 'o' }), null)
    await expect(wt.commit()).rejects.toThrow(ConcurrencyError)
    expect(await stateOf(wt)).toEqual(before)
  })

  it('when the merge after a lost race conflicts', async () => {
    const { writer, wt } = await setup()
    await stage(wt)
    const before = await stateOf(wt)
    raceOnce(wt, () => commit(writer, { change: 'theirs' }))
    await expect(wt.commit()).rejects.toThrow(MergeConflict)
    expect(await stateOf(wt)).toEqual(before)
  })

  it('when reading for the merge after a lost race fails', async () => {
    const { store, writer, wt } = await setup()
    await stage(wt)
    const before = await stateOf(wt)
    raceOnce(wt, async () => {
      await commit(writer, { other: 'o1' })
      // The merge's first read of the moved branch: its tip's root.
      const tip = COMMIT_ROOT(writer.head)
      failReads(store, (keys) => keys.includes(tip), new Error('store read failed'))
    })
    await expect(wt.commit()).rejects.toThrow('store read failed')
    expect(await stateOf(wt)).toEqual(before)
  })

  it('when a merge of another head fails to land', async () => {
    const { store, wt } = await setup()
    await wt.repo.branches.create('side', wt.head)
    const side = await wt.repo.worktree('side')
    await commit(side, { side: 's1' })
    await commit(wt, { main: 'm1' })
    const before = await stateOf(wt)
    store.arm(1)
    await expect(wt.merge({ commit: side.head })).rejects.toThrow('landing write failed')
    expect(await stateOf(wt)).toEqual(before)
  })

  it('when an apply fails to land', async () => {
    const { store, wt } = await setup()
    const parent = wt.head
    await commit(wt, { picked: 'p1' })
    const target = wt.head
    await commit(wt, {}, ['picked'])
    const before = await stateOf(wt)
    store.arm(1)
    await expect(wt.apply(parent, target)).rejects.toThrow('landing write failed')
    expect(await stateOf(wt)).toEqual(before)
  })
})

describe('a retry after a failed attempt', () => {
  it('lands the change on the moved HEAD', async () => {
    const { store, writer, wt } = await setup()
    raceOnce(wt, async () => {
      await commit(writer, { other: 'o1' })
      store.arm(1)
    })
    await expect(change(wt)).rejects.toThrow('landing write failed')

    const result = await change(wt)
    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('three_way')
    expect(wt.head).toBe(await wt.repo.branches.get('main'))
    const fresh = await openMain(store)
    expect(await stateOf(wt)).toEqual(await stateOf(fresh))
    expect(await valuesOf(wt)).toEqual({
      keep: 'k0',
      change: 'c1',
      add: 'a1',
      other: 'o1',
    })
  })

  it('after losing a race, keeps the merged state in step with HEAD', async () => {
    const { store, writer, wt } = await setup()
    raceOnce(wt, () => commit(writer, { other: 'o1' }))
    const result = await change(wt)
    expect(result.strategy).toBe('three_way')
    const fresh = await openMain(store)
    expect(await stateOf(wt)).toEqual(await stateOf(fresh))
  })
})

describe('a listing of the keys', () => {
  it('keeps the keys it returned across a commit', async () => {
    const { wt } = await setup()
    const listed = await wt.keys()
    const seen = [listed[0] as string]
    await change(wt)
    seen.push(...listed.slice(1))
    expect(seen.sort()).toEqual(['change', 'drop', 'keep'])

    expect(await wt.keys()).toEqual(['add', 'change', 'keep'])
  })

  it('keeps its keys across a rollback', async () => {
    const { store, wt } = await setup()
    const listed = await wt.keys()
    const seen = [listed[0] as string]
    // The first batch lands; the merge after the lost race fails.
    raceOnce(wt, async () => {
      await commit(await openMain(store), { other: 'o1' })
      store.arm(1)
    })
    await expect(change(wt)).rejects.toThrow('landing write failed')
    seen.push(...listed.slice(1))
    expect(seen.sort()).toEqual(['change', 'drop', 'keep'])
  })
})

describe('state-changing calls on one worktree are serialized', () => {
  it('concurrent commits all land, each on the last', async () => {
    const store = new Memory()
    const wt = await openMain(store)
    // Each commit takes its own key: the pending changes a commit
    // takes are read when it is called.
    const results = await Promise.all(
      Array.from({ length: 5 }, (_, i) => {
        wt.set(`k${i}`, bytes(String(i)))
        return wt.commit({ keys: [`k${i}`] })
      }),
    )
    expect(results.map((r) => r.strategy)).toEqual(Array(5).fill('fast_forward'))
    expect(wt.head).toBe(await wt.repo.branches.get('main'))
    const fresh = await openMain(store)
    expect(await stateOf(wt)).toEqual(await stateOf(fresh))
    expect(Object.keys(await valuesOf(wt)).sort()).toEqual(['k0', 'k1', 'k2', 'k3', 'k4'])
  })

  it('a refresh waits for a commit in flight', async () => {
    const { store, writer, wt } = await setup()
    await commit(writer, { other: 'o1' })
    await stage(wt)
    await Promise.all([wt.commit(), wt.refresh()])
    const fresh = await openMain(store)
    expect(await stateOf(wt)).toEqual(await stateOf(fresh))
  })

  it('a failed commit does not block the next one', async () => {
    const { store, wt } = await setup()
    await stage(wt)
    store.arm(1)
    const [failed, next] = await Promise.allSettled([
      wt.commit({ keys: ['change', 'add', 'drop'] }),
      (() => {
        wt.set('later', bytes('l1'))
        return wt.commit({ keys: ['later'] })
      })(),
    ])
    expect(failed.status).toBe('rejected')
    expect(next.status).toBe('fulfilled')
    // The failed change stays pending; what the branch holds is the
    // next commit alone.
    wt.discard()
    expect(await valuesOf(wt)).toEqual({
      keep: 'k0',
      change: 'c0',
      drop: 'd0',
      later: 'l1',
    })
  })
})

/** The keyset state a worktree reads through: the first one whose
 *  root is read while `during` runs. */
async function treeOf(during: () => Promise<unknown>): Promise<{ root(): string }> {
  const proto = (await core()).WorktreeState.prototype
  const real = proto.root
  let found: { root(): string } | null = null
  proto.root = function (this: { root(): string }) {
    found ??= this
    return real.call(this as never)
  }
  try {
    await during()
  } finally {
    proto.root = real
  }
  if (found === null) throw new Error('no keyset state was read')
  return found
}

/** A memory store that, after each write lands, notes in each of the
 *  turns that follow the commit a worktree is at and the root its keys
 *  are read from, both in the one turn. */
class Probing extends Memory {
  probe: { wt: Wt; state: { root(): string } } | null = null
  readonly seen: Array<{ commit: string; root: string }> = []

  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const landed = await super.casMany(expected, writes, removes)
    const probe = this.probe
    if (probe !== null) {
      let turn = Promise.resolve()
      for (let i = 0; i < 30; i++) {
        turn = turn.then(() => {
          this.seen.push({ commit: probe.wt.head, root: probe.state.root() })
        })
      }
    }
    return landed
  }

  /** Whether each turn noted read the keys of the commit it was at. */
  async consistent(): Promise<boolean> {
    await new Promise((resolve) => setTimeout(resolve, 0))
    for (const { commit, root } of this.seen) {
      const stored = await this.get(COMMIT_ROOT(commit))
      if (stored === null || JSON.parse(dec.decode(stored)) !== root) return false
    }
    return this.seen.length > 0
  }
}

describe('a read beside a state-changing call', () => {
  it('finds a commit and its keys moved together', async () => {
    const store = new Probing()
    const wt = await openMain(store)
    const state = await treeOf(() => commit(wt, { old: 'o' }))
    store.probe = { wt, state }
    await commit(wt, { new: 'n' })
    expect(await store.consistent()).toBe(true)
  })

  it('finds a reset move its commit and keys together', async () => {
    const store = new Probing()
    const wt = await openMain(store)
    const state = await treeOf(() => commit(wt, { old: 'o' }))
    const before = wt.head
    await commit(wt, { new: 'n' })
    store.probe = { wt, state }
    await wt.reset(before)
    expect(await store.consistent()).toBe(true)
  })
})
