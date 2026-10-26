/**
 * A commit that fails leaves the handle as it found it, on every failure
 * path; an iteration keeps the keys it started on across a commit; and
 * state-changing calls on one handle are serialized.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { ConcurrencyError, MergeConflict, VersionedKV } from '../src/index'
import { COMMIT_ROOT, IN_FLIGHT_KEY } from '../src/versioned/layout'
import { failReads, racePublish } from './races'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array | null): string | null => (b === null ? null : dec.decode(b))

/** Everything a failed attempt must leave as it was: the commit, the
 *  base, and every key's value. */
async function stateOf(vk: VersionedKV): Promise<unknown> {
  return { commit: vk.currentCommit, base: vk.baseCommit, values: await valuesOf(vk) }
}

async function valuesOf(vk: VersionedKV): Promise<Record<string, string | null>> {
  const out: Record<string, string | null> = {}
  for await (const k of vk.keys()) out[k] = text(await vk.get(k))
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
function raceOnce(loser: VersionedKV, advance: () => Promise<unknown>): void {
  racePublish(loser.store, loser.currentBranch, advance)
}

/** A branch with keys to update, keep and remove, and a second handle
 *  on it. */
async function setup(store = new FailingMemory()) {
  const writer = await VersionedKV.open(store)
  await writer.commit({
    updates: new Map([
      ['keep', bytes('k0')],
      ['change', bytes('c0')],
      ['drop', bytes('d0')],
    ]),
  })
  const vk = await VersionedKV.open(store)
  return { store, writer, vk }
}

const change = () => ({
  updates: new Map([
    ['change', bytes('c1')],
    ['add', bytes('a1')],
  ]),
  removals: new Set(['drop']),
})

describe('a failed commit attempt restores the key maps exactly', () => {
  it('when the fast-forward batch fails to land', async () => {
    const { store, vk } = await setup()
    const before = await stateOf(vk)
    store.arm(1)
    await expect(vk.commit(change())).rejects.toThrow('landing write failed')
    expect(await stateOf(vk)).toEqual(before)
  })

  it('when the merge batch after a lost race fails to land', async () => {
    const { store, writer, vk } = await setup()
    const before = await stateOf(vk)
    raceOnce(vk, async () => {
      await writer.commit({ updates: new Map([['other', bytes('o1')]]) })
      store.arm(1)
    })
    await expect(vk.commit(change())).rejects.toThrow('landing write failed')
    expect(await stateOf(vk)).toEqual(before)
  })

  it('when the merge publish loses a second race', async () => {
    const { store, writer, vk } = await setup()
    const before = await stateOf(vk)
    let n = 0
    racePublish(
      store,
      'main',
      () => writer.commit({ updates: new Map([[`other${n++}`, bytes('o')]]) }),
      null,
    )
    await expect(vk.commit(change())).rejects.toThrow(ConcurrencyError)
    expect(await stateOf(vk)).toEqual(before)
  })

  it('when the merge after a lost race conflicts', async () => {
    const { writer, vk } = await setup()
    const before = await stateOf(vk)
    raceOnce(vk, () => writer.commit({ updates: new Map([['change', bytes('theirs')]]) }))
    await expect(vk.commit(change())).rejects.toThrow(MergeConflict)
    expect(await stateOf(vk)).toEqual(before)
  })

  it('when reading for the merge after a lost race fails', async () => {
    const { store, writer, vk } = await setup()
    const before = await stateOf(vk)
    raceOnce(vk, async () => {
      await writer.commit({ updates: new Map([['other', bytes('o1')]]) })
      // The merge's first read of history: the two heads' parents.
      failReads(
        store,
        (keys) => keys.some((k) => k.startsWith('__parent_commit__')),
        new Error('store read failed'),
      )
    })
    await expect(vk.commit(change())).rejects.toThrow('store read failed')
    expect(await stateOf(vk)).toEqual(before)
  })

  it('when a merge of another head fails to land', async () => {
    const { store, vk } = await setup()
    const side = (await vk.createBranch('side')) as VersionedKV
    await side.commit({ updates: new Map([['side', bytes('s1')]]) })
    await vk.commit({ updates: new Map([['main', bytes('m1')]]) })
    const before = await stateOf(vk)
    store.arm(1)
    await expect(vk.mergeHeads(side.currentCommit)).rejects.toThrow('landing write failed')
    expect(await stateOf(vk)).toEqual(before)
  })

  it('when an apply fails to land', async () => {
    const { store, vk } = await setup()
    const parent = vk.currentCommit
    await vk.commit({ updates: new Map([['picked', bytes('p1')]]) })
    const target = vk.currentCommit
    await vk.commit({ removals: new Set(['picked']) })
    const before = await stateOf(vk)
    store.arm(1)
    await expect(vk.applyChange(parent, target)).rejects.toThrow('landing write failed')
    expect(await stateOf(vk)).toEqual(before)
  })
})

describe('a retry after a failed attempt', () => {
  it('lands the change on the moved HEAD', async () => {
    const { store, writer, vk } = await setup()
    raceOnce(vk, async () => {
      await writer.commit({ updates: new Map([['other', bytes('o1')]]) })
      store.arm(1)
    })
    await expect(vk.commit(change())).rejects.toThrow('landing write failed')

    const result = await vk.commit(change())
    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('three_way')
    expect(vk.currentCommit).toBe(await vk.latestHead())
    const fresh = await VersionedKV.open(store)
    expect(await stateOf(vk)).toEqual(await stateOf(fresh))
    expect(await valuesOf(vk)).toEqual({
      keep: 'k0',
      change: 'c1',
      add: 'a1',
      other: 'o1',
    })
  })

  it('after losing a race, keeps the merged state in step with HEAD', async () => {
    const { store, writer, vk } = await setup()
    raceOnce(vk, () => writer.commit({ updates: new Map([['other', bytes('o1')]]) }))
    const result = await vk.commit(change())
    expect(result.strategy).toBe('three_way')
    const fresh = await VersionedKV.open(store)
    expect(await stateOf(vk)).toEqual(await stateOf(fresh))
  })
})

describe('an iteration over the keys', () => {
  it('an iteration keeps the keys it started with across a commit', async () => {
    const { vk } = await setup()
    const it = vk.keys()[Symbol.asyncIterator]()
    const seen = [(await it.next()).value as string]
    await vk.commit(change())
    for (let r = await it.next(); !r.done; r = await it.next()) seen.push(r.value)
    expect(seen.sort()).toEqual(['change', 'drop', 'keep'])

    const after: string[] = []
    for await (const k of vk.keys()) after.push(k)
    expect(after.sort()).toEqual(['add', 'change', 'keep'])
  })

  it('an iteration keeps its keys across a rollback', async () => {
    const { store, vk } = await setup()
    const it = vk.keys()[Symbol.asyncIterator]()
    const seen = [(await it.next()).value as string]
    // The first batch lands; the merge after the lost race fails.
    raceOnce(vk, async () => {
      const other = await VersionedKV.open(store)
      await other.commit({ updates: new Map([['other', bytes('o1')]]) })
      store.arm(1)
    })
    await expect(vk.commit(change())).rejects.toThrow('landing write failed')
    for (let r = await it.next(); !r.done; r = await it.next()) seen.push(r.value)
    expect(seen.sort()).toEqual(['change', 'drop', 'keep'])
  })
})

describe('state-changing calls on one handle are serialized', () => {
  it('concurrent commits all land, each on the last', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const results = await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        vk.commit({ updates: new Map([[`k${i}`, bytes(String(i))]]) }),
      ),
    )
    expect(results.map((r) => r.strategy)).toEqual(Array(5).fill('fast_forward'))
    expect(vk.currentCommit).toBe(await vk.latestHead())
    const fresh = await VersionedKV.open(store)
    expect(await stateOf(vk)).toEqual(await stateOf(fresh))
    expect(Object.keys(await valuesOf(vk)).sort()).toEqual(['k0', 'k1', 'k2', 'k3', 'k4'])
  })

  it('a refresh waits for a commit in flight', async () => {
    const { store, writer, vk } = await setup()
    await writer.commit({ updates: new Map([['other', bytes('o1')]]) })
    await Promise.all([vk.commit(change()), vk.refresh()])
    const fresh = await VersionedKV.open(store)
    expect(await stateOf(vk)).toEqual(await stateOf(fresh))
  })

  it('a failed commit does not block the next one', async () => {
    const { store, vk } = await setup()
    store.arm(1)
    const [failed, next] = await Promise.allSettled([
      vk.commit(change()),
      vk.commit({ updates: new Map([['later', bytes('l1')]]) }),
    ])
    expect(failed.status).toBe('rejected')
    expect(next.status).toBe('fulfilled')
    expect(await valuesOf(vk)).toEqual({
      keep: 'k0',
      change: 'c0',
      drop: 'd0',
      later: 'l1',
    })
  })
})

/** A memory store that, after each write lands, notes in each of the
 *  turns that follow the commit a handle is at and the root its keys
 *  are read from, both in the one turn. */
class Probing extends Memory {
  vk: VersionedKV | null = null
  readonly seen: Array<{ commit: string; root: string }> = []

  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const landed = await super.casMany(expected, writes, removes)
    const vk = this.vk
    if (vk !== null) {
      const state = (vk as unknown as { state: { root(): string } }).state
      let turn = Promise.resolve()
      for (let i = 0; i < 30; i++) {
        turn = turn.then(() => {
          this.seen.push({ commit: vk.currentCommit, root: state.root() })
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
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['old', bytes('o')]]) })
    store.vk = vk
    await vk.commit({ updates: new Map([['new', bytes('n')]]) })
    expect(await store.consistent()).toBe(true)
  })

  it('finds a reset move its commit and keys together', async () => {
    const store = new Probing()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['old', bytes('o')]]) })
    const before = vk.currentCommit
    await vk.commit({ updates: new Map([['new', bytes('n')]]) })
    store.vk = vk
    await vk.resetTo(before)
    expect(await store.consistent()).toBe(true)
  })
})
