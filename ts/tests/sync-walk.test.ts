import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { UnknownCommitError } from '../src/errors'
import { VersionedKV, type WireCommit, applyWire, walkDelta } from '../src/index'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array): string => dec.decode(b)

async function collect(store: Memory, want: string, have?: string[]): Promise<WireCommit[]> {
  const out: WireCommit[] = []
  for await (const wc of walkDelta(store, { want, ...(have !== undefined && { have }) })) {
    out.push(wc)
  }
  return out
}

/**
 * Replay a wire stream into a fresh store, which rebuilds each keyset
 * and recomputes each commit hash from scratch. Proves WireCommit
 * carries everything hash identity depends on.
 */
async function verifyHashFidelity(wire: WireCommit[]): Promise<void> {
  const result = await applyWire(new Memory(), wire)
  expect(result.applied).toBe(wire.length)
}

describe('walkDelta — linear history', () => {
  it('yields the full history in parents-first order with correct deltas', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const initial = vk.currentCommit

    await vk.commit({ updates: new Map([['a', bytes('1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({
      updates: new Map([
        ['a', bytes('2')],
        ['b', bytes('x')],
      ]),
    })
    const c2 = vk.currentCommit
    await vk.commit({ removals: new Set(['b']), info: { note: 'drop b' } })
    const c3 = vk.currentCommit

    const wire = await collect(store, c3)
    expect(wire.map((w) => w.hash)).toEqual([initial, c1, c2, c3])

    const [w0, w1, w2, w3] = wire as [WireCommit, WireCommit, WireCommit, WireCommit]
    expect(w0.parents).toEqual([])
    expect(w0.updates.size).toBe(0)

    expect(w1.parents).toEqual([initial])
    expect([...w1.updates.keys()]).toEqual(['a'])
    expect(text(w1.updates.get('a') as Uint8Array)).toBe('1')

    expect([...w2.updates.keys()].sort()).toEqual(['a', 'b'])
    expect(text(w2.updates.get('a') as Uint8Array)).toBe('2')

    expect(w3.updates.size).toBe(0)
    expect([...w3.removals]).toEqual(['b'])
    expect(w3.info).toEqual({ note: 'drop b' })
    expect(w3.time.value).toBeGreaterThan(0)
    expect(w3.parentGens).toEqual([2])

    await verifyHashFidelity(wire)
  })

  it('respects `have`: only commits beyond the frontier are emitted', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['a', bytes('1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({ updates: new Map([['b', bytes('2')]]) })
    const c2 = vk.currentCommit

    const wire = await collect(store, c2, [c1])
    expect(wire.map((w) => w.hash)).toEqual([c2])
    expect([...(wire[0] as WireCommit).updates.keys()]).toEqual(['b'])
  })

  it('emits nothing when the receiver already has want (or beyond)', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['a', bytes('1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({ updates: new Map([['b', bytes('2')]]) })
    const c2 = vk.currentCommit

    expect(await collect(store, c2, [c2])).toEqual([])
    expect(await collect(store, c1, [c2])).toEqual([])
  })

  it('tolerates unknown hashes in have (conservative re-send)', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['a', bytes('1')]]) })
    const head = vk.currentCommit

    const wire = await collect(store, head, ['f'.repeat(40)])
    expect(wire.map((w) => w.hash)).toContain(head)
  })

  it('throws when want is missing from the store', async () => {
    const store = new Memory()
    await VersionedKV.open(store)
    await expect(collect(store, 'f'.repeat(40))).rejects.toThrow(UnknownCommitError)
  })
})

describe('walkDelta — merges', () => {
  /** Two writers on one branch with disjoint keys: writer B's commit
   *  lands as a three-way merge whose keyset takes B's key from B's
   *  side. */
  async function divergeAndMerge(store: Memory): Promise<{
    head: string
    oursCommit: string
    theirsCommit: string
  }> {
    const vkA = await VersionedKV.open(store)
    const vkB = await VersionedKV.open(store) // same base commit

    await vkA.commit({ updates: new Map([['ka', bytes('from-a')]]) })
    const theirsCommit = vkA.currentCommit

    // B commits against the stale base → HEAD moved → three-way merge.
    const r = await vkB.commit({ updates: new Map([['kb', bytes('from-b')]]) })
    expect(r.strategy).toBe('three_way')
    return {
      head: vkB.currentCommit,
      oursCommit: (await vkB.parents(vkB.currentCommit))[1] as string,
      theirsCommit,
    }
  }

  it('sends a key the merge took from its second parent as an update', async () => {
    const store = new Memory()
    const { head, oursCommit, theirsCommit } = await divergeAndMerge(store)

    const wire = await collect(store, head)
    const merge = wire.find((w) => w.hash === head) as WireCommit

    // parents[0] is "theirs" (won the race); kb differs from it, so it
    // travels with its bytes. ka matches parents[0] exactly, so it
    // doesn't travel at all.
    expect(merge.parents).toEqual([theirsCommit, oursCommit])
    expect([...merge.updates.keys()]).toEqual(['kb'])
    expect(text(merge.updates.get('kb') as Uint8Array)).toBe('from-b')
    expect(merge.parentGens).toEqual([1, 1])

    await verifyHashFidelity(wire)
  })

  it('classifies contested keys resolved by a merge fn as updates', async () => {
    const store = new Memory()
    const vkA = await VersionedKV.open(store)
    const vkB = await VersionedKV.open(store)

    await vkA.commit({ updates: new Map([['k', bytes('a')]]) })
    const r = await vkB.commit({
      updates: new Map([['k', bytes('b')]]),
      defaultMerge: (_old, ours, theirs) =>
        bytes(`${text(theirs as Uint8Array)}+${text(ours as Uint8Array)}`),
    })
    expect(r.strategy).toBe('three_way')
    expect(r.autoMergedKeys).toEqual(['k'])
    const head = vkB.currentCommit

    const wire = await collect(store, head)
    const merge = wire.find((w) => w.hash === head) as WireCommit
    // Merged values are written at the merge commit itself.
    expect(text(merge.updates.get('k') as Uint8Array)).toBe('a+b')

    await verifyHashFidelity(wire)
  })

  it('walks both lineages of a merge when have only covers one side', async () => {
    const store = new Memory()
    const { head, oursCommit, theirsCommit } = await divergeAndMerge(store)

    // Receiver has theirs (parents[0]) but not ours: the delta must
    // include the ours-side commit and the merge.
    const wire = await collect(store, head, [theirsCommit])
    expect(wire.map((w) => w.hash)).toEqual([oursCommit, head])
  })

  it('reproduces every hash across a fork-heavy DAG (fidelity sweep)', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['base', bytes('v')]]) })

    // Branch, advance both sides, merge back, keep committing.
    const feature = (await vk.createBranch('feature')) as VersionedKV
    await feature.commit({ updates: new Map([['f1', bytes('feature')]]) })
    await vk.commit({ updates: new Map([['m1', bytes('main')]]) })

    // Cross-branch merge: replay feature's key onto main via a stale
    // second writer on main.
    const stale = await VersionedKV.open(store, {
      branch: 'main',
      commitHash: vk.currentCommit,
    })
    await vk.commit({ updates: new Map([['m2', bytes('more')]]) })
    await stale.commit({
      updates: new Map([
        ['f1', bytes('feature')],
        ['s1', bytes('stale')],
      ]),
    })

    const wire = await collect(store, stale.currentCommit)
    await verifyHashFidelity(wire)

    // Determinism: a second walk yields identical order and content.
    const again = await collect(store, stale.currentCommit)
    expect(again.map((w) => w.hash)).toEqual(wire.map((w) => w.hash))
  })
})
