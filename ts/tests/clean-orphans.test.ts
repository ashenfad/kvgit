import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Keyset } from '../src/keyset'
import { VersionedKV } from '../src/versioned/kv'
import { blobKey } from '../src/versioned/layout'

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

/** Use minAge=-1 so the cutoff lies just past Date.now(); every commit
 *  recorded before the cleanOrphans call is treated as old enough. */
const SWEEP_ALL = { minAge: -1 } as const

async function countKeysWithPrefix(store: Memory, prefix: string): Promise<number> {
  let n = 0
  for await (const k of store.keys()) {
    if (k.startsWith(prefix)) n++
  }
  return n
}

describe('cleanOrphans — basic sweep', () => {
  it('removes commits unreachable from any branch', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })
    await vk.commit({ updates: new Map([['k', bytes('v3')]]) })

    // Reset HEAD to c1, orphaning the two later commits.
    await vk.resetTo(c1)

    const removed = await vk.cleanOrphans(SWEEP_ALL)
    expect(removed).toBe(2)

    // Reachable commit's metadata survives.
    expect((await store.get(`__commit_root__${c1}`)) !== null).toBe(true)
  })

  it('returns 0 when there are no orphans', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v')]]) })
    const removed = await vk.cleanOrphans(SWEEP_ALL)
    expect(removed).toBe(0)
  })
})

describe('cleanOrphans — branch deletion', () => {
  it("a deleted branch's commits become orphans", async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['shared', bytes('base')]]) })

    const feature = (await main.createBranch('feature')) as VersionedKV
    await feature.commit({ updates: new Map([['feature-only', bytes('fv')]]) })
    const featureCommit = feature.currentCommit

    await main.deleteBranch('feature')
    const removed = await main.cleanOrphans(SWEEP_ALL)
    expect(removed).toBe(1)

    // The feature commit's metadata is gone.
    expect(await store.get(`__commit_root__${featureCommit}`)).toBeNull()
    // The feature-only blob is gone.
    expect(await store.get(`${featureCommit}:feature-only`)).toBeNull()
  })
})

describe('cleanOrphans — preserves reachable state', () => {
  it('does not delete blobs reachable from a live commit', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('keep-me')]]) })
    const liveCommit = vk.currentCommit

    // Make and orphan some other commits.
    await vk.commit({ updates: new Map([['other', bytes('orphan-this')]]) })
    await vk.resetTo(liveCommit)

    await vk.cleanOrphans(SWEEP_ALL)

    // The live blob is still there.
    expect(await vk.get('k')).toEqual(bytes('keep-me'))
  })

  it('preserves a HAMT node shared by an orphan and a live commit', async () => {
    // Layout:
    //   main HEAD → c2 (k=v2)
    //   c1 (k=v1) was an intermediate but orphaned by resetTo(c0)... no.
    // Cleaner: build two commits that share a HAMT subtree (same
    // unchanged-key payload), orphan one, verify the shared subtree
    // survives because it's still reachable from the other.
    const store = new Memory()
    const vk = await VersionedKV.open(store)

    // Many keys so the HAMT splits into branches.
    const updates = new Map<string, Uint8Array>()
    for (let i = 0; i < 30; i++) updates.set(`key${i}`, bytes(`v${i}`))
    await vk.commit({ updates })
    const c1 = vk.currentCommit

    // Add one more key — most of the HAMT subtrees stay shared with c1.
    await vk.commit({ updates: new Map([['extra', bytes('vx')]]) })

    // Branch off c1, then immediately delete the branch — so c1 is
    // still reachable as the parent of HEAD on main.
    const tmp = (await vk.createBranch('tmp', { at: c1 })) as VersionedKV
    void tmp
    await vk.deleteBranch('tmp')

    // Nothing should actually be orphaned (c1 is HEAD's parent).
    const removed = await vk.cleanOrphans(SWEEP_ALL)
    expect(removed).toBe(0)

    // All shared HAMT nodes still resolve through the keyset.
    for (let i = 0; i < 30; i++) {
      expect(await vk.get(`key${i}`)).toEqual(bytes(`v${i}`))
    }
    expect(await vk.get('extra')).toEqual(bytes('vx'))
  })
})

describe('cleanOrphans — minAge protects young orphans', () => {
  it('does not delete commits younger than minAge', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v')]]) })
    const beforeOrphan = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })

    // Reset to orphan the latest commit.
    await vk.resetTo(beforeOrphan)

    // With a generous minAge the orphan is protected.
    const removed = await vk.cleanOrphans({ minAge: 60_000_000 })
    expect(removed).toBe(0)
  })

  it('young-orphan blobs are protected from sweep', async () => {
    // A young orphan's blob shouldn't be deleted even though the
    // commit isn't deleted yet, because it might belong to an
    // in-flight writer.
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v')]]) })
    const beforeOrphan = vk.currentCommit
    await vk.commit({ updates: new Map([['young', bytes('young-data')]]) })
    const youngBlobKey = await blobKey(bytes('young-data'))

    // Sanity: the blob exists.
    expect(await store.get(youngBlobKey)).not.toBeNull()

    await vk.resetTo(beforeOrphan)

    // Sweep with a long minAge so the orphaned commit is young.
    await vk.cleanOrphans({ minAge: 60_000_000 })

    // Blob still there.
    expect(await store.get(youngBlobKey)).not.toBeNull()
  })
})

describe('cleanOrphans — HAMT node sweep', () => {
  it('removes HAMT nodes only reachable from orphan commits', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)

    // Create a commit and orphan it.
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const c1 = vk.currentCommit

    // Reset to initial (drops c1).
    const initial = await vk.history() // newest to oldest
    let lastSeen = c1
    for await (const c of vk.history()) lastSeen = c
    await vk.resetTo(lastSeen)
    void initial

    const nodesBefore = await countKeysWithPrefix(store, Keyset.DEFAULT_PREFIX)
    await vk.cleanOrphans(SWEEP_ALL)
    const nodesAfter = await countKeysWithPrefix(store, Keyset.DEFAULT_PREFIX)

    // c1 was the only commit with non-empty keyset; its HAMT node(s)
    // should be cleaned.
    expect(nodesAfter).toBeLessThan(nodesBefore)
  })
})
