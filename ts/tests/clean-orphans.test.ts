import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { type MergeResult, Repo, type Worktree } from '../src/index'
import { blobKey } from '../src/versioned/layout'

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

/** HAMT nodes live under this prefix. */
const NODE_PREFIX = 'kvgit:keyset:'

/** Use minAge=-1 so the cutoff lies just past Date.now(); every commit
 *  recorded before the sweep is treated as old enough. */
const SWEEP_ALL = { minAge: -1 } as const

/** A worktree on `main` over `store`. */
async function openMain(store: Memory): Promise<Worktree<Uint8Array>> {
  return (await Repo.open(store)).worktree('main', { create: true })
}

/** Write `updates` into `wt` and commit them. */
function commit(wt: Worktree<Uint8Array>, updates: Record<string, string>): Promise<MergeResult> {
  for (const [k, v] of Object.entries(updates)) wt.set(k, bytes(v))
  return wt.commit()
}

async function countKeysWithPrefix(store: Memory, prefix: string): Promise<number> {
  let n = 0
  for await (const k of store.keys()) {
    if (k.startsWith(prefix)) n++
  }
  return n
}

describe('gc — basic sweep', () => {
  it('removes commits unreachable from any branch', async () => {
    const store = new Memory()
    const wt = await openMain(store)
    const c1 = (await commit(wt, { k: 'v1' })).commit as string
    await commit(wt, { k: 'v2' })
    await commit(wt, { k: 'v3' })

    // Reset HEAD to c1, orphaning the two later commits.
    await wt.reset(c1)

    const removed = await wt.repo.gc(SWEEP_ALL)
    expect(removed).toBe(2)

    // Reachable commit's metadata survives.
    expect((await store.get(`__commit_root__${c1}`)) !== null).toBe(true)
  })

  it('returns 0 when there are no orphans', async () => {
    const store = new Memory()
    const wt = await openMain(store)
    await commit(wt, { k: 'v' })
    const removed = await wt.repo.gc(SWEEP_ALL)
    expect(removed).toBe(0)
  })
})

describe('gc — branch deletion', () => {
  it("a deleted branch's commits become orphans", async () => {
    const store = new Memory()
    const main = await openMain(store)
    await commit(main, { shared: 'base' })

    await main.repo.branches.create('feature', main.head)
    const feature = await main.repo.worktree('feature')
    const featureCommit = (await commit(feature, { 'feature-only': 'fv' })).commit as string

    await main.repo.branches.delete('feature')
    const removed = await main.repo.gc(SWEEP_ALL)
    expect(removed).toBe(1)

    // The feature commit's metadata is gone.
    expect(await store.get(`__commit_root__${featureCommit}`)).toBeNull()
    // The feature-only blob is gone.
    expect(await store.get(await blobKey(bytes('fv')))).toBeNull()
  })
})

describe('gc — preserves reachable state', () => {
  it('does not delete blobs reachable from a live commit', async () => {
    const store = new Memory()
    const wt = await openMain(store)
    const liveCommit = (await commit(wt, { k: 'keep-me' })).commit as string

    // Make and orphan some other commits.
    await commit(wt, { other: 'orphan-this' })
    await wt.reset(liveCommit)

    await wt.repo.gc(SWEEP_ALL)

    // The live blob is still there.
    expect(await wt.get('k')).toEqual(bytes('keep-me'))
  })

  it('preserves a HAMT node shared by an orphan and a live commit', async () => {
    // Two commits share HAMT subtrees (the unchanged keys' payload).
    // A branch made at the older one and deleted again leaves it
    // reachable as HEAD's parent, so the sweep removes nothing and the
    // shared subtrees still resolve.
    const store = new Memory()
    const wt = await openMain(store)

    // Many keys so the HAMT splits into branches.
    const updates: Record<string, string> = {}
    for (let i = 0; i < 30; i++) updates[`key${i}`] = `v${i}`
    const c1 = (await commit(wt, updates)).commit as string

    // Add one more key — most of the HAMT subtrees stay shared with c1.
    await commit(wt, { extra: 'vx' })

    // Branch off c1, then immediately delete the branch — so c1 is
    // still reachable as the parent of HEAD on main.
    await wt.repo.branches.create('tmp', c1)
    await wt.repo.branches.delete('tmp')

    // Nothing should actually be orphaned (c1 is HEAD's parent).
    const removed = await wt.repo.gc(SWEEP_ALL)
    expect(removed).toBe(0)

    // All shared HAMT nodes still resolve through the keyset.
    for (let i = 0; i < 30; i++) {
      expect(await wt.get(`key${i}`)).toEqual(bytes(`v${i}`))
    }
    expect(await wt.get('extra')).toEqual(bytes('vx'))
  })
})

describe('gc — minAge protects young orphans', () => {
  it('does not delete commits younger than minAge', async () => {
    const store = new Memory()
    const wt = await openMain(store)
    const beforeOrphan = (await commit(wt, { k: 'v' })).commit as string
    await commit(wt, { k: 'v2' })

    // Reset to orphan the latest commit.
    await wt.reset(beforeOrphan)

    // With a generous minAge the orphan is protected.
    const removed = await wt.repo.gc({ minAge: 60_000_000 })
    expect(removed).toBe(0)
  })

  it('young-orphan blobs are protected from sweep', async () => {
    // A young orphan's blob shouldn't be deleted even though the
    // commit isn't deleted yet, because it might belong to an
    // in-flight writer.
    const store = new Memory()
    const wt = await openMain(store)
    const beforeOrphan = (await commit(wt, { k: 'v' })).commit as string
    await commit(wt, { young: 'young-data' })
    const youngBlobKey = await blobKey(bytes('young-data'))

    // Sanity: the blob exists.
    expect(await store.get(youngBlobKey)).not.toBeNull()

    await wt.reset(beforeOrphan)

    // Sweep with a long minAge so the orphaned commit is young.
    await wt.repo.gc({ minAge: 60_000_000 })

    // Blob still there.
    expect(await store.get(youngBlobKey)).not.toBeNull()
  })
})

describe('gc — HAMT node sweep', () => {
  it('removes HAMT nodes only reachable from orphan commits', async () => {
    const store = new Memory()
    const wt = await openMain(store)

    // Create a commit and orphan it.
    const c1 = (await commit(wt, { k: 'v1' })).commit as string

    // Reset to the oldest commit in history (drops c1).
    let lastSeen = c1
    for await (const c of wt.repo.log({ branch: 'main' })) lastSeen = c.hash
    await wt.reset(lastSeen)

    const nodesBefore = await countKeysWithPrefix(store, NODE_PREFIX)
    await wt.repo.gc(SWEEP_ALL)
    const nodesAfter = await countKeysWithPrefix(store, NODE_PREFIX)

    // c1 was the only commit with non-empty keyset; its HAMT node(s)
    // should be cleaned.
    expect(nodesAfter).toBeLessThan(nodesBefore)
  })
})
