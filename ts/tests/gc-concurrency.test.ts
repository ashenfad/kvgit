/**
 * The orphan sweep, and how it behaves while another writer commits.
 *
 * Blobs and HAMT nodes are keyed by content, so equal content is
 * stored once and shared by every commit that holds it, whatever
 * history it came from (see "content addressing" below, which measures
 * it). The sweep finds its deletion candidates by walking orphan
 * commits' keysets and keeping whatever a live commit still reaches.
 *
 * It runs under the store's GC lease, so a commit that starts while it
 * runs waits for it and lands afterwards, intact, even holding content
 * an orphan held: the sweep can't delete what the commit then writes.
 * The lease mechanics themselves are tested in gc-lease.test.ts.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Keyset } from '../src/keyset'
import { decodeEntry } from '../src/keyset'
import type { KeysetEntry } from '../src/types'
import { VersionedKV } from '../src/versioned/kv'
import { BLOB_PREFIX, COMMIT_ROOT, blobKey, loads } from '../src/versioned/layout'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

/** minAge=-1 puts the cutoff just past `Date.now()`, so every commit
 *  written before the call counts as old enough to sweep. */
const SWEEP_ALL = { minAge: -1 } as const

// ---------------------------------------------------------------------------
// Test seam
// ---------------------------------------------------------------------------

/**
 * A `Memory` that can be interrupted at a chosen point, deterministically.
 *
 * Every `await` inside the sweep is a yield point another task can land
 * a commit in — `store.keys(prefix)` is an async iterable, so even a
 * single-threaded JS context interleaves there. Rather than hope for
 * the interleaving, register a one-shot hook: `beforeScan` fires before
 * the first `keys(prefix)` scan of a given prefix, `beforeGet` before
 * the first `get(key)` of a given key. No sleeps, no timing luck.
 */
class HookedMemory extends Memory {
  readonly beforeScan = new Map<string, () => Promise<void>>()
  readonly beforeGet = new Map<string, () => Promise<void>>()

  override async *keys(prefix?: string): AsyncIterable<string> {
    if (prefix !== undefined) await this.fire(this.beforeScan, prefix)
    yield* super.keys(prefix)
  }

  override async get(key: string): Promise<Uint8Array | null> {
    await this.fire(this.beforeGet, key)
    return super.get(key)
  }

  private async fire(hooks: Map<string, () => Promise<void>>, key: string): Promise<void> {
    const hook = hooks.get(key)
    if (hook === undefined) return
    hooks.delete(key)
    await hook()
  }
}

/** Land a commit on `main` from an independent handle. */
async function landCommit(
  store: Memory,
  key: string,
  value: string,
): Promise<{ commit: string; root: string; blob: string }> {
  const writer = await VersionedKV.open(store)
  await writer.commit({ updates: new Map([[key, bytes(value)]]) })
  const commit = writer.currentCommit
  return { commit, root: await rootOf(store, commit), blob: await blobKey(bytes(value)) }
}

async function nodeKeys(store: Memory): Promise<Set<string>> {
  const out = new Set<string>()
  for await (const k of store.keys(Keyset.DEFAULT_PREFIX)) out.add(k)
  return out
}

async function blobKeys(store: Memory): Promise<Set<string>> {
  const out = new Set<string>()
  for await (const k of store.keys(BLOB_PREFIX)) out.add(k)
  return out
}

async function rootOf(store: Memory, commit: string): Promise<string> {
  const raw = await store.get(COMMIT_ROOT(commit))
  if (raw === null) throw new Error(`commit ${commit} has no root`)
  return loads(raw) as string
}

/** Every entry reachable from `root`, plus the node hashes visited. */
async function readTree(
  store: Memory,
  root: string,
): Promise<{ entries: Map<string, KeysetEntry>; nodes: Set<string>; missing: Set<string> }> {
  const entries = new Map<string, KeysetEntry>()
  const nodes = new Set<string>()
  const missing = new Set<string>()
  const queue = [root]
  while (queue.length > 0) {
    const h = queue.pop() as string
    if (nodes.has(h) || missing.has(h)) continue
    const raw = await store.get(Keyset.DEFAULT_PREFIX + h)
    if (raw === null) {
      missing.add(h)
      continue
    }
    nodes.add(h)
    const node = JSON.parse(dec.decode(raw)) as
      | { kind: 'leaf'; items: Record<string, string> }
      | { kind: 'branch'; children: Record<string, string> }
    if (node.kind === 'leaf') {
      for (const [k, b64] of Object.entries(node.items)) {
        const binary = atob(b64)
        const buf = new Uint8Array(binary.length)
        for (let i = 0; i < binary.length; i++) buf[i] = binary.charCodeAt(i)
        entries.set(k, decodeEntry(buf))
      }
    } else {
      queue.push(...Object.values(node.children))
    }
  }
  return { entries, nodes, missing }
}

// ---------------------------------------------------------------------------
// Content addressing
// ---------------------------------------------------------------------------

describe('content addressing', () => {
  it('two unrelated histories with identical final content hold the same nodes and blobs', async () => {
    // Same logical end state ({x: 1, y: 2}), reached by different
    // commit sequences in different stores.
    const storeA = new Memory()
    const a = await VersionedKV.open(storeA)
    await a.commit({ updates: new Map([['x', bytes('1')]]) })
    await a.commit({ updates: new Map([['y', bytes('2')]]) })

    const storeB = new Memory()
    const b = await VersionedKV.open(storeB)
    await b.commit({ updates: new Map([['y', bytes('2')]]) })
    await b.commit({ updates: new Map([['x', bytes('1')]]) })

    const rootA = await rootOf(storeA, a.currentCommit)
    expect(await rootOf(storeB, b.currentCommit)).toBe(rootA)
    expect(await blobKeys(storeA)).toEqual(await blobKeys(storeB))
    expect((await readTree(storeA, rootA)).nodes).toEqual((await readTree(storeB, rootA)).nodes)
  })

  it('stores a value written under two keys once', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({
      updates: new Map([
        ['a', bytes('same')],
        ['b', bytes('same')],
      ]),
    })
    const { entries } = await readTree(store, await rootOf(store, vk.currentCommit))
    expect(entries.get('a')?.blob).toBe(await blobKey(bytes('same')))
    expect(entries.get('b')?.blob).toBe(entries.get('a')?.blob)
    expect((await blobKeys(store)).size).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// The race
// ---------------------------------------------------------------------------

describe('cleanOrphans — a commit starting mid-sweep', () => {
  /**
   * The seam is the first `get` of the orphan's `__commit_root__`, the
   * first read of the orphan-collection loop: the mark phase is done
   * and the sweep is collecting what to delete. A commit started here
   * waits for the sweep's lease and lands after it.
   */
  async function sweepWithLateCommit(lateValue: string, orphanValue: string) {
    const store = new HookedMemory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const keep = vk.currentCommit
    await vk.commit({ updates: new Map([['garbage', bytes(orphanValue)]]) })
    const orphan = vk.currentCommit
    await vk.resetTo(keep)

    let late: Promise<{ commit: string; root: string; blob: string }> | null = null
    let landedDuringSweep = false
    store.beforeGet.set(COMMIT_ROOT(orphan), async () => {
      late = landCommit(store, 'late', lateValue).then((r) => {
        landedDuringSweep = !sweepDone
        return r
      })
    })
    let sweepDone = false
    const removed = await vk.cleanOrphans(SWEEP_ALL)
    sweepDone = true
    expect(late, 'seam never fired').not.toBeNull()
    const landed = await (late as unknown as Promise<{
      commit: string
      root: string
      blob: string
    }>)
    return { store, orphan, removed, landed, landedDuringSweep }
  }

  it('waits for the sweep, then lands with its nodes and blobs intact', async () => {
    const { store, orphan, removed, landed, landedDuringSweep } = await sweepWithLateCommit(
      'landed',
      'g',
    )
    expect(landedDuringSweep, 'the commit landed while the sweep held the lease').toBe(false)
    expect(removed).toBe(1)
    const headRaw = await store.get('__branch_head__main')
    expect(headRaw === null ? null : (loads(headRaw) as string)).toBe(landed.commit)

    const { missing, entries } = await readTree(store, landed.root)
    expect([...missing]).toEqual([])
    expect([...entries.keys()].sort()).toEqual(['late', 'seed'])
    expect(await store.get(landed.blob)).not.toBeNull()

    // The orphan was still collected.
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await store.get(await blobKey(bytes('g')))).toBeNull()
  })

  it('keeps content the late commit shares with the orphan it swept', async () => {
    // The sweep deletes the orphan's blob, then the waiting commit lands
    // and writes it again: the live head never names a missing blob.
    const { store, landed } = await sweepWithLateCommit('shared', 'shared')
    expect(landed.blob).toBe(await blobKey(bytes('shared')))
    expect(await store.get(landed.blob)).not.toBeNull()
    const reopened = await VersionedKV.open(store)
    expect(await reopened.get('late')).toEqual(bytes('shared'))
  })

  it('control: a sweep that finds no orphans deletes nothing', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const before = await nodeKeys(store)

    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(0)
    expect(await nodeKeys(store)).toEqual(before)
  })
})

// ---------------------------------------------------------------------------
// The sweep still sweeps
// ---------------------------------------------------------------------------

describe('cleanOrphans — still collects ordinary garbage', () => {
  it("removes an orphan's commit metadata, blobs and exclusively-owned nodes", async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const seeds = new Map<string, Uint8Array>()
    for (let i = 0; i < 40; i++) seeds.set(`key${i}`, bytes(`v${i}`))
    await vk.commit({ updates: seeds })
    const keep = vk.currentCommit
    const keepNodes = (await readTree(store, await rootOf(store, keep))).nodes

    await vk.commit({ updates: new Map([['garbage', bytes('g')]]) })
    const orphan = vk.currentCommit
    const orphanRoot = await rootOf(store, orphan)
    const orphanNodes = (await readTree(store, orphanRoot)).nodes
    const exclusive = [...orphanNodes].filter((h) => !keepNodes.has(h))
    expect(exclusive.length, 'orphan should own nodes the live commit does not').toBeGreaterThan(0)

    await vk.resetTo(keep)
    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(1)

    // Commit metadata gone.
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await store.get(`__parent_commit__${orphan}`)).toBeNull()
    expect(await store.get(`__commit_time__${orphan}`)).toBeNull()
    // Blob gone.
    expect(await store.get(`${orphan}:garbage`)).toBeNull()
    // Every node the orphan uniquely owned is gone...
    for (const h of exclusive) {
      expect(await store.get(Keyset.DEFAULT_PREFIX + h), `orphan node ${h} survived`).toBeNull()
    }
    // ...and every node the live commit needs survived.
    const live = await readTree(store, await rootOf(store, keep))
    expect([...live.missing]).toEqual([])
    for (let i = 0; i < 40; i++) {
      expect(await vk.get(`key${i}`)).toEqual(bytes(`v${i}`))
    }
  })

  it('collects an orphan chain, not just its tip', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v0')]]) })
    const keep = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })
    await vk.commit({ updates: new Map([['k', bytes('v3')]]) })
    await vk.resetTo(keep)

    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(3)

    // Nothing but the surviving commit's own nodes is left in the
    // namespace — the chain took its whole tree with it.
    const survivors = (await readTree(store, await rootOf(store, keep))).nodes
    const expected = new Set([...survivors].map((h) => Keyset.DEFAULT_PREFIX + h))
    expect(await nodeKeys(store)).toEqual(expected)
    expect(await vk.get('k')).toEqual(bytes('v0'))
  })
})

describe('cleanOrphans — structural sharing', () => {
  it('a subtree shared between a live commit and an orphan survives', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const seeds = new Map<string, Uint8Array>()
    for (let i = 0; i < 60; i++) seeds.set(`key${i}`, bytes(`v${i}`))
    await vk.commit({ updates: seeds })
    const base = vk.currentCommit

    // A branch that adds one key: most subtrees stay shared with base.
    const feature = (await vk.createBranch('feature')) as VersionedKV
    await feature.commit({ updates: new Map([['feature-only', bytes('fv')]]) })
    const featureCommit = feature.currentCommit
    const featureNodes = (await readTree(store, await rootOf(store, featureCommit))).nodes
    const baseNodes = (await readTree(store, await rootOf(store, base))).nodes
    const shared = [...featureNodes].filter((h) => baseNodes.has(h))
    expect(shared.length, 'expected structural sharing between base and feature').toBeGreaterThan(0)

    await vk.deleteBranch('feature')
    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(1)

    for (const h of shared) {
      expect(
        await store.get(Keyset.DEFAULT_PREFIX + h),
        `shared node ${h} was swept`,
      ).not.toBeNull()
    }
    const live = await readTree(store, await rootOf(store, base))
    expect([...live.missing]).toEqual([])
    expect(live.entries.size).toBe(60)
  })

  it('two orphans sharing a subtree both get collected without double-delete trouble', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const keep = vk.currentCommit

    const seeds = new Map<string, Uint8Array>()
    for (let i = 0; i < 40; i++) seeds.set(`key${i}`, bytes(`v${i}`))
    const a = (await vk.createBranch('a', { at: keep })) as VersionedKV
    await a.commit({ updates: seeds })
    const forkPoint = a.currentCommit
    await a.commit({ updates: new Map([['a-only', bytes('av')]]) })
    const b = (await vk.createBranch('b', { at: forkPoint })) as VersionedKV
    await b.commit({ updates: new Map([['b-only', bytes('bv')]]) })

    await vk.deleteBranch('a')
    await vk.deleteBranch('b')
    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(3)

    const live = await readTree(store, await rootOf(store, keep))
    expect([...live.missing]).toEqual([])
    expect(await vk.get('seed')).toEqual(bytes('s'))
  })
})

describe('cleanOrphans — damaged orphans', () => {
  it('does not crash on an orphan whose HAMT nodes are already missing', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const keep = vk.currentCommit
    await vk.commit({ updates: new Map([['garbage', bytes('g')]]) })
    const orphan = vk.currentCommit
    const orphanRoot = await rootOf(store, orphan)
    await vk.resetTo(keep)

    // Simulate a half-swept / interrupted store: the orphan's commit
    // metadata is there but its keyset nodes are gone.
    const orphanNodes = (await readTree(store, orphanRoot)).nodes
    const keepNodes = (await readTree(store, await rootOf(store, keep))).nodes
    await store.removeMany(
      [...orphanNodes].filter((h) => !keepNodes.has(h)).map((h) => Keyset.DEFAULT_PREFIX + h),
    )

    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(1)
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await vk.get('seed')).toEqual(bytes('s'))

    // The orphan's blob is only ever found through the keyset that
    // points at it, and that keyset is gone, so it survives until a
    // deepClean scans the blob namespace.
    const garbage = await blobKey(bytes('g'))
    expect(await store.get(garbage)).not.toBeNull()
    await vk.deepClean(SWEEP_ALL)
    expect(await store.get(garbage)).toBeNull()
    expect(await vk.get('seed')).toEqual(bytes('s'))
  })

  it('does not crash on an orphan whose root is corrupt', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const keep = vk.currentCommit
    await vk.commit({ updates: new Map([['garbage', bytes('g')]]) })
    const orphan = vk.currentCommit
    await vk.resetTo(keep)

    await store.set(COMMIT_ROOT(orphan), bytes('not json'))
    expect(await vk.cleanOrphans(SWEEP_ALL)).toBe(1)
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await vk.get('seed')).toEqual(bytes('s'))
  })
})

// ---------------------------------------------------------------------------
// deepClean
// ---------------------------------------------------------------------------

describe('deepClean', () => {
  it('reclaims stray nodes that no commit points at, which cleanOrphans leaves', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })

    // A keyset written but never committed — an interrupted write, or a
    // crash between the node write and the CAS. No orphan commit points
    // at it, so no orphan keyset walk can find it.
    const stray = await (await Keyset.empty(store)).persist({
      updates: [['stranded', { blob: 'kvgit:blob:deadbeef', meta: { size: 1 } }]],
    })
    const strayKey = Keyset.DEFAULT_PREFIX + stray.root
    expect(await store.get(strayKey)).not.toBeNull()

    // The incremental path cannot see it.
    await vk.cleanOrphans(SWEEP_ALL)
    expect(
      await store.get(strayKey),
      'cleanOrphans is scoped to orphan keysets and must leave commit-less nodes alone',
    ).not.toBeNull()

    // The quiescent-store path does.
    await vk.deepClean(SWEEP_ALL)
    expect(await store.get(strayKey)).toBeNull()

    // Live state is untouched.
    expect(await vk.get('seed')).toEqual(bytes('s'))
  })

  it('collects orphan commits too, and reports the same count', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const keep = vk.currentCommit
    await vk.commit({ updates: new Map([['garbage', bytes('g')]]) })
    const orphan = vk.currentCommit
    await vk.resetTo(keep)

    expect(await vk.deepClean(SWEEP_ALL)).toBe(1)
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await store.get(`${orphan}:garbage`)).toBeNull()
    expect(await vk.get('seed')).toEqual(bytes('s'))
  })

  it('protects nodes of young orphans from the namespace scan', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['seed', bytes('s')]]) })
    const keep = vk.currentCommit
    await vk.commit({ updates: new Map([['young', bytes('y')]]) })
    const young = vk.currentCommit
    const youngNodes = (await readTree(store, await rootOf(store, young))).nodes
    await vk.resetTo(keep)

    expect(await vk.deepClean({ minAge: 60_000_000 })).toBe(0)
    for (const h of youngNodes) {
      expect(
        await store.get(Keyset.DEFAULT_PREFIX + h),
        `young orphan node ${h} swept`,
      ).not.toBeNull()
    }
  })
})
