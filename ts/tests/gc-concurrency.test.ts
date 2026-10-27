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
import { Repo, type Worktree } from '../src/index'
import type { KVStore } from '../src/types'
import { BLOB_PREFIX, COMMIT_ROOT, blobKey, dumps, loads, sha256Hex } from '../src/versioned/layout'

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

  override async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const wanted = [...keys]
    for (const key of wanted) await this.fire(this.beforeGet, key)
    return super.getMany(wanted)
  }

  private async fire(hooks: Map<string, () => Promise<void>>, key: string): Promise<void> {
    const hook = hooks.get(key)
    if (hook === undefined) return
    hooks.delete(key)
    await hook()
  }
}

/** Where HAMT nodes live, keyed by the hash of their bytes. */
const NODE_PREFIX = 'kvgit:keyset:'

/** A worktree on `main`, created if the store has none. */
async function open(store: KVStore): Promise<Worktree> {
  const repo = await Repo.open(store)
  return repo.worktree('main', { create: true })
}

/** Write `updates` and commit them. */
async function commitAll(wt: Worktree, updates: Iterable<[string, Uint8Array]>): Promise<void> {
  for (const [k, v] of updates) wt.set(k, v)
  await wt.commit()
}

/** Write one key and commit it. */
function commitOne(wt: Worktree, key: string, value: string): Promise<void> {
  return commitAll(wt, [[key, bytes(value)]])
}

/** A worktree on a new branch `name` at `at` (default: `wt`'s head). */
async function branchFrom(wt: Worktree, name: string, at: string = wt.head): Promise<Worktree> {
  await wt.repo.branches.create(name, at)
  return wt.repo.worktree(name)
}

/** Land a commit on `main` from an independent repository handle. */
async function landCommit(
  store: Memory,
  key: string,
  value: string,
): Promise<{ commit: string; root: string; blob: string }> {
  const writer = await open(store)
  await commitOne(writer, key, value)
  const commit = writer.head
  return { commit, root: await rootOf(store, commit), blob: await blobKey(bytes(value)) }
}

async function nodeKeys(store: Memory): Promise<Set<string>> {
  const out = new Set<string>()
  for await (const k of store.keys(NODE_PREFIX)) out.add(k)
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
): Promise<{ entries: Map<string, { blob: string }>; nodes: Set<string>; missing: Set<string> }> {
  const entries = new Map<string, { blob: string }>()
  const nodes = new Set<string>()
  const missing = new Set<string>()
  const queue = [root]
  while (queue.length > 0) {
    const h = queue.pop() as string
    if (nodes.has(h) || missing.has(h)) continue
    const raw = await store.get(NODE_PREFIX + h)
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
        // An entry is the JSON `[blob key, meta]`.
        const [blob] = JSON.parse(dec.decode(buf)) as [string, unknown]
        entries.set(k, { blob })
      }
    } else {
      queue.push(...Object.values(node.children))
    }
  }
  return { entries, nodes, missing }
}

// Content addressing
// ---------------------------------------------------------------------------

describe('content addressing', () => {
  it('two unrelated histories with identical final content hold the same nodes and blobs', async () => {
    // Same logical end state ({x: 1, y: 2}), reached by different
    // commit sequences in different stores.
    const storeA = new Memory()
    const a = await open(storeA)
    await commitOne(a, 'x', '1')
    await commitOne(a, 'y', '2')

    const storeB = new Memory()
    const b = await open(storeB)
    await commitOne(b, 'y', '2')
    await commitOne(b, 'x', '1')

    const rootA = await rootOf(storeA, a.head)
    expect(await rootOf(storeB, b.head)).toBe(rootA)
    expect(await blobKeys(storeA)).toEqual(await blobKeys(storeB))
    expect((await readTree(storeA, rootA)).nodes).toEqual((await readTree(storeB, rootA)).nodes)
  })

  it('stores a value written under two keys once', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitAll(wt, [
      ['a', bytes('same')],
      ['b', bytes('same')],
    ])
    const { entries } = await readTree(store, await rootOf(store, wt.head))
    expect(entries.get('a')?.blob).toBe(await blobKey(bytes('same')))
    expect(entries.get('b')?.blob).toBe(entries.get('a')?.blob)
    expect((await blobKeys(store)).size).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// The race
// ---------------------------------------------------------------------------

describe('gc — a commit starting mid-sweep', () => {
  /**
   * The seam is the first read of the orphan's `__commit_root__`, the
   * first read of the orphan-collection loop: the mark phase is done
   * and the sweep is collecting what to delete. A commit started here
   * waits for the sweep's lease and lands after it.
   */
  async function sweepWithLateCommit(lateValue: string, orphanValue: string) {
    const store = new HookedMemory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const keep = wt.head
    await commitOne(wt, 'garbage', orphanValue)
    const orphan = wt.head
    await wt.reset(keep)

    let late: Promise<{ commit: string; root: string; blob: string }> | null = null
    let landedDuringSweep = false
    store.beforeGet.set(COMMIT_ROOT(orphan), async () => {
      late = landCommit(store, 'late', lateValue).then((r) => {
        landedDuringSweep = !sweepDone
        return r
      })
    })
    let sweepDone = false
    const removed = await wt.repo.gc(SWEEP_ALL)
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
    const reopened = await open(store)
    expect(await reopened.get('late')).toEqual(bytes('shared'))
  })

  it('control: a sweep that finds no orphans deletes nothing', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const before = await nodeKeys(store)

    expect(await wt.repo.gc(SWEEP_ALL)).toBe(0)
    expect(await nodeKeys(store)).toEqual(before)
  })
})

// ---------------------------------------------------------------------------
// The sweep still sweeps
// ---------------------------------------------------------------------------

describe('gc — still collects ordinary garbage', () => {
  it("removes an orphan's commit metadata, blobs and exclusively-owned nodes", async () => {
    const store = new Memory()
    const wt = await open(store)
    const seeds = new Map<string, Uint8Array>()
    for (let i = 0; i < 40; i++) seeds.set(`key${i}`, bytes(`v${i}`))
    await commitAll(wt, seeds)
    const keep = wt.head
    const keepNodes = (await readTree(store, await rootOf(store, keep))).nodes

    await commitOne(wt, 'garbage', 'g')
    const orphan = wt.head
    const orphanRoot = await rootOf(store, orphan)
    const orphanNodes = (await readTree(store, orphanRoot)).nodes
    const exclusive = [...orphanNodes].filter((h) => !keepNodes.has(h))
    expect(exclusive.length, 'orphan should own nodes the live commit does not').toBeGreaterThan(0)

    await wt.reset(keep)
    expect(await wt.repo.gc(SWEEP_ALL)).toBe(1)

    // Commit metadata gone.
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await store.get(`__parent_commit__${orphan}`)).toBeNull()
    expect(await store.get(`__commit_time__${orphan}`)).toBeNull()
    // Blob gone.
    expect(await store.get(await blobKey(bytes('g')))).toBeNull()
    // Every node the orphan uniquely owned is gone...
    for (const h of exclusive) {
      expect(await store.get(NODE_PREFIX + h), `orphan node ${h} survived`).toBeNull()
    }
    // ...and every node the live commit needs survived.
    const live = await readTree(store, await rootOf(store, keep))
    expect([...live.missing]).toEqual([])
    for (let i = 0; i < 40; i++) {
      expect(await wt.get(`key${i}`)).toEqual(bytes(`v${i}`))
    }
  })

  it('collects an orphan chain, not just its tip', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'k', 'v0')
    const keep = wt.head
    await commitOne(wt, 'k', 'v1')
    await commitOne(wt, 'k', 'v2')
    await commitOne(wt, 'k', 'v3')
    await wt.reset(keep)

    expect(await wt.repo.gc(SWEEP_ALL)).toBe(3)

    // Nothing but the surviving commit's own nodes is left in the
    // namespace — the chain took its whole tree with it.
    const survivors = (await readTree(store, await rootOf(store, keep))).nodes
    const expected = new Set([...survivors].map((h) => NODE_PREFIX + h))
    expect(await nodeKeys(store)).toEqual(expected)
    expect(await wt.get('k')).toEqual(bytes('v0'))
  })
})

describe('gc — structural sharing', () => {
  it('a subtree shared between a live commit and an orphan survives', async () => {
    const store = new Memory()
    const wt = await open(store)
    const seeds = new Map<string, Uint8Array>()
    for (let i = 0; i < 60; i++) seeds.set(`key${i}`, bytes(`v${i}`))
    await commitAll(wt, seeds)
    const base = wt.head

    // A branch that adds one key: most subtrees stay shared with base.
    const feature = await branchFrom(wt, 'feature')
    await commitOne(feature, 'feature-only', 'fv')
    const featureCommit = feature.head
    const featureNodes = (await readTree(store, await rootOf(store, featureCommit))).nodes
    const baseNodes = (await readTree(store, await rootOf(store, base))).nodes
    const shared = [...featureNodes].filter((h) => baseNodes.has(h))
    expect(shared.length, 'expected structural sharing between base and feature').toBeGreaterThan(0)

    await wt.repo.branches.delete('feature')
    expect(await wt.repo.gc(SWEEP_ALL)).toBe(1)

    for (const h of shared) {
      expect(await store.get(NODE_PREFIX + h), `shared node ${h} was swept`).not.toBeNull()
    }
    const live = await readTree(store, await rootOf(store, base))
    expect([...live.missing]).toEqual([])
    expect(live.entries.size).toBe(60)
  })

  it('two orphans sharing a subtree both get collected without double-delete trouble', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const keep = wt.head

    const seeds = new Map<string, Uint8Array>()
    for (let i = 0; i < 40; i++) seeds.set(`key${i}`, bytes(`v${i}`))
    const a = await branchFrom(wt, 'a', keep)
    await commitAll(a, seeds)
    const forkPoint = a.head
    await commitOne(a, 'a-only', 'av')
    const b = await branchFrom(wt, 'b', forkPoint)
    await commitOne(b, 'b-only', 'bv')

    await wt.repo.branches.delete('a')
    await wt.repo.branches.delete('b')
    expect(await wt.repo.gc(SWEEP_ALL)).toBe(3)

    const live = await readTree(store, await rootOf(store, keep))
    expect([...live.missing]).toEqual([])
    expect(await wt.get('seed')).toEqual(bytes('s'))
  })
})

describe('gc — damaged orphans', () => {
  it('does not crash on an orphan whose HAMT nodes are already missing', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const keep = wt.head
    await commitOne(wt, 'garbage', 'g')
    const orphan = wt.head
    const orphanRoot = await rootOf(store, orphan)
    await wt.reset(keep)

    // Simulate a half-swept / interrupted store: the orphan's commit
    // metadata is there but its keyset nodes are gone.
    const orphanNodes = (await readTree(store, orphanRoot)).nodes
    const keepNodes = (await readTree(store, await rootOf(store, keep))).nodes
    await store.removeMany(
      [...orphanNodes].filter((h) => !keepNodes.has(h)).map((h) => NODE_PREFIX + h),
    )

    expect(await wt.repo.gc(SWEEP_ALL)).toBe(1)
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await wt.get('seed')).toEqual(bytes('s'))

    // The orphan's blob is only ever found through the keyset that
    // points at it, and that keyset is gone, so it survives until a
    // deep sweep scans the blob namespace.
    const garbage = await blobKey(bytes('g'))
    expect(await store.get(garbage)).not.toBeNull()
    await wt.repo.gc({ ...SWEEP_ALL, deep: true, wait: false })
    expect(await store.get(garbage)).toBeNull()
    expect(await wt.get('seed')).toEqual(bytes('s'))
  })

  it('does not crash on an orphan whose root is corrupt', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const keep = wt.head
    await commitOne(wt, 'garbage', 'g')
    const orphan = wt.head
    await wt.reset(keep)

    await store.set(COMMIT_ROOT(orphan), bytes('not json'))
    expect(await wt.repo.gc(SWEEP_ALL)).toBe(1)
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await wt.get('seed')).toEqual(bytes('s'))
  })
})

// ---------------------------------------------------------------------------
// Deep sweeps
// ---------------------------------------------------------------------------

describe('gc — deep', () => {
  it('reclaims stray nodes that no commit points at, which a plain sweep leaves', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')

    // A keyset node written but never committed — an interrupted write,
    // or a crash between the node write and the CAS. No orphan commit
    // points at it, so no orphan keyset walk can find it.
    const entry = enc.encode(JSON.stringify(['kvgit:blob:deadbeef', { size: 1 }]))
    const node = dumps({ items: { stranded: btoa(String.fromCharCode(...entry)) }, kind: 'leaf' })
    const strayKey = NODE_PREFIX + (await sha256Hex(node))
    await store.set(strayKey, node)

    // The incremental path cannot see it.
    await wt.repo.gc(SWEEP_ALL)
    expect(
      await store.get(strayKey),
      'a plain sweep is scoped to orphan keysets and must leave commit-less nodes alone',
    ).not.toBeNull()

    // The quiescent-store path does.
    await wt.repo.gc({ ...SWEEP_ALL, deep: true, wait: false })
    expect(await store.get(strayKey)).toBeNull()

    // Live state is untouched.
    expect(await wt.get('seed')).toEqual(bytes('s'))
  })

  it('collects orphan commits too, and reports the same count', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const keep = wt.head
    await commitOne(wt, 'garbage', 'g')
    const orphan = wt.head
    await wt.reset(keep)

    expect(await wt.repo.gc({ ...SWEEP_ALL, deep: true, wait: false })).toBe(1)
    expect(await store.get(COMMIT_ROOT(orphan))).toBeNull()
    expect(await store.get(await blobKey(bytes('g')))).toBeNull()
    expect(await wt.get('seed')).toEqual(bytes('s'))
  })

  it('protects nodes of young orphans from the namespace scan', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'seed', 's')
    const keep = wt.head
    await commitOne(wt, 'young', 'y')
    const young = wt.head
    const youngNodes = (await readTree(store, await rootOf(store, young))).nodes
    await wt.reset(keep)

    expect(await wt.repo.gc({ minAge: 60_000_000, deep: true, wait: false })).toBe(0)
    for (const h of youngNodes) {
      expect(await store.get(NODE_PREFIX + h), `young orphan node ${h} swept`).not.toBeNull()
    }
  })
})
