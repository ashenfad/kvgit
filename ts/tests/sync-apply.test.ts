import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  VersionedKV,
  type WireCommit,
  applyWire,
  clearSyncHead,
  getSyncHead,
  setSyncHead,
  walkDelta,
} from '../src/index'
import { claimSyncHead } from '../src/sync/apply'
import { IN_FLIGHT_PREFIX, ROOT_COMMIT } from '../src/versioned/layout'

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

/** A small source store: linear commits plus a divergence merged back
 *  in (so the stream contains a multi-parent commit that takes a key
 *  from its second parent). */
async function buildSource(): Promise<{ store: Memory; head: string; midpoint: string }> {
  const store = new Memory()
  const vk = await VersionedKV.open(store)
  await vk.commit({ updates: new Map([['greeting', bytes('hello')]]) })
  await vk.commit({
    updates: new Map([
      ['greeting', bytes('hello world')],
      ['files/a.txt', bytes('aaa')],
    ]),
    info: { title: 'turn 2' },
  })
  const midpoint = vk.currentCommit

  // Divergence: a second writer on a stale base forces a three-way
  // merge that takes a key from the other side.
  const stale = await VersionedKV.open(store, { commitHash: midpoint })
  await vk.commit({ updates: new Map([['ka', bytes('from-a')]]) })
  const r = await stale.commit({ updates: new Map([['kb', bytes('from-b')]]) })
  expect(r.strategy).toBe('three_way')

  const final = await VersionedKV.open(store)
  await final.commit({ removals: new Set(['files/a.txt']) })
  return { store, head: final.currentCommit, midpoint }
}

describe('applyWire — round trips', () => {
  it('replays a full history into a fresh store, byte-identically', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)

    const dst = new Memory()
    const result = await applyWire(dst, wire, { createBranch: 'main' })
    expect(result.applied).toBe(wire.length)
    expect(result.skipped).toBe(0)
    expect(result.head).toBe(head)

    // The replayed store opens as a normal kvgit store at the same
    // content-addressed head, with values and meta intact.
    const vk = await VersionedKV.open(dst)
    expect(vk.currentCommit).toBe(head)
    expect(text((await vk.get('greeting')) as Uint8Array)).toBe('hello world')
    expect(text((await vk.get('ka')) as Uint8Array)).toBe('from-a')
    expect(text((await vk.get('kb')) as Uint8Array)).toBe('from-b')
    expect(await vk.get('files/a.txt')).toBeNull()

    // History (incl. the merge's two parents) survives the wire.
    const srcVk = await VersionedKV.open(src)
    const srcHistory: string[] = []
    for await (const c of srcVk.history(head, { allParents: true })) srcHistory.push(c)
    const dstHistory: string[] = []
    for await (const c of vk.history(head, { allParents: true })) dstHistory.push(c)
    expect(dstHistory).toEqual(srcHistory)

    // Commit info round-trips.
    const infos: unknown[] = []
    for (const wc of wire) infos.push(await vk.commitInfo(wc.hash))
    expect(infos).toContainEqual({ title: 'turn 2' })
  })

  it('applies an incremental delta on top of a previously-synced prefix', async () => {
    const { store: src, head, midpoint } = await buildSource()
    const dst = new Memory()

    // First sync: history up to the midpoint.
    await applyWire(dst, await collect(src, midpoint), { createBranch: 'main' })

    // Second sync: only the delta. Parents of the delta resolve from
    // the destination store (the cache-fallback path).
    const delta = await collect(src, head, [midpoint])
    const result = await applyWire(dst, delta)
    expect(result.applied).toBe(delta.length)
    expect(result.head).toBe(head)

    const vk = await VersionedKV.open(dst, { commitHash: head })
    expect(text((await vk.get('kb')) as Uint8Array)).toBe('from-b')
    expect(await vk.get('files/a.txt')).toBeNull()
  })

  it('is idempotent: re-applying skips every commit and changes nothing', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)

    const dst = new Memory()
    await applyWire(dst, wire, { createBranch: 'main' })
    const snapshot = new Map<string, string>()
    for await (const [k, v] of dst.items()) snapshot.set(k, text(v))

    const again = await applyWire(dst, wire)
    expect(again.applied).toBe(0)
    expect(again.skipped).toBe(wire.length)
    // Skipped commits are marked in flight until a head names them, as
    // written ones are; nothing else changes.
    expect(again.inFlight).toEqual(wire.map((w) => w.hash))

    const after = new Map<string, string>()
    for await (const [k, v] of dst.items()) {
      if (!k.startsWith(IN_FLIGHT_PREFIX)) after.set(k, text(v))
    }
    expect(after).toEqual(snapshot)
  })

  it('the replayed store is itself a valid transfer source (A→B→C)', async () => {
    const { store: a, head } = await buildSource()
    const b = new Memory()
    await applyWire(b, await collect(a, head), { createBranch: 'main' })
    const c = new Memory()
    await applyWire(c, await collect(b, head), { createBranch: 'main' })
    const vk = await VersionedKV.open(c)
    expect(vk.currentCommit).toBe(head)
    expect(text((await vk.get('greeting')) as Uint8Array)).toBe('hello world')
  })
})

describe('applyWire — refusals', () => {
  it('refuses a commit whose update bytes were tampered with', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)

    // Tamper with the newest commit that has updates.
    const target = [...wire].reverse().find((w) => w.updates.size > 0) as WireCommit
    const tampered = wire.map((w) =>
      w === target
        ? {
            ...w,
            updates: new Map([...w.updates].map(([k]) => [k, bytes('evil')] as const)),
          }
        : w,
    )

    const dst = new Memory()
    await expect(applyWire(dst, tampered, { createBranch: 'main' })).rejects.toThrow(
      /integrity check failed/,
    )
    // The tampered commit's records never landed.
    expect(await dst.get(`__commit_root__${target.hash}`)).toBeNull()
  })

  it('refuses a merge that drops a key it took from its second parent', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)
    const merge = wire.find((w) => w.parents.length > 1) as WireCommit
    expect(merge.updates.size).toBeGreaterThan(0)
    const stripped = wire.map((w) => (w === merge ? { ...w, updates: new Map() } : w))

    const dst = new Memory()
    await expect(applyWire(dst, stripped)).rejects.toThrow(/integrity check failed/)
  })

  it('refuses a commit whose info was altered', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)
    const titled = wire.find((w) => w.info !== null) as WireCommit
    const altered = wire.map((w) => (w === titled ? { ...w, info: { title: 'other' } } : w))

    const dst = new Memory()
    await expect(applyWire(dst, altered)).rejects.toThrow(/integrity check failed/)
  })

  it('refuses a commit whose parent generations were altered', async () => {
    // The commit hash doesn't cover them, so only the parents' own
    // records can catch it.
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)
    const merge = wire.find((w) => w.parents.length > 1) as WireCommit
    const altered = wire.map((w) => (w === merge ? { ...w, parentGens: [0, 0] } : w))

    await expect(applyWire(new Memory(), altered)).rejects.toThrow(/parent generations/)
  })

  it('refuses a root commit with content', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)
    expect(wire[0]?.hash).toBe(ROOT_COMMIT)
    const root = wire[0] as WireCommit
    const stuffed = [{ ...root, updates: new Map([['k', bytes('v')]]) }]

    await expect(applyWire(new Memory(), stuffed)).rejects.toThrow(/integrity check failed/)
  })

  it('refuses out-of-order streams (child before parent)', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)
    const dst = new Memory()
    await expect(applyWire(dst, [...wire].reverse())).rejects.toThrow(/parents-first/)
  })

  it('refuses createBranch onto an existing branch', async () => {
    const { store: src, head } = await buildSource()
    const wire = await collect(src, head)
    const dst = new Memory()
    await applyWire(dst, wire, { createBranch: 'main' })
    await expect(applyWire(dst, wire, { createBranch: 'main' })).rejects.toThrow(/already exists/)
  })
})

describe('sync heads', () => {
  it('round-trips and clears remote-tracking state', async () => {
    const store = new Memory()
    expect(await getSyncHead(store, 'chat-ab12')).toBeNull()
    await setSyncHead(store, 'chat-ab12', 'a'.repeat(40))
    expect(await getSyncHead(store, 'chat-ab12')).toBe('a'.repeat(40))
    await clearSyncHead(store, 'chat-ab12')
    expect(await getSyncHead(store, 'chat-ab12')).toBeNull()
  })

  it('does not pollute the branch namespace', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await setSyncHead(store, 'main', vk.currentCommit)
    await setSyncHead(store, 'main', vk.currentCommit, 'github:o/r')
    expect(await vk.listBranches()).toEqual(['main'])
  })

  it('keeps one head per remote', async () => {
    const store = new Memory()
    await setSyncHead(store, 'chat', 'a'.repeat(40), 'github:o/one')
    await setSyncHead(store, 'chat', 'b'.repeat(40), { id: 'github:o/two' })
    expect(await getSyncHead(store, 'chat', 'github:o/one')).toBe('a'.repeat(40))
    expect(await getSyncHead(store, 'chat', { id: 'github:o/two' })).toBe('b'.repeat(40))
    expect(await getSyncHead(store, 'chat', 'github:o/three')).toBeNull()
    // two remotes hold one: asked without a remote, there's no one answer
    expect(await getSyncHead(store, 'chat')).toBeNull()
    await clearSyncHead(store, 'chat', 'github:o/one')
    expect(await getSyncHead(store, 'chat', 'github:o/one')).toBeNull()
    expect(await getSyncHead(store, 'chat')).toBe('b'.repeat(40))
  })

  it("doesn't mix up branches whose names run into each other", async () => {
    const store = new Memory()
    await setSyncHead(store, 'a', 'a'.repeat(40), 'r')
    await setSyncHead(store, 'a/b', 'b'.repeat(40), 'r')
    await setSyncHead(store, 'a%2Fb', 'c'.repeat(40), 'r')
    expect(await getSyncHead(store, 'a')).toBe('a'.repeat(40))
    await clearSyncHead(store, 'a')
    expect(await getSyncHead(store, 'a/b', 'r')).toBe('b'.repeat(40))
    expect(await getSyncHead(store, 'a%2Fb', 'r')).toBe('c'.repeat(40))
  })

  it('clears every remote when none is named', async () => {
    const store = new Memory()
    await setSyncHead(store, 'chat', 'a'.repeat(40))
    await setSyncHead(store, 'other', 'c'.repeat(40), 'r1')
    await setSyncHead(store, 'chat', 'b'.repeat(40), 'r2')
    await setSyncHead(store, 'chat', 'b'.repeat(40), 'r1')
    await clearSyncHead(store, 'chat')
    expect(await getSyncHead(store, 'chat')).toBeNull()
    expect(await getSyncHead(store, 'chat', 'r1')).toBeNull()
    expect(await getSyncHead(store, 'chat', 'r2')).toBeNull()
    expect(await getSyncHead(store, 'other', 'r1')).toBe('c'.repeat(40))
  })

  it("reads a head kept before per-remote records, until a remote's own retires it", async () => {
    const store = new Memory()
    await setSyncHead(store, 'chat', 'a'.repeat(40)) // as an older kvgit kept it
    expect(await getSyncHead(store, 'chat')).toBe('a'.repeat(40))
    expect(await getSyncHead(store, 'chat', 'r1')).toBe('a'.repeat(40))
    await setSyncHead(store, 'chat', 'b'.repeat(40), 'r1')
    expect(await getSyncHead(store, 'chat', 'r1')).toBe('b'.repeat(40))
    expect(await getSyncHead(store, 'chat', 'r2')).toBeNull()
    expect(await getSyncHead(store, 'chat')).toBe('b'.repeat(40))
  })

  it('gives a head kept before per-remote records to one remote, when two claim it at once', async () => {
    const store = new Memory()
    await setSyncHead(store, 'chat', 'a'.repeat(40)) // as an older kvgit kept it
    const [one, two] = await Promise.all([
      claimSyncHead(store, 'chat', 'r1'),
      claimSyncHead(store, 'chat', 'r2'),
    ])
    expect([one, two].filter((h) => h !== null)).toEqual(['a'.repeat(40)])
    const owner = one !== null ? 'r1' : 'r2'
    const other = one !== null ? 'r2' : 'r1'
    expect(await getSyncHead(store, 'chat', owner)).toBe('a'.repeat(40))
    expect(await getSyncHead(store, 'chat', other)).toBeNull()
    // and claimed again, it's still that remote's
    expect(await claimSyncHead(store, 'chat', owner)).toBe('a'.repeat(40))
  })

  it('keeps the shared head for a remote without an id', async () => {
    const store = new Memory()
    await setSyncHead(store, 'chat', 'a'.repeat(40), {})
    expect(await getSyncHead(store, 'chat')).toBe('a'.repeat(40))
    expect(await getSyncHead(store, 'chat', {})).toBe('a'.repeat(40))
    await setSyncHead(store, 'chat', 'b'.repeat(40), 'r1')
    // a remote with no id reads only the shared record, which r1 retired
    expect(await getSyncHead(store, 'chat', {})).toBeNull()
  })
})
