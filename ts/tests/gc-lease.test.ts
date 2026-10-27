/**
 * The GC lease and in-flight markers: how a sweep and the writers
 * beside it stay out of each other's way.
 *
 * A sweep holds `__gc_lease__` while it runs; every write that adds
 * something a sweep must see lands only while no lease is live. A
 * commit's batch carries an `__inflight__<commit>` marker until the
 * write that publishes it, so a sweep starting in between keeps it.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  GcBusy,
  MemoryRemote,
  ROOT_COMMIT,
  Repo,
  StorageVersionError,
  type Worktree,
  applyWire,
  publishHead,
  pullBranch,
  walkDelta,
} from '../src/index'
import { PyFloat } from '../src/pyjson'
import type { KVStore } from '../src/types'
import { gcStore } from '../src/versioned/kv'
import {
  BRANCH_HEAD,
  COMMIT_ROOT,
  GC_LEASE_KEY,
  IN_FLIGHT_KEY,
  IN_FLIGHT_PREFIX,
  dumps,
  safeLoads,
} from '../src/versioned/layout'
import { GC_LEASE_TTL, leaseExpiry } from '../src/versioned/lease'

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

/** A worktree on `main`, created if the store has none. */
async function open(store: KVStore, opts: { now?: () => number } = {}): Promise<Worktree> {
  const repo = await Repo.open(store, opts)
  return repo.worktree('main', { create: true })
}

/** Write one key and commit it. */
function commitOne(
  wt: Worktree,
  key: string,
  value: string,
  opts: Parameters<Worktree['commit']>[0] = {},
) {
  wt.set(key, bytes(value))
  return wt.commit(opts)
}

async function markers(store: Memory): Promise<string[]> {
  const out: string[] = []
  for await (const k of store.keys(IN_FLIGHT_PREFIX)) out.push(k)
  return out
}

/** A lease record as a sweep writes one, held until `expires`. */
const liveLease = (expires: number): Uint8Array =>
  dumps(
    new Map<string, unknown>([
      ['owner', 'someone-else'],
      ['expires', expires],
    ]),
  )

/**
 * A Memory that runs a one-shot hook just before the `casMany` that
 * publishes a head: the window between a commit's batch and its
 * publish.
 */
class PublishHookStore extends Memory {
  beforePublish: (() => Promise<void>) | null = null

  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const hook = this.beforePublish
    if (hook !== null && [...writes.keys()].some((k) => k.startsWith('__branch_head__'))) {
      this.beforePublish = null
      await hook()
    }
    return super.casMany(expected, writes, removes)
  }
}

describe('in-flight markers', () => {
  it('a batch carries a marker and the publish removes it', async () => {
    const store = new PublishHookStore()
    const wt = await open(store)
    let seen: string[] = []
    store.beforePublish = async () => {
      seen = await markers(store)
    }
    await commitOne(wt, 'k', 'v')
    expect(seen).toEqual([IN_FLIGHT_KEY(wt.head)])
    expect(await markers(store)).toEqual([])
  })

  it('an unpublished commit survives a minAge 0 sweep, and then publishes', async () => {
    const store = new PublishHookStore()
    const wt = await open(store)
    let removed = -1
    store.beforePublish = async () => {
      // The batch is in, the head isn't: without the marker the commit
      // would look like abandoned work.
      removed = await gcStore(store, { minAge: 0 })
    }
    await commitOne(wt, 'k', 'v')
    expect(removed).toBe(0)
    expect(await store.get(COMMIT_ROOT(wt.head))).not.toBeNull()
    expect(await (await open(store)).get('k')).toEqual(bytes('v'))
    expect(await markers(store)).toEqual([])
  })

  it("keeps the merge path's own first commit through a sweep too", async () => {
    const store = new PublishHookStore()
    const a = await open(store)
    const b = await open(store)
    await commitOne(a, 'a', '1')
    // b finds the branch moved: it writes its own commit and a merge
    // commit over it, both in flight, and a sweep runs before the
    // merge publishes.
    let swept = false
    store.beforePublish = async () => {
      expect((await markers(store)).length).toBe(2)
      await gcStore(store, { minAge: 0 })
      swept = true
    }
    const result = await commitOne(b, 'b', '2')
    expect(swept).toBe(true)
    expect(result.strategy).toBe('three_way')
    const tip = await open(store)
    expect(await tip.get('a')).toEqual(bytes('1'))
    expect(await tip.get('b')).toEqual(bytes('2'))
    expect(await markers(store)).toEqual([])
  })

  it('an abandoned attempt withdraws its markers', async () => {
    const store = new Memory()
    const a = await open(store)
    const b = await open(store)
    await commitOne(a, 'k', 'a')
    const result = await commitOne(b, 'k', 'b', { onConflict: 'abandon' })
    expect(result.merged).toBe(false)
    // Withdrawal is best effort and asynchronous; let it land.
    await new Promise((r) => setTimeout(r, 0))
    expect(await markers(store)).toEqual([])
  })

  it('a lapsed marker releases its commit to the sweep', async () => {
    const store = new PublishHookStore()
    const wt = await open(store)
    let commit = ''
    let swept = -1
    store.beforePublish = async () => {
      // The writer stalls past its marker's term before publishing:
      // the marker reads as lapsed.
      const [key] = await markers(store)
      commit = (key as string).slice(IN_FLIGHT_PREFIX.length)
      await store.set(key as string, dumps(new PyFloat(1)))
      // minAge -1: the commit counts as old enough even within the
      // millisecond it was made.
      swept = await gcStore(store, { minAge: -1 })
    }
    // Its commit was swept, so its publish refuses rather than naming a
    // commit that is gone.
    await expect(commitOne(wt, 'k', 'v')).rejects.toThrow()
    expect(swept).toBe(1)
    expect(await store.get(COMMIT_ROOT(commit))).toBeNull()
    expect(await markers(store)).toEqual([])
    expect(safeLoads((await store.get(BRANCH_HEAD('main'))) as Uint8Array)).toBe(ROOT_COMMIT)
  })

  it('withdraws an abandoned marker only if it still holds its own bytes', async () => {
    const store = new Memory()
    const a = await open(store)
    const b = await open(store)
    await commitOne(a, 'k', 'a')
    // Another writer's batch rewrites b's marker key between b's batch
    // and its withdrawal: b must leave that marker alone.
    const original = store.casMany.bind(store)
    let theirs: Uint8Array | null = null
    store.casMany = async (expected, writes, removes = []) => {
      const list = [...removes]
      const marker = list.find((k) => k.startsWith(IN_FLIGHT_PREFIX))
      if (marker !== undefined && writes.size === 0 && theirs === null) {
        // A later marker than b's own, so the bytes differ.
        theirs = dumps(new PyFloat(Date.now() / 1000 + 1234.5))
        await store.set(marker, theirs)
      }
      return original(expected, writes, list)
    }
    const result = await commitOne(b, 'k', 'b', { onConflict: 'abandon' })
    expect(result.merged).toBe(false)
    await new Promise((r) => setTimeout(r, 0))
    expect(theirs).not.toBeNull()
    const left = await markers(store)
    expect(left.length).toBe(1)
    expect(await store.get(left[0] as string)).toEqual(theirs)
  })

  it('sweeps keep and judge markers by wall time, whatever clock commits use', async () => {
    // A handle on a frozen historical clock: its markers still expire in
    // wall time, so a sweep on the same clock keeps its commit.
    const store = new PublishHookStore()
    const wt = await open(store, { now: () => 0 })
    store.beforePublish = async () => {
      expect(await gcStore(store, { minAge: 0, now: () => 0 })).toBe(0)
    }
    await commitOne(wt, 'k', 'v')
    expect(await (await open(store)).get('k')).toEqual(bytes('v'))
  })
})

describe('writers under the lease', () => {
  it('a writer waits for a live lease and proceeds once it is released', async () => {
    const store = new Memory()
    const wt = await open(store)
    const now = Date.now() / 1000
    const held = liveLease(now + 60)
    await store.set(GC_LEASE_KEY, held)

    let landed = false
    const writing = commitOne(wt, 'k', 'v').then(() => {
      landed = true
    })
    await new Promise((r) => setTimeout(r, 120))
    expect(landed, 'the commit landed under a live lease').toBe(false)
    expect(await markers(store), 'its batch landed under a live lease').toEqual([])

    // Released the way a sweep releases: the record, expired.
    await store.cas(GC_LEASE_KEY, liveLease(0), held)
    await writing
    expect(landed).toBe(true)
    expect(await (await open(store)).get('k')).toEqual(bytes('v'))
  })

  it('a writer does not wait past the lease expiry', async () => {
    const store = new Memory()
    const wt = await open(store)
    // A holder that died: its lease runs out in 0.1s.
    await store.set(GC_LEASE_KEY, liveLease(Date.now() / 1000 + 0.1))
    const started = Date.now()
    await commitOne(wt, 'k', 'v')
    expect(Date.now() - started).toBeLessThan(2000)
  })

  it('tags, branches and resets wait for the lease too', async () => {
    const store = new Memory()
    const repo = await Repo.open(store)
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', bytes('v'))
    const commit = (await wt.commit()).commit as string
    const expires = Date.now() / 1000 + 0.15
    await store.set(GC_LEASE_KEY, liveLease(expires))

    // Waiting the lease out means landing no earlier than its expiry,
    // however long the runner took to get here.
    await repo.tags.create('t', commit)
    expect(Date.now() / 1000).toBeGreaterThanOrEqual(expires)
    await repo.branches.create('dev', commit)
    await wt.reset(ROOT_COMMIT)
    expect(await repo.tags.get('t')).toBe(commit)
  })
})

describe('the lease itself', () => {
  it('is taken by a sweep and released as an expired record no earlier reader matches', async () => {
    const store = new Memory()
    const wt = await open(store)
    await commitOne(wt, 'k', 'v')
    await gcStore(store, { minAge: 0 })
    const raw = await store.get(GC_LEASE_KEY)
    expect(raw).not.toBeNull()
    expect(leaseExpiry(raw)).toBe(0)
    const record = safeLoads(raw as Uint8Array) as { owner: string }
    expect(typeof record.owner).toBe('string')

    // A second sweep writes a different owner, so a writer holding the
    // first record can't land under it.
    await gcStore(store, { minAge: 0 })
    expect(
      (safeLoads((await store.get(GC_LEASE_KEY)) as Uint8Array) as { owner: string }).owner,
    ).not.toBe(record.owner)
  })

  it('refuses a second sweep with wait: false, and waits by default', async () => {
    const store = new Memory()
    await open(store)
    // A lease that outlives the refusals, however slow the runner.
    const held = liveLease(Date.now() / 1000 + 60)
    await store.set(GC_LEASE_KEY, held)
    await expect(gcStore(store, { wait: false })).rejects.toThrow(GcBusy)
    const repo = await Repo.open(store)
    await expect(repo.gc({ deep: true, wait: false })).rejects.toThrow(GcBusy)
    // A sweep waits the holder out by default: a short lease, so the
    // wait is.
    const expires = Date.now() / 1000 + 0.15
    expect(await store.cas(GC_LEASE_KEY, liveLease(expires), held)).toBe(true)
    expect(await repo.gc()).toBe(0)
    expect(Date.now() / 1000).toBeGreaterThanOrEqual(expires)
  })

  it('treats an expired or unreadable record as no lease', async () => {
    const store = new Memory()
    await open(store)
    await store.set(GC_LEASE_KEY, liveLease(1))
    expect(await gcStore(store, { wait: false })).toBe(0)
    await store.set(GC_LEASE_KEY, bytes('not json'))
    expect(await gcStore(store, { wait: false })).toBe(0)
  })

  it('is released when the sweep throws', async () => {
    class FailingMemory extends Memory {
      armed = false
      override async *keys(prefix?: string): AsyncIterable<string> {
        if (this.armed && prefix === COMMIT_ROOT('')) {
          this.armed = false
          throw new Error('backend down')
        }
        yield* super.keys(prefix)
      }
    }
    const store = new FailingMemory()
    await open(store)
    store.armed = true
    await expect(gcStore(store, { minAge: 0 })).rejects.toThrow('backend down')
    expect(leaseExpiry(await store.get(GC_LEASE_KEY))).toBe(0)
  })

  it('lasts GC_LEASE_TTL by default', async () => {
    class PeekingMemory extends Memory {
      during: Uint8Array | null = null
      override async *keys(prefix?: string): AsyncIterable<string> {
        if (this.during === null) this.during = await this.get(GC_LEASE_KEY)
        yield* super.keys(prefix)
      }
    }
    const store = new PeekingMemory()
    await open(store)
    store.during = null
    const at = Date.now() / 1000
    await gcStore(store, { minAge: 0 })
    expect(leaseExpiry(store.during)).toBeGreaterThan(at + GC_LEASE_TTL - 5)
  })

  it('leaves a store of another layout untouched', async () => {
    const store = new Memory()
    await store.set('__kvgit_version__', bytes('9'))
    await expect(gcStore(store)).rejects.toThrow(StorageVersionError)
    expect(await store.get(GC_LEASE_KEY)).toBeNull()
  })
})

describe('synced commits', () => {
  it('are marked in flight until the head that names them lands', async () => {
    const src = new Memory()
    const wt = await open(src)
    await commitOne(wt, 'k', 'v')
    const head = wt.head

    const dst = new Memory()
    const applied = await applyWire(dst, walkDelta(src, { want: head }))
    expect(applied.inFlight.length).toBe(applied.applied)
    expect((await markers(dst)).length).toBe(applied.applied)
    // A sweep before any head names them keeps them.
    expect(await gcStore(dst, { minAge: 0 })).toBe(0)
    expect(await dst.get(COMMIT_ROOT(head))).not.toBeNull()

    // createBranch publishes them and drops the markers in one write.
    const fresh = new Memory()
    const created = await applyWire(fresh, walkDelta(src, { want: head }), {
      createBranch: 'main',
    })
    expect(created.inFlight).toEqual([])
    expect(await markers(fresh)).toEqual([])
  })

  it('re-protects commits already present, which may be orphans from an earlier fetch', async () => {
    const src = new Memory()
    const wt = await open(src)
    await commitOne(wt, 'k', 'v')
    const head = wt.head

    // An earlier fetch left the commits here unpublished, and their
    // markers have lapsed: they are orphans now.
    const dst = new Memory()
    await applyWire(dst, walkDelta(src, { want: head }))
    for (const key of await markers(dst)) await dst.set(key, dumps(new PyFloat(1)))

    const again = await applyWire(dst, walkDelta(src, { want: head }))
    expect(again.applied).toBe(0)
    expect(again.inFlight).toContain(head)
    // A sweep between the apply and the publish keeps them.
    expect(await gcStore(dst, { minAge: -1 })).toBe(0)
    expect(await publishHead(dst, 'main', null, head, { inFlight: again.inFlight })).toBe(true)
    expect(await markers(dst)).toEqual([])
    expect(await (await open(dst)).get('k')).toEqual(bytes('v'))
  })

  it('a pull publishes the head and drops the markers together', async () => {
    const remoteStore = new Memory()
    const remote = new MemoryRemote(remoteStore)
    const writer = await open(remoteStore)
    await commitOne(writer, 'k', 'v')

    const local = new Memory()
    const outcome = await pullBranch(local, remote, 'main')
    expect(outcome.status).toBe('created')
    expect(await markers(local)).toEqual([])
    expect(await (await open(local)).get('k')).toEqual(bytes('v'))
  })
})
