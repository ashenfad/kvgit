/**
 * The GC lease: how a sweep and the writers beside it stay out of each
 * other's way, as Python kvgit does it.
 *
 * A sweep takes the store-wide lease (`__gc_lease__`) before it reads
 * anything. Every write that adds something a sweep must see (a
 * commit's batch, a head naming a commit, a tag) is conditional on the
 * lease record the writer read after waiting any live sweep out, so it
 * lands before a sweep starts or after it ends, never during. A
 * commit's batch also carries an in-flight marker (`__inflight__<c>`),
 * removed by the write that publishes it, so a sweep that starts
 * between the two marks the commit live.
 *
 * Lease and marker expiries coordinate writers in different handles
 * and processes, so they are always wall-clock seconds, whatever clock
 * a handle stamps its commits with.
 */

import { GcBusy } from '../errors'
import { PyFloat } from '../pyjson'
import type { KVStore } from '../types'
import { GC_LEASE_KEY, dumps, safeLoads } from './layout'

/** Default seconds a sweep's lease stays live: what a sweep that dies
 *  holding it costs writers, at most. */
export const GC_LEASE_TTL = 600

/** Seconds an in-flight marker protects an unpublished commit. */
export const IN_FLIGHT_TTL = 600

/** Seconds a writer sleeps between checks while a lease is live. */
export const GC_WAIT_POLL = 0.05

/**
 * Where leases and in-flight markers read the time (wall time, in
 * seconds), how a writer waits out a live lease, and how a lease names
 * its holder. Tests that need identical bytes from every run replace
 * these.
 *
 * @internal
 */
export const leaseEnv = {
  clock: (): number => Date.now() / 1000,
  sleep: (seconds: number): Promise<void> =>
    new Promise((resolve) => setTimeout(resolve, seconds * 1000)),
  owner: (): string => `ts-${globalThis.crypto.randomUUID().replace(/-/g, '').slice(0, 12)}`,
}

/** The clock leases and in-flight markers expire by. */
export const leaseClock = (): number => leaseEnv.clock()

/** A fresh in-flight marker: the time its protection lapses. */
export function inFlightMarker(): Uint8Array {
  return dumps(new PyFloat(leaseClock() + IN_FLIGHT_TTL))
}

/** Whether a marker still protects its commit. */
export function markerLive(raw: Uint8Array | null): boolean {
  const expires = raw === null ? null : safeLoads(raw)
  return typeof expires === 'number' && expires > leaseClock()
}

/** The time (seconds) the lease in `raw` runs out; 0 for no lease.
 *  Bytes that don't decode to a record with a numeric `expires` count
 *  as expired, so garbage under the key can't wedge the store. */
export function leaseExpiry(raw: Uint8Array | null): number {
  if (raw === null) return 0
  const record = safeLoads(raw)
  if (record === null || typeof record !== 'object' || Array.isArray(record)) return 0
  const expires = (record as Record<string, unknown>).expires
  return typeof expires === 'number' ? expires : 0
}

/**
 * Wait until no live lease is held; return the lease record then
 * (absent, or the expired record the last sweep left). A write that
 * must not land under a sweep expects these bytes: a sweep that starts
 * afterwards replaces them with a record of its own, so the write fails
 * and the writer comes back here.
 *
 * While a lease is live this polls, sleeping at most until that lease's
 * expiry, so a holder that died without releasing delays a writer by
 * the remainder of its term and no longer.
 */
export async function waitForGc(store: KVStore): Promise<Uint8Array | null> {
  let raw = await store.get(GC_LEASE_KEY)
  while (raw !== null) {
    const remaining = leaseExpiry(raw) - leaseClock()
    if (remaining <= 0) return raw
    await leaseEnv.sleep(Math.min(GC_WAIT_POLL, remaining))
    raw = await store.get(GC_LEASE_KEY)
  }
  return null
}

/**
 * Apply a batch unless a sweep has started since `lease` was read.
 *
 * @returns true when it landed; false when one of the caller's own
 *   expectations failed; null when the lease moved, so whatever the
 *   caller checked may no longer hold: wait, check again, retry.
 */
export async function tryLand(
  store: KVStore,
  lease: Uint8Array | null,
  expected: ReadonlyMap<string, Uint8Array | null>,
  writes: ReadonlyMap<string, Uint8Array>,
  removes: Iterable<string> = [],
): Promise<boolean | null> {
  const expect = new Map(expected)
  expect.set(GC_LEASE_KEY, lease)
  if (await store.casMany(expect, writes, removes)) return true
  return sameBytes(await store.get(GC_LEASE_KEY), lease) ? false : null
}

export function sameBytes(a: Uint8Array | null, b: Uint8Array | null): boolean {
  if (a === null || b === null) return a === b
  return a.length === b.length && a.every((v, i) => v === b[i])
}

/** A held lease: the bytes this sweep wrote and when they expire. */
export interface HeldLease {
  readonly bytes: Uint8Array
  readonly expires: number
}

function leaseRecord(owner: string, expires: number | PyFloat): Uint8Array {
  return dumps(
    new Map<string, unknown>([
      ['owner', owner],
      ['expires', expires],
    ]),
  )
}

/**
 * Take the lease by CAS, or throw {@link GcBusy}. The CAS expects
 * exactly the bytes the liveness decision was made from, so two sweeps
 * racing one expired lease can't both win, and a live lease is never
 * overwritten.
 */
export async function acquireGcLease(store: KVStore, ttl: number): Promise<HeldLease> {
  const raw = await store.get(GC_LEASE_KEY)
  const at = leaseClock()
  const expiry = leaseExpiry(raw)
  if (expiry > at) {
    const holder = safeLoads(raw as Uint8Array) as Record<string, unknown> | null
    throw new GcBusy(
      `A sweep holds the GC lease (owner ${JSON.stringify(holder?.owner ?? null)}, expires in ${(expiry - at).toFixed(1)}s)`,
    )
  }
  const expires = at + ttl
  const bytes = leaseRecord(leaseEnv.owner(), new PyFloat(expires))
  if (!(await store.cas(GC_LEASE_KEY, bytes, raw))) {
    throw new GcBusy('Another sweep took the GC lease first')
  }
  return { bytes, expires }
}

/**
 * Give up a lease this sweep took, if the store still holds it: a CAS
 * overwriting our bytes with the same record expired, never a delete.
 * Writers that read the lease before this sweep expect the bytes they
 * read, and a record carrying this sweep's owner can never match them
 * again. A failed CAS means the lease is no longer ours (it ran out and
 * another sweep claimed it), and that one must not be cleared.
 */
export async function releaseGcLease(store: KVStore, held: HeldLease, ttl: number): Promise<void> {
  const overrun = leaseClock() - held.expires
  if (overrun > 0) {
    console.warn(
      `kvgit: a sweep ran ${overrun.toFixed(1)}s past its ${ttl}s GC lease; writers were free to write during that window. Raise leaseTtl.`,
    )
  }
  const record = safeLoads(held.bytes) as Record<string, unknown> | null
  const owner = typeof record?.owner === 'string' ? record.owner : ''
  await store.cas(GC_LEASE_KEY, leaseRecord(owner, 0), held.bytes)
}
