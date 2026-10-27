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

import { safeLoads } from './layout'

/** Default seconds a sweep's lease stays live: what a sweep that dies
 *  holding it costs writers, at most. */
export const GC_LEASE_TTL = 600

/** Seconds an in-flight marker protects an unpublished commit. */
export const IN_FLIGHT_TTL = 600

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
