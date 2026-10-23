/**
 * Helpers the backends share for `casMany`.
 */

/** The removals of a batch, refusing a key that is also written. */
export function checkBatch(
  writes: ReadonlyMap<string, Uint8Array>,
  removes: Iterable<string>,
): string[] {
  const list = [...removes]
  for (const key of list) {
    if (writes.has(key)) throw new Error(`casMany: '${key}' is both written and removed`)
  }
  return list
}

/** Whether two values are the same bytes; `null` (absent) equals only `null`. */
export function bytesEqual(a: Uint8Array | null, b: Uint8Array | null): boolean {
  if (a === b) return true
  if (a === null || b === null) return false
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}
