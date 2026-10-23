import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { runConformance } from './kv-conformance'

runConformance('Memory', () => new Memory())

describe('Memory — backend-specific', () => {
  it("keeps a Node Buffer's bytes apart from the buffer under it", async () => {
    // `Buffer#slice` is a view, so copying with it would keep (and pin)
    // the whole buffer and see later writes to it.
    const under = Buffer.alloc(4096, 7)
    const s = new Memory()
    await s.set('k', under.subarray(10, 14))
    under.fill(0)
    const got = (await s.get('k')) as Uint8Array
    expect([...got]).toEqual([7, 7, 7, 7])
    expect(got.buffer.byteLength).toBe(4)
  })
})
