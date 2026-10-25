import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Hamt } from '../src/hamt'

const enc = new TextEncoder()
const dec = new TextDecoder()

const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array): string => dec.decode(b)

async function entries(h: Hamt): Promise<Array<[string, string]>> {
  const out: Array<[string, string]> = []
  for await (const [k, v] of h.items()) out.push([k, text(v)])
  out.sort(([a], [b]) => a.localeCompare(b))
  return out
}

describe('Hamt — empty', () => {
  it('an empty Hamt has the canonical empty hash', async () => {
    const store = new Memory()
    const h = await Hamt.empty(store)
    expect(h.root).toBe(await Hamt.emptyHash())
  })

  it('reads on an empty Hamt return null', async () => {
    const store = new Memory()
    const h = await Hamt.empty(store)
    expect(await h.get('any')).toBeNull()
    expect(await h.has('any')).toBe(false)
    expect(await h.size()).toBe(0)
  })

  it('two empty Hamts on different stores have the same root hash', async () => {
    const a = await Hamt.empty(new Memory())
    const b = await Hamt.empty(new Memory())
    expect(a.root).toBe(b.root)
  })
})

describe('Hamt — basic insert / get', () => {
  it('round-trips a single value', async () => {
    const store = new Memory()
    const h = await (await Hamt.empty(store)).persist({ updates: [['k', bytes('v')]] })
    expect(text((await h.get('k')) as Uint8Array)).toBe('v')
  })

  it('overwrites and yields the latest value', async () => {
    const store = new Memory()
    let h = await Hamt.empty(store)
    h = await h.persist({ updates: [['k', bytes('v1')]] })
    h = await h.persist({ updates: [['k', bytes('v2')]] })
    expect(text((await h.get('k')) as Uint8Array)).toBe('v2')
  })

  it('removes a key', async () => {
    const store = new Memory()
    let h = await Hamt.empty(store)
    h = await h.persist({ updates: [['k', bytes('v')]] })
    h = await h.persist({ removals: ['k'] })
    expect(await h.get('k')).toBeNull()
    expect(h.root).toBe(await Hamt.emptyHash())
  })

  it('idempotent insert is a no-op (same root)', async () => {
    const store = new Memory()
    const h1 = await (await Hamt.empty(store)).persist({ updates: [['k', bytes('v')]] })
    const h2 = await h1.persist({ updates: [['k', bytes('v')]] })
    expect(h2.root).toBe(h1.root)
  })

  it('removing a missing key is a no-op (same root)', async () => {
    const store = new Memory()
    const h1 = await (await Hamt.empty(store)).persist({ updates: [['k', bytes('v')]] })
    const h2 = await h1.persist({ removals: ['absent'] })
    expect(h2.root).toBe(h1.root)
  })
})

describe('Hamt — Map parity', () => {
  it('matches Map for a 50-key insert+get workload', async () => {
    const store = new Memory()
    let h = await Hamt.empty(store, { bucketMax: 4 }) // small buckets to force splits
    const ref = new Map<string, string>()

    const updates: Array<[string, Uint8Array]> = []
    for (let i = 0; i < 50; i++) {
      const k = `key${i}`
      const v = `val${i}`
      updates.push([k, bytes(v)])
      ref.set(k, v)
    }
    h = await h.persist({ updates })

    for (const [k, v] of ref) {
      expect(text((await h.get(k)) as Uint8Array)).toBe(v)
    }

    const got = await entries(h)
    const expected = [...ref].sort(([a], [b]) => a.localeCompare(b))
    expect(got).toEqual(expected)
    expect(await h.size()).toBe(50)
  })

  it('matches Map after a series of inserts and deletes', async () => {
    const store = new Memory()
    let h = await Hamt.empty(store, { bucketMax: 4 })
    const ref = new Map<string, string>()

    const insertOrder = Array.from({ length: 30 }, (_, i) => i)
    const deleteOrder = [3, 7, 11, 17, 19, 23, 29]

    for (const i of insertOrder) {
      h = await h.persist({ updates: [[`k${i}`, bytes(`v${i}`)]] })
      ref.set(`k${i}`, `v${i}`)
    }
    for (const i of deleteOrder) {
      h = await h.persist({ removals: [`k${i}`] })
      ref.delete(`k${i}`)
    }

    expect(await h.size()).toBe(ref.size)
    for (const [k, v] of ref) {
      expect(text((await h.get(k)) as Uint8Array)).toBe(v)
    }
    for (const i of deleteOrder) {
      expect(await h.get(`k${i}`)).toBeNull()
    }
  })
})

describe('Hamt — collapse-equivalence invariant', () => {
  it('delete-back-to-bucketMax produces the same root as never-overflowed', async () => {
    // Insert N > bucketMax keys (forces split into branch).
    // Delete enough that the survivors fit in a single leaf again.
    // The resulting root must equal a fresh HAMT built with just the
    // survivors — proving _try_collapse preserves canonical form.
    const bucketMax = 4
    const survivorCount = 3 // < bucketMax so they fit in one leaf

    // Path 1: insert all, then delete down.
    const storeA = new Memory()
    let a = await Hamt.empty(storeA, { bucketMax })
    const allKeys = Array.from({ length: 12 }, (_, i) => `k${i}`)
    const survivors = allKeys.slice(0, survivorCount)
    const toDelete = allKeys.slice(survivorCount)

    a = await a.persist({ updates: allKeys.map((k): [string, Uint8Array] => [k, bytes(k)]) })
    a = await a.persist({ removals: toDelete })

    // Path 2: insert only the survivors from the start.
    const storeB = new Memory()
    let b = await Hamt.empty(storeB, { bucketMax })
    b = await b.persist({ updates: survivors.map((k): [string, Uint8Array] => [k, bytes(k)]) })

    expect(a.root).toBe(b.root)
  })

  it('delete-down-to-empty yields the empty root', async () => {
    const store = new Memory()
    let h = await Hamt.empty(store, { bucketMax: 4 })
    const keys = Array.from({ length: 20 }, (_, i) => `k${i}`)
    h = await h.persist({ updates: keys.map((k): [string, Uint8Array] => [k, bytes(k)]) })
    h = await h.persist({ removals: keys })
    expect(h.root).toBe(await Hamt.emptyHash())
  })
})

describe('Hamt — diff', () => {
  it('reports added / removed / modified between two HAMTs', async () => {
    const store = new Memory()
    let a = await Hamt.empty(store, { bucketMax: 4 })
    a = await a.persist({
      updates: [
        ['k1', bytes('v1')],
        ['k2', bytes('v2')],
        ['k3', bytes('v3')],
      ],
    })

    let b = await Hamt.empty(store, { bucketMax: 4 })
    b = await b.persist({
      updates: [
        ['k1', bytes('v1')], // unchanged
        ['k2', bytes('v2-modified')], // modified
        ['k4', bytes('v4')], // added
        // k3 removed
      ],
    })

    const d = await a.diff(b)
    const added = [...d.added.keys()].sort()
    const removed = [...d.removed.keys()].sort()
    const modified = [...d.modified.keys()].sort()

    expect(added).toEqual(['k4'])
    expect(removed).toEqual(['k3'])
    expect(modified).toEqual(['k2'])
  })

  it('identical HAMTs diff to empty', async () => {
    const store = new Memory()
    const a = await (await Hamt.empty(store)).persist({
      updates: [
        ['k1', bytes('v1')],
        ['k2', bytes('v2')],
      ],
    })
    const b = await (await Hamt.empty(store)).persist({
      updates: [
        ['k1', bytes('v1')],
        ['k2', bytes('v2')],
      ],
    })
    expect(a.root).toBe(b.root) // sanity: same logical contents → same root
    const d = await a.diff(b)
    expect(d.added.size).toBe(0)
    expect(d.removed.size).toBe(0)
    expect(d.modified.size).toBe(0)
  })
})

describe('Hamt — diff of keys named like Object.prototype members', () => {
  // Leaves parse to plain objects, whose inherited members (`toString`,
  // `constructor`, ...) must not read as keys either side holds.
  for (const bucketMax of [1, 32]) {
    it(`reports them added, removed and modified (bucketMax ${bucketMax})`, async () => {
      const store = new Memory()
      const build = async (entries: Array<[string, string]>): Promise<Hamt> =>
        (await Hamt.empty(store, { bucketMax })).persist({
          updates: entries.map(([k, v]): [string, Uint8Array] => [k, bytes(v)]),
        })
      const a = await build([
        ['constructor', '1'],
        ['toString', '1'],
        ['__proto__', '1'],
        ['other', 'x'],
      ])
      const b = await build([
        ['constructor', '2'],
        ['hasOwnProperty', '1'],
        ['__proto__', '2'],
        ['other', 'x'],
      ])
      const summary = (d: Awaited<ReturnType<Hamt['diff']>>) => ({
        added: [...d.added].map(([k, v]) => [k, text(v)]).sort(),
        removed: [...d.removed].map(([k, v]) => [k, text(v)]).sort(),
        modified: [...d.modified].map(([k, [o, n]]) => [k, text(o), text(n)]).sort(),
      })
      expect(summary(await a.diff(b))).toEqual({
        added: [['hasOwnProperty', '1']],
        removed: [['toString', '1']],
        modified: [
          ['__proto__', '1', '2'],
          ['constructor', '1', '2'],
        ],
      })
      expect(summary(await b.diff(a))).toEqual({
        added: [['toString', '1']],
        removed: [['hasOwnProperty', '1']],
        modified: [
          ['__proto__', '2', '1'],
          ['constructor', '2', '1'],
        ],
      })
      const plain = await build([['other', 'x']])
      expect(summary(await plain.diff(a))).toEqual({
        added: [
          ['__proto__', '1'],
          ['constructor', '1'],
          ['toString', '1'],
        ],
        removed: [],
        modified: [],
      })
    })
  }
})

describe('Hamt — pending semantics', () => {
  it('updated() returns a Hamt with non-empty pending; flush() persists it', async () => {
    const store = new Memory()
    const empty = await Hamt.empty(store)
    const staged = await empty.updated({ updates: [['k', bytes('v')]] })
    expect(staged.pending.size).toBeGreaterThan(0)

    // The store has no nodes yet — only pending does.
    const allKeys: string[] = []
    for await (const k of store.keys()) allKeys.push(k)
    expect(allKeys.length).toBe(0)

    // After flush, the store has the nodes.
    const flushed = await staged.flush()
    expect(flushed.pending.size).toBe(0)
    const afterKeys: string[] = []
    for await (const k of store.keys()) afterKeys.push(k)
    expect(afterKeys.length).toBeGreaterThan(0)
    expect(text((await flushed.get('k')) as Uint8Array)).toBe('v')
  })

  it('reads on an unflushed Hamt resolve through pending', async () => {
    const store = new Memory()
    const empty = await Hamt.empty(store)
    const staged = await empty.updated({ updates: [['k', bytes('v')]] })
    expect(text((await staged.get('k')) as Uint8Array)).toBe('v')
  })
})

describe('Hamt — getMany', () => {
  it('finds what get finds, across a split tree, with one read per level', async () => {
    class CountingMemory extends Memory {
      batches = 0
      override async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
        this.batches++
        return super.getMany(keys)
      }
    }
    const store = new CountingMemory()
    const updates = Array.from({ length: 200 }, (_, i) => [`k${i}`, bytes(`v${i}`)] as const)
    // A fresh view of the persisted root, so every node comes from the
    // store rather than the writer's pending map.
    const h = new Hamt(store, (await (await Hamt.empty(store)).persist({ updates })).root)

    const asked = ['k0', 'k57', 'k199', 'missing', 'k57', 'also-missing']
    store.batches = 0
    const found = await h.getMany(asked)
    const batches = store.batches
    expect([...found.keys()].sort()).toEqual(['k0', 'k199', 'k57'])
    for (const [k, v] of found) expect(text(v)).toBe(text((await h.get(k)) as Uint8Array))
    // Bounded by the depth of the tree, not the number of keys.
    expect(batches).toBeGreaterThan(1)
    expect(batches).toBeLessThanOrEqual(4)
  })

  it('reads an empty Hamt without touching the store', async () => {
    const h = await Hamt.empty(new Memory())
    expect((await h.getMany(['a', 'b'])).size).toBe(0)
  })

  it('sees pending nodes', async () => {
    const h = await (await Hamt.empty(new Memory())).updated({
      updates: [['a', bytes('1')]],
    })
    expect(text((await h.getMany(['a'])).get('a') as Uint8Array)).toBe('1')
  })
})
