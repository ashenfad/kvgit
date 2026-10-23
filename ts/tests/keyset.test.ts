import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Keyset, decodeEntry, encodeEntry } from '../src/keyset'
import type { KeysetEntry } from '../src/types'

const enc = new TextDecoder()

function entry(blob: string, size: number): KeysetEntry {
  return { blob, meta: { size } }
}

describe('encodeEntry / decodeEntry', () => {
  it('round-trips a KeysetEntry', () => {
    const e = entry('abc123:user_key', 42)
    const decoded = decodeEntry(encodeEntry(e))
    expect(decoded).toEqual(e)
  })

  it('encoding is deterministic for the same logical entry', () => {
    const a = encodeEntry(entry('blob', 10))
    const b = encodeEntry(entry('blob', 10))
    expect(enc.decode(a)).toBe(enc.decode(b))
  })

  it('encodes as Python kvgit writes an entry', () => {
    expect(enc.decode(encodeEntry(entry('kvgit:blob:ab', 3)))).toBe('["kvgit:blob:ab",{"size":3}]')
    const chunked = encodeEntry({ blob: 'x', meta: { size: 1, chunks: ['c1', 'c2'] } })
    expect(enc.decode(chunked)).toBe('["x",{"size":1,"chunks":["c1","c2"]}]')
    expect(decodeEntry(chunked)).toEqual({ blob: 'x', meta: { size: 1, chunks: ['c1', 'c2'] } })
  })
})

describe('Keyset — empty', () => {
  it('empty Keyset has the canonical empty HAMT root', async () => {
    const ks = await Keyset.empty(new Memory())
    expect(ks.root).toBeTypeOf('string')
    expect(ks.root.length).toBe(64) // hex SHA-256
    expect(ks.prefix).toBe('kvgit:keyset:')
    expect(ks.bucketMax).toBe(8)
  })

  it('reads on an empty Keyset return null', async () => {
    const ks = await Keyset.empty(new Memory())
    expect(await ks.get('any')).toBeNull()
    expect(await ks.getBlob('any')).toBeNull()
    expect(await ks.has('any')).toBe(false)
    expect(await ks.size()).toBe(0)
  })
})

describe('Keyset — round-trip', () => {
  it('stores and retrieves a KeysetEntry', async () => {
    const store = new Memory()
    const ks0 = await Keyset.empty(store)
    const e = entry('commit1:k', 100)
    const ks1 = await ks0.persist({ updates: [['k', e]] })
    expect(await ks1.get('k')).toEqual(e)
    expect(await ks1.getBlob('k')).toBe(e.blob)
  })

  it('iterates all entries', async () => {
    const store = new Memory()
    let ks = await Keyset.empty(store, { bucketMax: 4 })
    const entries = new Map<string, KeysetEntry>()
    const updates: Array<[string, KeysetEntry]> = []
    for (let i = 0; i < 20; i++) {
      const k = `k${i}`
      const e = entry(`c${i}:${k}`, i)
      updates.push([k, e])
      entries.set(k, e)
    }
    ks = await ks.persist({ updates })
    expect(await ks.size()).toBe(20)
    const seen = new Map<string, KeysetEntry>()
    for await (const [k, v] of ks.items()) seen.set(k, v)
    expect(seen).toEqual(entries)
  })

  it('removes entries', async () => {
    const store = new Memory()
    let ks = await Keyset.empty(store)
    ks = await ks.persist({ updates: [['k', entry('blob', 1)]] })
    ks = await ks.persist({ removals: ['k'] })
    expect(await ks.get('k')).toBeNull()
  })
})

describe('Keyset — uses a different storage prefix from raw HAMT', () => {
  it('stores HAMT nodes under kvgit:keyset:* by default', async () => {
    const store = new Memory()
    const ks = await Keyset.empty(store)
    const ks1 = await ks.persist({ updates: [['k', entry('b', 1)]] })
    const allKeys: string[] = []
    for await (const k of store.keys()) allKeys.push(k)
    expect(allKeys.length).toBeGreaterThan(0)
    expect(allKeys.every((k) => k.startsWith('kvgit:keyset:'))).toBe(true)
    expect(ks1.root).not.toBe(ks.root) // sanity
  })
})

describe('Keyset — walk', () => {
  it('returns typed entries plus all reachable HAMT node hashes', async () => {
    const store = new Memory()
    let ks = await Keyset.empty(store, { bucketMax: 2 }) // force splits
    const updates: Array<[string, KeysetEntry]> = []
    for (let i = 0; i < 10; i++) {
      updates.push([`k${i}`, entry(`c:${i}`, i)])
    }
    ks = await ks.persist({ updates })

    const [entries, nodes] = await ks.walk()
    expect(entries.size).toBe(10)
    expect(nodes.size).toBeGreaterThan(1) // at least one branch + leaves
    for (const [k, e] of entries) {
      expect(e.blob).toBe(`c:${k.slice(1)}`)
    }
  })

  it('skipNodes prunes already-visited subtrees', async () => {
    const store = new Memory()
    let ks = await Keyset.empty(store, { bucketMax: 2 })
    ks = await ks.persist({
      updates: Array.from({ length: 8 }, (_, i): [string, KeysetEntry] => [
        `k${i}`,
        entry(`c:${i}`, i),
      ]),
    })
    const [, allNodes] = await ks.walk()
    const [entries2, nodes2] = await ks.walk(allNodes)
    // With every node already in skipNodes, we visit nothing.
    expect(entries2.size).toBe(0)
    expect(nodes2.size).toBe(0)
  })
})

describe('Keyset — diff', () => {
  it('returns typed KeysetEntry adds, removes, and modifications', async () => {
    const store = new Memory()
    let a = await Keyset.empty(store)
    a = await a.persist({
      updates: [
        ['k1', entry('c:1', 1)],
        ['k2', entry('c:2', 2)],
        ['k3', entry('c:3', 3)],
      ],
    })

    let b = await Keyset.empty(store)
    b = await b.persist({
      updates: [
        ['k1', entry('c:1', 1)], // unchanged
        ['k2', entry('c:2-new', 22)], // modified
        ['k4', entry('c:4', 4)], // added
        // k3 removed
      ],
    })

    const d = await a.diff(b)
    expect([...d.added.keys()]).toEqual(['k4'])
    expect([...d.removed.keys()]).toEqual(['k3'])
    expect([...d.modified.keys()]).toEqual(['k2'])

    expect(d.added.get('k4')).toEqual(entry('c:4', 4))
    expect(d.removed.get('k3')).toEqual(entry('c:3', 3))
    const [oldK2, newK2] = d.modified.get('k2') as [KeysetEntry, KeysetEntry]
    expect(oldK2).toEqual(entry('c:2', 2))
    expect(newK2).toEqual(entry('c:2-new', 22))
  })

  it('identical Keysets diff to empty', async () => {
    const store = new Memory()
    const updates: Array<[string, KeysetEntry]> = [
      ['k1', entry('c:1', 1)],
      ['k2', entry('c:2', 2)],
    ]
    const a = await (await Keyset.empty(store)).persist({ updates })
    const b = await (await Keyset.empty(store)).persist({ updates })
    expect(a.root).toBe(b.root)
    const d = await a.diff(b)
    expect(d.added.size).toBe(0)
    expect(d.removed.size).toBe(0)
    expect(d.modified.size).toBe(0)
  })
})

describe('Keyset — pending semantics', () => {
  it('updated() stages without writing to the store; flush() persists', async () => {
    const store = new Memory()
    const ks0 = await Keyset.empty(store)
    const ks1 = await ks0.updated({
      updates: [['k', entry('blob', 1)]],
    })
    expect(ks1.pending.size).toBeGreaterThan(0)
    const before: string[] = []
    for await (const k of store.keys()) before.push(k)
    expect(before.length).toBe(0)

    const ks2 = await ks1.flush()
    expect(ks2.pending.size).toBe(0)
    const after: string[] = []
    for await (const k of store.keys()) after.push(k)
    expect(after.length).toBeGreaterThan(0)
    expect(await ks2.get('k')).toEqual(entry('blob', 1))
  })
})

describe('Keyset — fromRoot', () => {
  it('reconstructs a Keyset from a known root hash', async () => {
    const store = new Memory()
    const ks1 = await (await Keyset.empty(store)).persist({ updates: [['k', entry('blob', 1)]] })

    // Fresh handle on the same data
    const ks2 = Keyset.fromRoot(store, ks1.root)
    expect(await ks2.get('k')).toEqual(entry('blob', 1))
    expect(ks2.root).toBe(ks1.root)
  })
})
