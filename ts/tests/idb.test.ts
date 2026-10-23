import { afterEach, describe, expect, it } from 'vitest'
import { IndexedDB } from '../src/backends/idb'
import { runConformance } from './kv-conformance'

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

// Keep one open handle per `makeStore` call so the test can call
// methods on it; close + delete on teardown so each test starts
// from a clean DB.
const openHandles: IndexedDB[] = []
const dbNames = new Set<string>()

let counter = 0
function uniqueDbName(): string {
  counter++
  const name = `kvgit-conformance-${Date.now()}-${counter}`
  dbNames.add(name)
  return name
}

afterEach(async () => {
  for (const h of openHandles) h.close()
  openHandles.length = 0
  for (const name of dbNames) {
    await IndexedDB.deleteDatabase(name).catch(() => undefined)
  }
  dbNames.clear()
})

runConformance('IndexedDB', async () => {
  const idb = await IndexedDB.open({ dbName: uniqueDbName() })
  openHandles.push(idb)
  return idb
})

describe('IndexedDB — backend-specific', () => {
  it('persists across close + reopen of the same database', async () => {
    const dbName = uniqueDbName()
    const a = await IndexedDB.open({ dbName })
    await a.set('k', bytes('v'))
    a.close()

    const b = await IndexedDB.open({ dbName })
    openHandles.push(b)
    const got = await b.get('k')
    expect(got).not.toBeNull()
    expect(new TextDecoder().decode(got as Uint8Array)).toBe('v')
  })

  it('different dbName values are isolated stores', async () => {
    const aName = uniqueDbName()
    const bName = uniqueDbName()
    const a = await IndexedDB.open({ dbName: aName })
    const b = await IndexedDB.open({ dbName: bName })
    openHandles.push(a, b)

    await a.set('k', bytes('a-value'))
    await b.set('k', bytes('b-value'))

    expect(new TextDecoder().decode((await a.get('k')) as Uint8Array)).toBe('a-value')
    expect(new TextDecoder().decode((await b.get('k')) as Uint8Array)).toBe('b-value')
  })

  it('concurrent CAS on the same key — only one succeeds', async () => {
    // IDB serializes readwrite transactions on the same object store,
    // so two simultaneous CAS calls expecting `null` must linearize:
    // one wins (sees null, writes), the other loses (sees the winner's
    // value, doesn't write).
    const idb = await IndexedDB.open({ dbName: uniqueDbName() })
    openHandles.push(idb)

    const [r1, r2] = await Promise.all([
      idb.cas('k', bytes('a'), null),
      idb.cas('k', bytes('b'), null),
    ])
    // Exactly one succeeded.
    expect(r1 !== r2).toBe(true)
    // The stored value is whichever side won.
    const stored = new TextDecoder().decode((await idb.get('k')) as Uint8Array)
    expect(stored === 'a' || stored === 'b').toBe(true)
  })

  it('concurrent set + get see consistent state', async () => {
    // Many writes interleaved with reads. Each get should return either
    // null (not yet written) or the exact value that was set; never a
    // partial / corrupted byte sequence.
    const idb = await IndexedDB.open({ dbName: uniqueDbName() })
    openHandles.push(idb)

    const ops: Promise<unknown>[] = []
    for (let i = 0; i < 50; i++) {
      ops.push(idb.set(`k${i}`, bytes(`v${i}`)))
    }
    await Promise.all(ops)

    for (let i = 0; i < 50; i++) {
      const got = await idb.get(`k${i}`)
      expect(new TextDecoder().decode(got as Uint8Array)).toBe(`v${i}`)
    }
  })

  it('reads back a value stored as a view into a larger buffer', async () => {
    // Values written before the backend copied views out are stored
    // with their whole buffer; they stay that way and read as their bytes.
    const dbName = uniqueDbName()
    const idb = await IndexedDB.open({ dbName })
    idb.close()
    const raw = await new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open(dbName)
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
    const under = new Uint8Array(4096).fill(9)
    under.set([1, 2, 3], 50)
    await new Promise<void>((resolve, reject) => {
      const tx = raw.transaction('kv', 'readwrite')
      tx.objectStore('kv').put(under.subarray(50, 53), 'old')
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
    raw.close()

    const b = await IndexedDB.open({ dbName })
    openHandles.push(b)
    expect([...((await b.get('old')) as Uint8Array)]).toEqual([1, 2, 3])
    expect([...((await b.getMany(['old'])).get('old') as Uint8Array)]).toEqual([1, 2, 3])
    expect(await b.cas('old', bytes('new'), new Uint8Array([1, 2, 3]))).toBe(true)
  })

  it('a cbor repository stores each value in a buffer of its own size', async () => {
    const { Repo } = await import('../src/repo')
    const idb = await IndexedDB.open({ dbName: uniqueDbName() })
    openHandles.push(idb)
    const repo = await Repo.open(idb, { codec: 'cbor' })
    const wt = await repo.worktree('main', { create: true })
    wt.set('text', 'x'.repeat(4096))
    wt.set('blob', new Uint8Array(65536))
    await wt.commit()

    let held = 0
    let stored = 0
    for await (const [, value] of idb.items()) {
      held += value.byteLength
      stored += value.buffer.byteLength
    }
    expect(held).toBeGreaterThan(4096 + 65536)
    expect(stored).toBe(held)
  })

  it('end-to-end VersionedKV over IndexedDB', async () => {
    // Sanity check that the higher layers compose correctly with this
    // backend — same VersionedKV/Staged behavior we test against
    // Memory should hold over IDB.
    const { VersionedKV } = await import('../src/versioned/kv')
    const { Staged } = await import('../src/staged')

    const dbName = uniqueDbName()
    const idb1 = await IndexedDB.open({ dbName })
    openHandles.push(idb1)

    const vk1 = await VersionedKV.open(idb1)
    const s1 = new Staged(vk1)
    s1.set('greeting', 'hello')
    await s1.commit({ info: { author: 'a' } })
    const head = vk1.currentCommit

    // Reopen via a second handle on the same DB — state survives.
    idb1.close()
    openHandles.length = 0
    const idb2 = await IndexedDB.open({ dbName })
    openHandles.push(idb2)
    const vk2 = await VersionedKV.open(idb2)
    expect(vk2.currentCommit).toBe(head)
    const s2 = new Staged(vk2)
    expect(await s2.get<string>('greeting')).toBe('hello')
    expect(await vk2.commitInfo()).toEqual({ author: 'a' })
  })
})

describe('IndexedDB.open with several stores in one database', () => {
  const text = (b: Uint8Array | null): string | null =>
    b === null ? null : new TextDecoder().decode(b)

  it('adds a store to a database that lacks it', async () => {
    const dbName = uniqueDbName()
    const a = await IndexedDB.open({ dbName })
    await a.set('k', bytes('in kv'))
    a.close()

    const b = await IndexedDB.open({ dbName, storeName: 'other' })
    openHandles.push(b)
    await b.set('k', bytes('in other'))
    const again = await IndexedDB.open({ dbName })
    openHandles.push(again)
    expect(text(await again.get('k'))).toBe('in kv')
    expect(text(await b.get('k'))).toBe('in other')
    await again.set('k2', bytes('2'))
    expect(await b.get('k2')).toBeNull()
  })

  it('two opens of the same new store both get it', async () => {
    const dbName = uniqueDbName()
    const a = await IndexedDB.open({ dbName })
    a.close()
    const [b, c] = await Promise.all([
      IndexedDB.open({ dbName, storeName: 'other' }),
      IndexedDB.open({ dbName, storeName: 'other' }),
    ])
    openHandles.push(b, c)
    await b.set('k', bytes('v'))
    expect(text(await c.get('k'))).toBe('v')
  })

  it('rejects when a connection blocks adding the store past blockedMs', async () => {
    const dbName = uniqueDbName()
    const a = await IndexedDB.open({ dbName })
    await expect(IndexedDB.open({ dbName, storeName: 'other', blockedMs: 100 })).rejects.toThrow(
      /kept it open for 100 ms/,
    )
    a.close()

    const b = await IndexedDB.open({ dbName, storeName: 'other' })
    openHandles.push(b)
    await b.set('k', bytes('v'))
    expect(text(await b.get('k'))).toBe('v')
  })
})

describe('IndexedDB.deleteDatabase', () => {
  it('waits for a connection that closes meanwhile, then deletes', async () => {
    const dbName = uniqueDbName()
    const a = await IndexedDB.open({ dbName })
    await a.set('k', bytes('v'))
    const deleting = IndexedDB.deleteDatabase(dbName)
    await new Promise((r) => setTimeout(r, 50))
    a.close()
    await deleting
    const again = await IndexedDB.open({ dbName })
    openHandles.push(again)
    expect(await again.get('k')).toBeNull()
  })

  it('rejects when a connection stays open past blockedMs', async () => {
    const dbName = uniqueDbName()
    const a = await IndexedDB.open({ dbName })
    openHandles.push(a)
    await expect(IndexedDB.deleteDatabase(dbName, { blockedMs: 100 })).rejects.toThrow(
      /kept it open for 100 ms/,
    )
  })
})
