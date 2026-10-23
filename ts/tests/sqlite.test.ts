import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { Sqlite } from '../src/backends/sqlite'
import { runConformance } from './kv-conformance'

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

const openHandles: Sqlite[] = []
const tempDirs: string[] = []

afterEach(async () => {
  for (const h of openHandles) {
    try {
      h.close()
    } catch {
      // already closed by the test
    }
  }
  openHandles.length = 0
  for (const dir of tempDirs) {
    await rm(dir, { recursive: true, force: true })
  }
  tempDirs.length = 0
})

async function freshTempPath(name = 'kvgit.db'): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'kvgit-sqlite-'))
  tempDirs.push(dir)
  return join(dir, name)
}

// --- Conformance against in-memory SQLite ---

runConformance('Sqlite (:memory:)', async () => {
  const db = await Sqlite.open()
  openHandles.push(db)
  return db
})

// --- SQLite-specific behaviors ---

describe('Sqlite — file persistence', () => {
  it('persists values across close + reopen of the same file', async () => {
    const path = await freshTempPath()
    const a = await Sqlite.open({ path })
    await a.set('k', bytes('v'))
    a.close()

    const b = await Sqlite.open({ path })
    openHandles.push(b)
    const got = await b.get('k')
    expect(got).not.toBeNull()
    expect(new TextDecoder().decode(got as Uint8Array)).toBe('v')
  })

  it('different file paths are isolated databases', async () => {
    const aPath = await freshTempPath('a.db')
    const bPath = await freshTempPath('b.db')
    const a = await Sqlite.open({ path: aPath })
    const b = await Sqlite.open({ path: bPath })
    openHandles.push(a, b)
    await a.set('k', bytes('a-value'))
    await b.set('k', bytes('b-value'))
    expect(new TextDecoder().decode((await a.get('k')) as Uint8Array)).toBe('a-value')
    expect(new TextDecoder().decode((await b.get('k')) as Uint8Array)).toBe('b-value')
  })
})

describe('Sqlite — concurrent CAS via WAL', () => {
  it('two handles on the same file linearize CAS correctly', async () => {
    // SQLite uses a single-writer model regardless of journal mode;
    // WAL just lets readers run concurrently with the writer. CAS
    // statements (INSERT OR IGNORE / conditional UPDATE) are atomic
    // single-statement forms, so two concurrent handles racing to
    // claim the same null-key must linearize: exactly one wins.
    const path = await freshTempPath()
    const a = await Sqlite.open({ path })
    const b = await Sqlite.open({ path })
    openHandles.push(a, b)

    const [r1, r2] = await Promise.all([a.cas('k', bytes('a'), null), b.cas('k', bytes('b'), null)])
    expect(r1 !== r2).toBe(true)
    const stored = new TextDecoder().decode((await a.get('k')) as Uint8Array)
    expect(stored === 'a' || stored === 'b').toBe(true)
  })
})

describe('Sqlite — bulk ops are transactional', () => {
  it('setMany commits atomically', async () => {
    const db = await Sqlite.open()
    openHandles.push(db)
    const items: Array<[string, Uint8Array]> = []
    for (let i = 0; i < 100; i++) items.push([`k${i}`, bytes(`v${i}`)])
    await db.setMany(items)
    for (let i = 0; i < 100; i++) {
      expect(new TextDecoder().decode((await db.get(`k${i}`)) as Uint8Array)).toBe(`v${i}`)
    }
  })

  it('removeMany commits atomically', async () => {
    const db = await Sqlite.open()
    openHandles.push(db)
    const items: Array<[string, Uint8Array]> = []
    for (let i = 0; i < 50; i++) items.push([`k${i}`, bytes(`v${i}`)])
    await db.setMany(items)
    await db.removeMany(['k0', 'k1', 'k2'])
    expect(await db.get('k0')).toBeNull()
    expect(await db.get('k1')).toBeNull()
    expect(await db.get('k2')).toBeNull()
    expect(new TextDecoder().decode((await db.get('k3')) as Uint8Array)).toBe('v3')
  })
})

describe('Sqlite — end-to-end VersionedKV over Sqlite (file-backed)', () => {
  it('VersionedKV state survives close + reopen', async () => {
    const { VersionedKV } = await import('../src/versioned/kv')
    const { Staged } = await import('../src/staged')

    const path = await freshTempPath()
    const db1 = await Sqlite.open({ path })
    openHandles.push(db1)

    const vk1 = await VersionedKV.open(db1)
    const s1 = new Staged(vk1)
    s1.set('greeting', 'hello sqlite')
    await s1.commit({ info: { author: 'a' } })
    const head = vk1.currentCommit
    db1.close()
    openHandles.length = 0

    const db2 = await Sqlite.open({ path })
    openHandles.push(db2)
    const vk2 = await VersionedKV.open(db2)
    expect(vk2.currentCommit).toBe(head)
    const s2 = new Staged(vk2)
    expect(await s2.get<string>('greeting')).toBe('hello sqlite')
    expect(await vk2.commitInfo()).toEqual({ author: 'a' })
  })
})
