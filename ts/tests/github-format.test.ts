/**
 * GitHub sync format 2, against the golden fixture in
 * tests/fixtures/github-format-2.json: a manifest, the pack it lists
 * (one value kept outside it, as a blob of its own), and the state the
 * commits it carries decode to. The reader decodes the stored pack; the
 * writer reproduces its CBOR byte for byte (gzip output may differ by
 * engine, so the stored bytes aren't compared).
 *
 * The fixture is made by this file:
 *
 *   KVGIT_WRITE_GITHUB_FIXTURE=1 npx vitest run tests/github-format.test.ts
 */

import { readFileSync, writeFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { FakeGithub } from '../src/github/fake'
import {
  MANIFEST_PATH,
  type PackManifest,
  PackRemote,
  type ValueRef,
  decodePack,
  encodePack,
  gitBlobSha1,
} from '../src/github/index'
import { gzip } from '../src/github/pack'
import { Repo, applyWire, pullBranch, walkDelta } from '../src/index'
import type { WireCommit } from '../src/types'
import { sha256Hex } from '../src/versioned/layout'

interface Fixture {
  description: string
  branch: string
  /** The commits the pack carries, parents first. */
  commits: Array<{ hash: string; parents: string[] }>
  /** Every key and value (base64) at the head. */
  state: Record<string, string>
  manifest: PackManifest
  /** The pack as stored (gzip), base64. */
  pack: string
  /** Its CBOR before gzip, which a writer reproduces exactly, base64. */
  packCbor: string
  /** Blobs outside the pack (the value's part), git SHA → base64. */
  blobs: Record<string, string>
}

const FIXTURE = new URL('./fixtures/github-format-2.json', import.meta.url)
const enc = new TextEncoder()
const b64 = (bytes: Uint8Array): string => Buffer.from(bytes).toString('base64')
const unb64 = (text: string): Uint8Array => new Uint8Array(Buffer.from(text, 'base64'))

async function collect(iter: AsyncIterable<WireCommit>): Promise<WireCommit[]> {
  const out: WireCommit[] = []
  for await (const wc of iter) out.push(wc)
  return out
}

async function stateOf(store: Memory, branch: string): Promise<Record<string, string>> {
  const wt = await (await Repo.open(store)).worktree(branch)
  const out: Record<string, string> = {}
  for (const key of (await wt.keys()).sort()) out[key] = b64((await wt.get(key)) as Uint8Array)
  return out
}

/** The value the fixture keeps outside its pack. */
const PHOTO = Uint8Array.from({ length: 96 }, (_, i) => (i * 37 + 11) % 256)

async function refsFor(value: Uint8Array): Promise<Map<string, ValueRef>> {
  const sha = await sha256Hex(value)
  return new Map([[sha, [sha, value.length, [await gitBlobSha1(value)]]]])
}

async function build(): Promise<Fixture> {
  let clock = 1_700_000_000.25
  const branch = 'golden'
  const store = new Memory()
  const wt = await (await Repo.open(store, { now: () => clock })).worktree(branch, {
    create: true,
  })
  wt.set('notes/hello.txt', enc.encode('hello, world\n'))
  wt.set('héllo/ünïcode', enc.encode('ü'))
  await wt.commit({ info: { tool: 'file_write', t: 1.5, nested: { list: [1, 'x', null] } } })
  clock += 1.5
  wt.set('bin/blob', Uint8Array.from([0, 255, 1, 254, 0x1f, 0x8b]))
  wt.set('notes/copy.txt', enc.encode('hello, world\n')) // a value repeated: one slot
  wt.set('media/photo.png', PHOTO) // kept outside the pack
  await wt.commit()
  clock += 2
  await wt.delete('héllo/ünïcode')
  wt.set('notes/hello.txt', enc.encode('hello again\n'))
  await wt.commit({ info: { tool: 'edit' } })

  const commits = await collect(walkDelta(store, { want: wt.head }))
  const refs = await refsFor(PHOTO)
  const packCbor = await encodePack(commits, { kernel: 'ts', base: null, compress: false, refs })
  const pack = await gzip(packCbor)
  const manifest: PackManifest = {
    format: 2,
    kernel: 'ts',
    branch,
    head: wt.head,
    packs: [
      {
        path: `kvgit/packs/000001-${wt.head.slice(0, 12)}.pack`,
        seq: 1,
        base: null,
        tip: wt.head,
        commits: commits.length,
        bytes: pack.length,
        sha256: await sha256Hex(pack),
        parts: [await gitBlobSha1(pack)],
      },
    ],
    nextSeq: 2,
    meta: { title: 'Golden' },
  }
  return {
    description:
      'A format-2 branch: its manifest, the one pack it lists, and the blob of the value the pack keeps outside it. Decoding the pack (resolving the value) and applying its commits gives `state` at `manifest.head`; encoding those commits, with the same value kept out, gives `packCbor` exactly.',
    branch,
    commits: commits.map((c) => ({ hash: c.hash, parents: [...c.parents] })),
    state: await stateOf(store, branch),
    manifest,
    pack: b64(pack),
    packCbor: b64(packCbor),
    blobs: { [await gitBlobSha1(PHOTO)]: b64(PHOTO) },
  }
}

if (process.env.KVGIT_WRITE_GITHUB_FIXTURE) {
  writeFileSync(FIXTURE, `${JSON.stringify(await build(), null, 2)}\n`)
}

const golden = JSON.parse(readFileSync(FIXTURE, 'utf8')) as Fixture
const blobs = new Map(Object.entries(golden.blobs).map(([sha, text]) => [sha, unb64(text)]))
const resolve = async ([, , parts]: ValueRef): Promise<Uint8Array> => {
  const [only] = parts.map((sha) => blobs.get(sha) as Uint8Array)
  return only as Uint8Array
}

describe('GitHub sync format 2, golden', () => {
  it('the manifest names the pack by its size, SHA-256 and git blob SHA', async () => {
    const pack = unb64(golden.pack)
    const [entry] = golden.manifest.packs
    expect(golden.manifest.format).toBe(2)
    expect(entry).toMatchObject({
      bytes: pack.length,
      sha256: await sha256Hex(pack),
      parts: [await gitBlobSha1(pack)],
      base: null,
      tip: golden.manifest.head,
    })
    for (const [sha, bytes] of blobs) expect(await gitBlobSha1(bytes)).toBe(sha)
  })

  it('a reader decodes the stored pack to the commits, and they apply to the state', async () => {
    const pack = await decodePack(unb64(golden.pack), resolve)
    expect(pack).toMatchObject({ kernel: 'ts', base: null, tip: golden.manifest.head })
    expect(pack.commits.map((c) => ({ hash: c.hash, parents: [...c.parents] }))).toEqual(
      golden.commits,
    )
    const store = new Memory()
    const applied = await applyWire(store, pack.commits, { createBranch: golden.branch })
    expect(applied.head).toBe(golden.manifest.head)
    expect(await stateOf(store, golden.branch)).toEqual(golden.state)
  })

  it('a writer encodes those commits to the same CBOR', async () => {
    const store = new Memory()
    const pack = await decodePack(unb64(golden.pack), resolve)
    await applyWire(store, pack.commits, { createBranch: golden.branch })
    const commits = await collect(walkDelta(store, { want: golden.manifest.head }))
    const cbor = await encodePack(commits, {
      kernel: 'ts',
      base: null,
      compress: false,
      refs: await refsFor(PHOTO),
    })
    expect(b64(cbor)).toBe(golden.packCbor)
  })

  it('PackRemote pulls the branch from a repo holding these files', async () => {
    const gh = new FakeGithub()
    const [entry] = golden.manifest.packs
    gh.plant(golden.branch, {
      [MANIFEST_PATH]: JSON.stringify(golden.manifest),
      [entry?.path as string]: unb64(golden.pack),
      [`kvgit/values/${await sha256Hex(PHOTO)}`]: PHOTO,
    })
    const store = new Memory()
    const pulled = await pullBranch(store, new PackRemote(gh.client(), store), golden.branch)
    expect(pulled.localHead).toBe(golden.manifest.head)
    expect(await stateOf(store, golden.branch)).toEqual(golden.state)
  })
})
