/**
 * The fake GitHub's REST git-data writes, object lookups and simulated
 * uplink, through the real client: what `PackRemote`'s REST path leans
 * on, answered as GitHub answers it.
 */

import { describe, expect, it } from 'vitest'
import { FakeGithub } from '../src/github/fake'
import { type GithubError, gitBlobSha1 } from '../src/github/index'

const enc = new TextEncoder()
const dec = new TextDecoder()

const OBJECTS = `query($owner: String!, $name: String!, $a: GitObjectID!, $b: GitObjectID!) {
  repository(owner: $owner, name: $name) { a: object(oid: $a) { oid } b: object(oid: $b) { oid } }
}`

describe('the fake GitHub, as git objects over REST', () => {
  it('makes blobs, a tree on a base tree, a commit, and moves a ref with updateRefs', async () => {
    const gh = new FakeGithub()
    const client = gh.client()
    const blob = await client.createBlob(enc.encode('pack bytes'))
    expect(blob).toBe(await gitBlobSha1(enc.encode('pack bytes')))

    const main = gh.refs.get('refs/heads/main') as string
    const base = gh.commits.get(main)?.tree
    const tree = await client.request<{ sha: string }>('POST', 'git/trees', {
      base_tree: base,
      tree: [
        { path: 'kvgit/packs/1.pack', mode: '100644', type: 'blob', sha: blob },
        { path: 'kvgit/manifest.json', mode: '100644', type: 'blob', content: '{"format": 2}' },
      ],
    })
    const commit = await client.request<{ sha: string }>('POST', 'git/commits', {
      message: 'kvgit s @ abc\n\nKvgit-Head: abc',
      tree: tree.sha,
      parents: [main],
    })
    expect(gh.refs.has('refs/heads/s')).toBe(false) // nothing references it yet
    expect(gh.commits.get(commit.sha)?.tree).toBe(tree.sha)

    const res = await client.graphql(
      `mutation($repoId: ID!, $updates: [RefUpdate!]!) {
        updateRefs(input: {repositoryId: $repoId, refUpdates: $updates}) { clientMutationId }
      }`,
      {
        repoId: 'R_1',
        updates: [{ name: 'refs/heads/s', beforeOid: '0'.repeat(40), afterOid: commit.sha }],
      },
      { mutation: true },
    )
    expect(res.errors).toEqual([])
    expect(gh.filesAt('s')).toEqual(['README.md', 'kvgit/manifest.json', 'kvgit/packs/1.pack'])
    expect(gh.messageAt('s')).toMatch(/^kvgit s @ abc/)
    expect(gh.count('rest')).toBe(3)

    // a tree entry with sha null deletes the file from the base tree
    const next = await client.request<{ sha: string }>('POST', 'git/trees', {
      base_tree: tree.sha,
      tree: [{ path: 'kvgit/packs/1.pack', mode: '100644', type: 'blob', sha: null }],
    })
    expect([...(gh.trees.get(next.sha)?.keys() ?? [])].sort()).toEqual([
      'README.md',
      'kvgit/manifest.json',
    ])
  })

  it('refuses a tree naming a blob it lacks, and a commit on a tree it lacks', async () => {
    const client = new FakeGithub().client()
    for (const [path, body] of [
      ['git/trees', { tree: [{ path: 'x', mode: '100644', type: 'blob', sha: 'a'.repeat(40) }] }],
      ['git/commits', { message: 'm', tree: 'a'.repeat(40), parents: [] }],
    ] as const) {
      const err = await client.request('POST', path, body).catch((e) => e)
      expect((err as GithubError).kind).toBe('validation')
    }
  })

  it('answers object lookups with null for what it lacks, and prune forgets unreferenced blobs', async () => {
    const gh = new FakeGithub()
    const client = gh.client()
    const kept = await client.createBlob(enc.encode('kept'))
    const vars = { owner: 'o', name: 'r', a: kept, b: 'b'.repeat(40) }
    const before = await client.graphql<{ repository: Record<string, unknown> }>(OBJECTS, vars)
    expect(before.data?.repository).toEqual({ a: { oid: kept }, b: null })

    gh.plant('s', { 'x.txt': 'kept' }) // the same bytes, now committed
    const loose = await client.createBlob(enc.encode('loose'))
    await gh.prune()
    expect(gh.blobs.has(kept)).toBe(true)
    expect(gh.blobs.has(loose)).toBe(false)
  })
})

describe('the fake GitHub on a slow uplink', () => {
  it('drops a GraphQL body still arriving at 5 s with a 499, and applies nothing', async () => {
    const gh = new FakeGithub({ uplinkBytesPerSecond: 20_000 })
    const client = gh.client()
    const t0 = gh.now()
    const err = await client
      .graphql('query($pad: String!) { a }', { pad: 'x'.repeat(150_000) })
      .catch((e) => e)
    expect((err as GithubError).kind).toBe('too-large')
    expect((err as GithubError).status).toBe(499)
    expect(gh.now() - t0).toBe(5_000)
  })

  it('answers a REST body still arriving at a minute 401, which the client reads as too-large', async () => {
    const gh = new FakeGithub({ uplinkBytesPerSecond: 20_000 })
    const client = gh.client()
    const big = new Uint8Array(1_000_000) // 1.33 MB as base64: 67 s at 20 kB/s
    const err = await client.createBlob(big).catch((e) => e)
    expect((err as GithubError).kind).toBe('too-large')
    expect((err as GithubError).status).toBe(401)
    expect(gh.blobs.size).toBe(1) // the README only

    // half of it arrives in time
    const sha = await client.createBlob(big.subarray(0, 500_000))
    expect(gh.blobs.has(sha)).toBe(true)
    expect(dec.decode(gh.blobs.get(sha)?.subarray(0, 1))).toBe('\0')
  })
})
