/**
 * The fake GitHub over HTTP: two devices sync a branch through it with
 * the real client and transport, its answers carry CORS headers, and
 * it answers injected failures and oversized requests as GitHub would.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { type FakeGithubServer, serveFakeGithub } from '../src/github/fake-server'
import { GithubClient, GithubError, PackRemote } from '../src/github/index'
import { Repo, pullBranch, pushBranch } from '../src/index'

const enc = new TextEncoder()
const dec = new TextDecoder()

let server: FakeGithubServer | null = null
afterEach(async () => {
  await server?.close()
  server = null
})

function device(s: FakeGithubServer, opts: { maxRetries?: number } = {}) {
  const store = new Memory()
  const client = new GithubClient({
    token: 'any',
    repo: s.github.repo,
    apiBase: s.url,
    writeIntervalMs: 0,
    maxRetries: opts.maxRetries ?? 0,
    retryBaseMs: 1,
  })
  return { store, remote: new PackRemote(client, store) }
}

async function write(store: Memory, key: string, value: string): Promise<string> {
  const wt = await (await Repo.open(store)).worktree('notes', { create: true })
  wt.set(key, enc.encode(value))
  await wt.commit()
  return wt.head
}

describe('the fake GitHub over HTTP', () => {
  it('carries a branch from one device to another', async () => {
    server = await serveFakeGithub({ repo: 'me/sync' })
    const a = device(server)
    const b = device(server)
    const head = await write(a.store, 'k', 'from a')
    await pushBranch(a.store, a.remote, 'notes')
    expect(server.github.filesAt('notes')).toContain('kvgit/manifest.json')

    await pullBranch(b.store, b.remote, 'notes')
    const wt = await (await Repo.open(b.store)).worktree('notes')
    expect(wt.head).toBe(head)
    expect(dec.decode((await wt.get('k')) as Uint8Array)).toBe('from a')
  })

  it('lets a page of any origin call it', async () => {
    server = await serveFakeGithub()
    const preflight = await fetch(`${server.url}/graphql`, {
      method: 'OPTIONS',
      headers: {
        origin: 'http://localhost:4173',
        'access-control-request-method': 'POST',
        'access-control-request-headers': 'authorization, content-type',
      },
    })
    expect(preflight.status).toBe(204)
    expect(preflight.headers.get('access-control-allow-origin')).toBe('*')
    expect(preflight.headers.get('access-control-allow-headers')).toBe(
      'authorization, content-type',
    )
    const missing = await fetch(`${server.url}/repos/o/r/git/blobs/${'0'.repeat(40)}`)
    expect(missing.status).toBe(404)
    expect(missing.headers.get('access-control-allow-origin')).toBe('*')
  })

  it('answers an injected failure in place of the next request', async () => {
    server = await serveFakeGithub()
    const a = device(server, { maxRetries: 1 })
    await write(a.store, 'k', 'v')
    // retried once, and through
    server.github.failNext({ status: 502, path: '/graphql' })
    await pushBranch(a.store, a.remote, 'notes')

    await write(a.store, 'k', 'w')
    server.github.failNext(
      {
        status: 403,
        body: { message: 'You have exceeded a secondary rate limit' },
        headers: { 'retry-after': '0' },
      },
      2,
    )
    const err = await pushBranch(a.store, a.remote, 'notes').catch((e) => e)
    expect(err).toBeInstanceOf(GithubError)
    expect((err as GithubError).kind).toBe('rate-limit')
  })

  it('refuses a request past its size limit', async () => {
    server = await serveFakeGithub({ maxBodyBytes: 2000 })
    const a = device(server)
    // random, so the pack can't compress it under the limit
    const noise = Array.from(crypto.getRandomValues(new Uint8Array(5000)), (b) =>
      b.toString(36),
    ).join('')
    await write(a.store, 'big', noise)
    const err = await pushBranch(a.store, a.remote, 'notes').catch((e) => e)
    expect(err).toBeInstanceOf(GithubError)
    expect((err as GithubError).status).toBe(413)
  })

  it('reads a Request as fetch does, and refuses another repo', async () => {
    server = await serveFakeGithub({ repo: 'me/sync' })
    const roster = (repo: string) =>
      server?.github.fetch(
        new Request(`${server.url}/graphql`, {
          method: 'POST',
          body: JSON.stringify({
            query:
              'query($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { refs(refPrefix: "refs/heads/") { nodes { name } } } }',
            variables: { owner: repo.split('/')[0], name: repo.split('/')[1] },
          }),
        }),
      )
    const ok = (await (await roster('Me/Sync'))?.json()) as { data: unknown }
    expect(ok.data).not.toBeNull()
    const other = (await (await roster('me/other'))?.json()) as {
      data: unknown
      errors: { type: string }[]
    }
    expect(other.data).toBeNull()
    expect(other.errors[0]?.type).toBe('NOT_FOUND')

    // a client configured for another repo fails at once, writing nothing
    const store = new Memory()
    await write(store, 'k', 'v')
    const client = new GithubClient({
      token: 'any',
      repo: 'me/other',
      apiBase: server.url,
      maxRetries: 0,
    })
    await expect(pushBranch(store, new PackRemote(client, store), 'notes')).rejects.toThrow()
    expect(server.github.filesAt('notes')).toEqual([])
  })

  it('gives an IPv6 host a URL a client can use', async () => {
    server = await serveFakeGithub({ host: '::1' })
    expect(server.url).toMatch(/^http:\/\/\[::1\]:\d+$/)
    expect((await fetch(`${server.url}/graphql`, { method: 'OPTIONS' })).status).toBe(204)
  })
})
