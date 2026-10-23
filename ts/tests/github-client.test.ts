import { describe, expect, it } from 'vitest'
import { GithubClient, GithubError } from '../src/github/index'

interface Scripted {
  status: number
  body?: unknown
  headers?: Record<string, string>
}

interface Harness {
  client: GithubClient
  calls: Array<{ method: string; url: string; body: unknown; headers: Record<string, string> }>
  sleeps: number[]
}

/** Client wired to a scripted fetch, a recording sleeper, and a clock
 *  that only advances via sleeps — fully deterministic. */
function makeClient(
  script: Scripted[],
  opts: { writeIntervalMs?: number; maxRetries?: number } = {},
): Harness {
  const calls: Harness['calls'] = []
  const sleeps: number[] = []
  let clock = 1_000_000

  const fetchImpl = (async (url: RequestInfo | URL, init?: RequestInit) => {
    const headers: Record<string, string> = {}
    for (const [k, v] of Object.entries(init?.headers ?? {})) headers[k.toLowerCase()] = v as string
    calls.push({
      method: init?.method ?? 'GET',
      url: String(url),
      body: init?.body !== undefined ? JSON.parse(init.body as string) : undefined,
      headers,
    })
    const next = script.shift() ?? { status: 500, body: { message: 'script exhausted' } }
    return new Response(JSON.stringify(next.body ?? {}), {
      status: next.status,
      headers: next.headers ?? {},
    })
  }) as typeof fetch

  const client = new GithubClient({
    token: 'tok',
    repo: 'o/r',
    fetchImpl,
    retryBaseMs: 100,
    sleeper: async (ms) => {
      sleeps.push(ms)
      clock += ms
    },
    now: () => clock,
    ...(opts.writeIntervalMs !== undefined && { writeIntervalMs: opts.writeIntervalMs }),
    ...(opts.maxRetries !== undefined && { maxRetries: opts.maxRetries }),
  })
  return { client, calls, sleeps }
}

describe('request shaping', () => {
  it('sends auth, accept, and api-version headers to the repo-scoped URL', async () => {
    const { client, calls } = makeClient([{ status: 200, body: { ok: true } }])
    await client.request('GET', 'git/ref/heads/main')
    const call = calls[0]
    expect(call?.url).toBe('https://api.github.com/repos/o/r/git/ref/heads/main')
    expect(call?.headers.authorization).toBe('Bearer tok')
    expect(call?.headers.accept).toBe('application/vnd.github+json')
    expect(call?.headers['x-github-api-version']).toBe('2022-11-28')
  })

  it('serializes mutations with the write interval; reads are unthrottled', async () => {
    const { client, sleeps } = makeClient(
      [
        { status: 201, body: { sha: 'a' } },
        { status: 200, body: { data: {} } },
        { status: 200, body: {} },
        { status: 200, body: { data: {} } },
      ],
      { writeIntervalMs: 750 },
    )
    await client.request('POST', 'git/blobs', {})
    await client.graphql('mutation { a }', {}, { mutation: true })
    // Exactly one throttle wait, of the full interval (clock advances
    // only via sleeps, so the second write sees zero elapsed time).
    expect(sleeps).toEqual([750])
    await client.request('GET', 'git/trees/x')
    await client.graphql('query { b }')
    expect(sleeps).toEqual([750]) // a GET and a query added no waits
  })
})

describe('retry and taxonomy', () => {
  it('retries 5xx with exponential backoff, then succeeds', async () => {
    const { client, calls, sleeps } = makeClient([
      { status: 502, body: { message: 'bad gateway' } },
      { status: 502, body: { message: 'bad gateway' } },
      { status: 200, body: { object: { sha: 'abc' } } },
    ])
    expect(await client.request('GET', 'git/ref/heads/main')).toEqual({ object: { sha: 'abc' } })
    expect(calls.length).toBe(3)
    expect(sleeps).toEqual([100, 200])
  })

  it('honors Retry-After on secondary rate limits', async () => {
    const { client, sleeps } = makeClient([
      {
        status: 403,
        body: { message: 'You have exceeded a secondary rate limit.' },
        headers: { 'retry-after': '2' },
      },
      { status: 200, body: { object: { sha: 'abc' } } },
    ])
    expect(await client.request('GET', 'git/ref/heads/main')).toEqual({ object: { sha: 'abc' } })
    expect(sleeps).toEqual([2000])
  })

  it('never retries auth/permission/validation failures', async () => {
    for (const [status, body, kind] of [
      [401, { message: 'Bad credentials' }, 'auth'],
      [403, { message: 'Resource not accessible by personal access token' }, 'permission'],
      [422, { message: 'Validation Failed' }, 'validation'],
    ] as const) {
      const { client, calls } = makeClient([{ status, body }])
      const err = await client.request('POST', 'git/blobs', {}).catch((e) => e)
      expect(err).toBeInstanceOf(GithubError)
      expect((err as GithubError).kind).toBe(kind)
      expect(calls.length).toBe(1)
    }
  })

  it('gives up after maxRetries on persistent 5xx', async () => {
    const { client, calls } = makeClient(
      [
        { status: 500, body: { message: 'boom' } },
        { status: 500, body: { message: 'boom' } },
        { status: 500, body: { message: 'boom' } },
      ],
      { maxRetries: 2 },
    )
    const err = await client.request('GET', 'x').catch((e) => e)
    expect((err as GithubError).kind).toBe('server')
    expect(calls.length).toBe(3)
  })
})

describe('graphql and raw blobs', () => {
  it('posts a GraphQL document and returns its data beside its errors', async () => {
    const { client, calls } = makeClient([
      { status: 200, body: { data: { a: 1 }, errors: [{ type: 'NOT_FOUND', message: 'x' }] } },
    ])
    const res = await client.graphql<{ a: number }>('query($v: Int) { a }', { v: 2 })
    expect(res).toEqual({ data: { a: 1 }, errors: [{ type: 'NOT_FOUND', message: 'x' }] })
    expect(calls[0]?.url).toBe('https://api.github.com/graphql')
    expect(calls[0]?.body).toEqual({ query: 'query($v: Int) { a }', variables: { v: 2 } })
  })

  it('reads a blob raw by its SHA, and null when the repo lacks it', async () => {
    const { client, calls } = makeClient([
      { status: 200, body: 'raw' },
      { status: 404, body: { message: 'Not Found' } },
    ])
    const bytes = await client.getBlobRaw('a'.repeat(40))
    expect(new TextDecoder().decode(bytes as Uint8Array)).toBe('"raw"')
    expect(calls[0]?.url).toBe(`https://api.github.com/repos/o/r/git/blobs/${'a'.repeat(40)}`)
    expect(calls[0]?.headers.accept).toBe('application/vnd.github.raw+json')
    expect(await client.getBlobRaw('b'.repeat(40))).toBeNull()
  })
})
