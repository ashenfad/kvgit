import { describe, expect, it } from 'vitest'
import { GithubClient, GithubError } from '../src/github/index'

interface Scripted {
  status: number
  body?: unknown
  headers?: Record<string, string>
  /** How long the request takes, on the harness clock. */
  takesMs?: number
  /** Fail as fetch does on a network error (a browser seeing GitHub's
   *  499, which has no CORS headers), after `takesMs`. */
  throws?: boolean
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
    const next: Scripted = script.shift() ?? { status: 500, body: { message: 'script exhausted' } }
    clock += next.takesMs ?? 0
    if (next.throws === true) throw new TypeError('Failed to fetch')
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

describe('rate limits', () => {
  const secondary = { message: 'You have exceeded a secondary rate limit.' }

  it('waits a minute on a secondary limit that names no wait', async () => {
    const { client, sleeps } = makeClient([
      { status: 403, body: secondary },
      { status: 201, body: { sha: 'a' } },
    ])
    await client.request('POST', 'git/blobs', {})
    expect(sleeps).toEqual([60_000])
  })

  it('waits until the reset when the quota is spent', async () => {
    const { client, sleeps } = makeClient([
      {
        status: 403,
        body: { message: 'API rate limit exceeded' },
        // the harness clock starts at 1,000 s
        headers: { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': '1030' },
      },
      { status: 200, body: {} },
    ])
    await client.request('GET', 'x')
    expect(sleeps).toEqual([30_000])
  })

  it('fails a wait longer than a minute at once, carrying it', async () => {
    for (const headers of [
      { 'retry-after': '90' },
      { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': '1600' },
    ]) {
      const { client, calls, sleeps } = makeClient([{ status: 429, body: secondary, headers }])
      const err = await client.request('POST', 'git/blobs', {}).catch((e) => e)
      expect(err).toBeInstanceOf(GithubError)
      expect((err as GithubError).kind).toBe('rate-limit')
      expect((err as GithubError).retryAfterMs).toBeGreaterThanOrEqual(90_000)
      expect((err as GithubError).message).toMatch(/GitHub asks to wait \d+ s/)
      expect(calls.length).toBe(1)
      expect(sleeps).toEqual([])
    }
  })

  it('carries the wait when retries run out', async () => {
    const { client, sleeps } = makeClient(
      [
        { status: 403, body: secondary },
        { status: 403, body: secondary },
      ],
      { maxRetries: 1 },
    )
    const err = await client.request('POST', 'git/blobs', {}).catch((e) => e)
    expect((err as GithubError).kind).toBe('rate-limit')
    expect((err as GithubError).retryAfterMs).toBe(60_000)
    expect(sleeps).toEqual([60_000])
  })
})

describe('requests too large for GitHub', () => {
  const big = 'A'.repeat(200 * 1024)

  it('takes a large GraphQL request failing at the deadline for a drop: too-large, sent once', async () => {
    const { client, calls } = makeClient([{ status: 0, throws: true, takesMs: 5_500 }])
    const err = await client
      .graphql('mutation($b: String!) { a }', { b: big }, { mutation: true })
      .catch((e) => e)
    expect(err).toBeInstanceOf(GithubError)
    expect((err as GithubError).kind).toBe('too-large')
    expect((err as GithubError).message).toMatch(/205 kB request failed after 5\.5 s/)
    expect(calls.length).toBe(1)
  })

  it('measures the body in UTF-8 bytes, as sent', async () => {
    // 30,000 three-byte characters: 30k UTF-16 units, 90 kB on the wire
    const { client, calls } = makeClient([{ status: 0, throws: true, takesMs: 5_500 }])
    const err = await client
      .graphql('query($b: String!) { a }', { b: '漢'.repeat(30_000) })
      .catch((e) => e)
    expect((err as GithubError).kind).toBe('too-large')
    expect((err as GithubError).message).toMatch(/^A 91 kB request/)
    expect(calls.length).toBe(1)
  })

  it('retries a network error that is early, on a small body, or on REST', async () => {
    for (const [send, takesMs] of [
      [(c: GithubClient) => c.graphql('query($b: String!) { a }', { b: big }), 100],
      [(c: GithubClient) => c.graphql('query { a }'), 5_500],
      [(c: GithubClient) => c.request('POST', 'git/blobs', { content: big }), 5_500],
    ] as const) {
      const { client, calls } = makeClient([
        { status: 0, throws: true, takesMs },
        { status: 200, body: { data: { a: 1 } } },
      ])
      await send(client)
      expect(calls.length).toBe(2)
    }
  })

  it('takes a large REST write answered 401 a minute in for the deadline, not the token', async () => {
    for (const script of [
      { status: 401, body: { message: 'Bad credentials' }, takesMs: 56_000 },
      { status: 0, throws: true, takesMs: 56_000 },
    ]) {
      const { client, calls } = makeClient([script])
      const err = await client
        .request('POST', 'git/blobs', { content: big, encoding: 'base64' })
        .catch((e) => e)
      expect((err as GithubError).kind).toBe('too-large')
      expect((err as GithubError).message).toMatch(/205 kB request .*after 56\.0 s/)
      expect((err as GithubError).message).toMatch(/about a minute/)
      expect(calls.length).toBe(1)
    }
  })

  it('reads a 401 that is early or on a small body as a bad token', async () => {
    for (const [body, takesMs] of [
      [{ content: big }, 1_000],
      [{ content: 'small' }, 56_000],
    ] as const) {
      const { client } = makeClient([
        { status: 401, body: { message: 'Bad credentials' }, takesMs },
      ])
      const err = await client.request('POST', 'git/blobs', body).catch((e) => e)
      expect((err as GithubError).kind).toBe('auth')
    }
  })

  it('reads 499 and 413 as too-large, naming the size, and never retries them', async () => {
    for (const [status, pattern] of [
      [499, /GitHub dropped a 205 kB request after 6\.0 s/],
      [413, /GitHub refused a 205 kB request as too large/],
    ] as const) {
      const { client, calls } = makeClient([{ status, body: {}, takesMs: 6_000 }])
      const err = await client.graphql('query($b: String!) { a }', { b: big }).catch((e) => e)
      expect((err as GithubError).kind).toBe('too-large')
      expect((err as GithubError).status).toBe(status)
      expect((err as GithubError).message).toMatch(pattern)
      expect(calls.length).toBe(1)
    }
  })
})

describe('graphql and raw blobs', () => {
  it('creates a blob as base64 and returns its SHA, checked', async () => {
    const sha = 'b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0' // git hash-object of "hello"
    const { client, calls } = makeClient([
      { status: 201, body: { sha } },
      { status: 201, body: { sha: 'f'.repeat(40) } },
    ])
    const hello = new TextEncoder().encode('hello')
    expect(await client.createBlob(hello)).toBe(sha)
    expect(calls[0]?.url).toBe('https://api.github.com/repos/o/r/git/blobs')
    expect(calls[0]?.body).toEqual({ content: 'aGVsbG8=', encoding: 'base64' })
    const err = await client.createBlob(hello).catch((e) => e)
    expect((err as GithubError).kind).toBe('validation')
    expect((err as GithubError).message).toMatch(/not b6fc4c6/)
  })

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
