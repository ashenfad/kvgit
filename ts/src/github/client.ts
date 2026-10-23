/**
 * Throttled, retrying GitHub client scoped to one repo: GraphQL, raw
 * blob reads and REST requests.
 *
 * Three concerns live here so the transport above stays mapping
 * logic:
 *
 * 1. **Mutation throttling.** GitHub's secondary rate limits cap
 *    content-creating requests (documented ~80/min) and dislike
 *    concurrent writes. All non-GET requests are serialized through
 *    one chain with a minimum spacing (`writeIntervalMs`, default
 *    750ms ≈ 80/min). Reads are not throttled (primary limit is
 *    5,000/hr with a PAT).
 * 2. **Retry/backoff.** `server` and `rate-limit` failures (and
 *    network errors) retry up to `maxRetries` with exponential
 *    backoff, honoring `Retry-After` / `x-ratelimit-reset` when
 *    present. `auth` / `permission` / `validation` never retry.
 * 3. **Error taxonomy.** Failures surface as `GithubError` with a
 *    `kind` callers can branch on (see `errors.ts`).
 */

import { GithubError, classify, errorMessage } from './errors'

export interface GithubClientOptions {
  /** PAT with contents read/write on the repo (fine-grained preferred). */
  token: string
  /** `owner/name`. */
  repo: string
  apiBase?: string
  /** Minimum ms between mutating requests. Default 750 (~80/min). */
  writeIntervalMs?: number
  /** Retries for retryable failures. Default 3. */
  maxRetries?: number
  /** Base backoff ms, doubling per attempt. Default 1000. */
  retryBaseMs?: number
  /** Injection seams (tests): fetch, sleep, clock. */
  fetchImpl?: typeof fetch
  sleeper?: (ms: number) => Promise<void>
  now?: () => number
}

/** One entry of a GraphQL response's `errors`. `type` is GitHub's
 *  classification (`STALE_DATA` for a lost `expectedHeadOid` race,
 *  `UNPROCESSABLE`, `NOT_FOUND`, …); `path` names the field. */
export interface GraphqlError {
  type?: string
  path?: Array<string | number>
  message: string
}

export interface GraphqlResult<T> {
  data: T | null
  errors: GraphqlError[]
}

const MAX_BACKOFF_MS = 60_000

const defaultSleeper = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

export class GithubClient {
  readonly repo: string
  /** The API's root URL (`https://api.github.com` unless set). */
  readonly apiBase: string
  readonly #token: string
  readonly #apiBase: string
  readonly #writeIntervalMs: number
  readonly #maxRetries: number
  readonly #retryBaseMs: number
  readonly #fetch: typeof fetch
  readonly #sleep: (ms: number) => Promise<void>
  readonly #now: () => number

  #writeChain: Promise<unknown> = Promise.resolve()
  #lastWriteAt = Number.NEGATIVE_INFINITY

  constructor(opts: GithubClientOptions) {
    this.repo = opts.repo
    this.#token = opts.token
    this.#apiBase = opts.apiBase ?? 'https://api.github.com'
    this.apiBase = this.#apiBase
    this.#writeIntervalMs = opts.writeIntervalMs ?? 750
    this.#maxRetries = opts.maxRetries ?? 3
    this.#retryBaseMs = opts.retryBaseMs ?? 1000
    this.#fetch = opts.fetchImpl ?? globalThis.fetch.bind(globalThis)
    this.#sleep = opts.sleeper ?? defaultSleeper
    this.#now = opts.now ?? Date.now
  }

  // -------------------------------------------------------------------------
  // Transport core
  // -------------------------------------------------------------------------

  /**
   * Issue one request against the repo (`path` is relative to
   * `/repos/<owner>/<name>/`). GETs run immediately and concurrently;
   * mutations are serialized and spaced by `writeIntervalMs`.
   */
  async request<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
    const url = `${this.#apiBase}/repos/${this.repo}/${path}`
    if (method === 'GET') return this.#attempt<T>(method, url, body)
    return this.#serialized(() => this.#attempt<T>(method, url, body))
  }

  /** Run a mutation on the write chain: one at a time, spaced by
   *  `writeIntervalMs`. */
  #serialized<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.#writeChain.then(async () => {
      const wait = this.#writeIntervalMs - (this.#now() - this.#lastWriteAt)
      if (wait > 0) await this.#sleep(wait)
      try {
        return await fn()
      } finally {
        this.#lastWriteAt = this.#now()
      }
    })
    // Keep the chain alive through failures; errors surface via `run`.
    this.#writeChain = run.catch(() => undefined)
    return run
  }

  /**
   * One GraphQL request. Queries run immediately; mutations
   * (`opts.mutation`) go through the write chain like REST writes.
   * HTTP-level failures throw `GithubError` (retrying the retryable
   * ones); GraphQL errors arrive with a 200 and are returned beside
   * whatever data the request did produce, since a document with
   * several mutations can partly succeed.
   */
  async graphql<T = unknown>(
    query: string,
    variables: Record<string, unknown> = {},
    opts: { mutation?: boolean } = {},
  ): Promise<GraphqlResult<T>> {
    const url = `${this.#apiBase}/graphql`
    const send = () =>
      this.#attempt<{ data?: T | null; errors?: GraphqlError[] }>('POST', url, {
        query,
        variables,
      })
    const raw = opts.mutation === true ? await this.#serialized(send) : await send()
    return { data: raw?.data ?? null, errors: raw?.errors ?? [] }
  }

  /**
   * A blob's bytes by its git SHA, raw (no base64, up to 100MB), or
   * null when the repo doesn't have it. The blobs endpoint returns the
   * bytes as stored; the contents API does not always: it transcodes a
   * non-UTF-8 file it takes for text (both as JSON and raw), so a value
   * that is mostly text with a binary header comes back altered.
   */
  async getBlobRaw(sha: string): Promise<Uint8Array | null> {
    const url = `${this.#apiBase}/repos/${this.repo}/git/blobs/${sha}`
    try {
      return await this.#attempt<Uint8Array>('GET', url, undefined, 'raw')
    } catch (err) {
      if (err instanceof GithubError && err.kind === 'not-found') return null
      throw err
    }
  }

  async #attempt<T>(
    method: string,
    url: string,
    body?: unknown,
    mode: 'json' | 'raw' = 'json',
  ): Promise<T> {
    const init: RequestInit = {
      method,
      headers: {
        Accept: mode === 'raw' ? 'application/vnd.github.raw+json' : 'application/vnd.github+json',
        Authorization: `Bearer ${this.#token}`,
        'X-GitHub-Api-Version': '2022-11-28',
        ...(body !== undefined && { 'Content-Type': 'application/json' }),
      },
      ...(body !== undefined && { body: JSON.stringify(body) }),
    }

    for (let attempt = 0; ; attempt++) {
      let resp: Response
      try {
        resp = await this.#fetch(url, init)
      } catch (err) {
        if (attempt >= this.#maxRetries) {
          const detail = err instanceof Error ? err.message : String(err)
          throw new GithubError('network', 0, `Network error contacting GitHub: ${detail}`)
        }
        await this.#sleep(this.#backoff(attempt))
        continue
      }

      if (resp.ok) {
        if (resp.status === 204) return undefined as T
        if (mode === 'raw') return new Uint8Array(await resp.arrayBuffer()) as T
        return (await resp.json()) as T
      }

      const raw = await resp.text()
      const message = errorMessage(raw, resp.status)
      const kind = classify(resp.status, message, resp.headers)
      const retryable = kind === 'server' || kind === 'rate-limit'
      if (retryable && attempt < this.#maxRetries) {
        const hinted = retryAfterMs(resp.headers, this.#now)
        await this.#sleep(Math.min(hinted ?? this.#backoff(attempt), MAX_BACKOFF_MS))
        continue
      }
      throw new GithubError(kind, resp.status, message, raw)
    }
  }

  #backoff(attempt: number): number {
    return this.#retryBaseMs * 2 ** attempt
  }
}

/** Server-hinted retry delay: `Retry-After` (seconds) or
 *  `x-ratelimit-reset` (epoch seconds), whichever is present. */
function retryAfterMs(headers: Headers, now: () => number): number | null {
  const retryAfter = headers.get('retry-after')
  if (retryAfter !== null) {
    const seconds = Number(retryAfter)
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000)
  }
  const reset = headers.get('x-ratelimit-reset')
  if (reset !== null) {
    const epochSeconds = Number(reset)
    if (Number.isFinite(epochSeconds)) return Math.max(0, epochSeconds * 1000 - now())
  }
  return null
}
