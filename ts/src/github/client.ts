/**
 * Throttled, retrying GitHub client scoped to one repo: GraphQL, raw
 * blob reads and REST requests.
 *
 * These concerns live here so the transport above stays mapping
 * logic:
 *
 * 1. **Mutation throttling.** GitHub's secondary rate limits cap
 *    content-creating requests (documented ~80/min) and dislike
 *    concurrent writes. All non-GET requests are serialized through
 *    one chain with a minimum spacing (`writeIntervalMs`, default
 *    750ms ≈ 80/min). Reads are not throttled (primary limit is
 *    5,000/hr with a PAT).
 * 2. **Retry/backoff.** `server` failures and network errors retry up
 *    to `maxRetries` with exponential backoff. `auth` / `permission` /
 *    `validation` / `too-large` never retry.
 * 3. **Rate limits, as GitHub asks.** A `rate-limit` failure waits the
 *    whole `Retry-After`; without one, until `x-ratelimit-reset` when
 *    the quota is spent, and otherwise (a secondary limit) a minute.
 *    It never retries sooner. A wait longer than a minute isn't spent
 *    inside the request: it fails as `rate-limit` with `retryAfterMs`,
 *    for the caller to schedule.
 * 4. **Body deadlines.** GitHub drops a GraphQL request whose body
 *    hasn't fully arrived after about 5 s: Node sees a 499, a browser a
 *    network error (the 499 has no CORS headers). It answers a REST
 *    request whose body is still arriving after about a minute with a
 *    401 "Bad credentials". A large request failing either way is
 *    `too-large`, not retried, since the same body would fail again.
 * 5. **Error taxonomy.** Failures surface as `GithubError` with a
 *    `kind` callers can branch on (see `errors.ts`).
 */

import { bytesToBase64 } from './base64'
import { GithubError, classify, errorMessage, formatBytes } from './errors'
import { gitBlobSha1 } from './git-hash'

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
/** The longest wait spent inside one request. A rate limit asking for
 *  longer fails the request, with the wait in `retryAfterMs`. */
const MAX_WAIT_MS = 60_000
/** How long to wait on a secondary rate limit that names no wait:
 *  GitHub's guidance is at least a minute. */
const SECONDARY_WAIT_MS = 60_000
/** A body smaller than this that fails at its deadline is taken for a
 *  network fault, and retried. */
const SLOW_BODY_BYTES = 64 * 1024

/** How long GitHub waits for a request's body on one kind of route. */
interface BodyDeadline {
  /** A large request failing this long in is taken for one GitHub
   *  stopped waiting for: a little under the measured deadline. */
  ms: number
  /** GitHub answers this route's late body with 401 "Bad credentials",
   *  so a 401 that late on a large body is the deadline, not the token. */
  as401: boolean
  /** The deadline, as a person reads it. */
  says: string
}

/** GitHub drops a GraphQL request whose body hasn't fully arrived
 *  after about 5 s (measured: time, not size; 20 MiB landed in 1.6 s,
 *  600 KiB throttled was cut at 6.1 s). */
const GRAPHQL_DEADLINE: BodyDeadline = {
  ms: 4_500,
  as401: false,
  says: "GitHub drops a GraphQL request whose body hasn't arrived after about 5 s",
}
/** GitHub takes a REST body for at least 55 s (measured), and answers
 *  one still arriving at 68 s with 401 "Bad credentials". */
const REST_DEADLINE: BodyDeadline = {
  ms: 55_000,
  as401: true,
  says: "GitHub answers a REST request whose body hasn't arrived after about a minute as if its token were bad",
}

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
    return this.#serialized(() => this.#attempt<T>(method, url, body, 'json', REST_DEADLINE))
  }

  /**
   * Store `bytes` as a git blob (`POST git/blobs`, base64) and return
   * its SHA, checked against the one computed here. Nothing references
   * the blob until a tree names it.
   */
  async createBlob(bytes: Uint8Array): Promise<string> {
    const sha = await gitBlobSha1(bytes)
    const made = await this.request<{ sha?: unknown }>('POST', 'git/blobs', {
      content: bytesToBase64(bytes),
      encoding: 'base64',
    })
    if (made?.sha !== sha) {
      throw new GithubError(
        'validation',
        201,
        `GitHub stored a ${formatBytes(bytes.length)} blob as ${String(made?.sha)}, not ${sha}`,
      )
    }
    return sha
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
      this.#attempt<{ data?: T | null; errors?: GraphqlError[] }>(
        'POST',
        url,
        { query, variables },
        'json',
        GRAPHQL_DEADLINE,
      )
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

  /** One request, retried as its failure allows. `deadline`: how long
   *  GitHub waits for this route's body, past which a large request's
   *  failure is taken for GitHub having stopped waiting. */
  async #attempt<T>(
    method: string,
    url: string,
    body?: unknown,
    mode: 'json' | 'raw' = 'json',
    deadline?: BodyDeadline,
  ): Promise<T> {
    const payload = body === undefined ? undefined : JSON.stringify(body)
    const init: RequestInit = {
      method,
      headers: {
        Accept: mode === 'raw' ? 'application/vnd.github.raw+json' : 'application/vnd.github+json',
        Authorization: `Bearer ${this.#token}`,
        'X-GitHub-Api-Version': '2022-11-28',
        ...(payload !== undefined && { 'Content-Type': 'application/json' }),
      },
      ...(payload !== undefined && { body: payload }),
    }

    for (let attempt = 0; ; attempt++) {
      let resp: Response
      const started = this.#now()
      try {
        resp = await this.#fetch(url, init)
      } catch (err) {
        const elapsed = this.#now() - started
        const size = pastDeadline(deadline, elapsed, payload)
        if (size !== null) {
          throw new GithubError('too-large', 0, tooLargeMessage(0, size, elapsed, '', deadline))
        }
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
      const elapsed = this.#now() - started
      let kind = classify(resp.status, message, resp.headers)
      if (kind === 'auth' && deadline?.as401 === true) {
        if (pastDeadline(deadline, elapsed, payload) !== null) kind = 'too-large'
      }
      if (kind === 'server' && attempt < this.#maxRetries) {
        const hinted = hintedWaitMs(resp.headers, this.#now)
        await this.#sleep(Math.min(hinted ?? this.#backoff(attempt), MAX_BACKOFF_MS))
        continue
      }
      if (kind === 'rate-limit') {
        const wait = rateLimitWaitMs(resp.headers, this.#now)
        if (wait <= MAX_WAIT_MS && attempt < this.#maxRetries) {
          await this.#sleep(wait)
          continue
        }
        const asked = wait > MAX_WAIT_MS ? ` (GitHub asks to wait ${Math.ceil(wait / 1000)} s)` : ''
        throw new GithubError(kind, resp.status, `${message}${asked}`, raw, wait)
      }
      if (kind === 'too-large') {
        throw new GithubError(
          kind,
          resp.status,
          tooLargeMessage(resp.status, utf8Bytes(payload ?? ''), elapsed, message, deadline),
          raw,
        )
      }
      throw new GithubError(kind, resp.status, message, raw)
    }
  }

  #backoff(attempt: number): number {
    return this.#retryBaseMs * 2 ** attempt
  }
}

/** The bytes `text` sends as, in UTF-8 (fetch's encoding of a string body). */
const utf8Bytes = (text: string): number => new TextEncoder().encode(text).length

/** The body's size when a failure `elapsed` ms into a request is GitHub
 *  having stopped waiting for it: a body of `SLOW_BODY_BYTES` or more,
 *  past the route's deadline. Null otherwise. */
function pastDeadline(
  deadline: BodyDeadline | undefined,
  elapsed: number,
  payload: string | undefined,
): number | null {
  if (deadline === undefined || payload === undefined || elapsed < deadline.ms) return null
  const size = utf8Bytes(payload)
  return size >= SLOW_BODY_BYTES ? size : null
}

/** Why a request was too large, as a person reads it: refused for its
 *  size (413), dropped before its body arrived (499), answered 401 for
 *  a body still arriving (REST), or failed at the deadline with a
 *  network error (status 0). */
function tooLargeMessage(
  status: number,
  bytes: number,
  ms: number,
  message: string,
  deadline: BodyDeadline | undefined,
): string {
  const size = formatBytes(bytes)
  const after = `${(ms / 1000).toFixed(1)} s`
  if (status === 413) return `GitHub refused a ${size} request as too large: ${message}`
  const says = deadline?.says ?? 'GitHub stopped waiting for its body'
  if (status === 499) return `GitHub dropped a ${size} request after ${after}: ${says}`
  if (status === 401) return `GitHub answered a ${size} request with 401 after ${after}: ${says}`
  return `A ${size} request failed after ${after}, as when ${says}: too large to send in one request on this connection`
}

/** How long a rate limit asks to wait: the whole `Retry-After`; else,
 *  with the quota spent, until `x-ratelimit-reset`; else (a secondary
 *  limit naming no wait) a minute. */
function rateLimitWaitMs(headers: Headers, now: () => number): number {
  const retryAfter = Number(headers.get('retry-after') ?? Number.NaN)
  if (Number.isFinite(retryAfter)) return Math.max(0, retryAfter * 1000)
  const reset = Number(headers.get('x-ratelimit-reset') ?? Number.NaN)
  if (headers.get('x-ratelimit-remaining') === '0' && Number.isFinite(reset)) {
    return Math.max(0, reset * 1000 - now())
  }
  return SECONDARY_WAIT_MS
}

/** A server error's hinted retry delay: `Retry-After` (seconds) or
 *  `x-ratelimit-reset` (epoch seconds), whichever is present. */
function hintedWaitMs(headers: Headers, now: () => number): number | null {
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
