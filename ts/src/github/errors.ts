/**
 * Error taxonomy for the GitHub transport.
 *
 * Callers branch on `kind`, not status codes — the orchestration and
 * UI layers need "is this a bad token vs. a rate limit vs. a real
 * validation failure", not HTTP trivia. The classification:
 *
 *   - `auth`        401 — token invalid / expired / missing scope
 *   - `rate-limit`  403/429 carrying rate-limit indicators (primary
 *                   or secondary) — retried after GitHub's wait; one
 *                   longer than a minute fails with `retryAfterMs`
 *   - `permission`  other 403 — token scope / repo access; NOT retryable
 *   - `not-found`   404 — repo or object missing (also: deleted refs)
 *   - `validation`  422 — payload rejected; NOT retryable (the CAS
 *                   "not a fast forward" 422 is handled by the client's
 *                   ref methods before this surfaces)
 *   - `too-large`   413, 499, or a large request that failed at GitHub's
 *                   body deadline — too big, or too slow to arrive
 *                   (GitHub drops a GraphQL request whose body hasn't
 *                   arrived within about 5 s, and answers a REST one
 *                   still arriving after about a minute with a 401);
 *                   NOT retryable: the same body fails the same way
 *   - `server`      5xx — GitHub-side; retryable
 *   - `network`     fetch itself failed; retryable
 */

export type GithubErrorKind =
  | 'auth'
  | 'rate-limit'
  | 'permission'
  | 'not-found'
  | 'validation'
  | 'too-large'
  | 'server'
  | 'network'

export class GithubError extends Error {
  override readonly name = 'GithubError'
  readonly kind: GithubErrorKind
  /** HTTP status, or 0 for network failures. */
  readonly status: number
  /** Raw response body, for debugging. */
  readonly body: string
  /** For `rate-limit`: how long GitHub asked to wait before the next
   *  request, in ms, when that is known. */
  readonly retryAfterMs: number | undefined

  constructor(
    kind: GithubErrorKind,
    status: number,
    message: string,
    body = '',
    retryAfterMs?: number,
  ) {
    super(message)
    this.kind = kind
    this.status = status
    this.body = body
    this.retryAfterMs = retryAfterMs
  }
}

/** Map an HTTP failure to a `GithubErrorKind`. */
export function classify(status: number, message: string, headers: Headers): GithubErrorKind {
  if (status === 401) return 'auth'
  if (status === 403 || status === 429) {
    const remaining = headers.get('x-ratelimit-remaining')
    if (remaining === '0' || /rate limit|secondary/i.test(message)) return 'rate-limit'
    return 'permission'
  }
  if (status === 404) return 'not-found'
  if (status === 413 || status === 499) return 'too-large'
  if (status === 422) return 'validation'
  if (status >= 500) return 'server'
  return 'validation'
}

/** A byte count as a person reads it: `512 kB`, `12.3 MB`. */
export function formatBytes(n: number): string {
  return n < 1e6 ? `${Math.ceil(n / 1e3)} kB` : `${(n / 1e6).toFixed(1)} MB`
}

/** Parse GitHub's `{"message": ...}` error body; fall back to raw text. */
export function errorMessage(body: string, status: number): string {
  try {
    const parsed = JSON.parse(body) as { message?: unknown } | null
    if (
      parsed !== null &&
      typeof parsed === 'object' &&
      typeof parsed.message === 'string' &&
      parsed.message.length > 0
    ) {
      return parsed.message
    }
  } catch {
    // non-JSON or null-traversal — fall through to the raw fallback
  }
  return body.slice(0, 200) || `HTTP ${status}`
}
