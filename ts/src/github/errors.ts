/**
 * Error taxonomy for the GitHub transport.
 *
 * Callers branch on `kind`, not status codes — the orchestration and
 * UI layers need "is this a bad token vs. a rate limit vs. a real
 * validation failure", not HTTP trivia. The classification:
 *
 *   - `auth`        401 — token invalid / expired / missing scope
 *   - `rate-limit`  403/429 carrying rate-limit indicators (primary
 *                   or secondary) — retryable with backoff
 *   - `permission`  other 403 — token scope / repo access; NOT retryable
 *   - `not-found`   404 — repo or object missing (also: deleted refs)
 *   - `validation`  422 — payload rejected; NOT retryable (the CAS
 *                   "not a fast forward" 422 is handled by the client's
 *                   ref methods before this surfaces)
 *   - `server`      5xx — GitHub-side; retryable
 *   - `network`     fetch itself failed; retryable
 */

export type GithubErrorKind =
  | 'auth'
  | 'rate-limit'
  | 'permission'
  | 'not-found'
  | 'validation'
  | 'server'
  | 'network'

export class GithubError extends Error {
  override readonly name = 'GithubError'
  readonly kind: GithubErrorKind
  /** HTTP status, or 0 for network failures. */
  readonly status: number
  /** Raw response body, for debugging. */
  readonly body: string

  constructor(kind: GithubErrorKind, status: number, message: string, body = '') {
    super(message)
    this.kind = kind
    this.status = status
    this.body = body
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
  if (status === 422) return 'validation'
  if (status >= 500) return 'server'
  return 'validation'
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
