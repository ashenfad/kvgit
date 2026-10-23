/**
 * A {@link FakeGithub} over HTTP, for Node: a local stand-in for the
 * GitHub API that a browser can sync against, through a
 * `GithubClient` whose `apiBase` is the server's URL. It answers every
 * origin's cross-origin requests, as api.github.com does for a token,
 * and keeps everything in memory, gone when it closes.
 *
 *     const server = await serveFakeGithub({ repo: 'me/sync' })
 *     new GithubClient({ token: 'any', repo: 'me/sync', apiBase: server.url })
 *     server.github.failNext({ status: 502 })   // inject a failure
 *     await server.close()
 */

import { type IncomingMessage, type ServerResponse, createServer } from 'node:http'
import { FakeGithub, type FakeGithubOptions } from './fake'

export interface FakeGithubServerOptions extends FakeGithubOptions {
  /** The port to listen on. Default 0: any free one. */
  port?: number
  /** The interface to listen on. Default `127.0.0.1`. */
  host?: string
  /** The fake to serve, if one exists already. */
  github?: FakeGithub
}

export interface FakeGithubServer {
  /** Its root URL, a `GithubClient`'s `apiBase`. */
  url: string
  /** The fake it serves: its refs, its calls, and `failNext`. */
  github: FakeGithub
  close(): Promise<void>
}

/** The headers every answer carries, so a page of any origin may read it. */
function cors(req: IncomingMessage): Record<string, string> {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, POST, PATCH, PUT, DELETE, OPTIONS',
    'access-control-allow-headers':
      req.headers['access-control-request-headers'] ?? 'authorization, content-type, accept',
    'access-control-expose-headers': 'retry-after, x-ratelimit-reset, x-ratelimit-remaining',
    'access-control-max-age': '600',
  }
}

/** Serve a fake GitHub on a local port until `close()`. */
export async function serveFakeGithub(
  opts: FakeGithubServerOptions = {},
): Promise<FakeGithubServer> {
  const github =
    opts.github ??
    new FakeGithub({
      ...(opts.repo !== undefined && { repo: opts.repo }),
      ...(opts.maxBodyBytes !== undefined && { maxBodyBytes: opts.maxBodyBytes }),
    })
  const host = opts.host ?? '127.0.0.1'
  const server = createServer((req, res) => {
    void answer(github, req, res).catch((err) => {
      if (!res.headersSent) res.writeHead(500, { ...cors(req), 'content-type': 'application/json' })
      res.end(JSON.stringify({ message: err instanceof Error ? err.message : String(err) }))
    })
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(opts.port ?? 0, host, () => resolve())
  })
  const address = server.address()
  if (address === null || typeof address === 'string')
    throw new Error('the fake GitHub has no port')
  return {
    // an IPv6 literal goes in brackets in a URL
    url: `http://${host.includes(':') ? `[${host}]` : host}:${address.port}`,
    github,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.closeAllConnections()
        server.close((err) => (err ? reject(err) : resolve()))
      }),
  }
}

async function answer(
  github: FakeGithub,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, cors(req))
    res.end()
    return
  }
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk as Buffer)
  const body = chunks.length > 0 ? Buffer.concat(chunks).toString('utf8') : undefined
  const response = await github.fetch(
    `http://${req.headers.host ?? 'localhost'}${req.url ?? '/'}`,
    {
      method: req.method ?? 'GET',
      ...(body !== undefined && { body }),
    },
  )
  const headers: Record<string, string> = { ...cors(req) }
  response.headers.forEach((value, key) => {
    headers[key] = value
  })
  res.writeHead(response.status, headers)
  res.end(Buffer.from(await response.arrayBuffer()))
}
