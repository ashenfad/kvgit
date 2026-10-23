#!/usr/bin/env node
/**
 * A fake GitHub on a local port, for trying sync without a real repo:
 * point a `GithubClient`'s `apiBase` (the studio's, in dev) at the URL
 * it prints. Everything is in memory, gone when it stops (Ctrl-C).
 *
 *   pnpm -F @agex-ts/kvgit build
 *   pnpm -F @agex-ts/kvgit fake-github -- --port 4180 --repo me/sync
 *
 * Options: --port (default 4180), --host (default 127.0.0.1), --repo
 * (default o/r: the repo name a client must use), --max-body (bytes; a
 * larger request is refused with 413, as GitHub refuses large ones).
 */

import { serveFakeGithub } from '../dist/github/fake-server.js'

const args = process.argv.slice(2)
const opt = (name, fallback) => {
  const i = args.indexOf(`--${name}`)
  return i >= 0 && i + 1 < args.length ? args[i + 1] : fallback
}
const maxBody = opt('max-body', undefined)
const server = await serveFakeGithub({
  port: Number(opt('port', '4180')),
  host: opt('host', '127.0.0.1'),
  repo: opt('repo', 'o/r'),
  ...(maxBody !== undefined && { maxBodyBytes: Number(maxBody) }),
})
console.log(`fake GitHub for ${server.github.repo} at ${server.url}`)
const stop = () => {
  server.close().then(() => process.exit(0))
}
process.on('SIGINT', stop)
process.on('SIGTERM', stop)
