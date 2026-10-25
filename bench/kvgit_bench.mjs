// The kvgit benchmarks (bench/README.md), run through the TypeScript
// package's public API on the in-memory backend. Build the package
// first; from the repository root:
//
//   (cd ts && pnpm build) && node bench/kvgit_bench.mjs [--quick] [--json PATH] [--only SHAPE]
//
// --quick shrinks every workload to check the script runs; its numbers
// mean nothing.

import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { writeFileSync } from 'node:fs'
import { cpus, release, type } from 'node:os'
import { parseArgs } from 'node:util'
import { Memory } from '../ts/dist/backends/memory.js'
import { Repo } from '../ts/dist/index.js'

const HOST = 'ts'

// --- Measuring ---

class Results {
  rows = {}

  add(name, timesMs, extra = {}) {
    const ordered = [...timesMs].sort((a, b) => a - b)
    const row = { median_ms: round(median(ordered)), n: ordered.length }
    // Nearest rank: the smallest sample at or above 90% of them.
    if (ordered.length >= 10) row.p90_ms = round(ordered[Math.ceil(ordered.length * 0.9) - 1])
    Object.assign(row, extra)
    this.rows[name] = row
    const shown = Object.entries(row)
      .map(([k, v]) => `${k} ${v}`)
      .join(', ')
    console.error(`${name.padEnd(24)} ${shown}`)
  }
}

const round = (ms, places = 3) => Math.round(ms * 10 ** places) / 10 ** places

function median(sorted) {
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

async function timed(fn) {
  const t0 = performance.now()
  const out = await fn()
  return [performance.now() - t0, out]
}

async function repeat(fn, times) {
  const out = []
  for (let i = 0; i < times; i++) out.push((await timed(fn))[0])
  return out
}

/** Pseudo-random bytes, seeded (xorshift32). */
function randomBytes(state, n) {
  const words = new Uint32Array(Math.ceil(n / 4))
  let x = state.x
  for (let i = 0; i < words.length; i++) {
    x ^= x << 13
    x ^= x >>> 17
    x ^= x << 5
    words[i] = x
  }
  state.x = x
  return new Uint8Array(words.buffer, 0, n)
}

function randrange(state, n) {
  return randomBytes(state, 4).reduce((acc, b) => acc * 256 + b, 0) % n
}

const enc = new TextEncoder()

/** Let the clock pass the last commit's time, so a sweep at `minAge`
 *  0 counts every orphan, as Python's finer clock does. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 10))

// --- The world's shape: one key per file ---

const filePath = (i) =>
  `files/d${String(i % 100).padStart(2, '0')}/s${Math.floor(i / 100) % 10}/f${i}.txt`
const fileBody = (i, version = 0) => enc.encode(`file ${i} version ${version}\n`.repeat(12))

async function world(results, files, rounds) {
  const imports = []
  let store
  let repo
  let wt
  for (let r = 0; r < 3; r++) {
    store = new Memory()
    repo = await Repo.open(store)
    wt = await repo.worktree('main', { create: true })
    for (let i = 0; i < files; i++) wt.set(filePath(i), fileBody(i))
    imports.push((await timed(() => wt.commit({ info: { message: 'import' } })))[0])
  }
  results.add('world.bulk_import', imports, { keys: files })

  for (const lazy of [false, true]) {
    const tag = lazy ? 'world.lazy.' : 'world.'
    const fresh = async () => (await Repo.open(store)).worktree('main', { lazy })

    results.add(`${tag}open`, await repeat(fresh, rounds))
    results.add(`${tag}keys_cold`, await repeat(async () => (await fresh()).keys(), rounds))
    const w = await fresh()
    await w.keys()
    results.add(`${tag}keys_warm`, await repeat(() => w.keys(), rounds))
    results.add(
      `${tag}list_dir`,
      await repeat(
        async () => (await w.keys()).filter((k) => k.startsWith('files/d07/')).sort(),
        rounds,
      ),
    )
    const readAll = async () => {
      const r = await fresh()
      const names = (await r.keys()).sort()
      for (let start = 0; start < names.length; start += 1000) {
        await r.getMany(names.slice(start, start + 1000))
      }
    }
    results.add(`${tag}read_all`, await repeat(readAll, Math.max(1, Math.floor(rounds / 2))))

    const rng = { x: 7 }
    const hot = Array.from({ length: 1000 }, () => filePath(randrange(rng, files)))
    const picks = Array.from({ length: 10_000 }, () => hot[randrange(rng, hot.length)])
    const reads = async () => {
      const r = await fresh()
      for (const k of picks) await r.get(k)
    }
    results.add(`${tag}reads_10k`, await repeat(reads, rounds))
  }

  const one = []
  for (let n = 0; n < rounds * 10; n++) {
    wt.set(filePath(n), fileBody(n, 1 + n))
    one.push((await timed(() => wt.commit()))[0])
  }
  results.add('world.commit_one', one)

  const bulk = []
  for (let n = 0; n < rounds; n++) {
    for (let i = 0; i < 1000; i++)
      wt.set(filePath(((n * 1000 + i) * 7) % files), fileBody(i, 100 + n))
    bulk.push((await timed(() => wt.commit()))[0])
  }
  results.add('world.commit_1k', bulk)

  const merges = []
  for (let n = 0; n < rounds; n++) {
    await repo.branches.create(`side/${n}`, wt.head)
    const side = await repo.worktree(`side/${n}`)
    for (let i = 0; i < 100; i++) {
      side.set(filePath(i * 13 + 1), fileBody(i, 1000 + n))
      wt.set(filePath(i * 13 + 5), fileBody(i, 2000 + n))
    }
    await side.commit()
    await wt.commit()
    merges.push((await timed(() => wt.merge({ branch: `side/${n}` })))[0])
  }
  results.add('world.merge', merges)

  await repo.branches.create('churn', wt.head)
  const churn = await repo.worktree('churn')
  for (let n = 0; n < 20; n++) {
    for (let i = 0; i < 100; i++) churn.set(filePath(i * 31 + n), fileBody(i, 3000 + n))
    await churn.commit()
  }
  await repo.branches.delete('churn')
  for (let n = 0; n < rounds; n++) await repo.branches.delete(`side/${n}`)
  await settle()
  const [gcMs, removed] = await timed(() => repo.gc({ minAge: 0 }))
  results.add('world.gc', [gcMs], { removed })
}

// --- kvsql's shape: a changeset log beside a chunked snapshot ---

const CHUNK = 8 << 20

async function kvsql(results, entries, chunks, rounds) {
  const rng = { x: 11 }
  const store = new Memory()
  const repo = await Repo.open(store)
  const wt = await repo.worktree('db/1', { create: true })
  for (let i = 0; i < 90; i++) wt.set(`checkpoint/${i}`, enc.encode(`checkpoint ${i}`))
  await wt.commit()

  const payload = Array.from({ length: chunks }, () => randomBytes(rng, CHUNK))
  const snapshot = async () => {
    for (let i = 0; i < chunks; i += 2) {
      for (let j = i; j < Math.min(i + 2, chunks); j++) wt.set(`snapshot/${j}`, payload[j])
      await wt.commit({ info: { v: 1, snapshot: { seq: 0, last: i + 2 >= chunks } } })
    }
    wt.set('epoch', enc.encode('{"last":0}'))
    await wt.commit()
  }
  const [snapMs] = await timed(snapshot)
  results.add('kvsql.snapshot', [snapMs], { chunks, chunk_bytes: CHUNK })

  const appends = []
  for (let seq = 1; seq <= entries; seq++) {
    const body = randomBytes(rng, 2048)
    wt.set('entry', body)
    const info = {
      v: 1,
      range: [seq, seq],
      length: body.length,
      sha256: createHash('sha256').update(body).digest('hex'),
      schema: 0,
    }
    appends.push((await timed(() => wt.commit({ info })))[0])
  }
  results.add('kvsql.append', appends, {
    total_ms: round(
      appends.reduce((a, b) => a + b, 0),
      1,
    ),
  })

  const fresh = async () => (await Repo.open(store)).worktree('db/1')
  results.add('kvsql.open', await repeat(fresh, rounds))

  const replay = async () => {
    const r = await Repo.open(store)
    let count = 0
    for await (const commit of r.log({ branch: 'db/1' }, { firstParent: true })) {
      if (commit.info !== null && commit.info !== undefined && 'range' in commit.info) {
        await (await r.snapshot({ commit: commit.hash })).get('entry')
        count++
      }
    }
    return count
  }
  results.add('kvsql.replay', await repeat(replay, 3), { entries: await replay() })

  const merges = []
  for (let n = 0; n < rounds; n++) {
    await repo.branches.create(`db/1-side/${n}`, wt.head)
    const side = await repo.worktree(`db/1-side/${n}`)
    side.set(`checkpoint/${n}`, enc.encode(`moved ${n}`))
    wt.set('entry', randomBytes(rng, 2048))
    await side.commit()
    await wt.commit()
    merges.push((await timed(() => wt.merge({ branch: `db/1-side/${n}` })))[0])
  }
  results.add('kvsql.merge', merges)

  for (let n = 0; n < rounds; n++) await repo.branches.delete(`db/1-side/${n}`)
  await repo.branches.delete('db/1')
  await settle()
  const [gcMs, removed] = await timed(() => repo.gc({ minAge: 0 }))
  results.add('kvsql.gc', [gcMs], { removed })
}

// --- The run ---

function machine() {
  let cpu = cpus()[0]?.model ?? 'unknown'
  try {
    cpu = execFileSync('sysctl', ['-n', 'machdep.cpu.brand_string'], { encoding: 'utf8' }).trim()
  } catch {}
  return `${cpu}, ${type()} ${release()}`
}

const { values: args } = parseArgs({
  options: { quick: { type: 'boolean' }, json: { type: 'string' }, only: { type: 'string' } },
})
const results = new Results()
const rounds = args.quick ? 2 : 5
if (args.only === undefined || args.only === 'world') {
  await world(results, args.quick ? 2_000 : 100_000, rounds)
}
if (args.only === undefined || args.only === 'kvsql') {
  await kvsql(results, args.quick ? 50 : 3_000, args.quick ? 2 : 8, rounds)
}
const report = {
  host: HOST,
  runtime: `Node ${process.versions.node}`,
  machine: machine(),
  quick: args.quick === true,
  results: results.rows,
}
if (args.json !== undefined) writeFileSync(args.json, `${JSON.stringify(report, null, 2)}\n`)
