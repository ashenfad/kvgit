// Node driver for the lazy-keyset bench (see lazy-core.mjs). Needs a
// build (`pnpm run build`) and --expose-gc.
//
//   node --expose-gc bench/lazy-node.mjs memory <n>          build + everything, one process
//   node --expose-gc bench/lazy-node.mjs sqlite <n> build    build a file store
//   node --expose-gc bench/lazy-node.mjs sqlite <n> shape|hits|eager|lazy
//
// Results are merged into $BENCH_RESULTS (a JSON file) under
// <backend>/<n>/<step>. Sqlite stores live in $BENCH_DIR.

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { Memory } from '../dist/backends/memory.js'
import { Sqlite } from '../dist/backends/sqlite.js'
import * as kvgit from '../dist/index.js'
import { build, hitRates, measure, shape } from './lazy-core.mjs'

const [backend, nArg, step] = process.argv.slice(2)
const n = Number(nArg)
const RESULTS = process.env.BENCH_RESULTS
const DIR = process.env.BENCH_DIR
if (!RESULTS || !DIR) throw new Error('set BENCH_RESULTS and BENCH_DIR')

const env = {
  now: () => performance.now(),
  mem: () => {
    const m = process.memoryUsage()
    return m.heapUsed + m.arrayBuffers
  },
  gc: async () => {
    globalThis.gc?.()
    await new Promise((r) => setTimeout(r, 0))
    globalThis.gc?.()
  },
  log: (m) => console.error(m),
}
const branch = 'db/bench'

function save(key, value) {
  const all = existsSync(RESULTS) ? JSON.parse(readFileSync(RESULTS, 'utf-8')) : {}
  all[backend] ??= {}
  all[backend][n] ??= {}
  all[backend][n][key] = value
  writeFileSync(RESULTS, `${JSON.stringify(all, null, 2)}\n`)
  console.error(`${backend} ${n} ${key}: ${JSON.stringify(value).slice(0, 400)}`)
}

const maxRssMB = () => process.resourceUsage().maxRSS / 1024

if (backend === 'memory') {
  const store = new Memory()
  save('build', await build(kvgit, store, env, { branch, n }))
  save('shape', await shape(kvgit, store, env, { branch }))
  for (const mode of ['eager', 'lazy'])
    save(mode, await measure(kvgit, store, env, { branch, n, mode }))
  save('hits', await hitRates(kvgit, store, env, { branch, n }))
} else if (backend === 'sqlite') {
  const store = await Sqlite.open({ path: `${DIR}/kv-${n}.sqlite` })
  if (step === 'build') save('build', await build(kvgit, store, env, { branch, n }))
  else if (step === 'shape') save('shape', await shape(kvgit, store, env, { branch }))
  else if (step === 'hits') save('hits', await hitRates(kvgit, store, env, { branch, n }))
  else if (step === 'eager' || step === 'lazy') {
    await env.gc()
    const before = maxRssMB()
    const out = await measure(kvgit, store, env, { branch, n, mode: step })
    out.maxRssBeforeMB = Math.round(before)
    out.maxRssPeakMB = Math.round(maxRssMB())
    save(step, out)
  } else throw new Error(`unknown step ${step}`)
  store.close?.()
} else {
  throw new Error(`unknown backend ${backend}`)
}
