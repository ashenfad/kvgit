// Commit time against branch size: one-key commits on a branch holding
// 1k, 10k and 100k keys, on the in-memory backend. Not part of the test
// suite. Build first, then run from ts/:
//
//   pnpm run build && node scripts/bench-commit.mjs [sizes...]

import { Memory } from '../dist/backends/memory.js'
import { VersionedKV } from '../dist/index.js'

const sizes = process.argv.slice(2).map(Number)
if (sizes.length === 0) sizes.push(1_000, 10_000, 100_000)
const COMMITS = 200
const WARMUP = 20

const enc = new TextEncoder()

for (const n of sizes) {
  const vk = await VersionedKV.open(new Memory())
  const seed = new Map()
  for (let i = 0; i < n; i++) seed.set(`key/${i}`, enc.encode(`value ${i}`))
  await vk.commit({ updates: seed })

  const times = []
  for (let i = 0; i < WARMUP + COMMITS; i++) {
    const updates = new Map([[`key/${i % n}`, enc.encode(`changed ${i}`)]])
    const t0 = performance.now()
    await vk.commit({ updates })
    if (i >= WARMUP) times.push(performance.now() - t0)
  }
  times.sort((a, b) => a - b)
  const median = times[Math.floor(times.length / 2)]
  const p90 = times[Math.floor(times.length * 0.9)]
  console.log(
    `${String(n).padStart(7)} keys: median ${median.toFixed(3)} ms, p90 ${p90.toFixed(3)} ms per one-key commit`,
  )
}
