// Lazy-keyset bench core: builds a `page/<n>` keyset and measures eager
// against lazy worktrees over it. Runs in Node and in the browser; the
// caller passes the kvgit module, a store, and memory/GC hooks.
//
// kvgit: { Repo, NodeCache }
// env:   { now(): ms, mem(): bytes | null, gc(): Promise<void> | void, log(msg) }

const PREFIX = 'kvgit:keyset:'
const pageKey = (n) => `page/${n}`

function randomPage(size) {
  const out = new Uint8Array(size)
  for (let i = 0; i < size; i += 65536)
    crypto.getRandomValues(out.subarray(i, Math.min(size, i + 65536)))
  return out
}

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b)
  return s.length === 0 ? null : s[Math.floor(s.length / 2)]
}
const round = (x, d = 2) => (x === null || x === undefined ? null : Number(x.toFixed(d)))

/** Seeded PRNG so runs pick the same pages. */
function rng(seed) {
  let s = seed >>> 0
  return () => {
    s = (s + 0x6d2b79f5) >>> 0
    let t = s
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/** Build branch `branch` holding pages 1..n, `batch` pages per commit,
 *  through a lazy worktree (the stored tree is the same either way). */
export async function build(kvgit, store, env, { branch, n, batch = 16384, valueSize = 4096 }) {
  const repo = await kvgit.Repo.open(store)
  if (await repo.branches.has(branch)) {
    const wt = await repo.worktree(branch, { lazy: true })
    if (await wt.has(pageKey(n))) return { skipped: true }
    throw new Error(`branch ${branch} exists but is incomplete`)
  }
  const wt = await repo.worktree(branch, { create: true, lazy: { cache: new kvgit.NodeCache() } })
  const t0 = env.now()
  for (let start = 1; start <= n; start += batch) {
    const end = Math.min(n, start + batch - 1)
    for (let p = start; p <= end; p++) wt.set(pageKey(p), randomPage(valueSize))
    await wt.commit({ onMoved: 'raise' })
    if ((end / batch) % 8 === 0 || end === n)
      env.log(`  built ${end}/${n} (${((env.now() - t0) / 1000).toFixed(1)}s)`)
  }
  return { ms: env.now() - t0, head: wt.head }
}

/** The HAMT's shape at the branch head: nodes per depth, leaf fill,
 *  branch fanout, node sizes. */
export async function shape(kvgit, store, env, { branch }) {
  const repo = await kvgit.Repo.open(store)
  const head = await repo.branches.get(branch)
  const { root } = await repo.getCommit(head)
  const depths = []
  const leafItems = new Map()
  const leafBytes = []
  const branchBytes = []
  const fanout = []
  const keyDepth = new Map() // key path length (nodes visited) -> keys
  let level = [root]
  let depth = 0
  const dec = new TextDecoder()
  while (level.length > 0) {
    const next = []
    let leaves = 0
    let branches = 0
    for (let i = 0; i < level.length; i += 4096) {
      const chunk = level.slice(i, i + 4096)
      const got = await store.getMany(chunk.map((h) => PREFIX + h))
      for (const [, raw] of got) {
        const node = JSON.parse(dec.decode(raw))
        if (node.kind === 'leaf') {
          leaves++
          const c = Object.keys(node.items).length
          leafItems.set(c, (leafItems.get(c) ?? 0) + 1)
          leafBytes.push(raw.length)
          keyDepth.set(depth + 1, (keyDepth.get(depth + 1) ?? 0) + c)
        } else {
          branches++
          const kids = Object.values(node.children)
          fanout.push(kids.length)
          branchBytes.push(raw.length)
          next.push(...kids)
        }
      }
    }
    depths.push({ depth, branches, leaves })
    level = next
    depth++
  }
  const sum = (xs) => xs.reduce((a, b) => a + b, 0)
  const max = (xs) => xs.reduce((a, b) => (b > a ? b : a), 0)
  const nLeaves = sum([...leafItems.values()])
  const nKeys = sum([...leafItems].map(([c, k]) => c * k))
  return {
    keys: nKeys,
    depths,
    maxDepth: depths.length - 1,
    keyPathLength: Object.fromEntries([...keyDepth].sort((a, b) => a[0] - b[0])),
    meanPathLength: round(sum([...keyDepth].map(([d, k]) => d * k)) / nKeys),
    leaves: nLeaves,
    leafFill: Object.fromEntries([...leafItems].sort((a, b) => a[0] - b[0])),
    meanLeafFill: round(nKeys / nLeaves),
    branches: fanout.length,
    meanFanout: round(sum(fanout) / fanout.length),
    leafBytes: { mean: round(sum(leafBytes) / leafBytes.length, 0), max: max(leafBytes) },
    branchBytes: {
      mean: round(sum(branchBytes) / branchBytes.length, 0),
      max: max(branchBytes),
    },
    totalNodeBytes: sum(leafBytes) + sum(branchBytes),
    branchNodeBytes: sum(branchBytes),
  }
}

async function open(kvgit, repo, branch, mode, cacheBytes) {
  return mode === 'lazy'
    ? repo.worktree(branch, { lazy: { cache: new kvgit.NodeCache({ maxBytes: cacheBytes }) } })
    : repo.worktree(branch)
}

/** Open, reads and commits for one mode. */
export async function measure(
  kvgit,
  store,
  env,
  { branch, n, mode, cacheBytes = 16 << 20, opens = 3, commits = 10, seed = 1 },
) {
  const repo = await kvgit.Repo.open(store)
  const rand = rng(seed)
  const pick = () => 1 + Math.floor(rand() * n)
  const out = { mode, n }

  // Open: time and memory, a few times; keep the last handle.
  const openMs = []
  let wt = null
  const memBefore = []
  const memAfter = []
  const memSettled = []
  for (let i = 0; i < opens; i++) {
    wt = null
    await env.gc()
    memBefore.push(env.mem())
    const t = env.now()
    wt = await open(kvgit, repo, branch, mode, cacheBytes)
    openMs.push(env.now() - t)
    memAfter.push(env.mem())
    await env.gc()
    memSettled.push(env.mem())
  }
  out.openMs = openMs.map((x) => round(x))
  out.openMedianMs = round(median(openMs))
  const delta = (a, b) => (a[0] === null ? null : round((median(b) - median(a)) / 1e6, 1))
  out.openRetainedMB = delta(memBefore, memAfter)
  out.openSettledMB = delta(memBefore, memSettled)

  const cache = wt.nodeCache
  const t = async (fn) => {
    const s = env.now()
    const r = await fn()
    return [env.now() - s, r]
  }

  // First get on the fresh handle: cold for lazy (empty cache).
  const p1 = pick()
  ;[out.firstGetMs] = await t(() => wt.get(pageKey(p1)))
  ;[out.warmGetMs] = await t(() => wt.get(pageKey(p1)))
  // Further single gets of other random pages: upper levels warm.
  const gets = []
  for (let i = 0; i < 50; i++) {
    const p = pick()
    gets.push((await t(() => wt.get(pageKey(p))))[0])
  }
  out.randomGetMedianMs = round(median(gets), 3)

  // getMany of 64 contiguous pages: on a fresh lazy handle (fully cold)
  // and on the warmed one.
  const range = (s) => Array.from({ length: 64 }, (_, i) => pageKey(1 + ((s + i) % n)))
  if (mode === 'lazy') {
    const fresh = await open(kvgit, repo, branch, mode, cacheBytes)
    const [ms, got] = await t(() => fresh.getMany(range(pick())))
    if (got.size !== 64) throw new Error(`getMany found ${got.size}`)
    out.getMany64ColdMs = round(ms)
  }
  const many = []
  for (let i = 0; i < 10; i++) {
    const s = pick()
    many.push((await t(() => wt.getMany(range(s))))[0])
  }
  out.getMany64Ms = round(median(many))

  // Commits: one page, then sixteen contiguous pages, each with raise.
  const commitMs = { 1: [], 16: [] }
  for (const k of [1, 16]) {
    for (let i = 0; i < commits; i++) {
      const s = pick()
      for (let j = 0; j < k; j++) wt.set(pageKey(1 + ((s + j) % n)), randomPage(4096))
      commitMs[k].push((await t(() => wt.commit({ onMoved: 'raise' })))[0])
    }
  }
  out.commit1MedianMs = round(median(commitMs[1]))
  out.commit16MedianMs = round(median(commitMs[16]))
  out.commit1Ms = commitMs[1].map((x) => round(x))
  out.commit16Ms = commitMs[16].map((x) => round(x))

  await env.gc()
  out.steadyMB = memBefore[0] === null ? null : round((env.mem() - median(memBefore)) / 1e6, 1)
  if (cache) out.cache = cache.stats()
  return out
}

/** Node-cache behaviour for a read workload, at several cache sizes:
 *  `warm` random single gets from a cold cache, then `gets` more
 *  (steady state), then `manys` getMany calls of 64 contiguous pages.
 *  Misses are store reads of HAMT nodes. */
export async function hitRates(
  kvgit,
  store,
  env,
  { branch, n, sizesMB = [1, 4, 16, 64, 128], warm = 5000, gets = 2000, manys = 100, seed = 2 },
) {
  const repo = await kvgit.Repo.open(store)
  const out = []
  const rate = (s) => round(s.hits / (s.hits + s.misses), 3)
  for (const mb of sizesMB) {
    const rand = rng(seed)
    const pick = () => 1 + Math.floor(rand() * n)
    const cache = new kvgit.NodeCache({ maxBytes: mb << 20 })
    const wt = await repo.worktree(branch, { lazy: { cache } })
    const s0 = env.now()
    for (let i = 0; i < warm; i++) await wt.get(pageKey(pick()))
    const warmStats = cache.stats()
    const warmMs = env.now() - s0
    cache.resetStats()
    const s1 = env.now()
    for (let i = 0; i < gets; i++) await wt.get(pageKey(pick()))
    const getStats = cache.stats()
    const getMs = env.now() - s1
    cache.resetStats()
    const s2 = env.now()
    for (let i = 0; i < manys; i++) {
      const s = pick()
      await wt.getMany(Array.from({ length: 64 }, (_, j) => pageKey(1 + ((s + j) % n))))
    }
    const manyStats = cache.stats()
    out.push({
      cacheMB: mb,
      coldGets: {
        hitRate: rate(warmStats),
        missesPerGet: round(warmStats.misses / warm, 2),
        meanMs: round(warmMs / warm, 3),
      },
      warmGets: {
        hitRate: rate(getStats),
        missesPerGet: round(getStats.misses / gets, 2),
        meanMs: round(getMs / gets, 3),
      },
      getMany64: {
        hitRate: rate(manyStats),
        missesPerCall: round(manyStats.misses / manys, 1),
        meanMs: round((env.now() - s2) / manys, 2),
      },
      entries: manyStats.entries,
      cachedMB: round(manyStats.bytes / 1e6, 1),
      evictions: manyStats.evictions,
    })
  }
  return out
}
