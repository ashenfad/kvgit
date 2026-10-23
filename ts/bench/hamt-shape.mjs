// The keyset HAMT's shape for `page/<n>` keysets at several bucket
// sizes (bucketMax: keys per leaf before it splits), keyset only, no
// blobs. kvgit stores use bucketMax 8; the others are what-ifs, since
// changing it changes every root hash (a format change).
//
//   node bench/hamt-shape.mjs [n,...] [bucketMax,...]

import { Memory } from '../dist/backends/memory.js'
import { Keyset } from '../dist/index.js'

const sizes = (process.argv[2] ?? '50000,262144,1000000').split(',').map(Number)
const buckets = (process.argv[3] ?? '8,16,32,64').split(',').map(Number)
const PREFIX = Keyset.DEFAULT_PREFIX
const dec = new TextDecoder()

const hex = (n) => n.toString(16).padStart(64, '0')
const entry = (n, v = 0) => ({ blob: `kvgit:blob:${hex(n * 7919 + v)}`, meta: { size: 4096 } })

async function nodeShape(store, root) {
  const levels = []
  let level = [root]
  let nodes = 0
  let bytes = 0
  let leafBytes = 0
  let leaves = 0
  let keys = 0
  let pathSum = 0
  while (level.length > 0) {
    const next = []
    let b = 0
    let l = 0
    for (let i = 0; i < level.length; i += 8192) {
      const got = await store.getMany(level.slice(i, i + 8192).map((h) => PREFIX + h))
      for (const raw of got.values()) {
        const node = JSON.parse(dec.decode(raw))
        nodes++
        bytes += raw.length
        if (node.kind === 'leaf') {
          l++
          leaves++
          leafBytes += raw.length
          const c = Object.keys(node.items).length
          keys += c
          pathSum += c * (levels.length + 1)
        } else {
          b++
          next.push(...Object.values(node.children))
        }
      }
    }
    levels.push(`${b}b/${l}l`)
    level = next
  }
  return {
    levels: levels.join(' '),
    nodes,
    keysetMB: +(bytes / 1e6).toFixed(1),
    meanLeafKeys: +(keys / leaves).toFixed(2),
    meanLeafBytes: Math.round(leafBytes / leaves),
    meanPath: +(pathSum / keys).toFixed(2),
  }
}

/** Bytes and nodes a commit changing `k` contiguous pages writes. */
async function commitCost(ks, n, k, reps = 20) {
  let nodes = 0
  let bytes = 0
  for (let r = 0; r < reps; r++) {
    const s = 1 + ((r * 104729) % (n - k))
    const updates = []
    for (let j = 0; j < k; j++) updates.push([`page/${s + j}`, entry(s + j, r + 1)])
    const next = await ks.updatedBatch({ updates })
    nodes += next.pending.size
    for (const v of next.pending.values()) bytes += v.length
  }
  return { nodes: +(nodes / reps).toFixed(1), KB: +(bytes / reps / 1024).toFixed(1) }
}

const rows = []
for (const n of sizes) {
  for (const bucketMax of buckets) {
    const store = new Memory()
    let ks = await Keyset.empty(store, { bucketMax })
    for (let s = 1; s <= n; s += 65536) {
      const updates = []
      for (let p = s; p < Math.min(n + 1, s + 65536); p++) updates.push([`page/${p}`, entry(p)])
      ks = await (await ks.updatedBatch({ updates })).flush()
    }
    const shape = await nodeShape(store, ks.root)
    const c1 = await commitCost(ks, n, 1)
    const c16 = await commitCost(ks, n, 16)
    const row = { n, bucketMax, ...shape, commit1: c1, commit16: c16 }
    rows.push(row)
    console.error(JSON.stringify(row))
  }
}
console.log(JSON.stringify(rows, null, 2))
