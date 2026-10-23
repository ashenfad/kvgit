import init, { version } from '../wasm/kvgit_wasm.js'

await init()
postMessage(version())
