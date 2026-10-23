#!/usr/bin/env node
/**
 * Bundle-shape check: scan the built `dist/` for patterns that mean
 * "this artifact won't run in a browser bundle."
 *
 * Catches regressions like:
 *  - tsup inlining a transitive dep that uses `createRequire('module')`
 *  - a Node-only sub-path leaking into a chunk that browser bundlers
 *    statically analyze (as `backends/sqlite` once did)
 *
 * The allowlist carves out the legitimately Node-only entry files that
 * are *expected* to import Node builtins. Browser bundlers resolve
 * those via the `"browser"` exports condition to a stub instead — see
 * `src/backends/sqlite.browser.ts`.
 *
 * Run via `pnpm check:dist` after `pnpm build`.
 */

import { readFile, readdir, stat } from 'node:fs/promises'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const pkgRoot = join(dirname(fileURLToPath(import.meta.url)), '..')
const distPath = join(pkgRoot, 'dist')

// Patterns we never want to see in a browser-targeted bundle.
const FORBIDDEN = [
  { pattern: /from\s+['"]node:module['"]/, label: "import from 'node:module'" },
  { pattern: /from\s+['"]module['"]/, label: "import from 'module'" },
  { pattern: /createRequire\s*\(/, label: 'createRequire call' },
  { pattern: /from\s+['"]node:worker_threads['"]/, label: "import from 'node:worker_threads'" },
  { pattern: /from\s+['"]worker_threads['"]/, label: "import from 'worker_threads'" },
  { pattern: /from\s+['"]node:sqlite['"]/, label: "import from 'node:sqlite'" },
]

// Files allowed to contain forbidden patterns, because a browser bundle
// never reaches them: the `"browser"` exports condition resolves them
// to a stub. Paths are relative to the package root.
const ALLOWLIST = new Set(['dist/backends/sqlite.js'])

async function* walkJsFiles(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) yield* walkJsFiles(path)
    else if (entry.isFile() && entry.name.endsWith('.js')) yield path
  }
}

function stripComments(code) {
  // Rough but good enough — we just need to skip JSDoc / line-comment
  // mentions of the forbidden tokens. Won't handle commented-out code
  // perfectly, which is fine: code in comments is unreachable, so a
  // false positive there is still worth flagging in a review.
  return code.replace(/\/\/.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '')
}

try {
  await stat(distPath)
} catch {
  console.error('check-dist-shape: no dist/ — run `pnpm build` first')
  process.exit(1)
}

const violations = []
for await (const file of walkJsFiles(distPath)) {
  const rel = relative(pkgRoot, file)
  if (ALLOWLIST.has(rel)) continue
  const code = stripComments(await readFile(file, 'utf8'))
  for (const { pattern, label } of FORBIDDEN) {
    if (pattern.test(code)) violations.push({ file: rel, label })
  }
}

if (violations.length > 0) {
  console.error('\ncheck-dist-shape FAILED — browser-hostile patterns found in built artifacts:\n')
  for (const v of violations) console.error(`  ${v.file}: ${v.label}`)
  console.error(
    '\nFix the leak in source (most often a tsup-inlined transitive dep, or a' +
      '\ndynamic-import target that needs a "browser" exports-condition stub),' +
      '\nor add the file to ALLOWLIST in scripts/check-dist-shape.mjs if it is' +
      '\na legitimately Node-only entry covered by an exports-condition stub.\n',
  )
  process.exit(1)
}

console.log('check-dist-shape: ok')
