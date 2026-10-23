import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/backends/memory.ts',
    'src/backends/idb.ts',
    'src/backends/sqlite.ts',
    'src/backends/sqlite.browser.ts',
    'src/github/index.ts',
    'src/github/fake.ts',
    'src/github/fake-server.ts',
  ],
  format: ['esm'],
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'es2022',
  treeshake: true,
})
