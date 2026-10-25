import { describe, expect, it } from 'vitest'
import { core } from '../src/core'
import { coreVersion } from './expected'

describe('the core in a browser', () => {
  it('loads in the main thread', async () => {
    expect((await core()).version()).toBe(coreVersion)
  })

  it('loads in a module worker', async () => {
    const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' })
    try {
      const reply = await new Promise<unknown>((resolve, reject) => {
        worker.onmessage = (event) => resolve(event.data)
        worker.onerror = (event) => reject(new Error(event.message))
      })
      expect(reply).toBe(coreVersion)
    } finally {
      worker.terminate()
    }
  })
})
