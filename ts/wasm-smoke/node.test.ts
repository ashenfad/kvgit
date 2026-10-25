import { describe, expect, it } from 'vitest'
import { core } from '../src/core'
import { coreVersion } from './expected'

describe('the core in Node', () => {
  it('loads from the bytes the package carries and reports its version', async () => {
    expect((await core()).version()).toBe(coreVersion)
  })
})
