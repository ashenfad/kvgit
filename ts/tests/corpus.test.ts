/**
 * Every corpus scenario this package runs, with its final store checked.
 *
 * With `KVGIT_CORPUS_RECORD=1` the final store of a scenario Python
 * doesn't run is written into it instead of checked; Python records the
 * rest. See `corpus/README.md`.
 */

import { describe, expect, it } from 'vitest'
import { HOST, Run, load, saveFinal, scenarioId, scenarioPaths } from './corpus/runner'

const RECORD = process.env.KVGIT_CORPUS_RECORD === '1'

describe('corpus', () => {
  for (const path of scenarioPaths()) {
    const scenario = load(path)
    const only = scenario.only ?? [HOST]
    it.skipIf(!only.includes(HOST))(scenarioId(path), async () => {
      const final = await new Run(scenario, scenarioId(path)).run()
      if (RECORD && !only.includes('python')) {
        saveFinal(path, final)
        return
      }
      expect(scenario.final, `${scenarioId(path)} has no final store; record it`).toBeDefined()
      expect(final).toEqual(scenario.final)
    })
  }
})
