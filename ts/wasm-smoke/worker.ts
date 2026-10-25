import { core } from '../src/core'

postMessage((await core()).version())
