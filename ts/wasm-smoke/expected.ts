/// <reference types="vite/client" />
import cargo from '../../Cargo.toml?raw'

/** The Cargo workspace's version, which the core reports as its own. */
export const coreVersion = /^version = "(.+)"$/m.exec(cargo)?.[1]
