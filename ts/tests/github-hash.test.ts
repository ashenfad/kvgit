import { describe, expect, it } from 'vitest'
import { base64ToBytes, bytesToBase64, gitBlobSha1 } from '../src/github/index'

const enc = new TextEncoder()

describe('gitBlobSha1', () => {
  // Vectors computed with `git hash-object --stdin`.
  it('matches git hash-object on known vectors', async () => {
    expect(await gitBlobSha1(new Uint8Array(0))).toBe('e69de29bb2d1d6434b8b29ae775ad8c2e48c5391')
    expect(await gitBlobSha1(enc.encode('hello\n'))).toBe(
      'ce013625030ba8dba906f756967f9e9ca394464a',
    )
    expect(await gitBlobSha1(enc.encode('test content\n'))).toBe(
      'd670460b4b4aece5915caf5c68d12f560a9fe3e4',
    )
  })

  it('matches git hash-object on binary content with NUL bytes', async () => {
    const bytes = new Uint8Array([0x62, 0x69, 0x6e, 0x00, 0x01, 0x02]) // "bin\0\x01\x02"
    expect(await gitBlobSha1(bytes)).toBe('e158ec5208bacda2ca76214f4a18bd2ce2ffee46')
  })
})

describe('base64 codecs', () => {
  it('round-trips binary, including multi-chunk sizes', () => {
    const big = new Uint8Array(0x8000 * 2 + 17)
    for (let i = 0; i < big.length; i++) big[i] = i % 251
    expect(base64ToBytes(bytesToBase64(big))).toEqual(big)
  })

  it('tolerates GitHub-style newline-wrapped base64', () => {
    const b64 = bytesToBase64(enc.encode('hello world'))
    const wrapped = `${b64.slice(0, 6)}\n${b64.slice(6)}\n`
    expect(new TextDecoder().decode(base64ToBytes(wrapped))).toBe('hello world')
  })
})
