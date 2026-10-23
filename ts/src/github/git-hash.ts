/**
 * Local git object hashing.
 *
 * `gitBlobSha1(bytes)` computes the SHA-1 GitHub will assign a blob
 * (`sha1("blob <len>\0" + bytes)`) — the key to push-side dedup: if we
 * know a blob's SHA is already on the remote (or in this push), we
 * skip the upload entirely. WebCrypto keeps it portable (browser +
 * Node 20).
 */

const _encoder = new TextEncoder()

export async function gitBlobSha1(bytes: Uint8Array): Promise<string> {
  const header = _encoder.encode(`blob ${bytes.length}\0`)
  const payload = new Uint8Array(header.length + bytes.length)
  payload.set(header, 0)
  payload.set(bytes, header.length)
  const digest = await globalThis.crypto.subtle.digest('SHA-1', payload as Uint8Array<ArrayBuffer>)
  const out = new Uint8Array(digest)
  let hex = ''
  for (let i = 0; i < out.length; i++) {
    const b = out[i] as number
    hex += (b < 16 ? '0' : '') + b.toString(16)
  }
  return hex
}
