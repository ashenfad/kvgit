/**
 * Base64 codecs for the Git Data API (blob content travels base64 in
 * both directions; GitHub inserts newlines into responses).
 *
 * Chunked btoa/atob keeps these browser-and-Node portable without a
 * Buffer dependency, and avoids `String.fromCharCode(...bytes)` call
 * stack overflows on multi-MB blobs.
 */

const CHUNK = 0x8000

export function bytesToBase64(bytes: Uint8Array): string {
  let binary = ''
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK))
  }
  return btoa(binary)
}

export function base64ToBytes(b64: string): Uint8Array {
  const cleaned = b64.replace(/\s+/g, '')
  const binary = atob(cleaned)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i)
  return bytes
}
