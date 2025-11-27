import { sha256 } from 'multiformats/hashes/sha2'

/**
 * EncodeContextID encodes a context ID by concatenating the space DID bytes
 * with the digest bytes and hashing the result with SHA2-256.
 *
 * Ported from https://github.com/storacha/go-libstoracha/blob/main/advertisement/advertisement.go#L26
 *
 * @param {string|Object} space - Space DID (string or DID object with toString())
 * @param {Uint8Array} digest - Multihash digest bytes
 * @returns {Promise<Uint8Array>} - The encoded context ID as a multihash
 */
export async function encodeContextID(space, digest) {
  // Convert DID to string if it's an object
  const spaceStr = typeof space === 'string' ? space : space.toString()
  const spaceBytes = new TextEncoder().encode(spaceStr)

  // Concatenate space bytes and digest bytes
  const combined = new Uint8Array(spaceBytes.length + digest.length)
  combined.set(spaceBytes, 0)
  combined.set(digest, spaceBytes.length)

  // Hash with SHA2-256 and return as multihash
  const hash = await sha256.digest(combined)
  return hash.bytes
}