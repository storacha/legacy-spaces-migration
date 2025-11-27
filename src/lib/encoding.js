/**
 * Encode an unsigned integer as a varint (variable-length integer)
 * Uses the same encoding as Go's varint package
 * @param {number} value - The value to encode
 * @returns {Uint8Array} - The varint-encoded bytes
 */
export function encodeUvarint(value) {
  const bytes = []
  let v = value

  while (v >= 0x80) {
    bytes.push((v & 0xff) | 0x80)
    v >>>= 7
  }
  bytes.push(v & 0xff)

  return new Uint8Array(bytes)
}
