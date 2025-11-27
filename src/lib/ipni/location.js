import * as dagcbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import { encodeUvarint } from '../encoding.js'

const LOCATION_COMMITMENT_ID = 0x3e0002

/**
 * Range represents an optional byte range within a shard
 * Encoded as a tuple: [offset, length]
 * @typedef {Object} Range
 * @property {bigint} offset
 * @property {bigint|undefined} length - optional length, undefined means unbounded
 */

/**
 * LocationCommitmentMetadata represents a commitment to the location of data
 * ported from https://github.com/storacha/go-libstoracha/blob/main/metadata/metadata.go
 *
 * Field mappings (from metadata.ipldsch):
 * - shard (Link, optional) -> "s"
 * - range (Range, optional) -> "r"
 * - expiration (Int) -> "e"
 * - claim (Link) -> "c"
 */
export class LocationCommitmentMetadata {
  /**
   * @param {Object} options
   * @param {CID} [options.shard] - optional alternate cid to use to lookup this location
   * @param {Range} [options.range] - optional byte range within a shard
   * @param {bigint} options.expiration - unix epoch in seconds
   * @param {CID} options.claim - the cid of the claim
   */
  constructor({ shard, range, expiration, claim }) {
    this.shard = shard
    this.range = range
    this.expiration = expiration
    this.claim = claim
  }

  /**
   * Returns the protocol ID for this metadata type
   * @returns {number}
   */
  id() {
    return LOCATION_COMMITMENT_ID
  }

  /**
   * Encode the metadata to bytes using dagcbor
   * Mirrors the Go implementation's marshalBinary function
   * @returns {Promise<Uint8Array>}
   */
  async marshalBinary() {
    // Encode the metadata ID as a varint
    const idBytes = encodeUvarint(LOCATION_COMMITMENT_ID)

    // Prepare the data structure for encoding with field renames
    // Only include optional fields if they are defined
    const data = /** @type {Record<string, any>} */ ({
      e: this.expiration,
      c: this.claim
    })

    if (this.shard !== undefined) {
      data.s = this.shard
    }

    if (this.range !== undefined) {
      data.r = [this.range.offset, this.range.length]
    }

    // Encode using dagcbor
    const cbcorBytes = dagcbor.encode(data)

    // Combine: varint-encoded ID + dagcbor-encoded data
    const result = new Uint8Array(idBytes.length + cbcorBytes.length)
    result.set(idBytes, 0)
    result.set(cbcorBytes, idBytes.length)

    return result
  }

  /**
   * Get the claim CID
   * @returns {CID}
   */
  getClaim() {
    return this.claim
  }
}
