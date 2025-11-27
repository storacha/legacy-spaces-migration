/**
 * @typedef {import('@libp2p/interface').PeerId} PeerId
 * @typedef {import('@multiformats/multiaddr').Multiaddr} Multiaddr
 */

/**
 * ProviderResult represents a provider returned from an IPNI find query
 * Ported from https://github.com/ipni/go-libipni/blob/main/find/model/model.go#L15
 *
 * @typedef {Object} AddrInfo
 * @property {PeerId} id - The peer ID
 * @property {Multiaddr[]} addrs - Array of multiaddrs
 *
 * @typedef {Object} ProviderResultOptions
 * @property {Uint8Array} [contextID] - The context ID bytes
 * @property {Uint8Array} [metadata] - The metadata bytes
 * @property {AddrInfo} [provider] - The provider's address info containing id and addrs
 */

/**
 * ProviderResult represents a single provider result from an IPNI query
 */
export class ProviderResult {
  /**
   * @param {ProviderResultOptions} options
   */
  constructor({ contextID, metadata, provider } = {}) {
    this.contextID = contextID
    this.metadata = metadata
    this.provider = provider
  }

  /**
   * Compare two ProviderResult instances for equality
   * Note: Comparison excludes provider addresses, matching Go implementation
   *
   * @param {ProviderResult} other
   * @returns {boolean}
   */
  equals(other) {
    if (!other) return false

    // Compare ContextID
    const contextIDEqual = this._bytesEqual(this.contextID, other.contextID)
    if (!contextIDEqual) return false

    // Compare Metadata
    const metadataEqual = this._bytesEqual(this.metadata, other.metadata)
    if (!metadataEqual) return false

    // Compare Provider ID (but not addresses)
    if (!this.provider && !other.provider) {
      return true
    }
    if (!this.provider || !other.provider) {
      return false
    }

    return this.provider.id === other.provider.id
  }

  /**
   * Helper to compare byte arrays
   * @private
   * @param {Uint8Array|undefined} a
   * @param {Uint8Array|undefined} b
   * @returns {boolean}
   */
  _bytesEqual(a, b) {
    if (a === b) return true
    if (!a || !b) return a === b
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false
    }
    return true
  }

  /**
   * Create a ProviderResult from a JSON object
   * @param {Record<string, any>} obj - JSON object with contextID, metadata, provider
   * @returns {ProviderResult}
   */
  static fromJSON(obj) {
    let provider = undefined
    if (obj.provider) {
      provider = {
        id: obj.provider.id,
        addrs: Array.isArray(obj.provider.addrs)
          ? obj.provider.addrs
          : obj.provider.addrs
          ? [obj.provider.addrs]
          : [],
      }
    }

    return new ProviderResult({
      contextID: obj.contextID
        ? new Uint8Array(
            typeof obj.contextID === 'string'
              ? Buffer.from(obj.contextID, 'base64')
              : obj.contextID
          )
        : undefined,
      metadata: obj.metadata
        ? new Uint8Array(
            typeof obj.metadata === 'string'
              ? Buffer.from(obj.metadata, 'base64')
              : obj.metadata
          )
        : undefined,
      provider,
    })
  }

  /**
   * Convert to JSON representation
   * @returns {Record<string, any>}
   */
  toJSON() {
    const obj = /** @type {Record<string, any>} */ ({})

    if (this.contextID) {
      obj.contextID = Buffer.from(this.contextID).toString('base64')
    }
    if (this.metadata) {
      obj.metadata = Buffer.from(this.metadata).toString('base64')
    }
    if (this.provider) {
      obj.provider = {
        id: this.provider.id,
        addrs: this.provider.addrs || [],
      }
    }

    return obj
  }
}
