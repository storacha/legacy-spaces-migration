/**
 * Utility functions for working with claims
 */
import { base58btc } from 'multiformats/bases/base58'
import { CID } from 'multiformats/cid'

/**
 * Check if a location claim has the specified space DID
 * Handles different space field formats from the indexing service
 * 
 * @param {any} claim - Location claim from indexing service
 * @param {string} expectedSpaceDID - Expected space DID (e.g., "did:key:z6Mk...")
 * @returns {boolean}
 */
export function claimHasSpace(claim, expectedSpaceDID) {
  if (!claim.space) return false
  
  try {
    // Handle different space formats
    if (typeof claim.space === 'string') {
      return claim.space === expectedSpaceDID
    } else if (claim.space instanceof Uint8Array) {
      // Space is stored as raw bytes (multicodec-encoded public key)
      // Convert to did:key string using base58btc encoding
      const spaceDID = `did:key:${base58btc.encode(claim.space)}`
      return spaceDID === expectedSpaceDID
    } else if (claim.space.did && typeof claim.space.did === 'function') {
      return claim.space.did() === expectedSpaceDID
    } else if (claim.space.did && typeof claim.space.did === 'string') {
      return claim.space.did === expectedSpaceDID
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    console.warn(`    Error checking space for claim:`, message)
  }
  
  return false
}

/**
 * Extract space DID from a capability's nb.space field
 * Handles different space field formats from raw CBOR capability structures
 * 
 * @param {any} capability - Capability from CBOR block (with nb.space field)
 * @returns {string|null} - The space DID if found, null otherwise
 */
export function getSpaceFromCapability(capability) {
  if (!capability.nb || !capability.nb.space) return null
  
  try {
    let claimSpace = null
    
    // Handle different space formats
    if (typeof capability.nb.space === 'string') {
      claimSpace = capability.nb.space
    } else if (typeof capability.nb.space?.did === 'function') {
      claimSpace = capability.nb.space.did()
    } else if (typeof capability.nb.space?.did === 'string') {
      claimSpace = capability.nb.space.did
    } else if (capability.nb.space instanceof Uint8Array) {
      // Space is stored as raw bytes (multicodec-encoded public key)
      // Convert to did:key string using base58btc encoding
      claimSpace = `did:key:${base58btc.encode(capability.nb.space)}`
    }
    
    return claimSpace
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    console.warn(`    Error extracting space from capability:`, message)
    return null
  }
}

/**
 * Find location claims for a specific shard by matching its multihash
 * 
 * @param {any[]} allLocationClaims - All location claims from indexing service
 * @param {string} shardCID - Shard CID to match
 * @returns {any[]} - Location claims matching this shard
 */
export function findClaimsForShard(allLocationClaims, shardCID) {
  const cid = CID.parse(shardCID)
  const multihashStr = base58btc.encode(cid.multihash.bytes)
  
  return allLocationClaims.filter((claim) => {
    // Check if claim has content with multihash
    if (!claim.content) return false
    
    // Handle different content field structures
    let claimMultihash
    if (claim.content.multihash && claim.content.multihash.bytes) {
      claimMultihash = base58btc.encode(claim.content.multihash.bytes)
    } else if (claim.content.digest && claim.content.digest.bytes) {
      // Some claims might use 'digest' instead of 'multihash'
      claimMultihash = base58btc.encode(claim.content.digest.bytes)
    } else if (claim.content.digest) {
      // digest might be raw bytes
      claimMultihash = base58btc.encode(claim.content.digest)
    } else {
      return false
    }
    
    return claimMultihash === multihashStr
  })
}
