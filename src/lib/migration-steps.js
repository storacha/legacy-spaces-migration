/**
 * Individual migration steps for legacy content
 * 
 * Each function handles one specific migration task:
 * 1. Check if migration is needed
 * 2. Generate DAG index (using index worker)
 * 3. Upload and register index
 * 4. Republish location claims with space
 * 5. Create gateway authorization
 */

import { sha256 } from 'multiformats/hashes/sha2'
import * as Link from 'multiformats/link'
import { generateShardedIndex } from './index-worker.js'
import { queryIndexingService } from './indexing-service.js'
import { getShardSize } from './tables/blob-registry-table.js'

/**
 * Determine what migration steps are needed for an upload
 * 
 * @param {object} upload - Upload from Upload Table
 * @param {string} upload.space - Space DID
 * @param {string} upload.root - Root CID
 * @param {string[]} upload.shards - Shard CIDs
 * @returns {Promise<{
 *   needsIndexGeneration: boolean,
 *   needsIndexRegistration: boolean,
 *   needsLocationClaims: boolean,
 *   needsGatewayAuth: boolean,
 *   hasIndexClaim: boolean,
 *   hasLocationClaim: boolean,
 *   locationHasSpace: boolean
 * }>}
 */
export async function checkMigrationNeeded(upload) {
  // Query indexing service for existing claims
  const indexingData = await queryIndexingService(upload.root)
  
  return {
    needsIndexGeneration: !indexingData.hasIndexClaim,
    needsIndexRegistration: !indexingData.hasIndexClaim,
    needsLocationClaims: !indexingData.hasLocationClaim || !indexingData.locationHasSpace,
    needsGatewayAuth: true, // Always create gateway auth for now
    hasIndexClaim: indexingData.hasIndexClaim,
    hasLocationClaim: indexingData.hasLocationClaim,
    locationHasSpace: indexingData.locationHasSpace,
  }
}

/**
 * Generate sharded DAG index using the index worker
 * 
 * @param {object} upload - Upload from Upload Table
 * @param {string} upload.space - Space DID
 * @param {string} upload.root - Root CID
 * @param {string[]} upload.shards - Shard CIDs
 * @returns {Promise<{indexBytes: Uint8Array, indexCID: import('multiformats').CID, indexDigest: import('multiformats').MultihashDigest}>}
 */
export async function generateDAGIndex(upload) {
  console.log(`  Generating DAG index for ${upload.root}...`)
  
  // Get shard sizes using three-table lookup
  console.log(`  Querying blob registry for shard sizes...`)
  const shards = []
  for (const shardCIDString of upload.shards) {
    const size = await getShardSize(upload.space, shardCIDString)
    shards.push({
      cid: shardCIDString,
      size,
    })
    console.log(`    ✓ ${shardCIDString}: ${size} bytes`)
  }
  
  // Generate index using worker
  const result = await generateShardedIndex(upload.root, shards)
  const indexBytes = result.indexBytes
  
  // Calculate index CID
  const indexDigest = await sha256.digest(indexBytes)
  const indexCID = Link.create(0x0202, indexDigest) // CAR codec
  
  console.log(`    ✓ Generated index: ${indexCID} (${indexBytes.length} bytes)`)
  
  return { indexBytes, indexCID, indexDigest }
}

/**
 * Upload index blob and register it via space/blob/add and space/index/add
 * 
 * @param {object} params
 * @param {string} params.space - Space DID
 * @param {Uint8Array} params.indexBytes - Index CAR bytes
 * @param {import('multiformats').CID} params.indexCID - Index CID
 * @param {import('multiformats').MultihashDigest} params.indexDigest - Index digest
 * @param {object} params.uploadClient - Upload client with credentials
 * @returns {Promise<void>}
 */
export async function uploadAndRegisterIndex({ space, indexBytes, indexCID, indexDigest, uploadClient }) {
  console.log(`  Uploading index blob...`)
  
  // TODO: Implement space/blob/add invocation
  // This requires:
  // 1. Create delegation with Absentee issuer for the space
  // 2. Service attests the delegation using ucan/attest
  // 3. Invoke space/blob/add with the attested delegation
  // 4. Upload bytes to the presigned URL
  
  console.log(`    TODO: Implement space/blob/add`)
  
  // TODO: Implement space/index/add invocation
  // This requires:
  // 1. Create delegation with Absentee issuer for the space
  // 2. Service attests the delegation
  // 3. Invoke space/index/add with the attested delegation
  // 4. This publishes assert/index claim to indexing service
  
  console.log(`    TODO: Implement space/index/add`)
}

// ============================================
// STEP 4: Republish Location Claims with Space
// ============================================

/**
 * Republish location claims with space information
 * 
 * @param {object} params
 * @param {string} params.space - Space DID
 * @param {string} params.root - Root CID
 * @param {string[]} params.shards - Shard CIDs
 * @param {object} params.uploadClient - Upload client with credentials
 * @returns {Promise<void>}
 */
export async function republishLocationClaims({ space, root, shards, uploadClient }) {
  console.log(`  Republishing location claims with space...`)
  
  // TODO: Implement location claim republishing
  // This requires:
  // 1. For each shard, create assert/location claim with space field
  // 2. Publish to indexing service
  // 3. The space field should be in the nb.space field of the capability
  
  console.log(`    TODO: Implement assert/location claim publishing`)
}

// ============================================
// STEP 5: Create Gateway Authorization
// ============================================

/**
 * Create gateway authorization delegation for the space
 * 
 * @param {object} params
 * @param {string} params.space - Space DID
 * @param {string} params.root - Root CID
 * @param {object} params.uploadClient - Upload client with credentials
 * @returns {Promise<void>}
 */
export async function createGatewayAuth({ space, root, uploadClient }) {
  console.log(`  Creating gateway authorization...`)
  
  // TODO: Implement gateway authorization
  // This requires:
  // 1. Create space/content/serve/* delegation
  // 2. Service attests the delegation using ucan/attest
  // 3. Store the delegation for gateway to use
  
  console.log(`    TODO: Implement gateway authorization`)
}
