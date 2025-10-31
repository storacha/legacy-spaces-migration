import { queryIndexingService } from './indexing-service.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'

/**
 * Verify that all migration steps completed successfully
 * 
 * Re-queries the indexing service to confirm:
 * 1. Index claim exists for the root CID
 * 2. Location claims exist for all shards
 * 3. Location claims include space information
 * 
 * Note: Gateway authorization verification is not included as it would require
 * querying the gateway's CloudFlare KV store, which is not easily accessible
 * from this script. We rely on the access/delegate invocation response to
 * confirm the delegation was stored.
 * 
 * @param {object} params
 * @param {object} params.upload - Upload record from Upload Table
 * @param {string} params.upload.space - Space DID
 * @param {string} params.upload.root - Root CID
 * @param {string[]} params.upload.shards - Shard CIDs
 * @returns {Promise<{
 *   success: boolean,
 *   indexVerified: boolean,
 *   locationClaimsVerified: boolean,
 *   allShardsHaveSpace: boolean,
 *   shardsWithoutSpace: string[],
 *   details: string
 * }>}
 */
export async function verifyMigration({ upload }) {
  console.log(`  Verifying migration...`)
  console.log(`    Root: ${upload.root}`)
  console.log(`    Space: ${upload.space}`)
  console.log(`    Shards: ${upload.shards.length}`)
  
  try {
    // Query indexing service to check current state
    const indexingData = await queryIndexingService(upload.root)
    
    // Verify index claim exists
    const indexVerified = indexingData.hasIndexClaim
    console.log(`    Index claim: ${indexVerified ? '✓ EXISTS' : '✗ MISSING'}`)
    
    // Extract location claims
    const locationClaims = indexingData.claims.filter(c => c.type === 'assert/location')
    console.log(`    Location claims found: ${locationClaims.length}`)
    
    // Build map of shard multihash -> location claim with space info
    const shardLocationMap = new Map()
    for (const claim of locationClaims) {
      const contentMultihash = claim.content.multihash ?? Digest.decode(claim.content.digest)
      const hasSpace = claim.space != null && claim.space === upload.space
      shardLocationMap.set(base58btc.encode(contentMultihash.bytes), { claim, hasSpace })
    }
    
    // Check each shard
    const shardsWithoutSpace = []
    let allShardsHaveLocationClaims = true
    
    for (const shardCID of upload.shards) {
      const cid = CID.parse(shardCID)
      const multihashStr = base58btc.encode(cid.multihash.bytes)
      const locationInfo = shardLocationMap.get(multihashStr)
      
      if (!locationInfo) {
        allShardsHaveLocationClaims = false
        shardsWithoutSpace.push(shardCID)
        console.log(`    ✗ ${shardCID}: no location claim`)
      } else if (!locationInfo.hasSpace) {
        shardsWithoutSpace.push(shardCID)
        console.log(`    ✗ ${shardCID}: location claim missing space field`)
      } else {
        console.log(`    ✓ ${shardCID}: location claim with space`)
      }
    }
    
    const locationClaimsVerified = allShardsHaveLocationClaims
    const allShardsHaveSpace = shardsWithoutSpace.length === 0
    const success = indexVerified && locationClaimsVerified && allShardsHaveSpace
    
    // Summary
    console.log(`\n  Verification Summary:`)
    console.log(`    Index: ${indexVerified ? '✓ VERIFIED' : '✗ FAILED'}`)
    console.log(`    Location claims: ${locationClaimsVerified ? '✓ VERIFIED' : '✗ FAILED'}`)
    console.log(`    Space information: ${allShardsHaveSpace ? '✓ VERIFIED' : '✗ FAILED'}`)
    console.log(`    Shards without space: ${shardsWithoutSpace.length}/${upload.shards.length}`)
    console.log(`    Overall: ${success ? '✓ PASSED' : '✗ FAILED'}`)
    
    let details = ''
    if (!success) {
      const issues = []
      if (!indexVerified) issues.push('index claim missing')
      if (!locationClaimsVerified) issues.push('location claims missing')
      if (!allShardsHaveSpace) issues.push(`${shardsWithoutSpace.length} shards missing space info`)
      details = issues.join(', ')
    }
    
    return {
      success,
      indexVerified,
      locationClaimsVerified,
      allShardsHaveSpace,
      shardsWithoutSpace,
      details: details || 'All verification checks passed',
    }
  } catch (error) {
    console.error(`    ✗ Verification failed: ${error.message}`)
    return {
      success: false,
      indexVerified: false,
      locationClaimsVerified: false,
      allShardsHaveSpace: false,
      shardsWithoutSpace: upload.shards,
      details: `Verification error: ${error.message}`,
    }
  }
}
