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
 * 4. Gateway authorization status (from previous step result)
 * 
 * Note: Gateway authorization verification relies on the result from the
 * createGatewayAuth step. Full verification would require querying the
 * gateway's CloudFlare KV store, which is not easily accessible from this script.
 * 
 * @param {object} params
 * @param {object} params.upload - Upload record from Upload Table
 * @param {string} params.upload.space - Space DID
 * @param {string} params.upload.root - Root CID
 * @param {string[]} params.upload.shards - Shard CIDs
 * @param {object} [params.gatewayAuthResult] - Result from createGatewayAuth step
 * @param {boolean} [params.gatewayAuthResult.success] - Whether gateway auth succeeded
 * @returns {Promise<{
 *   success: boolean,
 *   indexVerified: boolean,
 *   locationClaimsVerified: boolean,
 *   allShardsHaveSpace: boolean,
 *   gatewayAuthVerified: boolean,
 *   shardsWithoutSpace: string[],
 *   details: string
 * }>}
 */
export async function verifyMigration({ upload, gatewayAuthResult }) {
  
  try {
    // Query indexing service to check current state
    // Note: Querying the root CID returns index claim AND location claims for shards
    const indexingData = await queryIndexingService(upload.root)
    
    // Verify index claim exists
    const indexVerified = indexingData.hasIndexClaim
    
    // Extract location claims
    const locationClaims = indexingData.claims.filter(c => c.type === 'assert/location')
    
    // Build map of shard multihash -> array of location claims
    // There may be multiple claims per shard (old without space, new with space)
    const shardLocationMap = new Map()
    for (const claim of locationClaims) {
      const contentMultihash = claim.content.multihash ?? Digest.decode(claim.content.digest)
      const multihashStr = base58btc.encode(contentMultihash.bytes)
      
      if (!shardLocationMap.has(multihashStr)) {
        shardLocationMap.set(multihashStr, [])
      }
      shardLocationMap.get(multihashStr).push(claim)
    }
    
    // Check each shard
    const shardsWithoutSpace = []
    let allShardsHaveLocationClaims = true
    
    for (const shardCID of upload.shards) {
      const cid = CID.parse(shardCID)
      const multihashStr = base58btc.encode(cid.multihash.bytes)
      const claims = shardLocationMap.get(multihashStr) || []
      
      if (claims.length === 0) {
        allShardsHaveLocationClaims = false
        shardsWithoutSpace.push(shardCID)
      } else {
        // Check if ANY claim has space information matching this upload's space
        const hasClaimWithSpace = claims.some(claim => 
          claim.space != null && claim.space.did() === upload.space
        )
        
        if (!hasClaimWithSpace) {
          shardsWithoutSpace.push(shardCID)
        }
      }
    }
    
    const locationClaimsVerified = allShardsHaveLocationClaims
    const allShardsHaveSpace = shardsWithoutSpace.length === 0
    
    // Gateway authorization verification (based on previous step result)
    // null = skipped (test mode), undefined = not attempted, false = failed, true = success
    const gatewayAuthVerified = gatewayAuthResult?.success ?? false
    const gatewayAuthSkipped = gatewayAuthResult === null
    
    const success = indexVerified && locationClaimsVerified && allShardsHaveSpace && gatewayAuthVerified
    
    let details = ''
    if (!success) {
      const issues = []
      if (!indexVerified) issues.push('index claim missing')
      if (!locationClaimsVerified) issues.push('location claims missing')
      if (!allShardsHaveSpace) issues.push(`${shardsWithoutSpace.length} shards missing space info`)
      if (!gatewayAuthVerified && !gatewayAuthSkipped) issues.push('gateway authorization failed')
      details = issues.join(', ')
    }
    
    // Summary
    console.log(`  Index claim:           ${indexVerified ? '✓ verified' : '✗ failed'}`)
    console.log(`  Location claims:       ${locationClaimsVerified ? '✓ verified' : '✗ failed'}`)
    console.log(`  Space information:     ${allShardsHaveSpace ? '✓ verified' : '✗ failed'}`)
    if (gatewayAuthSkipped) {
      console.log(`  Gateway authorization: ⏭ skipped`)
    } else {
      console.log(`  Gateway authorization: ${gatewayAuthVerified ? '✓ verified' : '✗ failed'}`)
    }
    console.log(`\n  Result: ${success ? '✓ PASSED' : '✗ FAILED'}${!success ? ` (${details})` : ''}`)
    
    return {
      success,
      indexVerified,
      locationClaimsVerified,
      allShardsHaveSpace,
      gatewayAuthVerified,
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
      gatewayAuthVerified: false,
      shardsWithoutSpace: upload.shards,
      details: `Verification error: ${error.message}`,
    }
  }
}
