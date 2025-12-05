/**
 * Verification functions for migration steps
 * Checks that all migration steps completed successfully
 */
import { queryIndexingService } from './indexing-service.js'
import { getErrorMessage } from './error-utils.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import { claimHasSpace, findClaimsForShard } from './claim-utils.js'

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
 * @param {{success: boolean, [key: string]: any}|null} [params.gatewayAuthResult] - Result from createGatewayAuth step
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
    // First, verify index claim exists by querying the root CID
    const rootData = await queryIndexingService(upload.root)
    const indexVerified = rootData.hasIndexClaim
    
    // Check each content shard to see if it has a location claim with the correct space
    // We need to query each shard individually since location claims are indexed by shard multihash
    const shardsWithoutSpace = []
    let allShardsHaveLocationClaims = true
    
    for (const shardCID of upload.shards) {
      // Query the indexing service for THIS shard specifically
      const shardData = await queryIndexingService(shardCID)
      const allLocationClaims = shardData.claims.filter(c => c.type === 'assert/location')
      const cid = CID.parse(shardCID)
      const multihashStr = base58btc.encode(cid.multihash.bytes)
      
      console.log(`  Checking shard: ${shardCID}`)
      console.log(`    Multihash: ${multihashStr}`)
      console.log(`    Total location claims from indexer: ${allLocationClaims.length}`)
      
      // Find location claims for this shard using shared utility (same logic as checkMigrationNeeded)
      const shardLocationClaims = findClaimsForShard(allLocationClaims, shardCID)
      
      console.log(`    Matching location claims for this shard: ${shardLocationClaims.length}`)
      
      if (shardLocationClaims.length === 0) {
        allShardsHaveLocationClaims = false
        shardsWithoutSpace.push(shardCID)
        console.log(`    ✗ No location claims found for this shard`)
      } 
      else {
        // Check if at least one claim has the correct space using shared utility (same logic as checkMigrationNeeded)
        const hasClaimWithSpace = shardLocationClaims.some(claim => claimHasSpace(claim, upload.space))
        if (!hasClaimWithSpace) {
          console.log(`    ⚠️  Shard ${shardCID} missing location claim with space information in indexer`)
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
    
    // Success requires all checks to pass, but gateway auth can be skipped
    const success = indexVerified && locationClaimsVerified && allShardsHaveSpace && (gatewayAuthVerified || gatewayAuthSkipped)
    
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
    
    if (success) {
      console.log(`\n  ${'━'.repeat(35)}`)
      console.log(`  ✅ Result: COMPLETED`)
      console.log(`  ${'━'.repeat(35)}`)
    } else {
      console.log(`\n  ${'━'.repeat(35)}`)
      console.log(`  ❌ Result: FAILED`)
      console.log(`  ${'━'.repeat(35)}`)
    }
    
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
    console.error(`    ✗ Verification failed: ${getErrorMessage(error)}`, { cause: error })
    return {
      success: false,
      indexVerified: false,
      locationClaimsVerified: false,
      allShardsHaveSpace: false,
      gatewayAuthVerified: false,
      shardsWithoutSpace: upload.shards,
      details: `Verification error: ${getErrorMessage(error)}`,
    }
  }
}
