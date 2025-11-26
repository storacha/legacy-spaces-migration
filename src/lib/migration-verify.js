import { queryIndexingService } from './indexing-service.js'
import { verifyLocationClaimWithSpace } from './tables/content-claims-table.js'
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
    // First, verify index claim exists by querying the root CID
    const rootData = await queryIndexingService(upload.root)
    const indexVerified = rootData.hasIndexClaim
    
    // Get all location claims from the root query
    const allLocationClaims = rootData.claims.filter(c => c.type === 'assert/location')
    
    // Check each shard to see if it has a location claim with the correct space
    const shardsWithoutSpace = []
    let allShardsHaveLocationClaims = true
    
    for (const shardCID of upload.shards) {
      const cid = CID.parse(shardCID)
      const multihashStr = base58btc.encode(cid.multihash.bytes)
      
      console.log(`  Checking shard: ${shardCID}`)
      console.log(`    Multihash: ${multihashStr}`)
      console.log(`    Total location claims from indexer: ${allLocationClaims.length}`)
      
      // Find location claims for this shard by matching multihash
      const shardLocationClaims = allLocationClaims.filter((claim) => {
        if (!claim.content || !claim.content.multihash || !claim.content.multihash.bytes) {
          return false
        }
        const claimContentMultihash = base58btc.encode(claim.content.multihash.bytes)
        return claimContentMultihash === multihashStr
      })
      
      console.log(`    Matching location claims for this shard: ${shardLocationClaims.length}`)
      
      if (shardLocationClaims.length === 0) {
        allShardsHaveLocationClaims = false
        shardsWithoutSpace.push(shardCID)
        console.log(`    ✗ No location claims found for this shard`)
      } else {
        // Check if at least one claim has the correct space
        const hasClaimWithSpace = shardLocationClaims.some(claim => {
          if (!claim.space) return false
          
          try {
            // Handle different space formats
            if (typeof claim.space === 'string') {
              return claim.space === upload.space
            } else if (typeof claim.space.did === 'function') {
              return claim.space.did() === upload.space
            }
          } catch (err) {
            console.warn(`    Error checking space for claim:`, err.message)
          }
          return false
        })
        
        if (!hasClaimWithSpace) {
          // Fallback: Check Content Claims Service (DynamoDB) directly
          // The indexing service might be lagging behind due to caching/propagation
          console.log(`    ⚠️  Shard ${shardCID} missing space in indexer, checking claims service...`)
          try {
            const verifyResult = await verifyLocationClaimWithSpace(shardCID, upload.space)
            if (verifyResult.hasClaimWithSpace) {
              console.log(`    ✅ Found claim with space in claims service!`)
            } else {
              shardsWithoutSpace.push(shardCID)
            }
          } catch (err) {
            console.warn(`    Failed to check claims service: ${err.message}`)
            shardsWithoutSpace.push(shardCID)
          }
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
