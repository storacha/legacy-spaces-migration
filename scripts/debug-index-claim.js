#!/usr/bin/env node
/**
 * Debug script to check what the indexing service returns for a root CID
 * Includes detailed inspection of index structure and shards
 */
import { queryIndexingService } from '../src/lib/indexing-service.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'

const rootCID = process.argv[2]

if (!rootCID) {
  console.error('Usage: node scripts/debug-index-claim.js <root-cid>')
  process.exit(1)
}

console.log(`Querying indexing service for root CID: ${rootCID}`)
console.log()

const result = await queryIndexingService(rootCID)

console.log(`Has index claim: ${result.hasIndexClaim}`)
console.log(`Has location claim: ${result.hasLocationClaim}`)
console.log(`Location has space: ${result.locationHasSpace}`)
console.log(`Spaces: ${result.spaces.join(', ') || 'none'}`)
console.log(`Index CID: ${result.indexCID || 'none'}`)
console.log()

// --- Index Structure Details ---
if (result.indexes && result.indexes.size > 0) {
  console.log(`Found ${result.indexes.size} parsed index(es) in response`)
  console.log()

  for (const [contextID, index] of result.indexes.entries()) {
    console.log(`Index Context ID: ${contextID}`)
    console.log(`Number of shards in index: ${index.shards.size}`)
    console.log()
    
    console.log(`Shards referenced in index:`)
    for (const [shardDigest, slices] of index.shards.entries()) {
      try {
        // The shardDigest is the raw bytes of the multihash, but might be a Digest object
        const bytes = shardDigest.bytes || shardDigest
        const cid = CID.decode(bytes)
        console.log(`  - CID: ${cid.toString()}`)
        console.log(`    Multihash: ${base58btc.encode(cid.multihash.bytes)}`)
        console.log(`    Contains ${slices.size} content slice(s)`)
      } catch (e) {
        const bytes = shardDigest.bytes || shardDigest
        console.log(`  - Digest (hex): ${Buffer.from(bytes).toString('hex')}`)
        console.log(`    Contains ${slices.size} content slice(s)`)
      }
    }
    console.log()
  }
} else {
  console.log('⚠️ No parsed index data returned (might be missing or failed to parse)')
  console.log()
}

console.log(`Total claims: ${result.claims.length}`)
console.log()

const locationClaims = result.claims.filter(c => c.type === 'assert/location')
console.log(`Location Claims (${locationClaims.length}):`)
console.log()

for (const claim of locationClaims) {
  console.log(`─`.repeat(70))
  
  // Calculate multihash for comparison with index shards
  let multihashStr = 'unknown'
  try {
    // Handle both raw digest object and Uint8Array
    const digestValues = claim.content.digest
    const multihash = digestValues instanceof Uint8Array 
      ? digestValues 
      : new Uint8Array(Object.values(digestValues))
    multihashStr = base58btc.encode(multihash)
  } catch (e) {
    console.warn('Failed to decode claim content digest', e)
  }

  console.log(`  Multihash: ${multihashStr}`)
  // Show location(s)
  const locs = Array.isArray(claim.location) ? claim.location : [claim.location]
  locs.forEach(l => console.log(`  Location:  ${l}`))
  
  console.log(`  Has space: ${claim.space != null}`)
  if (claim.space) {
    console.log(`  Space:     ${claim.space.did ? claim.space.did() : claim.space}`)
  }
  console.log(`  Content:   ${JSON.stringify(claim.content, null, 0)}`)
}

const otherClaims = result.claims.filter(c => c.type !== 'assert/location')
if (otherClaims.length > 0) {
  console.log()
  console.log(`Other Claims (${otherClaims.length}):`)
  console.log()
  
  for (const claim of otherClaims) {
    console.log(`─`.repeat(70))
    console.log(`Claim type: ${claim.type}`)
    if (claim.type === 'assert/index') {
      console.log(`  Index CID:   ${claim.index}`)
      console.log(`  Content CID: ${claim.content}`)
    } else {
      console.log(JSON.stringify(claim, null, 2))
    }
  }
}
