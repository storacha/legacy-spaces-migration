#!/usr/bin/env node
/**
 * Debug script to inspect the index structure and see what shards it references
 */
import { queryIndexingService } from '../src/lib/indexing-service.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'

const rootCID = process.argv[2]

if (!rootCID) {
  console.error('Usage: STORACHA_ENV=staging node debug-index-structure.js <root-cid>')
  process.exit(1)
}

console.log(`Querying indexing service for root: ${rootCID}`)
console.log()

const indexingData = await queryIndexingService(rootCID)

console.log(`Has index claim: ${indexingData.hasIndexClaim}`)
console.log(`Index CID: ${indexingData.indexCID || 'none'}`)
console.log()

if (!indexingData.indexes || indexingData.indexes.size === 0) {
  console.log('❌ No index data returned in query result')
  process.exit(1)
}

console.log(`Found ${indexingData.indexes.size} index(es)`)
console.log()

for (const [contextID, index] of indexingData.indexes.entries()) {
  console.log(`Index Context ID: ${contextID}`)
  console.log(`Number of shards in index: ${index.shards.size}`)
  console.log()
  
  console.log(`Shards referenced in index:`)
  for (const [shardDigest, slices] of index.shards.entries()) {
    // The shardDigest is the raw bytes, need to convert to CID
    const cid = CID.decode(shardDigest)
    console.log(`  - ${cid.toString()}`)
    console.log(`    Multihash: ${base58btc.encode(cid.multihash.bytes)}`)
    console.log(`    Contains ${slices.size} content slice(s)`)
  }
  console.log()
}

console.log(`\nNow let's compare with location claims:`)
const locationClaims = indexingData.claims.filter(c => c.type === 'assert/location')
console.log(`Found ${locationClaims.length} location claim(s)`)
for (const claim of locationClaims) {
  const digest = claim.content.digest
  const multihash = new Uint8Array(Object.values(digest))
  const multihashStr = base58btc.encode(multihash)
  console.log(`  - Multihash: ${multihashStr}`)
  console.log(`    Location: ${claim.location[0]}`)
  console.log(`    Has space: ${claim.space != null}`)
  if (claim.space) {
    console.log(`    Space: ${claim.space.did ? claim.space.did() : claim.space}`)
  }
}
