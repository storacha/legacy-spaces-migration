#!/usr/bin/env node
/**
 * Debug script to see what shards are resolved from indexing service
 */
import { queryIndexingService } from '../src/lib/indexing-service.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'

const rootCID = process.argv[2]

if (!rootCID) {
  console.error('Usage: STORACHA_ENV=staging node debug-resolved-shards.js <root-cid>')
  process.exit(1)
}

console.log(`Resolving shards from indexing service for root: ${rootCID}`)
console.log()

const indexingData = await queryIndexingService(rootCID)

console.log(`Has index claim: ${indexingData.hasIndexClaim}`)
console.log(`Index CID: ${indexingData.indexCID || 'none'}`)
console.log()

const locationClaims = indexingData.claims.filter(c => c.type === 'assert/location')
console.log(`Found ${locationClaims.length} location claims`)
console.log()

// Extract shard CIDs from location claims (same logic as migrate.js)
const shardCIDs = []
for (const claim of locationClaims) {
  const digest = claim.content.digest
  const multihash = new Uint8Array(Object.values(digest))
  const multihashStr = base58btc.encode(multihash)
  
  const cid = CID.createV1(0x70, multihash) // 0x70 = dag-pb
  shardCIDs.push(cid.toString())
  
  console.log(`Shard: ${cid.toString()}`)
  console.log(`  Multihash: ${multihashStr}`)
  console.log(`  Location: ${claim.location}`)
  console.log(`  Has space: ${claim.space != null}`)
  if (claim.space) {
    console.log(`  Space: ${claim.space.did ? claim.space.did() : claim.space}`)
  }
  console.log()
}

console.log(`Total resolved shards: ${shardCIDs.length}`)
console.log(`Shards array: ${JSON.stringify(shardCIDs, null, 2)}`)
