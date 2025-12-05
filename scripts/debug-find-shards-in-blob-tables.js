#!/usr/bin/env node
/**
 * Debug script to check if we can find shards in the blob tables
 * This will help us understand if the shards from the index exist in allocations/store
 */
import { queryIndexingService } from '../src/lib/indexing-service.js'
import { getShardSize } from '../src/lib/tables/shard-data-table.js'
import { CID } from 'multiformats/cid'

const rootCID = process.argv[2]
const space = process.argv[3]

if (!rootCID || !space) {
  console.error('Usage: STORACHA_ENV=staging node debug-find-shards-in-blob-tables.js <root-cid> <space-did>')
  process.exit(1)
}

console.log(`Root CID: ${rootCID}`)
console.log(`Space: ${space}`)
console.log()

console.log(`Step 1: Query indexing service...`)
const indexingData = await queryIndexingService(rootCID)

if (!indexingData.hasIndexClaim) {
  console.log('❌ No index claim found')
  process.exit(1)
}

console.log(`✓ Index claim exists`)
console.log()

if (!indexingData.indexes || indexingData.indexes.size === 0) {
  console.log('❌ No index data returned')
  process.exit(1)
}

console.log(`Step 2: Extract shard CIDs from index...`)
const indexes = Array.from(indexingData.indexes.values())
const index = indexes[0]
const shardDigests = Array.from(index.shards.keys())

console.log(`Found ${shardDigests.length} shard(s) in index`)
console.log()

console.log(`Step 3: Convert to CIDs and check if they exist in blob tables...`)
for (const shardDigest of shardDigests) {
  const cid = CID.decode(shardDigest)
  const cidString = cid.toString()
  
  console.log(`\nShard: ${cidString}`)
  
  try {
    const size = await getShardSize(space, cidString)
    console.log(`  ✓ Found in blob tables`)
    console.log(`  Size: ${size} bytes`)
  } catch (error) {
    console.log(`  ✗ NOT found in blob tables`)
    console.log(`  Error: ${error.message}`)
  }
}
