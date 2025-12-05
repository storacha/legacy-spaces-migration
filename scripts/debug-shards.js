#!/usr/bin/env node
/**
 * Debug script to find shards for a root CID
 */
import { getShardSize } from '../src/lib/tables/shard-data-table.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'

const rootCID = process.argv[2]
const space = process.argv[3]

if (!rootCID || !space) {
  console.error('Usage: node debug-shards.js <root-cid> <space-did>')
  process.exit(1)
}

console.log(`Checking if root CID is also a shard...`)
console.log(`Root CID: ${rootCID}`)
console.log(`Space: ${space}`)
console.log()

try {
  // Try to get the size of the root CID as if it were a shard
  const size = await getShardSize(space, rootCID)
  console.log(`✅ Root CID IS a shard!`)
  console.log(`   Size: ${size} bytes`)
  console.log()
  console.log(`This is a single-shard upload where root CID === shard CID`)
} catch (error) {
  console.log(`❌ Root CID is NOT a shard`)
  console.log(`   Error: ${error.message}`)
  console.log()
  console.log(`This is a multi-shard upload. Shards need to be resolved from the DAG structure.`)
}
