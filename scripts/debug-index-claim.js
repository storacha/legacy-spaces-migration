#!/usr/bin/env node
/**
 * Debug script to check what the indexing service returns for a root CID
 */
import { queryIndexingService } from '../src/lib/indexing-service.js'

const rootCID = process.argv[2]

if (!rootCID) {
  console.error('Usage: node debug-index-claim.js <root-cid>')
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

console.log(`Total claims: ${result.claims.length}`)
console.log()

for (const claim of result.claims) {
  console.log(`─`.repeat(70))
  console.log(`Claim type: ${claim.type}`)
  
  if (claim.type === 'assert/index') {
    console.log(`  Index CID: ${claim.index}`)
    console.log(`  Content CID: ${claim.content}`)
  } else if (claim.type === 'assert/location') {
    console.log(`  Content: ${JSON.stringify(claim.content, null, 2)}`)
    console.log(`  Location: ${claim.location}`)
    console.log(`  Has space: ${claim.space != null}`)
    if (claim.space) {
      console.log(`  Space: ${claim.space.did ? claim.space.did() : claim.space}`)
    }
  }
  console.log()
}
