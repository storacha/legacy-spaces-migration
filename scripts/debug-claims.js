#!/usr/bin/env node

/**
 * Debug script to query and decode claims from the claims service
 * 
 * Usage:
 *   STORACHA_ENV=staging node debug-claims.js <cid>
 * 
 * Example:
 *   STORACHA_ENV=staging node debug-claims.js bagbaiera7vycsplauivbkhibstd2vhkz6gamc6yb5uacw6jkirsufrmt3oka
 */

import { Client } from '@storacha/indexing-service-client'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'

const cid = process.argv[2]
if (!cid) {
  console.error('Usage: node debug-claims.js <cid>')
  process.exit(1)
}

const indexingServiceURL = process.env.STORACHA_ENV === 'staging' 
  ? 'https://staging.indexer.storacha.network'
  : 'https://indexer.storacha.network'

console.log(`\nQuerying indexing service for: ${cid}`)
console.log(`Indexing Service: ${indexingServiceURL}`)
console.log(`${'='.repeat(80)}\n`)

try {
  // Use the same method as the migration script
  const parsedCid = CID.parse(cid)
  const digest = Digest.decode(parsedCid.multihash.bytes)
  
  console.log(`Parsed CID: ${parsedCid}`)
  console.log(`Multihash (base58btc): ${base58btc.encode(parsedCid.multihash.bytes)}\n`)
  
  const client = new Client({
    serviceURL: new URL(indexingServiceURL)
  })
  
  console.log(`Querying with indexing service client...\n`)
  
  // Query WITHOUT space filter (legacy context)
  console.log(`[1] Querying WITHOUT space filter (legacy context)...`)
  const resultWithoutSpace = await client.queryClaims({
    hashes: [digest],
    kind: 'standard'
  })
  
  if (resultWithoutSpace.error) {
    console.error(`Error: ${resultWithoutSpace.error.message}`)
  } else {
    const legacyClaims = Array.from(resultWithoutSpace.ok.claims.values())
    console.log(`   Found ${legacyClaims.length} claim(s) in legacy context\n`)
  }
  
  // Query WITH space filter (new context)
  const targetSpace = 'did:key:z6MkpuiC8Piy7HfssEe9RqqN4ZZEYkXYuV5Zvo7FfDMHpPek'
  console.log(`[2] Querying WITH space filter for: ${targetSpace}...`)
  const result = await client.queryClaims({
    hashes: [digest],
    match: { subject: [targetSpace] },
    kind: 'standard'
  })
  
  if (result.error) {
    console.error(`Error: ${result.error.message}`)
    process.exit(1)
  }
  
  const claims = Array.from(result.ok.claims.values())
  
  console.log(`Found ${claims.length} claim(s):\n`)
  console.log(`${'='.repeat(80)}`)
  
  for (let i = 0; i < claims.length; i++) {
    const claim = claims[i]
    console.log(`\nClaim ${i + 1}:`)
    console.log(`  Type: ${claim.type}`)
    
    if (claim.type === 'assert/location') {
      console.log(`  Location:`, claim.location)
      console.log(`  Range:`, claim.range)
      console.log(`  Space:`, claim.space ? claim.space.did() : '(none)')
    } else if (claim.type === 'assert/index') {
      console.log(`  CID:`, claim.content?.toString())
      console.log(`  Index:`, claim.index?.toString())
    }
    
    // Print all keys
    console.log(`  Keys:`, Object.keys(claim))
    
    console.log(`${'─'.repeat(80)}`)
  }
  
  console.log(`\n✓ Successfully decoded ${claims.length} claim(s)`)
  
  // Check if our migrated space is in the claims
  const hasTargetSpace = claims.some(c => 
    c.type === 'assert/location' && c.space && c.space.did() === targetSpace
  )
  
  if (hasTargetSpace) {
    console.log(`\n✅ Found location claim with target space: ${targetSpace}`)
  } else {
    console.log(`\n❌ No location claim found with target space: ${targetSpace}`)
    console.log(`   This means the claim hasn't propagated to the claims service yet.`)
  }
  
} catch (error) {
  console.error(`Error: ${error.message}`)
  console.error(error.stack)
  process.exit(1)
}
