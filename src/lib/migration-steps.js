/**
 * Individual migration steps for legacy content
 * 
 * Each function handles one specific migration task:
 * 1. Check if migration is needed
 * 2. Build, upload and register index
 * 3. Republish location claims with space info
 * 4. Create gateway content/serve authorization
 */

import { Blob as SpaceBlob, Index } from '@storacha/upload-client'
import * as Signer from '@ucanto/principal/ed25519'
import { connect } from '@ucanto/client'
import * as CAR from '@ucanto/transport/car'
import * as HTTP from '@ucanto/transport/http'
import { Assert } from '@web3-storage/content-claims/capability'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import { queryIndexingService } from './indexing-service.js'
import { getCustomerForSpace } from './tables/consumer-table.js'
import { incrementIndexCount } from './tables/migration-spaces-table.js'
import { getShardSize } from './tables/shard-data-table.js'
import {
  getOrCreateMigrationSpaceForCustomer,
  provisionMigrationSpace,
  delegateMigrationSpaceToCustomer,
  generateDAGIndex,
} from './migration-utils.js'
import { config } from '../config.js'

/**
 * Determine what migration steps are needed for an upload by checking the indexing service
 * for existing claims.
 * 
 * @param {object} upload - Upload from Upload Table
 * @param {string} upload.space - Space DID
 * @param {string} upload.root - Root CID
 * @param {string[]} upload.shards - Shard CIDs
 * @returns {Promise<{
 *   needsIndexGeneration: boolean,
 *   needsIndexRegistration: boolean,
 *   needsLocationClaims: boolean,
 *   needsGatewayAuth: boolean,
 *   hasIndexClaim: boolean,
 *   hasLocationClaim: boolean,
 *   locationHasSpace: boolean,
 *   shardsNeedingLocationClaims: string[]
 * }>}
 */
export async function checkMigrationNeeded(upload) {
  // Query root CID - this returns index claim AND location claims for shards
  const indexingData = await queryIndexingService(upload.root)
  
  // Extract location claims from the response
  const locationClaims = indexingData.claims.filter(c => c.type === 'assert/location')
  
  console.log(`  Checking location claims for ${upload.shards.length} shards...`)
  console.log(`    Found ${locationClaims.length} location claims from indexing service`)
  
  // Build map of shard multihash -> location claim with space info
  const shardLocationMap = new Map()
  
  for (const claim of locationClaims) {
    // Get the content multihash from the claim
    const contentMultihash = claim.content.multihash ?? Digest.decode(claim.content.digest)
    const hasSpace = claim.space != null
    shardLocationMap.set(base58btc.encode(contentMultihash.bytes), { claim, hasSpace })
  }
  
  // Check each shard to see if it needs a location claim or needs space info added
  const shardsNeedingLocationClaims = []
  for (const shardCID of upload.shards) {
    const cid = CID.parse(shardCID)
    const multihashStr = base58btc.encode(cid.multihash.bytes)
    
    const locationInfo = shardLocationMap.get(multihashStr)
    if (!locationInfo) {
      // No location claim exists for this shard
      shardsNeedingLocationClaims.push(shardCID)
      console.log(`    ✗ ${shardCID}: no location claim found`)
    } else if (!locationInfo.hasSpace) {
      // Location claim exists but doesn't have space field
      shardsNeedingLocationClaims.push(shardCID)
      console.log(`    ✗ ${shardCID}: location claim missing space field`)
    }
  }
  
  return {
    needsIndexGeneration: !indexingData.hasIndexClaim,
    needsIndexRegistration: !indexingData.hasIndexClaim,
    needsLocationClaims: shardsNeedingLocationClaims.length > 0,
    needsGatewayAuth: true, // Always create gateway auth for now
    hasIndexClaim: indexingData.hasIndexClaim,
    hasLocationClaim: indexingData.hasLocationClaim,
    locationHasSpace: indexingData.locationHasSpace,
    shardsNeedingLocationClaims, // Only shards that need republishing
  }
}

/**
 * Build, upload and register index
 * 
 * Generates a sharded DAG index, then uploads it to a migration space.
 * Creates one migration space per customer where all their legacy indexes are stored.
 * This provides clean proof chains and clear visibility for customers.
 * 
 * @param {object} params
 * @param {object} params.upload - Upload record from Upload Table
 * @param {string} params.upload.space - Original space DID (where content was uploaded)
 * @param {string} params.upload.root - Root CID (content to index)
 * @param {string[]} params.upload.shards - Shard CIDs (CAR files containing the content)
 * @returns {Promise<{migrationSpace: string, indexCID: import('multiformats').CID}>}
 *   - migrationSpace: DID of the migration space where index was uploaded
 *   - indexCID: CID of the generated index
 */
export async function buildAndMigrateIndex({ upload }) {
  console.log(`  Building and migrating index...`)
  console.log(`    Original space: ${upload.space}`)
  console.log(`    Content root: ${upload.root}`)
  
  if (!config.credentials.servicePrivateKey) {
    throw new Error('SERVICE_PRIVATE_KEY not configured')
  }

  // Generate sharded DAG index for the content
  const { indexBytes, indexCID, indexDigest } = await generateDAGIndex(upload)
  
  const serviceSigner = await Signer.parse(config.credentials.servicePrivateKey)
  const serviceURL = new URL(config.services.uploadService)
  const connection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: serviceURL })
  })
  
  console.log(`    Looking up customer for original space...`)
  const customer = await getCustomerForSpace(upload.space)
  if (!customer) {
    throw new Error(`No customer found for space ${upload.space}`)
  }
  
  console.log(`    Getting/creating migration space for customer...`)
  const { space: migrationSpace, isNew, spaceDID: migrationSpaceDID } = await getOrCreateMigrationSpaceForCustomer(customer)
  
  // Verify we have the migration space signer
  if (!migrationSpace) {
    throw new Error('Migration space not available - this should never happen!')
  }
  console.log(`    Migration space: ${migrationSpaceDID} ${isNew ? '(newly created)' : '(existing)'}`)

  // Provision migration space to customer (only if newly created)
  if (isNew) {
    await provisionMigrationSpace({
      customer,
      migrationSpaceDID,
    })
  }
  
  // Upload index blob to migration space via space/blob/add
  console.log(`    Uploading index blob (${indexBytes.length} bytes)...`)
  try {
    await SpaceBlob.add(
      {
        issuer: migrationSpace,      // Sign with migration space key
        with: migrationSpaceDID,     // Upload to migration space
        audience: connection.id,
      },
      indexDigest,
      indexBytes,
      { connection }
    )
    console.log(`    ✓ Index blob uploaded to migration space`)
  } catch (error) {
    throw new Error(`Failed to upload index blob: ${error.message}`)
  }
  
  // CRITICAL: Publish location claim for the index CAR itself BEFORE registering
  // The indexing service needs to fetch the index CAR, so it needs a location claim
  console.log(`    Publishing location claim for index CAR...`)
  try {
    await republishLocationClaims({
      space: migrationSpaceDID,
      shards: [indexCID.toString()],
    })
    console.log(`    ✓ Index CAR location claim published`)
  } catch (error) {
    throw new Error(`Failed to publish index CAR location claim: ${error.message}`)
  }
  
  // Register index via space/index/add (publishes assert/index claim)
  console.log(`    Registering index with indexing service...`)
  let indexInvocation
  try {
    indexInvocation = await Index.add(
      {
        issuer: migrationSpace,      // Sign with migration space key
        with: migrationSpaceDID,     // Register for migration space
        audience: connection.id,
      },
      indexCID,
      { connection }
    )
    console.log(`    ✓ Index registered (assert/index claim published)`)
  } catch (error) {
    throw new Error(`Failed to register index: ${error.message}`)
  }
  
  // Increment index count in tracking table
  await incrementIndexCount(customer)
  
  // Delegate full access to customer (only if newly created space)
  if (isNew) {
    console.log(`    Delegating migration space access to customer...`)
    await delegateMigrationSpaceToCustomer({
      migrationSpace,
      migrationSpaceDID,
      customer,
      cause: indexInvocation.cid, // Link delegation to index registration
    })
  }
  
  return { migrationSpace: migrationSpaceDID, indexCID }
}

/**
 * Republish location claims with space information
 * 
 * Publishes location claims signed directly with the service private key, including
 * space DIDs. This enables the freeway to properly track egress per space.
 * 
 * @param {object} params
 * @param {string} params.space - Space DID
 * @param {string[]} params.shards - Shard CIDs (CAR files) to republish
 * @param {Array<{cid: string, size: number}>} [params.shardsWithSizes] - Optional shard info from previous step (avoids re-querying DynamoDB)
 * @returns {Promise<void>}
 */
export async function republishLocationClaims({ space, shards, shardsWithSizes }) {
  console.log(`  Republishing ${shards.length} location claims with space...`)
  
  if (!config.credentials.servicePrivateKey) {
    throw new Error('SERVICE_PRIVATE_KEY not configured')
  }

  // Setup service signer and connection to claims service
  const serviceSigner = await Signer.parse(config.credentials.servicePrivateKey)
  const claimsServiceURL = new URL(config.services.contentClaims)
  const claimsConnection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: claimsServiceURL })
  })

  // Create lookup map if shardsWithSizes provided (optimization to avoid re-querying DynamoDB)
  const shardSizeMap = shardsWithSizes
    ? new Map(shardsWithSizes.map(s => [s.cid, s.size]))
    : null

  // Republish location claim for each shard
  for (const shardCID of shards) {
    try {
      // Get shard size - use cached value if available, otherwise query DynamoDB
      const size = shardSizeMap?.get(shardCID) ?? await getShardSize(space, shardCID)
      
      // Parse CID to get digest
      const cid = CID.parse(shardCID)
      const digest = cid.multihash.bytes
      
      // Construct location URL from carpark
      const location = `${config.storage.carparkPublicUrl}/${shardCID}`
      
      // Invoke directly with service signer (simpler than Absentee + attestation)
      const result = await Assert.location.invoke({
        issuer: serviceSigner,
        audience: claimsConnection.id,
        with: serviceSigner.did(),
        nb: {
          content: { digest },
          location: [location],
          range: { offset: 0, length: size },
          space,  // space field included to enable egress tracking
        },
        expiration: Infinity,
      }).execute(claimsConnection)

      if (result.out.error) {
        throw new Error(
          `Failed to republish location claim: ${result.out.error.message}`
        )
      }
      
      console.log(`    ✓ ${shardCID}`)
    } catch (error) {
      console.error(`    ✗ ${shardCID}: ${error.message}`)
      throw error
    }
  }
  
  console.log(`    ✓ Successfully republished ${shards.length} location claims`)

  // TODO: Clean up old location claims without space information
  // This requires:
  // 1. Query content-claims service for existing location claims for each shard
  // 2. Find claims without space field (old claims)  
  // 3. DELETE that record somehow (not sure how the deletion operation happens)
  // 4. Verify only new claims (with space) remain
  // NOTE: Need to confirm DELETE API exists in content-claims service
}

/**
 * Create gateway authorization delegation for the space
 * 
 * @param {object} params
 * @param {string} params.space - Space DID
 * @param {string} params.root - Root CID
 * @param {object} params.uploadClient - Upload client with credentials
 * @returns {Promise<void>}
 */
export async function createGatewayAuth({ space, root, uploadClient }) {
  console.log(`  Creating gateway authorization...`)
  
  // TODO: Implement gateway authorization
  // This requires:
  // 1. Create space/content/serve/* delegation
  // 2. Service attests the delegation using ucan/attest
  // 3. Store the delegation for gateway to use
  
  console.log(`    TODO: Implement gateway authorization`)
}
