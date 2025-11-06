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
import * as ed25519 from '@ucanto/principal/ed25519'
import { connect } from '@ucanto/client'
import * as CAR from '@ucanto/transport/car'
import * as HTTP from '@ucanto/transport/http'
import { Absentee } from '@ucanto/principal'
import * as UCAN from '@storacha/capabilities/ucan'
import { Assert } from '@web3-storage/content-claims/capability'
import { Access, Space as SpaceCapabilities } from '@storacha/capabilities'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import { queryIndexingService } from './indexing-service.js'
import { getCustomerForSpace } from './tables/consumer-table.js'
import { incrementIndexCount } from './tables/migration-spaces-table.js'
import { getShardSize } from './tables/shard-data-table.js'
import { DID } from '@ucanto/core'
import {
  getOrCreateMigrationSpaceForCustomer,
  delegateMigrationSpaceToCustomer,
  generateDAGIndex,
} from './migration-utils.js'
import { config, getClaimsSigner } from '../config.js'

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
  
  // Build map of shard multihash -> array of location claims
  // There may be multiple claims per shard (old without space, new with space)
  const shardLocationMap = new Map()
  
  for (const claim of locationClaims) {
    // Get the content multihash from the claim
    const contentMultihash = claim.content.multihash ?? Digest.decode(claim.content.digest)
    const multihashStr = base58btc.encode(contentMultihash.bytes)
    
    if (!shardLocationMap.has(multihashStr)) {
      shardLocationMap.set(multihashStr, [])
    }
    shardLocationMap.get(multihashStr).push(claim)
  }
  
  // Check each shard to see if it needs a location claim or needs space info added
  const shardsNeedingLocationClaims = []
  for (const shardCID of upload.shards) {
    const cid = CID.parse(shardCID)
    const multihashStr = base58btc.encode(cid.multihash.bytes)
    
    const claims = shardLocationMap.get(multihashStr) || []
    if (claims.length === 0) {
      // No location claim exists for this shard
      shardsNeedingLocationClaims.push(shardCID)
      console.log(`    ✗ ${shardCID}: no location claim found`)
    } else {
      // Check if ANY claim has space information
      const hasClaimWithSpace = claims.some(claim => claim.space != null)
      
      if (!hasClaimWithSpace) {
        // Location claims exist but none have space field
        shardsNeedingLocationClaims.push(shardCID)
        console.log(`    ✗ ${shardCID}: location claim missing space field`)
      }
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
    indexCID: indexingData.indexCID, // CID of the existing index (if any)
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
  
  // Upload index blob to migration space via space/blob/add
  console.log(`    Uploading index blob (${indexBytes.length} bytes)...`)
  
  // Parse the upload service DID for the audience
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID)
  
  // Get the signer from the migration space
  const spaceSigner = migrationSpace.signer || migrationSpace
  
  try {
    await SpaceBlob.add(
      {
        issuer: spaceSigner,         // Sign with the space's signer (owner can self-authorize)
        with: migrationSpaceDID,     // Upload to migration space
        audience: uploadServiceDID,  // Audience is the upload service DID
        proofs: [],                  // Empty proofs - space owner self-authorizes
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
        issuer: spaceSigner,         // Sign with the space's signer (owner can self-authorize)
        with: migrationSpaceDID,     // Register for migration space
        audience: uploadServiceDID,  // Audience is the upload service DID
        proofs: [],                  // Empty proofs - space owner self-authorizes
      },
      indexCID,
      { connection }
    )
    console.log(`    ✓ Index registered (assert/index claim published)`)
  } catch (error) {
    console.log(`    DEBUG: Index.add threw error:`, error.message)
    console.log(`    DEBUG: Error cause:`, JSON.stringify(error.cause, null, 2))
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
 * Register an existing index with the indexing service
 * This notifies the indexing service to pull in any new location claims
 * 
 * @param {object} params
 * @param {object} params.upload - Upload object with space and root
 * @param {import('multiformats').CID} params.indexCID - CID of the index to register
 */
export async function registerIndex({ upload, indexCID }) {
  console.log(`    DEBUG: registerIndex called with:`)
  console.log(`    - upload.space: ${upload.space} (type: ${typeof upload.space})`)
  console.log(`    - indexCID: ${indexCID} (type: ${typeof indexCID})`)
  
  if (!config.credentials.servicePrivateKey) {
    throw new Error('SERVICE_PRIVATE_KEY not configured')
  }

  const serviceSigner = await Signer.parse(config.credentials.servicePrivateKey)
  const serviceURL = new URL(config.services.uploadService)
  const connection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: serviceURL })
  })
  
  // Get migration space for this upload
  console.log(`    DEBUG: About to call getCustomerForSpace with: ${upload.space}`)
  const customer = await getCustomerForSpace(upload.space)
  if (!customer) {
    throw new Error(`No customer found for space ${upload.space}`)
  }
  
  const { space: migrationSpace, spaceDID: migrationSpaceDID } = await getOrCreateMigrationSpaceForCustomer(customer)
  
  console.log(`    DEBUG: migrationSpace type:`, typeof migrationSpace, migrationSpace?.constructor?.name)
  console.log(`    DEBUG: migrationSpaceDID:`, migrationSpaceDID)
  console.log(`    DEBUG: has signer:`, !!migrationSpace?.signer)
  
  // Register index via space/index/add (publishes assert/index claim)
  // Get the signer from the migration space
  const spaceSigner = migrationSpace.signer || migrationSpace
  
  console.log(`    DEBUG: config.services:`, JSON.stringify(config.services, null, 2))
  console.log(`    DEBUG: uploadServiceDID from config:`, config.services.uploadServiceDID)
  
  if (!config.services.uploadServiceDID) {
    throw new Error('uploadServiceDID not configured in config.services')
  }
  
  // Parse the upload service DID for the audience
  const { DID } = await import('@ucanto/core')
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID)
  
  console.log(`    DEBUG: parsed uploadServiceDID:`, uploadServiceDID.did())
  console.log(`    DEBUG: spaceSigner DID:`, spaceSigner.did())
  
  let result
  try {
    result = await Index.add(
      {
        issuer: spaceSigner,         // Sign with the space's signer (owner can self-authorize)
        with: migrationSpaceDID,     // Register index for migration space
        audience: uploadServiceDID,  // Audience is the upload service DID
        proofs: [],                  // Empty proofs - space owner self-authorizes
      },
      indexCID,
      { connection }
    )
  } catch (error) {
    console.log(`    DEBUG: Index.add threw error:`, error.message)
    console.log(`    DEBUG: Error cause:`, JSON.stringify(error.cause, null, 2))
    throw error
  }
  
  console.log(`    DEBUG: Index.add result:`, JSON.stringify(result, null, 2))
  console.log(`    Index registration result:`, result.out.ok ? '✓ Success' : `✗ Error: ${JSON.stringify(result.out.error)}`)
  
  if (result.out.error) {
    throw new Error(`Failed to register index: ${result.out.error.message || JSON.stringify(result.out.error)}`)
  }
  
  return result
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
  
  // Get claims service signer with the correct did:web identity
  const claimsSigner = await getClaimsSigner()
  
  const claimsServiceURL = new URL(config.services.contentClaims)
  const claimsConnection = connect({
    id: claimsSigner,
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
      
      // Invoke with claims service signer
      const result = await Assert.location.invoke({
        issuer: claimsSigner,
        audience: claimsConnection.id,
        with: claimsSigner.did(),
        nb: {
          content: { digest },
          location: [location],
          range: { offset: 0, length: size },
          space: ed25519.Verifier.parse(space),  // space field included to enable egress tracking
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
 * Creates a `space/content/serve/*` delegation for legacy spaces that don't have
 * existing gateway authorizations. Uses the Absentee issuer pattern since we don't
 * have the space's private key.
 * 
 * The delegation is published to the gateway's KV store via `access/delegate` invocation.
 * The gateway will use this delegation to authorize content serving requests.
 * 
 * **Idempotency Note**: Calling this multiple times for the same space will create
 * multiple delegations in the KV store with different CIDs. The KV store key is
 * `${space}:${delegation.cid}`, so each invocation creates a new entry. This is safe
 * but not optimal. In a future iteration, we could check existing delegations via
 * the KV store API before creating new ones.
 * 
 * @param {object} params
 * @param {string} params.space - Space DID
 * @returns {Promise<void>}
 */
export async function createGatewayAuth({ space }) {
  if (!config.credentials.servicePrivateKey) {
    throw new Error('SERVICE_PRIVATE_KEY not configured')
  }

  // Setup service signer and gateway connection
  const serviceSigner = await Signer.parse(config.credentials.servicePrivateKey)
  const gatewayServiceURL = new URL(config.services.gatewayService)
  const gatewayConnection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: gatewayServiceURL })
  })
  
  // Gateway DID (audience for the delegation)
  const gatewayDID = config.gateway.did
  
  // Create principal object for ucanto
  const gatewayPrincipal = {
    did: () => gatewayDID
  }

  try {
    // Create delegation with Absentee issuer (space doesn't have private key)
    // This grants the gateway the ability to serve content from this space
    const delegation = await SpaceCapabilities.contentServe.delegate({
      issuer: Absentee.from({ id: space }),
      audience: gatewayPrincipal,  // Principal with did() method
      with: space,
      nb: {},  // No additional caveats - allows serving all content
      expiration: Infinity,
    })
    console.log(`  Delegation created:    ✓ ${delegation.cid}`)

    // Service attests the delegation (proves service authorized this)
    const attestation = await UCAN.attest.delegate({
      issuer: serviceSigner,
      audience: serviceSigner,
      with: serviceSigner.did(),
      nb: { proof: delegation.cid },
      expiration: Infinity,
    })
    console.log(`  Attestation created:   ✓ ${attestation.cid}`)

    // Publish to gateway via access/delegate
    // The service is publishing to its own KV store, so 'with' should be the service DID
    const result = await Access.delegate.invoke({
      issuer: serviceSigner,
      audience: gatewayPrincipal,  // Principal with did() method
      with: serviceSigner.did(),  // Service is acting on its own behalf, not the space's
      nb: {
        delegations: {
          [delegation.cid.toString()]: delegation.cid,
        },
      },
      proofs: [delegation, attestation],
    }).execute(gatewayConnection)

    if (result.out.error) {
      throw new Error(
        `Failed to publish gateway delegation: ${result.out.error.message}`
      )
    }
    
    console.log(`  Publishing to gateway: ✓ published`)
    return { success: true }
  } catch (error) {
    // Log error but don't fail the migration - gateway auth can be retried independently
    console.log(`  Publishing to gateway: ✗ FAILED`)
    console.log(`\n  Error: ${error.message}`)
    console.log(`\n  Note: This can be retried independently`)
    // Don't throw - allow migration to continue
    return { success: false, error: error.message }
  }
}