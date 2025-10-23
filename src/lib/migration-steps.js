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
import { queryIndexingService } from './indexing-service.js'
import { getCustomerForSpace } from './tables/consumer-table.js'
import { incrementIndexCount } from './tables/migration-spaces-table.js'
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
 *   locationHasSpace: boolean
 * }>}
 */
export async function checkMigrationNeeded(upload) {
  const indexingData = await queryIndexingService(upload.root)
  
  return {
    needsIndexGeneration: !indexingData.hasIndexClaim,
    needsIndexRegistration: !indexingData.hasIndexClaim,
    needsLocationClaims: !indexingData.hasLocationClaim || !indexingData.locationHasSpace,
    needsGatewayAuth: true, // Always create gateway auth for now
    hasIndexClaim: indexingData.hasIndexClaim,
    hasLocationClaim: indexingData.hasLocationClaim,
    locationHasSpace: indexingData.locationHasSpace,
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
 * @param {object} params
 * @param {string} params.space - Space DID
 * @param {string} params.root - Root CID
 * @param {string[]} params.shards - Shard CIDs
 * @param {object} params.uploadClient - Upload client with credentials
 * @returns {Promise<void>}
 */
export async function republishLocationClaims({ space, root, shards, uploadClient }) {
  console.log(`  Republishing location claims with space...`)
  
  // TODO: Implement location claim republishing
  // This requires:
  // 1. For each shard, create assert/location claim with space field
  // 2. Publish to indexing service
  // 3. The space field should be in the nb.space field of the capability
  
  console.log(`    TODO: Implement assert/location claim publishing`)
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
