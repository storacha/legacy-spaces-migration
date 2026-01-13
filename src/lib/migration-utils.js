import { Absentee } from '@ucanto/principal'

/**
 * Utility functions for managing migration spaces
 * 
 * Handles creation, provisioning, and tracking of migration spaces
 * where legacy content indexes are stored.
 */

/**
 * Migration step constants
 * Tracks which step of the migration process is currently executing
 */
export const STEP = {
  /** Initial state before migration starts */
  INIT: 'INIT',
  /** Analyzing what migration steps are needed */
  ANALYZE: 'ANALYZE',
  /** Generating and registering sharded DAG index */
  INDEX_GENERATION: 'INDEX_GENERATION',
  /** Republishing location claims with space information */
  LOCATION_CLAIMS: 'LOCATION_CLAIMS',
  /** Creating gateway authorization delegations */
  GATEWAY_AUTH: 'GATEWAY_AUTH',
  /** Verifying all migration steps completed successfully by retrieving the content */
  VERIFY: 'VERIFY',
}

/**
 * Failure reason constants
 * Categorizes why a migration failed for easy filtering and debugging
 */
export const FAILURE_REASON = {
  /** Generic error - unable to categorize the failure */
  UNKNOWN_ERROR: 'UNKNOWN_ERROR',
  
  /** Failed during migration analysis step (checking what needs to be done) */
  ANALYSIS_FAILED: 'ANALYSIS_FAILED',
  
  /** Failed to generate or register the sharded DAG index
   * Common causes:
   * - Index worker unavailable
   * - Failed to upload index blob to migration space
   * - Failed to publish assert/index claim to indexing service
   */
  INDEX_GENERATION_FAILED: 'INDEX_GENERATION_FAILED',
  
  /** Failed to republish location claims with space information
   * Common causes:
   * - Failed to query shard sizes from DynamoDB (shard X not found in allocations or store table for space Y)
   * - Failed to publish assert/location claim to indexing service
   * - Failed to send IPNI advertisement to queue
   */
  LOCATION_CLAIM_FAILED: 'LOCATION_CLAIM_FAILED',
  
  /** Failed to create gateway authorization (excluding missing delegation)
   * Common causes:
   * - Failed to create Absentee delegations
   * - Failed to create service attestations
   * - Failed to invoke access/delegate on gateway
   */
  GATEWAY_AUTH_FAILED: 'GATEWAY_AUTH_FAILED',
  
  /** Space has no delegation to an account (legacy space without owner)
   * This is the most common failure for legacy spaces.
   * These spaces were created before the delegation system existed.
   * Resolution: Manual delegation creation or space transfer to new owner.
   */
  MISSING_DELEGATION: 'MISSING_DELEGATION',
  
  /** Verification failed: Index claim not found in indexing service
   * The index was supposedly created but can't be verified.
   * May indicate indexing service lag or failed index registration.
   */
  INDEX_MISSING: 'INDEX_MISSING',
  
  /** Verification failed: Location claims not found for one or more shards
   * Location claims were supposedly published but can't be verified.
   * May indicate indexing service lag or failed claim publication.
   */
  LOCATION_CLAIMS_MISSING: 'LOCATION_CLAIMS_MISSING',
  
  /** Verification failed: Location claims exist but lack space information
   * Claims were published but the space field is missing.
   * May indicate bug in republishLocationClaims function.
   */
  SPACE_INFO_MISSING: 'SPACE_INFO_MISSING',
  
  /** Verification failed for unknown reason
   * All steps completed but verification still failed.
   * Check verification details for more information.
   */
  VERIFICATION_FAILED: 'VERIFICATION_FAILED',
  
  /** Upload has no shards in database and no index claim
   * Cannot migrate because there's no way to determine what content to migrate.
   * This indicates data corruption or incomplete upload.
   */
  NO_SHARDS_NO_INDEX: 'NO_SHARDS_NO_INDEX',
  
  /** Indexing service returned 500 error during analysis
   * The indexing service failed to process the query, likely due to incomplete
   * provider addresses in legacy location claims (missing {blob} or {blobCID} endpoint).
   * Upload is skipped to avoid migration failures.
   */
  INDEXING_SERVICE_500: 'INDEXING_SERVICE_500',
}

import { sha256 } from 'multiformats/hashes/sha2'
import * as Link from 'multiformats/link'
import { Space } from '@storacha/access'
import { OwnedSpace } from '@storacha/access/space'
import * as Signer from '@ucanto/principal/ed25519'
import { delegate } from '@ucanto/core'
import { generateShardedIndex } from './index-worker.js'
import { getShardInfo } from './tables/shard-data-table.js'
import {
  getMigrationSpace,
  createMigrationSpace,
  markSpaceAsProvisioned,
} from './tables/migration-spaces-table.js'
import { provisionSpace } from './tables/consumer-table.js'
import { encryptPrivateKey, decryptPrivateKey } from './crypto-utils.js'
import { storeDelegations } from './tables/delegations-table.js'
import { getErrorMessage } from './error-utils.js'

/**
 * Get or create migration space for a customer
 * Creates one migration space per customer (reused across script runs)
 * 
 * @param {string} customer - Customer account DID (did:mailto:...)
 * @returns {Promise<{space: import('@storacha/access').OwnedSpace, isNew: boolean, spaceDID: string}>}
 */
export async function getOrCreateMigrationSpaceForCustomer(customer) {
  const existing = await getMigrationSpace(customer)
  
  if (existing && existing.privateKey) {
    // Decrypt and reconstruct signer from stored private key
    try {
      console.log(`    ✓ Migration space exists: ${existing.migrationSpace}`)
      const decryptedKey = decryptPrivateKey(existing.privateKey)
      const signer = await Signer.parse(decryptedKey)
      
      // Wrap the signer in an OwnedSpace object to match the type returned by Space.generate()
      const space = new OwnedSpace({
        signer,
        name: existing.spaceName,
      })
      
      return { space, isNew: false, spaceDID: existing.migrationSpace }
    } catch (error) {
      console.error(`    ✗ Failed to decrypt private key`, error)
      throw new Error(`Cannot reuse migration space: decryption failed: ${getErrorMessage(error)}`)
    }
  }
  
  // Create new migration space
  const spaceName = `Migrated Indexes - ${customer}`
  const migrationSpace = await Space.generate({ name: spaceName })
  
  // Encrypt private key for storage
  // Format signer as multibase string (same format that Signer.parse() expects)
  const privateKeyString = Signer.format(migrationSpace.signer)
  const encryptedKey = encryptPrivateKey(privateKeyString)
  
  // Store in tracking table with encrypted key
  await createMigrationSpace({
    customer,
    migrationSpace,
    spaceName,
    privateKey: encryptedKey,
  })
  
  console.log(`    ✓ Created migration space: ${migrationSpace.did()}`)
  
  // Provision the migration space to the customer
  await provisionMigrationSpace({
    customer,
    migrationSpace,
  })
  
  return { space: migrationSpace, isNew: true, spaceDID: migrationSpace.did() }
}

/**
 * Provision migration space to customer account (direct DB write)
 * 
 * For migration scripts, we bypass the provider/add UCAN ceremony and write
 * directly to the consumer table since we have admin DB access.
 * 
 * @param {object} params
 * @param {string} params.customer - Customer account DID
 * @param {import('@storacha/access').OwnedSpace} params.migrationSpace - Migration space
 * @returns {Promise<void>}
 */
export async function provisionMigrationSpace({ customer, migrationSpace }) {
  console.log(`    Provisioning space ${migrationSpace.did()} to customer account ${customer}...`)
  
  // Add the space to the consumer table
  await provisionSpace(customer, migrationSpace.did())
  
  // Mark as provisioned in our tracking table
  await markSpaceAsProvisioned(customer)

  console.log(`    ✓ Provisioned space ${migrationSpace.did()} to customer account ${customer}`)
}

/**
 * Delegate migration space access to customer
 * 
 * Creates a delegation from the migration space to the customer account,
 * granting full space/* access.
 * The delegation is stored in DynamoDB + S3 for the customer to retrieve.
 * 
 * @param {object} params
 * @param {import('@storacha/access').OwnedSpace} params.migrationSpace - Migration space signer
 * @param {string} params.customer - Customer account DID (did:mailto:...)
 * @param {import('@ucanto/interface').Link} [params.cause] - CID of invocation that triggered this delegation
 * @returns {Promise<void>}
 */
export async function delegateMigrationSpaceToCustomer({
  migrationSpace,
  customer,
  cause,
}) {
  // Only delegate to did:mailto accounts (email-based accounts)
  if (!customer.startsWith('did:mailto:')) {
    throw new Error(`Cannot delegate to ${customer} - only did:mailto accounts are supported`)
  }
  
  // Create account principal (Absentee since we don't have their private key)
  const customerAccount = Absentee.from({ id: /** @type {`did:${string}:${string}`} */ (customer) })
  console.log(`    Creating Migration Space delegation for customer account: ${customerAccount.did()}`)
  
  // Delegate full space access from migration space to customer account
  const delegation = await delegate({
    issuer: migrationSpace.signer,
    audience: customerAccount,
    capabilities: [
      {
        can: 'space/*',
        with: migrationSpace.did(),
      }
    ],
    expiration: Infinity,
  })
  
  console.log(`    ✓ Created delegation: ${delegation.cid}`)
  
  // Store delegation in DynamoDB + S3
  try {
    await storeDelegations([delegation], { cause })
    console.log(`    ✓ Delegation stored - customer can now access migration space`)
  } catch (error) {
    console.error('    ✗ Failed to store delegation')
    console.error(error)
    throw new Error(`Failed to store delegation: ${getErrorMessage(error)}`, {cause: error})
  }
}

/**
 * Generate sharded DAG index using the index worker
 * 
 * @param {object} upload - Upload from Upload Table
 * @param {string} upload.space - Space DID
 * @param {string} upload.root - Root CID
 * @param {string[]} upload.shards - Shard CIDs
 * @returns {Promise<{
 *   indexBytes: Uint8Array,
 *   indexCID: import('@storacha/access').CARLink,
 *   indexDigest: import('multiformats').MultihashDigest,
 *   shards: Array<{cid: string, size: number}>
 * }>}
 */
export async function generateDAGIndex(upload) {
  console.log(`  Generating DAG index for ${upload.root}...`)
  
  // Get shard sizes using three-table lookup
  console.log(`  Querying blob registry for shard sizes...`)
  const shards = []
  for (const shardCIDString of upload.shards) {
    const info = await getShardInfo(upload.space, shardCIDString)
    shards.push({
      cid: shardCIDString,
      size: info.size,
      protocol: info.protocol,
    })
    console.log(`    ✓ ${shardCIDString}: ${info.size} bytes (${info.protocol})`)
  }
  
  // Generate index using worker
  // Worker only needs cid and size
  const result = await generateShardedIndex(upload.root, shards)
  const indexBytes = result.indexBytes
  
  // Calculate index CID
  const indexDigest = await sha256.digest(indexBytes)
  const indexCID = Link.create(0x0202, indexDigest) // CAR codec (TODO: use raw code)
  
  console.log(`    ✓ Generated index: ${indexCID} (${indexBytes.length} bytes)`)
  
  // Return shards with sizes for reuse downstream (avoids re-querying DynamoDB)
  return { indexBytes, indexCID, indexDigest, shards }
}

/**
 * Verify that a resource (blob or car) exists in R2 storage before publishing a location claim
 * 
 * This handles incomplete uploads where allocation exists in DynamoDB but
 * the actual file was never uploaded to R2.
 * 
 * Works for both:
 * - Blob protocol: .blob files (from allocations table)
 * - Store protocol: .car files (from store table)
 * 
 * @param {string} locationURI - Full URL to the blob or car file in R2
 * @returns {Promise<{exists: boolean, error?: string}>}
 */
export async function verifyResourceExists(locationURI) {
  try {
    const headRes = await fetch(locationURI, { method: 'HEAD' })
    if (!headRes.ok) {
      return { exists: false, error: `HTTP ${headRes.status}` }
    }
    return { exists: true }
  } catch (fetchError) {
    return { exists: false, error: getErrorMessage(fetchError) }
  }
}
