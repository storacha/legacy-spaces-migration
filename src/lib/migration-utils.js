/**
 * Utility functions for managing migration spaces
 * 
 * Handles creation, provisioning, and tracking of migration spaces
 * where legacy content indexes are stored.
 */
import { sha256 } from 'multiformats/hashes/sha2'
import * as Link from 'multiformats/link'
import * as Space from '@storacha/access/space'
import * as Signer from '@ucanto/principal/ed25519'
import { Verifier } from '@ucanto/principal'
import { delegate } from '@ucanto/core'
import { generateShardedIndex } from './index-worker.js'
import { getShardSize } from './tables/shard-data-table.js'
import {
  getMigrationSpace,
  createMigrationSpace,
  markSpaceAsProvisioned,
} from './tables/migration-spaces-table.js'
import { provisionSpace } from './tables/consumer-table.js'
import { encryptPrivateKey, decryptPrivateKey } from './crypto-utils.js'
import { storeDelegations } from './tables/delegations-table.js'
import { config } from '../config.js'

/**
 * Get or create migration space for a customer
 * Creates one migration space per customer (reused across script runs)
 * 
 * @param {string} customer - Customer account DID (did:mailto:...)
 * @returns {Promise<{space: import('@ucanto/principal/ed25519').Signer.EdSigner | null, isNew: boolean, spaceDID: string}>}
 */
export async function getOrCreateMigrationSpaceForCustomer(customer) {
  const existing = await getMigrationSpace(customer)
  
  if (existing && existing.privateKey) {
    // Decrypt and reconstruct signer from stored private key
    try {
      console.log(`    ✓ Migration space exists: ${existing.migrationSpace}`)
      const decryptedKey = decryptPrivateKey(existing.privateKey)
      const space = await Signer.parse(decryptedKey)
      
      return { space, isNew: false, spaceDID: existing.migrationSpace }
    } catch (error) {
      console.error(`    ✗ Failed to decrypt private key: ${error.message}`)
      throw new Error(`Cannot reuse migration space: decryption failed`)
    }
  }
  
  // Create new migration space
  const spaceName = `Migrated Indexes - ${customer}`
  const space = await Space.generate({ name: spaceName })
  
  // Encrypt private key for storage
  // Extract the secret key from the signer
  const privateKeyBytes = space.signer.secret || space.signer.bytes
  const encryptedKey = encryptPrivateKey(privateKeyBytes)
  
  // Store in tracking table with encrypted key
  await createMigrationSpace({
    customer,
    migrationSpace: space.did(),
    spaceName,
    privateKey: encryptedKey,
  })
  
  console.log(`    ✓ Created migration space: ${space.did()}`)
  return { space, isNew: true, spaceDID: space.did() }
}

/**
 * Provision migration space to customer account (direct DB write)
 * 
 * For migration scripts, we bypass the provider/add UCAN ceremony and write
 * directly to the consumer table since we have admin DB access.
 * 
 * @param {object} params
 * @param {string} params.customer - Customer account DID
 * @param {string} params.migrationSpaceDID - Migration space DID
 * @returns {Promise<void>}
 */
export async function provisionMigrationSpace({ customer, migrationSpaceDID }) {
  console.log(`    Provisioning space to customer account...`)
  
  // Add the space to the consumer table
  await provisionSpace(customer, migrationSpaceDID)
  
  // Mark as provisioned in our tracking table
  await markSpaceAsProvisioned(customer)
}

/**
 * Delegate migration space access to customer
 * 
 * Creates a delegation from the migration space to the customer account,
 * granting full space/* access.
 * The delegation is stored in DynamoDB + S3 for the customer to retrieve.
 * 
 * @param {object} params
 * @param {import('@ucanto/principal/ed25519').Signer.EdSigner} params.migrationSpace - Migration space signer
 * @param {string} params.migrationSpaceDID - Migration space DID
 * @param {string} params.customer - Customer account DID (did:mailto:...)
 * @param {import('@ucanto/interface').Link} [params.cause] - CID of invocation that triggered this delegation
 * @returns {Promise<void>}
 */
export async function delegateMigrationSpaceToCustomer({
  migrationSpace,
  migrationSpaceDID,
  customer,
  cause,
}) {
  // Create account principal (Absentee since we don't have their private key)
  const customerAccount = Verifier.from({ id: customer })
  
  // Delegate full space access from migration space to customer account
  const delegation = await delegate({
    issuer: migrationSpace,
    audience: customerAccount,
    capabilities: [
      {
        can: 'space/*',
        with: migrationSpaceDID,
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
    console.error(`    ✗ Failed to store delegation: ${error.message}`)
    throw new Error(`Failed to store delegation: ${error.message}`)
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
 *   indexCID: import('multiformats').CID,
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
    const size = await getShardSize(upload.space, shardCIDString)
    shards.push({
      cid: shardCIDString,
      size,
    })
    console.log(`    ✓ ${shardCIDString}: ${size} bytes`)
  }
  
  // Generate index using worker
  const result = await generateShardedIndex(upload.root, shards)
  const indexBytes = result.indexBytes
  
  // Calculate index CID
  const indexDigest = await sha256.digest(indexBytes)
  const indexCID = Link.create(0x0202, indexDigest) // CAR codec
  
  console.log(`    ✓ Generated index: ${indexCID} (${indexBytes.length} bytes)`)
  
  // Return shards with sizes for reuse downstream (avoids re-querying DynamoDB)
  return { indexBytes, indexCID, indexDigest, shards }
}
