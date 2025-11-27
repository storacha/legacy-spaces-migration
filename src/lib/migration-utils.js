/**
 * Utility functions for managing migration spaces
 * 
 * Handles creation, provisioning, and tracking of migration spaces
 * where legacy content indexes are stored.
 */
import { sha256 } from 'multiformats/hashes/sha2'
import * as Link from 'multiformats/link'
import * as Space from '@storacha/access'
import { SpaceDID } from '@storacha/access'
import * as Signer from '@ucanto/principal/ed25519'
import { Verifier } from '@ucanto/principal/ed25519'
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
import { getErrorMessage } from './error-utils.js'

/**
 * Get or create migration space for a customer
 * Creates one migration space per customer (reused across script runs)
 * 
 * @param {string} customer - Customer account DID (did:mailto:...)
 * @returns {Promise<{space: import('@ucanto/principal/ed25519').Signer.EdSigner, isNew: boolean, spaceDID: string}>}
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
      const space = new Space.OwnedSpace({
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
 * @param {SpaceDID} params.migrationSpace - Migration space DID
 * @returns {Promise<void>}
 */
export async function provisionMigrationSpace({ customer, migrationSpace }) {
  console.log(`    Provisioning space ${migrationSpace.did()} to customer account ${customer}...`)
  
  // Add the space to the consumer table
  await provisionSpace(customer, migrationSpace)
  
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
 * @param {import('@ucanto/principal/ed25519').Signer.EdSigner} params.migrationSpace - Migration space signer
 * @param {SpaceDID} params.migrationSpaceDID - Migration space DID
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
  const customerAccount = Verifier.parse(customer)
  console.log(`    Creating Migration Space delegation for customer account: ${customerAccount.did()}`)
  
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
