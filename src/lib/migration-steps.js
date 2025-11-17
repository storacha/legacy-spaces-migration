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
import { DID } from '@ucanto/core'
import { connect } from '@ucanto/client'
import * as CAR from '@ucanto/transport/car'
import * as HTTP from '@ucanto/transport/http'
import { Absentee } from '@ucanto/principal'
import * as UCAN from '@storacha/capabilities/ucan'
import { Assert } from '@web3-storage/content-claims/capability'
import {
  Access as AccessCapabilities,
  Space as SpaceCapabilities
} from '@storacha/capabilities'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import { queryIndexingService } from './indexing-service.js'
import { getCustomerForSpace } from './tables/consumer-table.js'
import { incrementIndexCount } from './tables/migration-spaces-table.js'
import { getShardSize } from './tables/shard-data-table.js'
import { findDelegationByIssuer } from './tables/delegations-table.js'
import {
  getOrCreateMigrationSpaceForCustomer,
  delegateMigrationSpaceToCustomer,
  generateDAGIndex
} from './migration-utils.js'
import {
  config,
  getClaimsSigner,
  getGatewaySigner,
  getUploadServiceSigner
} from '../config.js'
import { content } from '@storacha/capabilities/space/blob'

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
export async function checkMigrationNeeded (upload) {
  // Query root CID - this returns index claim AND location claims for shards
  const indexingData = await queryIndexingService(upload.root)

  // Extract location claims from the response
  const locationClaims = indexingData.claims.filter(
    (c) => c.type === 'assert/location'
  )

  console.log(
    `  Checking location claims for ${upload.shards.length} shards...`
  )
  console.log(
    `    Found ${locationClaims.length} location claims from indexing service`
  )

  // Build map of shard multihash -> array of location claims
  // There may be multiple claims per shard (old without space, new with space)
  const shardLocationMap = new Map()

  for (const claim of locationClaims) {
    // Get the content multihash from the claim
    const contentMultihash =
      claim.content.multihash ?? Digest.decode(claim.content.digest)
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
      const hasClaimWithSpace = claims.some((claim) => claim.space != null)

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
    indexCID: indexingData.indexCID // CID of the existing index (if any)
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
export async function buildAndMigrateIndex ({ upload }) {
  console.log('  Building and migrating index...')
  console.log(`    Original space: ${upload.space}`)
  console.log(`    Content root: ${upload.root}`)

  // Generate sharded DAG index for the content
  const { indexBytes, indexCID, indexDigest } = await generateDAGIndex(upload)

  const serviceSigner = await getUploadServiceSigner()
  const serviceURL = new URL(config.services.uploadServiceURL)
  const connection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: serviceURL })
  })

  console.log('    Looking up customer for original space...')
  const customer = await getCustomerForSpace(upload.space)
  if (!customer) {
    throw new Error(`No customer found for space ${upload.space}`)
  }

  console.log('    Getting/creating migration space for customer...')
  const {
    space: migrationSpace,
    isNew,
    spaceDID: migrationSpaceDID
  } = await getOrCreateMigrationSpaceForCustomer(customer)

  // Verify we have the migration space signer
  if (!migrationSpace) {
    throw new Error(
      'Migration space not available - this should never happen!'
    )
  }
  console.log(
    `    Migration space: ${migrationSpaceDID} ${
      isNew ? '(newly created)' : '(existing)'
    }`
  )

  // Upload index blob to migration space via space/blob/add
  console.log(`    Uploading index blob (${indexBytes.length} bytes)...`)

  // Parse the upload service DID for the audience
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID)

  // Get the signer from the migration space
  const spaceSigner = migrationSpace.signer || migrationSpace

  try {
    await SpaceBlob.add(
      {
        issuer: spaceSigner, // Sign with the space's signer (owner can self-authorize)
        with: migrationSpaceDID, // Upload to migration space
        audience: uploadServiceDID, // Audience is the upload service DID
        proofs: [] // Empty proofs - space owner self-authorizes
      },
      indexDigest,
      indexBytes,
      { connection }
    )
    console.log('    ✓ Index blob uploaded to migration space')
  } catch (error) {
    throw new Error(`Failed to upload index blob: ${error.message}`)
  }

  // CRITICAL: Publish location claim for the index CAR itself BEFORE registering
  // The indexing service needs to fetch the index CAR, so it needs a location claim
  console.log('    Publishing location claim for index CAR...')
  try {
    await republishLocationClaims({
      space: migrationSpaceDID,
      shards: [indexCID.toString()]
    })
    console.log('    ✓ Index CAR location claim published')
  } catch (error) {
    throw new Error(
      `Failed to publish index CAR location claim: ${error.message}`
    )
  }

  // Register index via space/index/add (publishes assert/index claim)
  console.log('    Registering index with indexing service...')

  let indexInvocation
  try {
    indexInvocation = await Index.add(
      {
        issuer: spaceSigner, // Sign with the space's signer (owner can self-authorize)
        with: migrationSpaceDID, // Register for migration space
        audience: uploadServiceDID, // Audience is the upload service DID
        proofs: [] // Empty proofs - space owner self-authorizes
      },
      indexCID,
      { connection }
    )
    console.log('    ✓ Index registered (assert/index claim published)')
  } catch (error) {
    console.log('    DEBUG: Index.add threw error:', error.message)
    console.log(
      '    DEBUG: Error cause:',
      JSON.stringify(error.cause, null, 2)
    )
    throw new Error(`Failed to register index: ${error.message}`)
  }

  // Increment index count in tracking table
  await incrementIndexCount(customer)

  // Delegate full access to customer (only if newly created space)
  if (isNew) {
    console.log('    Delegating migration space access to customer...')
    await delegateMigrationSpaceToCustomer({
      migrationSpace,
      migrationSpaceDID,
      customer,
      cause: indexInvocation.cid // Link delegation to index registration
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
export async function registerIndex ({ upload, indexCID }) {
  console.log('    DEBUG: registerIndex called with:')
  console.log(
    `    - upload.space: ${upload.space} (type: ${typeof upload.space})`
  )
  console.log(`    - indexCID: ${indexCID} (type: ${typeof indexCID})`)

  const serviceSigner = await getUploadServiceSigner()
  const serviceURL = new URL(config.services.uploadServiceURL)
  const connection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: serviceURL })
  })

  // Get migration space for this upload
  const customer = await getCustomerForSpace(upload.space)
  if (!customer) {
    throw new Error(`No customer found for space ${upload.space}`)
  }

  const { space: migrationSpace, spaceDID: migrationSpaceDID } =
    await getOrCreateMigrationSpaceForCustomer(customer)

  // Register index via space/index/add (publishes assert/index claim)
  // Get the signer from the migration space
  const spaceSigner = migrationSpace.signer || migrationSpace
  if (!config.services.uploadServiceDID) {
    throw new Error('uploadServiceDID not configured in config.services')
  }

  // Parse the upload service DID for the audience
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID)
  let result
  try {
    result = await Index.add(
      {
        issuer: spaceSigner, // Sign with the space's signer (owner can self-authorize)
        with: migrationSpaceDID, // Register index for migration space
        audience: uploadServiceDID, // Audience is the upload service DID
        proofs: [] // Empty proofs - space owner self-authorizes
      },
      indexCID,
      { connection }
    )
  } catch (error) {
    console.error('Index.add threw error:', error.message)
    throw error
  }

  console.log(
    '    Index registration result:',
    result.out.ok ? '✓ Success' : `✗ Error: ${JSON.stringify(result.out.error)}`
  )

  if (result.out.error) {
    throw new Error(
      `Failed to register index: ${
        result.out.error.message || JSON.stringify(result.out.error)
      }`
    )
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
export async function republishLocationClaims ({
  space,
  shards,
  shardsWithSizes
}) {
  console.log(`  Republishing ${shards.length} location claims with space...`)

  // Get claims service signer with the correct did:web identity
  const claimsSigner = await getClaimsSigner()

  const claimsServiceURL = new URL(config.services.contentClaimsServiceURL)
  const claimsConnection = connect({
    id: claimsSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: claimsServiceURL })
  })

  // Create lookup map if shardsWithSizes provided (optimization to avoid re-querying DynamoDB)
  const shardSizeMap = shardsWithSizes
    ? new Map(shardsWithSizes.map((s) => [s.cid, s.size]))
    : null

  // Republish location claim for each shard
  for (const shardCID of shards) {
    try {
      // Get shard size - use cached value if available, otherwise query DynamoDB
      const size =
        shardSizeMap?.get(shardCID) ?? (await getShardSize(space, shardCID))

      // Parse CID to get digest
      const cid = CID.parse(shardCID)
      const digest = cid.multihash.bytes

      // Construct location URL from carpark
      const location = `${config.storage.carparkPublicUrl}/${shardCID}`

      // Invoke with claims service signer
      const result = await Assert.location
        .invoke({
          issuer: claimsSigner,
          audience: claimsConnection.id,
          with: claimsSigner.did(),
          nb: {
            content: { digest },
            location: [location],
            range: { offset: 0, length: size },
            space: ed25519.Verifier.parse(space) // space field included to enable egress tracking
          },
          expiration: Infinity
        })
        .execute(claimsConnection)

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

  console.log(
    `    ✓ Successfully republished ${shards.length} location claims`
  )

  // TODO: Clean up old location claims without space information
  // This requires:
  // 1. Query content-claims service for existing location claims for each shard
  // 2. Find claims without space field (old claims)
  // 3. DELETE that record somehow (not sure how the deletion operation happens)
  // 4. Verify only new claims (with space) remain
  // NOTE: Need to confirm DELETE API exists in content-claims service
}

/**
 * Create gateway authorization for a space using agent authorization pattern
 *
 * **Idempotency Note**: Multiple calls create multiple KV entries with different CIDs.
 * Safe but not optimal. Future improvement: check existing delegations before creating new ones.
 *
 * @param {object} params
 * @param {string} params.space - Space DID
 * @returns {Promise<void>}
 */
export async function createGatewayAuth ({ space }) {
  console.log('  Creating gateway authorization...')

  // Setup signers and connections
  // Create a temporary migration signer to act as the "agent"
  const uploadServiceSigner = await getUploadServiceSigner()
  const migrationSigner = await ed25519.Signer.generate()
  const gatewaySigner = await getGatewaySigner()
  const gatewayConnection = connect({
    id: migrationSigner, // Connect as agent
    codec: CAR.outbound,
    channel: HTTP.open({ url: new URL(config.services.gatewayServiceURL) })
  })

  // Gateway DID (audience for the delegation)
  const gatewayDID = config.services.gatewayServiceDID
  const uploadServiceDID = config.services.uploadServiceDID

  // Create principal objects for ucanto
  const gatewayPrincipal = {
    did: () => gatewayDID
  }

  try {
    // Find the space -> account delegation
    console.log('    Querying space -> account delegation...')
    const spaceAccessDelegation = await findDelegationByIssuer(space)
    if (!spaceAccessDelegation) {
      console.log(
        `    ⏭  No space -> account delegation found for space: ${space}`
      )
      console.log('       Is expected for legacy spaces?')
      console.log(
        '       Gateway authorization will be skipped for this space.'
      )
      return {
        ok: { skipped: true, reason: 'no-delegation-found' }
      }
    }

    const accountDID = spaceAccessDelegation.audience.did()
    console.log(
      `    ✓ Found space -> account delegation: ${space} → ${accountDID}`
    )

    // Create Absentee delegation for access/delegate (account → migration script)
    // This is like the agent authorization flow where account delegates to agent
    // where here the agent is the migration script that is running this code
    const accessDelegation = await AccessCapabilities.access.delegate({
      issuer: Absentee.from({ id: accountDID }), // Account as Absentee issuer (no signature)
      audience: migrationSigner, // Gateway is the "agent" receiving delegation
      with: space,
      nb: {}, // No additional caveats - allows serving all content
      proofs: [spaceAccessDelegation], // Proof that space delegated to account
      expiration: Infinity
    })
    console.log(
      `    ✓ Access Delegate delegation (account → migration script): ${accessDelegation.cid}`
    )

    // Create attestation for account, from gateway, for access/delegate
    // This matches createSessionProofs pattern: attestation.audience = agent (migration script)
    const assessAttestation = await UCAN.attest.delegate({
      issuer: gatewaySigner,
      audience: migrationSigner, // Migration is the audience (like agent in email flow)
      with: gatewayDID,
      nb: { proof: accessDelegation.cid },
      expiration: Infinity
    })
    console.log(
      `    ✓ Service attestation (for gateway) - access delegate: ${assessAttestation.cid}`
    )

    // Create Absentee delegation for content/serve (account → gateway)
    // This is like the agent authorization flow where account delegates to agent
    // where here the agent is the gateway service
    const contentServeDelegation =
      await SpaceCapabilities.contentServe.delegate({
        issuer: Absentee.from({ id: accountDID }), // Account as Absentee issuer (no signature)
        audience: gatewayPrincipal, // Gateway is the "agent" receiving delegation
        with: space,
        nb: {}, // No additional caveats - allows serving all content
        proofs: [spaceAccessDelegation], // Proof that space delegated to account
        expiration: Infinity
      })
    console.log(
      `    ✓ Content Serve delegation (account → gateway): ${contentServeDelegation.cid}`
    )

    // Create attestation for account, from gateway, for content serve
    // This matches createSessionProofs pattern: attestation.audience = agent (gateway)
    const contentServeAttestation = await UCAN.attest.delegate({
      issuer: uploadServiceSigner,
      audience: gatewayPrincipal, // Gateway is the audience (like agent in email flow)
      with: uploadServiceDID,
      nb: { proof: contentServeDelegation.cid },
      expiration: Infinity
    })
    console.log(
      `    ✓ Service attestation (for gateway) - content serve: ${contentServeAttestation.cid}`
    )

    // Publish session proofs to gateway via access/delegate
    const invocation = AccessCapabilities.delegate.invoke({
      issuer: migrationSigner,
      audience: gatewayPrincipal,
      with: space,
      nb: {
        delegations: {
          [contentServeDelegation.cid.toString()]: contentServeDelegation.cid,
          [contentServeAttestation.cid.toString()]: contentServeAttestation.cid
        }
      },
      proofs: [
        accessDelegation, // account → upload-service-principal (Absentee, no signature)
        assessAttestation // upload-service attestation (for gateway)
      ],
      facts: [
        // Include all blocks for stored delegations in the facts
        // so that they show in an export
        [...contentServeAttestation.iterateIPLDBlocks()]
          .concat([...contentServeDelegation.iterateIPLDBlocks()])
          .reduce((fct, b) => {
            fct[b.cid.toString()] = b.cid
            return fct
          }, {})
      ]
    })
    // Attach all blocks for the delegations and attestations
    for (const block of contentServeAttestation.iterateIPLDBlocks()) {
      invocation.attach(block)
    }

    for (const block of contentServeDelegation.iterateIPLDBlocks()) {
      invocation.attach(block)
    }
    const result = await invocation.execute(gatewayConnection)
    if (result.out.error) {
      throw new Error(
        `Failed to publish gateway delegation: ${result.out.error.message}`
      )
    }

    console.log('    ✓ Gateway authorization published successfully')
    return { success: true }
  } catch (error) {
    console.error(error)
    // Log error but don't fail the migration - gateway auth can be retried independently
    console.error(
      `    ✗ Failed to create gateway authorization: ${error.message}`
    )
    console.error(
      '    This can be retried independently without affecting other migration steps'
    )
    // Don't throw - allow migration to continue
    return { success: false }
  }
}

/**
 * Verify that all migration steps completed successfully
 *
 * Re-queries the indexing service to confirm:
 * 1. Index claim exists for the root CID
 * 2. Location claims exist for all shards
 * 3. Location claims include space information
 *
 * Note: Gateway authorization verification is not included as it would require
 * querying the gateway's CloudFlare KV store, which is not easily accessible
 * from this script. We rely on the access/delegate invocation response to
 * confirm the delegation was stored.
 *
 * @param {object} params
 * @param {object} params.upload - Upload record from Upload Table
 * @param {string} params.upload.space - Space DID
 * @param {string} params.upload.root - Root CID
 * @param {string[]} params.upload.shards - Shard CIDs
 * @returns {Promise<{
 *   success: boolean,
 *   indexVerified: boolean,
 *   locationClaimsVerified: boolean,
 *   allShardsHaveSpace: boolean,
 *   shardsWithoutSpace: string[],
 *   details: string
 * }>}
 */
export async function verifyMigration ({ upload }) {
  console.log('  Verifying migration...')
  console.log(`    Root: ${upload.root}`)
  console.log(`    Space: ${upload.space}`)
  console.log(`    Shards: ${upload.shards.length}`)

  try {
    // Query indexing service to check current state
    const indexingData = await queryIndexingService(upload.root)

    // Verify index claim exists
    const indexVerified = indexingData.hasIndexClaim
    console.log(`    Index claim: ${indexVerified ? '✓ EXISTS' : '✗ MISSING'}`)

    // Extract location claims
    const locationClaims = indexingData.claims.filter(
      (c) => c.type === 'assert/location'
    )
    console.log(`    Location claims found: ${locationClaims.length}`)

    // Build map of shard multihash -> array of location claims
    // There may be multiple claims per shard (old without space, new with space)
    const shardLocationMap = new Map()
    for (const claim of locationClaims) {
      const contentMultihash =
        claim.content.multihash ?? Digest.decode(claim.content.digest)
      const multihashStr = base58btc.encode(contentMultihash.bytes)

      if (!shardLocationMap.has(multihashStr)) {
        shardLocationMap.set(multihashStr, [])
      }
      shardLocationMap.get(multihashStr).push(claim)
    }

    // Check each shard
    const shardsWithoutSpace = []
    let allShardsHaveLocationClaims = true

    for (const shardCID of upload.shards) {
      const cid = CID.parse(shardCID)
      const multihashStr = base58btc.encode(cid.multihash.bytes)
      const claims = shardLocationMap.get(multihashStr) || []

      if (claims.length === 0) {
        allShardsHaveLocationClaims = false
        shardsWithoutSpace.push(shardCID)
        console.log(`    ✗ ${shardCID}: no location claim`)
      } else {
        // Check if ANY claim has space information matching this upload's space
        const hasClaimWithSpace = claims.some(
          (claim) => claim.space != null && claim.space === upload.space
        )

        if (hasClaimWithSpace) {
          console.log(`    ✓ ${shardCID}: location claim with space`)
        } else {
          shardsWithoutSpace.push(shardCID)
          console.log(`    ✗ ${shardCID}: location claim missing space field`)
        }
      }
    }

    const locationClaimsVerified = allShardsHaveLocationClaims
    const allShardsHaveSpace = shardsWithoutSpace.length === 0
    const success =
      indexVerified && locationClaimsVerified && allShardsHaveSpace

    // Summary
    console.log('\n  Verification Summary:')
    console.log(`    Index: ${indexVerified ? '✓ VERIFIED' : '✗ FAILED'}`)
    console.log(
      `    Location claims: ${
        locationClaimsVerified ? '✓ VERIFIED' : '✗ FAILED'
      }`
    )
    console.log(
      `    Space information: ${allShardsHaveSpace ? '✓ VERIFIED' : '✗ FAILED'}`
    )
    console.log(
      `    Shards without space: ${shardsWithoutSpace.length}/${upload.shards.length}`
    )
    console.log(`    Overall: ${success ? '✓ PASSED' : '✗ FAILED'}`)

    let details = ''
    if (!success) {
      const issues = []
      if (!indexVerified) issues.push('index claim missing')
      if (!locationClaimsVerified) issues.push('location claims missing')
      if (!allShardsHaveSpace) { issues.push(`${shardsWithoutSpace.length} shards missing space info`) }
      details = issues.join(', ')
    }

    return {
      success,
      indexVerified,
      locationClaimsVerified,
      allShardsHaveSpace,
      shardsWithoutSpace,
      details: details || 'All verification checks passed'
    }
  } catch (error) {
    console.error(`    ✗ Verification failed: ${error.message}`)
    return {
      success: false,
      indexVerified: false,
      locationClaimsVerified: false,
      allShardsHaveSpace: false,
      shardsWithoutSpace: upload.shards,
      details: `Verification error: ${error.message}`
    }
  }
}
