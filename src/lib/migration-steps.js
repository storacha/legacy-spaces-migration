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
import { DID } from '@ucanto/core'
import { connect } from '@ucanto/client'
import * as CAR from '@ucanto/transport/car'
import * as HTTP from '@ucanto/transport/http'
import { Absentee } from '@ucanto/principal'
import { Assert } from '@web3-storage/content-claims/capability'
import {
  Access as AccessCapabilities,
  Space as SpaceCapabilities,
  Claim as ClaimCapabilities,
  UCAN,
} from '@storacha/capabilities'
import * as IndexCapabilities from '@storacha/capabilities/space/index'
import * as ContentCapabilities from '@storacha/capabilities/space/content'
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
  generateDAGIndex,
} from './migration-utils.js'
import {
  config,
  getGatewaySigner,
  getMigrationSigner,
  getUploadServiceSigner,
  getIndexingServiceProof,
} from '../config.js'
import { LocationCommitmentMetadata } from './ipni/location.js'
import { getIPNIPublishingQueue } from './queues/ipni-publishing-queue.js'
import { encodeContextID } from './ipni/advertisement.js'
import { getErrorMessage } from './error-utils.js'
import { URI } from '@ucanto/core/schema'
import { claimHasSpace, findClaimsForShard } from './claim-utils.js'
import { storeClaim } from './stores/claim-store.js'

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
 *   indexCID: string | null
 * }>}
 */
export async function checkMigrationNeeded(upload) {
  // First, check if index claim exists by querying root CID
  const indexingData = await queryIndexingService(upload.root)

  console.log(
    `  Checking location claims for ${upload.shards.length} shards...`
  )

  // Get all location claims from the root query
  const allLocationClaims = indexingData.claims.filter(
    (c) => c.type === 'assert/location'
  )

  console.log(
    `    Found ${allLocationClaims.length} location claims from indexing service`
  )

  // Check each shard to see if it has a location claim with the correct space
  const shardsNeedingLocationClaims = []

  for (const shardCID of upload.shards) {
    // Find location claims for this shard using shared utility
    const shardLocationClaims = findClaimsForShard(allLocationClaims, shardCID)

    if (shardLocationClaims.length === 0) {
      // No location claim exists for this shard
      shardsNeedingLocationClaims.push(shardCID)
      console.log(`    ✗ ${shardCID}: no location claim found`)
    } else {
      // Check if at least one claim has the correct space using shared utility
      const hasClaimWithCorrectSpace = shardLocationClaims.some(
        (claim) => claimHasSpace(claim, upload.space)
      )

      if (!hasClaimWithCorrectSpace) {
        // Location claims exist but none have the correct space field
        shardsNeedingLocationClaims.push(shardCID)
        console.log(
          `    ✗ ${shardCID}: location claim missing correct space field`
        )
      }
    }
  }

  return {
    needsIndexGeneration: !indexingData.hasIndexClaim,
    needsIndexRegistration: !indexingData.hasIndexClaim,
    needsLocationClaims: shardsNeedingLocationClaims.length > 0,
    needsGatewayAuth: true, // Always create gateway auth for now
    hasIndexClaim: indexingData.hasIndexClaim,
    hasLocationClaim: allLocationClaims.length > 0,
    locationHasSpace: shardsNeedingLocationClaims.length === 0,
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
 * @returns {Promise<{migrationSpace: string, indexCID: import('@storacha/access').CARLink, shards: Array<{cid: string, size: number}>}>}
 *   - migrationSpace: DID of the migration space where index was uploaded
 *   - indexCID: CID of the generated index
 *   - shards: Shards with sizes
 */
export async function buildAndMigrateIndex({ upload }) {
  console.log('  Building and migrating index...')
  console.log(`    Original space: ${upload.space}`)
  console.log(`    Content root: ${upload.root}`)

  // Generate sharded DAG index for the content
  const { indexBytes, indexCID, indexDigest, shards } = await generateDAGIndex(upload)

  const uploadServiceSigner = await getUploadServiceSigner()
  const uploadServiceURL = new URL(config.services.uploadServiceURL)
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID)

  const connection = connect({
    id: uploadServiceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: uploadServiceURL }),
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
  } = await getOrCreateMigrationSpaceForCustomer(customer)

  // Verify we have the migration space signer
  if (!migrationSpace) {
    throw new Error('Migration space not available - this should never happen!')
  }
  console.log(
    `    Migration space: ${migrationSpace.did()} ${
      isNew ? '(newly created)' : '(existing)'
    }`
  )

  // Upload index blob to migration space via space/blob/add
  console.log(`    Uploading index blob (${indexBytes.length} bytes)...`)

  // Get the signer from the migration space
  const spaceSigner = migrationSpace.signer || migrationSpace

  try {
    // Upload the index blob to the migration space using SpaceBlob.add
    const blobAddResult = await SpaceBlob.add(
      {
        issuer: spaceSigner, // Sign with the space's signer (owner can self-authorize)
        with: migrationSpace.did(), // Upload to migration space
        audience: uploadServiceDID, // Audience is the upload service DID
        proofs: [], // Empty proofs - space owner self-authorizes
      },
      indexDigest,
      indexBytes,
       // new Blob([indexBytes], { type: 'application/vnd.ipld.car' }),
      { connection }
    )
    if (!blobAddResult || !blobAddResult.site) {
      throw new Error('Failed to upload index blob')
    }

    console.log('    ✓ Index blob uploaded to migration space')
  } catch (error) {
    console.error('    ✗ Failed to upload index blob')
    throw new Error(`Failed to upload index blob: ${getErrorMessage(error)}`, {cause: error})
  }

  // CRITICAL: Publish location claim for the index CAR itself BEFORE registering
  // The indexing service needs to fetch the index CAR, so it needs a location claim.
  // The index belongs to the migration space.
  console.log('    Publishing location claim for index CAR...')
  try {
    await republishLocationClaims({
      space:  migrationSpace.did(),
      migrationSpace,
      root: indexCID.toString(),
      shards: [indexCID.toString()],
      shardsWithSizes: [{ cid: indexCID.toString(), size: indexBytes.length }],
    })
    console.log('    ✓ Index CAR location claim published')
  } catch (error) {
    console.error('    ✗ Failed to publish index CAR location claim')
    throw new Error(
      `Failed to publish index CAR location claim: ${getErrorMessage(error)}`,
      { cause: error }
    )
  }

  // Register index via space/index/add (publishes assert/index claim)
  // We invoke the capability directly (not using Index.add client) so we can
  // include the content CID, which allows the upload-api to skip fetching the
  // index and delegate to the indexer, avoiding queries to broken legacy data.
  console.log('    Registering index with indexing service...')

  /** @type {any} */
  let indexInvocation
  try {
    // Parse the content root CID
    const contentCID = CID.parse(upload.root)
    
    // Invoke assert/index capability directly on the indexing service
    // We use 'with: migrationSpace.did()' to self-issue the capability as the space owner.
    // This bypasses upload-api checks and avoids service-level authorization issues.
    const indexingServiceURL = new URL(config.services.indexingServiceURL)
    const indexingServiceDID = config.services.indexingServiceDID
    const indexingServicePrincipal = DID.parse(indexingServiceDID)
    // We don't need indexingServiceProof if we are self-issuing as the space

    const indexingConnection = connect({
      id: indexingServicePrincipal,
      codec: CAR.outbound,
      channel: HTTP.open({ url: indexingServiceURL }),
    })

    indexInvocation = await Assert.index
      .invoke({
        issuer: spaceSigner,
        audience: indexingServicePrincipal,
        with: migrationSpace.did(),
        nb: {
          index: indexCID,
          content: contentCID,
        },
        proofs: [],
      })
      .execute(indexingConnection)
    
    // Check if the invocation succeeded
    if (indexInvocation.out.error) {
      console.error('    ✗ Index registration failed:', indexInvocation.out.error)
      throw new Error(`Index registration rejected: ${JSON.stringify(indexInvocation.out.error)}`)
    }
    
    console.log('    ✓ Index registered (assert/index claim published)')
  } catch (error) {
    console.error('    ✗ Index.add error:', error)
    throw new Error(`Failed to register index: ${getErrorMessage(error)}`)
  }

  // Increment index count in tracking table
  await incrementIndexCount(customer)

  // Delegate full access to customer (only if newly created space)
  if (isNew) {
    console.log('    Delegating migration space access to customer...')
    await delegateMigrationSpaceToCustomer({
      migrationSpace,
      customer,
      cause: indexInvocation.ran, // Link delegation to index registration
    })
  }

  return { migrationSpace: migrationSpace.did(), indexCID, shards }
}

/**
 * Register an existing index with the indexing service
 * This notifies the indexing service to pull in any new location claims
 *
 * @param {object} params
 * @param {object} params.upload - Upload record from Upload Table
 * @param {string} params.upload.space - Original space DID (where content was uploaded)
 * @param {string} params.upload.root - Root CID (content to index)
 * @param {import('multiformats').CID} params.indexCID - CID of the index to register
 */
export async function registerIndex({ upload, indexCID }) {
  const serviceSigner = await getUploadServiceSigner()
  const serviceURL = new URL(config.services.uploadServiceURL)
  const connection = connect({
    id: serviceSigner,
    codec: CAR.outbound,
    channel: HTTP.open({ url: serviceURL }),
  })

  // Get migration space for this upload
  const customer = await getCustomerForSpace(upload.space)
  if (!customer) {
    throw new Error(`No customer found for space ${upload.space}`)
  }

  const { space: migrationSpace } =
    await getOrCreateMigrationSpaceForCustomer(customer)

  // Register index via space/index/add (publishes assert/index claim)
  // Get the signer from the migration space
  const spaceSigner = migrationSpace.signer || migrationSpace
  if (!config.services.uploadServiceDID) {
    throw new Error('uploadServiceDID not configured in config.services')
  }

  // Parse the upload service DID for the audience
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID)
  /** @type {any} */
  let result
  try {
    result = await Index.add(
      {
        issuer: spaceSigner, // Sign with the space's signer (owner can self-authorize)
        with: migrationSpace.did(), // Register index for migration space
        audience: uploadServiceDID, // Audience is the upload service DID
        proofs: [], // Empty proofs - space owner self-authorizes
      },
      /** @type {any} */ (indexCID),
      { connection }
    )
  } catch (error) {
    console.error('Index.add threw error:', getErrorMessage(error), {cause: error})
    throw error
  }

  console.log(
    '    Index registration result:',
    result.out.ok ? '✓ Success' : `✗ Error: ${JSON.stringify(result.out.error)}`
  )

  if (result.out.error) {
    throw new Error(
      `Failed to register index: ${
        getErrorMessage(result.out.error) || JSON.stringify(result.out.error)
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
 * @param {import('@storacha/access').SpaceDID} params.space - Space DID
 * @param {string[]} params.shards - Shard CIDs (CAR files) to republish
 * @param {string} params.root - Root CID (index CAR)
 * @param {import('@storacha/access').OwnedSpace} [params.migrationSpace] - Migration space DID
 * @param {Array<{cid: string, size: number}> | null} [params.shardsWithSizes] - Optional shard info from previous step (avoids re-querying DynamoDB)
 * @returns {Promise<void>}
 */
export async function republishLocationClaims({
  space,
  migrationSpace: _migrationSpace,
  root: _root,
  shards,
  shardsWithSizes,
}) {
  console.log(`  Republishing ${shards.length} location claims with space ${space}...`)

    // Get indexing service proof (authorizes Piri to invoke claim/cache)
  /** @type {import('@ucanto/interface').Delegation} */
  let indexingServiceProof
  try {
    indexingServiceProof = await getIndexingServiceProof()
  } catch (err) {
    console.error('Failed to load INDEXING_SERVICE_PROOF. Please check your .env file.')
    throw err
  }

  const indexingServiceURL = new URL(config.services.indexingServiceURL)
  const indexingServiceDID = config.services.indexingServiceDID
  const indexingServicePrincipal = DID.parse(indexingServiceDID)
  const uploadServiceSigner = await getUploadServiceSigner()

  const indexingConnection = connect({
    id:indexingServicePrincipal,
    codec: CAR.outbound,
    channel: HTTP.open({ url: indexingServiceURL }),
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

      // Parse CID to get digest and codec
      const cid = CID.parse(shardCID)
      const digest = cid.multihash
      const isCAR = cid.code === 0x0202 // car codec

      // Determine protocol-specific values
      let location
      let providerAddrs

      if (isCAR) {
        // Store Protocol (CAR files)
        // URL format: {carpark}/{cid}/{cid}.car
        const locResult = URI.read(
          `${config.storage.carparkPublicUrl}/${shardCID}/${shardCID}.car`
        )
        if (locResult.error) {
          throw new Error(`Invalid location URI for CAR: ${locResult.error.message}`)
        }
        location = locResult.ok
        providerAddrs = [
          config.addresses.claimAddr.bytes,
          // Store Protocol Address
          config.addresses.storeProtocolBlobAddr.bytes,
        ]
      } else {
        // Blob Protocol (Raw blobs)
        // URL format: {carpark}/{digest}/{digest}.blob
        const locResult = URI.read(
          `${config.storage.carparkPublicUrl}/${base58btc.encode(
            digest.bytes
          )}/${base58btc.encode(digest.bytes)}.blob`
        )
        if (locResult.error) {
          throw new Error(`Invalid location URI for Blob: ${locResult.error.message}`)
        }
        location = locResult.ok
        providerAddrs = [
          config.addresses.claimAddr.bytes,
          // Blob Protocol Address
          config.addresses.blobProtocolBlobAddr.bytes,
        ]
      }

      console.log(`    Publishing location claim for ${shardCID}...`)

      // Create the invocation (matching upload-api pattern)
      const claim = await Assert.location.delegate({
        issuer: uploadServiceSigner,
        audience: DID.parse(space),
        with: uploadServiceSigner.did(),
        nb: {
          content: { digest: digest.bytes },
          location: [location],
          range: { offset: 0, length: size },
          space: DID.parse(space), // space field included to enable egress tracking - parse as Principal
        },
        expiration: Infinity,
        proofs: [], // Empty proofs since we're using the service private key to sign
      })

      // Store claim for audit/backup (non-blocking)
      try {
        await storeClaim(claim)
      } catch (storeError) {
        console.warn(
          `    ⚠️  Warning: Failed to store claim in S3 bucket (skipping): ${getErrorMessage(
            storeError
          )}`
        )
        throw storeError
      }

      const migrationSigner = await getMigrationSigner()
      const invocation = ClaimCapabilities.cache.invoke({
        issuer: migrationSigner,
        audience: indexingServicePrincipal,
        with: indexingServicePrincipal.did(),
        nb: {
          claim: claim.link(),
          provider: {
            addresses: providerAddrs,
          },
        },
        proofs: [
          indexingServiceProof,
          claim, // Include the claim itself so its blocks are sent
        ],
      })
      
      const result = await invocation.execute(indexingConnection)
      if (result.out.error) {
        console.error('    ✗ Failed to cache location claim:', result.out.error)
        throw new Error(
          `Failed to republish location claim: ${getErrorMessage(result.out.error)}`,
          { cause: result.out }
        )
      }

      const claimCID = CID.parse(claim.link().toString())
      const meta = new LocationCommitmentMetadata({
        shard: cid,
        claim: claimCID,
        expiration:
          claim.expiration === Infinity ? 0n : BigInt(claim.expiration),
      })

      // Encode context ID (Space DID + Content Multihash) and send job to queue
      const contextID = await encodeContextID(space, digest.bytes)
      // Provider info for IPNI advertisement
      const providerInfo = {
        id: config.addresses.peerID,
        addrs: [
          config.addresses.claimAddr, 
          config.addresses.blobProtocolBlobAddr,
          config.addresses.storeProtocolBlobAddr,
        ],
      }
      // Get singleton IPNI publishing queue instance
      const queue = getIPNIPublishingQueue()
      await queue.sendJob({
        providerInfo,
        contextID,
        digests: [digest],
        metadata: await meta.marshalBinary(),
      })

      console.log(`    ✓ ${shardCID}`)
    } catch (error) {
      console.error(`    ✗ ${shardCID}: ${getErrorMessage(error)}`)
      throw error
    }
  }

  console.log(`    ✓ Successfully republished ${shards.length} location claims`)
}

/**
 * Create gateway authorization for a space using agent authorization pattern
 *
 * **Idempotency Note**: Multiple calls create multiple KV entries with different CIDs.
 * Safe but not optimal. Future improvement: check existing delegations before creating new ones.
 *
 * @param {object} params
 * @param {import('@storacha/access').SpaceDID} params.space - Space DID
 * @returns {Promise<{success: boolean, skipped: boolean, reason: string}>}
 */
export async function createGatewayAuth({ space }) {
  console.log('  Creating gateway authorization...')

  // Setup signers and connections
  // Create a temporary migration signer to act as the "agent"
  const uploadServiceSigner = await getUploadServiceSigner()
  const migrationSigner = await getMigrationSigner()
  const gatewaySigner = await getGatewaySigner()
  const gatewayConnection = connect({
    id: migrationSigner, // Connect as agent
    codec: CAR.outbound,
    channel: HTTP.open({ url: new URL(config.services.gatewayServiceURL) }),
  })

  // Gateway DID (audience for the delegation)
  const gatewayDID = DID.parse(config.services.gatewayServiceDID).did()
  const uploadServiceDID = DID.parse(config.services.uploadServiceDID).did()

  // Create principal objects for ucanto
  const gatewayPrincipal = {
    did: () => gatewayDID,
  }

  try {
    // Find the space -> account delegation
    console.log(`    Querying space ${space} -> account delegation...`)
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
        success: false, skipped: true, reason: 'no-delegation-found',
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
      proofs: [spaceAccessDelegation], // Proof that space delegated to account
      expiration: Infinity,
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
      expiration: Infinity,
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
        expiration: Infinity,
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
      expiration: Infinity,
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
          [contentServeAttestation.cid.toString()]: contentServeAttestation.cid,
        },
      },
      proofs: [
        accessDelegation, // account → upload-service-principal (Absentee, no signature)
        assessAttestation, // upload-service attestation (for gateway)
      ],
      facts: [
        // Include all blocks for stored delegations in the facts
        // so that they show in an export
        [...contentServeAttestation.iterateIPLDBlocks()]
          .concat([...contentServeDelegation.iterateIPLDBlocks()])
          .reduce((fct, b) => {
            fct[b.cid.toString()] = b.cid
            return fct
          }, /** @type {Record<string, unknown>} */ ({})),
      ],
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
        `Failed to publish gateway delegation: ${getErrorMessage(result.out.error)}`
      )
    }

    console.log('    ✓ Gateway authorization published successfully')
    return { success: true, skipped: false, reason: '' }
  } catch (error) {
    console.error(error)
    // Log error but don't fail the migration - gateway auth can be retried independently
    console.error(
      `    ✗ Failed to create gateway authorization: ${getErrorMessage(error)}`
    )
    console.error(
      '    This can be retried independently without affecting other migration steps'
    )
    // Don't throw - allow migration to continue
    return { success: false, skipped: true, reason: 'Failed to publish gateway delegation' }
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
export async function verifyMigration({ upload }) {
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
          (/** @type {any} */ claim) =>
            claim.space != null && claim.space === upload.space
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
      if (!allShardsHaveSpace) {
        issues.push(`${shardsWithoutSpace.length} shards missing space info`)
      }
      details = issues.join(', ')
    }

    return {
      success,
      indexVerified,
      locationClaimsVerified,
      allShardsHaveSpace,
      shardsWithoutSpace,
      details: details || 'All verification checks passed',
    }
  } catch (error) {
    console.error(`    ✗ Verification failed: ${getErrorMessage(error)}`, { cause: error })
    return {
      success: false,
      indexVerified: false,
      locationClaimsVerified: false,
      allShardsHaveSpace: false,
      shardsWithoutSpace: upload.shards,
      details: `Verification error: ${getErrorMessage(error)}`,
    }
  }
}
