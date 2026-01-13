/**
 * Query the new indexing service
 */
import { Client } from '@storacha/indexing-service-client'
import * as Digest from 'multiformats/hashes/digest'
import { CID } from 'multiformats/cid'
import { config } from '../config.js'

/**
 * Create indexing service client
 */
export function createIndexingServiceClient() {
  return new Client({
    serviceURL: new URL(config.services.indexingServiceURL),
  })
}

/**
 * Query indexing service for claims about a content CID
 *
 * @param {string} contentCID - Content CID to query
 * @returns {Promise<{
 *   hasIndexClaim: boolean,
 *   hasLocationClaim: boolean,
 *   locationHasSpace: boolean,
 *   spaces: string[],
 *   indexCID: string | null,
 *   claims: any[]
 *   indexes: import('@storacha/indexing-service-client/api').QueryOk['indexes']
 * }>}
 */
export async function queryIndexingService(contentCID) {
  const client = createIndexingServiceClient()

  try {
    const cid = CID.parse(contentCID)
    const digest = Digest.decode(cid.multihash.bytes)

    const result = await client.queryClaims({
      hashes: [digest],
      kind: 'standard',
    })

    if (result.error) {
      const errorString = String(result.error)
      const errorMessage = result.error instanceof Error ? result.error.message : errorString
      
      // Check if this is a 500 error - these uploads cannot be processed by the indexing service
      if (errorString.includes('status: 500') || errorMessage.includes('status: 500')) {
        // Throw a specific error that can be caught and handled
        const indexingError = /** @type {Error & { code: string, originalError: unknown }} */ (
          new Error(`INDEXING_SERVICE_500: ${errorMessage}`)
        )
        indexingError.code = 'INDEXING_SERVICE_500'
        indexingError.originalError = result.error
        throw indexingError
      }
      
      // For other errors, log and return defaults (migration will proceed)
      console.warn(
        `Error querying indexing service for ${contentCID}:`,
        result.error
      )
      return {
        hasIndexClaim: false,
        hasLocationClaim: false,
        locationHasSpace: false,
        spaces: [],
        indexCID: null,
        claims: [],
        indexes: new Map(),
      }
    }

    const claims = Array.from(result.ok.claims.values())
    const indexes = result.ok.indexes // ShardedDAGIndex structures

    const hasIndexClaim = claims.some((c) => c.type === 'assert/index')
    const locationClaims = claims.filter((c) => c.type === 'assert/location')
    const hasLocationClaim = locationClaims.length > 0
    const locationHasSpace = locationClaims.some((c) => c.space != null)

    const spaces = /** @type {string[]} */ ([
      ...new Set(
        locationClaims.filter((c) => c.space).map((c) => c.space?.did())
      ),
    ])

    const indexClaim = claims.find((c) => c.type === 'assert/index')
    const indexCID = indexClaim ? indexClaim.index.toString() : null

    return {
      hasIndexClaim,
      hasLocationClaim,
      locationHasSpace,
      spaces,
      indexCID,
      claims,
      indexes, // Include the parsed ShardedDAGIndex structures
    }
  } catch (error) {
    // Check if this is ANY 500 error from the indexing service
    // This includes both provider address issues and index fetching failures
    const errorString = String(error)
    const errorMessage = error instanceof Error ? error.message : errorString
    
    // Check for 500 errors - these indicate the indexing service cannot process this upload
    if (errorString.includes('status: 500') || errorMessage.includes('status: 500')) {
      console.log(`\n  ⚠️  Indexing service returned 500 error`)
      console.log(`  ⏭  Skipping this upload - will be tracked in error counter`)
      // Throw a specific error that can be caught and handled
      const indexingError = /** @type {Error & { code: string, originalError: unknown }} */ (
        new Error(`INDEXING_SERVICE_500: ${errorMessage}`)
      )
      indexingError.code = 'INDEXING_SERVICE_500'
      indexingError.originalError = error
      throw indexingError
    }
    
    // For other errors, log and return defaults (migration will proceed)
    console.error(`Error querying indexing service for ${contentCID}:`, error)
    console.warn(`  ⚠️  Migration will proceed - Step 2 will republish location claims with correct provider metadata`)
    return {
      hasIndexClaim: false,
      hasLocationClaim: false,
      locationHasSpace: false,
      spaces: [],
      indexCID: null,
      claims: [],
      indexes: new Map(),
    }
  }
}
