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
      console.warn(`Error querying indexing service for ${contentCID}:`, result.error)
      return {
        hasIndexClaim: false,
        hasLocationClaim: false,
        locationHasSpace: false,
        spaces: [],
        indexCID: null,
        claims: [],
      }
    }
    
    const claims = Array.from(result.ok.claims.values())
    const hasIndexClaim = claims.some(c => c.type === 'assert/index')
    const locationClaims = claims.filter(c => c.type === 'assert/location')
    const hasLocationClaim = locationClaims.length > 0
    const locationHasSpace = locationClaims.some(c => c.space != null)
    
    const spaces = [
      ...new Set(
        locationClaims
          .filter(c => c.space)
          .map(c => c.space.did())
      )
    ]
    
    const indexClaim = claims.find(c => c.type === 'assert/index')
    const indexCID = indexClaim ? indexClaim.index.toString() : null
    
    return {
      hasIndexClaim,
      hasLocationClaim,
      locationHasSpace,
      spaces,
      indexCID,
      claims,
    }
  } catch (error) {
    console.error(`Error querying indexing service for ${contentCID}:`, error)
    return {
      hasIndexClaim: false,
      hasLocationClaim: false,
      locationHasSpace: false,
      spaces: [],
      indexCID: null,
      claims: [],
    }
  }
}
