/**
 * Query the content claims DynamoDB table
 */
import { QueryCommand } from '@aws-sdk/lib-dynamodb'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { CarReader } from '@ipld/car'
import { base58btc } from 'multiformats/bases/base58'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'
import { CID } from 'multiformats/cid'
import { decode } from '@ipld/dag-cbor'
import { getErrorMessage } from '../error-utils.js'

const s3Client = new S3Client({ 
  region: config.aws.region,
})

/**
 * Query location claims for a shard CID and check if a claim with the given space exists
 * 
 * @param {string} shardCID - Shard CID (e.g., bagbaiera...)
 * @param {string} spaceDID - Space DID to look for in claims
 * @returns {Promise<{
 *   hasLocationClaim: boolean,
 *   hasClaimWithSpace: boolean,
 *   totalClaims: number,
 *   claimCIDs: string[]
 * }>}
 */
export async function verifyLocationClaimWithSpace(shardCID, spaceDID) {
  const tableName = `${config.environment}-content-claims-claims-v1`
  // Use the actual deployed bucket name from config
  // SST generates bucket names, they're not predictable from environment alone
  const bucketName = config.storage.claimsBucket || `${config.environment}-content-claims-claims-v1`
  
  // Parse shard CID to get multihash
  
  const cid = CID.parse(shardCID)
  const contentKey = base58btc.encode(cid.multihash.bytes)
  
  console.log(`    [DEBUG] Querying DynamoDB table: ${tableName}`)
  console.log(`    [DEBUG] Content key (base58btc): ${contentKey}`)
  
  // Query DynamoDB for all claims for this content
  const docClient = getDynamoClient()
  const queryResult = await docClient.send(
    new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'content = :content',
      ExpressionAttributeValues: {
        ':content': contentKey,
      },
    })
  )
  
  const totalClaims = queryResult.Items?.length || 0
  console.log(`    [DEBUG] Found ${totalClaims} claim(s) in DynamoDB`)
  
  if (!queryResult.Items || queryResult.Items.length === 0) {
    return {
      hasLocationClaim: false,
      hasClaimWithSpace: false,
      totalClaims: 0,
      claimCIDs: []
    }
  }
  
  // Fetch and parse each claim from S3 to check for space field
  let hasClaimWithSpace = false
  let hasLocationClaim = false
  const claimCIDs = []
  
  for (const item of queryResult.Items) {
    const claimCID = item.claim
    claimCIDs.push(claimCID)
    
    try {
      // Fetch claim bytes from S3
      const key = `${claimCID}/${claimCID}.car`
      console.log(`    [DEBUG] Fetching claim from S3: ${key}`)
      
      const s3Response = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
        })
      )
      if (!s3Response.Body) {
        throw new Error(`Failed to fetch claim ${claimCID} from S3`)
      }
      
      const claimBytes = await s3Response.Body.transformToByteArray()
      
      // Parse CAR to get claim blocks
      const reader = await CarReader.fromBytes(claimBytes)
      
      for await (const block of reader.blocks()) {
        try {
          // Decode CBOR to check for assert/location capability with space field
          
          const data = decode(block.bytes)
          
          // Check if this is an invocation with assert/location capability
          if (data.att && Array.isArray(data.att)) {
            for (const capability of data.att) {
              if (capability.can === 'assert/location') {
                hasLocationClaim = true
                
                // Check if nb.space matches our space
                if (capability.nb && capability.nb.space) {
                  let claimSpace = null
                  
                  if (typeof capability.nb.space === 'string') {
                    claimSpace = capability.nb.space
                  } else if (typeof capability.nb.space?.did === 'function') {
                    claimSpace = capability.nb.space.did()
                  } else if (capability.nb.space instanceof Uint8Array) {
                    // Space is stored as raw bytes (multicodec-encoded public key)
                    // Convert to did:key string using base58btc encoding
                    claimSpace = `did:key:${base58btc.encode(capability.nb.space)}`
                  }
                  
                  console.log(`    [DEBUG] Found claim ${claimCID} with space: ${claimSpace}`)
                  
                  if (claimSpace === spaceDID) {
                    hasClaimWithSpace = true
                    console.log(`    [DEBUG] ✓ Claim matches target space!`)
                  }
                }
              }
            }
          }
        } catch (decodeErr) {
          // Skip blocks that can't be decoded
          continue
        }
      }
    } catch (err) {
      console.log(`    [DEBUG] Error fetching/parsing claim ${claimCID}:`, getErrorMessage(err))
    }
  }
  
  return {
    hasLocationClaim,
    hasClaimWithSpace,
    totalClaims,
    claimCIDs
  }
}
