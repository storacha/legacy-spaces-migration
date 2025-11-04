/**
 * Delegation table operations for storing space delegations
 * 
 * Stores delegations in DynamoDB for indexing and S3/R2 for content.
 * Simplified version for migration - only supports putMany for now.
 */
import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { base32 } from 'multiformats/bases/base32'
import { delegationsToBytes } from '@storacha/access/encoding'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

/**
 * Cached S3 client
 */
let cachedS3Client = null

/**
 * Get or create S3 client for delegation bucket
 * Caches the client to avoid creating multiple connections
 */
function getS3Client() {
  if (!cachedS3Client) {
    cachedS3Client = new S3Client({
      region: config.aws.region,
      credentials: config.aws.accessKeyId ? {
        accessKeyId: config.aws.accessKeyId,
        secretAccessKey: config.aws.secretAccessKey,
      } : undefined,
    })
  }
  return cachedS3Client
}

/**
 * Create delegation S3 key in w3infra format
 * 
 * @param {import('multiformats/cid').CID} cid - Delegation CID
 * @returns {string} - S3 key: /delegations/{cid-base32}.car
 */
function createDelegationsBucketKey(cid) {
  return `/delegations/${cid.toString(base32)}.car`
}

/**
 * Store delegations in DynamoDB + S3
 * 
 * Follows w3infra's exact pattern:
 * 1. Encode delegation as CAR bytes using delegationsToBytes([delegation])
 * 2. Store in S3/R2 with key: /delegations/{cid-base32}.car
 * 3. Index in DynamoDB with link (CID), audience, issuer, expiration, cause
 * 
 * @param {Array<import('@ucanto/interface').Delegation>} delegations
 * @param {object} [options]
 * @param {import('@ucanto/interface').Link} [options.cause] - CID of invocation that caused this delegation
 * @returns {Promise<void>}
 */
export async function storeDelegations(delegations, options = {}) {
  if (delegations.length === 0) {
    return
  }

  const dynamoClient = getDynamoClient()
  const s3Client = getS3Client()

  // Store delegation CAR bytes in S3/R2 (matching w3infra format)
  // Each delegation is encoded as a CAR file containing just that delegation
  await Promise.all(delegations.map(async (delegation) => {
    // Encode single delegation as CAR bytes
    const carBytes = delegationsToBytes([delegation])
    
    const command = new PutObjectCommand({
      Bucket: config.storage.delegationBucket,
      Key: createDelegationsBucketKey(delegation.cid),  // /delegations/{cid-base32}.car
      Body: carBytes,
      ContentType: 'application/car',
    })
    
    await s3Client.send(command)
    console.log(`      ✓ Stored delegation CAR: ${createDelegationsBucketKey(delegation.cid)}`)
  }))

  // Index delegations in DynamoDB
  const batchWrite = new BatchWriteCommand({
    RequestItems: {
      [config.tables.delegation]: delegations.map(d => ({
        PutRequest: {
          Item: {
            link: d.cid.toString(),
            audience: d.audience.did(),
            issuer: d.issuer.did(),
            expiration: d.expiration === Infinity ? null : d.expiration,
            cause: options.cause?.toString(), // CID of space/index/add invocation
          }
        }
      }))
    }
  })

  await dynamoClient.send(batchWrite)
  console.log(`      ✓ Indexed ${delegations.length} delegation(s) in DynamoDB`)
}
