/**
 * Delegation table operations for storing space delegations
 * 
 * Stores delegations in DynamoDB for indexing and S3/R2 for content.
 * Simplified version for migration - only supports putMany for now.
 */
import { BatchWriteCommand, QueryCommand } from '@aws-sdk/lib-dynamodb'
import { PutObjectCommand, GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { base32 } from 'multiformats/bases/base32'
import { delegationsToBytes, bytesToDelegations } from '@storacha/access/encoding'
import { parseLink } from '@ucanto/core'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'
import { getErrorMessage } from '../error-utils.js'

/**
 * Cached S3 clients
 */
/**
 * @type {import('@aws-sdk/client-s3').S3Client | null}
 */
let cachedS3Client = null
/**
 * @type {import('@aws-sdk/client-s3').S3Client | null}
 */
let cachedR2Client = null

/**
 * Get or create S3 client for AWS S3
 * Caches the client to avoid creating multiple connections
 */
function getS3Client() {
  if (!cachedS3Client) {
    if (!config.aws.region || !config.aws.accessKeyId || !config.aws.secretAccessKey) {
      throw new Error('AWS configuration is missing')
    }
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
 * Get or create R2 client for Cloudflare R2
 * Caches the client to avoid creating multiple connections
 */
function getR2Client() {
  if (!cachedR2Client) {
    if (!config.storage.r2Endpoint || !config.storage.r2AccessKeyId || !config.storage.r2SecretAccessKey) {
      throw new Error('R2 configuration is missing') 
    }
    cachedR2Client = new S3Client({
      region: config.storage.r2Region,
      endpoint: config.storage.r2Endpoint,
      credentials: {
        accessKeyId: config.storage.r2AccessKeyId,
        secretAccessKey: config.storage.r2SecretAccessKey,
      },
    })
  }
  return cachedR2Client
}

/**
 * Create delegation S3 key in w3infra format
 * 
 * @param {import('multiformats').UnknownLink} cid - Delegation CID
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
  
  // Always use R2 for delegation storage
  const client = getR2Client()
  const bucketName = config.storage.r2DelegationBucket

  // Store delegation CAR bytes in R2 (matching w3infra format)
  // Each delegation is encoded as a CAR file containing just that delegation
  await Promise.all(delegations.map(async (delegation) => {
    // Encode single delegation as CAR bytes
    const carBytes = delegationsToBytes([delegation])
    
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: createDelegationsBucketKey(delegation.cid),  // /delegations/{cid-base32}.car
      Body: carBytes,
      ContentType: 'application/car',
    })
    
    await client.send(command)
    console.log(`      ✓ Stored delegation CAR to R2: ${createDelegationsBucketKey(delegation.cid)}`)
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

/**
 * Find a space→account delegation by issuer (space)
 * 
 * Spaces delegate authority to accounts (did:mailto). This delegation is signed
 * by the space's private key and proves the account has authority over the space.
 * Uses the 'issuer' global secondary index to find delegations where the space
 * is the issuer and the account is the audience.
 * 
 * @param {string} spaceDID - The space DID to query (e.g., did:key:z6Mkk...)
 * @returns {Promise<import('@ucanto/interface').Delegation | null>}
 */
export async function findDelegationByIssuer(spaceDID) {
  const dynamoClient = getDynamoClient()
  const s3Client = getS3Client()

  // Query DynamoDB using issuer index - limit to 1 result
  const queryCommand = new QueryCommand({
    TableName: config.tables.delegation,
    IndexName: 'issuer',
    KeyConditionExpression: 'issuer = :issuer',
    ExpressionAttributeValues: {
      ':issuer': spaceDID,
    },
    ProjectionExpression: 'link, audience',
    Limit: 1, // Only need one delegation
  })

  const response = await dynamoClient.send(queryCommand)
  const items = response.Items ?? []

  if (items.length === 0) {
    console.log(`      ℹ No delegation found for issuer: ${spaceDID}`)
    return null
  }

  const item = items[0]
  const link = parseLink(item.link)
  
  console.log(`      ✓ Found delegation from space: ${spaceDID} → ${item.audience}`)

  // Try R2 first (primary storage), fall back to S3 if not found
  const key = createDelegationsBucketKey(link)
  console.log(`      ✓ Fetching delegation from R2: ${key}`)
  let carBytes
  
  // Try R2 first if configured (w3infra staging uses R2)
  if (config.storage.r2DelegationBucket) {
    try {
      const r2Client = getR2Client()
      const r2Command = new GetObjectCommand({
        Bucket: config.storage.r2DelegationBucket,
        Key: key,
      })
      const r2Object = await r2Client.send(r2Command)
      carBytes = await r2Object.Body?.transformToByteArray()
      console.log(`      ✓ Fetched from R2: ${config.storage.r2DelegationBucket}`)
    } catch (r2Error) {
      // If R2 fails with NoSuchKey, try S3 fallback
      if (r2Error && typeof r2Error === 'object' && 'name' in r2Error && r2Error.name === 'NoSuchKey') {
        console.log(`      ℹ Not in R2, trying S3...`)
        try {
          const s3Command = new GetObjectCommand({
            Bucket: config.storage.delegationBucket,
            Key: key,
          })
          const s3Object = await s3Client.send(s3Command)
          carBytes = await s3Object.Body?.transformToByteArray()
          console.log(`      ✓ Fetched from S3: ${config.storage.delegationBucket}`)
        } catch (s3Error) {
          console.error(`      ✗ Failed to fetch from both R2 and S3:`)
          console.error(`        R2: ${getErrorMessage(r2Error)}`)
          console.error(`        S3: ${getErrorMessage(s3Error)}`)
          throw s3Error
        }
      } else {
        // Other R2 errors (permissions, network, etc.)
        console.error(`      ✗ Failed to fetch delegation ${link}:`, getErrorMessage(r2Error), { cause: r2Error })
        throw r2Error
      }
    }
  } else {
    // No R2 configured, use S3 only
    try {
      const s3Command = new GetObjectCommand({
        Bucket: config.storage.delegationBucket,
        Key: key,
      })
      const s3Object = await s3Client.send(s3Command)
      carBytes = await s3Object.Body?.transformToByteArray()
      console.log(`      ✓ Fetched from S3: ${config.storage.delegationBucket}`)
    } catch (s3Error) {
      console.error(`      ✗ Failed to fetch delegation ${link}:`, getErrorMessage(s3Error), { cause: s3Error })
      throw s3Error
    }
  }

  if (!carBytes) {
    throw new Error(`Delegation ${link} not found in CAR file`)
  }
  
  // Parse delegation from CAR bytes
  const delegationsList = bytesToDelegations(carBytes)
  const delegation = delegationsList.find(d => d.cid.equals(link))
  
  if (!delegation) {
    throw new Error(`Delegation ${link} not found in CAR file`)
  }

  console.log(`        - ${delegation.issuer.did()} → ${delegation.audience.did()} (${delegation.capabilities[0]?.can})`)
  return delegation
}
