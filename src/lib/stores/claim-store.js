/**
 * Delegation table operations for storing space delegations
 *
 * Stores delegations in DynamoDB for indexing and S3/R2 for content.
 * Simplified version for migration - only supports putMany for now.
 */
import {
  PutObjectCommand,
  GetObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3'
import { Delegation } from '@ucanto/core'
import { config } from '../../config.js'
import { getErrorMessage } from '../error-utils.js'

/**
 * Cached S3 clients
 * @type {S3Client | null}
 */
let cachedS3Client = null

/**
 * Get or create S3 client for AWS S3
 * Caches the client to avoid creating multiple connections
 */
function getS3Client() {
  if (!cachedS3Client) {
    cachedS3Client = new S3Client({
      region: config.aws.region,
      credentials: config.aws.accessKeyId
        ? {
            accessKeyId: config.aws.accessKeyId,
            secretAccessKey: config.aws.secretAccessKey || '',
          }
        : undefined,
    })
  }
  return cachedS3Client
}

/**
 * Create claim S3 key in piri format
 *
 * @param {import('multiformats/link').Link} link - Delegation CID
 * @returns {string} - S3 key: /delegations/{cid-base32}.car
 */
function createClaimBucketKey(link) {
  return `${link.toString()}`
}

/**
 * Store delegations in DynamoDB + S3
 *
 * Follows w3infra's exact pattern:
 * 1. Encode delegation as CAR bytes using delegationsToBytes([delegation])
 * 2. Store in S3/R2 with key: /delegations/{cid-base32}.car
 * 3. Index in DynamoDB with link (CID), audience, issuer, expiration, cause
 *
 * @param {import('@ucanto/interface').Delegation} delegation
 * @returns {Promise<void>}
 */
export async function storeClaim(delegation) {
  const client = getS3Client()
  const bucketName = config.storage.claimsBucket

  const res = await delegation.archive()
  if (res.error) {
    throw new Error(`Failed to archive delegation: ${getErrorMessage(res.error)}`, { cause: res.error })
  }

  const carBytes = res.ok

  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: createClaimBucketKey(delegation.cid), // /delegations/{cid-base32}.car
    Body: carBytes,
    ContentType: 'application/car',
  })

  await client.send(command)
  console.log(
    `      ✓ Stored delegation CAR to S3: ${createClaimBucketKey(
      delegation.cid
    )}`
  )
}

/**
 * Find a delegation by CID
 *
 * @param {import('multiformats/link').Link} link - The CID of the delegation to query
 * @returns {Promise<import('@ucanto/interface').Delegation | null>}
 */
export async function getClaim(link) {
  const s3Client = getS3Client()

  // Try R2 first (primary storage), fall back to S3 if not found
  const key = createClaimBucketKey(link)
  console.log(`      ✓ Fetching delegation from S3: ${key}`)
  let carBytes
  try {
    const s3Command = new GetObjectCommand({
      Bucket: config.storage.claimsBucket,
      Key: key,
    })
    const s3Object = await s3Client.send(s3Command)
    if (!s3Object.Body) {
      throw new Error(`No body found in S3 object for key: ${key}`)
    }
    carBytes = await s3Object.Body.transformToByteArray()
    console.log(`      ✓ Fetched from S3: ${config.storage.claimsBucket}`)
  } catch (s3Error) {
    console.error(
      `      ✗ Failed to fetch delegation ${link}:`,
      getErrorMessage(s3Error), { cause: s3Error }
    )
    throw s3Error
  }

  // Parse delegation from CAR bytes
  const res = await Delegation.extract(carBytes)

  if (res.error) {
    throw new Error(
      `Failed to extract delegation from CAR: ${getErrorMessage(res.error)}`,
      { cause: res.error }
    )
  }

  const delegation = res.ok
  console.log(
    `        - ${delegation.issuer.did()} → ${delegation.audience.did()} (${
      delegation.capabilities[0]?.can
    })`
  )
  return delegation
}
