/**
 * Singleton IPNI Publishing Queue
 *
 * Provides a single instance of SQSPublishingQueue to avoid creating
 * multiple connections for every location claim publish operation.
 */

import { SQSPublishingQueue } from '../ipni/sqsqueue.js'
import { SQSClient } from '@aws-sdk/client-sqs'
import { S3Client } from '@aws-sdk/client-s3'
import { config } from '../../config.js'

/** @type {SQSClient | null} */
let sqsClient = null
/** @type {S3Client | null} */
let s3Client = null

/**
 * Get or create SQS client with correct region
 * @returns {SQSClient}
 */
function getSQSClient() {
  if (!sqsClient) {
    sqsClient = new SQSClient({
      region: config.aws.region,
      credentials: config.aws.accessKeyId
        ? {
            accessKeyId: config.aws.accessKeyId,
            secretAccessKey: config.aws.secretAccessKey || '',
          }
        : undefined,
    })
  }
  return sqsClient
}

/**
 * Get or create S3 client with correct region
 * @returns {S3Client}
 */
function getS3Client() {
  if (!s3Client) {
    s3Client = new S3Client({
      region: config.aws.region,
      credentials: config.aws.accessKeyId
        ? {
            accessKeyId: config.aws.accessKeyId,
            secretAccessKey: config.aws.secretAccessKey || '',
          }
        : undefined,
    })
  }
  return s3Client
}

/** @type {SQSPublishingQueue | null} */
let blobQueueInstance = null
/** @type {SQSPublishingQueue | null} */
let storeQueueInstance = null
/**
 * Get or create the IPNI publishing queue singleton
 * @param {'blob' | 'store'} protocol - Protocol type for the queue
 * @returns {SQSPublishingQueue}
 */
export function getIPNIPublishingQueue(protocol) {
  switch (protocol) {
    case 'blob':
      return getBlobPublishingQueue()
    case 'store':
      return getStorePublishingQueue()
    default:
      throw new Error(`Unknown protocol type: ${protocol}`)
  }
}

/**
 * Get or create the Blob IPNI publishing queue singleton
 * @returns {SQSPublishingQueue}
 */
export function getBlobPublishingQueue() {
  if (!blobQueueInstance) {
    blobQueueInstance = new SQSPublishingQueue({
      queueUrl: config.queues.ipniBlobPublishingQueue,
      bucketName: config.storage.ipniBlobPublishingBucket,
      sqsClient: getSQSClient(),
      s3Client: getS3Client(),
    })
  }
  return blobQueueInstance
}

/**
 * Get or create the Store IPNI publishing queue singleton
 * @returns {SQSPublishingQueue}
 */
export function getStorePublishingQueue() {
  if (!storeQueueInstance) {
    storeQueueInstance = new SQSPublishingQueue({
      queueUrl: config.queues.ipniStorePublishingQueue,
      bucketName: config.storage.ipniStorePublishingBucket,
      sqsClient: getSQSClient(),
      s3Client: getS3Client(),
    })
  }
  return storeQueueInstance
}

/**
 * Reset the queue instance (useful for testing)
 * @param {'blob' | 'store'} protocol - Protocol type for the queue
 */
export function resetIPNIPublishingQueue(protocol) {
  switch (protocol) {
    case 'blob':
      blobQueueInstance = null
      break
    case 'store':
      storeQueueInstance = null
      break
    default:
      throw new Error(`Unknown protocol type: ${protocol}`)
  }
}
