/**
 * Singleton IPNI Publishing Queue
 *
 * Provides a single instance of SQSPublishingQueue to avoid creating
 * multiple connections for every location claim publish operation.
 */

import { SQSPublishingQueue } from '../ipni/sqsqueue.js'
import { config } from '../../config.js'

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
