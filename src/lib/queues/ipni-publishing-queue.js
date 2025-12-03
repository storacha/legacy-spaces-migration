/**
 * Singleton IPNI Publishing Queue
 * 
 * Provides a single instance of SQSPublishingQueue to avoid creating
 * multiple connections for every location claim publish operation.
 */

import { SQSPublishingQueue } from '../ipni/sqsqueue.js'
import { config } from '../../config.js'

/** @type {SQSPublishingQueue | null} */
let queueInstance = null

/**
 * Get or create the IPNI publishing queue singleton
 * @returns {SQSPublishingQueue}
 */
export function getIPNIPublishingQueue() {
  if (!queueInstance) {
    queueInstance = new SQSPublishingQueue({
      queueUrl: config.queues.ipniPublishingQueue,
      bucketName: config.storage.ipniPublishingBucket,
    })
  }
  return queueInstance
}

/**
 * Reset the queue instance (useful for testing)
 */
export function resetIPNIPublishingQueue() {
  queueInstance = null
}
