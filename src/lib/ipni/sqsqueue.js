import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
} from '@aws-sdk/client-s3'
import { v4 as uuidv4 } from 'uuid'
import { ProviderResult } from './provider-result.js'

/**
 * @typedef {Object} AddrInfo
 * @property {import('@libp2p/interface').PeerId} id - Peer ID
 * @property {import('@multiformats/multiaddr').Multiaddr[]} addrs - Array of multiaddrs
 *
 * @typedef {Object} PublishingJob
 * @property {AddrInfo} providerInfo - Peer address info
 * @property {string} contextID - Context ID string
 * @property {import('multiformats/hashes/interface').MultihashDigest[]} digests - Array of multihashes
 * @property {Uint8Array} metadata - Serialized metadata bytes
 */

/**
 * SQSPublishingQueue is a combined implementation of ExtendedQueue and jobMarshaller
 * It stores publishing jobs in SQS with extended data in S3
 * Ported from https://github.com/storacha/go-libstoracha/blob/main/ipnipublisher/queue/aws/sqspublishingqueue.go
 */
export class SQSPublishingQueue {
  /**
   * @param {Object} options
   * @param {string} options.queueUrl - SQS queue URL
   * @param {string} options.bucketName - S3 bucket name
   * @param {SQSClient} [options.sqsClient] - SQS client instance
   * @param {S3Client} [options.s3Client] - S3 client instance
   */
  constructor({ queueUrl, bucketName, sqsClient, s3Client }) {
    this.queueUrl = queueUrl
    this.bucketName = bucketName
    this.sqsClient = sqsClient || new SQSClient({})
    this.s3Client = s3Client || new S3Client({})
  }

  /**
   * Send a publishing job to the queue
   * Marshals the job into a ProviderResult message with extended data in S3
   * @param {PublishingJob} job
   * @returns {Promise<void>}
   */
  async sendJob(job) {
    // Marshal the job to a serializable format
    const serialized = await this._marshallJob(job)

    // Generate a unique job ID for S3 storage
    const jobId = uuidv4()

    try {
      // Upload extended data (digests) to S3
      if (serialized.extended && serialized.extended.length > 0) {
        await this.s3Client.send(
          new PutObjectCommand({
            Bucket: this.bucketName,
            Key: jobId,
            Body: serialized.extended,
          })
        )
      }

      // Send the message to SQS with S3 reference
      await this.sqsClient.send(
        new SendMessageCommand({
          QueueUrl: this.queueUrl,
          MessageBody: JSON.stringify({
            JobID: jobId,
            Message: serialized.message,
          }),
          MessageGroupId: serialized.groupId, // For FIFO queues
        })
      )
    } catch (error) {
      // Clean up S3 object if SQS send fails
      try {
        await this.s3Client.send(
          new DeleteObjectCommand({
            Bucket: this.bucketName,
            Key: jobId,
          })
        )
      } catch (cleanupError) {
        console.error('Failed to clean up S3 object:', cleanupError)
      }
      throw error
    }
  }

  /**
   * Marshal a PublishingJob into a serializable format
   * @private
   * @param {PublishingJob} job
   * @returns {Promise<{message: Object, extended: Uint8Array, groupId: string}>}
   */
  async _marshallJob(job) {
    // Convert digests to JSON
    const digestsJson = JSON.stringify(job.digests)
    const extended = new TextEncoder().encode(digestsJson)

    // Base64 encode the context ID for use as GroupID (FIFO queue ordering)
    const groupId = Buffer.from(job.contextID).toString('base64')

    // Create a ProviderResult with the job data
    const message = new ProviderResult({
      contextID: Buffer.from(job.contextID),
      metadata: job.metadata,
      provider: job.providerInfo,
    })

    return {
      message: message.toJSON(),
      extended,
      groupId,
    }
  }
}
