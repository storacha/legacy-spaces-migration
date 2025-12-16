/**
 * Migration Progress Table operations
 * 
 * Tracks space-level migration progress for resume/retry capability
 * 
 * Table Schema:
 * - PK: customer (string) - Customer DID
 * - SK: space (string) - Space DID
 * - status (string) - 'pending' | 'in-progress' | 'completed' | 'failed'
 * - totalUploads (number) - Total uploads in this space
 * - completedUploads (number) - Number of uploads migrated
 * - lastProcessedUpload (string) - Last upload CID processed
 * - instanceId (string) - EC2 instance processing this space
 * - workerId (string) - Worker ID processing this space
 * - error (string) - Error message if failed
 * - createdAt (string) - ISO timestamp
 * - updatedAt (string) - ISO timestamp
 * 
 * GSI: status-index
 * - PK: status
 * - SK: updatedAt
 */

import { 
  GetCommand, 
  PutCommand, 
  UpdateCommand,
  QueryCommand,
  ScanCommand 
} from '@aws-sdk/lib-dynamodb'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

const PROGRESS_TABLE = config.tables.migrationProgress

/**
 * Get progress for a specific space
 * 
 * @param {string} customer - Customer DID
 * @param {string} space - Space DID
 * @returns {Promise<{customer: string, space: string, status: string, totalUploads?: number, completedUploads?: number, lastProcessedUpload?: string, instanceId?: string, workerId?: string, error?: string, createdAt: string, updatedAt: string} | null>}
 */
export async function getSpaceProgress(customer, space) {
  const client = getDynamoClient()
  
  const command = new GetCommand({
    TableName: PROGRESS_TABLE,
    Key: { customer, space },
  })
  
  const response = await client.send(command)
  // @ts-expect-error - DynamoDB returns Record<string, any> but we know the shape
  return response.Item || null
}

/**
 * Create initial progress record for a space
 * 
 * @param {object} params
 * @param {string} params.customer - Customer DID
 * @param {string} params.space - Space DID
 * @param {number} params.totalUploads - Total uploads in space
 * @param {string} params.instanceId - EC2 instance ID
 * @param {string} params.workerId - Worker ID
 * @returns {Promise<void>}
 */
export async function createSpaceProgress({ customer, space, totalUploads, instanceId, workerId }) {
  const client = getDynamoClient()
  
  const now = new Date().toISOString()
  
  const command = new PutCommand({
    TableName: PROGRESS_TABLE,
    Item: {
      customer,
      space,
      status: 'in-progress',
      totalUploads,
      completedUploads: 0,
      instanceId,
      workerId,
      createdAt: now,
      updatedAt: now,
    },
    ConditionExpression: 'attribute_not_exists(customer)',
  })
  
  try {
    await client.send(command)
  } catch (error) {
    // @ts-expect-error - If already exists, that's OK (resume scenario)
    if (error.name === 'ConditionalCheckFailedException') {
      return
    }
    throw error
  }
}

/**
 * Update progress for a space
 * 
 * @param {object} params
 * @param {string} params.customer - Customer DID
 * @param {string} params.space - Space DID
 * @param {number} params.completedUploads - Number of uploads completed
 * @param {string} [params.lastProcessedUpload] - Last upload CID processed
 * @returns {Promise<void>}
 */
export async function updateSpaceProgress({ customer, space, completedUploads, lastProcessedUpload }) {
  const client = getDynamoClient()
  
  const updateExpression = lastProcessedUpload
    ? 'SET completedUploads = :completed, lastProcessedUpload = :lastUpload, updatedAt = :now'
    : 'SET completedUploads = :completed, updatedAt = :now'
  
  const expressionValues = {
    ':completed': completedUploads,
    ':now': new Date().toISOString(),
  }
  
  if (lastProcessedUpload) {
    // @ts-expect-error - Property ':lastUpload' does not exist on type '{ ':completed': number; ':now': string; }'.
    expressionValues[':lastUpload'] = lastProcessedUpload
  }
  
  const command = new UpdateCommand({
    TableName: PROGRESS_TABLE,
    Key: { customer, space },
    UpdateExpression: updateExpression,
    ExpressionAttributeValues: expressionValues,
  })
  
  await client.send(command)
}

/**
 * Mark space as completed
 * 
 * @param {string} customer - Customer DID
 * @param {string} space - Space DID
 * @returns {Promise<void>}
 */
export async function markSpaceCompleted(customer, space) {
  const client = getDynamoClient()
  
  const command = new UpdateCommand({
    TableName: PROGRESS_TABLE,
    Key: { customer, space },
    UpdateExpression: 'SET #status = :status, updatedAt = :now, #error = :empty',
    ExpressionAttributeNames: {
      '#status': 'status',
      '#error': 'error',
    },
    ExpressionAttributeValues: {
      ':status': 'completed',
      ':now': new Date().toISOString(),
      ':empty': '',
    },
  })
  
  await client.send(command)
}

/**
 * Mark space as failed
 * 
 * @param {string} customer - Customer DID
 * @param {string} space - Space DID
 * @param {string} error - Error message
 * @returns {Promise<void>}
 */
export async function markSpaceFailed(customer, space, error) {
  const client = getDynamoClient()
  
  const command = new UpdateCommand({
    TableName: PROGRESS_TABLE,
    Key: { customer, space },
    UpdateExpression: 'SET #status = :status, #error = :error, updatedAt = :now',
    ExpressionAttributeNames: {
      '#status': 'status',
      '#error': 'error',
    },
    ExpressionAttributeValues: {
      ':status': 'failed',
      ':error': error,
      ':now': new Date().toISOString(),
    },
  })
  
  await client.send(command)
}

/**
 * Get all spaces for a customer
 * 
 * @param {string} customer - Customer DID
 * @returns {Promise<Array<{customer: string, space: string, status: string, totalUploads?: number, completedUploads?: number, instanceId?: string, workerId?: string, error?: string, updatedAt?: string}>>}
 */
export async function getCustomerSpaces(customer) {
  const client = getDynamoClient()
  
  const command = new QueryCommand({
    TableName: PROGRESS_TABLE,
    KeyConditionExpression: 'customer = :customer',
    ExpressionAttributeValues: {
      ':customer': customer,
    },
  })
  
  const response = await client.send(command)
  // @ts-expect-error - DynamoDB returns Record<string, any>[] but we know the shape
  return response.Items || []
}

/**
 * Get failed migrations
 * 
 * @returns {Promise<Array<{customer: string, space: string, status: string, instanceId?: string, workerId?: string, error?: string, updatedAt: string}>>}
 */
export async function getFailedMigrations() {
  const client = getDynamoClient()
  
  const command = new ScanCommand({
    TableName: PROGRESS_TABLE,
    FilterExpression: '#status = :failed',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':failed': 'failed',
    },
  })
  
  const response = await client.send(command)
  // @ts-expect-error - DynamoDB returns Record<string, any>[] but we know the shape
  return response.Items || []
}

/**
 * Get stuck migrations (in-progress for >1 hour)
 * 
 * @returns {Promise<Array<{customer: string, space: string, status: string, instanceId?: string, workerId?: string, completedUploads?: number, totalUploads?: number, updatedAt: string}>>}
 */
export async function getStuckMigrations() {
  const client = getDynamoClient()
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString()
  
  const command = new ScanCommand({
    TableName: PROGRESS_TABLE,
    FilterExpression: '#status = :inProgress AND updatedAt < :threshold',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':inProgress': 'in-progress',
      ':threshold': oneHourAgo,
    },
  })
  
  const response = await client.send(command)
  // @ts-expect-error - DynamoDB returns Record<string, any>[] but we know the shape
  return response.Items || []
}

/**
 * Get spaces by instance
 * 
 * @param {string} instanceId - Instance ID
 * @returns {Promise<Array<{customer: string, space: string, status: string, totalUploads?: number, completedUploads?: number, workerId?: string}>>}
 */
export async function getInstanceSpaces(instanceId) {
  const client = getDynamoClient()
  
  const command = new ScanCommand({
    TableName: PROGRESS_TABLE,
    FilterExpression: 'instanceId = :instanceId',
    ExpressionAttributeValues: {
      ':instanceId': instanceId,
    },
  })
  
  const response = await client.send(command)
  // @ts-expect-error - DynamoDB returns Record<string, any>[] but we know the shape
  return response.Items || []
}

/**
 * Scan all progress records (for statistics)
 * 
 * @param {object} [options]
 * @param {Record<string, any>} [options.lastEvaluatedKey] - For pagination
 * @returns {Promise<{items: Array<{customer: string, space: string, status: string, totalUploads?: number, completedUploads?: number, instanceId?: string, workerId?: string}>, lastEvaluatedKey?: Record<string, any>}>}
 */
export async function scanAllProgress(options = {}) {
  const client = getDynamoClient()
  
  const command = new ScanCommand({
    TableName: PROGRESS_TABLE,
    ExclusiveStartKey: options.lastEvaluatedKey,
  })
  
  const response = await client.send(command)
  
  return {
    // @ts-expect-error - DynamoDB returns Record<string, any>[] but we know the shape
    items: response.Items || [],
    lastEvaluatedKey: response.LastEvaluatedKey,
  }
}
