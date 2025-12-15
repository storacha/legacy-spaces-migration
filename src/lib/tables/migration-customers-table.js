/**
 * Migration Customers Table operations
 * 
 * Tracks customer-level migration progress for resume/retry capability
 * 
 * Table Schema:
 * - PK: customer (string) - Customer DID (did:mailto:...)
 * - status (string) - 'pending' | 'in-progress' | 'completed' | 'failed'
 * - totalSpaces (number) - Total spaces for this customer
 * - completedSpaces (number) - Number of spaces migrated
 * - totalUploads (number) - Total uploads across all spaces
 * - completedUploads (number) - Number of uploads migrated
 * - instanceId (string) - EC2 instance assigned to this customer
 * - filter (string) - Filter used when assigning (e.g., 'storacha.network')
 * - error (string) - Error code/summary if failed
 * - assignedAt (string) - ISO timestamp when assigned
 * - updatedAt (string) - ISO timestamp of last update
 * - completedAt (string) - ISO timestamp when completed
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
  ScanCommand,
  BatchWriteCommand
} from '@aws-sdk/lib-dynamodb'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

const CUSTOMERS_TABLE = config.tables.migrationCustomers

/**
 * @typedef {object} MigrationCustomer
 * @property {string} customer - Customer DID
 * @property {'pending'|'in-progress'|'completed'|'failed'} status
 * @property {number} totalSpaces
 * @property {number} completedSpaces
 * @property {number} totalUploads
 * @property {number} completedUploads
 * @property {string} [instanceId]
 * @property {string} [filter]
 * @property {string} [error]
 * @property {string} assignedAt
 * @property {string} updatedAt
 * @property {string} [completedAt]
 */

/**
 * Get a customer's migration status
 * 
 * @param {string} customer - Customer DID
 * @returns {Promise<MigrationCustomer | null>}
 */
export async function getCustomerStatus(customer) {
  const client = getDynamoClient()
  
  const command = new GetCommand({
    TableName: CUSTOMERS_TABLE,
    Key: { customer },
  })
  
  const response = await client.send(command)
  return /** @type {MigrationCustomer | null} */ (response.Item || null)
}

/**
 * Create or update a customer record (used by setup-distribution)
 * 
 * @param {object} params
 * @param {string} params.customer - Customer DID
 * @param {number} params.totalSpaces - Total spaces for customer
 * @param {number} params.totalUploads - Total uploads for customer
 * @param {number} params.instanceId - Assigned instance ID
 * @param {string} [params.filter] - Filter used (e.g., 'storacha.network')
 * @returns {Promise<void>}
 */
export async function assignCustomer({ customer, totalSpaces, totalUploads, instanceId, filter }) {
  const client = getDynamoClient()
  
  const now = new Date().toISOString()
  
  const command = new PutCommand({
    TableName: CUSTOMERS_TABLE,
    Item: {
      customer,
      status: 'pending',
      totalSpaces,
      completedSpaces: 0,
      totalUploads,
      completedUploads: 0,
      instanceId: String(instanceId),
      filter: filter || null,
      assignedAt: now,
      updatedAt: now,
    },
  })
  
  await client.send(command)
}

/**
 * Batch assign multiple customers (used by setup-distribution)
 * 
 * @param {Array<{customer: string, totalSpaces: number, totalUploads: number, instanceId: number, filter?: string}>} customers
 * @returns {Promise<void>}
 */
export async function batchAssignCustomers(customers) {
  const client = getDynamoClient()
  const now = new Date().toISOString()
  
  // DynamoDB batch write limit is 25 items
  const BATCH_SIZE = 25
  
  for (let i = 0; i < customers.length; i += BATCH_SIZE) {
    const batch = customers.slice(i, i + BATCH_SIZE)
    
    const command = new BatchWriteCommand({
      RequestItems: {
        [CUSTOMERS_TABLE]: batch.map(c => ({
          PutRequest: {
            Item: {
              customer: c.customer,
              status: 'pending',
              totalSpaces: c.totalSpaces,
              completedSpaces: 0,
              totalUploads: c.totalUploads,
              completedUploads: 0,
              instanceId: String(c.instanceId),
              filter: c.filter || null,
              assignedAt: now,
              updatedAt: now,
            }
          }
        }))
      }
    })
    
    await client.send(command)
  }
}

/**
 * Mark customer as in-progress
 * 
 * @param {string} customer - Customer DID
 * @returns {Promise<void>}
 */
export async function markCustomerInProgress(customer) {
  const client = getDynamoClient()
  
  const command = new UpdateCommand({
    TableName: CUSTOMERS_TABLE,
    Key: { customer },
    UpdateExpression: 'SET #status = :status, updatedAt = :now',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':status': 'in-progress',
      ':now': new Date().toISOString(),
    },
  })
  
  await client.send(command)
}

/**
 * Update customer progress
 * 
 * @param {object} params
 * @param {string} params.customer - Customer DID
 * @param {number} params.completedSpaces - Number of spaces completed
 * @param {number} params.completedUploads - Number of uploads completed
 * @returns {Promise<void>}
 */
export async function updateCustomerProgress({ customer, completedSpaces, completedUploads }) {
  const client = getDynamoClient()
  
  const command = new UpdateCommand({
    TableName: CUSTOMERS_TABLE,
    Key: { customer },
    UpdateExpression: 'SET completedSpaces = :spaces, completedUploads = :uploads, updatedAt = :now',
    ExpressionAttributeValues: {
      ':spaces': completedSpaces,
      ':uploads': completedUploads,
      ':now': new Date().toISOString(),
    },
  })
  
  await client.send(command)
}

/**
 * Mark customer as completed
 * 
 * @param {string} customer - Customer DID
 * @returns {Promise<void>}
 */
export async function markCustomerCompleted(customer) {
  const client = getDynamoClient()
  
  const now = new Date().toISOString()
  
  const command = new UpdateCommand({
    TableName: CUSTOMERS_TABLE,
    Key: { customer },
    UpdateExpression: 'SET #status = :status, updatedAt = :now, completedAt = :now',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':status': 'completed',
      ':now': now,
    },
  })
  
  await client.send(command)
}

/**
 * Mark customer as failed with error
 * 
 * @param {string} customer - Customer DID
 * @param {string} error - Error code/summary
 * @returns {Promise<void>}
 */
export async function markCustomerFailed(customer, error) {
  const client = getDynamoClient()
  
  const command = new UpdateCommand({
    TableName: CUSTOMERS_TABLE,
    Key: { customer },
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
 * Get customers by status
 * 
 * @param {'pending'|'in-progress'|'completed'|'failed'} status
 * @returns {Promise<MigrationCustomer[]>}
 */
export async function getCustomersByStatus(status) {
  const client = getDynamoClient()
  
  const command = new QueryCommand({
    TableName: CUSTOMERS_TABLE,
    IndexName: 'status-index',
    KeyConditionExpression: '#status = :status',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':status': status,
    },
  })
  
  const response = await client.send(command)
  return /** @type {MigrationCustomer[]} */ (response.Items || [])
}

/**
 * Get customers assigned to a specific instance
 * 
 * @param {string} instanceId - Instance ID
 * @returns {Promise<MigrationCustomer[]>}
 */
export async function getCustomersByInstance(instanceId) {
  const client = getDynamoClient()
  
  const command = new ScanCommand({
    TableName: CUSTOMERS_TABLE,
    FilterExpression: 'instanceId = :instanceId',
    ExpressionAttributeValues: {
      ':instanceId': instanceId,
    },
  })
  
  const response = await client.send(command)
  return /** @type {MigrationCustomer[]} */ (response.Items || [])
}

/**
 * Get failed customers
 * 
 * @returns {Promise<MigrationCustomer[]>}
 */
export async function getFailedCustomers() {
  return getCustomersByStatus('failed')
}

/**
 * Get all customers (for statistics)
 * 
 * @returns {Promise<MigrationCustomer[]>}
 */
export async function getAllCustomers() {
  const client = getDynamoClient()
  
  /** @type {MigrationCustomer[]} */
  const allItems = []
  /** @type {Record<string, any> | undefined} */
  let lastEvaluatedKey = undefined
  
  do {
    const command = new ScanCommand({
      TableName: CUSTOMERS_TABLE,
      ExclusiveStartKey: lastEvaluatedKey,
    })
    
    const response = await client.send(command)
    if (response.Items) {
      allItems.push(.../** @type {MigrationCustomer[]} */ (response.Items))
    }
    lastEvaluatedKey = response.LastEvaluatedKey
  } while (lastEvaluatedKey)
  
  return allItems
}

/**
 * Check if a customer is already completed
 * 
 * @param {string} customer - Customer DID
 * @returns {Promise<boolean>}
 */
export async function isCustomerCompleted(customer) {
  const status = await getCustomerStatus(customer)
  return status?.status === 'completed'
}
