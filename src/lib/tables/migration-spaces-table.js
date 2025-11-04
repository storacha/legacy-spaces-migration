/**
 * Migration Spaces Table operations
 * 
 * Tracks one migration space per customer for storing legacy indexes
 */
import { GetCommand, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

/**
 * Get migration space for a customer
 * 
 * @param {string} customer - Customer account DID (did:mailto:...)
 * @returns {Promise<{migrationSpace: string, spaceName: string, indexCount: number, privateKey?: string} | null>}
 */
export async function getMigrationSpace(customer) {
  const client = getDynamoClient()
  
  const command = new GetCommand({
    TableName: config.tables.migrationSpaces,
    Key: { customer },
    ProjectionExpression: 'migrationSpace, spaceName, indexCount, #status, privateKey',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
  })
  
  const response = await client.send(command)
  
  if (!response.Item) {
    return null
  }
  
  return {
    migrationSpace: response.Item.migrationSpace,
    spaceName: response.Item.spaceName,
    indexCount: response.Item.indexCount || 0,
    status: response.Item.status,
    privateKey: response.Item.privateKey, // Encrypted private key
  }
}

/**
 * Create a new migration space record
 * 
 * @param {object} params
 * @param {string} params.customer - Customer account DID
 * @param {string} params.migrationSpace - Migration space DID
 * @param {string} params.spaceName - Human-readable space name
 * @param {string} [params.privateKey] - Encrypted private key (optional)
 * @returns {Promise<void>}
 */
export async function createMigrationSpace({ customer, migrationSpace, spaceName, privateKey }) {
  const client = getDynamoClient()
  
  const now = new Date().toISOString()
  
  const item = {
    customer,
    migrationSpace,
    spaceName,
    created: now,
    lastUsed: now,
    indexCount: 0,
    status: 'active',
  }
  
  // Add encrypted private key if provided
  if (privateKey) {
    item.privateKey = privateKey
  }
  
  const command = new PutCommand({
    TableName: config.tables.migrationSpaces,
    Item: item,
    // Prevent overwriting if race condition
    ConditionExpression: 'attribute_not_exists(customer)',
  })
  
  try {
    await client.send(command)
  } catch (error) {
    // If space already exists (race condition), that's OK
    if (error.name === 'ConditionalCheckFailedException') {
      console.log(`    ⊘ Migration space already exists for ${customer}`)
      return
    }
    throw error
  }
}

/**
 * Update migration space status to provisioned
 * 
 * @param {string} customer - Customer account DID
 * @returns {Promise<void>}
 */
export async function markSpaceAsProvisioned(customer) {
  const client = createDynamoClient()
  
  const command = new UpdateItemCommand({
    TableName: config.tables.migrationSpaces,
    Key: marshall({ customer }),
    UpdateExpression: 'SET #status = :status, lastUsed = :now',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: marshall({
      ':status': 'provisioned',
      ':now': new Date().toISOString(),
    }),
  })
  
  await client.send(command)
}

/**
 * Increment index count and update last used timestamp
 * 
 * @param {string} customer - Customer account DID
 * @returns {Promise<void>}
 */
export async function incrementIndexCount(customer) {
  const client = getDynamoClient()
  
  const command = new UpdateCommand({
    TableName: config.tables.migrationSpaces,
    Key: { customer },
    UpdateExpression: 'SET indexCount = if_not_exists(indexCount, :zero) + :one, lastUsed = :now',
    ExpressionAttributeValues: {
      ':zero': 0,
      ':one': 1,
      ':now': new Date().toISOString(),
    },
  })
  
  await client.send(command)
}
