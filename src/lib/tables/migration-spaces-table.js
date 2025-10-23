/**
 * Migration Spaces Table operations
 * 
 * Tracks one migration space per customer for storing legacy indexes
 */
import { DynamoDBClient, GetItemCommand, PutItemCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { config } from '../../config.js'

/**
 * Create DynamoDB client
 */
export function createDynamoClient() {
  return new DynamoDBClient({
    region: config.aws.region,
    credentials: config.aws.accessKeyId ? {
      accessKeyId: config.aws.accessKeyId,
      secretAccessKey: config.aws.secretAccessKey,
    } : undefined,
  })
}

/**
 * Get migration space for a customer
 * 
 * @param {string} customer - Customer account DID (did:mailto:...)
 * @returns {Promise<{migrationSpace: string, spaceName: string, indexCount: number, privateKey?: string} | null>}
 */
export async function getMigrationSpace(customer) {
  const client = createDynamoClient()
  
  const command = new GetItemCommand({
    TableName: config.tables.migrationSpaces,
    Key: marshall({ customer }),
    ProjectionExpression: 'migrationSpace, spaceName, indexCount, #status, privateKey',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
  })
  
  const response = await client.send(command)
  
  if (!response.Item) {
    return null
  }
  
  const item = unmarshall(response.Item)
  return {
    migrationSpace: item.migrationSpace,
    spaceName: item.spaceName,
    indexCount: item.indexCount || 0,
    status: item.status,
    privateKey: item.privateKey, // Encrypted private key
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
  const client = createDynamoClient()
  
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
  
  const command = new PutItemCommand({
    TableName: config.tables.migrationSpaces,
    Item: marshall(item),
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
  const client = createDynamoClient()
  
  const command = new UpdateItemCommand({
    TableName: config.tables.migrationSpaces,
    Key: marshall({ customer }),
    UpdateExpression: 'SET indexCount = if_not_exists(indexCount, :zero) + :one, lastUsed = :now',
    ExpressionAttributeValues: marshall({
      ':zero': 0,
      ':one': 1,
      ':now': new Date().toISOString(),
    }),
  })
  
  await client.send(command)
}
