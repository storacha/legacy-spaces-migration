/**
 * Query Consumer Table to get space ownership (space -> customer mapping)
 */
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
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

// Cache for space -> customer lookups
// Reset per migration run (not persistent across restarts)
const customerCache = new Map()

/**
 * Get customer (account) DID for a given space (with caching)
 * 
 * @param {string} space - Space DID (consumer)
 * @returns {Promise<string | null>} - Customer DID (did:mailto:...) or null if not found
 */
export async function getCustomerForSpace(space) {
  // Check cache first
  if (customerCache.has(space)) {
    return customerCache.get(space)
  }
  
  // Query DynamoDB
  const client = createDynamoClient()
  
  const command = new QueryCommand({
    TableName: config.tables.consumer,
    IndexName: 'consumerV2',
    KeyConditionExpression: 'consumer = :consumer',
    ExpressionAttributeValues: {
      ':consumer': { S: space },
    },
    ProjectionExpression: 'customer',
    Limit: 1,
  })
  
  const response = await client.send(command)
  
  const customer = response.Items && response.Items.length > 0
    ? unmarshall(response.Items[0]).customer || null
    : null
  
  // Cache the result (even if null to avoid repeated failed lookups)
  customerCache.set(space, customer)
  
  return customer
}

/**
 * Get cache statistics for monitoring
 * 
 * @returns {{ size: number, hitRate: number }}
 */
export function getCacheStats() {
  return {
    size: customerCache.size,
  }
}

/**
 * Clear the cache (useful for testing)
 */
export function clearCache() {
  customerCache.clear()
}
