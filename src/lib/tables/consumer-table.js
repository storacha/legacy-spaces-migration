/**
 * Query Consumer Table to get space ownership (space -> customer mapping)
 */
import { GetCommand, PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb'
import { CBOR } from '@ucanto/core'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

// Cache for space -> customer lookups
// Reset per migration run (not persistent across restarts)
const customerCache = new Map()

/**
 * Get customer (account) DID for a given space (with caching)
 * It uses the consumerV2 index to be able to retrieve the customer from the space
 * 
 * @param {string} space - Space DID (consumer)
 * @returns {Promise<string | null>} - Customer DID (did:mailto:...) or null if not found
 */
export async function getCustomerForSpace(space) {
  // Check cache first
  if (customerCache.has(space)) {
    return customerCache.get(space)
  }
  
  // Query DynamoDB using the consumer GSI
  // The consumer table has composite key (subscription, provider) but consumer is indexed via GSI
  const client = getDynamoClient()
  
  // Try querying with each configured provider until we find the space
  for (const provider of config.services.storageProviders) {
    const command = new QueryCommand({
      TableName: config.tables.consumer,
      IndexName: 'consumerV2',
      KeyConditionExpression: 'consumer = :consumer',
      ExpressionAttributeValues: {
        ':consumer': space,
      },
      Limit: 1,
    })
    
    const response = await client.send(command)
    
    if (response.Items && response.Items.length > 0) {
      const customer = response.Items[0].customer
      // Cache the result
      customerCache.set(space, customer)
      return customer
    }
  }
  
  // Not found in any provider
  customerCache.set(space, null)
  return null
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

/**
 * Generate subscription ID from space (consumer)
 * 
 * Follows w3infra's exact pattern: subscription = CID(CBOR({ consumer: space }))
 * Note: The CBOR payload uses "consumer" as the key to match w3infra's implementation.
 * 
 * @param {string} space - Space DID (consumer)
 * @returns {Promise<string>} - Subscription ID (CID as string)
 */
async function createProvisionSubscriptionId(space) {
  // Match w3infra's exact implementation:
  // https://github.com/storacha/w3infra/blob/main/upload-api/stores/provisions.js
  // The CBOR must use 'consumer' as the key to generate the same CID
  const subscription = (await CBOR.write({ consumer: space })).cid.toString()
  return subscription
}

/**
 * Provision a space to a customer (direct DB write for migration)
 * 
 * This bypasses the provider/add UCAN invocation and writes directly to DynamoDB.
 * Creates both subscription and consumer records, following w3infra's pattern.
 * 
 * @param {string} customer - Customer account DID (did:mailto:...)
 * @param {string} space - Space DID (did:key:...)
 * @param {string} [provider] - Provider DID (default: service DID from config)
 * @returns {Promise<void>}
 */
export async function provisionSpace(customer, space, provider) {
  const client = getDynamoClient()
  
  const providerDID = provider || config.credentials.serviceDID
  const now = new Date().toISOString()
  
  // Generate subscription ID deterministically from consumer (space)
  const subscription = await createProvisionSubscriptionId(space)
  
  // Step 1: Create subscription record (idempotent)
  const subscriptionCommand = new PutCommand({
    TableName: config.tables.subscription,
    Item: {
      subscription,           // Subscription ID (partition key)
      provider: providerDID,  // Provider DID (sort key)
      customer,               // Customer account DID
      insertedAt: now,
    },
    // Don't overwrite if already exists (idempotent)
    ConditionExpression: 'attribute_not_exists(subscription) AND attribute_not_exists(provider)',
  })
  
  try {
    await client.send(subscriptionCommand)
    console.log(`    ✓ Created subscription ${subscription}`)
  } catch (error) {
    if (error.name === 'ConditionalCheckFailedException') {
      console.log(`    ⊘ Subscription already exists`)
      // Continue to create consumer record
    } else {
      throw new Error(`Failed to create subscription: ${error.message}`)
    }
  }
  
  // Step 2: Create consumer record (links space to subscription)
  const consumerCommand = new PutCommand({
    TableName: config.tables.consumer,
    Item: {
      subscription,           // Subscription ID (partition key)
      provider: providerDID,  // Provider DID (sort key)
      consumer: space,        // Space DID (indexed via GSI)
      customer,               // Customer account DID
      insertedAt: now,
    },
    // Don't overwrite if already exists (idempotent)
    ConditionExpression: 'attribute_not_exists(subscription) AND attribute_not_exists(provider)',
  })
  
  try {
    await client.send(consumerCommand)
    console.log(`    ✓ Provisioned space ${space} to ${customer}`)
  } catch (error) {
    if (error.name === 'ConditionalCheckFailedException') {
      console.log(`    ⊘ Space already provisioned`)
      return
    }
    throw new Error(`Failed to provision space: ${error.message}`)
  }
}
