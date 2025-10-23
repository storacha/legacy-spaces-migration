/**
 * Query Consumer Table to get space ownership (space -> customer mapping)
 */
import { DynamoDBClient, QueryCommand, PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall, marshall } from '@aws-sdk/util-dynamodb'
import { CBOR } from '@ucanto/core'
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
  
  const command = new GetItemCommand({
    TableName: config.tables.consumer,
    Key: marshall({ consumer: space }),
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
  const client = createDynamoClient()
  
  const providerDID = provider || config.credentials.serviceDID
  const now = new Date().toISOString()
  
  // Generate subscription ID deterministically from consumer (space)
  const subscription = await createProvisionSubscriptionId(space)
  
  // Step 1: Create subscription record (idempotent)
  const subscriptionCommand = new PutItemCommand({
    TableName: config.tables.subscription,
    Item: marshall({
      subscription,           // Subscription ID (partition key)
      provider: providerDID,  // Provider DID (sort key)
      customer,               // Customer account DID
      insertedAt: now,
    }),
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
  const consumerCommand = new PutItemCommand({
    TableName: config.tables.consumer,
    Item: marshall({
      subscription,           // Subscription ID (partition key)
      provider: providerDID,  // Provider DID (sort key)
      consumer: space,        // Space DID (indexed via GSI)
      customer,               // Customer account DID
      insertedAt: now,
    }),
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
