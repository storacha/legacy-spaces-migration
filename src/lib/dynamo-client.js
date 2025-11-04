/**
 * Centralized DynamoDB Document Client
 * 
 * Provides a cached DynamoDB Document Client instance that can be reused
 * across all table operations to avoid creating multiple connections.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { config } from '../config.js'

/**
 * Cached DynamoDB Document Client instance
 */
let cachedClient = null

/**
 * Get or create DynamoDB Document Client
 * 
 * Returns a cached instance to avoid creating multiple connections.
 * The Document Client automatically handles marshalling/unmarshalling
 * between JavaScript types and DynamoDB types.
 * 
 * @returns {DynamoDBDocumentClient}
 */
export function getDynamoClient() {
  if (!cachedClient) {
    const baseClient = new DynamoDBClient({
      region: config.aws.region,
      credentials: config.aws.accessKeyId ? {
        accessKeyId: config.aws.accessKeyId,
        secretAccessKey: config.aws.secretAccessKey,
      } : undefined,
    })
    
    cachedClient = DynamoDBDocumentClient.from(baseClient)
  }
  
  return cachedClient
}

/**
 * Reset the cached client (useful for testing)
 */
export function resetClient() {
  cachedClient = null
}
