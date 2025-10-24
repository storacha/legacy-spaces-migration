/**
 * Query shard data by looking up the size in the allocations table,
 * then the store table if not found.
 * The Blob Registry was meant to replace allocations but migration never completed.
 * We are still writing to the blob-registry and allocations tables.
 * So we can query either table to get the size, but the majority of the data is in allocations.
 */
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
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
 * Query allocations table to get shard size
 * Falls back to store table if not found
 * 
 * Query order:
 * 1. allocations (current, still being written to)
 * 2. store (legacy)
 * 
 * @param {string} space - Space DID
 * @param {string} shardCID - Shard CID (CAR CID)
 * @returns {Promise<number>} - Blob size in bytes
 */
export async function getShardSize(space, shardCID) {
  const client = createDynamoClient()
  
  // Parse the CAR CID to get its multihash
  const cid = CID.parse(shardCID)
  const digest = base58btc.encode(cid.multihash.bytes)
  
  // Query allocations table (current, still being written to)
  // blob-registry was meant to replace allocations but migration never completed
  // In allocations table, the key is "multihash" (base58btc encoded)
  const allocationsCommand = new QueryCommand({
    TableName: config.tables.allocations,
    KeyConditionExpression: '#space = :space AND multihash = :multihash',
    ExpressionAttributeNames: {
      '#space': 'space',
    },
    ExpressionAttributeValues: {
      ':space': { S: space },
      ':multihash': { S: digest },
    },
  })
  
  const allocationsResponse = await client.send(allocationsCommand)
  
  if (allocationsResponse.Items && allocationsResponse.Items.length > 0) {
    const blob = unmarshall(allocationsResponse.Items[0])
    return parseInt(blob.size, 10)
  }
  
  // Fall back to store table (legacy table)
  // In the store table, the key is "link" (the CID string) instead of "digest"
  const storeCommand = new QueryCommand({
    TableName: config.tables.store,
    KeyConditionExpression: '#space = :space AND link = :link',
    ExpressionAttributeNames: {
      '#space': 'space',
    },
    ExpressionAttributeValues: {
      ':space': { S: space },
      ':link': { S: shardCID },
    },
  })
  
  const storeResponse = await client.send(storeCommand)
  if (storeResponse.Items && storeResponse.Items.length > 0) {
    const blob = unmarshall(storeResponse.Items[0])
    return parseInt(blob.size, 10)
  }
  
  throw new Error(`Shard ${shardCID} not found in allocations or store table for space ${space}`)
}
