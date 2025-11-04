/**
 * Query shard data by looking up the size in the allocations table,
 * then the store table if not found.
 * The Blob Registry was meant to replace allocations but migration never completed.
 * We are still writing to the blob-registry and allocations tables.
 * So we can query either table to get the size, but the majority of the data is in allocations.
 */
import { QueryCommand } from '@aws-sdk/lib-dynamodb'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

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
  const client = getDynamoClient()
  
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
      ':space': space,
      ':multihash': digest,
    },
  })
  
  const allocationsResponse = await client.send(allocationsCommand)
  
  if (allocationsResponse.Items && allocationsResponse.Items.length > 0) {
    const blob = allocationsResponse.Items[0]
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
      ':space': space,
      ':link': shardCID,
    },
  })
  
  const storeResponse = await client.send(storeCommand)
  if (storeResponse.Items && storeResponse.Items.length > 0) {
    const blob = storeResponse.Items[0]
    return parseInt(blob.size, 10)
  }
  
  throw new Error(`Shard ${shardCID} not found in allocations or store table for space ${space}`)
}
