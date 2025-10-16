/**
 * Query Blob Registry Table
 */
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'
import { config } from '../config.js'

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
 * Query blob registry to get shard size
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
  
  const command = new QueryCommand({
    TableName: config.tables.blobRegistry,
    KeyConditionExpression: '#space = :space AND digest = :digest',
    ExpressionAttributeNames: {
      '#space': 'space',
    },
    ExpressionAttributeValues: {
      ':space': { S: space },
      ':digest': { S: digest },
    },
  })
  
  const response = await client.send(command)
  
  if (!response.Items || response.Items.length === 0) {
    throw new Error(`Shard ${shardCID} (digest: ${digest}) not found in blob registry for space ${space}`)
  }
  
  const blob = unmarshall(response.Items[0])
  return blob.size
}
