/**
 * Query Upload Table to get legacy uploads
 */
import { DynamoDBClient, ScanCommand, QueryCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
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
 * Sample uploads from the Upload Table
 * 
 * @param {object} options
 * @param {number} options.limit - Maximum number of uploads to return
 * @param {string} [options.space] - Filter by specific space DID
 * @returns {AsyncGenerator<{space: string, root: string, shards: string[], insertedAt: string}>}
 */
export async function* sampleUploads({ limit, space }) {
  const client = createDynamoClient()
  let count = 0
  let lastEvaluatedKey
  
  while (count < limit) {
    const command = space
      ? new QueryCommand({
          TableName: config.tables.upload,
          KeyConditionExpression: '#space = :space',
          ExpressionAttributeNames: {
            '#space': 'space',
          },
          ExpressionAttributeValues: {
            ':space': { S: space },
          },
          Limit: Math.min(100, limit - count),
          ExclusiveStartKey: lastEvaluatedKey,
        })
      : new ScanCommand({
          TableName: config.tables.upload,
          Limit: Math.min(100, limit - count),
          ExclusiveStartKey: lastEvaluatedKey,
        })
    
    const response = await client.send(command)
    
    if (!response.Items || response.Items.length === 0) {
      break
    }
    
    for (const item of response.Items) {
      const upload = unmarshall(item)
      yield {
        space: upload.space,
        root: upload.root,
        shards: upload.shards ? Array.from(upload.shards) : [],
        insertedAt: upload.insertedAt,
        updatedAt: upload.updatedAt,
      }
      count++
      
      if (count >= limit) {
        break
      }
    }
    
    lastEvaluatedKey = response.LastEvaluatedKey
    if (!lastEvaluatedKey) {
      break
    }
  }
}

/**
 * Get a specific upload by space and root
 * 
 * @param {string} space - Space DID
 * @param {string} root - Root CID
 * @returns {Promise<{space: string, root: string, shards: string[]} | null>}
 */
export async function getUpload(space, root) {
  const client = createDynamoClient()
  
  const command = new QueryCommand({
    TableName: config.tables.upload,
    KeyConditionExpression: '#space = :space AND #root = :root',
    ExpressionAttributeNames: {
      '#space': 'space',
      '#root': 'root',
    },
    ExpressionAttributeValues: {
      ':space': { S: space },
      ':root': { S: root },
    },
  })
  
  const response = await client.send(command)
  
  if (!response.Items || response.Items.length === 0) {
    return null
  }
  
  const upload = unmarshall(response.Items[0])
  return {
    space: upload.space,
    root: upload.root,
    shards: upload.shards ? Array.from(upload.shards) : [],
    insertedAt: upload.insertedAt,
  }
}

/**
 * Get upload by root CID using the global secondary index
 * 
 * @param {string} root - Root CID
 * @returns {Promise<{space: string, root: string, shards: string[]} | null>}
 */
export async function getUploadByRoot(root) {
  const client = createDynamoClient()
  
  const command = new QueryCommand({
    TableName: config.tables.upload,
    IndexName: 'cid',
    KeyConditionExpression: '#root = :root',
    ExpressionAttributeNames: {
      '#root': 'root',
    },
    ExpressionAttributeValues: {
      ':root': { S: root },
    },
    Limit: 1,
  })
  
  const response = await client.send(command)
  
  if (!response.Items || response.Items.length === 0) {
    return null
  }
  
  const upload = unmarshall(response.Items[0])
  return {
    space: upload.space,
    root: upload.root,
    shards: upload.shards ? Array.from(upload.shards) : [],
    insertedAt: upload.insertedAt,
  }
}
