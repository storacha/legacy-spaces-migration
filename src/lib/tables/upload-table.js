/**
 * Query Upload Table to get legacy uploads
 */
import { ScanCommand, QueryCommand } from '@aws-sdk/lib-dynamodb'
import { config } from '../../config.js'
import { getDynamoClient } from '../dynamo-client.js'

/**
 * Get uploads for a specific space from the Upload Table
 * 
 * @param {object} options
 * @param {number} [options.limit] - Maximum number of uploads to return (default: Infinity)
 * @param {string} options.space - Space DID to get uploads for
 * @returns {AsyncGenerator<{space: string, root: string, shards: string[], insertedAt: string, updatedAt: string}>}
 */
export async function* getUploadsForSpace({ limit = Infinity, space }) {
  const client = getDynamoClient()
  let count = 0
  /** @type {Record<string, any> | undefined} */
  let lastEvaluatedKey
  
  while (count < limit) {
    /** @type {QueryCommand | ScanCommand} */
    const command = space
      ? new QueryCommand({
          TableName: config.tables.upload,
          KeyConditionExpression: '#space = :space',
          ExpressionAttributeNames: {
            '#space': 'space',
          },
          ExpressionAttributeValues: {
            ':space': space,
          },
          Limit: Math.min(100, limit - count),
          ExclusiveStartKey: lastEvaluatedKey,
        })
      : new ScanCommand({
          TableName: config.tables.upload,
          Limit: Math.min(100, limit - count),
          ExclusiveStartKey: lastEvaluatedKey,
        })
    
    /** @type {import('@aws-sdk/lib-dynamodb').QueryCommandOutput | import('@aws-sdk/lib-dynamodb').ScanCommandOutput} */
    const response = await client.send(command)
    if (!response.Items || response.Items.length === 0) {
      break
    }
    
    for (const upload of response.Items) {
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
 * @returns {Promise<{
 *   space: string,
 *   root: string,
 *   shards: string[],
 *   insertedAt: string,
 *   updatedAt: string
 * } | null>}
 */
export async function getUpload(space, root) {
  const client = getDynamoClient()
  
  const command = new QueryCommand({
    TableName: config.tables.upload,
    KeyConditionExpression: '#space = :space AND #root = :root',
    ExpressionAttributeNames: {
      '#space': 'space',
      '#root': 'root',
    },
    ExpressionAttributeValues: {
      ':space': space,
      ':root': root,
    },
  })
  
  const response = await client.send(command)
  
  if (!response.Items || response.Items.length === 0) {
    return null
  }
  
  const upload = response.Items[0]
  return {
    space: upload.space,
    root: upload.root,
    shards: upload.shards ? Array.from(upload.shards) : [],
    insertedAt: upload.insertedAt,
    updatedAt: upload.updatedAt,
  }
}

/**
 * Get upload by root CID using the global secondary index
 * 
 * @param {string} root - Root CID
 * @returns {Promise<{
 *   space: string,
 *   root: string,
 *   shards: string[],
 *   insertedAt: string,
 *   updatedAt: string
 * } | null>}
 */
export async function getUploadByRoot(root) {
  const client = getDynamoClient()
  
  const command = new QueryCommand({
    TableName: config.tables.upload,
    IndexName: 'cid',
    KeyConditionExpression: '#root = :root',
    ExpressionAttributeNames: {
      '#root': 'root',
    },
    ExpressionAttributeValues: {
      ':root': root,
    },
    Limit: 1,
  })
  
  const response = await client.send(command)
  
  if (!response.Items || response.Items.length === 0) {
    return null
  }
  
  const upload = response.Items[0]
  return {
    space: upload.space,
    root: upload.root,
    shards: upload.shards ? Array.from(upload.shards) : [],
    insertedAt: upload.insertedAt,
    updatedAt: upload.updatedAt,
  }
}

/**
 * Count uploads for a specific space
 * 
 * @param {string} space - Space DID
 * @returns {Promise<number>} Upload count (0 if space is empty)
 */
export async function countUploadsForSpace(space) {
  const client = getDynamoClient()
  
  // First query to check if space has any uploads
  const command = new QueryCommand({
    TableName: config.tables.upload,
    KeyConditionExpression: '#space = :space',
    ExpressionAttributeNames: {
      '#space': 'space',
    },
    ExpressionAttributeValues: {
      ':space': space,
    },
    Select: 'COUNT',
  })
  
  let count = 0
  let lastEvaluatedKey
  
  do {
    // If we have a last evaluated key, add it to the command
    if (lastEvaluatedKey) {
      command.input.ExclusiveStartKey = lastEvaluatedKey
    }
    
    const response = await client.send(command)
    count += response.Count || 0
    lastEvaluatedKey = response.LastEvaluatedKey
    
  } while (lastEvaluatedKey)
  
  return count
}
