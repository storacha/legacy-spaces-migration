#!/usr/bin/env node
/**
 * Debug script to inspect upload records in DynamoDB
 */
import { QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb'
import { getDynamoClient } from '../src/lib/dynamo-client.js'
import { config } from '../src/config.js'

const rootCID = process.argv[2]

if (!rootCID) {
  console.error('Usage: node debug-upload.js <root-cid>')
  process.exit(1)
}

console.log(`Searching for all uploads with root CID: ${rootCID}`)
console.log()

const client = getDynamoClient()

// Query using the GSI to find all uploads with this root CID
const command = new QueryCommand({
  TableName: config.tables.upload,
  IndexName: 'cid',
  KeyConditionExpression: '#root = :root',
  ExpressionAttributeNames: {
    '#root': 'root',
  },
  ExpressionAttributeValues: {
    ':root': rootCID,
  },
})

const response = await client.send(command)

if (!response.Items || response.Items.length === 0) {
  console.log('❌ No uploads found with this root CID')
  process.exit(1)
}

console.log(`Found ${response.Items.length} upload(s) with this root CID:`)
console.log()

for (const upload of response.Items) {
  console.log('─'.repeat(70))
  console.log(`Space: ${upload.space}`)
  console.log(`Root:  ${upload.root}`)
  console.log(`Shards: ${upload.shards ? upload.shards.length : 0}`)
  if (upload.shards && upload.shards.length > 0) {
    console.log(`Shard CIDs:`)
    for (const shard of upload.shards) {
      console.log(`  - ${shard}`)
    }
  }
  console.log(`InsertedAt: ${upload.insertedAt}`)
  console.log(`UpdatedAt: ${upload.updatedAt || 'N/A'}`)
  console.log()
}
