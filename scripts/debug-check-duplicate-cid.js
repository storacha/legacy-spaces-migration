import dotenv from 'dotenv'
dotenv.config()
import { QueryCommand } from '@aws-sdk/lib-dynamodb'
import { config, validateConfig } from '../src/config.js'
import { getDynamoClient } from '../src/lib/dynamo-client.js'

validateConfig()

const client = getDynamoClient()
const cid = 'bafkreia46bon7uw6pbagjaffr5xev6ycaltew4o52hwza4y7tz4nxzyq4q'

const command = new QueryCommand({
  TableName: config.tables.upload,
  IndexName: 'cid',
  KeyConditionExpression: '#root = :root',
  ExpressionAttributeNames: {
    '#root': 'root',
  },
  ExpressionAttributeValues: {
    ':root': cid,
  },
})

const response = await client.send(command)

console.log(`Found ${response.Items.length} uploads with root CID: ${cid}`)
console.log()

for (const item of response.Items) {
  console.log(`Space: ${item.space}`)
  console.log(`Root: ${item.root}`)
  console.log(`Shards: ${item.shards?.length || 0}`)
  console.log(`Shards: ${JSON.stringify(item.shards || [])}`)
  console.log(`InsertedAt: ${item.insertedAt}`)
  console.log()
}
