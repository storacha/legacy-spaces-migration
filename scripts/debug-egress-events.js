import 'dotenv/config'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb'

/**
 * Query egress traffic events by space or customer DID
 * 
 * Usage:
 *   STORACHA_ENV=staging node scripts/query-egress-events.js <space-or-customer-did>
 *   STORACHA_ENV=production node scripts/query-egress-events.js <space-or-customer-did>
 */

const TABLES = {
  staging: 'staging-w3infra-egress-traffic-events',
  production: 'prod-w3infra-egress-traffic-events'
}

const env = process.env.STORACHA_ENV || 'staging'
const tableName = TABLES[env]

if (!tableName) {
  console.error(`Invalid environment: ${env}. Use 'staging' or 'production'`)
  process.exit(1)
}

const did = process.argv[2]
if (!did) {
  console.error('Usage: node scripts/query-egress-events.js <space-or-customer-did>')
  console.error('Example: node scripts/query-egress-events.js did:key:z6Mkk...')
  process.exit(1)
}

console.log(`=== Querying Egress Events ===`)
console.log(`Environment: ${env}`)
console.log(`Table: ${tableName}`)
console.log(`DID: ${did}`)
console.log()

// Determine region based on environment
const REGIONS = {
  staging: 'us-east-2',
  production: 'us-west-2'
}

const region = process.env.AWS_REGION || REGIONS[env]

const dynamoClient = new DynamoDBClient({ region })

const docClient = DynamoDBDocumentClient.from(dynamoClient)

/**
 * Query by space DID (partition key pk starts with space DID)
 */
async function queryBySpace(spaceDid) {
  console.log(`Querying by space: ${spaceDid}`)
  
  let allItems = []
  let lastEvaluatedKey = undefined
  
  do {
    const params = {
      TableName: tableName,
      KeyConditionExpression: 'begins_with(pk, :space)',
      ExpressionAttributeValues: {
        ':space': spaceDid
      },
      ExclusiveStartKey: lastEvaluatedKey
    }
    
    const result = await docClient.send(new QueryCommand(params))
    allItems = allItems.concat(result.Items || [])
    lastEvaluatedKey = result.LastEvaluatedKey
    
    console.log(`  Fetched ${result.Items?.length || 0} items (total: ${allItems.length})`)
  } while (lastEvaluatedKey)
  
  return allItems
}

/**
 * Query by customer DID using GSI
 */
async function queryByCustomer(customerDid) {
  console.log(`Querying by customer: ${customerDid}`)
  
  let allItems = []
  let lastEvaluatedKey = undefined
  
  do {
    const params = {
      TableName: tableName,
      IndexName: 'customer',
      KeyConditionExpression: 'customer = :customer',
      ExpressionAttributeValues: {
        ':customer': customerDid
      },
      ExclusiveStartKey: lastEvaluatedKey
    }
    
    const result = await docClient.send(new QueryCommand(params))
    allItems = allItems.concat(result.Items || [])
    lastEvaluatedKey = result.LastEvaluatedKey
    
    console.log(`  Fetched ${result.Items?.length || 0} items (total: ${allItems.length})`)
  } while (lastEvaluatedKey)
  
  return allItems
}

/**
 * Format bytes to human-readable size
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
}

/**
 * Format timestamp (handles both ISO strings and numbers)
 */
function formatTimestamp(timestamp) {
  if (typeof timestamp === 'string') {
    return new Date(timestamp).toISOString()
  }
  return new Date(timestamp).toISOString()
}

/**
 * Display events
 */
function displayEvents(events) {
  if (events.length === 0) {
    console.log('\nNo events found.')
    return
  }
  
  console.log(`\n=== Found ${events.length} Events ===\n`)
  
  // Sort by servedAt timestamp (most recent first)
  events.sort((a, b) => {
    const timeA = new Date(a.servedAt).getTime()
    const timeB = new Date(b.servedAt).getTime()
    return timeB - timeA
  })
  
  // Calculate totals
  const totalBytes = events.reduce((sum, e) => sum + (e.bytes || 0), 0)
  
  console.log(`Total Bytes: ${formatBytes(totalBytes)}`)
  console.log()
  
  // Group by space
  const bySpace = {}
  events.forEach(event => {
    if (!bySpace[event.space]) {
      bySpace[event.space] = []
    }
    bySpace[event.space].push(event)
  })
  
  console.log(`Spaces: ${Object.keys(bySpace).length}`)
  console.log()
  
  // Display events
  Object.entries(bySpace).forEach(([space, spaceEvents]) => {
    console.log(`\n## Space: ${space}`)
    console.log(`   Customer: ${spaceEvents[0].customer || 'N/A'}`)
    console.log(`   Events: ${spaceEvents.length}`)
    
    const spaceBytes = spaceEvents.reduce((sum, e) => sum + (e.bytes || 0), 0)
    console.log(`   Total Bytes: ${formatBytes(spaceBytes)}`)
    console.log()
    
    // Show recent events (limit to 10 per space)
    const recentEvents = spaceEvents.slice(0, 10)
    recentEvents.forEach((event, i) => {
      console.log(`   ${i + 1}. ${formatTimestamp(event.servedAt)}`)
      console.log(`      Resource: ${event.resource}`)
      console.log(`      Bytes: ${formatBytes(event.bytes || 0)}`)
      if (event.cause) {
        console.log(`      Cause: ${event.cause}`)
      }
    })
    
    if (spaceEvents.length > 10) {
      console.log(`   ... and ${spaceEvents.length - 10} more events`)
    }
  })
}

// Main execution
try {
  let events
  
  // Try querying by space first
  try {
    events = await queryBySpace(did)
  } catch (error) {
    console.log(`  Not found as space, trying customer...`)
    events = await queryByCustomer(did)
  }
  
  displayEvents(events)
} catch (error) {
  console.error('Error querying egress events:', error.message)
  console.error(error)
  process.exit(1)
}
