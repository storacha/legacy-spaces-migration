#!/usr/bin/env node

/**
 * Debug script to analyze egress traffic events from prod-w3infra-egress-traffic-events table
 * 
 * Usage:
 *   node debug-egress-traffic.js
 * 
 * Environment variables required:
 *   AWS_ACCESS_KEY_ID
 *   AWS_SECRET_ACCESS_KEY
 *   AWS_REGION (default: us-west-2)
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, ScanCommand, QueryCommand } from '@aws-sdk/lib-dynamodb'
import dotenv from 'dotenv'

dotenv.config()

const EGRESS_TABLE_NAME = 'prod-w3infra-egress-traffic-events'
const CUSTOMER_TABLE_NAME = 'prod-w3infra-customer'
const REGION = process.env.REGION || 'us-west-2'

// Initialize DynamoDB client
const dynamoClient = new DynamoDBClient({ region: REGION })
const docClient = DynamoDBDocumentClient.from(dynamoClient)

/**
 * Scan the egress traffic events table
 * @param {number} limit - Maximum number of items to scan (optional)
 */
async function scanEgressTraffic(limit = 10000) {
  console.log(`Scanning ${EGRESS_TABLE_NAME}...`)
  console.log(`Region: ${REGION}`)
  console.log(`Limit: ${limit} items\n`)

  const events = []
  let lastEvaluatedKey = undefined
  let scanCount = 0

  try {
    do {
      const command = new ScanCommand({
        TableName: EGRESS_TABLE_NAME,
        Limit: Math.min(1000, limit - events.length),
        ExclusiveStartKey: lastEvaluatedKey
      })

      const response = await docClient.send(command)
      
      if (response.Items) {
        events.push(...response.Items)
        scanCount++
        console.log(`Scanned ${events.length} items so far...`)
      }

      lastEvaluatedKey = response.LastEvaluatedKey

      // Stop if we've reached the limit
      if (events.length >= limit) {
        break
      }
    } while (lastEvaluatedKey)

    console.log(`\nTotal items scanned: ${events.length}`)
    console.log(`Scan operations: ${scanCount}\n`)

    return events
  } catch (error) {
    console.error('Error scanning table:', error)
    throw error
  }
}

/**
 * Get customer plan information from prod-w3infra-customer table
 * @param {string} customerDid - Customer DID
 * @returns {Promise<{product: string} | null>}
 */
async function getCustomerPlan(customerDid) {
  try {
    const command = new QueryCommand({
      TableName: CUSTOMER_TABLE_NAME,
      KeyConditionExpression: 'customer = :customer',
      ExpressionAttributeValues: {
        ':customer': customerDid
      },
      Limit: 1
    })

    const response = await docClient.send(command)
    
    if (response.Items && response.Items.length > 0) {
      return response.Items[0]
    }
    
    return null
  } catch (error) {
    console.error(`Error fetching customer plan for ${customerDid}:`, error.message)
    return null
  }
}

/**
 * Get plan details (free tier and overage rate)
 * @param {string} product - Product name from customer table
 * @returns {{name: string, freeTierGB: number, overageRate: number}}
 */
function getPlanDetails(product) {
  // Map product names to plan details
  // Support both old (web3.storage) and new (storacha.network) product DIDs
  const plans = {
    // Old web3.storage DIDs
    'did:web:starter.web3.storage': { name: 'Starter', freeTierGB: 5, overageRate: 0.15 },
    'did:web:lite.web3.storage': { name: 'Lite', freeTierGB: 100, overageRate: 0.05 },
    'did:web:business.web3.storage': { name: 'Business', freeTierGB: 2000, overageRate: 0.03 },
    // New storacha.network DIDs
    'did:web:starter.storacha.network': { name: 'Starter', freeTierGB: 5, overageRate: 0.15 },
    'did:web:lite.storacha.network': { name: 'Lite', freeTierGB: 100, overageRate: 0.05 },
    'did:web:business.storacha.network': { name: 'Business', freeTierGB: 2000, overageRate: 0.03 }
  }
  
  return plans[product] || { name: 'Unknown', freeTierGB: 0, overageRate: 0.15 }
}

/**
 * Analyze egress traffic events
 */
async function analyzeEgressTraffic(events) {
  console.log('# Egress Traffic Analysis')
  console.log()

  // Basic stats
  const totalEvents = events.length
  const totalBytes = events.reduce((sum, event) => sum + (event.bytes || 0), 0)
  const totalGB = (totalBytes / (1024 ** 3)).toFixed(2)

  console.log('## Basic Statistics')
  console.log()
  console.log(`- **Total Events:** ${totalEvents.toLocaleString()}`)
  console.log(`- **Total Bytes:** ${totalBytes.toLocaleString()} (${totalGB} GB)`)
  console.log(`- **Average Bytes per Event:** ${(totalBytes / totalEvents).toFixed(0).toLocaleString()}`)
  console.log()

  // Unique counts
  const uniqueSpaces = new Set(events.map(e => e.space).filter(Boolean))
  const uniqueCustomers = new Set(events.map(e => e.customer).filter(Boolean))
  const uniqueResources = new Set(events.map(e => e.resource).filter(Boolean))

  console.log('## Unique Entities')
  console.log()
  console.log(`- **Unique Spaces:** ${uniqueSpaces.size.toLocaleString()}`)
  console.log(`- **Unique Customers:** ${uniqueCustomers.size.toLocaleString()}`)
  console.log(`- **Unique Resources (CIDs):** ${uniqueResources.size.toLocaleString()}`)
  console.log()

  // Top spaces by traffic
  const spaceTraffic = new Map()
  events.forEach(event => {
    const space = event.space
    if (space) {
      const current = spaceTraffic.get(space) || { count: 0, bytes: 0 }
      current.count++
      current.bytes += (event.bytes || 0)
      spaceTraffic.set(space, current)
    }
  })

  console.log('## Top 10 Spaces by Traffic')
  console.log()
  const topSpaces = Array.from(spaceTraffic.entries())
    .sort((a, b) => b[1].bytes - a[1].bytes)
    .slice(0, 10)

  topSpaces.forEach(([space, stats], index) => {
    const gb = (stats.bytes / (1024 ** 3)).toFixed(2)
    const percentage = ((stats.bytes / totalBytes) * 100).toFixed(2)
    console.log(`${index + 1}. \`${space}\``)
    console.log(`   - Events: ${stats.count.toLocaleString()}`)
    console.log(`   - Bytes: ${stats.bytes.toLocaleString()} (${gb} GB, ${percentage}%)`)
  })
  console.log()

  // Top customers by traffic
  const customerTraffic = new Map()
  events.forEach(event => {
    const customer = event.customer
    if (customer) {
      const current = customerTraffic.get(customer) || { count: 0, bytes: 0 }
      current.count++
      current.bytes += (event.bytes || 0)
      customerTraffic.set(customer, current)
    }
  })

  console.log('## Top 10 Customers by Traffic')
  console.log()
  const topCustomers = Array.from(customerTraffic.entries())
    .sort((a, b) => b[1].bytes - a[1].bytes)
    .slice(0, 10)

  topCustomers.forEach(([customer, stats], index) => {
    const gb = (stats.bytes / (1024 ** 3)).toFixed(2)
    const percentage = ((stats.bytes / totalBytes) * 100).toFixed(2)
    console.log(`${index + 1}. \`${customer}\``)
    console.log(`   - Events: ${stats.count.toLocaleString()}`)
    console.log(`   - Bytes: ${stats.bytes.toLocaleString()} (${gb} GB, ${percentage}%)`)
  })
  console.log()

  // Top resources (CIDs) by traffic
  const resourceTraffic = new Map()
  events.forEach(event => {
    const resource = event.resource
    if (resource) {
      const current = resourceTraffic.get(resource) || { count: 0, bytes: 0 }
      current.count++
      current.bytes += (event.bytes || 0)
      resourceTraffic.set(resource, current)
    }
  })

  console.log('## Top 10 Resources (CIDs) by Traffic')
  console.log()
  const topResources = Array.from(resourceTraffic.entries())
    .sort((a, b) => b[1].bytes - a[1].bytes)
    .slice(0, 10)

  topResources.forEach(([resource, stats], index) => {
    const gb = (stats.bytes / (1024 ** 3)).toFixed(2)
    const percentage = ((stats.bytes / totalBytes) * 100).toFixed(2)
    console.log(`${index + 1}. \`${resource}\``)
    console.log(`   - Events: ${stats.count.toLocaleString()}`)
    console.log(`   - Bytes: ${stats.bytes.toLocaleString()} (${gb} GB, ${percentage}%)`)
  })
  console.log()

  // Time-based analysis
  const eventsByDate = new Map()
  events.forEach(event => {
    const servedAt = event.servedAt
    if (servedAt) {
      const date = servedAt.split('T')[0]
      const current = eventsByDate.get(date) || { count: 0, bytes: 0 }
      current.count++
      current.bytes += (event.bytes || 0)
      eventsByDate.set(date, current)
    }
  })

  console.log('## Traffic by Date')
  console.log()
  const sortedDates = Array.from(eventsByDate.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))

  sortedDates.forEach(([date, stats]) => {
    const gb = (stats.bytes / (1024 ** 3)).toFixed(2)
    console.log(`- **${date}:** ${stats.count.toLocaleString()} events, ${gb} GB egress`)
  })
  console.log()

  // Size distribution
  console.log('## Event Size Distribution')
  console.log()
  console.log('```')
  const sizeRanges = [
    { label: '< 1 KB', min: 0, max: 1024 },
    { label: '1 KB - 10 KB', min: 1024, max: 10 * 1024 },
    { label: '10 KB - 100 KB', min: 10 * 1024, max: 100 * 1024 },
    { label: '100 KB - 1 MB', min: 100 * 1024, max: 1024 * 1024 },
    { label: '1 MB - 10 MB', min: 1024 * 1024, max: 10 * 1024 * 1024 },
    { label: '10 MB - 100 MB', min: 10 * 1024 * 1024, max: 100 * 1024 * 1024 },
    { label: '> 100 MB', min: 100 * 1024 * 1024, max: Infinity }
  ]

  const sizeDistribution = sizeRanges.map(range => {
    const count = events.filter(e => {
      const bytes = e.bytes || 0
      return bytes >= range.min && bytes < range.max
    }).length
    const percentage = ((count / totalEvents) * 100).toFixed(2)
    return { label: range.label, count, percentage: parseFloat(percentage) }
  })

  // Find max percentage for scaling the bars
  const maxPercentage = Math.max(...sizeDistribution.map(d => d.percentage))
  const barWidth = 50 // Maximum bar width in characters

  sizeDistribution.forEach(({ label, count, percentage }) => {
    const barLength = Math.round((percentage / maxPercentage) * barWidth)
    const bar = '█'.repeat(barLength)
    const paddedLabel = label.padEnd(20)
    const paddedCount = count.toLocaleString().padStart(6)
    const paddedPercentage = `${percentage.toFixed(2)}%`.padStart(7)
    console.log(`${paddedLabel} ${bar} ${paddedCount} (${paddedPercentage})`)
  })
  console.log('```')
  console.log()

  // Revenue Analysis - fetch customer plans and calculate revenue
  console.log('## Revenue Analysis')
  console.log()
  
  // Fetch customer plans for top customers
  const customerPlans = new Map()
  for (const [customer] of topCustomers) {
    const planInfo = await getCustomerPlan(customer)
    if (planInfo) {
      customerPlans.set(customer, planInfo)
    }
  }

  let totalActualRevenue = 0
  const revenueByPlan = { Starter: 0, Lite: 0, Business: 0, Unknown: 0 }

  console.log('### Top 10 Customers Revenue')
  console.log()

  for (const [index, [customer, stats]] of topCustomers.entries()) {
    const gb = stats.bytes / (1024 ** 3)
    const percentage = ((stats.bytes / totalBytes) * 100).toFixed(2)
    
    // Get customer's actual plan
    const planInfo = customerPlans.get(customer)
    const planDetails = planInfo ? getPlanDetails(planInfo.product) : { name: 'Unknown', freeTierGB: 0, overageRate: 0.15 }
    
    // Calculate actual revenue based on plan
    const overageGB = Math.max(0, gb - planDetails.freeTierGB)
    const actualRevenue = overageGB * planDetails.overageRate
    totalActualRevenue += actualRevenue
    revenueByPlan[planDetails.name] += actualRevenue
    
    const withinFreeTier = gb < planDetails.freeTierGB
    const remainingFreeTier = Math.max(0, planDetails.freeTierGB - gb)
    
    console.log(`${index + 1}. \`${customer}\``)
    console.log(`   - Plan: **${planDetails.name}** (${planDetails.freeTierGB} GB free, $${planDetails.overageRate}/GB overage)`)
    console.log(`   - Traffic: ${gb.toFixed(2)} GB (${percentage}%)`)
    if (withinFreeTier) {
      console.log(`   - Status: ✅ **Within free tier** (${remainingFreeTier.toFixed(2)} GB remaining)`)
      console.log(`   - **Revenue: $0.00** (no overage)`)
    } else {
      console.log(`   - Overage: ${overageGB.toFixed(2)} GB`)
      console.log(`   - **Revenue: $${actualRevenue.toFixed(2)}**`)
    }
  }
  console.log()
  
  console.log('### Revenue Summary')
  console.log()
  console.log(`- **Total Revenue (Top 10 Customers):** $${totalActualRevenue.toFixed(2)}`)
  console.log()
  console.log('**Revenue by Plan:**')
  console.log()
  console.log(`- **Starter Plan:** $${revenueByPlan.Starter.toFixed(2)}`)
  console.log(`- **Lite Plan:** $${revenueByPlan.Lite.toFixed(2)}`)
  console.log(`- **Business Plan:** $${revenueByPlan.Business.toFixed(2)}`)
  if (revenueByPlan.Unknown > 0) {
    console.log(`- **Unknown Plan:** $${revenueByPlan.Unknown.toFixed(2)}`)
  }
  console.log()
}

// Main execution
async function main() {
  try {
    // Check for required environment variables
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      console.error('Error: AWS credentials not found in environment variables')
      console.error('Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set')
      process.exit(1)
    }

    // Get limit from command line args or use default
    const limit = parseInt(process.argv[2]) || 10000

    // Scan the table
    const events = await scanEgressTraffic(limit)

    if (events.length === 0) {
      console.log('No events found in the table')
      return
    }

    // Analyze the data
    analyzeEgressTraffic(events)
  } catch (error) {
    console.error('Fatal error:', error)
    process.exit(1)
  }
}

main()
