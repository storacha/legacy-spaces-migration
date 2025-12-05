#!/usr/bin/env node

/**
 * Debug script to check for consumers mapped to multiple providers
 * 
 * This script queries the consumer table to find any space DIDs that have
 * records with both did:web:web3.storage and did:web:up.storacha.network providers.
 * 
 * Such duplicates could cause issues with the fallback query logic.
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb'
import dotenv from 'dotenv'
dotenv.config()

const REGION = 'us-west-2'
const TABLE_NAME = 'prod-w3infra-consumer'

const LEGACY_PROVIDER = 'did:web:web3.storage'
const NEW_PROVIDER = 'did:web:up.storacha.network'

async function main() {
  console.log('🔍 Checking for consumers with multiple providers...\n')
  console.log(`Table: ${TABLE_NAME}`)
  console.log(`Region: ${REGION}\n`)

  const client = new DynamoDBClient({ region: REGION })
  const docClient = DynamoDBDocumentClient.from(client)

  // Map to track consumers and their providers
  // consumer DID -> Set of provider DIDs
  const consumerProviders = new Map()

  let scannedCount = 0
  let lastEvaluatedKey = undefined
  let hasMore = true

  console.log('Scanning consumer table...')

  while (hasMore) {
    const command = new ScanCommand({
      TableName: TABLE_NAME,
      ProjectionExpression: 'consumer, provider',
      ExclusiveStartKey: lastEvaluatedKey,
      Limit: 1000
    })

    const response = await docClient.send(command)
    
    if (response.Items) {
      scannedCount += response.Items.length
      
      for (const item of response.Items) {
        const consumer = item.consumer
        const provider = item.provider

        if (!consumerProviders.has(consumer)) {
          consumerProviders.set(consumer, new Set())
        }
        consumerProviders.get(consumer).add(provider)
      }

      process.stdout.write(`\rScanned ${scannedCount} records...`)
    }

    lastEvaluatedKey = response.LastEvaluatedKey
    hasMore = !!lastEvaluatedKey
  }

  console.log('\n\n📊 Analysis Results:\n')

  // Count providers
  const providerCounts = new Map()
  
  // Find consumers with multiple providers
  const duplicates = []
  const legacyOnly = []
  const newOnly = []
  const otherProviders = []

  for (const [consumer, providers] of consumerProviders.entries()) {
    if (providers.size > 1) {
      duplicates.push({ consumer, providers: Array.from(providers) })
    } else {
      const provider = Array.from(providers)[0]
      
      // Count this provider
      providerCounts.set(provider, (providerCounts.get(provider) || 0) + 1)
      
      if (provider === LEGACY_PROVIDER) {
        legacyOnly.push(consumer)
      } else if (provider === NEW_PROVIDER) {
        newOnly.push(consumer)
      } else {
        otherProviders.push({ consumer, provider })
      }
    }
  }

  console.log(`Total unique consumers: ${consumerProviders.size}`)
  console.log(`Total records scanned: ${scannedCount}\n`)

  // Show distinct providers with counts and percentages
  console.log('Distinct Providers:')
  const sortedProviders = Array.from(providerCounts.entries())
    .sort((a, b) => b[1] - a[1]) // Sort by count descending
  
  for (const [provider, count] of sortedProviders) {
    const percentage = ((count / consumerProviders.size) * 100).toFixed(2)
    console.log(`  - ${provider}`)
    console.log(`    Count: ${count.toLocaleString()} consumers (${percentage}%)`)
  }
  console.log()

  console.log('Provider Distribution:')
  console.log(`  - Legacy only (${LEGACY_PROVIDER}): ${legacyOnly.length}`)
  console.log(`  - New only (${NEW_PROVIDER}): ${newOnly.length}`)
  console.log(`  - Other providers: ${otherProviders.length}`)
  console.log(`  - Multiple providers (DUPLICATES): ${duplicates.length}\n`)

  if (duplicates.length > 0) {
    console.log('⚠️  WARNING: Found consumers with multiple providers!\n')
    console.log('This could cause issues with the fallback query logic.')
    console.log('The same space would be found with both providers.\n')
    
    console.log('Duplicate consumers (showing first 10):')
    for (const dup of duplicates.slice(0, 10)) {
      console.log(`\n  Consumer: ${dup.consumer}`)
      console.log(`  Providers:`)
      for (const provider of dup.providers) {
        console.log(`    - ${provider}`)
      }
    }

    if (duplicates.length > 10) {
      console.log(`\n  ... and ${duplicates.length - 10} more`)
    }
  } else {
    console.log('✅ No duplicates found! Each consumer has exactly one provider.')
    console.log('   The fallback query is safe to implement.')
  }

  if (otherProviders.length > 0) {
    console.log('\n⚠️  Found consumers with unexpected providers:')
    for (const item of otherProviders.slice(0, 5)) {
      console.log(`  - ${item.consumer} -> ${item.provider}`)
    }
    if (otherProviders.length > 5) {
      console.log(`  ... and ${otherProviders.length - 5} more`)
    }
  }

  console.log('\n📈 Summary:')
  console.log(`  - Fallback query safety: ${duplicates.length === 0 ? '✅ SAFE' : '❌ UNSAFE'}`)
  console.log(`  - Legacy spaces that will use fallback: ${legacyOnly.length}`)
  console.log(`  - New spaces (no fallback needed): ${newOnly.length}`)
  
  const fallbackPercentage = ((legacyOnly.length / consumerProviders.size) * 100).toFixed(2)
  console.log(`  - Fallback usage rate: ${fallbackPercentage}%`)

  console.log('\n💡 Recommendations:')
  if (duplicates.length === 0) {
    console.log('  ✅ Safe to deploy fallback query logic')
    console.log(`  ⚠️  ${legacyOnly.length} legacy spaces will trigger fallback (extra query)`)
    if (legacyOnly.length > 1000) {
      console.log('  💡 Consider migrating legacy spaces to reduce fallback queries')
    }
  } else {
    console.log('  ❌ DO NOT deploy fallback logic yet')
    console.log('  🔧 Need to resolve duplicate provider mappings first')
    console.log('  💡 Investigate why spaces have multiple providers')
  }
}

main().catch(err => {
  console.error('Error:', err)
  process.exit(1)
})
