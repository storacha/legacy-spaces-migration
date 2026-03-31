#!/usr/bin/env node

/**
 * OpenSea Data Deletion Script
 * 
 * Discovers, verifies, and deletes OpenSea data from Storacha storage.
 * Uses streaming approach to handle 250TB+ of data without loading all into memory.
 * 
 * Usage:
 *   # Discovery only (default - safe, read-only, counts records and sizes)
 *   STORACHA_ENV=production node scripts/opensea-deletion.js
 * 
 *   # With verification (checks R2 existence as it discovers)
 *   STORACHA_ENV=production node scripts/opensea-deletion.js --verify
 * 
 *   # Dry-run deletion (shows what would be deleted, limited to 100 records)
 *   STORACHA_ENV=production node scripts/opensea-deletion.js --delete
 * 
 *   # Dry-run deletion with custom limit
 *   STORACHA_ENV=production node scripts/opensea-deletion.js --delete --limit 500
 * 
 *   # Dry-run deletion with no limit (all records)
 *   STORACHA_ENV=production node scripts/opensea-deletion.js --delete --limit 0
 * 
 *   # ACTUAL deletion (requires explicit flag)
 *   STORACHA_ENV=production node scripts/opensea-deletion.js --delete --execute
 * 
 *   # Resume from a specific space (if interrupted)
 *   STORACHA_ENV=production node scripts/opensea-deletion.js --delete --execute --resume-from "did:key:z6Mk..."
 */

import { QueryCommand, DeleteCommand } from '@aws-sdk/lib-dynamodb'
import { S3Client, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { parseArgs } from 'node:util'
import { writeFileSync, readFileSync, existsSync } from 'node:fs'
import { config } from '../src/config.js'
import { getDynamoClient } from '../src/lib/dynamo-client.js'
import { getSpacesForCustomer } from '../src/lib/tables/consumer-table.js'
import { CID } from 'multiformats/cid'
import { base58btc } from 'multiformats/bases/base58'

// OpenSea customer DID
const OPENSEA_CUSTOMER = 'did:mailto:opensea.io:sylvia.hoang'

// Default limit for dry-run deletion
const DEFAULT_DELETE_LIMIT = 100

// Progress file for resumability
const PROGRESS_FILE = 'opensea-deletion-progress.json'

// Parse command line arguments
const { values: args } = parseArgs({
  options: {
    verify: {
      type: 'boolean',
      short: 'v',
      default: false,
      description: 'Verify blobs exist in R2'
    },
    delete: {
      type: 'boolean',
      short: 'd',
      default: false,
      description: 'Run deletion phase (dry-run by default)'
    },
    execute: {
      type: 'boolean',
      short: 'e',
      default: false,
      description: 'Actually execute deletions (requires --delete)'
    },
    limit: {
      type: 'string',
      short: 'l',
      default: String(DEFAULT_DELETE_LIMIT),
      description: 'Limit number of records to process (0 = no limit)'
    },
    'resume-from': {
      type: 'string',
      default: '',
      description: 'Resume from a specific space DID'
    },
    concurrency: {
      type: 'string',
      short: 'c',
      default: '20',
      description: 'Number of concurrent R2 operations'
    }
  }
})

const dryRun = !args.execute
// In discovery mode (no --verify, no --delete), default to no limit
const isDiscoveryOnly = !args.verify && !args.delete
const processLimit = isDiscoveryOnly && args.limit === String(DEFAULT_DELETE_LIMIT) ? 0 : parseInt(args.limit, 10)
const resumeFrom = args['resume-from']
const concurrency = parseInt(args.concurrency, 10)

/**
 * @typedef {Object} BlobRecord
 * @property {string} space - Space DID
 * @property {string} multihash - Base58btc encoded multihash (for blob-registry/allocations)
 * @property {string} [link] - CID string (for store table)
 * @property {number} size - Size in bytes
 * @property {string} table - Source table name
 * @property {string} insertedAt - Timestamp
 */

/**
 * Stream records from allocations table for a space
 * @param {import('@aws-sdk/lib-dynamodb').DynamoDBDocumentClient} client
 * @param {string} space
 * @param {function} onRecords - Callback to process each page of records
 * @param {function} [onProgress] - Optional progress callback
 * @returns {Promise<{count: number, size: number}>}
 */
async function streamAllocations(client, space, onRecords, onProgress, limit = 0) {
  let lastEvaluatedKey
  let pages = 0
  let totalCount = 0
  let totalSize = 0
  let stopped = false

  do {
    const command = new QueryCommand({
      TableName: config.tables.allocations,
      KeyConditionExpression: '#space = :space',
      ExpressionAttributeNames: { '#space': 'space' },
      ExpressionAttributeValues: { ':space': space },
      ExclusiveStartKey: lastEvaluatedKey
    })

    const response = await client.send(command)
    pages++
    
    if (response.Items && response.Items.length > 0) {
      let records = response.Items.map(item => ({
        space: item.space,
        multihash: item.multihash,
        size: item.size || 0,
        table: 'allocations',
        insertedAt: item.insertedAt || 'unknown'
      }))
      
      // Apply limit if specified
      if (limit > 0 && totalCount + records.length > limit) {
        records = records.slice(0, limit - totalCount)
        stopped = true
      }
      
      for (const r of records) {
        totalSize += r.size
      }
      totalCount += records.length
      
      await onRecords(records)
    }

    if (onProgress) onProgress('allocations', totalCount, pages)
    lastEvaluatedKey = response.LastEvaluatedKey
  } while (lastEvaluatedKey && !stopped)

  return { count: totalCount, size: totalSize }
}

/**
 * Stream records from store table for a space (legacy CAR files)
 * @param {import('@aws-sdk/lib-dynamodb').DynamoDBDocumentClient} client
 * @param {string} space
 * @param {function} onRecords - Callback to process each page of records
 * @param {function} [onProgress] - Optional progress callback
 * @returns {Promise<{count: number, size: number}>}
 */
async function streamStore(client, space, onRecords, onProgress, limit = 0) {
  let lastEvaluatedKey
  let pages = 0
  let totalCount = 0
  let totalSize = 0
  let stopped = false

  do {
    const command = new QueryCommand({
      TableName: config.tables.store,
      KeyConditionExpression: '#space = :space',
      ExpressionAttributeNames: { '#space': 'space' },
      ExpressionAttributeValues: { ':space': space },
      ExclusiveStartKey: lastEvaluatedKey
    })

    const response = await client.send(command)
    pages++
    
    if (response.Items && response.Items.length > 0) {
      let records = response.Items.map(item => ({
        space: item.space,
        link: item.link,
        multihash: item.link ? cidToMultihash(item.link) : undefined,
        size: item.size || 0,
        table: 'store',
        insertedAt: item.insertedAt || 'unknown'
      }))
      
      // Apply limit if specified
      if (limit > 0 && totalCount + records.length > limit) {
        records = records.slice(0, limit - totalCount)
        stopped = true
      }
      
      for (const r of records) {
        totalSize += r.size
      }
      totalCount += records.length
      
      await onRecords(records)
    }

    if (onProgress) onProgress('store', totalCount, pages)
    lastEvaluatedKey = response.LastEvaluatedKey
  } while (lastEvaluatedKey && !stopped)

  return { count: totalCount, size: totalSize }
}

/**
 * Convert CID string to base58btc multihash
 * @param {string} cidStr
 * @returns {string|undefined}
 */
function cidToMultihash(cidStr) {
  try {
    const cid = CID.parse(cidStr)
    return base58btc.encode(cid.multihash.bytes)
  } catch {
    return undefined
  }
}

/**
 * Get R2 URL for a blob
 * @param {BlobRecord} record
 * @returns {string}
 */
function getR2Url(record) {
  if (record.table === 'store' && record.link) {
    // Store protocol: {cid}/{cid}.car
    return `${config.storage.carparkPublicUrl}/${record.link}/${record.link}.car`
  } else if (record.multihash) {
    // Blob protocol: {multihash}/{multihash}.blob
    return `${config.storage.carparkPublicUrl}/${record.multihash}/${record.multihash}.blob`
  }
  return ''
}

/**
 * Get R2 key for a blob (for deletion)
 * @param {BlobRecord} record
 * @returns {string}
 */
function getR2Key(record) {
  if (record.table === 'store' && record.link) {
    return `${record.link}/${record.link}.car`
  } else if (record.multihash) {
    return `${record.multihash}/${record.multihash}.blob`
  }
  return ''
}


/**
 * Format bytes to human readable
 * @param {number} bytes
 * @returns {string}
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
}


/**
 * Save progress to file for resumability
 * @param {object} progress
 */
function saveProgress(progress) {
  writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2))
}

/**
 * Load progress from file
 * @returns {object|null}
 */
function loadProgress() {
  if (existsSync(PROGRESS_FILE)) {
    try {
      return JSON.parse(readFileSync(PROGRESS_FILE, 'utf-8'))
    } catch {
      return null
    }
  }
  return null
}

/**
 * Process a batch of records (verify and/or delete)
 * @param {BlobRecord[]} records
 * @param {object} ctx - Processing context with clients and stats
 * @param {function} [onProgress] - Optional progress callback
 * @returns {Promise<void>}
 */
async function processBatch(records, ctx, onProgress) {
  const chunks = []
  for (let i = 0; i < records.length; i += ctx.concurrency) {
    chunks.push(records.slice(i, i + ctx.concurrency))
  }

  let chunksProcessed = 0
  for (const chunk of chunks) {
    await Promise.all(chunk.map(async (record) => {
      // Check if we've hit the limit
      if (ctx.processLimit > 0 && ctx.stats.processed >= ctx.processLimit) {
        ctx.stats.skipped++
        return
      }

      const url = getR2Url(record)
      const r2Key = getR2Key(record)
      const cid = record.link || record.multihash

      // Verify if requested
      if (ctx.verify && url) {
        try {
          const res = await fetch(url, { method: 'HEAD' })
          if (res.ok) {
            ctx.stats.verified++
            console.log(`\n  ✓ [${record.table}] ${cid} (${formatBytes(record.size)})`)
            console.log(`    URL: ${url}`)
          } else {
            ctx.stats.missing++
            console.log(`\n  ✗ [${record.table}] ${cid} - MISSING (${res.status})`)
            console.log(`    URL: ${url}`)
            return // Skip deletion if blob doesn't exist in R2
          }
        } catch (err) {
          ctx.stats.verifyErrors++
          console.log(`\n  ⚠ [${record.table}] ${cid} - ERROR: ${err.message}`)
          console.log(`    URL: ${url}`)
          return
        }
      }

      // Delete if requested
      if (ctx.shouldDelete) {
        if (ctx.dryRun) {
          ctx.stats.wouldDelete++
          // Log first 10 dry-run deletions
          if (ctx.stats.wouldDelete <= 10) {
            console.log(`  [DRY-RUN] Would delete: ${record.table}: ${record.multihash || record.link} (${formatBytes(record.size)})`)
          }
        } else {
          try {
            // Delete from R2
            if (r2Key && ctx.r2Client) {
              await ctx.r2Client.send(new DeleteObjectCommand({
                Bucket: config.storage.carparkBucket,
                Key: r2Key
              }))
            }

            // Delete from DynamoDB
            // console.log('')
            // if (record.table === 'blob-registry') {
            //   await ctx.dynamoClient.send(new DeleteCommand({
            //     TableName: config.tables.blobRegistry,
            //     Key: { space: record.space, multihash: record.multihash }
            //   }))
            // } else if (record.table === 'allocations') {
            //   await ctx.dynamoClient.send(new DeleteCommand({
            //     TableName: config.tables.allocations,
            //     Key: { space: record.space, multihash: record.multihash }
            //   }))
            // } else if (record.table === 'store') {
            //   await ctx.dynamoClient.send(new DeleteCommand({
            //     TableName: config.tables.store,
            //     Key: { space: record.space, link: record.link }
            //   }))
            // }

            ctx.stats.deleted++
          } catch (err) {
            ctx.stats.deleteFailed++
          }
        }
      }

      ctx.stats.processed++
    }))

    // Call progress callback after each chunk
    chunksProcessed++
    if (onProgress) {
      onProgress()
    }
  }
}

async function main() {
  console.log('=' .repeat(80))
  console.log('OpenSea Data Deletion Script (Streaming Mode)')
  console.log('=' .repeat(80))
  console.log()
  console.log('Environment:', config.environment)
  console.log('Customer:', OPENSEA_CUSTOMER)
  console.log('Mode:', args.delete ? (dryRun ? 'DRY-RUN DELETION' : '⚠️  ACTUAL DELETION') : (args.verify ? 'DISCOVERY + VERIFY' : 'DISCOVERY'))
  if (args.delete || args.verify) {
    console.log('Limit:', processLimit === 0 ? 'NO LIMIT (all records)' : processLimit)
  }
  console.log('Concurrency:', concurrency)
  if (resumeFrom) {
    console.log('Resume from:', resumeFrom)
  }
  console.log()

  if (args.execute && !args.delete) {
    console.error('ERROR: --execute requires --delete flag')
    process.exit(1)
  }

  if (args.execute) {
    console.log('⚠️  WARNING: ACTUAL DELETION MODE - Data will be permanently deleted!')
    console.log('    Press Ctrl+C within 5 seconds to abort...')
    await new Promise(resolve => setTimeout(resolve, 5000))
    console.log('    Proceeding with deletion...\n')
  }

  const dynamoClient = getDynamoClient()

  // Initialize R2 client if we're doing actual deletions
  let r2Client = null
  if (args.delete && !dryRun) {
    r2Client = new S3Client({
      region: 'auto',
      endpoint: config.storage.r2Endpoint,
      credentials: {
        accessKeyId: config.storage.r2AccessKeyId,
        secretAccessKey: config.storage.r2SecretAccessKey
      }
    })
  }

  // Get all spaces for OpenSea
  console.log('Fetching spaces for OpenSea customer...')
  const allSpaces = await getSpacesForCustomer(OPENSEA_CUSTOMER)
  console.log(`Found ${allSpaces.length} spaces`)

  if (allSpaces.length === 0) {
    console.log('\nNo spaces found for this customer. Exiting.')
    return
  }

  // Handle resume
  let spaces = allSpaces
  if (resumeFrom) {
    const resumeIndex = allSpaces.findIndex(s => s === resumeFrom)
    if (resumeIndex === -1) {
      console.error(`ERROR: Resume space not found: ${resumeFrom}`)
      process.exit(1)
    }
    spaces = allSpaces.slice(resumeIndex)
    console.log(`Resuming from space ${resumeIndex + 1}/${allSpaces.length}`)
  }

  // Show first few spaces
  console.log('\nSpaces to process:')
  for (const space of spaces.slice(0, 5)) {
    console.log(`  - ${space}`)
  }
  if (spaces.length > 5) {
    console.log(`  ... and ${spaces.length - 5} more`)
  }

  // Processing context
  const ctx = {
    dynamoClient,
    r2Client,
    verify: args.verify,
    shouldDelete: args.delete,
    dryRun,
    processLimit,
    concurrency,
    stats: {
      spacesProcessed: 0,
      totalRecords: 0,
      processed: 0,
      verified: 0,
      missing: 0,
      verifyErrors: 0,
      deleted: 0,
      deleteFailed: 0,
      wouldDelete: 0,
      skipped: 0,
      totalSize: 0,
      tableStats: {
        'allocations': { count: 0, size: 0 },
        'store': { count: 0, size: 0 }
      }
    }
  }

  console.log('\n' + '-'.repeat(80))
  console.log('PROCESSING SPACES (streaming mode)')
  console.log('-'.repeat(80))

  const startTime = Date.now()
  let lastProgressUpdate = Date.now()

  /**
   * Print progress line
   */
  function printProgress(spaceIdx, currentSpace) {
    const now = Date.now()
    const elapsed = (now - startTime) / 1000
    const rate = ctx.stats.totalRecords / elapsed || 0
    const pct = ((spaceIdx / spaces.length) * 100).toFixed(1)
    
    // Build status parts
    const parts = [
      `Space ${spaceIdx + 1}/${spaces.length}`,
      `${ctx.stats.totalRecords.toLocaleString()} records`,
      formatBytes(ctx.stats.totalSize)
    ]
    
    if (ctx.verify) {
      parts.push(`✓${ctx.stats.verified} ✗${ctx.stats.missing}`)
    }
    
    if (ctx.shouldDelete) {
      if (ctx.dryRun) {
        parts.push(`would-del: ${ctx.stats.wouldDelete}`)
      } else {
        parts.push(`deleted: ${ctx.stats.deleted}`)
      }
    }
    
    parts.push(`${rate.toFixed(0)} rec/s`)
    
    // Clear line and print
    process.stdout.write(`\r\x1b[K[${pct}%] ${parts.join(' | ')}`)
    lastProgressUpdate = now
  }

  // Process each space immediately (streaming)
  for (let i = 0; i < spaces.length; i++) {
    const space = spaces[i]
    
    // Check if we've hit the limit
    if (ctx.processLimit > 0 && ctx.stats.processed >= ctx.processLimit) {
      console.log(`\n\nLimit reached (${ctx.processLimit} records). Stopping.`)
      break
    }

    // Progress callback - shows current table being processed
    const onQueryProgress = (table, count, pages) => {
      process.stdout.write(`\r\x1b[K[${((i / spaces.length) * 100).toFixed(1)}%] Space ${i + 1}/${spaces.length} | ${table} ${count.toLocaleString()} rec (pg ${pages}) | Total: ${ctx.stats.totalRecords.toLocaleString()} | ${formatBytes(ctx.stats.totalSize)} | ✓${ctx.stats.verified} ✗${ctx.stats.missing}`)
    }

    // Process records as they come in (true streaming)
    const onRecords = async (records) => {
      await processBatch(records, ctx)
    }

    // Initial progress
    printProgress(i, space)

    // Calculate remaining limit for each table
    const getRemainingLimit = () => ctx.processLimit > 0 ? Math.max(0, ctx.processLimit - ctx.stats.processed) : 0

    // Stream and process allocations (blob-registry is a duplicate, skip it)
    const allocLimit = getRemainingLimit()
    const allocStats = await streamAllocations(dynamoClient, space, onRecords, onQueryProgress, allocLimit)
    ctx.stats.tableStats['allocations'].count += allocStats.count
    ctx.stats.tableStats['allocations'].size += allocStats.size
    ctx.stats.totalRecords += allocStats.count
    ctx.stats.totalSize += allocStats.size

    if (ctx.processLimit > 0 && ctx.stats.processed >= ctx.processLimit) {
      console.log(`\n\nLimit reached (${ctx.processLimit} records). Stopping.`)
      break
    }

    // Stream and process store
    const storeLimit = getRemainingLimit()
    const storeStats = await streamStore(dynamoClient, space, onRecords, onQueryProgress, storeLimit)
    ctx.stats.tableStats['store'].count += storeStats.count
    ctx.stats.tableStats['store'].size += storeStats.size
    ctx.stats.totalRecords += storeStats.count
    ctx.stats.totalSize += storeStats.size

    ctx.stats.spacesProcessed++

    // Save progress periodically (every 10 spaces)
    if (i % 10 === 0) {
      saveProgress({
        lastSpace: space,
        spaceIndex: i,
        stats: ctx.stats,
        timestamp: new Date().toISOString()
      })
    }
  }

  const elapsed = (Date.now() - startTime) / 1000

  console.log('\n\n')
  console.log('=' .repeat(80))
  console.log('RESULTS')
  console.log('=' .repeat(80))
  console.log()
  console.log('Summary:')
  console.log('-'.repeat(40))
  console.log(`  Spaces processed:    ${ctx.stats.spacesProcessed}/${spaces.length}`)
  console.log(`  Total records:       ${ctx.stats.totalRecords.toLocaleString()}`)
  console.log(`  Total size:          ${formatBytes(ctx.stats.totalSize)}`)
  console.log(`  Time elapsed:        ${elapsed.toFixed(1)}s`)
  console.log()
  console.log('By Table:')
  console.log(`  allocations:         ${ctx.stats.tableStats['allocations'].count.toLocaleString()} records (${formatBytes(ctx.stats.tableStats['allocations'].size)})`)
  console.log(`  store:               ${ctx.stats.tableStats['store'].count.toLocaleString()} records (${formatBytes(ctx.stats.tableStats['store'].size)})`)

  if (args.verify) {
    console.log()
    console.log('Verification:')
    console.log('-'.repeat(40))
    console.log(`  Verified in R2:      ${ctx.stats.verified.toLocaleString()}`)
    console.log(`  Missing from R2:     ${ctx.stats.missing.toLocaleString()}`)
    console.log(`  Verify errors:       ${ctx.stats.verifyErrors.toLocaleString()}`)
    if (ctx.stats.verified + ctx.stats.missing > 0) {
      const foundPct = ((ctx.stats.verified / (ctx.stats.verified + ctx.stats.missing)) * 100).toFixed(1)
      console.log(`  R2 presence rate:    ${foundPct}%`)
    }
  }

  if (args.delete) {
    console.log()
    console.log('Deletion:')
    console.log('-'.repeat(40))
    if (dryRun) {
      console.log(`  Would delete:        ${ctx.stats.wouldDelete.toLocaleString()}`)
      console.log(`  Skipped (limit):     ${ctx.stats.skipped.toLocaleString()}`)
      console.log()
      console.log('⚠️  This was a DRY-RUN. No data was actually deleted.')
      console.log('    To actually delete, run with --delete --execute')
    } else {
      console.log(`  Deleted:             ${ctx.stats.deleted.toLocaleString()}`)
      console.log(`  Failed:              ${ctx.stats.deleteFailed.toLocaleString()}`)
      console.log(`  Skipped (limit):     ${ctx.stats.skipped.toLocaleString()}`)
    }
  }

  // Save final progress
  saveProgress({
    lastSpace: spaces[ctx.stats.spacesProcessed - 1] || spaces[0],
    spaceIndex: ctx.stats.spacesProcessed - 1,
    stats: ctx.stats,
    timestamp: new Date().toISOString(),
    completed: true
  })

  console.log('\n')
  console.log('=' .repeat(80))
  console.log('Script complete')
  console.log('=' .repeat(80))
  console.log(`Progress saved to: ${PROGRESS_FILE}`)
}

main().catch(err => {
  console.error('Fatal error:', err)
  process.exit(1)
})
