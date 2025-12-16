#!/usr/bin/env node
/**
 * Cost Estimation Tool for Index Worker Migration
 * 
 * Samples uploads from the database and estimates the cost of generating
 * indices using the Cloudflare Workers index service.
 * 
 * Usage:
 *   node src/estimate-costs.js --sample 1000
 *   node src/estimate-costs.js --sample 1000 --total 1000000
 *   node src/estimate-costs.js --sample 500 --space did:key:z6Mk...
 */
import dotenv from 'dotenv'
const envFile = process.env.STORACHA_ENV === 'production' ? '.env-production' : '.env-staging'
dotenv.config({ path: envFile, override: true })
import { parseArgs } from 'node:util'
import { validateConfig } from './config.js'
import { getUploadsForSpace } from './lib/tables/upload-table.js'
import { getShardSize } from './lib/tables/shard-data-table.js'
import { generateShardedIndex } from './lib/index-worker.js'
import { getErrorMessage } from './lib/error-utils.js'

/**
 * Cloudflare Workers Pricing
 */
const PRICING = {
  // Free tier
  FREE_REQUESTS_PER_DAY: 100_000,
  FREE_GB_SECONDS_PER_DAY: 400_000,
  
  // Paid tier (after free tier)
  COST_PER_MILLION_REQUESTS: 0.50, // USD
  COST_PER_MILLION_GB_SECONDS: 12.50, // USD
  
  // Bundled plan
  BUNDLED_MONTHLY_COST: 5.00, // USD
  BUNDLED_REQUESTS: 10_000_000,
  BUNDLED_GB_SECONDS: 30_000_000,
  
  // Estimated worker execution time per request
  ESTIMATED_DURATION_MS: 100, // Conservative estimate
  ESTIMATED_MEMORY_MB: 128, // Workers default
}

/**
 * Analyze a sample of uploads and estimate costs by actually running the worker
 * 
 * @param {number} sampleSize - Number of uploads to sample
 * @param {string} [spaceFilter] - Optional space filter
 * @param {boolean} dryRun - Whether to run in dry run mode
 */
async function estimateCosts(sampleSize, spaceFilter, dryRun = false) {
  console.log('Index Worker Cost Estimation')
  console.log('='.repeat(50))
  console.log(`Sample size: ${sampleSize}`)
  console.log(`Mode: ${dryRun ? 'DRY RUN (count only)' : 'LIVE (call worker)'}`)
  if (spaceFilter) console.log(`Space filter: ${spaceFilter}`)
  console.log()
  
  /**
   * @type {{
   *   totalUploads: number,
   *   totalShards: number,
   *   totalWorkerCalls: number,
   *   totalExecutionTimeMs: number,
   *   shardDistribution: Record<number, number>,
   *   errors: number,
   *   successfulIndexes: number
   * }}
   */
  const stats = {
    totalUploads: 0,
    totalShards: 0,
    totalWorkerCalls: 0,
    totalExecutionTimeMs: 0,
    shardDistribution: {}, // { shardCount: uploadCount }
    errors: 0,
    successfulIndexes: 0,
  }
  
  console.log('Sampling uploads...')
  console.log()
  
  for await (const upload of getUploadsForSpace({ limit: sampleSize, space: spaceFilter })) {
    stats.totalUploads++
    
    const shardCount = upload.shards.length
    stats.totalShards += shardCount
    stats.shardDistribution[shardCount] = (stats.shardDistribution[shardCount] || 0) + 1
    
    // If not dry run, actually call the worker
    if (!dryRun) {
      console.log(`[${stats.totalUploads}/${sampleSize}] Processing ${upload.root}...`)
      try {
        // Get shard sizes (needed for worker call)
        console.log(`  Loading ${upload.shards.length} shard(s) from DB...`)
        const shards = []
        for (const shardCID of upload.shards) {
          const size = await getShardSize(upload.space, shardCID)
          shards.push({ cid: shardCID, size })
        }
        console.log(`  ✓ Loaded ${shards.length} shard(s), generating index...`)
        
        // Call the worker and measure time
        const startTime = Date.now()
        const result = await generateShardedIndex(upload.root, shards)
        const executionTime = Date.now() - startTime
        
        stats.totalWorkerCalls += result.totalRequests
        stats.totalExecutionTimeMs += executionTime
        stats.successfulIndexes++
        
        console.log(`  ✓ Complete: ${shards.length} shard(s), ${result.totalRequests} HTTP requests, ${executionTime}ms`)
        
        // Periodic summary every 50 uploads
        if (stats.successfulIndexes % 50 === 0) {
          const avgTime = (stats.totalExecutionTimeMs / stats.successfulIndexes).toFixed(0)
          const avgRequests = (stats.totalWorkerCalls / stats.successfulIndexes).toFixed(1)
          console.log()
          console.log(` Progress Summary (${stats.successfulIndexes}/${sampleSize}):`)  
          console.log(`   Avg time: ${avgTime}ms/upload, Avg requests: ${avgRequests}/upload`)
          console.log(`   Errors: ${stats.errors}`)
          console.log()
        }
      } catch (error) {
        stats.errors++
        console.error(`  ✗ FAILED: ${getErrorMessage(error)}`)
      }
    }
    
    // Progress indicator
    if (stats.totalUploads % 10 === 0 && dryRun) {
      console.log(`  Processed ${stats.totalUploads} uploads...`)
    }
  }
  
  console.log()
  console.log('Sample Statistics')
  console.log('='.repeat(50))
  console.log(`Total uploads sampled: ${stats.totalUploads}`)
  console.log(`Total shards: ${stats.totalShards}`)
  console.log(`Average shards per upload: ${(stats.totalShards / stats.totalUploads).toFixed(2)}`)
  
  if (!dryRun) {
    console.log()
    console.log('Worker Execution Metrics:')
    console.log(`  Successful indexes: ${stats.successfulIndexes}`)
    console.log(`  Errors: ${stats.errors}`)
    console.log(`  Total HTTP requests: ${stats.totalWorkerCalls}`)
    console.log(`  Average requests per upload: ${(stats.totalWorkerCalls / stats.successfulIndexes).toFixed(1)}`)
    console.log(`  Total execution time: ${(stats.totalExecutionTimeMs / 1000).toFixed(2)}s`)
    console.log(`  Average time per upload: ${(stats.totalExecutionTimeMs / stats.successfulIndexes).toFixed(0)}ms`)
    console.log(`  Average time per request: ${(stats.totalExecutionTimeMs / stats.totalWorkerCalls).toFixed(0)}ms`)
  }
  
  console.log()
  
  // Shard distribution
  console.log('Shard Distribution:')
  const sortedDistribution = Object.entries(stats.shardDistribution)
    .sort(([a], [b]) => parseInt(a) - parseInt(b))
  for (const [shardCount, uploadCount] of sortedDistribution) {
    const percentage = ((uploadCount / stats.totalUploads) * 100).toFixed(1)
    console.log(`  ${shardCount} shard(s): ${uploadCount} uploads (${percentage}%)`)
  }
  console.log()
  
  return stats
}

/**
 * Calculate costs based on statistics
 * 
 * @param {object} stats - Statistics from estimateCosts
 * @param {number} stats.totalUploads - Total uploads sampled
 * @param {number} stats.totalShards - Total shards sampled
 * @param {number} stats.successfulIndexes - Successful indexes
 * @param {number} stats.errors - Errors
 * @param {number} stats.totalWorkerCalls - Total worker calls
 * @param {number} stats.totalExecutionTimeMs - Total execution time (ms)
 * @param {object} stats.shardDistribution - Shard distribution
 * @param {number} totalUploadsInDB - Total uploads in database
 * @param {boolean} dryRun - Whether to run in dry run mode
 */
function calculateCosts(stats, totalUploadsInDB, dryRun) {
  console.log('Cost Estimation')
  console.log('='.repeat(50))
  
  // Extrapolate to full database
  const scaleFactor = totalUploadsInDB / stats.totalUploads
  const estimatedTotalShards = Math.ceil(stats.totalShards * scaleFactor)
  
  console.log(`Estimated total uploads: ${totalUploadsInDB.toLocaleString()}`)
  console.log(`Estimated total shards: ${estimatedTotalShards.toLocaleString()}`)
  console.log(`Average shards per upload: ${(estimatedTotalShards / totalUploadsInDB).toFixed(2)}`)
  console.log()
  
  // Worker requests - extrapolate from real data if available, otherwise estimate
  let totalRequests
  if (!dryRun && stats.totalWorkerCalls > 0) {
    // Use real request count ratio
    totalRequests = Math.ceil(stats.totalWorkerCalls * scaleFactor)
    console.log(`Using REAL request count: ${(stats.totalWorkerCalls / stats.totalUploads).toFixed(1)} requests per upload`)
  } else {
    // Conservative estimate: assume 1 request per shard (will be underestimate)
    totalRequests = estimatedTotalShards
    console.log(`Using ESTIMATED request count: 1 request per shard (likely underestimate)`)
  }
  console.log(`Estimated total HTTP requests: ${totalRequests.toLocaleString()}`)
  console.log()
  
  // Estimate compute time - use real metrics if available, otherwise use estimate
  let avgDurationSeconds
  if (!dryRun && stats.totalWorkerCalls > 0) {
    avgDurationSeconds = (stats.totalExecutionTimeMs / stats.totalWorkerCalls) / 1000
    console.log(`Using REAL average execution time: ${(avgDurationSeconds * 1000).toFixed(0)}ms per request`)
  } else {
    avgDurationSeconds = PRICING.ESTIMATED_DURATION_MS / 1000
    console.log(`Using ESTIMATED execution time: ${(avgDurationSeconds * 1000).toFixed(0)}ms per request`)
  }
  console.log()
  
  const memoryGB = PRICING.ESTIMATED_MEMORY_MB / 1024
  const totalGBSeconds = totalRequests * avgDurationSeconds * memoryGB
  
  console.log('Worker Usage:')
  console.log(`  Total requests: ${totalRequests.toLocaleString()}`)
  console.log(`  Estimated GB-seconds: ${totalGBSeconds.toLocaleString()}`)
  console.log()
  
  // Calculate costs
  
  // Option 1: Free tier only (per day)
  const daysOnFreeTier = Math.ceil(totalRequests / PRICING.FREE_REQUESTS_PER_DAY)
  console.log('Option 1: Free Tier Only')
  console.log(`  Days required: ${daysOnFreeTier}`)
  console.log(`  Cost: $0.00`)
  console.log()
  
  // Option 2: Paid tier (all at once)
  const paidRequests = Math.max(0, totalRequests - PRICING.FREE_REQUESTS_PER_DAY)
  const paidGBSeconds = Math.max(0, totalGBSeconds - PRICING.FREE_GB_SECONDS_PER_DAY)
  const requestCost = (paidRequests / 1_000_000) * PRICING.COST_PER_MILLION_REQUESTS
  const computeCost = (paidGBSeconds / 1_000_000) * PRICING.COST_PER_MILLION_GB_SECONDS
  const totalPaidCost = requestCost + computeCost
  
  console.log('Option 2: Paid Tier (One-time run)')
  console.log(`  Request cost: $${requestCost.toFixed(2)}`)
  console.log(`  Compute cost: $${computeCost.toFixed(2)}`)
  console.log(`  Total cost: $${totalPaidCost.toFixed(2)}`)
  console.log()
  
  // Option 3: Bundled plan
  const monthsNeeded = Math.ceil(totalRequests / PRICING.BUNDLED_REQUESTS)
  const bundledCost = monthsNeeded * PRICING.BUNDLED_MONTHLY_COST
  
  console.log('Option 3: Bundled Plan ($5/month)')
  console.log(`  Months needed: ${monthsNeeded}`)
  console.log(`  Total cost: $${bundledCost.toFixed(2)}`)
  console.log()
  
  // Recommendation
  console.log('Recommendation:')
  if (totalRequests <= PRICING.FREE_REQUESTS_PER_DAY && daysOnFreeTier <= 7) {
    console.log('  ✓ Use FREE TIER - spread migration over a few days')
  } else if (totalPaidCost < bundledCost) {
    console.log(`  ✓ Use PAID TIER - one-time cost of $${totalPaidCost.toFixed(2)}`)
  } else {
    console.log(`  ✓ Use BUNDLED PLAN - $${bundledCost.toFixed(2)} over ${monthsNeeded} month(s)`)
  }
  console.log()
}

/**
 * Main function
 */
async function main() {
  const { values } = parseArgs({
    options: {
      sample: {
        type: 'string',
        short: 's',
        default: '1000',
      },
      space: {
        type: 'string',
      },
      total: {
        type: 'string',
        short: 't',
      },
      'dry-run': {
        type: 'boolean',
        default: false,
      },
    },
  })
  
  validateConfig()
  
  const sampleSize = parseInt(values.sample, 10)
  const totalUploads = values.total ? parseInt(values.total, 10) : null
  const dryRun = values['dry-run']
  
  if (!totalUploads) {
    console.log('Note: Use --total to specify total uploads in DB for accurate cost estimation')
    console.log('      Example: --total 1000000')
    console.log()
  }
  
  if (!dryRun) {
    console.log('⚠️  LIVE MODE: Will actually call the index worker!')
    console.log('   Use --dry-run to just count shards without calling the worker')
    console.log()
  }
  
  // Run estimation
  const stats = await estimateCosts(sampleSize, values.space, dryRun)
  
  if (totalUploads) {
    calculateCosts(stats, totalUploads, dryRun)
  } else {
    console.log('To calculate costs, re-run with --total parameter')
    console.log(`Example: node src/estimate-costs.js --sample ${sampleSize} --total 1000000`)
  }
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
