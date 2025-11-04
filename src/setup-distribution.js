#!/usr/bin/env node
/**
 * Setup Distribution Script
 * 
 * Discovers all customers from the Upload Table and distributes them
 * across multiple EC2 instances for parallel migration.
 * 
 * Uses parallel DynamoDB scan (4 segments by default) for faster discovery.
 * 
 * Usage:
 *   # Analyze customer distribution (dry run)
 *   node src/setup-distribution.js --analyze
 * 
 *   # Generate distribution for 5 instances
 *   node src/setup-distribution.js --instances 5
 * 
 *   # Use more parallel segments for faster scanning (1-10)
 *   node src/setup-distribution.js --analyze --parallel-segments 8
 * 
 *   # Generate distribution with filters
 *   node src/setup-distribution.js --instances 5 --min-uploads 100
 * 
 *   # Estimate with different worker counts
 *   node src/setup-distribution.js --instances 5 --workers-per-instance 15
 */
import dotenv from 'dotenv'
dotenv.config()
import { parseArgs } from 'node:util'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, ScanCommand, QueryCommand } from '@aws-sdk/lib-dynamodb'
import { validateConfig, config } from './config.js'
import fs from 'fs/promises'
import path from 'path'

const DISTRIBUTION_DIR = 'migration-state'

/**
 * Scan consumer table to get space -> customer mappings
 * 
 * @param {number} segment - Segment number (0-based)
 * @param {number} totalSegments - Total number of segments
 * @returns {Promise<Map>} Map of customer -> Set of spaces
 */
async function scanConsumerSegment(segment, totalSegments) {
  const baseClient = new DynamoDBClient({
    region: config.aws.region,
  })
  const client = DynamoDBDocumentClient.from(baseClient)
  const customerSpacesMap = new Map() // customer -> Set(spaces)
  
  let scanned = 0
  let lastEvaluatedKey
  const startTime = Date.now()
  
  console.log(`  [Segment ${segment}] Scanning consumer table...`)
  
  while (true) {
    const command = new ScanCommand({
      TableName: config.tables.consumer,
      ProjectionExpression: 'consumer, customer',
      Limit: 1000,
      ExclusiveStartKey: lastEvaluatedKey,
      Segment: segment,
      TotalSegments: totalSegments,
    })
    
    const response = await client.send(command)
    
    if (!response.Items || response.Items.length === 0) {
      break
    }
    
    for (const record of response.Items) {
      if (record.consumer && record.customer) {
        if (!customerSpacesMap.has(record.customer)) {
          customerSpacesMap.set(record.customer, new Set())
        }
        customerSpacesMap.get(record.customer).add(record.consumer)
      }
    }
    
    scanned += response.Items.length
    
    lastEvaluatedKey = response.LastEvaluatedKey
    if (!lastEvaluatedKey) {
      break
    }
  }
  
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`  [Segment ${segment}] Complete: ${scanned.toLocaleString()} consumer records in ${elapsed}s`)
  
  return { segment, scanned, customerSpacesMap }
}

/**
 * Count uploads for a specific space with retry logic
 * 
 * @param {DynamoDBClient} client - Reusable DynamoDB client
 * @param {string} space - Space DID
 * @returns {Promise<number>} Upload count (0 if space is empty)
 */
async function countUploadsForSpace(client, space) {
  const maxRetries = 5
  const baseDelay = 1000 // 1 second
  
  async function executeWithRetry(command, retryCount = 0) {
    try {
      return await client.send(command)
    } catch (error) {
      // Retry on timeout or throttling errors
      if ((error.code === 'ETIMEDOUT' || error.name === 'TimeoutError' || 
           error.name === 'ProvisionedThroughputExceededException') && 
          retryCount < maxRetries) {
        const delay = baseDelay * Math.pow(2, retryCount) // Exponential backoff
        console.log(`    [Retry ${retryCount + 1}/${maxRetries}] Connection error, retrying in ${delay}ms...`)
        await new Promise(resolve => setTimeout(resolve, delay))
        return executeWithRetry(command, retryCount + 1)
      }
      throw error
    }
  }
  
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
    Limit: 1, // Just check if space has any uploads
  })
  
  const response = await executeWithRetry(command)
  
  // If space is empty, return 0 immediately
  if (!response.Count || response.Count === 0) {
    return 0
  }
  
  // If space has uploads and needs pagination, continue counting
  let count = response.Count
  let lastEvaluatedKey = response.LastEvaluatedKey
  
  while (lastEvaluatedKey) {
    const paginatedCommand = new QueryCommand({
      TableName: config.tables.upload,
      KeyConditionExpression: '#space = :space',
      ExpressionAttributeNames: {
        '#space': 'space',
      },
      ExpressionAttributeValues: {
        ':space': space,
      },
      Select: 'COUNT',
      ExclusiveStartKey: lastEvaluatedKey,
    })
    
    const paginatedResponse = await executeWithRetry(paginatedCommand)
    count += paginatedResponse.Count || 0
    lastEvaluatedKey = paginatedResponse.LastEvaluatedKey
  }
  
  return count
}

/**
 * Discover all unique customers and their upload counts using parallel scan
 * 
 * @param {object} options
 * @param {number} [options.minUploads] - Minimum uploads to include customer
 * @param {number} [options.parallelSegments] - Number of parallel scan segments
 * @returns {Promise<Array<{customer: string, uploadCount: number, spaceCount: number}>>}
 */
async function discoverCustomers({ minUploads = 0, parallelSegments = 4 } = {}) {
  console.log('Step 1: Scanning Consumer Table for customer->space mappings...')
  console.log(`Using ${parallelSegments} parallel segments for faster scanning`)
  console.log('='.repeat(70))
  
  const startTime = Date.now()
  
  // Launch parallel scans of consumer table
  const scanPromises = []
  for (let i = 0; i < parallelSegments; i++) {
    scanPromises.push(scanConsumerSegment(i, parallelSegments))
  }
  
  // Progress monitoring
  const progressInterval = setInterval(() => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`  Scanning consumer table... ${elapsed}s elapsed`)
  }, 10000) // Update every 10 seconds
  
  // Wait for all segments to complete
  const results = await Promise.all(scanPromises)
  clearInterval(progressInterval)
  
  // Merge results from all segments
  const customerSpacesMap = new Map() // customer -> Set(spaces)
  let totalConsumerRecords = 0
  
  for (const result of results) {
    totalConsumerRecords += result.scanned
    
    for (const [customer, spaces] of result.customerSpacesMap.entries()) {
      if (!customerSpacesMap.has(customer)) {
        customerSpacesMap.set(customer, new Set())
      }
      for (const space of spaces) {
        customerSpacesMap.get(customer).add(space)
      }
    }
  }
  
  const scanElapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log()
  console.log(`✓ Consumer table scan complete in ${scanElapsed}s`)
  console.log(`✓ Total consumer records scanned: ${totalConsumerRecords.toLocaleString()}`)
  console.log(`✓ Unique customers found: ${customerSpacesMap.size.toLocaleString()}`)
  console.log()
  
  // Step 2: Count uploads for each customer's spaces (in parallel)
  console.log('Step 2: Counting uploads per customer...')
  console.log('='.repeat(70))
  
  const totalCustomers = customerSpacesMap.size
  const concurrency = 20 // Process 20 customers in parallel (reduced to avoid overwhelming DynamoDB)
  
  console.log(`Processing ${totalCustomers.toLocaleString()} customers with concurrency ${concurrency}...`)
  console.log()
  
  const customerEntries = Array.from(customerSpacesMap.entries())
  const customers = []
  let processedCustomers = 0
  let totalSpacesProcessed = 0
  
  // Create a shared DynamoDB Document Client for reuse
  const baseClient = new DynamoDBClient({
    region: config.aws.region,
  })
  const client = DynamoDBDocumentClient.from(baseClient)
  
  const countStartTime = Date.now()
  
  // Checkpoint file for resume capability
  const checkpointFile = path.join(DISTRIBUTION_DIR, 'counting-checkpoint.json')
  let checkpoint = { processedCustomers: 0, customers: [] }
  
  // Try to load existing checkpoint
  try {
    await fs.mkdir(DISTRIBUTION_DIR, { recursive: true })
    const checkpointData = await fs.readFile(checkpointFile, 'utf-8')
    checkpoint = JSON.parse(checkpointData)
    console.log(`Resuming from checkpoint: ${checkpoint.processedCustomers} customers already processed`)
    customers.push(...checkpoint.customers)
    processedCustomers = checkpoint.processedCustomers
    console.log()
  } catch (error) {
    // No checkpoint exists, start fresh
  }
  
  // Process customers in batches
  for (let i = checkpoint.processedCustomers; i < customerEntries.length; i += concurrency) {
    const batch = customerEntries.slice(i, i + concurrency)
    
    const batchStartTime = Date.now()
    const batchResults = await Promise.all(
      batch.map(async ([customer, spaces], batchIndex) => {
        const customerStartTime = Date.now()
        let totalUploads = 0
        let spacesProcessed = 0
        let emptySpaces = 0
        let spacesWithUploads = 0
        
        // Count uploads for each space sequentially per customer to avoid too many parallel queries
        for (const space of spaces) {
          const count = await countUploadsForSpace(client, space)
          totalUploads += count
          spacesProcessed++
          
          if (count === 0) {
            emptySpaces++
          } else {
            spacesWithUploads++
          }
          
          // Log progress for customers with many spaces
          if (spaces.size > 10 && spacesProcessed % 50 === 0) {
            const customerElapsed = ((Date.now() - customerStartTime) / 1000).toFixed(1)
            console.log(`    [Batch ${Math.floor(i / concurrency) + 1}, Customer ${batchIndex + 1}/${batch.length}] ${spacesProcessed}/${spaces.size} spaces (${emptySpaces} empty, ${spacesWithUploads} with uploads), ${totalUploads.toLocaleString()} uploads, ${customerElapsed}s`)
          }
        }
        
        return {
          customer,
          uploadCount: totalUploads,
          spaceCount: spacesWithUploads, // Spaces with uploads
          emptySpaceCount: emptySpaces, // Spaces without uploads
          totalSpaceCount: spaces.size, // All spaces
        }
      })
    )
    
    const batchElapsed = ((Date.now() - batchStartTime) / 1000).toFixed(1)
    
    customers.push(...batchResults)
    processedCustomers += batch.length
    totalSpacesProcessed += batch.reduce((sum, [, spaces]) => sum + spaces.size, 0)
    
    const pct = ((processedCustomers / totalCustomers) * 100).toFixed(1)
    const elapsed = ((Date.now() - countStartTime) / 1000).toFixed(1)
    const rate = (processedCustomers / (Date.now() - countStartTime) * 1000).toFixed(1)
    console.log(`  ${processedCustomers.toLocaleString()}/${totalCustomers.toLocaleString()} customers (${pct}%) | ${totalSpacesProcessed.toLocaleString()} spaces | ${elapsed}s | ${rate} customers/s | batch: ${batchElapsed}s`)
    
    // Save checkpoint every 10 batches
    if (processedCustomers % (concurrency * 10) === 0) {
      await fs.writeFile(checkpointFile, JSON.stringify({
        processedCustomers,
        customers,
        timestamp: new Date().toISOString()
      }, null, 2))
    }
  }
  
  // Clean up checkpoint file on completion
  try {
    await fs.unlink(checkpointFile)
  } catch (error) {
    // Ignore if file doesn't exist
  }
  
  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log()
  console.log(`✓ Upload counting complete in ${totalElapsed}s`)
  console.log()
  
  // Filter and sort
  const filteredCustomers = customers
    .filter(c => c.uploadCount >= minUploads)
    .sort((a, b) => b.uploadCount - a.uploadCount) // Sort by upload count descending
  
  if (minUploads > 0) {
    const filtered = customers.length - filteredCustomers.length
    console.log(`Filtered out ${filtered} customers with < ${minUploads} uploads`)
    console.log()
  }
  
  return filteredCustomers
}

/**
 * Analyze customer distribution statistics
 */
function analyzeDistribution(customers) {
  console.log('Customer Distribution Analysis')
  console.log('='.repeat(70))
  
  const totalCustomers = customers.length
  const totalUploads = customers.reduce((sum, c) => sum + c.uploadCount, 0)
  const totalSpaces = customers.reduce((sum, c) => sum + c.spaceCount, 0)
  const totalEmptySpaces = customers.reduce((sum, c) => sum + (c.emptySpaceCount || 0), 0)
  const totalAllSpaces = customers.reduce((sum, c) => sum + (c.totalSpaceCount || c.spaceCount), 0)
  const avgUploadsPerCustomer = totalUploads / totalCustomers
  const avgSpacesPerCustomer = totalSpaces / totalCustomers
  const emptySpacePct = ((totalEmptySpaces / totalAllSpaces) * 100).toFixed(1)
  
  console.log(`Total customers: ${totalCustomers.toLocaleString()}`)
  console.log(`Total uploads: ${totalUploads.toLocaleString()}`)
  console.log(`Total spaces (with uploads): ${totalSpaces.toLocaleString()}`)
  console.log(`Total empty spaces: ${totalEmptySpaces.toLocaleString()} (${emptySpacePct}%)`)
  console.log(`Total all spaces: ${totalAllSpaces.toLocaleString()}`)
  console.log(`Average uploads/customer: ${Math.round(avgUploadsPerCustomer).toLocaleString()}`)
  console.log(`Average spaces/customer: ${avgSpacesPerCustomer.toFixed(1)}`)
  console.log()
  
  // Top customers
  console.log('Top 100 Customers by Upload Count:')
  console.log('-'.repeat(70))
  customers.slice(0, 100).forEach((c, i) => {
    const pct = ((c.uploadCount / totalUploads) * 100).toFixed(2)
    console.log(`  ${String(i + 1).padStart(3)}. ${c.customer.padEnd(50)} | ${String(c.uploadCount.toLocaleString()).padStart(12)} uploads (${String(pct).padStart(5)}%) | ${String(c.spaceCount).padStart(6)} spaces`)
  })
  console.log()
  
  // Distribution buckets
  console.log('Upload Count Distribution:')
  const buckets = [
    { label: '1-10', min: 1, max: 10 },
    { label: '11-100', min: 11, max: 100 },
    { label: '101-1K', min: 101, max: 1000 },
    { label: '1K-10K', min: 1001, max: 10000 },
    { label: '10K-100K', min: 10001, max: 100000 },
    { label: '100K+', min: 100001, max: Infinity },
  ]
  
  for (const bucket of buckets) {
    const count = customers.filter(c => c.uploadCount >= bucket.min && c.uploadCount <= bucket.max).length
    const pct = ((count / totalCustomers) * 100).toFixed(1)
    console.log(`  ${bucket.label.padEnd(10)}: ${count.toLocaleString().padStart(8)} customers (${pct}%)`)
  }
  console.log()
  
  return { totalCustomers, totalUploads, totalSpaces, avgUploadsPerCustomer }
}

/**
 * Distribute customers across instances using greedy load balancing
 * 
 * @param {Array} customers - Array of customer objects
 * @param {number} numInstances - Number of instances
 * @returns {Array<{instanceId: number, customers: Array, totalUploads: number, totalSpaces: number}>}
 */
function distributeCustomers(customers, numInstances) {
  // Initialize instances
  const instances = Array(numInstances).fill(null).map((_, i) => ({
    instanceId: i + 1,
    customers: [],
    totalUploads: 0,
    totalSpaces: 0,
    totalEmptySpaces: 0,
    totalAllSpaces: 0,
  }))
  
  // Greedy assignment: assign each customer to instance with least load
  for (const customer of customers) {
    const lightestInstance = instances.reduce((minIdx, inst, idx) => 
      inst.totalUploads < instances[minIdx].totalUploads ? idx : minIdx
    , 0)
    
    instances[lightestInstance].customers.push(customer.customer)
    instances[lightestInstance].totalUploads += customer.uploadCount
    instances[lightestInstance].totalSpaces += customer.spaceCount
    instances[lightestInstance].totalEmptySpaces += customer.emptySpaceCount || 0
    instances[lightestInstance].totalAllSpaces += customer.totalSpaceCount || customer.spaceCount
  }
  
  return instances
}

/**
 * Print distribution summary
 */
function printDistributionSummary(distribution, totalUploads, workersPerInstance = 10) {
  console.log('Instance Distribution')
  console.log('='.repeat(70))
  
  const UPLOADS_PER_MIN_PER_WORKER = 27
  
  for (const instance of distribution) {
    const pct = ((instance.totalUploads / totalUploads) * 100).toFixed(1)
    const avgUploadsPerCustomer = Math.round(instance.totalUploads / instance.customers.length)
    const emptyPct = instance.totalAllSpaces > 0 
      ? ((instance.totalEmptySpaces / instance.totalAllSpaces) * 100).toFixed(1)
      : '0.0'
    
    // Time estimate for this instance
    const uploadsPerWorker = instance.totalUploads / workersPerInstance
    const minutesRequired = uploadsPerWorker / UPLOADS_PER_MIN_PER_WORKER
    const hoursRequired = minutesRequired / 60
    const daysRequired = hoursRequired / 24
    
    console.log(`Instance ${instance.instanceId}:`)
    console.log(`  Customers: ${instance.customers.length.toLocaleString()}`)
    console.log(`  Uploads: ${instance.totalUploads.toLocaleString()} (${pct}%)`)
    console.log(`  Spaces (with uploads): ${instance.totalSpaces.toLocaleString()}`)
    console.log(`  Empty spaces: ${instance.totalEmptySpaces.toLocaleString()} (${emptyPct}%)`)
    console.log(`  Total spaces: ${instance.totalAllSpaces.toLocaleString()}`)
    console.log(`  Avg uploads/customer: ${avgUploadsPerCustomer.toLocaleString()}`)
    console.log(`  Estimated time (${workersPerInstance} workers): ${daysRequired.toFixed(1)} days (${hoursRequired.toFixed(1)} hours)`)
    console.log()
  }
  
  // Load balance check
  const uploadsPerInstance = distribution.map(i => i.totalUploads)
  const minUploads = Math.min(...uploadsPerInstance)
  const maxUploads = Math.max(...uploadsPerInstance)
  const avgUploads = uploadsPerInstance.reduce((a, b) => a + b, 0) / uploadsPerInstance.length
  const variance = ((maxUploads - minUploads) / avgUploads * 100).toFixed(1)
  
  console.log('Load Balance:')
  console.log(`  Min uploads/instance: ${minUploads.toLocaleString()}`)
  console.log(`  Max uploads/instance: ${maxUploads.toLocaleString()}`)
  console.log(`  Avg uploads/instance: ${Math.round(avgUploads).toLocaleString()}`)
  console.log(`  Variance: ${variance}%`)
  console.log()
}

/**
 * Estimate migration time
 */
function estimateMigrationTime(distribution, workersPerInstance = 10) {
  console.log('Migration Time Estimate')
  console.log('='.repeat(70))
  
  // From previous measurements: ~27 uploads/min per worker
  const UPLOADS_PER_MIN_PER_WORKER = 27
  
  const totalWorkers = distribution.length * workersPerInstance
  const throughput = totalWorkers * UPLOADS_PER_MIN_PER_WORKER
  
  // Find the instance with most work (critical path)
  const maxUploads = Math.max(...distribution.map(i => i.totalUploads))
  const uploadsPerWorker = maxUploads / workersPerInstance
  const minutesRequired = uploadsPerWorker / UPLOADS_PER_MIN_PER_WORKER
  const hoursRequired = minutesRequired / 60
  const daysRequired = hoursRequired / 24
  
  console.log(`Workers per instance: ${workersPerInstance}`)
  console.log(`Total workers: ${totalWorkers}`)
  console.log(`Combined throughput: ${throughput.toLocaleString()} uploads/min`)
  console.log()
  console.log(`Critical path (slowest instance):`)
  console.log(`  Uploads: ${maxUploads.toLocaleString()}`)
  console.log(`  Time: ${minutesRequired.toLocaleString()} minutes`)
  console.log(`       = ${hoursRequired.toFixed(1)} hours`)
  console.log(`       = ${daysRequired.toFixed(1)} days`)
  console.log()
  
  // Show estimates for different worker counts
  console.log('Time estimates for different worker counts:')
  console.log('-'.repeat(70))
  for (const workers of [5, 10, 15, 20]) {
    const mins = (maxUploads / workers) / UPLOADS_PER_MIN_PER_WORKER
    const hours = mins / 60
    const days = hours / 24
    console.log(`  ${String(workers).padStart(2)} workers/instance: ${String(days.toFixed(1)).padStart(5)} days (${String(hours.toFixed(1)).padStart(6)} hours) | Total workers: ${workers * distribution.length}`)
  }
  console.log()
  
  // Workload distribution scenarios
  console.log('Migration Workload Scenarios')
  console.log('='.repeat(70))
  console.log()
  
  const scenarios = [
    { instances: 5, workers: 10, name: 'Baseline (Current Plan)' },
    { instances: 5, workers: 15, name: 'Increased Workers' },
    { instances: 5, workers: 20, name: 'Maximum Workers' },
    { instances: 10, workers: 10, name: 'Double Instances' },
    { instances: 10, workers: 15, name: 'Double Instances + More Workers' },
  ]
  
  for (const scenario of scenarios) {
    const totalWorkers = scenario.instances * scenario.workers
    const uploadsPerInstance = maxUploads // Assumes similar distribution
    const uploadsPerWorker = uploadsPerInstance / scenario.workers
    const mins = uploadsPerWorker / UPLOADS_PER_MIN_PER_WORKER
    const hours = mins / 60
    const days = hours / 24
    const throughput = totalWorkers * UPLOADS_PER_MIN_PER_WORKER
    
    console.log(`Scenario: ${scenario.name}`)
    console.log(`  Configuration: ${scenario.instances} instances × ${scenario.workers} workers = ${totalWorkers} total workers`)
    console.log(`  Throughput: ${throughput.toLocaleString()} uploads/min`)
    console.log(`  Time to complete: ${days.toFixed(1)} days (${hours.toFixed(1)} hours)`)
    console.log(`  Cost (EC2 @ $0.10/hour): $${(scenario.instances * hours * 0.10).toFixed(2)}`)
    console.log()
  }
}

/**
 * Save distribution to files
 */
async function saveDistribution(distribution) {
  // Ensure directory exists
  await fs.mkdir(DISTRIBUTION_DIR, { recursive: true })
  
  console.log('Saving distribution files...')
  console.log('='.repeat(70))
  
  for (const instance of distribution) {
    const filename = `instance-${instance.instanceId}-customers.json`
    const filepath = path.join(DISTRIBUTION_DIR, filename)
    
    const data = {
      instanceId: instance.instanceId,
      totalCustomers: instance.customers.length,
      estimatedUploads: instance.totalUploads,
      estimatedSpaces: instance.totalSpaces,
      customers: instance.customers,
      createdAt: new Date().toISOString(),
    }
    
    await fs.writeFile(filepath, JSON.stringify(data, null, 2))
    console.log(`✓ ${filename}`)
  }
  
  console.log()
  console.log(`Distribution files saved to: ${DISTRIBUTION_DIR}/`)
  console.log()
}

/**
 * Print usage instructions
 */
function printUsageInstructions(numInstances) {
  console.log('Next Steps')
  console.log('='.repeat(70))
  console.log()
  console.log('To start migration, run these commands on each EC2 instance:')
  console.log()
  
  for (let i = 1; i <= numInstances; i++) {
    console.log(`  EC2 Instance ${i}: node src/migrate-instance.js --instance ${i}`)
  }
  
  console.log()
  console.log('To monitor progress:')
  console.log('  node src/monitor.js')
  console.log()
}

/**
 * Main function
 */
async function main() {
  const { values } = parseArgs({
    options: {
      analyze: {
        type: 'boolean',
        default: false,
        description: 'Analyze customer distribution without generating files',
      },
      instances: {
        type: 'string',
        short: 'i',
        description: 'Number of EC2 instances',
      },
      'min-uploads': {
        type: 'string',
        default: '0',
        description: 'Minimum uploads to include customer',
      },
      'workers-per-instance': {
        type: 'string',
        short: 'w',
        default: '10',
        description: 'Workers per instance for time estimate',
      },
      'parallel-segments': {
        type: 'string',
        short: 'p',
        default: '4',
        description: 'Number of parallel scan segments (1-10)',
      },
    },
  })
  
  validateConfig()
  
  const analyzeOnly = values.analyze
  const numInstances = values.instances ? parseInt(values.instances, 10) : null
  const minUploads = parseInt(values['min-uploads'], 10)
  const parallelSegments = Math.max(1, Math.min(10, parseInt(values['parallel-segments'], 10)))
  const workersPerInstance = parseInt(values['workers-per-instance'], 10)
  
  console.log()
  console.log('Legacy Content Migration - Setup Distribution')
  console.log('='.repeat(70))
  console.log()
  
  // Discover customers
  const customers = await discoverCustomers({ minUploads, parallelSegments })
  
  // Analyze distribution
  const stats = analyzeDistribution(customers)
  
  // If analyze-only mode, stop here
  if (analyzeOnly) {
    console.log('Analysis complete. Use --instances N to generate distribution.')
    return
  }
  
  // Generate distribution
  if (!numInstances) {
    console.log('Error: --instances required to generate distribution')
    console.log('Example: node src/setup-distribution.js --instances 5')
    return
  }
  
  const distribution = distributeCustomers(customers, numInstances)
  printDistributionSummary(distribution, stats.totalUploads, workersPerInstance)
  estimateMigrationTime(distribution, workersPerInstance)
  
  // Save to files
  await saveDistribution(distribution)
  
  // Print usage instructions
  printUsageInstructions(numInstances)
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
