#!/usr/bin/env node

/**
 * Monitor migration progress from DynamoDB
 * 
 * Usage:
 *   node src/migration-monitor.js                           # Overall stats
 *   node src/migration-monitor.js --customer <did>          # Customer progress
 *   node src/migration-monitor.js --space <did>             # Space status (requires --customer)
 *   node src/migration-monitor.js --instance <N>            # Instance progress
 *   node src/migration-monitor.js --failed                  # Show failed migrations
 *   node src/migration-monitor.js --stuck                   # Show stuck migrations (>1 hour)
 *   node src/migration-monitor.js --watch                   # Live monitoring (refresh every 30s)
 */

import dotenv from 'dotenv'
dotenv.config()
import { parseArgs } from 'node:util'
import { validateConfig } from './config.js'
import {
  getSpaceProgress,
  getCustomerSpaces,
  getFailedMigrations,
  getStuckMigrations,
  getInstanceSpaces,
  scanAllProgress,
} from './lib/tables/migration-progress-table.js'

/**
 * Get overall migration statistics
 */
async function getOverallStats() {
  const stats = {
    total: 0,
    pending: 0,
    inProgress: 0,
    completed: 0,
    failed: 0,
    totalUploads: 0,
    completedUploads: 0,
    byInstance: {},
  }

  let lastEvaluatedKey = undefined

  do {
    const { items, lastEvaluatedKey: nextKey } = await scanAllProgress({ lastEvaluatedKey })

    for (const item of items) {
      stats.total++
      stats.totalUploads += item.totalUploads || 0
      stats.completedUploads += item.completedUploads || 0

      // Count by status
      if (item.status === 'pending') stats.pending++
      else if (item.status === 'in-progress') stats.inProgress++
      else if (item.status === 'completed') stats.completed++
      else if (item.status === 'failed') stats.failed++

      // Count by instance
      if (item.instanceId) {
        if (!stats.byInstance[item.instanceId]) {
          stats.byInstance[item.instanceId] = {
            total: 0,
            completed: 0,
            failed: 0,
            uploads: 0,
            completedUploads: 0,
          }
        }
        stats.byInstance[item.instanceId].total++
        if (item.status === 'completed') stats.byInstance[item.instanceId].completed++
        if (item.status === 'failed') stats.byInstance[item.instanceId].failed++
        stats.byInstance[item.instanceId].uploads += item.totalUploads || 0
        stats.byInstance[item.instanceId].completedUploads += item.completedUploads || 0
      }
    }

    lastEvaluatedKey = nextKey
  } while (lastEvaluatedKey)

  return stats
}


/**
 * Print overall statistics
 */
function printOverallStats(stats) {
  console.log()
  console.log('Migration Progress Overview')
  console.log('='.repeat(70))
  console.log()
  
  const completionPct = stats.total > 0 ? ((stats.completed / stats.total) * 100).toFixed(1) : '0.0'
  const uploadPct = stats.totalUploads > 0 ? ((stats.completedUploads / stats.totalUploads) * 100).toFixed(1) : '0.0'
  // add a red X emoji here -> 
  console.log(`Total Spaces: ${stats.total.toLocaleString()}`)
  console.log(`  🟢 Completed: ${stats.completed.toLocaleString()} (${completionPct}%)`)
  console.log(`  🔵 In Progress: ${stats.inProgress.toLocaleString()}`)
  console.log(`  🟡 Pending: ${stats.pending.toLocaleString()}`)
  console.log(`  🔴 Failed: ${stats.failed.toLocaleString()}`)
  console.log()
  console.log(`Total Uploads: ${stats.totalUploads.toLocaleString()}`)
  console.log(`  🟢 Completed: ${stats.completedUploads.toLocaleString()} (${uploadPct}%)`)
  console.log()
  
  if (Object.keys(stats.byInstance).length > 0) {
    console.log('Progress by Instance:')
    console.log('-'.repeat(70))
    
    for (const [instanceId, instanceStats] of Object.entries(stats.byInstance).sort()) {
      const instPct = instanceStats.total > 0 ? ((instanceStats.completed / instanceStats.total) * 100).toFixed(1) : '0.0'
      const instUploadPct = instanceStats.uploads > 0 ? ((instanceStats.completedUploads / instanceStats.uploads) * 100).toFixed(1) : '0.0'
      
      console.log()
      console.log(`Instance ${instanceId}:`)
      console.log(`  Spaces: ${instanceStats.completed.toLocaleString()}/${instanceStats.total.toLocaleString()} (${instPct}%)`)
      console.log(`  Uploads: ${instanceStats.completedUploads.toLocaleString()}/${instanceStats.uploads.toLocaleString()} (${instUploadPct}%)`)
      if (instanceStats.failed > 0) {
        console.log(`  Failed: ${instanceStats.failed.toLocaleString()}`)
      }
    }
  }
  
  console.log()
}

/**
 * Print customer progress
 */
function printCustomerProgress(customer, spaces) {
  console.log()
  console.log(`Customer: ${customer}`)
  console.log('='.repeat(70))
  console.log()
  console.log(`Total Spaces: ${spaces.length}`)
  console.log()
  
  if (spaces.length === 0) {
    console.log('No migration data found for this customer.')
    return
  }
  
  const completed = spaces.filter(s => s.status === 'completed').length
  const inProgress = spaces.filter(s => s.status === 'in-progress').length
  const failed = spaces.filter(s => s.status === 'failed').length
  const pending = spaces.filter(s => s.status === 'pending').length
  
  console.log(`Status:`)
  console.log(`  🟢 Completed: ${completed}`)
  console.log(`  🔵 In Progress: ${inProgress}`)
  console.log(`  🟡 Pending: ${pending}`)
  console.log(`  🔴 Failed: ${failed}`)
  console.log()
  
  console.log('Spaces:')
  console.log('-'.repeat(70))
  
  for (const space of spaces.slice(0, 20)) {
    const statusIcon = space.status === 'completed' ? '🟢' : 
                       space.status === 'in-progress' ? '🔵' :
                       space.status === 'failed' ? '🔴' : '🟡'
    const uploadProgress = space.totalUploads > 0 ? 
      ` (${space.completedUploads}/${space.totalUploads} uploads)` : ''
    
    console.log(`  ${statusIcon} ${space.space}${uploadProgress}`)
    if (space.status === 'in-progress') {
      console.log(`     Instance: ${space.instanceId}, Worker: ${space.workerId}`)
      console.log(`     Updated: ${new Date(space.updatedAt).toLocaleString()}`)
    }
    if (space.status === 'failed' && space.error) {
      console.log(`     Error: ${space.error}`)
    }
  }
  
  if (spaces.length > 20) {
    console.log(`  ... and ${spaces.length - 20} more spaces`)
  }
  
  console.log()
}

/**
 * Print space status
 */
function printSpaceStatus(space) {
  console.log()
  console.log('Space Migration Status')
  console.log('='.repeat(70))
  console.log()
  
  if (!space) {
    console.log('No migration data found for this space.')
    return
  }
  
  const statusIcon = space.status === 'completed' ? '🟢' : 
                     space.status === 'in-progress' ? '🔵' :
                     space.status === 'failed' ? '🔴' : '🟡'
  
  console.log(`Space: ${space.space}`)
  console.log(`Customer: ${space.customer}`)
  console.log(`Status: ${statusIcon} ${space.status}`)
  console.log()
  console.log(`Uploads: ${space.completedUploads || 0}/${space.totalUploads || 0}`)
  
  if (space.instanceId) {
    console.log(`Instance: ${space.instanceId}`)
  }
  if (space.workerId) {
    console.log(`Worker: ${space.workerId}`)
  }
  
  console.log()
  console.log(`Created: ${new Date(space.createdAt).toLocaleString()}`)
  console.log(`Updated: ${new Date(space.updatedAt).toLocaleString()}`)
  
  if (space.lastProcessedUpload) {
    console.log(`Last Upload: ${space.lastProcessedUpload}`)
  }
  
  if (space.error) {
    console.log()
    console.log(`Error: ${space.error}`)
  }
  
  console.log()
}

/**
 * Print failed migrations
 */
function printFailedMigrations(failed) {
  console.log()
  console.log('Failed Migrations')
  console.log('='.repeat(70))
  console.log()
  console.log(`Total Failed: ${failed.length}`)
  console.log()
  
  if (failed.length === 0) {
    console.log('No failed migrations found.')
    return
  }
  
  for (const item of failed.slice(0, 20)) {
    console.log(`🔴 ${item.space}`)
    console.log(`  Customer: ${item.customer}`)
    console.log(`  Instance: ${item.instanceId}, Worker: ${item.workerId}`)
    console.log(`  Error: ${item.error || 'Unknown error'}`)
    console.log(`  Updated: ${new Date(item.updatedAt).toLocaleString()}`)
    console.log()
  }
  
  if (failed.length > 20) {
    console.log(`... and ${failed.length - 20} more failed migrations`)
  }
  
  console.log()
}

/**
 * Print stuck migrations
 */
function printStuckMigrations(stuck) {
  console.log()
  console.log('Stuck Migrations (in-progress >1 hour)')
  console.log('='.repeat(70))
  console.log()
  console.log(`Total Stuck: ${stuck.length}`)
  console.log()
  
  if (stuck.length === 0) {
    console.log('No stuck migrations found.')
    return
  }
  
  for (const item of stuck.slice(0, 20)) {
    const stuckDuration = Math.floor((Date.now() - new Date(item.updatedAt).getTime()) / (60 * 1000))
    console.log(`🔵 ${item.space}`)
    console.log(`  Customer: ${item.customer}`)
    console.log(`  Instance: ${item.instanceId}, Worker: ${item.workerId}`)
    console.log(`  Stuck for: ${stuckDuration} minutes`)
    console.log(`  Progress: ${item.completedUploads}/${item.totalUploads} uploads`)
    console.log(`  Last Update: ${new Date(item.updatedAt).toLocaleString()}`)
    console.log()
  }
  
  if (stuck.length > 20) {
    console.log(`... and ${stuck.length - 20} more stuck migrations`)
  }
  
  console.log()
}

/**
 * Print instance progress
 */
function printInstanceProgress(instanceId, spaces) {
  console.log()
  console.log(`Instance ${instanceId} Progress`)
  console.log('='.repeat(70))
  console.log()
  
  if (spaces.length === 0) {
    console.log('No migration data found for this instance.')
    return
  }
  
  const completed = spaces.filter(s => s.status === 'completed').length
  const inProgress = spaces.filter(s => s.status === 'in-progress').length
  const failed = spaces.filter(s => s.status === 'failed').length
  const pending = spaces.filter(s => s.status === 'pending').length
  
  const totalUploads = spaces.reduce((sum, s) => sum + (s.totalUploads || 0), 0)
  const completedUploads = spaces.reduce((sum, s) => sum + (s.completedUploads || 0), 0)
  
  console.log(`Total Spaces: ${spaces.length}`)
  console.log(`  🟢 Completed: ${completed}`)
  console.log(`  🔵 In Progress: ${inProgress}`)
  console.log(`  🟡 Pending: ${pending}`)
  console.log(`  🔴 Failed: ${failed}`)
  console.log()
  console.log(`Total Uploads: ${totalUploads.toLocaleString()}`)
  console.log(`  🟢 Completed: ${completedUploads.toLocaleString()}`)
  console.log()
  
  // Group by worker
  const byWorker = {}
  for (const space of spaces) {
    if (space.workerId) {
      if (!byWorker[space.workerId]) {
        byWorker[space.workerId] = { total: 0, completed: 0, inProgress: 0, failed: 0 }
      }
      byWorker[space.workerId].total++
      if (space.status === 'completed') byWorker[space.workerId].completed++
      if (space.status === 'in-progress') byWorker[space.workerId].inProgress++
      if (space.status === 'failed') byWorker[space.workerId].failed++
    }
  }
  
  if (Object.keys(byWorker).length > 0) {
    console.log('Progress by Worker:')
    console.log('-'.repeat(70))
    
    for (const [workerId, stats] of Object.entries(byWorker).sort()) {
      console.log(`  Worker ${workerId}: ${stats.completed}/${stats.total} completed`)
      if (stats.inProgress > 0) console.log(`    In Progress: ${stats.inProgress}`)
      if (stats.failed > 0) console.log(`    Failed: ${stats.failed}`)
    }
  }
  
  console.log()
}

/**
 * Main function
 */
async function main() {
  const { values } = parseArgs({
    options: {
      customer: {
        type: 'string',
        description: 'Query specific customer DID',
      },
      space: {
        type: 'string',
        description: 'Query specific space DID (requires --customer)',
      },
      instance: {
        type: 'string',
        description: 'Show progress for specific instance',
      },
      failed: {
        type: 'boolean',
        default: false,
        description: 'Show failed migrations',
      },
      stuck: {
        type: 'boolean',
        default: false,
        description: 'Show stuck migrations (>1 hour)',
      },
      watch: {
        type: 'boolean',
        default: false,
        description: 'Live monitoring (refresh every 30s)',
      },
    },
  })

  validateConfig()

  const runQuery = async () => {
    try {
      // Specific queries
      if (values.customer && values.space) {
        const space = await getSpaceProgress(values.customer, values.space)
        printSpaceStatus(space)
      } else if (values.customer) {
        const spaces = await getCustomerSpaces(values.customer)
        printCustomerProgress(values.customer, spaces)
      } else if (values.instance) {
        const spaces = await getInstanceSpaces(values.instance)
        printInstanceProgress(values.instance, spaces)
      } else if (values.failed) {
        const failed = await getFailedMigrations()
        printFailedMigrations(failed)
      } else if (values.stuck) {
        const stuck = await getStuckMigrations()
        printStuckMigrations(stuck)
      } else {
        // Default: overall stats
        const stats = await getOverallStats()
        printOverallStats(stats)
      }
    } catch (error) {
      console.error('Error querying migration progress:', error)
      process.exit(1)
    }
  }

  // Run once or watch
  if (values.watch) {
    console.log('Starting live monitoring (Ctrl+C to stop)...')
    while (true) {
      console.clear()
      await runQuery()
      console.log('Refreshing in 30 seconds...')
      await new Promise(resolve => setTimeout(resolve, 30000))
    }
  } else {
    await runQuery()
  }
}

main()
