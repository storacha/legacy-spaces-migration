#!/usr/bin/env node
/**
 * Main migration script for legacy content
 * 
 * Orchestrates the complete migration workflow:
 * 1. [x] Query uploads needing migration
 * 2. [x] Check what migration steps are needed
 * 3. [x] Generate DAG indices (using index worker)
 * 4. [x] Upload and register indices
 * 5. [x] Republish location claims with space
 * 6. [x] Create gateway authorizations
 * 7. [x] Verify migration completed successfully
 * 
 * Usage Examples:
 * 
 *   # Full migration (all steps):
 *   node src/migrate.js --limit 10
 * 
 *   # Test index generation only:
 *   node src/migrate.js --test-index --limit 10
 * 
 *   # Test location claims only:
 *   node src/migrate.js --test-location-claims --limit 10
 * 
 *   # Test gateway auth only:
 *   node src/migrate.js --test-gateway-auth --limit 10
 * 
 *   # Verify migration only (no changes):
 *   node src/migrate.js --verify-only --limit 10
 *   node src/migrate.js --verify-only --space did:key:z6Mk...
 *   node src/migrate.js --verify-only --customer did:mailto:...
 * 
 *   # Migrate specific space:
 *   node src/migrate.js --space did:key:z6Mk... --limit 50
 * 
 *   # Migrate using customer list from file:
 *   node src/migrate.js --customers-file migration-state/instance-1-customers.json --limit 100
 *   node src/migrate.js --customers-file /path/to/customers.json --limit 100
 */
import dotenv from 'dotenv'
dotenv.config()
import { parseArgs } from 'node:util'
import { readFile } from 'fs/promises'
import { validateConfig, config } from './config.js'
import { sampleUploads } from './lib/tables/upload-table.js'
import { 
  checkMigrationNeeded,
  buildAndMigrateIndex,
  republishLocationClaims,
  createGatewayAuth,
  registerIndex,
} from './lib/migration-steps.js'
import { verifyMigration } from './lib/migration-verify.js'
import { CID } from 'multiformats/cid'

/**
 * Load customer list from a JSON file
 * 
 * @param {string} filepath - Path to the customers JSON file
 * @returns {Promise<Array<string>>} - Array of customer DIDs
 */
async function loadCustomersFromFile(filepath) {
  try {
    const data = await readFile(filepath, 'utf-8')
    const json = JSON.parse(data)
    
    if (!json.customers || !Array.isArray(json.customers)) {
      throw new Error(`Invalid file format: missing customers array`)
    }
    
    console.log(`Loaded ${json.customers.length} customers from ${filepath}`)
    console.log(`  Estimated uploads: ${json.estimatedUploads?.toLocaleString() || 'unknown'}`)
    console.log(`  Estimated spaces: ${json.estimatedSpaces?.toLocaleString() || 'unknown'}`)
    console.log()
    
    return json.customers
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error(`Customer file not found: ${filepath}`)
    }
    throw error
  }
}

/**
 * Migrate a single upload through all required steps
 * 
 * @param {object} upload - Upload from Upload Table
 * @param {object} options - Migration options
 * @param {string} options.testMode - Test mode: 'index' | 'location-claims' | 'gateway-auth' | null (null = full migration)
 * @param {boolean} options.verifyOnly - If true, only verify migration status without making changes
 */
async function migrateUpload(upload, options = {}) {
  console.log(`\n${'='.repeat(70)}`)
  console.log(`📦 Migrating Upload: ${upload.root}`)
  console.log(`   Space: ${upload.space}`)
  console.log(`   Shards: ${upload.shards.length}`)
  console.log('='.repeat(70))
  
  try {
    // If verify-only mode, skip to verification
    if (options.verifyOnly) {
      console.log(`\n>>> Verify-Only Mode: Checking migration status...`)
      const verificationResult = await verifyMigration({ upload })
      
      return {
        success: verificationResult.success,
        verifyOnly: true,
        upload: upload.root,
        space: upload.space,
        verification: verificationResult,
      }
    }
    
    // Step 1: Check what migration steps are needed
    console.log(`\n1) Checking migration status...`)
    const status = await checkMigrationNeeded(upload)
    
    console.log(`\n::: Migration Status:::`)
    console.log(`   Index: ${status.hasIndexClaim ? '✓ EXISTS' : '✗ MISSING'}`)
    console.log(`   Location claims: ${status.hasLocationClaim ? '✓ EXISTS' : '✗ MISSING'}`)
    if (status.hasLocationClaim) {
      console.log(`   Location has space: ${status.locationHasSpace ? '✓ YES' : '✗ NO'}`)
    }
    console.log(`   Shards needing location claims: ${status.shardsNeedingLocationClaims.length}/${upload.shards.length}`)
    
    console.log(`\n!!! Actions Required!!!`)
    console.log(`   ${status.needsIndexGeneration ? '☐' : '✓'} Generate and register index`)
    console.log(`   ${status.needsLocationClaims ? '☐' : '✓'} Republish location claims with space`)
    console.log(`   ${status.needsGatewayAuth ? '☐' : '✓'} Create gateway authorization`)
    
    // If nothing needs to be done, we're done!
    if (!status.needsIndexGeneration && 
        !status.needsLocationClaims && 
        !status.needsGatewayAuth) {
      console.log(`\n✅ Upload already fully migrated!`)
      return { 
        success: true, 
        alreadyMigrated: true,
        status 
      }
    }
    
    let shardsWithSizes = null
    let migrationSpace = null
    let indexCID = status.indexCID
    
    // Step 2: Build and register index
    const shouldRunIndex = !options.testMode || options.testMode === 'index'
    
    if (status.needsIndexGeneration && shouldRunIndex) {
      console.log(`\n2) Building and registering index...`)
      const result = await buildAndMigrateIndex({ upload })
      shardsWithSizes = result.shards
      migrationSpace = result.migrationSpace
      indexCID = result.indexCID
      console.log(`   ✅ Index created: ${indexCID}`)
      console.log(`   ✅ Migration space: ${migrationSpace}`)
      
      // If test mode, stop here
      if (options.testMode === 'index') {
        console.log(`\n⏸ Test mode: Index only`)
        return {
          success: true,
          testMode: 'index',
          upload: upload.root,
          space: upload.space,
          migrationSpace,
          indexCID: indexCID?.toString(),
          status,
        }
      }
    } else if (status.needsIndexGeneration) {
      console.log(`\n⏭  Skipping index generation (test mode: ${options.testMode})`)
    } else {
      console.log(`\n⏭  Index already exists, skipping`)
    }
    
    // Step 3: Republish location claims
    const shouldRunLocationClaims = !options.testMode || options.testMode === 'location-claims'
    
    if (status.needsLocationClaims && shouldRunLocationClaims) {
      console.log(`\n3)  Republishing location claims with space...`)
      
      // Use the original user's space for location claims (for egress billing)
      await republishLocationClaims({
        space: upload.space,  // Original space, not migration space
        shards: status.shardsNeedingLocationClaims,
        shardsWithSizes, // Reuse from step 2 if available
      })
      console.log(`   ✅ Location claims republished for ${status.shardsNeedingLocationClaims.length} shards`)
      
      // Note: We don't need to re-register the index. The indexing service will
      // automatically pick up the new location claims we just published.
      // Re-registering would require uploading the index blob again to the migration space.
      
      // If test mode, stop here
      if (options.testMode === 'location-claims') {
        console.log(`\n⏸ Test mode: Location claims only`)
        return {
          success: true,
          testMode: 'location-claims',
          upload: upload.root,
          space: upload.space,
          shardsRepublished: status.shardsNeedingLocationClaims.length,
          status,
        }
      }
    } else if (status.needsLocationClaims) {
      console.log(`\n⏭  Skipping location claims (test mode: ${options.testMode})`)
    } else {
      console.log(`\n⏭  Location claims already have space, skipping`)
    }
    
    // Step 4: Create gateway authorization
    const shouldRunGatewayAuth = !options.testMode || options.testMode === 'gateway-auth'
    
    if (status.needsGatewayAuth && shouldRunGatewayAuth) {
      console.log(`\n4)  Creating gateway authorization...`)
      await createGatewayAuth({
        space: upload.space,
      })
      console.log(`   ✅ Gateway authorization created (or already exists)`)
      
      // If test mode, stop here
      if (options.testMode === 'gateway-auth') {
        console.log(`\n⏸  Test mode: Gateway auth only`)
        return {
          success: true,
          testMode: 'gateway-auth',
          upload: upload.root,
          space: upload.space,
          status,
        }
      }
    } else if (status.needsGatewayAuth) {
      console.log(`\n⏭  Skipping gateway auth (test mode: ${options.testMode})`)
    } else {
      console.log(`\n⏭  Gateway authorization already exists, skipping`)
    }
    
    // Step 5: Verify migration completed successfully
    console.log(`\n5) Verifying migration...`)
    const verificationResult = await verifyMigration({ upload })
    
    if (!verificationResult.success) {
      console.error(`\n⚠️  Verification failed: ${verificationResult.details}`)
      console.error(`   Migration may need to be retried`)
    }
    
    console.log(`\n${'='.repeat(70)}`)
    console.log(`${verificationResult.success ? '✅' : '⚠️'}  Migration ${verificationResult.success ? 'complete' : 'completed with issues'} for ${upload.root}`)
    console.log('='.repeat(70))
    
    return {
      success: verificationResult.success,
      upload: upload.root,
      space: upload.space,
      migrationSpace,
      indexCID: indexCID?.toString(),
      shardsRepublished: status.shardsNeedingLocationClaims.length,
      status,
      verification: verificationResult,
    }
    
  } catch (error) {
    console.error(`\n❌ Migration failed for ${upload.root}:`, error.message)
    console.error(error.stack)
    
    return {
      success: false,
      upload: upload.root,
      space: upload.space,
      error: error.message,
    }
  }
}

/**
 * Run migration mode - process multiple uploads
 */
async function runMigrationMode(values) {
  // Determine test mode
  let testMode = null
  let modeLabel = 'Full Migration'
  const verifyOnly = values['verify-only'] || false
  
  if (verifyOnly) {
    modeLabel = 'Verification Only'
  } else if (values['test-index']) {
    testMode = 'index'
    modeLabel = 'Index Generation Only'
  } else if (values['test-location-claims']) {
    testMode = 'location-claims'
    modeLabel = 'Location Claims Only'
  } else if (values['test-gateway-auth']) {
    testMode = 'gateway-auth'
    modeLabel = 'Gateway Auth Only'
  }
  
  console.log(`Legacy Content Migration - ${modeLabel}`)
  console.log('='.repeat(70))
  console.log()
  
  const limit = parseInt(values.limit || '10', 10)
  const concurrency = parseInt(values.concurrency || '1', 10)
  
  // Load customers from file if --customers-file is provided
  let customers = null
  if (values['customers-file']) {
    customers = await loadCustomersFromFile(values['customers-file'])
  }
  
  console.log('Configuration:')
  console.log(`  Mode: ${modeLabel}`)
  console.log(`  Limit: ${limit} uploads`)
  console.log(`  Concurrency: ${concurrency}`)
  if (values['customers-file']) console.log(`  Customers file: ${values['customers-file']} (${customers.length} customers)`)
  if (values.space) console.log(`  Space filter: ${values.space}`)
  if (values.customer) console.log(`  Customer filter: ${values.customer}`)
  console.log()
  
  const results = []
  let processed = 0
  
  // Process uploads
  // If --customers-file is provided, filter uploads by customers in the file
  // Otherwise, sample from all uploads (existing behavior)
  const customerSet = customers ? new Set(customers) : null
  
  if (customerSet) {
    console.log(`Filtering uploads by ${customerSet.size} customers from file...`)
    console.log()
  }
  
  // If --cid is provided, fetch and process that single upload
  if (values.cid) {
    console.log(`Fetching specific upload: ${values.cid}`)
    if (values.space) {
      console.log(`  Using space filter: ${values.space}`)
    }
    console.log()
    
    let upload
    if (values.space) {
      // If space is provided, use primary index for accurate data
      const { getUpload } = await import('./lib/tables/upload-table.js')
      upload = await getUpload(values.space, values.cid)
    } else {
      console.error(`❌ Space not provided`)
      process.exit(1)
    }
    
    if (!upload) {
      console.error(`❌ Upload not found: ${values.cid}`)
      process.exit(1)
    }
    
    const result = await migrateUpload(upload, {
      testMode,
      verifyOnly,
    })
    
    results.push(result)
  } else {
    // Import getCustomerForSpace to check upload ownership
    const { getCustomerForSpace } = await import('./lib/tables/consumer-table.js')
    
    for await (const upload of sampleUploads({ 
      limit: limit * 10, // Sample more to account for filtering
      space: values.space,
    })) {
    // If instance mode, check if upload belongs to one of our customers
    if (customerSet) {
      const customer = await getCustomerForSpace(upload.space)
      if (!customer || !customerSet.has(customer)) {
        continue // Skip uploads not belonging to our customers
      }
    }
    
    processed++
    console.log(`\n[${processed}/${limit}]`)
    
    const result = await migrateUpload(upload, {
      testMode,
      verifyOnly,
    })
    
    results.push(result)
    
    if (processed >= limit) {
      break
    }
    
      // Small delay between uploads to avoid overwhelming services
      await new Promise(resolve => setTimeout(resolve, 100))
    }
  }
  
  // Print summary
  console.log()
  console.log('='.repeat(70))
  console.log('Migration Summary')
  console.log('='.repeat(70))
  
  const successful = results.filter(r => r.success).length
  const failed = results.filter(r => !r.success).length
  const alreadyMigrated = results.filter(r => r.alreadyMigrated).length
  const testModeResults = results.filter(r => r.testMode).length
  const verifyOnlyResults = results.filter(r => r.verifyOnly).length
  
  console.log(`Total processed: ${results.length}`)
  console.log(`Successful: ${successful}`)
  console.log(`Already migrated: ${alreadyMigrated}`)
  if (verifyOnly) {
    console.log(`Verified: ${verifyOnlyResults}`)
    console.log(`Verification passed: ${results.filter(r => r.verifyOnly && r.success).length}`)
    console.log(`Verification failed: ${results.filter(r => r.verifyOnly && !r.success).length}`)
  } else if (testMode) {
    console.log(`Test mode (${testMode}): ${testModeResults}`)
  }
  console.log(`Failed: ${failed}`)
  
  if (failed > 0) {
    console.log()
    console.log('Failed uploads:')
    results.filter(r => !r.success).forEach(r => {
      console.log(`  ❌ ${r.upload}: ${r.error}`)
    })
  }
  
  // Save results to file
  if (values.output) {
    const fs = await import('fs/promises')
    await fs.writeFile(values.output, JSON.stringify(results, null, 2))
    console.log()
    console.log(`Results saved to: ${values.output}`)
  }
  
  console.log('='.repeat(70))
}

/**
 * Main function
 */
async function main() {
  const { values } = parseArgs({
    options: {
      'test-index': {
        type: 'boolean',
        default: false,
        description: 'Test mode: Only test index generation',
      },
      'test-location-claims': {
        type: 'boolean',
        default: false,
        description: 'Test mode: Only test location claims republishing',
      },
      'test-gateway-auth': {
        type: 'boolean',
        default: false,
        description: 'Test mode: Only test gateway authorization',
      },
      'verify-only': {
        type: 'boolean',
        default: false,
        description: 'Verify migration status without making changes',
      },
      cid: {
        type: 'string',
        description: 'Specific upload CID',
      },
      limit: {
        type: 'string',
        short: 'l',
        default: '10',
        description: 'Number of uploads to process',
      },
      space: {
        type: 'string',
        short: 's',
        description: 'Filter by space DID',
      },
      customer: {
        type: 'string',
        short: 'c',
        description: 'Filter by customer DID',
      },
      'customers-file': {
        type: 'string',
        description: 'Path to customers JSON file (filters migration to these customers)',
      },
      concurrency: {
        type: 'string',
        default: '1',
        description: 'Number of concurrent migrations',
      },
      output: {
        type: 'string',
        short: 'o',
        default: 'migration-results.json',
        description: 'Output file for results',
      },
    },
  })
  
  validateConfig()
  
  // Display environment information
  console.log()
  console.log('Environment Configuration')
  console.log('='.repeat(50))
  console.log(`  Environment: ${config.environment}`)
  console.log(`  AWS Region: ${config.aws.region}`)
  console.log(`  Upload Service: ${config.services.uploadService}`)
  console.log(`  Upload Table: ${config.tables.upload}`)
  console.log('='.repeat(50))
  console.log()
  
  // Run migration mode (handles all cases including test modes)
  await runMigrationMode(values)
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
