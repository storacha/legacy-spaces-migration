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
  const { uploadNumber, totalUploads } = options
  
  // Add spacing before each upload
  if (uploadNumber && uploadNumber > 1) {
    console.log('\n')
  }
  
  console.log('━'.repeat(70))
  if (uploadNumber && totalUploads) {
    console.log(`[${uploadNumber}/${totalUploads}] MIGRATING UPLOAD`)
  } else {
    console.log(`MIGRATING UPLOAD`)
  }
  console.log('━'.repeat(70))
  console.log(`Root:   ${upload.root}`)
  console.log(`Space:  ${upload.space}`)
  console.log(`Shards: ${upload.shards.length}`)
  
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
        error: verificationResult.success ? undefined : verificationResult.details,
      }
    }
    
    // Step 1: Check what migration steps are needed
    console.log(`\nSTEP 1: Analyze Migration Status ${'─'.repeat(35)}`)
    const status = await checkMigrationNeeded(upload)
    
    console.log(`  Index claim:           ${status.hasIndexClaim ? '✓ exists' : '✗ missing'}`)
    console.log(`  Location claims:       ${status.hasLocationClaim ? `✓ exists (${upload.shards.length} shards)` : '✗ missing'}`)
    if (status.hasLocationClaim) {
      console.log(`  Space information:     ${status.locationHasSpace ? '✓ present' : '✗ missing'}`)
    }
    
    console.log(`\n  Actions needed:`)
    console.log(`    ${status.needsIndexGeneration ? '☐' : '✓'} Index generation and registration`)
    console.log(`    ${status.needsLocationClaims ? '☐' : '✓'} Location claims with space information`)
    console.log(`    ${status.needsGatewayAuth ? '☐' : '✓'} Gateway authorization`)
    console.log(`\n  Result: ✓ COMPLETE`)
    
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
    
    console.log(`\nSTEP 2: Generate and Register Index ${'─'.repeat(33)}`)
    if (status.needsIndexGeneration && shouldRunIndex) {
      const result = await buildAndMigrateIndex({ upload })
      shardsWithSizes = result.shards
      migrationSpace = result.migrationSpace
      indexCID = result.indexCID
      console.log(`  Building index:        ✓ complete (${upload.shards.length} entries)`)
      console.log(`  Uploading blob:        ✓ uploaded`)
      console.log(`  Registering index:     ✓ registered`)
      console.log(`  Index CID:             ${indexCID}`)
      console.log(`\n  Result: ✓ COMPLETE`)
      
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
      console.log(`  Status: ⏭  SKIPPED`)
      console.log(`  Reason: Test mode (${options.testMode})`)
    } else {
      console.log(`  Status: ⏭  SKIPPED`)
      console.log(`  Reason: Index already exists`)
    }
    
    // Step 3: Republish location claims
    const shouldRunLocationClaims = !options.testMode || options.testMode === 'location-claims'
    
    console.log(`\nSTEP 3: Republish Location Claims ${'─'.repeat(34)}`)
    if (status.needsLocationClaims && shouldRunLocationClaims) {
      console.log(`  Shards to republish:   ${status.shardsNeedingLocationClaims.length}`)
      
      // Use the original user's space for location claims (for egress billing)
      await republishLocationClaims({
        space: upload.space,  // Original space, not migration space
        shards: status.shardsNeedingLocationClaims,
        shardsWithSizes, // Reuse from step 2 if available
      })
      console.log(`  Publishing claims:     ✓ complete (${status.shardsNeedingLocationClaims.length}/${status.shardsNeedingLocationClaims.length})`)
      console.log(`\n  Result: ✓ COMPLETE`)
      
      // Note: We don't need to re-register the index. The indexing service will
      // automatically pick up the new location claims we just published.
      
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
      console.log(`  Status: ⏭  SKIPPED`)
      console.log(`  Reason: Test mode (${options.testMode})`)
    } else {
      console.log(`  Status: ⏭  SKIPPED`)
      console.log(`  Reason: Location claims already have space information`)
    }
    
    // Step 4: Create gateway authorization
    const shouldRunGatewayAuth = !options.testMode || options.testMode === 'gateway-auth'
    let gatewayAuthResult = null
    
    console.log(`\nSTEP 4: Create Gateway Authorization ${'─'.repeat(32)}`)
    if (status.needsGatewayAuth && shouldRunGatewayAuth) {
      gatewayAuthResult = await createGatewayAuth({
        space: upload.space,
      })
      
      if (gatewayAuthResult.success) {
        console.log(`\n  Result: ✓ COMPLETE`)
      } else {
        console.log(`\n  Result: ✗ FAILED`)
      }
      
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
      console.log(`  Status: ⏭  SKIPPED`)
      console.log(`  Reason: Test mode (${options.testMode})`)
      // Mark as null so verification knows it was skipped
      gatewayAuthResult = null
    } else {
      console.log(`  Status: ⏭  SKIPPED`)
      console.log(`  Reason: Gateway authorization already exists`)
      // If we're skipping because it already exists, mark it as successful
      gatewayAuthResult = { success: true }
    }
    
    // Step 5: Verify migration completed successfully
    console.log(`\nSTEP 5: Verify Migration ${'─'.repeat(43)}`)
    const verificationResult = await verifyMigration({ upload, gatewayAuthResult })
    
    return {
      success: verificationResult.success,
      upload: upload.root,
      space: upload.space,
      migrationSpace,
      indexCID: indexCID?.toString(),
      shardsRepublished: status.shardsNeedingLocationClaims.length,
      status,
      verification: verificationResult,
      error: verificationResult.success ? undefined : verificationResult.details,
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
    
    const result = await migrateUpload(upload, {
      testMode,
      verifyOnly,
      uploadNumber: processed,
      totalUploads: limit,
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
  console.log('━'.repeat(70))
  console.log('📈 MIGRATION SUMMARY')
  console.log('━'.repeat(70))
  
  const successful = results.filter(r => r.success).length
  const failed = results.filter(r => !r.success).length
  const alreadyMigrated = results.filter(r => r.alreadyMigrated).length
  const testModeResults = results.filter(r => r.testMode).length
  const verifyOnlyResults = results.filter(r => r.verifyOnly).length
  
  const successRate = results.length > 0 ? Math.round((successful / results.length) * 100) : 0
  const failureRate = results.length > 0 ? Math.round((failed / results.length) * 100) : 0
  
  console.log(`Total processed:     ${results.length}`)
  console.log(`✓ Successful:        ${successful} (${successRate}%)`)
  console.log(`✗ Failed:            ${failed} (${failureRate}%)`)
  console.log(`⏭  Already migrated: ${alreadyMigrated}`)
  
  if (verifyOnly) {
    const verifyPassed = results.filter(r => r.verifyOnly && r.success).length
    const verifyFailed = results.filter(r => r.verifyOnly && !r.success).length
    console.log(`\nVerification Results:`)
    console.log(`  Total verified:    ${verifyOnlyResults}`)
    console.log(`  ✓ Passed:          ${verifyPassed}`)
    console.log(`  ✗ Failed:          ${verifyFailed}`)
  } else if (testMode) {
    console.log(`\nTest mode (${testMode}): ${testModeResults}`)
  }
  
  if (failed > 0) {
    console.log()
    console.log(`Failed Uploads (${failed}):`)
    results.filter(r => !r.success).forEach(r => {
      console.log(`  ✗ ${r.upload}`)
      console.log(`    └─ ${r.error}`)
    })
    
    // Step failure breakdown
    const stepFailures = {}
    results.filter(r => !r.success && r.verification).forEach(r => {
      const details = r.verification.details || r.error
      if (details) {
        stepFailures[details] = (stepFailures[details] || 0) + 1
      }
    })
    
    if (Object.keys(stepFailures).length > 0) {
      console.log()
      console.log('Failure Breakdown:')
      Object.entries(stepFailures).forEach(([step, count]) => {
        const percentage = Math.round((count / failed) * 100)
        console.log(`  ${step}: ${count} upload(s) (${percentage}% of failures)`)
      })
    }
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
  console.log(`  Upload Service: ${config.services.uploadServiceURL} (${config.services.uploadServiceDID})`)
  console.log(`  Indexer Service: ${config.services.indexingServiceURL}`)
  console.log(`  Claims Service: ${config.services.contentClaimsServiceURL} (${config.services.claimsServiceDID})`)
  console.log(`  Gateway Service: ${config.services.gatewayServiceURL} (${config.services.gatewayServiceDID})`)
  console.log(`  Storage Providers: ${config.services.storageProviders.join(', ')}`)
  console.log('='.repeat(50))
  console.log()
  
  // Run migration mode (handles all cases including test modes)
  await runMigrationMode(values)
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
