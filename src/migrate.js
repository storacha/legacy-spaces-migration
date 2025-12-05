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
 *   # Sample migration (default: 10 uploads):
 *   node src/migrate.js
 *   node src/migrate.js --limit 100
 *
 *   # Migrate all uploads for a space (unlimited by default):
 *   node src/migrate.js --space did:key:z6Mk...
 *   node src/migrate.js --space did:key:z6Mk... --limit 50
 *
 *   # Migrate all uploads for a customer (unlimited by default):
 *   node src/migrate.js --customer did:mailto:...
 *   node src/migrate.js --customer did:mailto:... --concurrency 5
 *
 *   # Migrate using customer list from file (unlimited by default):
 *   node src/migrate.js --customers-file customers.json
 *   node src/migrate.js --customers-file customers.json --limit 1000
 *
 *   # Test modes (limit to 10 for testing):
 *   node src/migrate.js --test-index --limit 10
 *   node src/migrate.js --test-location-claims --limit 10
 *   node src/migrate.js --test-gateway-auth --limit 10
 *
 *   # Verify migration only (no changes):
 *   node src/migrate.js --verify-only --space did:key:z6Mk...
 *   node src/migrate.js --verify-only --customer did:mailto:...
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
} from './lib/migration-steps.js'
import { verifyMigration } from './lib/migration-verify.js'
import { getErrorMessage } from './lib/error-utils.js'
import { SpaceDID } from '@storacha/capabilities/utils'
import { getCustomerForSpace } from './lib/tables/consumer-table.js'
import { getOrCreateMigrationSpaceForCustomer } from './lib/migration-utils.js'
import { getUpload } from './lib/tables/upload-table.js'

/**
 * Load customers from a JSON file
 * File should contain an array of customer DIDs
*
 * @param {string} filePath - Path to customers file
 * @returns {Promise<string[]>} - Array of customer DIDs
 */
async function loadCustomersFromFile(filePath) {
  const content = await readFile(filePath, 'utf-8')
  const data = JSON.parse(content)

  if (!Array.isArray(data)) {
    throw new Error('Customers file must contain an array of customer DIDs')
  }

  return data
}

/**
 * Resolve target spaces based on filter options
 * 
 * @param {object} options
 * @param {string} [options.space] - Single space DID
 * @param {string} [options.customer] - Single customer DID
 * @param {string[]} [options.customers] - Array of customer DIDs from file
 * @returns {Promise<string[] | undefined>} - Array of space DIDs to migrate, or undefined for no filter
 */
async function resolveTargetSpaces({ space, customer, customers }) {
  const { getSpacesForCustomer } = await import('./lib/tables/consumer-table.js')
  
  // Single space mode
  if (space) {
    return [space]
  }
  
  // Customer mode - build customer set from both --customer and --customers-file
  const customerSet = new Set()
  if (customer) {
    customerSet.add(customer)
  }
  if (customers) {
    customers.forEach(c => customerSet.add(c))
  }
  
  if (customerSet.size > 0) {
    console.log(`Loading spaces for ${customerSet.size} customer(s)...`)
    const allSpaces = []
    
    for (const customerDID of customerSet) {
      const customerSpaces = await getSpacesForCustomer(customerDID)
      console.log(`  ${customerDID}: ${customerSpaces.length} spaces`)
      allSpaces.push(...customerSpaces)
    }
    
    console.log(`Total spaces to migrate: ${allSpaces.length}`)
    console.log()
    
    return allSpaces
  }
  
  // No filter - sample from all uploads
  return undefined
}

/**
 * Migrate a single upload through all required steps
 *
 * @param {{space: string, root: string, shards: string[]}} upload - Upload from Upload Table
 * @param {object} options - Migration options
 * @param {string} [options.testMode] - Test mode: 'index' | 'location-claims' | 'gateway-auth' | null (null = full migration)
 * @param {boolean} [options.verifyOnly] - If true, only verify migration status without making changes
 * @param {number} [options.uploadNumber] - Current upload number (for logging)
 * @param {number} [options.totalUploads] - Total uploads being processed (for logging)
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
  console.log(`Space:  ${upload.space}`)
  console.log(`Root:   ${upload.root}`)
  console.log(`Shards: ${upload.shards.length}`)

  // If upload.shards is empty, extract original content shards from the index
  if (upload.shards.length === 0) {
    console.log(`\n⚠️  Upload has no shards in database`)
    const { queryIndexingService } = await import('./lib/indexing-service.js')
    const { CID } = await import('multiformats/cid')

    const indexingData = await queryIndexingService(upload.root)

    if (indexingData.hasIndexClaim) {
      console.log(`   ✓ Index claim exists`)
      console.log(`   Extracting original content shards from index...`)

      // Get the index from the query result
      const indexes = Array.from(indexingData.indexes.values() || [])

      if (indexes.length === 0) {
        console.log(`   ✗ No index data returned`)
        console.log(`   ⏭  Skipping shard resolution`)
      } else {
        // Extract shard digests from the index
        const index = indexes[0]
        const shardDigests = Array.from(index.shards.keys())

        // Convert digests to CIDs
        const shardCIDs = shardDigests.map((digest) => {
          // Shards are stored as multihashes in the index
          return CID.createV1(0x55, digest).toString()
        })

        upload.shards = shardCIDs
        console.log(
          `   ✓ Extracted ${shardCIDs.length} original content shard(s) from index`
        )
      }
    } else {
      console.log(`   ✗ No index claim found`)
      console.log(`   ❌ Cannot migrate: upload has no shards and no index`)
      throw new Error(
        'Upload has no shards in database and no index claim - cannot migrate'
      )
    }
  }

  try {
    // If verify-only mode, skip to verification
    if (options.verifyOnly) {
      console.log(`\n>>> Verify-Only Mode: Checking migration status...`)
      // Pass null for gatewayAuthResult to indicate it should be skipped in verify-only mode
      const verificationResult = await verifyMigration({
        upload,
        gatewayAuthResult: null, // null = skip gateway auth check
      })

      return {
        success: verificationResult.success,
        verifyOnly: true,
        upload: upload.root,
        space: upload.space,
        verification: verificationResult,
        error: verificationResult.success
          ? undefined
          : verificationResult.details,
      }
    }

    // Step 1: Check what migration steps are needed
    console.log(`\nSTEP 1: Analyze Migration Status ${'─'.repeat(35)}`)
    const status = await checkMigrationNeeded(upload)

    console.log(
      `  Index claim:           ${
        status.hasIndexClaim ? '✓ exists' : '✗ missing'
      }`
    )
    console.log(
      `  Location claims:       ${
        status.hasLocationClaim
          ? `✓ exists (${upload.shards.length} shards)`
          : '✗ missing'
      }`
    )
    if (status.hasLocationClaim) {
      console.log(
        `  Space information:     ${
          status.locationHasSpace ? '✓ present' : '✗ missing'
        }`
      )
    }

    console.log(`\n  Actions needed:`)
    console.log(
      `    ${
        status.needsIndexGeneration ? '☐' : '✓'
      } Index generation and registration`
    )
    console.log(
      `    ${
        status.needsLocationClaims ? '☐' : '✓'
      } Location claims with space information`
    )
    console.log(
      `    ${status.needsGatewayAuth ? '☐' : '✓'} Gateway authorization`
    )
    console.log(`\n  Result: ✓ COMPLETE`)

    // If nothing needs to be done, we're done!
    if (
      !status.needsIndexGeneration &&
      !status.needsLocationClaims &&
      !status.needsGatewayAuth
    ) {
      console.log(`\n✅ Upload already fully migrated!`)
      return {
        success: true,
        alreadyMigrated: true,
        status,
      }
    }

    let shardsWithSizes = null
    let migrationSpace = null
    let indexCID = status.indexCID

    // Step 2: Build and register index
    const shouldRunIndex = !options.testMode || options.testMode === 'index'

    console.log(`\nSTEP 2: Generate and Register Index ${'─'.repeat(33)}`)
    if (status.needsIndexGeneration && shouldRunIndex) {
      const result = await buildAndMigrateIndex({ upload,  })
      shardsWithSizes = result.shards
      migrationSpace = result.migrationSpace
      indexCID = result.indexCID
      console.log(
        `  Building index:        ✓ complete (${upload.shards.length} entries)`
      )
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

    // Ensure migrationSpace is available for subsequent steps even if Step 2 was skipped
    if (!migrationSpace && (status.needsLocationClaims || status.needsGatewayAuth)) {
      const customer = await getCustomerForSpace(upload.space)
      if (customer) {
        const res = await getOrCreateMigrationSpaceForCustomer(customer)
        migrationSpace = res.space
      }
    }

    // Step 3: Republish location claims
    const shouldRunLocationClaims =
      !options.testMode || options.testMode === 'location-claims'

    console.log(`\nSTEP 3: Republish Location Claims ${'─'.repeat(34)}`)
    if (status.needsLocationClaims && shouldRunLocationClaims) {
      console.log(
        `  Shards to republish:   ${status.shardsNeedingLocationClaims.length}`
      )

      // Use the MIGRATION SPACE for location claims so Freeway authorizes it!
      // (Legacy spaces cannot be authorized easily)
      if (!migrationSpace) {
        throw new Error('Migration space not available for location claims')
      }

      await republishLocationClaims({
        space: SpaceDID.from(upload.space), 
        root: upload.root,
        shards: status.shardsNeedingLocationClaims,
        shardsWithSizes, // Reuse from step 2 if available
      })
      console.log(
        `  Publishing claims:     ✓ complete (${status.shardsNeedingLocationClaims.length}/${status.shardsNeedingLocationClaims.length})`
      )
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
    const shouldRunGatewayAuth =
      !options.testMode || options.testMode === 'gateway-auth'
    let gatewayAuthResult = null

    console.log(`\nSTEP 4: Create Gateway Authorization ${'─'.repeat(32)}`)
    if (status.needsGatewayAuth && shouldRunGatewayAuth) {
      // Use Migration Space for gateway auth
      if (!migrationSpace) {
         throw new Error('Migration space not available for gateway auth')
      }
      gatewayAuthResult = await createGatewayAuth({
        space: SpaceDID.from(upload.space),
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
    const verificationResult = await verifyMigration({
      upload,
      gatewayAuthResult,
    })

    return {
      success: verificationResult.success,
      upload: upload.root,
      space: upload.space,
      migrationSpace,
      indexCID: indexCID?.toString(),
      shardsRepublished: status.shardsNeedingLocationClaims.length,
      status,
      verification: verificationResult,
      error: verificationResult.success
        ? undefined
        : verificationResult.details,
    }
  } catch (error) {
    console.error(`\n❌ Migration failed for ${upload.root}:`, getErrorMessage(error))
    if (error instanceof Error) {
      console.error(error.stack)
    }

    return {
      success: false,
      upload: upload.root,
      space: upload.space,
      error: getErrorMessage(error),
    }
  }
}

/**
 * Run migration mode - process multiple uploads
 * @param {Record<string, any>} values - Parsed arguments
 */
async function runMigrationMode(values) {
  // Determine test mode
  let testMode
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

  // Default limit: 10 for sampling, unlimited for space/customer filtering
  const hasFilter = values.space || values.customer || values['customers-file']
  const limit = values.limit ? parseInt(values.limit, 10) : (hasFilter ? Infinity : 10)
  const concurrency = parseInt(values.concurrency || '1', 10)

  // Load customers from file if --customers-file is provided
  /** @type {string[] | undefined} */
  let customers = undefined
  if (values['customers-file']) {
    customers = await loadCustomersFromFile(values['customers-file'])
  }

  console.log('Configuration:')
  console.log(`  Mode: ${modeLabel}`)
  console.log(`  Limit: ${limit === Infinity ? 'unlimited' : `${limit} uploads`}`)
  console.log(`  Concurrency: ${concurrency}`)
  if (values['customers-file'])
    console.log(
      `  Customers file: ${values['customers-file']} (${customers?.length} customers)`
    )
  if (values.space) console.log(`  Space filter: ${values.space}`)
  if (values.customer) console.log(`  Customer filter: ${values.customer}`)
  console.log()

  const results = []
  let processed = 0
  const processedSpaces = new Set()
  const spacesWithFailures = new Set()

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
    // Resolve target spaces based on filter options
    const targetSpaces = await resolveTargetSpaces({
      space: values.space,
      customer: values.customer,
      customers,
    })

    // Process uploads from target spaces
    if (targetSpaces && targetSpaces.length > 0) {
      // Iterate through each space to sample uploads based on the specified limit
      for (const space of targetSpaces) {
        for await (const upload of sampleUploads({
          limit: limit,
          space: space,
        })) {
          processed++
          processedSpaces.add(space)

          const result = await migrateUpload(upload, {
            testMode,
            verifyOnly,
            uploadNumber: processed,
            totalUploads: limit,
          })

          if (!result.success) {
            spacesWithFailures.add(space)
          }

          results.push(result)

          if (limit !== Infinity && processed >= limit) {
            break
          }

          // Small delay between uploads to avoid overwhelming services
          await new Promise((resolve) => setTimeout(resolve, 100))
        }
        
        if (limit !== Infinity && processed >= limit) {
          break
        }
      }
    } else {
      console.log("No target spaces found. Exiting.")
      process.exit(0)
    }
  }

  // Calculate statistics
  const successful = results.filter((r) => r.success).length
  const failed = results.filter((r) => !r.success).length
  const alreadyMigrated = results.filter((r) => r.alreadyMigrated).length

  const successRate =
    results.length > 0 ? Math.round((successful / results.length) * 100) : 0
  const failureRate =
    results.length > 0 ? Math.round((failed / results.length) * 100) : 0
  
  const spacesWithFailuresRate =
    processedSpaces.size > 0 ? Math.round((spacesWithFailures.size / processedSpaces.size) * 100) : 0
  const spacesWithoutFailuresRate =
    processedSpaces.size > 0 ? Math.round(((processedSpaces.size - spacesWithFailures.size) / processedSpaces.size) * 100) : 0

  // Print failures first
  if (failed > 0) {
    console.log()
    console.log(`Failed Uploads (${failed}):`)
    
    // Group failures by space
    /** @type {Map<string, Array<{upload: string, error: string}>>} */
    const failuresBySpace = new Map()
    
    results
      .filter((r) => !r.success)
      .forEach((r) => {
        const space = r.space || 'unknown'
        if (!failuresBySpace.has(space)) {
          failuresBySpace.set(space, [])
        }
        const failures = failuresBySpace.get(space)
        if (failures) {
          failures.push({
            upload: r.upload || 'unknown',
            error: r.error || 'unknown error'
          })
        }
      })
    
    // Print failures grouped by space
    for (const [space, failures] of failuresBySpace) {
      console.log(`\n  Space: ${space}`)
      failures.forEach(({ upload, error }) => {
        console.log(`    ✗ ${upload}`)
        console.log(`      └─ ${error}`)
      })
    }

    // Step failure breakdown
    /** @type {Record<string, number>} */
    const stepFailures = {}
    results
      .filter((r) => !r.success && r.verification)
      .forEach((r) => {
        const details = r.verification?.details || r.error
        if (details) {
          stepFailures[details] = (stepFailures[details] || 0) + 1
        }
      })

    if (Object.keys(stepFailures).length > 0) {
      console.log()
      console.log('Failure Breakdown:')
      Object.entries(stepFailures).forEach(([step, count]) => {
        const percentage = Math.round((count / failed) * 100)
        console.log(
          `  ${step}: ${count} upload(s) (${percentage}% of failures)`
        )
      })
    }
  }

  // Print summary after failures
  console.log()
  console.log('━'.repeat(70))
  console.log('📈 MIGRATION SUMMARY')
  console.log('━'.repeat(70))

  console.log(`\nSpaces:`)
  console.log(`  Total processed:     ${processedSpaces.size}`)
  if (spacesWithFailures.size > 0) {
    console.log(`  With failures:       ${spacesWithFailures.size} (${spacesWithFailuresRate}%)`)
    console.log(`  Without failures:    ${processedSpaces.size - spacesWithFailures.size} (${spacesWithoutFailuresRate}%)`)
  }
  
  console.log(`\nUploads:`)
  console.log(`  Total processed:     ${results.length}`)
  if (verifyOnly) {
    console.log(`  ✅ Verified (passed):  ${successful} (${successRate}%)`)
    console.log(`  ❌ Verified (failed):  ${failed} (${failureRate}%)`)
    
    // If all passed, add celebration
    if (failed === 0 && successful > 0) {
      console.log()
      console.log(`  ${'='.repeat(30)}`)
      console.log(`  ✅ ALL VERIFICATIONS PASSED!!!`)
      console.log(`  ${'='.repeat(30)}`)
    }
  } else if (testMode) {
    console.log(`  Mode: Test (${testMode})`)
    console.log(`  ✅ Successful:        ${successful} (${successRate}%)`)
    console.log(`  ❌ Failed:            ${failed} (${failureRate}%)`)
  } else {
    console.log(`  ✅ Migrated:          ${successful} (${successRate}%)`)
    console.log(`  ❌ Failed:            ${failed} (${failureRate}%)`)
    console.log(`  ⏭  Already migrated: ${alreadyMigrated}`)
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
        description: 'Number of uploads to process (default: 10 for sampling, unlimited for filters)',
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
        description:
          'Path to customers JSON file (filters migration to these customers)',
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
  console.log(
    `  Upload Service: ${config.services.uploadServiceURL} (${config.services.uploadServiceDID})`
  )
  console.log(`  Indexer Service: ${config.services.indexingServiceURL}`)
  console.log(
    `  Claims Service: ${config.services.contentClaimsServiceURL} (${config.services.claimsServiceDID})`
  )
  console.log(
    `  Gateway Service: ${config.services.gatewayServiceURL} (${config.services.gatewayServiceDID})`
  )
  console.log(
    `  Storage Providers: ${config.services.storageProviders.join(', ')}`
  )
  console.log('='.repeat(50))
  console.log()

  // Run migration mode (handles all cases including test modes)
  await runMigrationMode(values)
}

main().catch((error) => {
  console.error('Fatal error:', error)
  process.exit(1)
})
