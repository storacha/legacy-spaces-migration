#!/usr/bin/env node
/**
 * Main migration script for legacy content
 * 
 * Orchestrates the complete migration workflow:
 * 1. [x] Query uploads needing migration
 * 2. [x] Check what migration steps are needed
 * 3. [x] Generate DAG indices (using index worker)
 * 4. [ ] Upload and register indices
 * 5. [ ] Republish location claims with space
 * 6. [ ] Create gateway authorizations
 * 
 * Usage:
 *   node src/migrate.js --limit 10
 *   node src/migrate.js --space did:key:z6Mk...
 *   node src/migrate.js --resource bafybei... --limit 10
 *   node src/migrate.js --customer did:key:z6Mk... --limit 100
 *   
 *   # Test index generation only:
 *   node src/migrate.js --test-index --cid bafybei...
 */
import dotenv from 'dotenv'
dotenv.config()
import { parseArgs } from 'node:util'
import { validateConfig } from './config.js'
import { sampleUploads, getUploadByRoot } from './lib/tables/upload-table.js'
import { generateDAGIndex } from './lib/migration-steps.js'
import { base58btc } from 'multiformats/bases/base58'

/**
 * Test index generation for a single upload
 */
async function testIndexGeneration(upload) {
  console.log()
  console.log('Testing Index Generation')
  console.log('='.repeat(50))
  console.log(`Root CID (Upload): ${upload.root}`)
  console.log(`Space: ${upload.space}`)
  console.log(`Shards: ${upload.shards.length}`)
  console.log()
  
  try {
    const { indexBytes, indexCID, indexDigest } = await generateDAGIndex(upload)
    console.log()
    console.log('Metadata')
    console.log(`  CID: ${indexCID}`)
    console.log(`  Size: ${indexBytes.length} bytes`)
    console.log(`  Multihash: ${base58btc.encode(indexDigest.bytes)}`)
    console.log()
    
    return {
      success: true,
      upload: upload.root,
      space: upload.space,
      indexCID: indexCID.toString(),
      indexSize: indexBytes.length,
    }
    
  } catch (error) {
    console.error(`\nIndex generation failed for ${upload.root}:`, error.message)
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
 * Run index generation test mode
 */
async function runTestIndexMode(values) {
  console.log('Legacy Content Migration - Step 1: Index Generation Test')
  console.log('='.repeat(50))
  console.log()
  
  let upload = null
  
  // If specific CID provided, query it directly using the GSI
  if (values.cid) {
    console.log(`Querying upload by CID: ${values.cid}`)
    
    if (!values.space) {
      console.log('No space specified, using GSI to find upload...')
      upload = await getUploadByRoot(values.cid)
    } else {
      console.log(`Using space: ${values.space}`)
      const { getUpload } = await import('./lib/tables/upload-table.js')
      upload = await getUpload(values.space, values.cid)
    }
    
    if (!upload) {
      console.error(`Upload not found for CID: ${values.cid}${values.space ? ` in space ${values.space}` : ''}`)
      process.exit(1)
    }
    
    console.log(`Found upload in space: ${upload.space}`)
    console.log()
  } else {
    const limit = parseInt(values.limit || '1', 10)
    console.log(`Querying ${limit} upload(s)...`)
    if (values.space) console.log(`Space filter: ${values.space}`)
    console.log()
    
    for await (const u of sampleUploads({ limit, space: values.space })) {
      upload = u
      break
    }
    
    if (!upload) {
      console.error('No uploads found')
      process.exit(1)
    }
  }
  
  // Test index generation
  const result = await testIndexGeneration(upload)
  
  // Print summary
  console.log()
  console.log('='.repeat(50))
  if (result.success) {
    console.log('SUCCESS: Index generated successfully!')
  } else {
    console.log('FAILED: Index generation failed')
    console.log(`  Upload: ${result.upload}`)
    console.log(`  Error: ${result.error}`)
  }
  console.log('='.repeat(50))
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
        description: 'Run in index generation test mode',
      },
      cid: {
        type: 'string',
        description: 'Specific upload CID (for test mode)',
      },
      limit: {
        type: 'string',
        short: 'l',
        default: '10',
      },
      space: {
        type: 'string',
        short: 's',
        description: 'Filter by space DID',
      },
      resource: {
        type: 'string',
        short: 'r',
        description: 'Filter by resource (root CID)',
      },
      customer: {
        type: 'string',
        short: 'c',
        description: 'Filter by customer DID',
      },
      'dry-run': {
        type: 'boolean',
        default: false,
      },
      concurrency: {
        type: 'string',
        default: '1',
      },
      output: {
        type: 'string',
        short: 'o',
        default: 'migration-results.json',
      },
    },
  })
  
  validateConfig()
  
  // Run in test mode or full migration mode
  if (values['test-index']) {
    await runTestIndexMode(values)
  } else {
    //await runMigrationMode(values)
  }
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
