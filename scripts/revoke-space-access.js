import dotenv from 'dotenv'
dotenv.config()
import * as DidMailto from '@storacha/did-mailto'
import * as readline from 'readline'
import { removeDelegation, getDelegation } from '../src/lib/tables/delegations-table.js'

// Load env vars immediately
dotenv.config({path: '../.env'})

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

const question = (query) => new Promise((resolve) => rl.question(query, resolve))

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2)
  return {
    execute: args.includes('--execute')
  }
}

/**
 * Convert email to did:mailto format if needed
 * Uses @storacha/did-mailto for proper encoding (handles special chars like +)
 * @param {string} input - Email address or did:mailto: DID
 * @returns {string} - did:mailto: DID
 */
function parseUserDID(input) {
  const trimmed = input.trim()
  
  // If already a DID, validate and return it
  if (trimmed.startsWith('did:mailto:')) {
    try {
      return DidMailto.fromString(trimmed)
    } catch (err) {
      throw new Error(`Invalid did:mailto format: ${err.message}`)
    }
  }
  
  // If it's an email, convert to did:mailto with proper encoding
  if (trimmed.includes('@')) {
    try {
      const emailAddress = DidMailto.email(trimmed)
      return DidMailto.fromEmail(emailAddress)
    } catch (err) {
      throw new Error(`Invalid email address: ${err.message}`)
    }
  }
  
  // Otherwise assume it's malformed
  throw new Error(`Invalid input: "${input}". Expected email address or did:mailto: DID`)
}

async function main() {
  const { execute } = parseArgs()
  
  console.log('--- Space Access Revocation Tool ---')
  console.log(`Mode: ${execute ? '🔴 EXECUTE' : '🟢 DRY RUN'}`)
  console.log('-----------------------------------\n')
  
  // Dynamic imports to ensure env vars are loaded first
  const { getUploadServiceSigner } = await import('../src/config.js')
  const { findDelegationByIssuer } = await import('../src/lib/tables/delegations-table.js')
  const { createRevocationsTable } = await import('../src/lib/tables/revocations-table.js')

  // Get inputs
  const spaceDID = (await question('Enter Space DID (did:key:...): ')).trim()
  const userInput = (await question('Enter User email or DID to revoke: ')).trim()

  if (!spaceDID || !userInput) {
    console.error('Error: Both Space DID and User email/DID are required.')
    rl.close()
    process.exit(1)
  }

  // Parse user input to did:mailto format
  let userDID
  try {
    userDID = parseUserDID(userInput)
    if (userInput !== userDID) {
      console.log(`✓ DID Mail To: ${userDID}`)
    }
  } catch (err) {
    console.error(`Error: ${err.message}`)
    rl.close()
    process.exit(1)
  }

  console.log('\n--- STEP 1: Finding Delegations ---')
  
  // Find all delegations from this space
  const spaceDelegations = await findDelegationByIssuer(spaceDID)
  if (!spaceDelegations) {
    console.error(`❌ No delegations found for space ${spaceDID}`)
    rl.close()
    process.exit(1)
  }

  // Filter by audience (user to revoke)
  const userDelegations = Array.isArray(spaceDelegations) 
    ? spaceDelegations.filter(d => d.audience.did() === userDID)
    : (spaceDelegations.audience.did() === userDID ? [spaceDelegations] : [])

  if (userDelegations.length === 0) {
    console.error(`❌ No delegations found from space ${spaceDID} to user ${userDID}`)
    rl.close()
    process.exit(1)
  }

  console.log(`✓ Found ${userDelegations.length} delegation(s)\n`)

  // Display delegations and let user select
  let selectedDelegation
  if (userDelegations.length === 1) {
    selectedDelegation = userDelegations[0]
    console.log('Auto-selected the only delegation found:')
  } else {
    console.log('Multiple delegations found:')
    userDelegations.forEach((d, i) => {
      console.log(`\n[${i + 1}] Delegation CID: ${d.cid}`)
      console.log(`    Issuer: ${d.issuer.did()}`)
      console.log(`    Audience: ${d.audience.did()}`)
      console.log(`    Capabilities: ${JSON.stringify(d.capabilities.map(c => c.can))}`)
      console.log(`    Expiration: ${d.expiration === Infinity ? 'Never' : new Date(d.expiration * 1000).toISOString()}`)
    })
    
    const selection = await question('\nSelect delegation number to revoke: ')
    const index = parseInt(selection) - 1
    
    if (isNaN(index) || index < 0 || index >= userDelegations.length) {
      console.error('Invalid selection')
      rl.close()
      process.exit(1)
    }
    
    selectedDelegation = userDelegations[index]
  }

  console.log(`\nSelected delegation: ${selectedDelegation.cid}`)

  // Extract space metadata if available
  const spaceMeta = selectedDelegation.facts?.[0]?.space ?? {}
  const spaceName = spaceMeta.name || '(unnamed)'

  console.log('\n--- STEP 2: Safety Checks ---')
  
  // Check if already revoked
  const revocationsStorage = await createRevocationsTable()
  const existingRevocations = await revocationsStorage.query({
    [selectedDelegation.cid.toString()]: {}
  })

  if (existingRevocations.ok && Object.keys(existingRevocations.ok).length > 0) {
    console.log('⚠️  WARNING: This delegation appears to already be revoked!')
    console.log('Existing revocations:', JSON.stringify(existingRevocations.ok, null, 2))
    const continueAnyway = await question('\nContinue anyway? (y/N): ')
    if (continueAnyway.toLowerCase() !== 'y') {
      console.log('Aborted.')
      rl.close()
      process.exit(0)
    }
  }

  // Count other delegations for this space
  const allSpaceDelegations = Array.isArray(spaceDelegations) ? spaceDelegations : [spaceDelegations]
  const otherDelegations = allSpaceDelegations.filter(d => 
    !d.cid.equals(selectedDelegation.cid) && 
    d.expiration > Date.now() / 1000
  )

  if (otherDelegations.length === 0) {
    console.log('⚠️  WARNING: This is the ONLY active delegation for this space!')
    console.log('⚠️  Revoking will leave the space with NO owners!')
  } else {
    console.log(`✓ Found ${otherDelegations.length} other active delegation(s) for this space:`)
    otherDelegations.forEach(d => {
      console.log(`  - ${d.audience.did()} (${d.capabilities.map(c => c.can).join(', ')})`)
    })
  }

  console.log('\n--- STEP 3: Confirmation ---')
  console.log('Space:', spaceDID)
  console.log('Space Name:', spaceName)
  console.log('Revoking access for:', userDID)
  console.log('Delegation CID:', selectedDelegation.cid.toString())
  console.log('Capabilities:', selectedDelegation.capabilities.map(c => c.can).join(', '))
  console.log('Other active delegations:', otherDelegations.length)
  console.log('Mode:', execute ? '🔴 LIVE' : ' 🟢 DRY RUN')
  console.log('-----------------------------------')

  const confirm = await question('\nProceed with revocation? (y/N): ')
  if (confirm.toLowerCase() !== 'y') {
    console.log('Aborted.')
    rl.close()
    process.exit(0)
  }

  if (!execute) {
    console.log('\nDRY RUN MODE - No changes made')
    console.log(`Would revoke delegation: ${selectedDelegation.cid}`)
    console.log(`User ${userDID} would lose access to ${spaceName}`)
    console.log('\nRun with --execute to perform revocation')
    return
  }

  console.log('\n--- STEP 4: Executing Revocation ---')
  
  try {
    // Write revocation directly to DynamoDB
    // CRITICAL: The scope must be either the delegation issuer (space) or audience (user)
    // The revocation validation checks: delegation.issuer.did() OR delegation.audience.did()
    // We use the space DID (issuer) as the scope since we're revoking on behalf of the space
    const result = await revocationsStorage.reset({
      revoke: selectedDelegation.cid,
      scope: selectedDelegation.issuer.did(), // Use space DID (issuer), not service DID
      cause: selectedDelegation.cid // Admin revocation - use delegation CID as cause
    })

    if (result.error) {
      throw new Error(`Failed to write revocation: ${result.error.message}`)
    }

    console.log('✅ Successfully revoked delegation:', selectedDelegation.cid.toString())
    
    // Also delete the delegation from DynamoDB so it doesn't appear in access/claim
    // Note: We only delete from DynamoDB index, not from R2 storage (for audit trail)
    console.log('   Verifying delegation in DynamoDB before deletion...')
    try {
      const delegationItem = await getDelegation({
        link: selectedDelegation.cid
      })
      
      if (!delegationItem) {
        console.log('   ℹ️  Delegation not found in DynamoDB (may have been deleted already)')
      } else {
        console.log('   Found delegation in DynamoDB:')
        console.log(`     Link: ${delegationItem.link}`)
        console.log(`     Audience: ${delegationItem.audience}`)
        console.log(`     Issuer: ${delegationItem.issuer}`)
        console.log(`     Expiration: ${delegationItem.expiration === null ? 'Infinity' : new Date(delegationItem.expiration * 1000).toISOString()}`)
        
        const confirmDelete = await question('\n   Confirm deletion from DynamoDB? (y/N): ')
        if (confirmDelete.toLowerCase() !== 'y') {
          console.log('   ⚠️  Skipped delegation deletion')
          console.log('   Note: Delegation is revoked but will still appear in access/claim')
        } else {
          await removeDelegation({
            link: selectedDelegation.cid
          })
          
          console.log('   ✅ Delegation deleted from DynamoDB index')
          console.log('      (Delegation CAR file remains in R2 for audit trail)')
        }
      }
    } catch (deleteError) {
      console.warn('   ⚠️  Warning: Failed to delete delegation from DynamoDB:', deleteError.message)
      console.warn('      The delegation is revoked but will still appear in access/claim')
    }
    
    console.log(`✅ User ${userDID} no longer has access to ${spaceName}`)
  } catch (err) {
    console.error('❌ Failed to revoke delegation:', err)
    throw err
  } finally {
    // Close readline interface after all prompts are done
    rl.close()
  }
}

main().catch(console.error)
