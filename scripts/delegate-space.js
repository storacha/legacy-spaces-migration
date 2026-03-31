import dotenv from 'dotenv'
dotenv.config()
import { DID } from '@ucanto/core'
import { Absentee } from '@ucanto/principal'
import { UCAN } from '@storacha/capabilities'
import * as DIDMailto from '@storacha/did-mailto'
import { delegate } from '@ucanto/core'
import { SpaceAccess } from '@storacha/access'
import * as readline from 'readline'

// Load env vars immediately
dotenv.config({path: '../.env'})

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

const question = (query) => new Promise((resolve) => rl.question(query, resolve))

/**
 * Parse email or DID to a valid did:mailto: DID
 * @param {string} input - Email address or did:mailto: DID
 * @returns {string} Valid did:mailto: DID
 */
function parseEmailOrDID(input) {
  const trimmed = input.trim()
  
  // If it's already a did:mailto: DID, validate and return
  if (trimmed.startsWith('did:mailto:')) {
    try {
      return DIDMailto.fromString(trimmed)
    } catch (err) {
      throw new Error(`Invalid did:mailto: DID: ${err.message}`)
    }
  }
  
  // If it looks like an email, convert to DID
  if (trimmed.includes('@')) {
    try {
      return DIDMailto.fromEmail(trimmed)
    } catch (err) {
      throw new Error(`Invalid email address: ${err.message}`)
    }
  }
  
  throw new Error('Input must be either an email address or a did:mailto: DID')
}

async function main() {
  console.log('--- Space Ownership Transfer Tool ---\n')
  
  // Dynamic imports to ensure env vars are loaded first
  const { getUploadServiceSigner } = await import('../src/config.js')
  const { storeDelegations, findDelegationByIssuer } = await import('../src/lib/tables/delegations-table.js')
  const { updateSpaceProvisioning } = await import('../src/lib/tables/consumer-table.js')

  const spaceDID = (await question('Enter Space DID (did:key:...): ')).trim()
  const fromInput = (await question('Enter Current Owner (email or did:mailto:...): ')).trim()
  const toInput = (await question('Enter New Owner (email or did:mailto:...): ')).trim()
  if (!spaceDID || !fromInput || !toInput) {
    console.error('Error: All fields are required.')
    rl.close()
    process.exit(1)
  }

  // Parse and validate inputs
  let inputFromDID, toDID
  try {
    inputFromDID = parseEmailOrDID(fromInput)
    console.log(`✓ Current Owner DID: ${inputFromDID}`)
  } catch (err) {
    console.error(`✗ Current Owner Error: ${err.message}`)
    rl.close()
    process.exit(1)
  }

  try {
    toDID = parseEmailOrDID(toInput)
    console.log(`✓ New Owner DID: ${toDID}`)
  } catch (err) {
    console.error(`✗ New Owner Error: ${err.message}`)
    rl.close()
    process.exit(1)
  }

  console.log(`\nLooking up current owner delegation (${inputFromDID})...`)
  const spaceDelegation = await findDelegationByIssuer(spaceDID)
  if (!spaceDelegation) {
    throw new Error(`No delegation found for space ${spaceDID}`)
  }

  // Validate current owner
  const currentOwnerDID = spaceDelegation.audience.did()
  if (currentOwnerDID !== inputFromDID) {
    console.error('\n❌ Error: Current Owner mismatch!')
    console.error(`  Input:  ${inputFromDID}`)
    console.error(`  Actual: ${currentOwnerDID} (from delegation)`)
    console.error('Aborting transfer to prevent errors.')
    rl.close()
    process.exit(1)
  }

  // Extract space name for display
  const spaceMeta = spaceDelegation.facts?.[0]?.space ?? {}
  const currentSpaceName = spaceMeta.name || '(unnamed)'
  const spaceAccess = SpaceAccess.from(spaceMeta.access)

  console.log('\n--- CONFIRM TRANSFER ---')
  console.log(`Space:         ${spaceDID}`)
  console.log(`Space Name:    ${currentSpaceName}`)
  console.log(`Access Type:   ${spaceAccess.type}`)
  console.log(`Current Owner: ${currentOwnerDID} (Verified)`)
  console.log(`New Owner:     ${toDID}`)
  console.log('------------------------')

  console.log('Found delegation:', spaceDelegation.cid.toString())
  console.log('Issuer:', spaceDelegation.issuer.did())
  console.log('Audience:', spaceDelegation.audience.did())
  console.log('Capabilities:', JSON.stringify(spaceDelegation.capabilities, null, 2))

  const confirm = await question('\nProceed with transfer? (y/N): ')
  if (confirm.toLowerCase() !== 'y') {
    console.log('Aborted.')
    rl.close()
    process.exit(0)
  }

  console.log('\nProceeding...')

  // Create From -> To delegation (Absentee)
  console.log('\nCreating new delegation...')
  const serviceSigner = await getUploadServiceSigner()
  const uploadServiceDID = serviceSigner.did() 
  
  // Create Absentee issuer for the "From" account
  const absenteeIssuer = Absentee.from({ id: currentOwnerDID })
  
  // Reuse spaceMeta already extracted earlier
  const currentName = spaceMeta.name || '(unnamed)'
  
  console.log(`\nCurrent space name: ${currentName}`)
  const renamePrompt = await question('Enter new space name (or press Enter to keep current): ')
  const newSpaceName = renamePrompt.trim() || spaceMeta.name
  
  // Close readline after all prompts are done
  rl.close()
  
  if (newSpaceName && newSpaceName !== spaceMeta.name) {
    console.log(`✓ Space will be renamed to: "${newSpaceName}"`)
  } else if (spaceMeta.name) {
    console.log(`✓ Keeping current name: "${spaceMeta.name}"`)
  } else {
    console.log(`ℹ Space will remain unnamed`)
  }
  
  // Create facts with updated name and normalized access type
  const facts = [{ space: { name: newSpaceName, access: spaceAccess } }]
  
  // Delegate the same capabilities as the original delegation
  // We use the generic delegate function from @ucanto/core
  const newDelegation = await delegate({
    issuer: absenteeIssuer,
    audience: DID.parse(toDID),
    capabilities: spaceDelegation.capabilities, // Propagate capabilities
    facts, // Preserve space metadata with updated name and normalized access
    proofs: [spaceDelegation],
    expiration: Infinity
  })

  console.log(`Created delegation: ${newDelegation.cid}`)

  // Create Attestation (Service -> To)
  console.log('\nAttesting delegation...')
  const attestation = await UCAN.attest.delegate({
    issuer: serviceSigner,
    audience: DID.parse(toDID),
    with: uploadServiceDID,
    nb: {
      proof: newDelegation.cid
    },
    expiration: Infinity
  })
  
  console.log(`Created attestation: ${attestation.cid}`)

  // Store delegations
  console.log('\nStoring delegations in w3infra...')
  try {
    // Store new delegation and attestation
    await storeDelegations([newDelegation, attestation])
    console.log('✓ Delegations stored successfully')
  } catch (err) {
    console.error('✗ Failed to store delegations:', err)
    throw err
  }

  // Update provisioning (billing)
  console.log('\nUpdating provisioning (billing) to new owner...')
  try {
    // This overwrites the existing record in consumer table, effectively transferring ownership
    await updateSpaceProvisioning(toDID, spaceDID)
    console.log('✓ Provisioning updated successfully')
  } catch (err) {
    console.error('✗ Failed to update provisioning:', err)
    throw err
  }

  console.log('\n-----------------------------------------------------------')
  console.log('SUCCESS! Space ownership has been transferred.')
  console.log('')
  console.log('INSTRUCTIONS FOR NEW OWNER:')
  console.log('1. Logout of the CLI/Client:')
  console.log('   storacha  logout')
  console.log('2. Login with the new email account:')
  console.log(`   storacha login <new_customer_email>`)
  console.log('3. The new space should appear in your space list.')
  console.log('-----------------------------------------------------------')
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
