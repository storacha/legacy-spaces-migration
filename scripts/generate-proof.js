import { parseArgs } from 'node:util'
import * as Signer from '@ucanto/principal/ed25519'
import { Claim } from '@storacha/capabilities'
import { DID } from '@ucanto/core'

/**
 * Generate a proof for the Indexing Service to authorize Piri to cache claims.
 * 
 * Usage:
 *   node scripts/generate-proof.js --issuer <base64-key> --issuer-did <did:web:...> --audience <did>
 */
async function main() {
  const { values } = parseArgs({
    options: {
      issuer: { type: 'string', short: 'i' }, // Private key of Indexing Service
      'issuer-did': { type: 'string', short: 'd' }, // DID of Indexing Service (did:web:...)
      audience: { type: 'string', short: 'a' }, // DID of Piri
    }
  })

  if (!values.issuer || !values.audience) {
    console.error('Usage: node scripts/generate-proof.js --issuer <base64-key> [--issuer-did <did:web:...>] --audience <did>')
    process.exit(1)
  }

  // 1. Parse Issuer
  let issuer = Signer.parse(values.issuer)
  
  // If issuer-did is provided, wrap the signer to act as that DID (e.g. did:web)
  if (values['issuer-did']) {
    const did = DID.parse(values['issuer-did'])
    issuer = issuer.withDID(did.did())
  }
  
  console.log(`Issuer: ${issuer.did()}`)

  // 2. Audience
  const audience = DID.parse(values.audience)
  console.log(`Audience: ${audience.did()}`)

  // 3. Create Delegation
  // Delegating 'claim/cache' capability
  console.log('Creating delegation for claim/cache...')
  const proof = await Claim.cache.delegate({
    issuer,
    audience,
    with: issuer.did(), // Authorize for the indexing service DID
    expiration: Infinity
  })
  
  console.log(`Delegation CID: ${proof.cid}`)

  // 4. Serialize to CAR
  // Use archive() to get the CAR bytes
  const archive = await proof.archive()
  if (archive.error) {
    throw new Error(`Failed to archive delegation: ${archive.error.message}`)
  }
  
  const bytes = archive.ok

  console.log('\nProof (Base64 CAR):')
  console.log(Buffer.from(bytes).toString('base64'))
}

main().catch(console.error)
