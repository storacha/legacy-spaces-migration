import { parseArgs } from 'node:util'
import { DID } from '@ucanto/core'
import { delegate } from '@ucanto/core'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as Link from 'multiformats/link'
import { identity } from 'multiformats/hashes/identity'
import { base64 } from 'multiformats/bases/base64'

/**
 * Function copied from https://gist.github.com/alanshaw/c1a3508311f015cc670db5f471e7b904
 * Issuer: indexingServiceDID
 * Audience: storageProviderDID
 */
const delegateIndexingServiceToAudience = async (issuerPK, issuerDID, audienceDID) => {
  const issuer = ed25519.parse(issuerPK).withDID(DID.parse(issuerDID))
  const audience = DID.parse(audienceDID)
  const abilities = ['claim/cache']

  const delegation = await delegate({
    issuer,
    audience,
    capabilities: abilities.map(can => ({ can, with: issuer.did(), nb: {} })),
    expiration: Infinity
  })

  console.log(await formatDelegation(delegation))
}

/** @param {import('@ucanto/interface').Delegation} */
const formatDelegation = async delegation => {
  const { ok: archive, error } = await delegation.archive()
  if (error) throw error

  const digest = identity.digest(archive)
  const link = Link.create(0x0202, digest)
  return link.toString(base64)
}

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

  await delegateIndexingServiceToAudience(values.issuer, values['issuer-did'], values.audience)
}

main().catch(console.error)
