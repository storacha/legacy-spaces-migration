/**
 * Configuration for the legacy spaces migration tool
 */
import dotenv from 'dotenv'
dotenv.config()
import * as Signer from '@ucanto/principal/ed25519'
import { DID, Delegation } from '@ucanto/core'
import { CarReader } from '@ipld/car'
import { uriToMultiaddr } from '@multiformats/uri-to-multiaddr'
import { peerIdFromString } from '@libp2p/peer-id'
/**
 * Environment-specific defaults
 * @type {Record<string, {
 *  region: string,
 *  tablePrefix: string,
 *  carparkBucket: string,
 *  carparkPublicUrl: string,
 *  claimsBucket: string,
 *  indexingService: string,
 *  indexingServiceDID: string,
 *  contentClaims: string,
 *  claimsServiceDID: string,
 *  uploadService: string,
 *  uploadServiceDID: string,
 *  gatewayService: string,
 *  gatewayServiceDID: string,
 *  piriServiceDID: string,
 *  piriService: string,
 *  piriPeerID: string,
 *  storageProviders: string[],
 *  ipniPublishingBucket: string,
 *  ipniPublishingQueue: string,
 * }>}
 */
const ENVIRONMENTS = {
  production: {
    region: 'us-west-2',
    tablePrefix: 'prod-w3infra',
    carparkBucket: 'carpark-prod-0',
    carparkPublicUrl: 'https://carpark-prod-0.r2.w3s.link',
    claimsBucket: 'prod-storage-claim-store-bucket',
    indexingService: 'https://indexer.storacha.network',
    indexingServiceDID: 'did:web:indexer.storacha.network',
    contentClaims: 'https://claims.web3.storage',
    claimsServiceDID: 'did:web:claims.web3.storage',
    uploadService: 'https://up.storacha.network',
    uploadServiceDID: 'did:web:up.storacha.network',
    gatewayService: 'https://storacha.link',
    gatewayServiceDID: 'did:web:w3s.link',
    piriServiceDID: 'did:web:storage.storacha.network',
    piriService: 'https://storage.storacha.network',
    piriPeerID: '12D3KooWLiYS7k5GnBcngSRHemu98HQ1yqzJdYQcqqa2kpDDX9hf',
    storageProviders: ['did:web:up.storacha.network', 'did:web:web3.storage'],
    ipniPublishingBucket: 'prod-storage-ipni-publisher',
    ipniPublishingQueue:
      'https://sqs.us-west-2.amazonaws.com/505595374361/prod-storage-ipni-publisher.fifo',
  },
  staging: {
    region: 'us-east-2',
    tablePrefix: 'staging-w3infra',
    carparkBucket: 'carpark-staging-0',
    carparkPublicUrl: 'https://carpark-staging-0.r2.w3s.link',
    claimsBucket: 'staging-storage-claim-store-bucket',
    indexingService: 'https://staging.indexer.storacha.network',
    indexingServiceDID: 'did:web:staging.indexer.storacha.network',
    contentClaims: 'https://staging.claims.web3.storage',
    claimsServiceDID: 'did:web:staging.claims.web3.storage',
    uploadService: 'https://staging.up.storacha.network',
    uploadServiceDID: 'did:web:staging.up.storacha.network',
    gatewayService: 'https://freeway-fforbeck.protocol-labs.workers.dev',
    // gatewayService: 'https://gateway.storacha.network',
    gatewayServiceDID: 'did:web:staging.w3s.link',
    piriServiceDID: 'did:web:staging.storage.storacha.network',
    piriService: 'https://staging.storage.storacha.network',
    piriPeerID: '12D3KooWPMQTKSMA3eFUxc23gBfMHEgzfk7W1TBezKNsBwPMRLQ7',

    storageProviders: [
      'did:web:staging.web3.storage',
      'did:web:staging.up.storacha.network',
    ],
    ipniPublishingBucket: 'staging-storage-ipni-publisher',
    ipniPublishingQueue:
      'https://sqs.us-east-2.amazonaws.com/505595374361/staging-storage-ipni-publisher.fifo',
  },
}

/**
 * Get current environment from ENV var or default to production
 */
const getEnvironment = () => {
  const env = process.env.STORACHA_ENV || 'production'
  if (!ENVIRONMENTS[env]) {
    throw new Error(
      `Invalid STORACHA_ENV: ${env}. Must be 'production' or 'staging'`
    )
  }
  return ENVIRONMENTS[env]
}

const env = getEnvironment()

export const config = {
  environment: process.env.STORACHA_ENV || 'production',

  aws: {
    region: process.env.AWS_REGION || env.region,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },

  tables: {
    upload: process.env.UPLOAD_TABLE_NAME || `${env.tablePrefix}-upload`,
    blobRegistry:
      process.env.BLOB_REGISTRY_TABLE_NAME ||
      `${env.tablePrefix}-blob-registry`,
    store: process.env.STORE_TABLE_NAME || `${env.tablePrefix}-store`, // Legacy table
    allocations:
      process.env.ALLOCATIONS_TABLE_NAME || `${env.tablePrefix}-allocation`, // Billing table (also has blob info)
    consumer: process.env.CONSUMER_TABLE_NAME || `${env.tablePrefix}-consumer`, // Space ownership (space -> customer mapping)
    subscription:
      process.env.SUBSCRIPTION_TABLE_NAME || `${env.tablePrefix}-subscription`, // Billing subscriptions (customer -> provider relationship)
    delegation:
      process.env.DELEGATION_TABLE_NAME || `${env.tablePrefix}-delegation`, // Delegation storage
    migrationSpaces:
      process.env.MIGRATION_SPACES_TABLE_NAME ||
      `${process.env.STORACHA_ENV || 'prod'}-migration-spaces`, // Migration space tracking
    migrationProgress:
      process.env.MIGRATION_PROGRESS_TABLE_NAME ||
      `${
        process.env.STORACHA_ENV === 'staging' ? 'staging' : 'prod'
      }-migration-progress`, // Migration progress tracking
  },

  addresses: {
    peerID: peerIdFromString(process.env.PIRI_PEER_ID || env.piriPeerID),
    blobProtocolBlobAddr: createMultiAddr(
      process.env.CARPARK_PUBLIC_URL || env.carparkPublicUrl,
      '{blob}/{blob}.blob'
    ),
    storeProtocolBlobAddr: createMultiAddr(
      process.env.CARPARK_PUBLIC_URL || env.carparkPublicUrl,
      '{blobCID}/{blobCID}.car'
    ),
    claimAddr: createMultiAddr(
      process.env.PIRI_SERVICE_URL || env.piriService,
      'claim/{claim}'
    ),
  },

  services: {
    indexingServiceURL: process.env.INDEXING_SERVICE_URL || env.indexingService,
    indexingServiceDID:
      process.env.INDEXING_SERVICE_DID || env.indexingServiceDID,
    contentClaimsServiceURL:
      process.env.CONTENT_CLAIMS_SERVICE_URL || env.contentClaims,
    claimsServiceDID: process.env.CLAIMS_SERVICE_DID || env.claimsServiceDID,
    uploadServiceURL: process.env.UPLOAD_SERVICE_URL || env.uploadService,
    uploadServiceDID: process.env.UPLOAD_SERVICE_DID || env.uploadServiceDID,
    gatewayServiceURL: process.env.GATEWAY_SERVICE_URL || env.gatewayService,
    gatewayServiceDID: process.env.GATEWAY_SERVICE_DID || env.gatewayServiceDID,
    piriServiceDID: process.env.PIRI_SERVICE_DID || env.piriServiceDID,
    piriServiceURL: process.env.PIRI_SERVICE_URL || env.piriService,
    storageProviders: env.storageProviders, // Provider DIDs for querying consumer table
  },

  queues: {
    ipniPublishingQueue:
      process.env.IPNI_PUBLISHING_QUEUE || env.ipniPublishingQueue,
  },

  storage: {
    carparkBucket: process.env.CARPARK_BUCKET || env.carparkBucket,
    claimsBucket: process.env.CLAIMS_BUCKET || env.claimsBucket,
    // W3infra uses format: {name}-{stage}-{version} for S3 buckets
    delegationBucket:
      process.env.DELEGATION_BUCKET_NAME ||
      `delegation-${process.env.STORACHA_ENV || 'production'}-0`,
    carparkPublicUrl: process.env.CARPARK_PUBLIC_URL || env.carparkPublicUrl,
    // R2 (Cloudflare) configuration for delegations (fallback if S3 not found)
    r2DelegationBucket: process.env.R2_DELEGATION_BUCKET_NAME,
    r2Endpoint: process.env.R2_ENDPOINT,
    r2AccessKeyId: process.env.R2_ACCESS_KEY_ID,
    r2SecretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    r2Region: process.env.R2_REGION || 'auto',
    ipniPublishingBucket:
      process.env.IPNI_PUBLISHING_BUCKET || env.ipniPublishingBucket,
  },

  migration: {
    batchSize: parseInt(process.env.BATCH_SIZE || '100', 10),
    maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || '10', 10),
    dryRun: process.env.DRY_RUN === 'true',
  },

  credentials: {
    // Migration agent private key
    migrationAgentPrivateKey: process.env.MIGRATION_AGENT_PRIVATE_KEY,
    // Service private key for publishing to upload service
    uploadServicePrivateKey: process.env.UPLOAD_SERVICE_PRIVATE_KEY,
    // Claims service private key for publishing location claims
    claimsServicePrivateKey: process.env.CLAIMS_SERVICE_PRIVATE_KEY,
    // Gateway private key for publishing delegations
    gatewayPrivateKey: process.env.GATEWAY_PRIVATE_KEY,
    // PIRI private key for indexing service access
    piriPrivateKey: process.env.PIRI_PRIVATE_KEY,
    // Indexing service private key
    indexingServiceProof: process.env.INDEXING_SERVICE_PROOF,
  },

  admin: {
    privateKey: process.env.ADMIN_PRIVATE_KEY,
  },

  encryption: {
    // 32-byte key for AES-256 encryption of migration space private keys
    // Generate with: node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"
    key: process.env.MIGRATION_ENCRYPTION_KEY
      ? Buffer.from(process.env.MIGRATION_ENCRYPTION_KEY, 'base64')
      : null,
  },
}

/**
 *
 * @param {string} baseUrl
 * @param {string} path
 * @returns {import('@multiformats/multiaddr').Multiaddr}
 */
function createMultiAddr(baseUrl, path) {
  const url = new URL(path, baseUrl)
  return uriToMultiaddr(url.toString())
}
/**
 * Validate required configuration
 */
export function validateConfig() {
  const required = [
    ['AWS_REGION', config.aws.region],
    ['UPLOAD_TABLE_NAME', config.tables.upload],
  ]

  const missing = required
    .filter(([, value]) => !value)
    .map(([name]) => name)

  if (missing.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missing.join(', ')}`
    )
  }
}

/**
 * Get the claims service signer with the correct did:web identity
 * @returns {Promise<import('@ucanto/interface').Signer>}
 */
export async function getClaimsSigner() {
  if (!config.credentials.claimsServicePrivateKey) {
    throw new Error('CLAIMS_SERVICE_PRIVATE_KEY not configured')
  }

  // Parse the private key and override with the did:web identity from environment config
  const claimsKeyPair = Signer.parse(config.credentials.claimsServicePrivateKey)
  return claimsKeyPair.withDID(
    DID.parse(config.services.claimsServiceDID).did()
  )
}

/**
 * Get the upload service signer with the correct did:web identity
 * @returns {Promise<import('@ucanto/interface').Signer>}
 */
export async function getUploadServiceSigner() {
  if (!config.credentials.uploadServicePrivateKey) {
    throw new Error('SERVICE_PRIVATE_KEY not configured')
  }

  // Parse the private key and override with the did:web identity from environment config
  const uploadKeyPair = Signer.parse(config.credentials.uploadServicePrivateKey)
  return uploadKeyPair.withDID(
    DID.parse(config.services.uploadServiceDID).did()
  )
}

/**
 * Get the migration signer with the correct did:web identity
 * Key: did:key:z6MkmjS2fNbyMz9NviLJ9owtSQK5569EbjvLRbFFim9NLLar
 * 
 * @returns {Promise<import('@ucanto/interface').Signer>}
 */
export async function getMigrationSigner() {
  if (!config.credentials.migrationAgentPrivateKey) {
    throw new Error('MIGRATION_AGENT_PRIVATE_KEY not configured')
  }

  const migrationKeyPair = Signer.parse(config.credentials.migrationAgentPrivateKey)
  return migrationKeyPair
}

/**
 * Get the gateway signer with the correct did:web identity
 * @returns {Promise<import('@ucanto/interface').Signer>}
 */
export async function getGatewaySigner() {
  if (!config.credentials.gatewayPrivateKey) {
    throw new Error('GATEWAY_PRIVATE_KEY not configured')
  }

  // Parse the private key and override with the did:web identity from environment config
  const gatewayKeyPair = Signer.parse(config.credentials.gatewayPrivateKey)
  return gatewayKeyPair.withDID(
    DID.parse(config.services.gatewayServiceDID).did()
  )
}

/**
 * Get the gateway signer with the correct did:web identity
 * @returns {Promise<import('@ucanto/interface').Signer>}
 */
export async function getPiriSigner() {
  if (!config.credentials.piriPrivateKey) {
    throw new Error('PIRI_PRIVATE_KEY not configured')
  }

  /** Parse the private key and override with the did:web identity from environment config */
  const piriKeyPair = Signer.parse(config.credentials.piriPrivateKey)
  return piriKeyPair.withDID(DID.parse(config.services.piriServiceDID).did())
}

/**
 * Get the indexing service proof
 * @returns {Promise<import('@ucanto/interface').Delegation>}
 */
export async function getIndexingServiceProof() {
  if (!config.credentials.indexingServiceProof) {
    throw new Error('INDEXING_SERVICE_PROOF not configured')
  }

  // Decode base64 string
  const bytes = Buffer.from(config.credentials.indexingServiceProof, 'base64')

  // Read CAR
  const reader = await CarReader.fromBytes(bytes)
  const [root] = await reader.getRoots()

  if (!root) {
    throw new Error('Proof CAR has no roots')
  }

  // Collect all blocks
  const blockMap = new Map()
  for await (const block of reader.blocks()) {
    blockMap.set(block.cid.toString(), block)
  }

  // Create Delegation view
  return Delegation.view({
    root: /** @type {any} */ (root),
    blocks: blockMap,
  })
}
