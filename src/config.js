/**
 * Configuration for the legacy spaces migration tool
 */
import dotenv from 'dotenv'
dotenv.config()
import * as Signer from '@ucanto/principal/ed25519'

/**
 * Environment-specific defaults
 */
const ENVIRONMENTS = {
  production: {
    region: 'us-west-2',
    tablePrefix: 'prod-w3infra',
    carparkBucket: 'carpark-prod-0',
    carparkPublicUrl: 'https://carpark-prod-0.r2.w3s.link',
    indexingService: 'https://indexer.storacha.network',
    contentClaims: 'https://claims.web3.storage',
    claimsServiceDID: 'did:web:claims.web3.storage',
    uploadService: 'https://up.storacha.network',
    uploadServiceDID: 'did:web:up.storacha.network',
    gatewayService: 'https://storacha.link',
    gatewayServiceDID: 'did:web:w3s.link',
    storageProviders: ['did:web:up.storacha.network', 'did:web:web3.storage'],
  },
  staging: {
    region: 'us-east-2',
    tablePrefix: 'staging-w3infra',
    carparkBucket: 'carpark-staging-0',
    carparkPublicUrl: 'https://carpark-staging-0.r2.w3s.link',
    indexingService: 'https://staging.indexer.storacha.network',
    contentClaims: 'https://staging.claims.web3.storage',
    claimsServiceDID: 'did:web:staging.claims.web3.storage',
    uploadService: 'https://staging.up.storacha.network',
    uploadServiceDID: 'did:web:staging.up.storacha.network',
    gatewayService: 'https://freeway-fforbeck.protocol-labs.workers.dev',
    // gatewayService: 'https://gateway.storacha.network',
    gatewayServiceDID: 'did:web:staging.w3s.link',
    storageProviders: ['did:web:staging.web3.storage', 'did:web:staging.up.storacha.network'],
  },
}

/**
 * Get current environment from ENV var or default to production
 */
const getEnvironment = () => {
  const env = process.env.STORACHA_ENV || 'production'
  if (!ENVIRONMENTS[env]) {
    throw new Error(`Invalid STORACHA_ENV: ${env}. Must be 'production' or 'staging'`)
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
    blobRegistry: process.env.BLOB_REGISTRY_TABLE_NAME || `${env.tablePrefix}-blob-registry`,
    store: process.env.STORE_TABLE_NAME || `${env.tablePrefix}-store`, // Legacy table
    allocations: process.env.ALLOCATIONS_TABLE_NAME || `${env.tablePrefix}-allocation`, // Billing table (also has blob info)
    consumer: process.env.CONSUMER_TABLE_NAME || `${env.tablePrefix}-consumer`, // Space ownership (space -> customer mapping)
    subscription: process.env.SUBSCRIPTION_TABLE_NAME || `${env.tablePrefix}-subscription`, // Billing subscriptions (customer -> provider relationship)
    delegation: process.env.DELEGATION_TABLE_NAME || `${env.tablePrefix}-delegation`, // Delegation storage
    migrationSpaces: process.env.MIGRATION_SPACES_TABLE_NAME || `${process.env.STORACHA_ENV || 'prod'}-migration-spaces`, // Migration space tracking
    migrationProgress: process.env.MIGRATION_PROGRESS_TABLE_NAME || `${process.env.STORACHA_ENV === 'staging' ? 'staging' : 'prod'}-migration-progress`, // Migration progress tracking
  },
  
  services: {
    indexingServiceURL: process.env.INDEXING_SERVICE_URL || env.indexingService,
    contentClaimsServiceURL: process.env.CONTENT_CLAIMS_SERVICE_URL || env.contentClaims,
    claimsServiceDID: process.env.CLAIMS_SERVICE_DID || env.claimsServiceDID,
    uploadServiceURL: process.env.UPLOAD_SERVICE_URL || env.uploadService,
    uploadServiceDID: process.env.UPLOAD_SERVICE_DID || env.uploadServiceDID,
    gatewayServiceURL: process.env.GATEWAY_SERVICE_URL || env.gatewayService,
    gatewayServiceDID: process.env.GATEWAY_SERVICE_DID || env.gatewayServiceDID,
    storageProviders: env.storageProviders, // Provider DIDs for querying consumer table
  },
  
  storage: {
    carparkBucket: process.env.CARPARK_BUCKET || env.carparkBucket,
    delegationBucket: process.env.DELEGATION_BUCKET_NAME || `${env.tablePrefix}-delegation`,
    carparkPublicUrl: process.env.CARPARK_PUBLIC_URL || env.carparkPublicUrl,
  },
  
  migration: {
    batchSize: parseInt(process.env.BATCH_SIZE || '100', 10),
    maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || '10', 10),
    dryRun: process.env.DRY_RUN === 'true',
  },
  
  credentials: {
    // Service private key for publishing to upload service
    uploadServicePrivateKey: process.env.SERVICE_PRIVATE_KEY,
    // Claims service private key for publishing location claims
    claimsServicePrivateKey: process.env.CLAIMS_SERVICE_PRIVATE_KEY,
    // Gateway private key for publishing delegations
    gatewayPrivateKey: process.env.GATEWAY_PRIVATE_KEY,
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
 * Validate required configuration
 */
export function validateConfig() {
  const required = [
    ['AWS_REGION', config.aws.region],
    ['UPLOAD_TABLE_NAME', config.tables.upload],
  ]
  
  const missing = required.filter(([name, value]) => !value).map(([name]) => name)
  
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`)
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
  const claimsKeyPair = await Signer.parse(config.credentials.claimsServicePrivateKey)
  return claimsKeyPair.withDID(config.services.claimsServiceDID)
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
  const uploadKeyPair = await Signer.parse(config.credentials.uploadServicePrivateKey)
  return uploadKeyPair.withDID(config.services.uploadServiceDID)
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
  const gatewayKeyPair = await Signer.parse(config.credentials.gatewayPrivateKey)
  return gatewayKeyPair.withDID(config.services.gatewayServiceDID)
}
