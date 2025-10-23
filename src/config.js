/**
 * Configuration for the legacy spaces migration tool
 */

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
    uploadService: 'https://up.storacha.network',
  },
  staging: {
    region: 'us-east-2',
    tablePrefix: 'staging-w3infra',
    carparkBucket: 'carpark-staging-0',
    carparkPublicUrl: 'https://carpark-prod-0.r2.w3s.link', // Use prod carpark for staging ?
    indexingService: 'https://staging.indexer.storacha.network',
    contentClaims: 'https://staging.claims.web3.storage',
    uploadService: 'https://staging.up.storacha.network',
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
  },
  
  services: {
    indexingService: process.env.INDEXING_SERVICE_URL || env.indexingService,
    contentClaims: process.env.CONTENT_CLAIMS_SERVICE_URL || env.contentClaims,
    uploadService: process.env.UPLOAD_SERVICE_URL || env.uploadService,
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
    // Service private key for publishing claims and acting on behalf of spaces
    servicePrivateKey: process.env.SERVICE_PRIVATE_KEY,
    serviceDID: process.env.SERVICE_DID || 'did:web:up.storacha.network',
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
