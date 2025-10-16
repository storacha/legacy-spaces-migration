/**
 * Configuration for legacy spaces migration
 */

export const config = {
  aws: {
    region: process.env.AWS_REGION || 'us-west-2',
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  
  tables: {
    upload: process.env.UPLOAD_TABLE_NAME || 'prod-w3infra-upload',
    blobRegistry: process.env.BLOB_REGISTRY_TABLE_NAME || 'prod-w3infra-blob-registry',
  },
  
  services: {
    indexingService: process.env.INDEXING_SERVICE_URL || 'https://indexer.storacha.network',
    contentClaims: process.env.CONTENT_CLAIMS_SERVICE_URL || 'https://claims.web3.storage',
  },
  
  storage: {
    carparkBucket: process.env.CARPARK_BUCKET || 'carpark-prod-0',
    carparkPublicUrl: process.env.CARPARK_PUBLIC_URL || 'https://carpark-prod-0.r2.w3s.link',
  },
  
  migration: {
    batchSize: parseInt(process.env.BATCH_SIZE || '100', 10),
    maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || '10', 10),
    dryRun: process.env.DRY_RUN === 'true',
  },
  
  admin: {
    privateKey: process.env.ADMIN_PRIVATE_KEY,
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
