# Legacy Spaces Migration

Migration tooling to move legacy content from old indexing systems to the modern sharded DAG index system.

## Goals

1. **Analyze legacy content** to determine what needs migration
2. **Add space information** to location claims
3. **Build/migrate sharded DAG indices** where needed
4. **Create gateway delegations** for content serving

## Architecture

### Three Legacy Systems

1. **Block Location Table** (oldest) - DynamoDB table with block-level locations
2. **Content Claims Service** (middle) - Claims stored in S3/buckets
3. **Sharded DAG Indices** (target) - Modern system

