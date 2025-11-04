# Legacy Spaces Migration

Migration tooling to move legacy content from old indexing systems to the modern sharded DAG index system.

## Goals

1. ✅ **Estimate costs** to build sharded DAG indices using the Index Worker
2. ✅ **Build sharded DAG indices** where needed (using Index Worker)
3. ✅ **Upload and register indices** to migration spaces via space/blob/add and assert/index
4. ✅ **Republish location claims** with space information for egress tracking
5. ✅ **Create gateway delegations** for content serving
6. ✅ **Migration Data Analysis** to understand the worklaod
7. ✅ **Migration State Management & Monitoring** to retry, resume and watch the migration process

## Setup

### 1. Install Dependencies

```bash
pnpm install
```

### 2. Configure Environment Variables

Create a `.env` file based on `.env.example`:

**The easiest way to switch between environments is using `STORACHA_ENV`:**

```bash
# .env file

# For staging (us-east-2, staging-w3infra-* tables)
STORACHA_ENV=staging

# For production (us-west-2, prod-w3infra-* tables) - default
# STORACHA_ENV=production

# AWS credentials (same for both environments)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# Service credentials
SERVICE_PRIVATE_KEY=your_base64_encoded_ed25519_private_key
```

This automatically configures:
- **AWS Region** (staging: `us-east-2`, production: `us-west-2`)
- **Table names** (`staging-w3infra-*` vs `prod-w3infra-*`)
- **Service URLs** (`staging.*` vs `production.*`)
- **R2 buckets** (`carpark-staging-0` vs `carpark-prod-0`)

Alternatively, you can configure your environment variables manually:

```bash
cp .env.example .env
```

Edit `.env` and set the following required variables:

```bash
# AWS Configuration
AWS_REGION=us-west-2                          # Your AWS region
AWS_ACCESS_KEY_ID=your_access_key             # IAM user access key
# Create a new UAM user with the following access policy: arn:aws:iam::505595374361:policy/legacy-spaces-migration-access
AWS_SECRET_ACCESS_KEY=your_secret_key         # IAM user secret key

# DynamoDB Tables
UPLOAD_TABLE_NAME=prod-w3infra-upload         # Upload table name
STORE_TABLE_NAME=prod-w3infra-store           # Legacy store table
BLOB_REGISTRY_TABLE_NAME=prod-w3infra-blob-registry  # Blob registry table
ALLOCATION_TABLE_NAME=prod-w3infra-allocation # Allocation/billing table

# Services
INDEXING_SERVICE_URL=https://indexer.storacha.network
CONTENT_CLAIMS_SERVICE_URL=https://claims.web3.storage

# R2/S3 Storage
CARPARK_BUCKET=carpark-prod-0
CARPARK_PUBLIC_URL=https://carpark-prod-0.r2.w3s.link
```

**AWS IAM Permissions Required:**
- `dynamodb:Query` - Read from upload, store, blob-registry, and allocation tables
- `dynamodb:GetItem` - Get specific items from tables
- `dynamodb:Scan` - Scan tables for sampling (optional)
- See policy: `arn:aws:iam::505595374361:policy/legacy-spaces-migration-access`

### 3. Verify Configuration

The script will display the active environment at startup:

```bash
node src/migrate.js --test-index --limit 1
```

```
Environment Configuration
==================================================
  Environment: production
  AWS Region: us-west-2
  Upload Service: https://up.storacha.network
  Upload Table: prod-w3infra-upload
==================================================
```

### Switch Environments:

```bash
# For staging
STORACHA_ENV=staging node src/migrate.js --test-index --limit 1

# For production (default)
STORACHA_ENV=production node src/migrate.js --test-index --limit 1

# Or set in .env file
echo "STORACHA_ENV=staging" >> .env
```

## Usage

The migration process has 4 main steps:

### 1. Estimate Costs

Analyze the workload and estimate time/costs:

```bash
node src/estimate-costs.js --sample 2000 --total 37028823
```

[📖 Detailed cost estimation guide](docs/COST_ESTIMATION.md)

### 2. Setup Distribution

Distribute customers across EC2 instances:

```bash
node src/setup-distribution.js --instances 5
```

[📖 Detailed setup guide](docs/SETUP_DISTRIBUTION.md)

### 3. Run Migration

Execute the migration on each EC2 instance:

```bash
node src/migrate.js --customers-file migration-state/instance-1-customers.json --limit 1000
```

[📖 Detailed migration guide](docs/MIGRATION.md)

### 4. Monitor Progress

Track migration status in real-time:

```bash
node src/migration-monitor.js --watch
```

[📖 Detailed monitoring guide](docs/MONITORING.md)

### Example Output

```
Legacy Content Migration - Full Migration
======================================================================

Configuration:
  Mode: Full Migration
  Limit: 10 uploads
  Concurrency: 1


[1/10]

======================================================================
📦 Migrating Upload: bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu
   Space: did:key:z6Mki2bMA7RKuhtNbGpEQdfBn1gWzSDyRs1Akytx6giHKxRJ
   Shards: 1
======================================================================

1) Checking migration status...
  Checking location claims for 1 shards...
    Found 2 location claims from indexing service
    ✗ bagbaiera...: location claim missing space field

::: Migration Status:::
   Index: ✓ EXISTS
   Location claims: ✓ EXISTS
   Location has space: ✗ NO
   Shards needing location claims: 1/1

!!! Actions Required!!!
   ✓ Generate and register index
   ☐ Republish location claims with space
   ☐ Create gateway authorization

⏭  Index already exists, skipping

3)  Republishing location claims with space...
    ✓ bagbaiera...

    ✓ Successfully republished 1 location claims

⏭  Skipping gateway auth (test mode: null)

======================================================================
✅ Migration complete for bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu
======================================================================
```

---

## Architecture

### Three Legacy Systems

1. **Block Location Table** (oldest) - DynamoDB table with block-level locations
2. **Content Claims Service** (middle) - Claims stored in S3/buckets
3. **Sharded DAG Indices** (target) - Modern system

