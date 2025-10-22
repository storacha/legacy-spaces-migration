# Legacy Spaces Migration

Migration tooling to move legacy content from old indexing systems to the modern sharded DAG index system.

## Goals

1. **Estimate costs** to build sharded DAG indices using the Index Worker
2. **Build sharded DAG indices** where needed
3. **Add space information** to location claims
4. **Create gateway delegations** for content serving
5. **Publish indices** to enable the indexing service
6. **Publish location claims** with space information if needed
7. **Publish public gateway authorizations** for content serving and egress tracking

## Setup

### 1. Install Dependencies

```bash
pnpm install
```

### 2. Configure Environment Variables

Copy the example environment file and configure your AWS credentials:

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

### 3. Test Index Generation

Test the complete index generation flow on a single upload:

```bash
# Test specific upload (requires space and CID)
node src/migrate.js --test-index \
  --space did:key:z6Mki2bMA7RKuhtNbGpEQdfBn1gWzSDyRs1Akytx6giHKxRJ \
  --cid bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu
```

**Example output:**
```
Legacy Content Migration - Step 1: Index Generation Test
==================================================

Querying upload by CID: bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu
Using space: did:key:z6Mki2bMA7RKuhtNbGpEQdfBn1gWzSDyRs1Akytx6giHKxRJ
Found upload in space: did:key:z6Mki2bMA7RKuhtNbGpEQdfBn1gWzSDyRs1Akytx6giHKxRJ


Testing Index Generation
==================================================
Root CID (Upload): bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu
Space: did:key:z6Mki2bMA7RKuhtNbGpEQdfBn1gWzSDyRs1Akytx6giHKxRJ
Shards: 1

  Generating DAG index for bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu...
  Querying blob registry for shard sizes...
    ✓ bagbaiera7vycsplauivbkhibstd2vhkz6gamc6yb5uacw6jkirsufrmt3oka: 8409 bytes
Generating sharded index for bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu with 1 shards...
Building index for zQmfPy42F7kg9w1eY8YWvq2989ufv8wKJFvA2K4RfYfk4KV/zQmfPy42F7kg9w1eY8YWvq2989ufv8wKJFvA2K4RfYfk4KV.blob?offset=0...
Built index for zQmfPy42F7kg9w1eY8YWvq2989ufv8wKJFvA2K4RfYfk4KV/zQmfPy42F7kg9w1eY8YWvq2989ufv8wKJFvA2K4RfYfk4KV.blob with 1 blocks

Building Sharded DAG Index
==================================================

Shard: zQmfPy42F7kg9w1eY8YWvq2989ufv8wKJFvA2K4RfYfk4KV
  Slices (2):
    zQmfPy42F7kg9w1eY8YWvq2989ufv8wKJFvA2K4RfYfk4KV @ 0-8409
    zQmY5aZff9bdqtcS5pPQFyCCD7VUajnPdxED5iswgSfYWe8 @ 97-8409

Sharded index for bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu generated with success
    ✓ Generated index: bagbaieravpqlox52iz7p5i7ack44q2vml7ykmzkaid3c3ecw4czw26csjyla (408 bytes)

Metadata
  CID: bagbaieravpqlox52iz7p5i7ack44q2vml7ykmzkaid3c3ecw4czw26csjyla
  Size: 408 bytes
  Multihash: zQmZubAazYZAebEmj7nAhH96AeQufizRTRaj8ZYQn4kd1cy


==================================================
SUCCESS: Index generated successfully!
==================================================
```

## Usage

### Test Index Generation

Test on a single upload before running full migration:

```bash
# Test specific upload (requires space and CID)
node src/migrate.js --test-index \
  --space did:key:z6Mki2bMA7RKuhtNbGpEQdfBn1gWzSDyRs1Akytx6giHKxRJ \
  --cid bafkreieqxb4eiieaswm3iixmmgzxjwzguzpcwbjz762hmtz2ndbekmjecu
```

### Cost Estimation

Before running the full migration, estimate the costs and duration using the sampling tool.

#### Quick Start

Run a sample of 2,000 uploads to estimate costs:

```bash
node src/estimate-costs.js --sample 2000 --total 37028823
```

#### Command Options

```bash
node src/estimate-costs.js [options]

Options:
  --sample <N>      Number of uploads to sample (default: 1000)
  --total <N>       Total uploads in database for extrapolation
  --space <DID>     Filter to specific space DID (optional)
  --dry-run         Only count shards without calling index worker
```

## Architecture

### Three Legacy Systems

1. **Block Location Table** (oldest) - DynamoDB table with block-level locations
2. **Content Claims Service** (middle) - Claims stored in S3/buckets
3. **Sharded DAG Indices** (target) - Modern system

