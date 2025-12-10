# Legacy Spaces Migration Guide

This guide walks through the complete process of running the legacy spaces migration, from cost estimation to execution and monitoring.

## Prerequisites

1. **Environment Setup**
   ```bash
   # Clone and install dependencies
   cd legacy-spaces-migration
   pnpm install
   ```

2. **Configure Environment**
   ```bash
   # Copy and configure .env file
   cp .env.example .env
   
   # Set environment (staging or production)
   export STORACHA_ENV=staging  # or production
   # Other environment variables are set in .env
   ```

3. **AWS Credentials**
   - Ensure AWS credentials are configured for DynamoDB access
   - Verify access to required tables (uploads, allocations, store, etc.)

## Step 1: Setup Distribution (Optional)

For large-scale migrations across multiple customers, use the distribution tool to analyze and distribute workload.

```bash
# Analyze customer distribution (see what needs migration)
STORACHA_ENV=staging node src/setup-distribution.js --analyze

# Generate distribution for 5 instances (for parallel migration)
STORACHA_ENV=staging node src/setup-distribution.js --instances 5
```

See [Helper Scripts](#helper-scripts) section for detailed documentation.

## Step 2: Verify Customer Status

Check the current migration status for your customer account.

```bash
# Verify migration status for a customer
STORACHA_ENV=staging node src/migrate.js \
  --verify-only \
  --customer did:mailto:storacha.network:<name>
```

**This shows:**
- Which uploads are already migrated (✅ Passed)
- Which uploads need migration (❌ Failed)
- What's missing: index claims, location claims with space, gateway auth
- Summary statistics and failure breakdown

**Example output:**
```
MIGRATION SUMMARY
Total processed:     10
Successful:        6 (60%)
Failed:            4 (40%)

Failed Uploads (4):
  bafkreiglr7x3n5f...
    index claim missing, 1 shards missing space info
  bafkreiazlapcpxt...
    1 shards missing space info
```

**Note:** For large customers, use `--limit` to sample instead of checking all uploads.

**Alternative targets:**
```bash
# Verify specific space
STORACHA_ENV=staging node src/migrate.js \
  --verify-only \
  --space did:key:z6Mkk...space

# Verify specific upload
STORACHA_ENV=staging node src/migrate.js \
  --verify-only \
  --cid bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u \
  --space did:key:z6Mkk...space
```

## Step 3: Run Migration

Migrate all uploads for a customer account (unlimited by default).

```bash
# Migrate all uploads for a customer
STORACHA_ENV=staging node src/migrate.js \
  --customer did:mailto:storacha.network:your-email \
  --concurrency 5
```

**What this does:**
1. Queries all uploads for the customer
2. Checks what migration steps are needed for each upload
3. Generates and registers DAG indices (if needed)
4. Republishes location claims with space info (if needed)
5. Creates gateway authorizations (if needed)
6. Verifies all steps completed successfully

**Options:**
```bash
# Limit to first N uploads (for testing)
STORACHA_ENV=staging node src/migrate.js \
  --customer did:mailto:storacha.network:your-email \
  --limit 100 \
  --concurrency 5

# Migrate from customer list file
STORACHA_ENV=staging node src/migrate.js \
  --customers-file customers.json \
  --concurrency 10
```

**Alternative targets:**
```bash
# Migrate specific space
STORACHA_ENV=staging node src/migrate.js \
  --space did:key:z6Mkk...space \
  --concurrency 5

# Migrate specific upload
STORACHA_ENV=staging node src/migrate.js \
  --cid bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u \
  --space did:key:z6Mkk...space
```

## Step 4: Monitor Migration

### Real-time Monitoring

The migration script outputs detailed progress:

```
Processing upload 1/100: bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u
  Space: did:key:z6Mkk...
  Shards: 1
  
  Step 1: Check migration status...
    ✓ Index claim exists
    ⚠️  Location claims missing space info (1 shards)
    ⚠️  Gateway auth missing
  
  Step 3: Republish location claims...
    ✓ Published location claim for shard bagbaiera...
  
  Step 4: Create gateway authorization...
    ✓ Gateway authorization created
  
  Step 5: Verify migration...
    ✓ All checks passed
  
✅ Migration complete for bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u

Summary:
  Total processed: 100
  Successful: 98
  Failed: 2
  Skipped: 0
```

### Verify Specific Upload

```bash
# Verify a specific upload was migrated correctly
STORACHA_ENV=staging node src/migrate.js \
  --verify-only \
  --cid bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u
```

### Troubleshooting with Test Modes

If migrations are failing, use test modes to isolate which step is causing issues. Test with the specific failing upload to pinpoint the problem:

```bash
# Test index generation for a specific failing upload
STORACHA_ENV=staging node src/migrate.js \
  --test-index \
  --cid bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u \
  --space did:key:z6Mkk...space

# Test location claims for a specific failing upload
STORACHA_ENV=staging node src/migrate.js \
  --test-location-claims \
  --cid bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u \
  --space did:key:z6Mkk...space

# Test gateway authorization for a specific failing upload
STORACHA_ENV=staging node src/migrate.js \
  --test-gateway-auth \
  --cid bafkreih7ipnmjm2kbug4nveiooyqwuq4muuul7rbkedazpjqv2weokcc7u \
  --space did:key:z6Mkk...space

# Or test on a sample from customer
STORACHA_ENV=staging node src/migrate.js \
  --test-index \
  --customer did:mailto:storacha.network:<name> \
  --limit 10
```

**These modes:**
- Run only the specified migration step
- Skip other steps
- Use `--cid` + `--space` to test specific failing uploads
- Use `--customer` + `--limit` to test on a sample
- Helps pinpoint exactly which step is failing

## Migration Parameters

### Required Parameters

- `--limit` or `--cid` or `--customer` or `--space` - Specify what to migrate

### Optional Parameters

- `--concurrency N` - Number of concurrent migrations (default: 1)
- `--customers-file FILE` - Path to customer distribution file
- `--verify-only` - Only verify, don't make changes
- `--test-index` - Only test index generation
- `--test-location-claims` - Only test location claims
- `--test-gateway-auth` - Only test gateway authorization

## Migration States

Each upload can be in one of these states:

1. **Not Started** - No index, no space in claims, no gateway auth
2. **Index Only** - Index exists, but location claims missing space
3. **Claims Only** - Location claims have space, but no gateway auth
4. **Complete** - Index + location claims with space + gateway auth

The migration script automatically detects the current state and only runs needed steps.

## Error Handling

### Common Errors

**Blob not found in carpark**
```
✗ FAILED: Blob not found in carpark
```
- Shard CAR file doesn't exist in R2
- Upload metadata exists but actual data is missing
- Skip this upload or investigate data loss

**Indexing service timeout**
```
✗ FAILED: Request timeout
```
- Indexing service is slow or down
- Retry with lower concurrency
- Check indexing service health

**Gateway authorization failed**
```
✗ FAILED: Claim {"can":"access/delegate"} is not authorized
```
- Gateway doesn't have validator proofs
- Check GATEWAY_VALIDATOR_PROOF environment variable
- Verify attestation chain is correct

### Retry Failed Migrations

```bash
# Re-run migration for failed uploads
# The script will skip already-migrated uploads
STORACHA_ENV=staging node src/migrate.js \
  --customer did:key:z6Mkk...customer \
  --concurrency 5
```

## Helper Scripts

### setup-distribution.js

Discovers all customers from the Upload Table and distributes them across multiple EC2 instances for parallel migration. Uses parallel DynamoDB scan for faster discovery.

```bash
# Analyze customer distribution (dry run)
STORACHA_ENV=staging node src/setup-distribution.js --analyze

# Analyze with more parallel segments for faster scanning (1-10)
STORACHA_ENV=staging node src/setup-distribution.js --analyze --parallel-segments 8

# Generate distribution for 5 instances
STORACHA_ENV=staging node src/setup-distribution.js --instances 5

# Generate distribution with filters
STORACHA_ENV=staging node src/setup-distribution.js \
  --instances 5 \
  --min-uploads 100

# Estimate with different worker counts
STORACHA_ENV=staging node src/setup-distribution.js \
  --instances 5 \
  --workers-per-instance 15
```

**Output:**
- Creates `migration-state/instance-N-customers.json` files (one per instance)
- Load-balanced by upload count per customer
- Includes time estimates and cost projections
- Shows customer distribution analysis and top customers

**Features:**
- Parallel DynamoDB scanning (4 segments by default)
- Resume capability with checkpoints
- Greedy load balancing across instances
- Empty space detection and filtering
- Migration time estimates based on worker count

### estimate-costs.js

Samples uploads from the database and estimates the cost of generating indices using the Cloudflare Workers index service.

```bash
# Dry run - just count shards without calling worker (fast)
STORACHA_ENV=staging node src/estimate-costs.js --sample 1000 --dry-run

# Live run - actually call index worker to measure timing (slower but accurate)
STORACHA_ENV=staging node src/estimate-costs.js --sample 100

# With total uploads count for cost extrapolation
STORACHA_ENV=staging node src/estimate-costs.js \
  --sample 100 \
  --total 3115

# Filter by specific space
STORACHA_ENV=staging node src/estimate-costs.js \
  --sample 500 \
  --space did:key:z6Mk...
```

**Modes:**
- **Dry run** (`--dry-run`): Only counts shards, no API calls (fast)
- **Live mode** (default): Calls index worker, measures real execution time

**Output:**
- Sample statistics (uploads, shards, distribution)
- Worker execution metrics (requests, time per upload)
- Cost estimates for Cloudflare Workers:
  - Free tier (100K requests/day)
  - Paid tier ($0.50 per million requests)
  - Bundled plan ($5/month)
- Recommended pricing tier

**Parameters:**
- `--sample` or `-s`: Number of uploads to sample (default: 1000)
- `--total` or `-t`: Total uploads in DB for cost extrapolation
- `--space`: Filter by specific space DID
- `--dry-run`: Skip worker calls, just count shards

### migration-monitor.js

Monitor migration progress from DynamoDB with rich formatting and multiple views.

```bash
# Overall migration statistics
STORACHA_ENV=staging node src/migration-monitor.js

# Customer progress (all spaces for a customer)
STORACHA_ENV=staging node src/migration-monitor.js \
  --customer did:key:z6Mkk...customer

# Specific space status
STORACHA_ENV=staging node src/migration-monitor.js \
  --customer did:key:z6Mkk...customer \
  --space did:key:z6Mkk...space

# Instance progress
STORACHA_ENV=staging node src/migration-monitor.js --instance 1

# Show failed migrations
STORACHA_ENV=staging node src/migration-monitor.js --failed

# Show stuck migrations (in-progress >1 hour)
STORACHA_ENV=staging node src/migration-monitor.js --stuck

# Live monitoring (refresh every 30s)
STORACHA_ENV=staging node src/migration-monitor.js --watch
```

**Views:**
- **Overall stats**: Total spaces, completion %, progress by instance
- **Customer progress**: All spaces for a customer with status
- **Space status**: Detailed status for a specific space
- **Instance progress**: Progress breakdown by instance and worker
- **Failed migrations**: List of failed spaces with errors
- **Stuck migrations**: In-progress spaces stuck >1 hour

**Output includes:**
- 🟢 Completed, 🔵 In Progress, 🟡 Pending, 🔴 Failed
- Upload counts and percentages
- Instance and worker assignments
- Error messages for failures
- Last update timestamps

**Parameters:**
- `--customer`: Query specific customer DID
- `--space`: Query specific space DID (requires --customer)
- `--instance`: Show progress for specific instance
- `--failed`: Show failed migrations
- `--stuck`: Show stuck migrations (>1 hour)
- `--watch`: Live monitoring (refresh every 30s)

## Best Practices

1. **Use Verify-Only** - Check current state before making changes
2. **Staged Rollout** - Migrate staging first, then production
3. **Customer-by-Customer** - Migrate one customer at a time for easier analysis
4. **Low Concurrency** - Start with `--concurrency 5`, increase if stable
5. **Check Logs** - Monitor for errors and adjust accordingly
6. **Use Helper Scripts** - Leverage `setup-distribution.js` and `migration-monitor.js`

## Production Checklist

Before running production migration

- [ ] Successfully migrated staging environment
- [ ] Verified migrated uploads in staging
- [ ] Estimated production costs
- [ ] Prepared customer distribution list
- [ ] Set up monitoring
- [ ] Coordinated with team on timing
- [ ] Set `STORACHA_ENV=production`
- [ ] Start small and some sample customers
- [ ] Monitor for errors before scaling up
- [ ] Launch more instances if sample customers are migrating successfully

## Rollback

The is no way to rollback a migration. If something goes wrong, you will need to manually clean up the data.

1. **Stop Migration** - Cancel running migration script
2. **Verify State** - Use `--verify-only` to check what was migrated
3. **No Automatic Rollback** - Migration creates new data, doesn't delete old
4. **Manual Cleanup** - If needed, manually remove:
   - Gateway authorizations (KV store)
   - Location claims (indexing service)
   - Index claims (indexing service)

Note: Migration is designed to be idempotent - re-running is safe.
