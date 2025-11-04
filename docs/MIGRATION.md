# Migration Guide

Execute the migration on EC2 instances to process customer uploads.

## Quick Start

```bash
# On EC2 Instance 1
node src/migrate.js --customers-file migration-state/instance-1-customers.json --limit 1000

# On EC2 Instance 2
node src/migrate.js --customers-file migration-state/instance-2-customers.json --limit 1000

# ... etc
```

## What It Does

The migration script:
1. Reads customer list from the provided JSON file
2. Spawns multiple workers to process spaces in parallel
3. For each upload:
   - Checks migration status (index, location claims)
   - Generates and registers index if needed
   - Republishes location claims with space information
   - Creates gateway authorization (when implemented)
4. Tracks progress in DynamoDB (`migration-progress` table)
5. Creates local checkpoints for fast recovery

## Prerequisites

- Distribution files created by `setup-distribution.js`
- DynamoDB `migration-progress` table created
- `.env` file configured with AWS credentials

## Command Options

```bash
node src/migrate.js [options]

Options:
  --customers-file <path>     Path to customers JSON file (required for distributed migration)
  --limit <N>                 Number of uploads to process (default: 10)
  --concurrency <N>           Number of concurrent migrations (default: 1)
  --test-index                Test mode: Only test index generation
  --test-location-claims      Test mode: Only test location claims
  --verify-only               Verify migration status without making changes
```

## Examples

### Basic Migration

```bash
# Run migration with customer file
node src/migrate.js --customers-file migration-state/instance-1-customers.json --limit 1000
```

### Without Customer File

```bash
# Migrate from all uploads (no filtering)
node src/migrate.js --limit 100
```

### Test Mode

```bash
# Test with customer file
node src/migrate.js --customers-file migration-state/instance-1-customers.json --test-index --limit 10
```

## Migration Steps

For each upload, the migration performs:

### 1. Check Migration Status
- Query indexing service for existing index
- Check location claims for space information
- Determine required actions

### 2. Generate & Register Index
- Create sharded DAG index if missing
- Upload index to storage
- Register with `space/blob/add` and `assert/index`

### 3. Republish Location Claims
- Add space information to location claims
- Publish updated claims to indexing service

### 4. Create Gateway Authorization
- Generate gateway delegation (when implemented)
- Enable content serving with egress tracking

## Progress Tracking

**DynamoDB Progress Table:**
- Tracks space-level progress
- Records: status, uploads completed, timestamps
- Enables resume on failure

**Local Checkpoints:**
- Fast recovery from crashes
- Saved to `migration-state/instance-N-checkpoint.json`
- Updated every N uploads (configurable)

## Resume Capability

The migration is fully resumable:
- DynamoDB tracks which spaces are completed
- Local checkpoints track upload-level progress
- Re-run the same command to resume automatically
- Idempotent: safe to run multiple times

## Monitoring

While migration is running, monitor progress:

```bash
# Overall statistics
node src/migration-monitor.js

# Live monitoring (refresh every 30s)
node src/migration-monitor.js --watch
```

See [Monitoring Guide](MONITORING.md) for details.

## Performance

**Throughput:**
- ~27 uploads/minute per worker
- 10 workers = ~270 uploads/minute per instance
- 5 instances = ~1,350 uploads/minute total

**Resource Usage:**
- CPU: Moderate (index generation, UCAN signing)
- Memory: ~2GB per instance
- Network: High (DynamoDB, S3, API calls)
- Disk: Minimal (checkpoints only)

## Error Handling

**Automatic Retry:**
- Network errors: 3 retries with exponential backoff
- Rate limiting: Automatic backoff and retry
- Transient failures: Marked for retry

**Failed Migrations:**
- Tracked in DynamoDB with error message
- Can be queried: `node src/migration-monitor.js --failed`
- Can be retried manually or automatically

## Testing

Before running full migration, test on a small sample:

```bash
# Test index generation
node src/migrate.js --test-index --limit 10

# Test location claims
node src/migrate.js --test-location-claims --limit 10

# Test specific upload
node src/migrate.js --test-index \
  --space did:key:z6Mk... \
  --cid bafkreieqxb4...
```

## Next Steps

After starting migration:
1. Monitor progress with [Monitoring Guide](MONITORING.md)
2. Check for failed/stuck migrations
3. Adjust worker count if needed
