# Monitoring Guide

Monitor migration progress in real-time using the monitoring script.

## Quick Start

```bash
# Overall migration statistics
node src/migration-monitor.js

# Watch mode (refresh every 30s)
node src/migration-monitor.js --watch
```

## Command Options

```bash
node src/migration-monitor.js [options]

Options:
  --watch                Live monitoring (refresh every 30s)
  --customer <DID>       Show customer progress
  --space <DID>          Show space status (requires --customer)
  --instance <N>         Show instance progress
  --failed               Show failed migrations
  --stuck                Show stuck migrations (>1 hour)
```

## Examples

### Overall Statistics

```bash
node src/migration-monitor.js
```

Output:
```
Migration Progress Overview
======================================================================

Environment: production
Region: us-west-2
Table: prod-migration-progress

Total Spaces: 80,304
  🟢 Completed: 45,123 (56.2%)
  🔵 In Progress: 234
  🟡 Pending: 34,947
  🔴 Failed: 0

Total Uploads: 37,088,337
  🟢 Completed: 20,845,123 (56.2%)

Progress by Instance:
----------------------------------------------------------------------
Instance 1:
  Spaces: 8,234/16,060 (51.3%)
  Uploads: 4,653,123/8,295,089 (56.1%)
...
```

### Customer Progress

```bash
node src/migration-monitor.js --customer did:mailto:user@example.com
```

Shows all spaces for a customer with migration status.

### Space Status

```bash
node src/migration-monitor.js \
  --customer did:mailto:user@example.com \
  --space did:key:z6Mk...
```

Shows detailed status for a specific space.

### Instance Progress

```bash
node src/migration-monitor.js --instance 1
```

Shows progress for a specific EC2 instance, grouped by worker.

### Failed Migrations

```bash
node src/migration-monitor.js --failed
```

Lists all failed migrations with error details.

### Stuck Migrations

```bash
node src/migration-monitor.js --stuck
```

Shows in-progress migrations that haven't updated in over 1 hour.

### Live Monitoring

```bash
node src/migration-monitor.js --watch
```

Auto-refreshes every 30 seconds. Press `Ctrl+C` to stop.

## Understanding the Output

**Status Icons:**
- 🟢 Completed - Migration finished successfully
- 🔵 In Progress - Currently being processed
- 🟡 Pending - Not yet started
- 🔴 Failed - Migration failed with error

**Progress Metrics:**
- Total spaces and uploads
- Completion percentage
- Progress by instance
- Worker-level breakdown

## DynamoDB Table

The monitoring script reads from the `migration-progress` table:

**Table Schema:**
- PK: customer (Customer DID)
- SK: space (Space DID)
- Attributes: status, totalUploads, completedUploads, instanceId, workerId, error, timestamps

**GSI:** status-index (for querying by status)

## Progress Updates

Workers update progress:
- **On space start:** Create initial progress record
- **Every 1000 uploads:** Update completedUploads
- **On completion:** Mark status as completed
- **On failure:** Mark status as failed with error

## Troubleshooting

### No Data Showing

- Verify `migration-progress` table exists
- Check AWS credentials
- Ensure migration workers have started

### Stuck Migrations

If migrations are stuck (>1 hour):
1. Check if EC2 instance is running
2. Review instance logs
3. Restart the worker (migration is idempotent)

### Failed Migrations

For failed migrations:
1. Review error message
2. Check if transient (network) or permanent (data)
3. Restart worker for transient errors
4. Investigate data for permanent errors

## Performance

- **Scan operations** (overall, failed, stuck) - may be slow with millions of records
- **Query operations** (customer, space) - fast, uses primary key
- **Watch mode** - queries every 30s, use sparingly
- Use **instance** view for focused monitoring

## Next Steps

- Monitor progress during migration
- Check for failed/stuck migrations regularly
- Adjust worker count if needed
- Review [Migration Guide](MIGRATION.md) for troubleshooting
