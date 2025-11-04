# Migration Monitoring

Monitor the progress of the legacy spaces migration using the `migration-monitor.js` script.

## DynamoDB Table Structure

The migration uses a `migration-progress` table to track space-level progress:

### Table Schema

```
Primary Key:
  - PK: customer (string) - Customer DID
  - SK: space (string) - Space DID

Attributes:
  - status (string) - 'pending' | 'in-progress' | 'completed' | 'failed'
  - totalUploads (number) - Total uploads in this space
  - completedUploads (number) - Number of uploads migrated
  - lastProcessedUpload (string) - Last upload CID processed
  - instanceId (string) - EC2 instance processing this space
  - workerId (string) - Worker ID processing this space
  - error (string) - Error message if failed
  - createdAt (string) - ISO timestamp
  - updatedAt (string) - ISO timestamp

Global Secondary Index (GSI): status-index
  - PK: status
  - SK: updatedAt
```

### Table Operations

All table operations are available in `src/lib/tables/migration-progress-table.js`:

```javascript
import {
  createDynamoClient,
  getSpaceProgress,
  createSpaceProgress,
  updateSpaceProgress,
  markSpaceCompleted,
  markSpaceFailed,
  getCustomerSpaces,
  getFailedMigrations,
  getStuckMigrations,
  getInstanceSpaces,
  scanAllProgress,
} from './lib/tables/migration-progress-table.js'
```

## Usage

### Overall Statistics (Default)

Shows total spaces, uploads, completion percentage, and breakdown by instance:

```bash
node src/migration-monitor.js
```

**Output:**
```
Migration Progress Overview
======================================================================

Total Spaces: 80,304
  ✓ Completed: 45,123 (56.2%)
  ⏳ In Progress: 234
  ⏸️  Pending: 34,947
  ✗ Failed: 0

Total Uploads: 37,088,337
  ✓ Completed: 20,845,123 (56.2%)

Progress by Instance:
----------------------------------------------------------------------

Instance 1:
  Spaces: 8,234/16,060 (51.3%)
  Uploads: 4,653,123/8,295,089 (56.1%)

Instance 2:
  Spaces: 9,123/16,061 (56.8%)
  Uploads: 4,123,456/7,198,312 (57.3%)
...
```

### Customer Progress

Shows all spaces for a specific customer with their migration status:

```bash
node src/migration-monitor.js --customer did:mailto:example.com:user
```

**Output:**
```
Customer: did:mailto:example.com:user
======================================================================

Total Spaces: 4

Status:
  ✓ Completed: 2
  ⏳ In Progress: 1
  ⏸️  Pending: 1
  ✗ Failed: 0

Spaces:
----------------------------------------------------------------------
  ✓ did:key:z6Mkk... (1,234/1,234 uploads)
  ✓ did:key:z6Mkj... (5,678/5,678 uploads)
  ⏳ did:key:z6Mki... (234/1,000 uploads)
     Instance: 2, Worker: 5
     Updated: 10/31/2025, 3:15:23 PM
  ⏸️  did:key:z6Mkh...
```

### Space Status

Shows detailed status for a specific space (requires customer DID):

```bash
node src/migration-monitor.js --customer did:mailto:example.com:user --space did:key:z6Mkk...
```

**Output:**
```
Space Migration Status
======================================================================

Space: did:key:z6Mkk...
Customer: did:mailto:example.com:user
Status: ⏳ in-progress

Uploads: 234/1,000

Instance: 2
Worker: 5

Created: 10/31/2025, 2:00:00 PM
Updated: 10/31/2025, 3:15:23 PM

Last Upload: bafkreiabc123...
```

### Instance Progress

Shows progress for a specific EC2 instance, grouped by worker:

```bash
node src/migration-monitor.js --instance 1
```

**Output:**
```
Instance 1 Progress
======================================================================

Total Spaces: 16,060
  ✓ Completed: 8,234
  ⏳ In Progress: 45
  ⏸️  Pending: 7,781
  ✗ Failed: 0

Total Uploads: 8,295,089
  ✓ Completed: 4,653,123

Progress by Worker:
----------------------------------------------------------------------
  Worker 1: 823/1,606 completed
  Worker 2: 834/1,606 completed
    In Progress: 2
  Worker 3: 812/1,606 completed
    In Progress: 1
...
```

### Failed Migrations

Lists all failed migrations with error details:

```bash
node src/migration-monitor.js --failed
```

**Output:**
```
Failed Migrations
======================================================================

Total Failed: 3

✗ did:key:z6Mkk...
  Customer: did:mailto:example.com:user
  Instance: 2, Worker: 5
  Error: Connection timeout after 3 retries
  Updated: 10/31/2025, 2:45:12 PM

✗ did:key:z6Mkj...
  Customer: did:mailto:example.com:other
  Instance: 3, Worker: 2
  Error: Invalid delegation proof
  Updated: 10/31/2025, 1:23:45 PM
...
```

### Stuck Migrations

Shows in-progress migrations that haven't updated in over 1 hour:

```bash
node src/migration-monitor.js --stuck
```

**Output:**
```
Stuck Migrations (in-progress >1 hour)
======================================================================

Total Stuck: 2

⏳ did:key:z6Mkk...
  Customer: did:mailto:example.com:user
  Instance: 2, Worker: 5
  Stuck for: 127 minutes
  Progress: 234/1,000 uploads
  Last Update: 10/31/2025, 1:08:23 PM

⏳ did:key:z6Mkj...
  Customer: did:mailto:example.com:other
  Instance: 4, Worker: 8
  Stuck for: 93 minutes
  Progress: 567/2,000 uploads
  Last Update: 10/31/2025, 1:42:15 PM
```

### Live Monitoring

Auto-refreshes every 30 seconds for real-time monitoring:

```bash
node src/migration-monitor.js --watch
```

Press `Ctrl+C` to stop.

## Integration with Migration Scripts

The monitoring script reads from the same DynamoDB table that the migration workers write to. Workers update progress:

- **On space start:** Create initial progress record
- **Every 1000 uploads:** Update `completedUploads` and `lastProcessedUpload`
- **On space completion:** Mark status as `completed`
- **On space failure:** Mark status as `failed` with error message

This provides real-time visibility into the migration progress without impacting worker performance.

## Troubleshooting

### No data showing

- Verify the `migration-progress` table exists in DynamoDB
- Check AWS credentials are configured correctly
- Ensure the migration workers have started writing progress

### Stuck migrations

If you see stuck migrations (>1 hour with no updates):

1. Check if the EC2 instance is still running
2. Check instance logs for errors
3. Consider restarting the stuck worker
4. The migration is designed to be idempotent - restarting will resume from the last checkpoint

### Failed migrations

For failed migrations:

1. Review the error message
2. Check if it's a transient error (network timeout) or permanent (invalid data)
3. For transient errors, restart the worker - it will retry
4. For permanent errors, investigate the specific space/customer data

## Performance Considerations

- **Scan operations** (overall stats, failed, stuck) scan the entire table - may be slow with millions of records
- **Query operations** (customer, space) use the primary key - fast and efficient
- **Watch mode** runs queries every 30 seconds - use sparingly to avoid DynamoDB throttling
- Consider using the **instance** view for focused monitoring during active migration
