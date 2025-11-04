# Setup Distribution Guide

Distribute customers across multiple EC2 instances for parallel migration processing.

## Quick Start

```bash
# Analyze customer distribution (no files created)
node src/setup-distribution.js --analyze

# Generate distribution for 5 instances
node src/setup-distribution.js --instances 5
```

## What It Does

The setup script:
1. Scans the Consumer table to find all customers and their spaces
2. Counts uploads for each customer
3. Distributes customers across instances using load balancing
4. Generates JSON files with customer assignments per instance

## Output Files

Creates one file per instance in `migration-state/`:
- `instance-1-customers.json`
- `instance-2-customers.json`
- `instance-3-customers.json`
- ... (one per instance)

Each file contains:
- Instance ID
- List of customer DIDs assigned to this instance
- Estimated uploads and spaces
- Creation timestamp

## Command Options

```bash
node src/setup-distribution.js [options]

Options:
  --analyze                    Analyze distribution without generating files
  --instances <N>              Number of EC2 instances
  --parallel-segments <N>      Parallel scan segments (1-10, default: 4)
  --workers-per-instance <N>   Workers per instance for estimates (default: 10)
  --min-uploads <N>            Minimum uploads to include customer (default: 0)
```

## Examples

### Analyze Before Generating

```bash
# See customer distribution without creating files
node src/setup-distribution.js --analyze
```

### Generate Distribution

```bash
# Create distribution for 5 instances
node src/setup-distribution.js --instances 5

# Use more parallel segments for faster scanning
node src/setup-distribution.js --instances 5 --parallel-segments 8

# Adjust worker count for time estimates
node src/setup-distribution.js --instances 5 --workers-per-instance 15
```

### Filter Customers

```bash
# Only include customers with 100+ uploads
node src/setup-distribution.js --instances 5 --min-uploads 100
```

## Understanding the Output

```
Customer Distribution Analysis
======================================================================
Total customers: 125,432
Total uploads: 37,028,823
Total spaces: 256,789
Average uploads/customer: 295

Instance Distribution
======================================================================
Instance 1:
  Customers: 25,086
  Uploads: 7,405,764 (20.0%)
  Spaces: 51,357
  Estimated time (10 workers): 15.3 days

Instance 2:
  Customers: 25,087
  Uploads: 7,405,765 (20.0%)
  Spaces: 51,358
  Estimated time (10 workers): 15.3 days

...

Load Balance:
  Min uploads/instance: 7,405,764
  Max uploads/instance: 7,405,765
  Variance: 0.0%
```

## Load Balancing

The script uses greedy load balancing:
- Customers sorted by upload count (descending)
- Each customer assigned to instance with least load
- Results in balanced distribution across instances

## Performance

**Scanning Speed:**
- Uses parallel DynamoDB scans (4 segments by default)
- Processes ~10,000 consumer records/second
- Full scan typically completes in 2-5 minutes

**Upload Counting:**
- Processes 20 customers in parallel
- Uses connection pooling for efficiency
- Includes retry logic for timeouts
- Saves checkpoints for resume capability

## Resume Capability

If the script is interrupted during upload counting:
- Progress is saved to `migration-state/counting-checkpoint.json`
- Re-run the same command to resume from checkpoint
- Checkpoint is automatically deleted on completion

## Deploying to EC2 Instances

After generating distribution files, you need to distribute them to each EC2 instance:

### 1. Copy Distribution Files

Copy the generated `instance-N-customers.json` file to each corresponding EC2 instance:

```bash
# Copy to Instance 1
scp migration-state/instance-1-customers.json ec2-instance-1:/path/to/legacy-spaces-migration/migration-state/

# ... etc
```

### 2. Verify Files

On each EC2 instance, verify the file exists:

```bash
ls -lh migration-state/instance-*.json
```

The migration script will read from this file to determine which customers to process.

## Next Steps

After deploying distribution files:
1. Proceed to [Migration Guide](MIGRATION.md)
2. Start migration on each instance: `node src/migrate-instance.js --instance N`
