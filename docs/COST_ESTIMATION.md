# Cost Estimation Guide

Analyze the workload before starting the migration to estimate costs, time, and storage requirements.

## Quick Start

```bash
# Quick analysis (2000 sample uploads out of 37M total)
node src/estimate-costs.js --sample 2000 --total 37028823

# Detailed analysis (larger sample out of 37M total)
node src/estimate-costs.js --sample 10000 --total 37028823

# Analyze specific space
node src/estimate-costs.js --sample 1000 --space did:key:z6Mk...
```

## What It Does

The cost estimation script:
1. Samples a subset of uploads from the database
2. Counts shards and calculates index generation costs
3. Extrapolates to the total upload count
4. Estimates time and storage requirements

## Output

The script provides:
- **Total uploads and shards** to process
- **Estimated index generation costs** (API calls, compute time)
- **Estimated time to complete** (based on throughput)
- **Storage requirements** (CAR files, indices)

## Command Options

```bash
node src/estimate-costs.js [options]

Options:
  --sample <N>      Number of uploads to sample (default: 1000)
  --total <N>       Total uploads in database for extrapolation
  --space <DID>     Filter to specific space DID (optional)
  --dry-run         Only count shards without calling index worker
```

## Examples

### Basic Estimation

```bash
# Sample 2000 uploads from 37M total
node src/estimate-costs.js --sample 2000 --total 37028823
```

### Space-Specific Estimation

```bash
# Estimate costs for a specific space
node src/estimate-costs.js \
  --sample 1000 \
  --space did:key:z6MkiExample... \
  --total 50000
```

### Dry Run (No API Calls)

```bash
# Count shards without calling the index worker
node src/estimate-costs.js --sample 5000 --total 37028823 --dry-run
```

## Understanding the Output

```
Cost Estimation Results
======================================================================

Sample Analysis:
  Uploads sampled: 2,000
  Shards found: 3,456
  Avg shards/upload: 1.73

Extrapolated Totals:
  Total uploads: 37,028,823
  Estimated shards: 64,059,863
  
Index Generation:
  API calls: 64,059,863
  Estimated cost: $3,202.99
  Estimated time: 15.2 days (5 instances, 10 workers each)
  
Storage:
  CAR files: 1.2 TB
  Indices: 450 GB
```

## Tips

- **Sample size**: Use at least 1000-2000 uploads for accurate estimates
- **Dry run**: Use `--dry-run` for quick shard counts without API costs
- **Space filtering**: Test specific spaces before full migration
- **Multiple runs**: Run multiple samples to verify consistency

## Next Steps

After estimating costs:
1. Review the output and confirm budget/timeline
2. Proceed to [Setup Distribution](SETUP_DISTRIBUTION.md)
3. Or adjust your migration plan based on estimates
