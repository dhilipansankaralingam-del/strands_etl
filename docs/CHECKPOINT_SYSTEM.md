# Checkpoint System for Hourly ETL Jobs

## Overview

The checkpoint system enables **graceful job restarts** within the same hour. When a job fails midway through processing multiple tables, you can restart it without re-processing tables that already completed successfully.

## Problem It Solves

### Without Checkpoints ❌

```
9:00 AM - Job starts with 3 tables
  ✅ Table 1 (mrd_mbr_prd_dtl__ct) - Completed
  ✅ Table 2 (mrd_mbr_info__ct) - Completed
  ❌ Table 3 (mrd_mbr_contact__ct) - FAILED

9:15 AM - Restart job
  🔄 Table 1 - Re-processes (wastes time, duplicate data risk)
  🔄 Table 2 - Re-processes (wastes time, duplicate data risk)
  ❌ Table 3 - Still fails

Result: Tables 1 & 2 processed twice in same hour!
```

### With Checkpoints ✅

```
9:00 AM - Job starts with 3 tables
  ✅ Table 1 (mrd_mbr_prd_dtl__ct) - Completed → Checkpoint created
  ✅ Table 2 (mrd_mbr_info__ct) - Completed → Checkpoint created
  ❌ Table 3 (mrd_mbr_contact__ct) - FAILED → No checkpoint

9:15 AM - Restart job
  ⏭️  Table 1 - SKIPPED (checkpoint found for 9 AM hour)
  ⏭️  Table 2 - SKIPPED (checkpoint found for 9 AM hour)
  🔄 Table 3 - Re-runs (no checkpoint, will retry)

Result: Only failed table is re-processed!
```

---

## How It Works

### 1. Run Hour Identifier

Each job execution is assigned a "run hour" identifier:

```
Format: YYYY-MM-DD-HH

Examples:
  2026-02-10-09  (9 AM hour)
  2026-02-10-10  (10 AM hour)
  2026-02-10-21  (9 PM hour)
```

Jobs with the same run hour are grouped together for checkpoint purposes.

### 2. Checkpoint Creation

When a table completes successfully:

```python
# Checkpoint location
s3://bucket/checkpoints/{run_hour}/{table_name}.SUCCESS

# Example
s3://staging/checkpoints/2026-02-10-09/mrd_mbr_prd_dtl__ct.SUCCESS
s3://staging/checkpoints/2026-02-10-09/mrd_mbr_info__ct.SUCCESS
```

**Checkpoint file contains**:
- Table name
- Run hour
- Completion timestamp
- Job run ID
- Files processed count
- Rows loaded count
- Status

### 3. Checkpoint Check

Before processing each table:

```
1. Get current run hour (e.g., 2026-02-10-09)
2. Check S3: checkpoints/2026-02-10-09/{table_name}.SUCCESS
3. If exists → Skip job (already completed this hour)
4. If not exists → Process job normally
```

### 4. Automatic Cleanup

Old checkpoints are automatically deleted after 48 hours to prevent S3 clutter.

---

## Configuration

### Enable/Disable Checkpoints

Checkpoints are **always enabled**. They're lightweight and essential for production reliability.

### Continue on Error (Optional)

Control whether job stops on first failure or continues to process remaining tables:

```json
{
  "global_settings": {
    "continue_on_error": false,  // Default: stop on first failure
    "cleanup_old_checkpoints": true  // Default: cleanup after 48 hours
  }
}
```

**Options**:

| Setting | Behavior | Use Case |
|---------|----------|----------|
| `continue_on_error: false` | Stop on first failure | **Recommended** - Fast failure detection |
| `continue_on_error: true` | Process all tables, fail at end | Special cases where you want full run stats |

---

## Execution Examples

### Example 1: Normal Success (All Tables Complete)

```bash
10:00 AM - Job starts (run_hour: 2026-02-10-10)

[CHECKPOINT CHECK] mrd_mbr_prd_dtl__ct
  ✓ No checkpoint found - proceeding with job execution

[STEP 1-13] Processing...
  ✓ 1,234 rows loaded successfully

[STEP 14] Creating checkpoint...
  ✓ Created checkpoint: s3://staging/checkpoints/2026-02-10-10/mrd_mbr_prd_dtl__ct.SUCCESS

[CHECKPOINT CHECK] mrd_mbr_info__ct
  ✓ No checkpoint found - proceeding with job execution

[STEP 1-13] Processing...
  ✓ 5,678 rows loaded successfully

[STEP 14] Creating checkpoint...
  ✓ Created checkpoint: s3://staging/checkpoints/2026-02-10-10/mrd_mbr_info__ct.SUCCESS

JOB EXECUTION SUMMARY
=====================
Run Hour: 2026-02-10-10
Total Jobs: 2
Jobs Skipped: 0
Jobs Processed: 2
Jobs Failed: 0

✅ ALL JOBS COMPLETED SUCCESSFULLY
```

### Example 2: Partial Failure (Second Table Fails)

```bash
10:00 AM - Job starts (run_hour: 2026-02-10-10)

[CHECKPOINT CHECK] mrd_mbr_prd_dtl__ct
  ✓ No checkpoint found - proceeding with job execution

[STEP 1-14] Processing...
  ✓ Checkpoint created

[CHECKPOINT CHECK] mrd_mbr_info__ct
  ✓ No checkpoint found - proceeding with job execution

[STEP 1-7] Processing...
  ❌ Validation failed: 15 invalid records found

❌ JOB 2/2 FAILED: Membership Info Staging Load
Error: Validation failures detected

❌ Stopping execution (continue_on_error=false)
   On restart, completed jobs will be skipped via checkpoints

💡 RESTART INSTRUCTIONS:
   When you restart this job, it will automatically skip jobs
   that completed successfully this hour (2026-02-10-10)
   Only failed jobs will re-run.
```

### Example 3: Restart After Failure (Skips Completed Jobs)

```bash
10:15 AM - Job restarts (same hour: 2026-02-10-10)

[CHECKPOINT CHECK] mrd_mbr_prd_dtl__ct
  ✓ Checkpoint found: s3://staging/checkpoints/2026-02-10-10/mrd_mbr_prd_dtl__ct.SUCCESS
  Job 'mrd_mbr_prd_dtl__ct' already completed successfully for hour 2026-02-10-10

⏭️  SKIPPING JOB: Already completed successfully this hour
   To re-run, delete: s3://staging/checkpoints/2026-02-10-10/mrd_mbr_prd_dtl__ct.SUCCESS

[CHECKPOINT CHECK] mrd_mbr_info__ct
  ✓ No checkpoint found - proceeding with job execution

[STEP 1-14] Processing...
  ✓ Issue fixed, validation passed
  ✓ Checkpoint created

JOB EXECUTION SUMMARY
=====================
Run Hour: 2026-02-10-10
Total Jobs: 2
Jobs Skipped: 1
Jobs Processed: 1
Jobs Failed: 0

💡 TIP: 1 job(s) were skipped because they already completed this hour
    To re-run skipped jobs, delete checkpoints: s3://staging/checkpoints/2026-02-10-10/

✅ ALL JOBS COMPLETED SUCCESSFULLY
```

### Example 4: New Hour (All Jobs Run Again)

```bash
11:00 AM - Job starts (NEW hour: 2026-02-10-11)

[CHECKPOINT CHECK] mrd_mbr_prd_dtl__ct
  ✓ No checkpoint found - proceeding with job execution
  (Looking for: checkpoints/2026-02-10-11/...)

[Processing both jobs normally - new hour means fresh start]

JOB EXECUTION SUMMARY
=====================
Run Hour: 2026-02-10-11  ← New hour!
Total Jobs: 2
Jobs Skipped: 0
Jobs Processed: 2
Jobs Failed: 0
```

---

## S3 Structure

```
s3://staging/
├── checkpoints/
│   ├── 2026-02-10-08/           # 8 AM hour
│   │   ├── mrd_mbr_prd_dtl__ct.SUCCESS
│   │   └── mrd_mbr_info__ct.SUCCESS
│   │
│   ├── 2026-02-10-09/           # 9 AM hour
│   │   ├── mrd_mbr_prd_dtl__ct.SUCCESS
│   │   └── mrd_mbr_info__ct.SUCCESS
│   │
│   └── 2026-02-10-10/           # 10 AM hour (partial)
│       └── mrd_mbr_prd_dtl__ct.SUCCESS  (mrd_mbr_info__ct missing - failed)
│
├── audit_table/                 # Audit records
├── landing/                     # Source data
└── archive/                     # Processed data
```

---

## Checkpoint File Content

**File**: `s3://staging/checkpoints/2026-02-10-10/mrd_mbr_prd_dtl__ct.SUCCESS`

```
JOB CHECKPOINT
==============
Table: mrd_mbr_prd_dtl__ct
Run Hour: 2026-02-10-10
Completed: 2026-02-10T10:15:23.456789
Job Run ID: jr_1234567890abcdef
Files Processed: 5
Rows Loaded: 12345
Status: SUCCESS

This checkpoint indicates the job completed successfully.
If you restart the job within the same hour, this job will be skipped.
Checkpoints are automatically cleaned up after 48 hours.
```

---

## Use Cases

### Use Case 1: Network Timeout

**Scenario**: Job times out after processing 2 of 5 tables

**Solution**:
1. First 2 tables have checkpoints
2. Restart job with higher timeout
3. Skips first 2 tables automatically
4. Processes remaining 3 tables

### Use Case 2: Validation Failure

**Scenario**: Table 3 has bad data, causes validation failure

**Solution**:
1. Tables 1 & 2 have checkpoints
2. Fix data at source
3. Restart job
4. Skips tables 1 & 2
5. Re-processes table 3 with clean data

### Use Case 3: Glue Worker Crash

**Scenario**: Glue worker crashes mid-execution

**Solution**:
1. Completed tables have checkpoints
2. AWS auto-retries job
3. Skips completed tables
4. Continues from crash point

### Use Case 4: Manual Abort

**Scenario**: Manually stop job to apply hot fix

**Solution**:
1. Stop job after 3 of 7 tables
2. Apply code fix
3. Restart job
4. Skips 3 completed tables
5. Processes remaining 4 with fix

---

## Advanced Operations

### Force Re-run All Jobs (Same Hour)

If you need to re-process everything despite checkpoints:

```bash
# Delete all checkpoints for current hour
aws s3 rm s3://staging/checkpoints/2026-02-10-10/ --recursive

# Or delete specific table checkpoint
aws s3 rm s3://staging/checkpoints/2026-02-10-10/mrd_mbr_prd_dtl__ct.SUCCESS
```

### Check Checkpoint Status

```bash
# List all checkpoints for current hour
aws s3 ls s3://staging/checkpoints/2026-02-10-10/

# Output:
# 2026-02-10 10:15:23    234 mrd_mbr_prd_dtl__ct.SUCCESS
# 2026-02-10 10:18:45    241 mrd_mbr_info__ct.SUCCESS

# View checkpoint details
aws s3 cp s3://staging/checkpoints/2026-02-10-10/mrd_mbr_prd_dtl__ct.SUCCESS - | cat
```

### Query Checkpoints

```sql
-- Using Athena (if you create a table over checkpoints)
SELECT
    SUBSTRING(key, 13, 13) as run_hour,
    SUBSTRING(key, POSITION('/' IN key, 25) + 1,
              POSITION('.SUCCESS' IN key) - POSITION('/' IN key, 25) - 1) as table_name,
    size,
    last_modified
FROM s3_checkpoints
WHERE last_modified >= CURRENT_DATE - INTERVAL '2' DAY
ORDER BY run_hour DESC, table_name;
```

### Manual Cleanup

```bash
# Delete checkpoints older than 48 hours
CUTOFF_DATE=$(date -d '48 hours ago' +%Y-%m-%d-%H)

aws s3 ls s3://staging/checkpoints/ | \
  awk -v cutoff="$CUTOFF_DATE" '$4 < cutoff {print $4}' | \
  xargs -I {} aws s3 rm s3://staging/checkpoints/{}/ --recursive
```

---

## Monitoring & Alerting

### CloudWatch Metrics

Track checkpoint stats:

```python
import boto3
cloudwatch = boto3.client('cloudwatch')

# Publish checkpoint metrics
cloudwatch.put_metric_data(
    Namespace='ETL/Checkpoints',
    MetricData=[
        {
            'MetricName': 'JobsSkipped',
            'Value': jobs_skipped,
            'Unit': 'Count',
            'Timestamp': datetime.now()
        },
        {
            'MetricName': 'JobsProcessed',
            'Value': jobs_processed,
            'Unit': 'Count'
        },
        {
            'MetricName': 'JobsFailed',
            'Value': jobs_failed,
            'Unit': 'Count'
        }
    ]
)
```

### CloudWatch Alarms

Alert on repeated failures:

```bash
# Alert if same job fails 3 times in same hour (checkpoint never created)
aws cloudwatch put-metric-alarm \
  --alarm-name etl-repeated-failure \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 3 \
  --metric-name JobsFailed \
  --namespace ETL/Checkpoints \
  --period 3600 \
  --statistic Sum \
  --threshold 0 \
  --alarm-actions arn:aws:sns:us-east-1:123456789012:etl-alerts
```

### Query Checkpoint History

```bash
# Count checkpoints per hour for last 24 hours
aws s3 ls s3://staging/checkpoints/ --recursive | \
  awk '{print $4}' | \
  cut -d'/' -f2 | \
  sort | uniq -c | \
  tail -24
```

---

## Troubleshooting

### Issue 1: Job Keeps Skipping All Tables

**Symptoms**:
```
Jobs Skipped: 2
Jobs Processed: 0
```

**Cause**: Checkpoints exist from previous successful run

**Solution**:
```bash
# Check if you're still in same hour
date +%Y-%m-%d-%H

# If new hour started but checkpoints exist, this is expected
# They're from previous hour and will be cleaned up automatically

# To force re-run now:
aws s3 rm s3://staging/checkpoints/2026-02-10-10/ --recursive
```

### Issue 2: Checkpoint Not Created After Success

**Symptoms**: Job completes but checkpoint file missing

**Cause**: S3 write permissions issue

**Solution**:
```bash
# Check IAM role has PutObject permission
aws s3api put-object \
  --bucket staging \
  --key checkpoints/test.txt \
  --body /dev/null

# Add to IAM role if missing:
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
  "Resource": "arn:aws:s3:::staging/checkpoints/*"
}
```

### Issue 3: Old Checkpoints Not Cleaned Up

**Symptoms**: Hundreds of checkpoint folders in S3

**Cause**: `cleanup_old_checkpoints` disabled or job not running to completion

**Solution**:
```json
{
  "global_settings": {
    "cleanup_old_checkpoints": true  // Ensure this is true
  }
}
```

Or manual cleanup:
```bash
# Delete all checkpoints older than 2 days
find /tmp -type f -name "*.txt" -mtime +2 -delete

# For S3:
python cleanup_old_checkpoints.py --bucket staging --days 2
```

### Issue 4: Checkpoint Exists But Job Still Runs

**Symptoms**: Checkpoint file exists but job doesn't skip

**Cause**: Run hour mismatch (job running in different hour)

**Debug**:
```bash
# Check current run hour
date +%Y-%m-%d-%H

# Check checkpoint hour
aws s3 ls s3://staging/checkpoints/ | tail -5

# They should match for skip to work
# If different hours, this is correct behavior
```

---

## Best Practices

### 1. ✅ Always Use Checkpoints in Production

Checkpoints are lightweight (few KB) and provide huge value for job reliability.

### 2. ✅ Monitor Skip Rates

Track `jobs_skipped / jobs_total`:
- **0%** = Normal fresh run or all jobs restarted successfully
- **50%** = Half completed before failure (expected on restart)
- **100%** = All jobs already done (possible unnecessary restart)

### 3. ✅ Set Appropriate Retry Strategy

```json
{
  "MaxRetries": 1,  // Let Glue retry once automatically
  "Timeout": 120     // 2 hours for complete run
}
```

Checkpoints ensure retries are efficient (skip successful jobs).

### 4. ✅ Use continue_on_error for Critical Jobs

```json
{
  "continue_on_error": true  // For jobs that MUST process all tables
}
```

Example: End-of-day reports that need full data even if some validations fail.

### 5. ✅ Clean Up Checkpoints Regularly

Default 48-hour cleanup is usually sufficient. For more aggressive cleanup:

```json
{
  "cleanup_old_checkpoints": true,
  "checkpoint_retention_hours": 24  // Keep only 24 hours
}
```

### 6. ❌ Don't Delete Checkpoints During Active Hour

```bash
# BAD: Deleting checkpoints while job might be running
aws s3 rm s3://staging/checkpoints/$(date +%Y-%m-%d-%H)/ --recursive

# GOOD: Only delete old checkpoints
aws s3 rm s3://staging/checkpoints/2026-02-10-08/ --recursive
```

### 7. ✅ Document Recovery Procedures

Create runbook for your team:
```markdown
## Recovery from ETL Failure

1. Check which tables completed:
   aws s3 ls s3://staging/checkpoints/$(date +%Y-%m-%d-%H)/

2. Fix root cause (data quality, permissions, etc.)

3. Restart Glue job:
   aws glue start-job-run --job-name membership-etl-job

4. Verify only failed tables re-run (check logs for "SKIPPING JOB")

5. Confirm all tables now have checkpoints
```

---

## Performance Impact

### Storage

- **Per checkpoint**: ~250 bytes
- **Per hour (10 tables)**: ~2.5 KB
- **Per day (10 tables × 24 hours)**: ~60 KB
- **Per month (30 days, auto-cleanup)**: ~100 KB
- **Cost**: < $0.01/month

### Execution Time

- **Checkpoint check**: ~50ms per table (S3 HEAD request)
- **Checkpoint creation**: ~100ms per table (S3 PUT request)
- **Total overhead**: ~150ms per table
- **For 10 tables**: ~1.5 seconds total (negligible)

### Benefits

- **Avoid duplicate processing**: Save 5-30 minutes per restart
- **Reduce load on source systems**: No redundant file reads
- **Faster recovery**: Only process failed tables
- **Lower costs**: Less Glue DPU-hours consumed

**ROI**: Checkpoints pay for themselves after first restart!

---

## Summary

✅ **Automatic Checkpoint Creation** - After each successful table
✅ **Intelligent Skip Logic** - Restart only processes failed tables
✅ **Hour-Based Grouping** - Fresh start each hour
✅ **Auto Cleanup** - Old checkpoints deleted after 48 hours
✅ **Zero Configuration** - Works out of the box
✅ **Negligible Overhead** - < 200ms per table
✅ **High Reliability** - Production-proven pattern

The checkpoint system makes your hourly ETL jobs **resilient, efficient, and production-ready**!
