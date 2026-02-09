# Enhanced Config-Driven Glue Job - Complete Guide

## Overview

This guide explains the enhanced Glue ETL job that includes:

✅ **Schema auto-detection** from target table
✅ **Config-driven validations** with negative checks
✅ **Age-based folder processing** (> 59 minutes)
✅ **File count validation** against loaded counts
✅ **Comprehensive auditing** with detailed logs
✅ **FAIL file checkpoint** system
✅ **Archive only on success** - atomic operations
✅ **Abort on no files** - fail fast

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Configuration Structure](#configuration-structure)
3. [Validation Queries](#validation-queries)
4. [FAIL File System](#fail-file-system)
5. [Folder Age Logic](#folder-age-logic)
6. [Audit Records](#audit-records)
7. [Setup Instructions](#setup-instructions)
8. [Running the Job](#running-the-job)
9. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
10. [Examples](#examples)

---

## How It Works

### Execution Flow

```
1. Check FAIL File
   └─ If exists → ABORT (previous failure not resolved)
   └─ If not exists → Continue

2. Find Eligible Folders
   └─ Scan landing location
   └─ Filter folders > 59 minutes old
   └─ If no folders → CREATE FAIL file + ABORT

3. Collect CSV Files
   └─ Get all .csv files from eligible folders
   └─ If no files → CREATE FAIL file + ABORT

4. Count Source Rows
   └─ Count rows in all CSV files (exclude headers)
   └─ Store count for validation

5. Get Table Schema
   └─ Query target table for schema
   └─ Extract business columns (exclude audit columns)
   └─ Use schema to read CSV files

6. Load CSV Files
   └─ Read files with derived schema
   └─ Add audit columns (timestamps, flags, etc.)
   └─ Create temp view

7. Validate Row Counts
   └─ Compare file rows vs loaded rows
   └─ If mismatch → CREATE FAIL file + ABORT

8. Run Validation Queries
   └─ Execute all negative validation queries
   └─ If any query returns count > max_allowed → CREATE FAIL file + ABORT

9. Insert into Target
   └─ Load data into target table
   └─ Commit transaction

10. Write Audit Record
    └─ Log: folders, files, counts, validations, timestamp
    └─ Store in s3://bucket/audit/table_name/

11. Archive Files
    └─ Move processed folders to archive
    └─ Maintain folder structure
    └─ Delete from landing location

12. Delete FAIL File
    └─ Clean up any old FAIL files
    └─ Job completes successfully
```

---

## Configuration Structure

### Example Config (`glue_job_config.json`)

```json
{
  "jobs": [
    {
      "job_name": "Customer Staging Load",
      "s3_bucket": "your-bucket-name",
      "landing_loc": "landing/customers/",
      "file_pattern_name": "*.csv",
      "database_name": "etl_demo_db",
      "landing_table": "staging_customers",
      "archive_s3_path": "archive/customers/",
      "temp_s3_path": "temp/customers/",

      "validation_queries": [
        {
          "name": "Check for NULL customer IDs",
          "query": "SELECT COUNT(*) FROM staging_temp WHERE customer_id IS NULL",
          "max_allowed": 0,
          "severity": "critical",
          "description": "Customer ID is a required field"
        }
      ]
    }
  ]
}
```

### Config Fields Explained

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `job_name` | string | Yes | Descriptive name for the job |
| `s3_bucket` | string | Yes | S3 bucket name |
| `landing_loc` | string | Yes | Prefix for landing location (e.g., `landing/customers/`) |
| `file_pattern_name` | string | Yes | File pattern (e.g., `*.csv`) |
| `database_name` | string | Yes | Glue database name |
| `landing_table` | string | Yes | Target table name |
| `archive_s3_path` | string | Yes | Archive location prefix |
| `temp_s3_path` | string | No | Temporary location (future use) |
| `validation_queries` | array | Yes | List of validation queries |

---

## Validation Queries

### Structure

```json
{
  "name": "Validation Name",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE ...",
  "max_allowed": 0,
  "severity": "critical|high|medium|low",
  "description": "What this validation checks"
}
```

### Rules

1. **Query must return a single count value**
   - ✅ Good: `SELECT COUNT(*) FROM staging_temp WHERE ...`
   - ❌ Bad: `SELECT customer_id, COUNT(*) FROM staging_temp GROUP BY ...`

2. **Use `staging_temp` as table reference**
   - The loaded DataFrame is registered as `staging_temp`
   - Don't use the actual table name in queries

3. **Negative validations** - Check for bad data
   - Count should be **0** for success
   - Any count > 0 means failure

4. **max_allowed** defines the threshold
   - `"max_allowed": 0` - No bad records allowed
   - `"max_allowed": 10` - Up to 10 bad records acceptable

### Example Validation Queries

#### 1. Check for NULL Values

```json
{
  "name": "Check for NULL customer IDs",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE customer_id IS NULL",
  "max_allowed": 0,
  "severity": "critical"
}
```

#### 2. Check for Invalid Formats

```json
{
  "name": "Check for invalid email formats",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
  "max_allowed": 0,
  "severity": "high"
}
```

#### 3. Check for Duplicates

```json
{
  "name": "Check for duplicate customer IDs",
  "query": "SELECT COUNT(*) FROM (SELECT customer_id, COUNT(*) as cnt FROM staging_temp GROUP BY customer_id HAVING cnt > 1)",
  "max_allowed": 0,
  "severity": "critical"
}
```

#### 4. Check for Data Ranges

```json
{
  "name": "Check for invalid amounts",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE amount <= 0 OR amount > 1000000",
  "max_allowed": 0,
  "severity": "high"
}
```

#### 5. Check for Future Dates

```json
{
  "name": "Check for future dates",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE transaction_date > CURRENT_DATE()",
  "max_allowed": 0,
  "severity": "high"
}
```

#### 6. Check Referential Integrity

```json
{
  "name": "Check for orphan customers",
  "query": "SELECT COUNT(*) FROM staging_temp st LEFT JOIN glue_catalog.{database}.dim_customers c ON st.customer_id = c.customer_id WHERE c.customer_id IS NULL",
  "max_allowed": 0,
  "severity": "critical"
}
```

**Note:** Use `{database}` placeholder for database name - it will be replaced at runtime.

#### 7. Check Data Consistency

```json
{
  "name": "Check amount/price consistency",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE ABS(amount - (quantity * unit_price)) > 0.01",
  "max_allowed": 0,
  "severity": "high"
}
```

---

## FAIL File System

### Purpose

The FAIL file system prevents cascading failures:
- If a job fails, it creates a `.FAIL` file
- Subsequent runs check for this file at the start
- If found, job aborts immediately
- Manual intervention required to investigate and fix

### Location

```
s3://your-bucket/status/{table_name}.FAIL
```

Example:
```
s3://etl-demo-bucket/status/staging_customers.FAIL
```

### FAIL File Content

```
FAILURE DETAILS
===============
Table: staging_customers
Timestamp: 2026-01-27T14:32:15
Job Name: customer-staging-load
Job Run ID: jr_abc123

Failure Reason:
Validation queries failed:

  - Check for NULL customer IDs: Count 5 > 0
  - Check for invalid email formats: Count 12 > 0

Action Required:
1. Investigate the failure reason above
2. Fix the data quality issues
3. Delete this FAIL file: s3://bucket/status/staging_customers.FAIL
4. Re-run the job
```

### How to Recover

1. **Check CloudWatch Logs** for detailed error
2. **Investigate the data** - why did validation fail?
3. **Fix the source data** or **fix the validation query**
4. **Delete the FAIL file:**
   ```bash
   aws s3 rm s3://your-bucket/status/staging_customers.FAIL
   ```
5. **Re-run the job**

---

## Folder Age Logic

### Why 59 Minutes?

Jobs run every hour. We only process folders that are:
- **Complete** - All files have been uploaded
- **Stable** - No more files being added
- **Aged** - Older than 59 minutes

### How It Works

```
Current Time: 14:32
Threshold: 59 minutes

Folders:
├── 20260127_1300/  (Last modified: 13:30) → Age: 62 min → ✅ PROCESS
├── 20260127_1400/  (Last modified: 14:15) → Age: 17 min → ❌ SKIP (too recent)
└── 20260127_1200/  (Last modified: 12:45) → Age: 107 min → ✅ PROCESS
```

### Folder Structure Expected

```
s3://bucket/landing/customers/
├── 20260127_1200/
│   ├── file1.csv
│   ├── file2.csv
│   └── file3.csv
├── 20260127_1300/
│   ├── file1.csv
│   └── file2.csv
└── 20260127_1400/  (too recent - skipped)
    └── file1.csv
```

### What If No Eligible Folders?

```
❌ Error: No folders older than 59 minutes found
→ Creates FAIL file
→ Job aborts
```

---

## Audit Records

### Location

```
s3://bucket/audit/{table_name}/audit_{timestamp}.txt
```

Example:
```
s3://etl-demo-bucket/audit/staging_customers/audit_20260127_143215.txt
```

### Audit Record Format

```
AUDIT RECORD
============
Staging Table Name: etl_demo_db.staging_customers
Job Name: customer-staging-load
Job Run ID: jr_abc123

Subfolders Considered for Load:
  - landing/customers/20260127_1200/
  - landing/customers/20260127_1300/

Number of Files Used: 6
File Row Count: 15,234
Loaded Row Count: 15,234
Count Match: PASS

Validation Checks: PASS
All 5 validations passed

Load Timestamp: 2026-01-27 14:32:15
Duration: 127.45 seconds

Status: SUCCESS
```

### Audit Fields

| Field | Description |
|-------|-------------|
| Staging Table Name | Full table name (database.table) |
| Subfolders Considered | List of folders processed |
| Number of Files Used | Total CSV files loaded |
| File Row Count | Rows counted in source CSV files |
| Loaded Row Count | Rows loaded into DataFrame |
| Count Match | PASS if counts match, FAIL otherwise |
| Validation Checks | PASS/FAIL status |
| Load Timestamp | When the load completed |
| Duration | How long the job took |
| Status | SUCCESS or FAIL |

---

## Setup Instructions

### 1. Create Staging Tables

Create the target staging tables in Glue:

```sql
CREATE TABLE IF NOT EXISTS etl_demo_db.staging_customers (
  customer_id STRING,
  customer_name STRING,
  email STRING,
  phone STRING,
  segment_id STRING,
  signup_date STRING,

  -- Audit columns (will be added by script)
  hourly_processed_ind STRING,
  daily_processed_ind STRING,
  insert_timestamp STRING,
  inserted_by STRING,
  updated_timestamp STRING,
  updated_by STRING,
  load_timestamp STRING
)
USING iceberg
LOCATION 's3://your-bucket/warehouse/staging_customers'
```

### 2. Upload Config to S3

```bash
# Upload config file
aws s3 cp config/glue_job_config.json s3://your-bucket/config/glue_job_config.json
```

### 3. Upload Script to S3

```bash
# Upload Glue script
aws s3 cp pyscript/enhanced_config_driven_glue_job.py \
  s3://your-bucket/scripts/enhanced_config_driven_glue_job.py
```

### 4. Create Glue Job

```bash
# Create Glue job
aws glue create-job \
  --name enhanced-config-driven-etl \
  --role ETLGlueServiceRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-bucket/scripts/enhanced_config_driven_glue_job.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--json_file_name": "config/glue_job_config.json",
    "--bucket_name": "your-bucket"
  }' \
  --glue-version "3.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --timeout 180
```

### 5. Create S3 Folder Structure

```bash
# Create necessary folders
aws s3api put-object --bucket your-bucket --key landing/customers/
aws s3api put-object --bucket your-bucket --key archive/customers/
aws s3api put-object --bucket your-bucket --key status/
aws s3api put-object --bucket your-bucket --key audit/staging_customers/
```

---

## Running the Job

### Manual Run via AWS Console

1. Go to **AWS Glue** → **Jobs**
2. Select `enhanced-config-driven-etl`
3. Click **Run job**
4. Monitor in **Run details**

### Manual Run via AWS CLI

```bash
aws glue start-job-run \
  --job-name enhanced-config-driven-etl \
  --arguments '{
    "--json_file_name": "config/glue_job_config.json",
    "--bucket_name": "your-bucket"
  }'
```

### Scheduled Run

Create a trigger to run hourly:

```bash
aws glue create-trigger \
  --name hourly-staging-load \
  --type SCHEDULED \
  --schedule "cron(0 * * * ? *)" \
  --actions '[
    {
      "JobName": "enhanced-config-driven-etl"
    }
  ]' \
  --start-on-creation
```

---

## Monitoring & Troubleshooting

### Check Job Status

```bash
# Get recent job runs
aws glue get-job-runs \
  --job-name enhanced-config-driven-etl \
  --max-results 5

# Get specific run
aws glue get-job-run \
  --job-name enhanced-config-driven-etl \
  --run-id jr_abc123
```

### Check CloudWatch Logs

1. Go to **CloudWatch** → **Log groups**
2. Find `/aws-glue/jobs/output` or `/aws-glue/jobs/error`
3. Search for your job run ID

### Check for FAIL Files

```bash
# List all FAIL files
aws s3 ls s3://your-bucket/status/ | grep FAIL

# Read FAIL file content
aws s3 cp s3://your-bucket/status/staging_customers.FAIL -
```

### Check Audit Records

```bash
# List audit records
aws s3 ls s3://your-bucket/audit/staging_customers/

# Read latest audit record
aws s3 cp s3://your-bucket/audit/staging_customers/audit_20260127_143215.txt -
```

### Common Issues

#### Issue 1: No Folders Eligible

**Error:** `No folders older than 59 minutes found`

**Cause:** All folders are too recent (< 59 minutes old)

**Solution:** Wait for folders to age, or adjust the threshold in code

#### Issue 2: Row Count Mismatch

**Error:** `Row count mismatch: Files=1000, Loaded=950`

**Cause:**
- Headers counted incorrectly
- Some CSV files malformed
- Encoding issues

**Solution:**
- Check CSV file structure
- Verify header is present in all files
- Check for encoding issues (UTF-8 vs UTF-16)

#### Issue 3: Validation Failures

**Error:** `Check for NULL customer IDs: Count 5 > 0`

**Cause:** Data quality issues in source files

**Solution:**
- Fix source data
- Or adjust validation threshold if acceptable
- Check data generation process

#### Issue 4: Schema Mismatch

**Error:** `Column 'xyz' not found in table`

**Cause:** CSV columns don't match table schema

**Solution:**
- Verify table schema matches CSV structure
- Update table schema if needed
- Check column names (case-sensitive)

---

## Examples

### Example 1: Simple Customer Load

**Config:**
```json
{
  "job_name": "Customer Load",
  "s3_bucket": "my-bucket",
  "landing_loc": "landing/customers/",
  "database_name": "etl_db",
  "landing_table": "staging_customers",
  "archive_s3_path": "archive/customers/",
  "validation_queries": [
    {
      "name": "Check NULLs",
      "query": "SELECT COUNT(*) FROM staging_temp WHERE customer_id IS NULL",
      "max_allowed": 0
    }
  ]
}
```

**Folder Structure:**
```
s3://my-bucket/landing/customers/20260127_1300/
  ├── customers_part1.csv (500 rows)
  └── customers_part2.csv (300 rows)
```

**Expected Result:**
```
✓ 2 files processed
✓ 800 rows loaded
✓ 1 validation passed
✓ Files archived to s3://my-bucket/archive/customers/
```

### Example 2: Transaction Load with Multiple Validations

**Config:**
```json
{
  "validation_queries": [
    {
      "name": "NULL transaction IDs",
      "query": "SELECT COUNT(*) FROM staging_temp WHERE transaction_id IS NULL",
      "max_allowed": 0
    },
    {
      "name": "Invalid amounts",
      "query": "SELECT COUNT(*) FROM staging_temp WHERE amount <= 0",
      "max_allowed": 0
    },
    {
      "name": "Future dates",
      "query": "SELECT COUNT(*) FROM staging_temp WHERE transaction_date > CURRENT_DATE()",
      "max_allowed": 0
    }
  ]
}
```

**Execution:**
```
[1] NULL transaction IDs → 0 records → ✓ PASS
[2] Invalid amounts → 0 records → ✓ PASS
[3] Future dates → 2 records → ❌ FAIL

❌ FAIL file created: s3://bucket/status/staging_transactions.FAIL
```

---

## Best Practices

1. **Always test validations** - Run queries manually first
2. **Start with critical validations** - NULL checks, duplicates
3. **Monitor FAIL files** - Set up alerts for FAIL file creation
4. **Review audit records** - Check patterns and trends
5. **Archive old audit records** - Clean up periodically
6. **Use appropriate thresholds** - Not everything needs `max_allowed: 0`
7. **Document validations** - Use clear names and descriptions
8. **Test with sample data** - Verify behavior before production

---

## Summary

This enhanced Glue job provides:

✅ **Production-ready** - All error handling and validations
✅ **Config-driven** - No code changes for new tables
✅ **Self-documenting** - Comprehensive audit trail
✅ **Fail-safe** - FAIL file system prevents cascading failures
✅ **Efficient** - Only processes eligible files
✅ **Atomic** - Archive only on success

Ready to use in your ETL pipeline!
