# Audit Table Guide - ETL Job Monitoring

## Overview

The enhanced Glue job now writes audit records in **two formats**:

1. **Human-readable text files** - For manual review
   - Location: `s3://bucket/audit/{table_name}/audit_YYYYMMDD_HHMMSS.txt`

2. **Pipe-delimited files** - For querying as a table
   - Location: `s3://bucket/audit_table/year=YYYY/month=MM/day=DD/audit_YYYYMMDD_HHMMSS.txt`
   - Partitioned by year/month/day
   - Can be queried using AWS Athena or Glue

---

## File Structure

### Partition Structure

```
s3://your-bucket/audit_table/
├── year=2026/
│   ├── month=01/
│   │   ├── day=27/
│   │   │   ├── audit_20260127_143215.txt
│   │   │   ├── audit_20260127_153022.txt
│   │   │   └── audit_20260127_163445.txt
│   │   └── day=28/
│   │       └── audit_20260128_023015.txt
│   └── month=02/
│       └── day=09/
│           ├── audit_20260209_024530.txt
│           └── audit_20260209_034622.txt
```

### File Content Format

**Header:**
```
table_name|job_name|job_run_id|folders|file_count|file_row_count|loaded_count|count_match|validation_status|validation_details|load_timestamp|duration|status
```

**Data Row:**
```
etl_demo_db.staging_customers|customer-staging-load|jr_abc123|landing/customers/20260209T020000/,landing/customers/20260209T030000/|6|15234|15234|PASS|PASS|All 5 validations passed|2026-02-09 14:32:15|127.45 seconds|SUCCESS
```

---

## Step 1: Create the Audit Table

### Option A: Using AWS Athena

1. Go to **AWS Athena** console
2. Select your database (e.g., `etl_demo_db`)
3. Run the SQL from `sql/create_audit_table.sql`:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS etl_demo_db.etl_audit_log (
    table_name STRING COMMENT 'Staging table name',
    job_name STRING COMMENT 'Glue job name',
    job_run_id STRING COMMENT 'Glue job run ID',
    folders STRING COMMENT 'Comma-separated list of folders processed',
    file_count INT COMMENT 'Number of CSV files loaded',
    file_row_count BIGINT COMMENT 'Total rows in source CSV files',
    loaded_count BIGINT COMMENT 'Total rows loaded into table',
    count_match STRING COMMENT 'PASS if counts match, FAIL otherwise',
    validation_status STRING COMMENT 'Overall validation status (PASS/FAIL)',
    validation_details STRING COMMENT 'Details of validation results',
    load_timestamp STRING COMMENT 'When the load completed',
    duration STRING COMMENT 'Job duration in seconds',
    status STRING COMMENT 'Job status (SUCCESS/FAIL)'
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://your-bucket-name/audit_table/'
TBLPROPERTIES (
    'skip.header.line.count'='1'
);
```

**Important:** Replace `your-bucket-name` with your actual S3 bucket!

### Option B: Using AWS CLI

```bash
# Upload the SQL file to S3
aws s3 cp sql/create_audit_table.sql s3://your-bucket/scripts/

# Execute via Athena CLI (requires AWS CLI v2)
aws athena start-query-execution \
  --query-string "$(cat sql/create_audit_table.sql | head -n 30)" \
  --result-configuration "OutputLocation=s3://your-bucket/athena-results/" \
  --query-execution-context "Database=etl_demo_db"
```

---

## Step 2: Discover Partitions (MSCK REPAIR)

After your Glue jobs run and create audit files, discover the partitions:

### Using Athena

```sql
MSCK REPAIR TABLE etl_demo_db.etl_audit_log;
```

**Expected output:**
```
Partitions not in metastore: etl_audit_log:year=2026/month=02/day=09
Repair: Added partition to metastore etl_audit_log:year=2026/month=02/day=09
```

### Automate MSCK REPAIR

Create a Lambda function or Glue job that runs daily:

```python
import boto3

def lambda_handler(event, context):
    athena = boto3.client('athena')

    response = athena.start_query_execution(
        QueryString='MSCK REPAIR TABLE etl_demo_db.etl_audit_log;',
        ResultConfiguration={
            'OutputLocation': 's3://your-bucket/athena-results/'
        },
        QueryExecutionContext={
            'Database': 'etl_demo_db'
        }
    )

    return {'statusCode': 200, 'body': 'MSCK REPAIR initiated'}
```

---

## Step 3: Query the Audit Table

### Basic Queries

#### 1. View Recent Audit Records

```sql
SELECT
    table_name,
    job_name,
    status,
    file_count,
    loaded_count,
    load_timestamp,
    duration
FROM etl_demo_db.etl_audit_log
ORDER BY load_timestamp DESC
LIMIT 100;
```

#### 2. Check for Failed Jobs

```sql
SELECT
    table_name,
    job_name,
    job_run_id,
    status,
    validation_status,
    load_timestamp
FROM etl_demo_db.etl_audit_log
WHERE status = 'FAIL'
ORDER BY load_timestamp DESC;
```

#### 3. Check for Validation Failures

```sql
SELECT
    table_name,
    validation_status,
    validation_details,
    load_timestamp
FROM etl_demo_db.etl_audit_log
WHERE validation_status = 'FAIL'
ORDER BY load_timestamp DESC;
```

#### 4. Check for Count Mismatches

```sql
SELECT
    table_name,
    file_row_count,
    loaded_count,
    (file_row_count - loaded_count) AS difference,
    count_match,
    load_timestamp
FROM etl_demo_db.etl_audit_log
WHERE count_match = 'FAIL'
ORDER BY load_timestamp DESC;
```

### Advanced Queries

#### 5. Job Performance by Table

```sql
SELECT
    table_name,
    COUNT(*) as run_count,
    AVG(CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE)) as avg_duration_seconds,
    MIN(CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE)) as min_duration_seconds,
    MAX(CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE)) as max_duration_seconds,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as fail_count,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM etl_demo_db.etl_audit_log
WHERE year = '2026' AND month = '02'
GROUP BY table_name
ORDER BY run_count DESC;
```

#### 6. Daily Job Statistics

```sql
SELECT
    year,
    month,
    day,
    COUNT(*) as total_jobs,
    SUM(file_count) as total_files_processed,
    SUM(loaded_count) as total_rows_loaded,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_jobs,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_jobs,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM etl_demo_db.etl_audit_log
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC
LIMIT 30;
```

#### 7. Jobs with Longest Duration

```sql
SELECT
    table_name,
    job_name,
    job_run_id,
    file_count,
    loaded_count,
    CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE) as duration_seconds,
    load_timestamp
FROM etl_demo_db.etl_audit_log
WHERE status = 'SUCCESS'
ORDER BY duration_seconds DESC
LIMIT 20;
```

#### 8. Data Volume Trends

```sql
SELECT
    DATE_PARSE(load_timestamp, '%Y-%m-%d %H:%i:%s') as load_date,
    table_name,
    SUM(file_count) as total_files,
    SUM(loaded_count) as total_rows,
    AVG(CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE)) as avg_duration
FROM etl_demo_db.etl_audit_log
WHERE year = '2026'
  AND month = '02'
  AND status = 'SUCCESS'
GROUP BY DATE_PARSE(load_timestamp, '%Y-%m-%d %H:%i:%s'), table_name
ORDER BY load_date DESC, table_name;
```

#### 9. Folder Processing Patterns

```sql
SELECT
    table_name,
    folders,
    COUNT(*) as times_processed,
    AVG(loaded_count) as avg_rows_loaded,
    MAX(load_timestamp) as last_processed
FROM etl_demo_db.etl_audit_log
WHERE status = 'SUCCESS'
GROUP BY table_name, folders
ORDER BY times_processed DESC
LIMIT 50;
```

#### 10. Validation Success Rate

```sql
SELECT
    table_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN validation_status = 'PASS' THEN 1 ELSE 0 END) as validations_passed,
    SUM(CASE WHEN validation_status = 'FAIL' THEN 1 ELSE 0 END) as validations_failed,
    ROUND(100.0 * SUM(CASE WHEN validation_status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) as validation_pass_rate_pct
FROM etl_demo_db.etl_audit_log
WHERE year = '2026'
GROUP BY table_name
ORDER BY validation_pass_rate_pct ASC;
```

---

## Step 4: Create Views for Common Queries

### View 1: Recent Job Status

```sql
CREATE OR REPLACE VIEW etl_demo_db.vw_recent_job_status AS
SELECT
    table_name,
    job_name,
    job_run_id,
    status,
    validation_status,
    count_match,
    file_count,
    loaded_count,
    CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE) as duration_seconds,
    load_timestamp,
    year,
    month,
    day
FROM etl_demo_db.etl_audit_log
WHERE year = CAST(YEAR(CURRENT_DATE) AS VARCHAR)
  AND month = LPAD(CAST(MONTH(CURRENT_DATE) AS VARCHAR), 2, '0')
ORDER BY load_timestamp DESC;
```

**Usage:**
```sql
SELECT * FROM etl_demo_db.vw_recent_job_status LIMIT 100;
```

### View 2: Failed Jobs

```sql
CREATE OR REPLACE VIEW etl_demo_db.vw_failed_jobs AS
SELECT
    table_name,
    job_name,
    job_run_id,
    status,
    validation_status,
    validation_details,
    load_timestamp,
    year,
    month,
    day
FROM etl_demo_db.etl_audit_log
WHERE status = 'FAIL' OR validation_status = 'FAIL' OR count_match = 'FAIL'
ORDER BY load_timestamp DESC;
```

**Usage:**
```sql
SELECT * FROM etl_demo_db.vw_failed_jobs;
```

### View 3: Job Performance Summary

```sql
CREATE OR REPLACE VIEW etl_demo_db.vw_job_performance AS
SELECT
    table_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct,
    AVG(CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE)) as avg_duration_seconds,
    SUM(loaded_count) as total_rows_loaded,
    MAX(load_timestamp) as last_run_timestamp
FROM etl_demo_db.etl_audit_log
GROUP BY table_name;
```

**Usage:**
```sql
SELECT * FROM etl_demo_db.vw_job_performance ORDER BY total_runs DESC;
```

---

## Step 5: Set Up Monitoring

### Option 1: CloudWatch Dashboard

Create a CloudWatch dashboard that queries the audit table via Athena.

### Option 2: QuickSight Dashboard

1. Go to **AWS QuickSight**
2. Create a new dataset from Athena
3. Select `etl_demo_db.etl_audit_log`
4. Create visualizations:
   - Job success rate over time
   - Average job duration by table
   - Failed jobs count
   - Data volume loaded per day

### Option 3: SNS Alerts

Create a Lambda that queries for failures and sends SNS notifications:

```python
import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    athena = boto3.client('athena')
    sns = boto3.client('sns')

    # Query for failures in last hour
    query = """
    SELECT table_name, job_name, status, load_timestamp
    FROM etl_demo_db.etl_audit_log
    WHERE status = 'FAIL'
      AND load_timestamp > DATE_FORMAT(CURRENT_TIMESTAMP - INTERVAL '1' HOUR, '%Y-%m-%d %H:%i:%s')
    """

    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': 's3://your-bucket/athena-results/'},
        QueryExecutionContext={'Database': 'etl_demo_db'}
    )

    # Wait for query completion and check results
    # If failures found, send SNS alert
    # ... (implementation details)
```

---

## Step 6: Maintenance

### Clean Up Old Partitions

```sql
-- View all partitions
SHOW PARTITIONS etl_demo_db.etl_audit_log;

-- Drop old partitions (if needed)
ALTER TABLE etl_demo_db.etl_audit_log
DROP PARTITION (year='2025', month='12', day='01');
```

### Archive Old Data

```bash
# Move old audit files to archive
aws s3 mv s3://your-bucket/audit_table/year=2025/ \
  s3://your-bucket/audit_archive/year=2025/ \
  --recursive
```

### Refresh Partitions Regularly

Set up a daily EventBridge rule to run MSCK REPAIR:

```bash
# Create EventBridge rule
aws events put-rule \
  --name daily-audit-partition-refresh \
  --schedule-expression "cron(0 1 * * ? *)"

# Add Athena query as target
# ... (use Lambda or Step Functions)
```

---

## Troubleshooting

### Issue 1: No Data in Table

**Check:**
```sql
-- Verify partitions exist
SHOW PARTITIONS etl_demo_db.etl_audit_log;

-- If empty, run MSCK REPAIR
MSCK REPAIR TABLE etl_demo_db.etl_audit_log;
```

### Issue 2: Incorrect Data

**Check file format:**
```bash
# Download and inspect a sample file
aws s3 cp s3://your-bucket/audit_table/year=2026/month=02/day=09/audit_20260209_024530.txt - | head -5
```

Ensure it has:
- Header line
- Pipe-delimited fields
- No extra pipes in data

### Issue 3: Query Performance Slow

**Solutions:**
- Use partition filters: `WHERE year='2026' AND month='02'`
- Create views for common queries
- Consider converting to Parquet format for better performance

---

## Summary

You now have:

✅ **Human-readable audit files** in `s3://bucket/audit/{table_name}/`
✅ **Queryable audit table** in `s3://bucket/audit_table/`
✅ **Partitioned by date** for efficient queries
✅ **MSCK REPAIR support** for automatic partition discovery
✅ **SQL queries** for monitoring and analysis
✅ **Views** for common use cases

Run your Glue jobs, then query the audit table to track all executions! 📊
