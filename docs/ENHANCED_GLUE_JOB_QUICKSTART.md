# Enhanced Glue Job - Quick Start

## What You Have

A production-ready, config-driven Glue ETL job with:

✅ **Schema auto-detection** from staging table
✅ **Config-driven validations**
✅ **Age-based folder filtering** (> 59 minutes)
✅ **File count validation**
✅ **Comprehensive auditing**
✅ **FAIL file checkpoint system**
✅ **Archive only on success**

---

## Files Created

1. **pyscript/enhanced_config_driven_glue_job.py** - Main Glue script (900+ lines)
2. **config/glue_job_config.json** - Example configuration
3. **docs/ENHANCED_GLUE_JOB_GUIDE.md** - Complete documentation

---

## Quick Setup (5 Steps)

### Step 1: Create Staging Table

```sql
CREATE TABLE etl_demo_db.staging_customers (
  customer_id STRING,
  customer_name STRING,
  email STRING,
  phone STRING,

  -- Audit columns (added by script)
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

### Step 2: Upload Config

Update `config/glue_job_config.json` with your bucket name:

```json
{
  "jobs": [
    {
      "s3_bucket": "your-bucket-name",  // UPDATE THIS
      "landing_loc": "landing/customers/",
      "database_name": "etl_demo_db",
      "landing_table": "staging_customers",
      "archive_s3_path": "archive/customers/",

      "validation_queries": [
        {
          "name": "Check for NULL customer IDs",
          "query": "SELECT COUNT(*) FROM staging_temp WHERE customer_id IS NULL",
          "max_allowed": 0
        }
      ]
    }
  ]
}
```

Upload to S3:

```bash
aws s3 cp config/glue_job_config.json s3://your-bucket/config/
```

### Step 3: Upload Script

```bash
aws s3 cp pyscript/enhanced_config_driven_glue_job.py \
  s3://your-bucket/scripts/
```

### Step 4: Create Glue Job

```bash
aws glue create-job \
  --name enhanced-staging-load \
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
  --worker-type "G.1X"
```

### Step 5: Create Test Data

Create folder structure:

```bash
aws s3api put-object --bucket your-bucket --key landing/customers/20260127_1200/
```

Upload sample CSV:

```csv
customer_id,customer_name,email,phone
CUST0001,John Doe,john@example.com,555-1234
CUST0002,Jane Smith,jane@example.com,555-5678
```

```bash
aws s3 cp sample.csv s3://your-bucket/landing/customers/20260127_1200/
```

---

## Run the Job

```bash
# Wait 60 minutes (for folder to age)
# Or modify the script to use a lower threshold for testing

# Run job
aws glue start-job-run --job-name enhanced-staging-load
```

---

## What Happens

```
1. ✓ Check FAIL file (none found)
2. ✓ Find folders > 59 minutes old
3. ✓ Collect CSV files
4. ✓ Count source rows: 2
5. ✓ Get schema from staging_customers
6. ✓ Load CSV files
7. ✓ Validate row counts: 2 = 2
8. ✓ Run validations: PASS
9. ✓ Insert 2 rows into staging_customers
10. ✓ Write audit record
11. ✓ Archive files
12. ✓ Job complete
```

---

## Key Features Explained

### 1. Schema Auto-Detection

**No need to specify schema in config!**

```python
# Script queries the target table
schema = get_table_schema(database_name, table_name)

# Uses that schema to read CSV files
df = spark.read.schema(schema).csv(csv_files)
```

### 2. Config-Driven Validations

**Define checks in JSON, not code:**

```json
{
  "name": "Check NULLs",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE customer_id IS NULL",
  "max_allowed": 0
}
```

**If count > 0:**
- ❌ Validation fails
- FAIL file created
- Job aborts
- Data NOT loaded

### 3. Folder Age Filter

**Only process folders > 59 minutes old:**

```
Current time: 14:32

Folders:
├── 20260127_1300/ (Modified: 13:30) → Age: 62 min → ✓ PROCESS
└── 20260127_1400/ (Modified: 14:15) → Age: 17 min → ✗ SKIP
```

**Why?** Ensures all files are uploaded before processing.

### 4. FAIL File System

**Prevents cascading failures:**

```
Run 1: Validation fails → Creates staging_customers.FAIL
Run 2: Checks for FAIL file → Found! → Aborts immediately
Run 3: User fixes data + deletes FAIL file → Job runs
```

**Location:** `s3://bucket/status/staging_customers.FAIL`

### 5. File Count Validation

**Counts rows in CSV files vs loaded rows:**

```
CSV files: 1,000 rows (excluding headers)
Loaded: 950 rows

❌ Mismatch! Creates FAIL file, aborts.
```

### 6. Archive on Success Only

**Files only moved after:**
- ✓ Row counts match
- ✓ All validations pass
- ✓ Data inserted successfully

**Atomic operation:** All or nothing.

### 7. Audit Trail

**Every run creates audit record:**

```
s3://bucket/audit/staging_customers/audit_20260127_143215.txt

Content:
- Folders processed
- Files loaded
- Row counts
- Validation results
- Timestamp
- Duration
```

---

## Example Validations

### Check for NULLs

```json
{
  "name": "Check NULL IDs",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE customer_id IS NULL",
  "max_allowed": 0
}
```

### Check for Duplicates

```json
{
  "name": "Check duplicates",
  "query": "SELECT COUNT(*) FROM (SELECT customer_id, COUNT(*) as cnt FROM staging_temp GROUP BY customer_id HAVING cnt > 1)",
  "max_allowed": 0
}
```

### Check Email Format

```json
{
  "name": "Check email format",
  "query": "SELECT COUNT(*) FROM staging_temp WHERE email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
  "max_allowed": 0
}
```

### Check Referential Integrity

```json
{
  "name": "Check orphan customers",
  "query": "SELECT COUNT(*) FROM staging_temp st LEFT JOIN glue_catalog.{database}.dim_customers c ON st.customer_id = c.customer_id WHERE c.customer_id IS NULL",
  "max_allowed": 0
}
```

---

## Monitoring

### Check Job Status

```bash
aws glue get-job-runs --job-name enhanced-staging-load --max-results 5
```

### Check for FAIL Files

```bash
aws s3 ls s3://your-bucket/status/ | grep FAIL
```

### View Audit Records

```bash
aws s3 ls s3://your-bucket/audit/staging_customers/
aws s3 cp s3://your-bucket/audit/staging_customers/audit_20260127_143215.txt -
```

### CloudWatch Logs

Go to CloudWatch → Log groups → `/aws-glue/jobs/output`

---

## Troubleshooting

### No Folders Found

**Error:** `No folders older than 59 minutes found`

**Fix:**
- Wait for folders to age
- Or modify age threshold for testing:
  ```python
  age_threshold = timedelta(minutes=5)  # For testing
  ```

### Validation Failures

**Error:** `Check for NULL customer IDs: Count 5 > 0`

**Fix:**
1. Check source data
2. Fix data quality issues
3. Delete FAIL file
4. Re-run job

### Row Count Mismatch

**Error:** `Row count mismatch: Files=1000, Loaded=950`

**Fix:**
- Check CSV format
- Verify all files have headers
- Check for encoding issues

---

## Next Steps

1. **Add more validations** to config
2. **Set up hourly trigger**
3. **Monitor FAIL files**
4. **Review audit records**
5. **Deploy to production**

---

## Full Documentation

See **docs/ENHANCED_GLUE_JOB_GUIDE.md** for complete details.

---

## Summary

You now have:

✅ Production-ready Glue job
✅ Config-driven validations
✅ Comprehensive error handling
✅ Audit trail for all runs
✅ Fail-safe mechanism

**Zero code changes needed to add new tables - just update config!** 🚀
