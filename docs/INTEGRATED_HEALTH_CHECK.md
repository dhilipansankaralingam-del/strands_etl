# Integrated Health Check Module

## Overview

The health check module is now **integrated directly into the enhanced_config_driven_glue_job.py** script. It runs automatically at the end of your ETL job execution based on configuration settings.

## How It Works

### 1. Execution Flow

```
ETL Job Starts
    ↓
Process Job 1 (Customer data)
    ↓
Process Job 2 (Transaction data)
    ↓
Process Job 3 (Product data)
    ↓
All Jobs Complete Successfully
    ↓
Check: Should run health check?
    • Is health_check.enabled = true?
    • Is current hour >= trigger_hour (21 = 9 PM)?
    • Did health check already run today?
    ↓
If YES → Run Health Check:
    • Query audit table for today's data
    • Calculate health metrics
    • Check for issues
    • Generate HTML email dashboard
    • Send email via SMTP
    • Create marker file (prevent duplicate runs)
    ↓
ETL Job Completes
```

### 2. Key Features

✅ **Automatic Trigger**: Runs after all ETL jobs complete, if conditions are met
✅ **Once-Per-Day**: Uses marker file to prevent duplicate runs
✅ **Time-Based**: Only runs at or after configured hour (default: 9 PM)
✅ **Non-Blocking**: Health check failures don't fail the main ETL job
✅ **Comprehensive Dashboard**: Beautiful HTML email with charts
✅ **Health Scoring**: Pass/Fail validation logic as requested

## Configuration

### Add to `glue_job_config.json`

```json
{
  "jobs": [
    // ... your existing jobs ...
  ],

  "global_settings": {
    "folder_age_threshold_minutes": 59,
    // ... other settings ...

    "health_check": {
      "enabled": true,
      "trigger_hour": 21,
      "database": "etl_demo_db",
      "audit_table": "etl_audit_log",
      "athena_output_location": "s3://your-bucket-name/athena-results/",

      "email": {
        "sender": "master@ace.aaa.com",
        "recipients": [
          "email@ab.com",
          "data-team@company.com"
        ],
        "smtp_host": "10.19.10.29",
        "smtp_port": 25,
        "use_tls": true,
        "username": "",
        "password": ""
      }
    }
  }
}
```

### Configuration Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `enabled` | boolean | Enable/disable health check | `false` |
| `trigger_hour` | number | Hour (0-23) when health check can run | `21` (9 PM) |
| `database` | string | Glue database with audit table | Required |
| `audit_table` | string | Audit table name | Required |
| `athena_output_location` | string | S3 path for Athena results | Required |
| `email.sender` | string | Email FROM address | Required |
| `email.recipients` | array | List of email addresses | Required |
| `email.smtp_host` | string | SMTP server | Required |
| `email.smtp_port` | number | SMTP port | `25` |
| `email.use_tls` | boolean | Use STARTTLS | `false` |
| `email.username` | string | SMTP auth username | Optional |
| `email.password` | string | SMTP auth password | Optional |

## Health Check Logic (As Requested)

For each table processed during the day, the health check validates:

### 1. File Count vs Loaded Count
```
SUM(file_row_count) = SUM(loaded_count)
```
If counts don't match → **FAIL** → Issue reported

### 2. Count Match Status
```
All records must have count_match = 'PASS'
```
If any record has count_match = 'FAIL' → **FAIL** → Issue reported

### 3. Validation Status
```
All records must have validation_status = 'PASS'
```
If any validation failed → **FAIL** → Issue reported

### 4. Job Status
```
All job runs must have status = 'SUCCESS'
```
If any job failed → **FAIL** → Issue reported

### Overall Health Score

```
Health Score = (Passed Checks / Total Checks) × 100%

Where:
  Passed Checks = validation_pass + count_match_pass
  Failed Checks = validation_fail + count_match_fail
```

**Exit Behavior**:
- If **ALL checks pass**: Email shows ✅ **100% Health Score**
- If **ANY check fails**: Email shows ⚠️/🚨 with **reduced Health Score**

**Important**: Health check failures **do NOT fail the ETL job** - they only send an alert email.

## Scheduling Strategy

### Option 1: Hourly ETL Job (Recommended)

Schedule your ETL job to run **every hour** from 8 AM to 8 PM:

```bash
# EventBridge schedule: Every hour from 8 AM to 8 PM
cron(0 8-20 * * ? *)
```

**Behavior**:
- **8:00 AM - 8:00 PM**: ETL runs hourly, processes data, writes audit
- **9:00 PM run**: ETL completes → Health check triggers (trigger_hour = 21)
- **9:00 PM**: Email sent with full day's summary
- **10:00 PM - 7:00 PM**: ETL runs but health check skips (already ran today)

### Option 2: Continuous ETL Job

Schedule your ETL job to run **continuously** (e.g., every 30 minutes):

```bash
# EventBridge schedule: Every 30 minutes
rate(30 minutes)
```

**Behavior**:
- First run at or after 9 PM each day → Health check triggers
- Subsequent runs → Health check skips (marker file exists)

### Option 3: Custom Time

Change `trigger_hour` to match your needs:

```json
{
  "health_check": {
    "trigger_hour": 18  // 6 PM
  }
}
```

## Installation Requirements

### Add matplotlib to Glue Job

The health check uses matplotlib for charts. Add to your Glue job:

**Via AWS CLI**:
```bash
aws glue update-job \
  --job-name your-etl-job \
  --job-update '{
    "DefaultArguments": {
      "--additional-python-modules": "matplotlib==3.7.1,numpy==1.24.3"
    }
  }'
```

**Via AWS Console**:
1. Go to AWS Glue → Jobs → Your Job
2. Edit job → Job details
3. Add to **Python library path**:
   ```
   matplotlib==3.7.1,numpy==1.24.3
   ```

**Fallback**: If matplotlib installation fails, charts will be skipped but email will still be sent with tables and metrics.

## Testing

### 1. Enable Health Check in Config

```json
{
  "health_check": {
    "enabled": true,
    "trigger_hour": 0  // Set to 0 for immediate testing
  }
}
```

### 2. Run ETL Job Manually

```bash
aws glue start-job-run \
  --job-name your-etl-job \
  --arguments '{"--json_file_name":"config/glue_job_config.json","--bucket_name":"your-bucket"}'
```

### 3. Check Logs

```bash
# Look for health check section in logs
aws logs tail /aws-glue/jobs/output --follow \
  --log-stream-name-prefix your-job-run-id

# Expected output:
# ================================================================================
# DAILY HEALTH CHECK
# ================================================================================
# Running health check for 2026-02-10
# ✓ Found audit data for 3 table(s)
# ✅ HEALTH CHECK PASSED
# ✓ Health check email sent to 2 recipient(s)
# ✓ Created health check marker: health_check_markers/2026-02-10.ran
# ✓ Health check completed (Score: 100.0%)
```

### 4. Verify Email

Check your inbox for email with subject like:
```
🎉 Data Quality Summary - 2026-02-10 | 100% Health Score
```

### 5. Reset for Next Test

Delete the marker file to allow re-running:

```bash
aws s3 rm s3://your-bucket/health_check_markers/2026-02-10.ran
```

## Troubleshooting

### Health Check Not Running

**Check 1**: Is it enabled?
```json
"health_check": { "enabled": true }
```

**Check 2**: Is it the right time?
```json
"trigger_hour": 21  // Current hour must be >= 21
```

**Check 3**: Did it already run today?
```bash
# Check for marker file
aws s3 ls s3://your-bucket/health_check_markers/
```

**Check 4**: Check logs
```bash
aws logs tail /aws-glue/jobs/output --follow
```

### No Email Received

**Check 1**: SMTP settings
- Verify `smtp_host` and `smtp_port`
- Test connectivity: `telnet 10.19.10.29 25`

**Check 2**: Email addresses
- Verify `sender` and `recipients` are correct

**Check 3**: Logs
```bash
# Look for "Health check email sent" message
aws logs tail /aws-glue/jobs/output --follow | grep "email"
```

### Charts Not Showing in Email

**Cause**: matplotlib not installed

**Solution 1**: Install matplotlib
```bash
aws glue update-job \
  --job-name your-etl-job \
  --job-update '{
    "DefaultArguments": {
      "--additional-python-modules": "matplotlib==3.7.1"
    }
  }'
```

**Solution 2**: Use without charts
- Email will still send with KPI cards and tables
- Charts are optional

### Athena Query Timeout

**Cause**: Large audit table

**Solution 1**: Run MSCK REPAIR
```sql
MSCK REPAIR TABLE etl_demo_db.etl_audit_log;
```

**Solution 2**: Increase Glue timeout
```bash
aws glue update-job --job-name your-etl-job --job-update '{"Timeout": 120}'
```

## Email Dashboard Preview

### Subject Line
```
🎉 Data Quality Summary - 2026-02-10 | 98% Health Score
```

### Email Content
- **Header**: Date, time, branding
- **Hero Card**: Large health score percentage with emoji
- **Charts**: Donut (pass/fail) + Bar (issues by category)
- **KPI Cards**: Files, rows, tables processed
- **Detailed Table**: Per-table validation results
- **Footer**: Dynamic message based on health score

### Health Score Indicators

| Score | Emoji | Status | Email Tone |
|-------|-------|--------|------------|
| 90-100% | 🎉 | Excellent | "Stellar data quality!" |
| 70-89% | 👍 | Good | "Good progress!" |
| 50-69% | ⚠️ | Warning | "Issues need attention" |
| 0-49% | 🚨 | Critical | "Critical issues detected!" |

## Comparison: Integrated vs Standalone

### Integrated Health Check (This Implementation)

✅ Single Glue job
✅ Automatically runs after ETL completes
✅ Shares configuration with ETL
✅ No additional scheduling needed
✅ Simpler to maintain

**Use when**: You want automatic health checks without separate job management

### Standalone Health Check (Previous Implementation)

✅ Separate Glue job
✅ Independent scheduling
✅ Can run without ETL job
✅ More flexible timing

**Use when**: You want health checks independent of ETL execution

## Best Practices

1. **Set Appropriate Trigger Hour**: Choose a time after all ETL jobs typically complete
   ```json
   "trigger_hour": 21  // 9 PM - after 8 PM final ETL run
   ```

2. **Monitor Marker Files**: Periodically clean up old markers
   ```bash
   # Keep only last 30 days
   aws s3 ls s3://bucket/health_check_markers/ | \
     awk '$1 < "'$(date -d '30 days ago' +%Y-%m-%d)'" {print $4}' | \
     xargs -I {} aws s3 rm s3://bucket/health_check_markers/{}
   ```

3. **Use IAM Roles for SMTP Credentials**: Store in AWS Secrets Manager instead of config
   ```python
   # Add to script
   secrets_client = boto3.client('secretsmanager')
   secret = secrets_client.get_secret_value(SecretId='smtp-creds')
   creds = json.loads(secret['SecretString'])
   ```

4. **Set Up Alerts**: Create CloudWatch alarm for health check failures
   ```bash
   aws cloudwatch put-metric-alarm \
     --alarm-name etl-health-low \
     --comparison-operator LessThanThreshold \
     --evaluation-periods 1 \
     --metric-name HealthScore \
     --namespace ETL \
     --period 86400 \
     --threshold 80 \
     --alarm-actions arn:aws:sns:us-east-1:123:etl-alerts
   ```

5. **Test First**: Set `trigger_hour: 0` for testing, then change to desired hour

## Migration from Standalone to Integrated

If you already have the standalone `daily_health_check_glue_job.py`:

### Step 1: Update Configuration
Add `health_check` section to your existing `glue_job_config.json` (already done above)

### Step 2: Update Glue Job Script
Replace your Glue job script with the updated `enhanced_config_driven_glue_job.py`

```bash
aws s3 cp pyscript/enhanced_config_driven_glue_job.py \
  s3://your-bucket/scripts/enhanced_config_driven_glue_job.py
```

### Step 3: Install matplotlib
```bash
aws glue update-job \
  --job-name your-etl-job \
  --job-update '{
    "DefaultArguments": {
      "--additional-python-modules": "matplotlib==3.7.1,numpy==1.24.3"
    }
  }'
```

### Step 4: Delete Standalone Job (Optional)
```bash
# Delete standalone health check job
aws glue delete-job --job-name daily-health-check

# Delete EventBridge rule
aws events delete-rule --name daily-health-check-9pm
```

### Step 5: Test
```bash
# Run ETL job manually with trigger_hour set to current hour
aws glue start-job-run --job-name your-etl-job
```

## Summary

The integrated health check:

1. ✅ **Runs automatically** after ETL jobs complete
2. ✅ **Once per day** at configured hour
3. ✅ **Validates data quality** using your exact logic:
   - File counts = loaded counts
   - All count_match = PASS
   - All validation_status = PASS
   - All job status = SUCCESS
4. ✅ **Sends beautiful HTML email** with dashboard
5. ✅ **Non-intrusive** - doesn't fail ETL job if checks fail
6. ✅ **Easy to configure** - all in `glue_job_config.json`

This provides **end-to-end monitoring** without needing separate job management!
