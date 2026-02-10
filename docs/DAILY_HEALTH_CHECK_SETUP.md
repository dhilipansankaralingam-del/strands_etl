# Daily Health Check Setup Guide

## Overview

The Daily Health Check Glue job queries the `etl_audit_log` table, validates data quality, generates a comprehensive dashboard, and sends an HTML email report. It's designed to run once daily at 9 PM to provide end-of-day ETL health status.

## Features

### Health Check Logic

For each table processed during the day:
1. ✅ **File Count vs Loaded Count**: Sum of `file_row_count` must equal sum of `loaded_count`
2. ✅ **Count Match Status**: All `count_match` must be 'PASS'
3. ✅ **Validation Status**: All `validation_status` must be 'PASS'
4. ✅ **Job Status**: All jobs must complete with `status = 'SUCCESS'`

If **all checks pass**: Job exits with SUCCESS status ✅
If **any check fails**: Job exits with FAILURE status ❌

### Email Dashboard Includes

- 📊 **Health Score Hero Card** - Overall percentage with emoji indicator
- 🍩 **Donut Chart** - Pass/Fail distribution visualization
- 📊 **Bar Chart** - Issues breakdown by category
- 📈 **KPI Cards** - Files processed, total rows, loaded rows, table count
- ⏱️ **Gauge Charts** - Error rate by table
- 📋 **Detailed Validation Table** - Complete breakdown per table
- 🎨 **Professional Design** - Gradient headers, responsive layout, color-coded status

---

## Quick Start

### Step 1: Upload Configuration

```bash
# Edit the configuration file with your settings
vi config/health_check_config.json

# Upload to S3
aws s3 cp config/health_check_config.json \
  s3://your-bucket-name/config/health_check_config.json
```

### Step 2: Upload Glue Script

```bash
aws s3 cp pyscript/daily_health_check_glue_job.py \
  s3://your-bucket-name/scripts/daily_health_check_glue_job.py
```

### Step 3: Create Glue Job

```bash
aws glue create-job \
  --name daily-health-check \
  --role YourGlueServiceRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-bucket-name/scripts/daily_health_check_glue_job.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--CONFIG_S3_PATH": "s3://your-bucket-name/config/health_check_config.json",
    "--enable-glue-datacatalog": "true",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--TempDir": "s3://your-bucket-name/temp/"
  }' \
  --max-retries 0 \
  --timeout 60 \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X"
```

### Step 4: Install Matplotlib (Required for Charts)

The job requires matplotlib for chart generation. Add it to your Glue job:

**Option A: Use --additional-python-modules**
```bash
aws glue update-job \
  --job-name daily-health-check \
  --job-update '{
    "DefaultArguments": {
      "--additional-python-modules": "matplotlib==3.7.1"
    }
  }'
```

**Option B: Create Python wheel**
```bash
# On your local machine
pip install matplotlib -t python/
cd python && zip -r ../matplotlib.zip . && cd ..
aws s3 cp matplotlib.zip s3://your-bucket-name/libs/

# Update job
aws glue update-job \
  --job-name daily-health-check \
  --job-update '{
    "DefaultArguments": {
      "--extra-py-files": "s3://your-bucket-name/libs/matplotlib.zip"
    }
  }'
```

### Step 5: Schedule with EventBridge (9 PM Daily)

**Via AWS Console:**
1. Go to **Amazon EventBridge** > **Rules** > **Create rule**
2. Name: `daily-health-check-9pm`
3. Rule type: **Schedule**
4. Schedule pattern: **Cron expression**
   ```
   0 21 * * ? *
   ```
   (This runs at 9:00 PM UTC daily)
5. Target: **AWS Glue workflow** > Select `daily-health-check`
6. Create rule

**Via AWS CLI:**
```bash
# Create EventBridge rule
aws events put-rule \
  --name daily-health-check-9pm \
  --schedule-expression "cron(0 21 * * ? *)" \
  --state ENABLED \
  --description "Trigger daily health check at 9 PM"

# Add Glue job as target
aws events put-targets \
  --rule daily-health-check-9pm \
  --targets '[{
    "Id": "1",
    "Arn": "arn:aws:glue:us-east-1:123456789012:job/daily-health-check",
    "RoleArn": "arn:aws:iam::123456789012:role/EventBridgeGlueRole",
    "Input": "{\"CONFIG_S3_PATH\":\"s3://your-bucket-name/config/health_check_config.json\"}"
  }]'
```

**Important:** Adjust the cron expression for your timezone:
- **9 PM EST (UTC-5)**: `cron(0 2 * * ? *)` (2 AM UTC)
- **9 PM PST (UTC-8)**: `cron(0 5 * * ? *)` (5 AM UTC)
- **9 PM IST (UTC+5:30)**: `cron(30 15 * * ? *)` (3:30 PM UTC)

---

## Configuration File

### Example: `config/health_check_config.json`

```json
{
  "database": "etl_demo_db",
  "audit_table": "etl_audit_log",
  "athena_output_location": "s3://your-bucket-name/athena-results/",

  "check_previous_day": false,
  "send_email_on_no_data": true,

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
```

### Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `database` | string | Glue database containing audit table |
| `audit_table` | string | Name of audit table (default: `etl_audit_log`) |
| `athena_output_location` | string | S3 location for Athena query results |
| `check_previous_day` | boolean | If `true`, checks yesterday's data instead of today |
| `send_email_on_no_data` | boolean | Send notification if no audit records found |
| `email.sender` | string | Email FROM address |
| `email.recipients` | array | List of email recipients |
| `email.smtp_host` | string | SMTP server hostname or IP |
| `email.smtp_port` | number | SMTP port (25, 587, or 465) |
| `email.use_tls` | boolean | Enable STARTTLS encryption |
| `email.username` | string | SMTP authentication username (optional) |
| `email.password` | string | SMTP authentication password (optional) |

---

## Manual Testing

### Test Run via Console

1. Go to **AWS Glue** > **Jobs**
2. Select `daily-health-check`
3. Click **Run job**
4. Monitor execution in **Run details**

### Test Run via CLI

```bash
# Start job run
RUN_ID=$(aws glue start-job-run \
  --job-name daily-health-check \
  --arguments '{"--CONFIG_S3_PATH":"s3://your-bucket-name/config/health_check_config.json"}' \
  --query 'JobRunId' --output text)

echo "Job run started: $RUN_ID"

# Check status
aws glue get-job-run \
  --job-name daily-health-check \
  --run-id $RUN_ID \
  --query 'JobRun.JobRunState'

# View logs
aws logs tail /aws-glue/jobs/output --follow \
  --log-stream-name-prefix $RUN_ID
```

---

## Understanding the Output

### Exit Codes

| Exit Code | Meaning | Description |
|-----------|---------|-------------|
| 0 | SUCCESS ✅ | All health checks passed |
| 1 | FAILURE ❌ | One or more health checks failed |

### Health Score Calculation

```
Health Score = (Passed Checks / Total Checks) × 100
```

Where:
- **Passed Checks** = `validation_pass + count_match_pass`
- **Failed Checks** = `validation_fail + count_match_fail`

### Health Score Thresholds

| Score | Status | Emoji | Action |
|-------|--------|-------|--------|
| 90-100% | Excellent | 🎉 | No action needed |
| 70-89% | Good | 👍 | Monitor trends |
| 50-69% | Warning | ⚠️ | Investigate issues |
| 0-49% | Critical | 🚨 | Immediate attention required |

---

## Email Dashboard Examples

### Sample Email Subject Lines

```
🎉 Data Quality Summary - 2026-02-10 | 98% Health Score
📊 Data Quality Summary - 2026-02-10 | 75% Health Score
⚠️ Data Quality Summary - 2026-02-10 | 62% Health Score
🚨 Data Quality Summary - 2026-02-10 | 45% Health Score
```

### Dashboard Sections

1. **Header Banner**
   - Gradient background
   - Date and timestamp
   - Professional branding

2. **Health Score Hero**
   - Large percentage display
   - Color-coded (green/yellow/red)
   - Pass/Fail count badges

3. **Charts (if matplotlib available)**
   - Donut chart: Pass/Fail distribution
   - Bar chart: Issues by category
   - Gauge charts: Error rate per table

4. **KPI Cards**
   - 📁 Files Processed
   - 📊 Total Rows
   - ✅ Rows Loaded
   - 🗃️ Tables Processed

5. **Detailed Validation Table**
   - Table name
   - Validation details
   - Pass/Fail counts
   - Status indicators

6. **Footer**
   - Dynamic message based on health score
   - Contact information
   - Generated timestamp

---

## Troubleshooting

### Issue: No Email Received

**Check 1: SMTP Settings**
```bash
# Test SMTP connectivity
telnet 10.19.10.29 25
```

**Check 2: Email Configuration**
- Verify sender address is allowed
- Check recipient addresses are correct
- Ensure SMTP host/port are accessible from Glue VPC

**Check 3: Job Logs**
```bash
aws logs tail /aws-glue/jobs/output --follow \
  --log-stream-name-prefix daily-health-check
```

### Issue: No Charts in Email

**Cause**: matplotlib not installed

**Solution**: Install matplotlib using `--additional-python-modules`:
```bash
aws glue update-job \
  --job-name daily-health-check \
  --job-update '{
    "DefaultArguments": {
      "--additional-python-modules": "matplotlib==3.7.1,numpy==1.24.3"
    }
  }'
```

### Issue: Athena Query Timeout

**Cause**: Large audit table

**Solution 1**: Add date filter to queries (script already does this)

**Solution 2**: Increase Glue job timeout:
```bash
aws glue update-job \
  --job-name daily-health-check \
  --job-update '{"Timeout": 120}'
```

**Solution 3**: Run MSCK REPAIR on audit table:
```sql
MSCK REPAIR TABLE etl_demo_db.etl_audit_log;
```

### Issue: Job Fails with "No audit data found"

**Cause**: No ETL jobs ran on the check date

**Solution**: This is expected behavior. Set `send_email_on_no_data: true` to receive notifications.

---

## Advanced Configuration

### Check Previous Day's Data

If your health check runs early morning and you want to check yesterday:

```json
{
  "check_previous_day": true
}
```

### Multiple Email Recipients

```json
{
  "email": {
    "recipients": [
      "team-lead@company.com",
      "data-engineers@company.com",
      "operations@company.com"
    ]
  }
}
```

### SMTP with Authentication

For SMTP servers requiring login:

```json
{
  "email": {
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "use_tls": true,
    "username": "your-email@gmail.com",
    "password": "your-app-password"
  }
}
```

**Security Note**: Store credentials in AWS Secrets Manager:

```python
# Add to script
import json
secrets_client = boto3.client('secretsmanager')
secret = secrets_client.get_secret_value(SecretId='smtp-credentials')
creds = json.loads(secret['SecretString'])
config['email']['username'] = creds['username']
config['email']['password'] = creds['password']
```

---

## Integration with Monitoring

### CloudWatch Alarms

Create an alarm for failed health checks:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name etl-health-check-failed \
  --alarm-description "Alert when daily health check fails" \
  --metric-name JobRunState \
  --namespace Glue \
  --statistic Sum \
  --period 3600 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=JobName,Value=daily-health-check \
  --alarm-actions arn:aws:sns:us-east-1:123456789012:etl-alerts
```

### SNS Notifications

Send SNS notification on failure:

```python
# Add to script after health check
if not is_healthy:
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:123456789012:etl-alerts',
        Subject=f'ETL Health Check Failed - {check_date}',
        Message=f'Health check failed with {len(issues)} issues. Check email for details.'
    )
```

---

## Maintenance

### Update Email Template

The HTML template is embedded in `generate_html_report()` function. To customize:

1. Edit `pyscript/daily_health_check_glue_job.py`
2. Modify the HTML string in `generate_html_report()`
3. Upload to S3
4. Update Glue job script location

### Add Custom Metrics

To add custom KPI cards:

```python
# In generate_html_report() function
kpi_cards = [
    {"icon": "📁", "value": f"{total_files:,}", "label": "Files Processed", "color": "#3B82F6"},
    {"icon": "⏱️", "value": f"{avg_duration}s", "label": "Avg Duration", "color": "#F59E0B"},  # NEW
    # ... more cards
]
```

---

## Best Practices

1. **Schedule After ETL Jobs**: Run health check after all ETL jobs complete (e.g., 9 PM if ETL runs hourly from 8 AM-8 PM)

2. **Monitor Job History**: Review CloudWatch logs weekly to identify trends

3. **Test Email Rendering**: Send test emails to verify formatting across email clients

4. **Archive Old Audit Data**: Partition audit table by year/month/day for efficient queries

5. **Use SNS for Critical Alerts**: Combine email dashboard with SNS for immediate alerts

6. **Version Control**: Keep configuration files in Git alongside scripts

7. **Document Thresholds**: Document why specific health score thresholds were chosen

---

## FAQ

**Q: Can I run this multiple times per day?**
A: Yes, just create multiple EventBridge rules with different schedules.

**Q: Does this delete audit records?**
A: No, it only reads the audit table. No data is modified.

**Q: Can I customize the email template?**
A: Yes, edit the HTML in the `generate_html_report()` function.

**Q: What if matplotlib fails to install?**
A: Charts will be skipped, but the email will still be sent with tables and metrics.

**Q: Can I query multiple days at once?**
A: Yes, modify the SQL query in `get_daily_audit_summary()` to use a date range.

---

## Next Steps

1. ✅ Upload configuration to S3
2. ✅ Create Glue job
3. ✅ Install matplotlib dependency
4. ✅ Schedule with EventBridge
5. ✅ Test manual run
6. ✅ Verify email delivery
7. ✅ Set up CloudWatch alarms
8. ✅ Document for team

---

**Need Help?**
- 📧 Email: data-engineering@company.com
- 📚 Docs: https://docs.aws.amazon.com/glue/
- 🔧 Support: Create ticket in JIRA
