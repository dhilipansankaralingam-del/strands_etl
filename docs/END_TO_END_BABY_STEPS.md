# END TO END BABY STEPS

Complete guide for provisioning, manual component testing, and E2E testing of the ETL Framework.

---

## Table of Contents

- [PHASE 1: PREREQUISITES](#phase-1-prerequisites)
- [PHASE 2: PROVISIONING](#phase-2-provisioning)
  - [2.1 DynamoDB Tables](#21-dynamodb-tables)
  - [2.2 API Gateway](#22-api-gateway)
  - [2.3 Slack Webhook](#23-slack-webhook)
  - [2.4 Teams Webhook](#24-teams-webhook)
  - [2.5 Email (SES)](#25-email-ses)
- [PHASE 3: COMPONENT TESTING](#phase-3-component-testing)
  - [3.1 Test Config Parsing](#31-test-config-parsing)
  - [3.2 Test Notification Flags](#32-test-notification-flags)
  - [3.3 Test Notification Manager](#33-test-notification-manager)
  - [3.4 Test Teams Integration](#34-test-teams-integration)
  - [3.5 Test API Gateway](#35-test-api-gateway)
  - [3.6 Test Audit System](#36-test-audit-system)
- [PHASE 4: INTEGRATION TESTING](#phase-4-integration-testing)
  - [4.1 Simple JSON Configs](#41-simple-json-configs)
  - [4.2 Complex JSON Configs](#42-complex-json-configs)
- [PHASE 5: END-TO-END TESTING](#phase-5-end-to-end-testing)
- [PHASE 6: VERIFICATION CHECKLIST](#phase-6-verification-checklist)

---

# PHASE 1: PREREQUISITES

## 1.1 Environment Setup

```bash
# Navigate to project directory
cd /home/user/strands_etl

# Set Python path (REQUIRED for all tests)
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Verify Python path
echo $PYTHONPATH
```

## 1.2 Install Dependencies

```bash
pip install boto3 requests
```

## 1.3 AWS Credentials Setup

```bash
# Option A: Using AWS CLI
aws configure
# Enter: Access Key ID, Secret Access Key, Region (us-east-1), Output format (json)

# Option B: Using Environment Variables
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1

# Verify credentials
aws sts get-caller-identity
```

**Expected Output:**
```json
{
    "UserId": "AIDAXXXXXXXXXXXXXXXXX",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-user"
}
```

---

# PHASE 2: PROVISIONING

## 2.1 DynamoDB Tables

### Manual Steps (AWS Console)

1. Go to AWS Console → DynamoDB → Tables
2. Create table: `etl_run_audit`
   - Partition key: `run_id` (String)
   - Add GSI: `job_name-index` (job_name + started_date)
   - Add GSI: `status-index` (status + started_date)
3. Create table: `etl_dq_audit`
   - Partition key: `dq_id` (String)
   - Add GSI: `run_id-index`
4. Create table: `etl_recommendations_audit`
   - Partition key: `recommendation_id` (String)
   - Add GSI: `job_name-index`

### Automated Script

```bash
python scripts/provisioning/provision_dynamodb.py --region us-east-1
```

**Expected Output:**
```
============================================================
PROVISIONING: DynamoDB Tables
============================================================
Region: us-east-1

  Creating table: etl_run_audit
  [DONE] Table etl_run_audit created
  Creating table: etl_dq_audit
  [DONE] Table etl_dq_audit created
  Creating table: etl_recommendations_audit
  [DONE] Table etl_recommendations_audit created

============================================================
SUMMARY
============================================================
  etl_run_audit: SUCCESS
  etl_dq_audit: SUCCESS
  etl_recommendations_audit: SUCCESS

  [SUCCESS] All DynamoDB tables provisioned!
```

### Verify DynamoDB Tables

```bash
aws dynamodb list-tables --region us-east-1
```

**Expected Output:**
```json
{
    "TableNames": [
        "etl_dq_audit",
        "etl_recommendations_audit",
        "etl_run_audit"
    ]
}
```

---

## 2.2 API Gateway

### Manual Steps (AWS Console)

1. **Create IAM Role for Lambda:**
   - Go to IAM → Roles → Create Role
   - Select Lambda as trusted entity
   - Attach policies:
     - AWSLambdaBasicExecutionRole
     - AmazonDynamoDBReadOnlyAccess
   - Name: `etl-framework-api-lambda-role`

2. **Create Lambda Function:**
   - Go to Lambda → Create Function
   - Name: `etl-framework-api-handler`
   - Runtime: Python 3.9
   - Role: Select the role created above
   - Add code (basic health check handler)

3. **Create API Gateway:**
   - Go to API Gateway → Create API → REST API
   - Name: `etl-framework-api`
   - Create resource: `/{proxy+}`
   - Create method: ANY → Lambda Proxy Integration
   - Deploy to stage: `prod`

### Automated Script

```bash
python scripts/provisioning/provision_api_gateway.py --region us-east-1
```

**Expected Output:**
```
============================================================
PROVISIONING: API Gateway
============================================================
Region: us-east-1
Account ID: 123456789012

  Creating IAM role: etl-framework-api-lambda-role
  [DONE] Role created
  Creating Lambda function: etl-framework-api-handler
  [DONE] Lambda created
  Creating API Gateway: etl-framework-api
    API ID: abc123xyz
  [DONE] API Gateway created
    URL: https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod

============================================================
SUCCESS
============================================================
  API URL: https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod
```

### Verify API Gateway

```bash
# Test health endpoint (replace with your API URL)
curl https://YOUR_API_ID.execute-api.us-east-1.amazonaws.com/prod/health
```

**Expected Output:**
```json
{"status": "healthy", "timestamp": "2024-01-15T10:30:00", "version": "1.0.0"}
```

---

## 2.3 Slack Webhook

### Manual Steps

1. **Create Slack App:**
   - Go to https://api.slack.com/apps
   - Click "Create New App" → "From scratch"
   - App Name: `ETL Framework Alerts`
   - Select your workspace
   - Click "Create App"

2. **Enable Incoming Webhooks:**
   - In app settings → "Incoming Webhooks"
   - Toggle ON "Activate Incoming Webhooks"
   - Click "Add New Webhook to Workspace"
   - Select channel: `#etl-alerts`
   - Click "Allow"
   - Copy the Webhook URL

3. **Store Webhook URL:**
   ```bash
   # Option A: Environment variable
   export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"

   # Option B: Store in Secrets Manager
   aws secretsmanager create-secret \
     --name etl-framework/slack-webhook \
     --secret-string '{"webhook_url":"YOUR_WEBHOOK_URL"}' \
     --region us-east-1
   ```

### Test Slack Webhook

```bash
# View setup instructions
python scripts/provisioning/setup_slack.py --instructions

# Test webhook (without sending)
python scripts/provisioning/setup_slack.py --webhook-url "$SLACK_WEBHOOK_URL"

# Test webhook (send test message)
python scripts/provisioning/setup_slack.py --webhook-url "$SLACK_WEBHOOK_URL" --send-test
```

**Expected Output (with --send-test):**
```
============================================================
SETUP: Slack Integration
============================================================

  Validating webhook URL format...
    [PASS] Webhook URL format valid

  Testing webhook connectivity...
    [PASS] Test message sent successfully!

  JSON Configuration Snippet:
  --------------------------------------------------
{
  "notifications": {
    "enabled": "Y",
    "slack": {
      "enabled": "Y",
      "webhook_url": "https://hooks.slack.com/services/...",
      "channel": "#etl-alerts"
    }
  }
}

============================================================
SUMMARY
============================================================
  [SUCCESS] Slack integration setup complete!
```

### Verify in Slack

Check your `#etl-alerts` channel for the test message.

---

## 2.4 Teams Webhook

### Manual Steps

1. **Open Microsoft Teams**
2. **Navigate to your channel** (e.g., "ETL Alerts")
3. **Add Incoming Webhook Connector:**
   - Click "..." next to channel name → "Connectors"
   - Search for "Incoming Webhook"
   - Click "Configure"
   - Name: `ETL Framework`
   - Click "Create"
   - Copy the webhook URL
   - Click "Done"

4. **Store Webhook URL:**
   ```bash
   # Option A: Environment variable
   export TEAMS_WEBHOOK_URL="https://outlook.office.com/webhook/..."

   # Option B: Store in Secrets Manager
   aws secretsmanager create-secret \
     --name etl-framework/teams-webhook \
     --secret-string '{"webhook_url":"YOUR_WEBHOOK_URL"}' \
     --region us-east-1
   ```

### Test Teams Webhook

```bash
# View setup instructions
python scripts/provisioning/setup_teams.py --instructions

# Test webhook (without sending)
python scripts/provisioning/setup_teams.py --webhook-url "$TEAMS_WEBHOOK_URL"

# Test webhook (send test message)
python scripts/provisioning/setup_teams.py --webhook-url "$TEAMS_WEBHOOK_URL" --send-test
```

**Expected Output (with --send-test):**
```
============================================================
SETUP: Microsoft Teams Integration
============================================================

  Validating webhook URL format...
    [PASS] Webhook URL format valid

  Testing webhook connectivity...
    [PASS] Test message sent successfully!

============================================================
SUMMARY
============================================================
  [SUCCESS] Teams integration setup complete!
```

### Verify in Teams

Check your Teams channel for the test message with Adaptive Card.

---

## 2.5 Email (SES)

### Manual Steps (AWS Console)

1. **Verify Email Identity:**
   - Go to AWS SES → Verified Identities
   - Click "Create Identity"
   - Choose "Email address"
   - Enter sender email: `etl-alerts@yourdomain.com`
   - Click "Create Identity"
   - Check email inbox and click verification link

2. **Verify Recipient Emails (if in sandbox):**
   - Repeat for each recipient email

3. **Request Production Access (optional):**
   - Go to SES → Account Dashboard
   - Click "Request Production Access"

### Test Email Configuration

```bash
# Test SES sending
aws ses send-email \
  --from "etl-alerts@yourdomain.com" \
  --to "recipient@yourdomain.com" \
  --subject "ETL Framework Test" \
  --text "This is a test email from ETL Framework" \
  --region us-east-1
```

---

# PHASE 3: COMPONENT TESTING

> **IMPORTANT:** Run each test individually. All tests must pass before proceeding to E2E testing.

## 3.1 Test Config Parsing

Tests that all JSON configuration files can be parsed correctly.

```bash
cd /home/user/strands_etl
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

python tests/component_tests/test_01_config_parsing.py
```

**Expected Output:**
```
============================================================
COMPONENT TEST 1: Configuration Parsing
============================================================

[SIMPLE CONFIGURATIONS]

  Testing: simple_s3_to_s3.json
  --------------------------------------------------
    Job Name: simple_s3_copy
    Source Type: s3
    Target Type: s3
    [PASS] Configuration valid

  Testing: simple_glue_catalog.json
  --------------------------------------------------
    Job Name: simple_glue_etl
    Source Type: glue_catalog
    Target Type: s3
    [PASS] Configuration valid

  Testing: simple_email_alerts.json
  --------------------------------------------------
    Job Name: daily_report_etl
    Source Type: jdbc
    Target Type: s3
    [PASS] Configuration valid

  Testing: simple_teams_only.json
  --------------------------------------------------
    Job Name: inventory_sync
    Source Type: jdbc
    Target Type: glue_catalog
    [PASS] Configuration valid

  Testing: simple_all_notifications.json
  --------------------------------------------------
    Job Name: critical_financial_etl
    Source Type: glue_catalog
    Target Type: redshift
    [PASS] Configuration valid

[COMPLEX CONFIGURATIONS]

  Testing: complex_full_pipeline.json
  --------------------------------------------------
    Job Name: enterprise_sales_pipeline
    Source Type: multi_source
    Target Type: iceberg
    [PASS] Configuration valid

  Testing: complex_streaming_pipeline.json
  --------------------------------------------------
    Job Name: realtime_clickstream_pipeline
    Source Type: kinesis
    Target Type: delta
    [PASS] Configuration valid

  Testing: complex_eks_karpenter.json
  --------------------------------------------------
    Job Name: ml_feature_engineering
    Source Type: multi_source
    Target Type: delta
    [PASS] Configuration valid

============================================================
SUMMARY
============================================================
  Total:  8
  Passed: 8
  Failed: 0

  [SUCCESS] All configuration parsing tests passed!
```

**✅ CHECKPOINT: All 8 configs must pass before continuing.**

---

## 3.2 Test Notification Flags

Tests Y/N flag parsing (Y, N, YES, NO, true, false, 1, 0).

```bash
python tests/component_tests/test_02_notification_flags.py
```

**Expected Output:**
```
============================================================
COMPONENT TEST 2: Notification Y/N Flag Parsing
============================================================

[Y/N FLAG VARIATIONS]

  Testing: All Y flags (uppercase)
  --------------------------------------------------
    notifications_enabled: True (expected: True) [PASS]
    slack_enabled: True (expected: True) [PASS]
    teams_enabled: True (expected: True) [PASS]
    email_enabled: True (expected: True) [PASS]
    [PASS] All flags parsed correctly

  Testing: All N flags (uppercase)
  --------------------------------------------------
    notifications_enabled: False (expected: False) [PASS]
    slack_enabled: False (expected: False) [PASS]
    teams_enabled: False (expected: False) [PASS]
    email_enabled: False (expected: False) [PASS]
    [PASS] All flags parsed correctly

  Testing: Mixed Y/N flags (lowercase)
  --------------------------------------------------
    notifications_enabled: True (expected: True) [PASS]
    slack_enabled: True (expected: True) [PASS]
    teams_enabled: False (expected: False) [PASS]
    email_enabled: True (expected: True) [PASS]
    [PASS] All flags parsed correctly

  Testing: YES/NO format
  --------------------------------------------------
    [PASS] All flags parsed correctly

  Testing: Boolean format
  --------------------------------------------------
    [PASS] All flags parsed correctly

  Testing: Numeric 1/0 format
  --------------------------------------------------
    [PASS] All flags parsed correctly

  Testing: Empty/Missing notifications
  --------------------------------------------------
    [PASS] All flags parsed correctly

[TESTING WITH CONFIG FILES]

  Testing: simple_s3_to_s3.json
  --------------------------------------------------
    Slack: False (expected: False)
    Teams: False (expected: False)
    Email: False (expected: False)
    [PASS]

  Testing: simple_glue_catalog.json
  --------------------------------------------------
    Slack: True (expected: True)
    Teams: False (expected: False)
    Email: False (expected: False)
    [PASS]

  ... (more config files)

============================================================
SUMMARY
============================================================
  Total:  13
  Passed: 13
  Failed: 0

  [SUCCESS] All notification flag tests passed!
```

**✅ CHECKPOINT: All flag parsing tests must pass before continuing.**

---

## 3.3 Test Notification Manager

Tests unified notification manager creation and status.

```bash
python tests/component_tests/test_03_notification_manager.py
```

**Expected Output:**
```
============================================================
COMPONENT TEST 3: Notification Manager
============================================================

[UNIT TESTS]

  Testing: Manager Status Structure
  --------------------------------------------------
    [PASS] Status structure is correct

  Testing: Should Notify Logic
  --------------------------------------------------
    on_start: False (expected: False) [PASS]
    on_success: True (expected: True) [PASS]
    on_failure: True (expected: True) [PASS]
    on_warning: False (expected: False) [PASS]
    [PASS] Should notify logic correct

[SIMPLE CONFIGURATIONS]

  Testing: simple_s3_to_s3.json
  --------------------------------------------------
    Master Enabled: False
    Channels:
      - Slack: enabled=False, configured=False
      - Teams: enabled=False, configured=False
      - Email: enabled=False, configured=False
    [PASS] Manager created successfully

  Testing: simple_all_notifications.json
  --------------------------------------------------
    Master Enabled: True
    Channels:
      - Slack: enabled=True, configured=True
      - Teams: enabled=True, configured=True
      - Email: enabled=True, configured=True
    [PASS] Manager created successfully

... (more tests)

============================================================
SUMMARY
============================================================
  Total:  10
  Passed: 10
  Failed: 0

  [SUCCESS] All notification manager tests passed!
```

**✅ CHECKPOINT: All notification manager tests must pass before continuing.**

---

## 3.4 Test Teams Integration

Tests Teams configuration, message building, and adaptive cards.

```bash
python tests/component_tests/test_04_teams_integration.py
```

**Expected Output:**
```
============================================================
COMPONENT TEST 4: Teams Integration
============================================================

[UNIT TESTS]

  Testing: Teams Config Creation
  --------------------------------------------------
    webhook_url: https://test.webhook.office...
    channel_name: test-channel
    enabled: True
    notify_on_start: True
    notify_on_success: True
    notify_on_failure: True
    [PASS] Teams config created

  Testing: Teams Message Creation
  --------------------------------------------------
    Title: Test ETL Job Completed
    Text: Job test_job completed successfully.
    Severity: good
    Facts: 3 items
    Actions: 1 items
    [PASS] Teams message created

  Testing: Adaptive Card Building
  --------------------------------------------------
    Card type: message
    Attachment contentType: application/vnd.microsoft.card.adaptive
    Body elements: 4
    [PASS] Adaptive card built correctly

  Testing: Simple Message Card Building
  --------------------------------------------------
    Card @type: MessageCard
    Theme color: 00FF00
    Sections: 1
    [PASS] Simple message card built correctly

  Testing: Factory from JSON Config
  --------------------------------------------------
    Enabled config: Teams created = True
    Disabled config: Teams created = False
    [PASS] Factory correctly handles Y/N flags

[TESTING WITH CONFIG FILES]

  Testing: simple_teams_only.json
  --------------------------------------------------
    Teams created: True (expected: True) [PASS]
    Channel: Warehouse Ops

  Testing: complex_eks_karpenter.json
  --------------------------------------------------
    Teams created: True (expected: True) [PASS]
    Channel: ML Platform

============================================================
SUMMARY
============================================================
  Total:  9
  Passed: 9
  Failed: 0

  [SUCCESS] All Teams integration tests passed!
```

**✅ CHECKPOINT: All Teams integration tests must pass before continuing.**

---

## 3.5 Test API Gateway

Tests API Gateway handlers, routes, health check, and CORS.

```bash
python tests/component_tests/test_05_api_gateway.py
```

**Expected Output:**
```
============================================================
COMPONENT TEST 5: API Gateway Integration
============================================================

  Testing: API Config Creation
  --------------------------------------------------
    api_name: test-etl-api
    stage: test
    region: us-east-1
    auth_type: api_key
    rate_limit: 1000
    enable_cors: True
    [PASS] API config created

  Testing: API Response Structure
  --------------------------------------------------
    statusCode: 200
    Content-Type: application/json
    CORS headers present: True
    [PASS] API response structure correct

  Testing: Route Registration
  --------------------------------------------------
    Routes registered: 3
      /test: ['GET', 'POST']
      /test/{id}: ['GET']
    [PASS] Routes registered correctly

  Testing: Health Check Endpoint
  --------------------------------------------------
    Status Code: 200
    Health Status: healthy
    Version: 1.0.0
    [PASS] Health endpoint works

  Testing: CORS Preflight (OPTIONS)
  --------------------------------------------------
    Status Code: 200
    Allow-Origin: *
    Allow-Methods: GET,POST,PUT,DELETE,OPTIONS
    [PASS] CORS preflight handled correctly

  Testing: 404 Not Found
  --------------------------------------------------
    Status Code: 404
    Error: Route not found: GET /nonexistent/route
    [PASS] 404 handled correctly

  Testing: All Registered Routes
  --------------------------------------------------
    Total routes: 10
      GET /health
      GET /jobs
      GET /jobs/{job_name}
      POST /jobs/{job_name}/trigger
      ...
    [PASS] All expected routes registered

============================================================
SUMMARY
============================================================
  Total:  7
  Passed: 7
  Failed: 0

  [SUCCESS] All API Gateway tests passed!
```

**✅ CHECKPOINT: All API Gateway tests must pass before continuing.**

---

## 3.6 Test Audit System

Tests audit record creation, status transitions, and serialization.

```bash
python tests/component_tests/test_06_audit_system.py
```

**Expected Output:**
```
============================================================
COMPONENT TEST 6: Audit System
============================================================

[UNIT TESTS]

  Testing: Audit Record Creation
  --------------------------------------------------
    run_id: test-1705312200000
    job_name: test_job
    status: RUNNING
    platform: glue
    source_type: s3
    target_type: redshift
    [PASS] Audit record created

  Testing: Audit Status Transitions
  --------------------------------------------------
    Initial status: PENDING
    After start: RUNNING
    After completion: SUCCEEDED
    Valid statuses: ['PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED']
    [PASS] Status transitions work correctly

  Testing: Audit Metrics Update
  --------------------------------------------------
    rows_read: 1,000,000
    rows_written: 999,500
    bytes_read: 500,000,000
    bytes_written: 480,000,000
    dq_score: 99.5%
    estimated_cost_usd: $2.50
    recommendations: 2
    [PASS] Metrics updated correctly

  Testing: Audit to Dictionary
  --------------------------------------------------
    Dict keys: 25
    Sample keys: ['run_id', 'job_name', 'status', 'started_at', 'completed_at']...
    JSON serializable: Yes (523 chars)
    [PASS] Audit serialization works

  Testing: Data Quality Audit Record
  --------------------------------------------------
    dq_id: dq-1705312200000
    overall_score: 95.0%
    total_rules: 10
    passed_rules: 9
    failed_rules: 1
    [PASS] DQ audit record created

  Testing: Platform Recommendation Audit
  --------------------------------------------------
    current_platform: glue
    recommended_platform: emr
    estimated_savings_pct: 40.0%
    [PASS] Platform recommendation audit created

[TESTING WITH CONFIG FILES]

  Testing: simple_s3_to_s3.json
  --------------------------------------------------
    job_name: simple_s3_copy
    platform: auto
    source_type: s3
    target_type: s3
    [PASS]

  ... (more configs)

============================================================
SUMMARY
============================================================
  Total:  10
  Passed: 10
  Failed: 0

  [SUCCESS] All audit system tests passed!
```

**✅ CHECKPOINT: All audit system tests must pass before continuing.**

---

## 3.7 Run All Component Tests at Once

After testing individually, run all component tests together:

```bash
python tests/run_all_tests.py --component-only
```

**Expected Output:**
```
======================================================================
   ETL FRAMEWORK - COMPLETE TEST SUITE
======================================================================
   Started: 2024-01-15T10:30:00
======================================================================

======================================================================
   COMPONENT TESTS
======================================================================

Found 6 component tests

[PASS] test_01_config_parsing (0.15s)
[PASS] test_02_notification_flags (0.23s)
[PASS] test_03_notification_manager (0.18s)
[PASS] test_04_teams_integration (0.12s)
[PASS] test_05_api_gateway (0.25s)
[PASS] test_06_audit_system (0.14s)

======================================================================
   FINAL SUMMARY
======================================================================

Component Tests:
  [PASS] test_01_config_parsing (0.15s)
  [PASS] test_02_notification_flags (0.23s)
  [PASS] test_03_notification_manager (0.18s)
  [PASS] test_04_teams_integration (0.12s)
  [PASS] test_05_api_gateway (0.25s)
  [PASS] test_06_audit_system (0.14s)

----------------------------------------------------------------------
  Total Tests:  6
  Passed:       6 (100.0%)
  Failed:       0
----------------------------------------------------------------------

  *** ALL TESTS PASSED ***
```

**✅ CHECKPOINT: All 6 component tests must pass (100%) before proceeding to Phase 4.**

---

# PHASE 4: INTEGRATION TESTING

## 4.1 Simple JSON Configs

Test each simple configuration manually.

### Test 1: No Notifications (simple_s3_to_s3.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_s3_to_s3.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])
print('Notifications enabled:', config.get('notifications', {}).get('enabled'))

manager = create_notification_manager(config)
status = manager.get_status()

print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

assert status['channels']['slack']['enabled'] == False
assert status['channels']['teams']['enabled'] == False
assert status['channels']['email']['enabled'] == False
print('[PASS] No notifications configured correctly')
"
```

### Test 2: Slack Only (simple_glue_catalog.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_glue_catalog.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])

manager = create_notification_manager(config)
status = manager.get_status()

print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

assert status['channels']['slack']['enabled'] == True
assert status['channels']['teams']['enabled'] == False
assert status['channels']['email']['enabled'] == False
print('[PASS] Slack only configured correctly')
"
```

### Test 3: Teams Only (simple_teams_only.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_teams_only.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])

manager = create_notification_manager(config)
status = manager.get_status()

print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

assert status['channels']['slack']['enabled'] == False
assert status['channels']['teams']['enabled'] == True
assert status['channels']['email']['enabled'] == False
print('[PASS] Teams only configured correctly')
"
```

### Test 4: Email Only (simple_email_alerts.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_email_alerts.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])

manager = create_notification_manager(config)
status = manager.get_status()

print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

assert status['channels']['slack']['enabled'] == False
assert status['channels']['teams']['enabled'] == False
assert status['channels']['email']['enabled'] == True
print('[PASS] Email only configured correctly')
"
```

### Test 5: All Notifications (simple_all_notifications.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_all_notifications.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])

manager = create_notification_manager(config)
status = manager.get_status()

print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

assert status['channels']['slack']['enabled'] == True
assert status['channels']['teams']['enabled'] == True
assert status['channels']['email']['enabled'] == True
print('[PASS] All notifications configured correctly')
"
```

**✅ CHECKPOINT: All 5 simple config tests must pass before continuing.**

---

## 4.2 Complex JSON Configs

### Test 1: Full Pipeline (complex_full_pipeline.json)

```bash
python -c "
import json
import sys
import time
from datetime import datetime
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager
from audit.etl_audit import ETLRunAudit, RunStatus

with open('test_configs/complex_full_pipeline.json') as f:
    config = json.load(f)

print('='*60)
print('Job:', config['job_name'])
print('='*60)

# Source info
print('Source type:', config['source']['type'])
print('Number of sources:', len(config['source'].get('sources', [])))

# Target info
print('Target type:', config['target']['type'])

# Platform info
print('Primary platform:', config['platform']['primary'])
print('Fallback platforms:', config['platform']['fallback'])

# DQ info
dq_rules = config.get('data_quality', {}).get('rules', [])
print('DQ Rules:', len(dq_rules))

# Transformations
print('Transformations:', len(config.get('transformations', [])))

# Notifications
manager = create_notification_manager(config)
status = manager.get_status()
print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

# Create audit record
audit = ETLRunAudit(
    run_id=f\"{config['job_name']}-{int(time.time()*1000)}\",
    job_name=config['job_name'],
    status=RunStatus.RUNNING.value,
    started_at=datetime.now().isoformat(),
    platform=config['platform']['primary'],
    config_hash='test',
    source_type=config['source']['type'],
    target_type=config['target']['type']
)

# Simulate completion
audit.status = RunStatus.SUCCEEDED.value
audit.completed_at = datetime.now().isoformat()
audit.rows_read = 1000000
audit.rows_written = 998500
audit.dq_score = 0.95

print('Audit run_id:', audit.run_id)
print('Audit status:', audit.status)
print('[PASS] Complex full pipeline test passed')
"
```

### Test 2: Streaming Pipeline (complex_streaming_pipeline.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/complex_streaming_pipeline.json') as f:
    config = json.load(f)

print('='*60)
print('Job:', config['job_name'])
print('='*60)

print('Source type:', config['source']['type'])
print('Stream name:', config['source'].get('stream_name'))
print('Target type:', config['target']['type'])
print('Target path:', config['target'].get('path'))

manager = create_notification_manager(config)
status = manager.get_status()
print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

print('[PASS] Complex streaming pipeline test passed')
"
```

### Test 3: EKS Karpenter (complex_eks_karpenter.json)

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

with open('test_configs/complex_eks_karpenter.json') as f:
    config = json.load(f)

print('='*60)
print('Job:', config['job_name'])
print('='*60)

print('Primary platform:', config['platform']['primary'])
print('EKS cluster:', config['platform']['eks_config']['cluster_name'])
print('Use Karpenter:', config['platform']['eks_config']['use_karpenter'])
print('Use SPOT:', config['platform']['eks_config']['use_spot'])
print('Use Graviton:', config['platform']['eks_config']['use_graviton'])

manager = create_notification_manager(config)
status = manager.get_status()
print('Slack:', status['channels']['slack']['enabled'])
print('Teams:', status['channels']['teams']['enabled'])
print('Email:', status['channels']['email']['enabled'])

print('[PASS] Complex EKS Karpenter test passed')
"
```

**✅ CHECKPOINT: All 3 complex config tests must pass before continuing.**

---

# PHASE 5: END-TO-END TESTING

> **PREREQUISITE:** All Phase 3 and Phase 4 tests must pass before running E2E tests.

## 5.1 Run Full E2E Test Suite

```bash
python tests/run_all_tests.py
```

**Expected Output:**
```
======================================================================
   ETL FRAMEWORK - COMPLETE TEST SUITE
======================================================================
   Started: 2024-01-15T10:30:00
======================================================================

======================================================================
   COMPONENT TESTS
======================================================================

Found 6 component tests

============================================================
RUNNING: test_01_config_parsing
============================================================
... [PASS]

============================================================
RUNNING: test_02_notification_flags
============================================================
... [PASS]

============================================================
RUNNING: test_03_notification_manager
============================================================
... [PASS]

============================================================
RUNNING: test_04_teams_integration
============================================================
... [PASS]

============================================================
RUNNING: test_05_api_gateway
============================================================
... [PASS]

============================================================
RUNNING: test_06_audit_system
============================================================
... [PASS]

======================================================================
   END-TO-END TESTS
======================================================================

Found 8 config files

[SIMPLE CONFIGURATIONS]

  Testing E2E: simple_s3_to_s3.json
  --------------------------------------------------
    Job: simple_s3_copy
    Notifications: slack=False, teams=False, email=False
    Audit: SUCCEEDED
    [PASS]

  Testing E2E: simple_glue_catalog.json
  --------------------------------------------------
    Job: simple_glue_etl
    Notifications: slack=True, teams=False, email=False
    Audit: SUCCEEDED
    [PASS]

  Testing E2E: simple_email_alerts.json
  --------------------------------------------------
    Job: daily_report_etl
    Notifications: slack=False, teams=False, email=True
    Audit: SUCCEEDED
    [PASS]

  Testing E2E: simple_teams_only.json
  --------------------------------------------------
    Job: inventory_sync
    Notifications: slack=False, teams=True, email=False
    Audit: SUCCEEDED
    [PASS]

  Testing E2E: simple_all_notifications.json
  --------------------------------------------------
    Job: critical_financial_etl
    Notifications: slack=True, teams=True, email=True
    Audit: SUCCEEDED
    [PASS]

[COMPLEX CONFIGURATIONS]

  Testing E2E: complex_full_pipeline.json
  --------------------------------------------------
    Job: enterprise_sales_pipeline
    Notifications: slack=True, teams=True, email=True
    Audit: SUCCEEDED
    [PASS]

  Testing E2E: complex_streaming_pipeline.json
  --------------------------------------------------
    Job: realtime_clickstream_pipeline
    Notifications: slack=True, teams=False, email=True
    Audit: SUCCEEDED
    [PASS]

  Testing E2E: complex_eks_karpenter.json
  --------------------------------------------------
    Job: ml_feature_engineering
    Notifications: slack=False, teams=True, email=True
    Audit: SUCCEEDED
    [PASS]

======================================================================
   FINAL SUMMARY
======================================================================

Component Tests:
  [PASS] test_01_config_parsing (0.15s)
  [PASS] test_02_notification_flags (0.23s)
  [PASS] test_03_notification_manager (0.18s)
  [PASS] test_04_teams_integration (0.12s)
  [PASS] test_05_api_gateway (0.25s)
  [PASS] test_06_audit_system (0.14s)

E2E Tests:
  [PASS] simple_s3_to_s3.json (0.05s)
  [PASS] simple_glue_catalog.json (0.04s)
  [PASS] simple_email_alerts.json (0.04s)
  [PASS] simple_teams_only.json (0.04s)
  [PASS] simple_all_notifications.json (0.05s)
  [PASS] complex_full_pipeline.json (0.06s)
  [PASS] complex_streaming_pipeline.json (0.05s)
  [PASS] complex_eks_karpenter.json (0.05s)

----------------------------------------------------------------------
  Total Tests:  14
  Passed:       14 (100.0%)
  Failed:       0
----------------------------------------------------------------------

  *** ALL TESTS PASSED ***
```

## 5.2 Generate Test Report

```bash
python tests/run_all_tests.py --report test_report.json

# View report
cat test_report.json
```

---

# PHASE 6: VERIFICATION CHECKLIST

## Final Verification

| Phase | Step | Status |
|-------|------|--------|
| **PHASE 1** | Prerequisites | ☐ |
| | Python path set | ☐ |
| | Dependencies installed | ☐ |
| | AWS credentials configured | ☐ |
| **PHASE 2** | Provisioning | ☐ |
| | DynamoDB tables created | ☐ |
| | API Gateway deployed | ☐ |
| | Slack webhook configured | ☐ |
| | Teams webhook configured | ☐ |
| | Email (SES) configured | ☐ |
| **PHASE 3** | Component Tests | ☐ |
| | test_01_config_parsing: PASS | ☐ |
| | test_02_notification_flags: PASS | ☐ |
| | test_03_notification_manager: PASS | ☐ |
| | test_04_teams_integration: PASS | ☐ |
| | test_05_api_gateway: PASS | ☐ |
| | test_06_audit_system: PASS | ☐ |
| **PHASE 4** | Integration Tests | ☐ |
| | Simple configs (5/5): PASS | ☐ |
| | Complex configs (3/3): PASS | ☐ |
| **PHASE 5** | E2E Tests | ☐ |
| | Full test suite: 14/14 PASS | ☐ |
| | Test report generated | ☐ |

## Quick Verification Commands

```bash
# 1. Verify DynamoDB tables
aws dynamodb list-tables --region us-east-1 | grep etl_

# 2. Test API Gateway health
curl https://YOUR_API.execute-api.us-east-1.amazonaws.com/prod/health

# 3. Run all tests
python tests/run_all_tests.py

# 4. Check test results
echo "Exit code: $?"  # Should be 0
```

---

# QUICK REFERENCE

## Commands Summary

```bash
# Setup
cd /home/user/strands_etl
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Provisioning
python scripts/provisioning/provision_dynamodb.py --region us-east-1
python scripts/provisioning/provision_api_gateway.py --region us-east-1
python scripts/provisioning/setup_slack.py --webhook-url "URL" --send-test
python scripts/provisioning/setup_teams.py --webhook-url "URL" --send-test

# Component Tests (run individually)
python tests/component_tests/test_01_config_parsing.py
python tests/component_tests/test_02_notification_flags.py
python tests/component_tests/test_03_notification_manager.py
python tests/component_tests/test_04_teams_integration.py
python tests/component_tests/test_05_api_gateway.py
python tests/component_tests/test_06_audit_system.py

# All Component Tests
python tests/run_all_tests.py --component-only

# E2E Tests Only
python tests/run_all_tests.py --e2e-only

# Full Test Suite
python tests/run_all_tests.py

# With Report
python tests/run_all_tests.py --report test_report.json
```

## JSON Notification Flag Reference

| Value | Meaning |
|-------|---------|
| `"Y"`, `"YES"`, `"TRUE"`, `"1"`, `true` | Enabled |
| `"N"`, `"NO"`, `"FALSE"`, `"0"`, `false` | Disabled |

## Test Config Files

| File | Slack | Teams | Email |
|------|-------|-------|-------|
| `simple_s3_to_s3.json` | N | N | N |
| `simple_glue_catalog.json` | Y | N | N |
| `simple_teams_only.json` | N | Y | N |
| `simple_email_alerts.json` | N | N | Y |
| `simple_all_notifications.json` | Y | Y | Y |
| `complex_full_pipeline.json` | Y | Y | Y |
| `complex_streaming_pipeline.json` | Y | N | Y |
| `complex_eks_karpenter.json` | N | Y | Y |
