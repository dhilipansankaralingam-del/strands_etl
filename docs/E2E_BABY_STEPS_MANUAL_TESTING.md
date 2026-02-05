# End-to-End Baby Steps - Manual Testing Guide

This guide provides step-by-step instructions for manually testing the ETL Framework end-to-end with simple and complex JSON configurations.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Test Configuration Files](#test-configuration-files)
3. [Step 1: Test Configuration Parsing](#step-1-test-configuration-parsing)
4. [Step 2: Test Notification Manager (Y/N Flags)](#step-2-test-notification-manager-yn-flags)
5. [Step 3: Test Teams Integration](#step-3-test-teams-integration)
6. [Step 4: Test Slack Integration](#step-4-test-slack-integration)
7. [Step 5: Test Email Integration](#step-5-test-email-integration)
8. [Step 6: Test API Gateway](#step-6-test-api-gateway)
9. [Step 7: Test Audit System](#step-7-test-audit-system)
10. [Step 8: Full E2E Flow](#step-8-full-e2e-flow)
11. [Step 9: Run Automated Tests](#step-9-run-automated-tests)
12. [Troubleshooting](#troubleshooting)

---

## Prerequisites

```bash
# 1. Navigate to project directory
cd /path/to/strands_etl

# 2. Install dependencies
pip install boto3 requests

# 3. Set AWS credentials (for DynamoDB, SES, etc.)
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# 4. Verify Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

---

## Test Configuration Files

The framework includes these test configurations in `test_configs/`:

### Simple Configurations (Basic Testing)

| File | Description | Notifications |
|------|-------------|---------------|
| `simple_s3_to_s3.json` | S3 to S3 copy | None |
| `simple_glue_catalog.json` | Glue Catalog to S3 | Slack only |
| `simple_email_alerts.json` | JDBC to S3 | Email only |
| `simple_teams_only.json` | Inventory sync | Teams only |
| `simple_all_notifications.json` | Critical ETL | All channels |

### Complex Configurations (Advanced Testing)

| File | Description | Features |
|------|-------------|----------|
| `complex_full_pipeline.json` | Multi-source sales | Full DQ, all notifications, compliance |
| `complex_streaming_pipeline.json` | Kinesis streaming | Real-time, Delta Lake |
| `complex_eks_karpenter.json` | ML features on EKS | Karpenter, Graviton, SPOT |

---

## Step 1: Test Configuration Parsing

### 1.1 Test Simple Config (No Notifications)

```python
#!/usr/bin/env python3
"""Step 1.1: Test simple config parsing"""
import json
import sys
sys.path.insert(0, '.')

# Load config
with open('test_configs/simple_s3_to_s3.json', 'r') as f:
    config = json.load(f)

print("=" * 60)
print("Step 1.1: Simple Config Parsing Test")
print("=" * 60)
print(f"Job Name: {config.get('job_name')}")
print(f"Source Type: {config.get('source', {}).get('type')}")
print(f"Target Type: {config.get('target', {}).get('type')}")
print(f"Platform: {config.get('platform')}")
print(f"Notifications Enabled: {config.get('notifications', {}).get('enabled')}")
print("\n[PASS] Config parsed successfully")
```

### 1.2 Test Complex Config

```python
#!/usr/bin/env python3
"""Step 1.2: Test complex config parsing"""
import json
import sys
sys.path.insert(0, '.')

# Load config
with open('test_configs/complex_full_pipeline.json', 'r') as f:
    config = json.load(f)

print("=" * 60)
print("Step 1.2: Complex Config Parsing Test")
print("=" * 60)
print(f"Job Name: {config.get('job_name')}")
print(f"Source Type: {config.get('source', {}).get('type')}")
print(f"Number of Sources: {len(config.get('source', {}).get('sources', []))}")
print(f"Target Type: {config.get('target', {}).get('type')}")
print(f"Platform Primary: {config.get('platform', {}).get('primary')}")
print(f"Platform Fallbacks: {config.get('platform', {}).get('fallback')}")
print(f"DQ Rules Count: {len(config.get('data_quality', {}).get('rules', []))}")
print(f"Transformations Count: {len(config.get('transformations', []))}")

# Notifications
notif = config.get('notifications', {})
print(f"\nNotifications:")
print(f"  - Slack Enabled: {notif.get('slack', {}).get('enabled')}")
print(f"  - Teams Enabled: {notif.get('teams', {}).get('enabled')}")
print(f"  - Email Enabled: {notif.get('email', {}).get('enabled')}")

print("\n[PASS] Complex config parsed successfully")
```

---

## Step 2: Test Notification Manager (Y/N Flags)

### 2.1 Test Notification Config Parsing

```python
#!/usr/bin/env python3
"""Step 2.1: Test notification config with Y/N flags"""
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import NotificationConfig

# Test with all notifications config
with open('test_configs/simple_all_notifications.json', 'r') as f:
    config = json.load(f)

print("=" * 60)
print("Step 2.1: Notification Config Y/N Flag Test")
print("=" * 60)

notif_config = NotificationConfig.from_json(config)

print(f"Master Notifications Enabled: {notif_config.notifications_enabled}")
print(f"\nChannel Status:")
print(f"  - Slack Enabled: {notif_config.slack_enabled}")
print(f"  - Teams Enabled: {notif_config.teams_enabled}")
print(f"  - Email Enabled: {notif_config.email_enabled}")

print(f"\nPreferences:")
print(f"  - On Start: {notif_config.notify_on_start}")
print(f"  - On Success: {notif_config.notify_on_success}")
print(f"  - On Failure: {notif_config.notify_on_failure}")
print(f"  - On DQ Failure: {notif_config.notify_on_dq_failure}")
print(f"  - On Cost Alert: {notif_config.notify_on_cost_alert}")

print(f"\nThresholds:")
print(f"  - DQ Score: {notif_config.dq_score_threshold}")
print(f"  - Cost Alert: ${notif_config.cost_alert_threshold_usd}")

print("\n[PASS] Y/N flags parsed correctly")
```

### 2.2 Test Notification Manager Creation

```python
#!/usr/bin/env python3
"""Step 2.2: Test notification manager creation"""
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

# Test with different configs
configs_to_test = [
    ('simple_s3_to_s3.json', 'No notifications'),
    ('simple_glue_catalog.json', 'Slack only'),
    ('simple_teams_only.json', 'Teams only'),
    ('simple_email_alerts.json', 'Email only'),
    ('simple_all_notifications.json', 'All channels'),
]

print("=" * 60)
print("Step 2.2: Notification Manager Creation Test")
print("=" * 60)

for config_file, description in configs_to_test:
    with open(f'test_configs/{config_file}', 'r') as f:
        config = json.load(f)

    manager = create_notification_manager(config)
    status = manager.get_status()

    print(f"\n{config_file} ({description}):")
    print(f"  Slack: {'ON' if status['channels']['slack']['enabled'] else 'OFF'}")
    print(f"  Teams: {'ON' if status['channels']['teams']['enabled'] else 'OFF'}")
    print(f"  Email: {'ON' if status['channels']['email']['enabled'] else 'OFF'}")

print("\n[PASS] All notification managers created successfully")
```

---

## Step 3: Test Teams Integration

### 3.1 Test Teams Config Parsing

```python
#!/usr/bin/env python3
"""Step 3.1: Test Teams integration config"""
import json
import sys
sys.path.insert(0, '.')

from integrations.teams_integration import TeamsIntegrationFactory

# Load config with Teams enabled
with open('test_configs/complex_eks_karpenter.json', 'r') as f:
    config = json.load(f)

print("=" * 60)
print("Step 3.1: Teams Integration Config Test")
print("=" * 60)

teams = TeamsIntegrationFactory.from_json_flags(config.get('notifications', {}))

if teams:
    print(f"Teams Integration Created: YES")
    print(f"Channel: {teams.config.channel_name}")
    print(f"Webhook Configured: {bool(teams.config.webhook_url)}")
    print(f"Notify on Start: {teams.config.notify_on_start}")
    print(f"Notify on Success: {teams.config.notify_on_success}")
    print(f"Notify on Failure: {teams.config.notify_on_failure}")
    print("\n[PASS] Teams integration configured")
else:
    print("Teams Integration Created: NO (disabled in config)")
```

### 3.2 Test Teams Message Building (Mock)

```python
#!/usr/bin/env python3
"""Step 3.2: Test Teams message building"""
import json
import sys
sys.path.insert(0, '.')

from integrations.teams_integration import (
    TeamsIntegration, TeamsConfig, TeamsMessage, AlertSeverity
)

# Create Teams integration with mock webhook
teams_config = TeamsConfig(
    webhook_url="https://mock-webhook.example.com",
    channel_name="test-channel",
    enabled=True
)

teams = TeamsIntegration(teams_config)

print("=" * 60)
print("Step 3.2: Teams Message Building Test")
print("=" * 60)

# Build test message
message = TeamsMessage(
    title="Test ETL Job Completed",
    text="Job `test_job` completed successfully.",
    severity=AlertSeverity.SUCCESS,
    facts={
        "Run ID": "test-12345",
        "Duration": "120 seconds",
        "Rows Processed": "10,000"
    }
)

# Build adaptive card
card = teams._build_adaptive_card(message)

print("Adaptive Card Structure:")
print(json.dumps(card, indent=2)[:500] + "...")
print("\n[PASS] Teams message built successfully")
```

---

## Step 4: Test Slack Integration

### 4.1 Test Slack Config Parsing

```python
#!/usr/bin/env python3
"""Step 4.1: Test Slack integration config"""
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import NotificationConfig

# Load config with Slack enabled
with open('test_configs/simple_glue_catalog.json', 'r') as f:
    config = json.load(f)

print("=" * 60)
print("Step 4.1: Slack Integration Config Test")
print("=" * 60)

notif_config = NotificationConfig.from_json(config)

print(f"Slack Enabled: {notif_config.slack_enabled}")
print(f"Slack Channel: {notif_config.slack_channel}")
print(f"Webhook Configured: {bool(notif_config.slack_webhook_url)}")

print("\n[PASS] Slack config parsed")
```

---

## Step 5: Test Email Integration

### 5.1 Test Email Config Parsing

```python
#!/usr/bin/env python3
"""Step 5.1: Test Email integration config"""
import json
import sys
sys.path.insert(0, '.')

from integrations.notification_manager import NotificationConfig

# Load config with Email enabled
with open('test_configs/simple_email_alerts.json', 'r') as f:
    config = json.load(f)

print("=" * 60)
print("Step 5.1: Email Integration Config Test")
print("=" * 60)

notif_config = NotificationConfig.from_json(config)

print(f"Email Enabled: {notif_config.email_enabled}")
print(f"Sender: {notif_config.email_sender}")
print(f"Recipients: {notif_config.email_recipients}")
print(f"SES Region: {notif_config.email_ses_region}")

print("\n[PASS] Email config parsed")
```

---

## Step 6: Test API Gateway

### 6.1 Test API Handler Creation

```python
#!/usr/bin/env python3
"""Step 6.1: Test API Gateway handler"""
import json
import sys
sys.path.insert(0, '.')

from integrations.api_gateway import APIConfig, create_api_handler

print("=" * 60)
print("Step 6.1: API Gateway Handler Test")
print("=" * 60)

# Create API handler
api_config = APIConfig(
    api_name="test-etl-api",
    stage="test"
)

api = create_api_handler(api_config)

print(f"API Name: {api_config.api_name}")
print(f"Stage: {api_config.stage}")
print(f"Auth Type: {api_config.auth_type.value}")
print(f"Routes Registered: {len(api._routes)}")

# List routes
print("\nRegistered Routes:")
for path, methods in api._routes.items():
    for method in methods:
        print(f"  {method} {path}")

print("\n[PASS] API handler created")
```

### 6.2 Test API Health Check

```python
#!/usr/bin/env python3
"""Step 6.2: Test API health check endpoint"""
import json
import sys
sys.path.insert(0, '.')

from integrations.api_gateway import APIConfig, create_api_handler

print("=" * 60)
print("Step 6.2: API Health Check Test")
print("=" * 60)

api = create_api_handler(APIConfig())

# Mock Lambda event
event = {
    'httpMethod': 'GET',
    'path': '/health',
    'headers': {},
    'queryStringParameters': {},
    'pathParameters': {},
    'body': None
}

response = api.handle_request(event, None)

print(f"Status Code: {response['statusCode']}")
print(f"Body: {response['body']}")

if response['statusCode'] == 200:
    print("\n[PASS] Health check passed")
else:
    print("\n[FAIL] Health check failed")
```

---

## Step 7: Test Audit System

### 7.1 Test Audit Record Creation

```python
#!/usr/bin/env python3
"""Step 7.1: Test audit record creation"""
import sys
import time
from datetime import datetime
sys.path.insert(0, '.')

from audit.etl_audit import ETLRunAudit, RunStatus

print("=" * 60)
print("Step 7.1: Audit Record Creation Test")
print("=" * 60)

# Create audit record
run_id = f"test-{int(time.time() * 1000)}"
audit = ETLRunAudit(
    run_id=run_id,
    job_name="test_etl_job",
    status=RunStatus.RUNNING.value,
    started_at=datetime.now().isoformat(),
    platform="glue",
    config_hash="test_hash_123",
    source_type="s3",
    target_type="redshift"
)

print(f"Run ID: {audit.run_id}")
print(f"Job Name: {audit.job_name}")
print(f"Status: {audit.status}")
print(f"Started At: {audit.started_at}")
print(f"Platform: {audit.platform}")

# Simulate completion
time.sleep(0.1)
audit.status = RunStatus.SUCCEEDED.value
audit.completed_at = datetime.now().isoformat()
audit.rows_read = 50000
audit.rows_written = 49800
audit.dq_score = 0.996
audit.estimated_cost_usd = 0.15

print(f"\nAfter Completion:")
print(f"Status: {audit.status}")
print(f"Completed At: {audit.completed_at}")
print(f"Rows Read: {audit.rows_read:,}")
print(f"Rows Written: {audit.rows_written:,}")
print(f"DQ Score: {audit.dq_score:.1%}")
print(f"Estimated Cost: ${audit.estimated_cost_usd:.4f}")

print("\n[PASS] Audit record created and updated")
```

### 7.2 Test Audit to Dictionary

```python
#!/usr/bin/env python3
"""Step 7.2: Test audit record serialization"""
import sys
import json
import time
from datetime import datetime
sys.path.insert(0, '.')

from audit.etl_audit import ETLRunAudit, RunStatus

print("=" * 60)
print("Step 7.2: Audit Serialization Test")
print("=" * 60)

audit = ETLRunAudit(
    run_id=f"test-{int(time.time() * 1000)}",
    job_name="serialization_test",
    status=RunStatus.SUCCEEDED.value,
    started_at=datetime.now().isoformat(),
    completed_at=datetime.now().isoformat(),
    platform="emr",
    config_hash="abc123",
    source_type="glue_catalog",
    target_type="iceberg",
    rows_read=100000,
    rows_written=99500,
    dq_score=0.995,
    estimated_cost_usd=2.50,
    recommendations=["Consider using Graviton instances", "Enable dynamic allocation"]
)

# Convert to dict
audit_dict = audit.to_dict()
print("Audit as Dictionary (partial):")
print(json.dumps({k: v for k, v in audit_dict.items() if v}, indent=2))

print("\n[PASS] Audit serialized successfully")
```

---

## Step 8: Full E2E Flow

### 8.1 Complete E2E Test (Mock Mode)

```python
#!/usr/bin/env python3
"""Step 8.1: Full E2E flow test with mock data"""
import json
import sys
import time
from datetime import datetime
sys.path.insert(0, '.')

from audit.etl_audit import ETLRunAudit, RunStatus
from integrations.notification_manager import create_notification_manager

print("=" * 60)
print("Step 8.1: Full E2E Flow Test (Mock)")
print("=" * 60)

# Load complex config
with open('test_configs/complex_full_pipeline.json', 'r') as f:
    config = json.load(f)

job_name = config['job_name']
run_id = f"{job_name}-{int(time.time() * 1000)}"

print(f"\n1. Starting ETL Job: {job_name}")
print(f"   Run ID: {run_id}")

# Step 1: Create notification manager
print("\n2. Creating Notification Manager...")
notif_manager = create_notification_manager(config)
status = notif_manager.get_status()
print(f"   Channels: Slack={status['channels']['slack']['enabled']}, "
      f"Teams={status['channels']['teams']['enabled']}, "
      f"Email={status['channels']['email']['enabled']}")

# Step 2: Create audit record
print("\n3. Creating Audit Record...")
audit = ETLRunAudit(
    run_id=run_id,
    job_name=job_name,
    status=RunStatus.RUNNING.value,
    started_at=datetime.now().isoformat(),
    platform=config.get('platform', {}).get('primary', 'auto'),
    config_hash="e2e_test_hash",
    source_type=config['source']['type'],
    target_type=config['target']['type']
)
print(f"   Audit Status: {audit.status}")

# Step 3: Simulate processing
print("\n4. Simulating ETL Processing...")
time.sleep(0.5)

# Step 4: Data Quality Check
dq_rules = config.get('data_quality', {}).get('rules', [])
print(f"\n5. Running Data Quality ({len(dq_rules)} rules)...")
dq_results = {
    'overall_score': 0.95,
    'passed_rules': len(dq_rules) - 1,
    'failed_rules': ['completeness_check'],
    'total_rules': len(dq_rules)
}
print(f"   DQ Score: {dq_results['overall_score']:.1%}")
print(f"   Passed: {dq_results['passed_rules']}/{dq_results['total_rules']}")

# Step 5: Update audit with results
print("\n6. Updating Audit Record...")
audit.status = RunStatus.SUCCEEDED.value
audit.completed_at = datetime.now().isoformat()
audit.rows_read = 1_000_000
audit.rows_written = 998_500
audit.dq_score = dq_results['overall_score']
audit.estimated_cost_usd = 3.50
audit.recommendations = [
    "Consider partitioning by year/month",
    "Enable Adaptive Query Execution",
    "Use Graviton instances for 40% cost savings"
]

print(f"   Final Status: {audit.status}")
print(f"   Rows: {audit.rows_read:,} read / {audit.rows_written:,} written")
print(f"   DQ Score: {audit.dq_score:.1%}")
print(f"   Cost: ${audit.estimated_cost_usd:.2f}")
print(f"   Recommendations: {len(audit.recommendations)}")

# Step 6: Notification check
print("\n7. Notification Status:")
print(f"   Would notify on success: {status['preferences']['on_success']}")
print(f"   Would notify on failure: {status['preferences']['on_failure']}")
print(f"   Would notify on DQ failure: {status['preferences']['on_dq_failure']}")

# Summary
print("\n" + "=" * 60)
print("E2E FLOW SUMMARY")
print("=" * 60)
print(f"Job: {job_name}")
print(f"Run ID: {run_id}")
print(f"Status: {audit.status}")
print(f"Duration: ~0.5s (simulated)")
print(f"Rows Processed: {audit.rows_read:,}")
print(f"Data Quality: {audit.dq_score:.1%}")
print(f"Cost: ${audit.estimated_cost_usd:.2f}")
print(f"Notifications: Slack={status['channels']['slack']['enabled']}, "
      f"Teams={status['channels']['teams']['enabled']}, "
      f"Email={status['channels']['email']['enabled']}")

print("\n[PASS] Full E2E flow completed successfully")
```

---

## Step 9: Run Automated Tests

### 9.1 Run All Tests

```bash
# Run the automated test runner
cd /path/to/strands_etl
python test_configs/e2e_test_runner.py
```

### 9.2 Run Tests for Specific Config

```bash
# Test specific config file
python test_configs/e2e_test_runner.py --config simple_all_notifications.json
```

### 9.3 Generate Test Report

```bash
# Generate JSON report
python test_configs/e2e_test_runner.py --report test_results.json
```

### Expected Output

```
============================================================
ETL Framework E2E Test Runner
============================================================
Test configs directory: /path/to/strands_etl/test_configs
Mock mode: True
Config files to test: 7
============================================================

============================================================
Testing: simple_s3_to_s3.json
============================================================
  [PASS] config_parsing:simple_s3_to_s3.json (0.001s)
  [PASS] notification_config:simple_s3_to_s3.json (0.002s)
  [PASS] notification_manager:simple_s3_to_s3.json (0.001s)
  [SKIP] dq_config:simple_s3_to_s3.json (0.001s)
  [PASS] audit_manager:simple_s3_to_s3.json (0.001s)
  [SKIP] teams_integration:simple_s3_to_s3.json (0.001s)
  [PASS] api_handlers:simple_s3_to_s3.json (0.005s)
  [PASS] e2e_flow_mock:simple_s3_to_s3.json (0.102s)

... (more configs)

============================================================
TEST SUMMARY
============================================================
Total Tests:  56
Passed:       48 (85.7%)
Failed:       0
Skipped:      8
============================================================
```

---

## Troubleshooting

### Common Issues

#### 1. Import Error: Module not found

```bash
# Solution: Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

#### 2. Boto3 Credentials Error

```bash
# Solution: Configure AWS credentials
aws configure
# Or export environment variables
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
```

#### 3. Webhook URL Not Working

- Verify the webhook URL is correct
- For Slack: Check app is installed in workspace
- For Teams: Verify connector is configured

#### 4. DynamoDB Table Not Found

```bash
# Create tables first (if using actual AWS)
python -c "
from audit.etl_audit import ETLAuditManager
manager = ETLAuditManager()
manager.create_tables()
"
```

---

## Quick Reference: JSON Flag Values

| Flag | Enable | Disable |
|------|--------|---------|
| String | `"Y"`, `"YES"`, `"TRUE"`, `"1"` | `"N"`, `"NO"`, `"FALSE"`, `"0"` |
| Boolean | `true` | `false` |

### Example Notification Config

```json
{
  "notifications": {
    "enabled": "Y",

    "slack": {
      "enabled": "Y",
      "webhook_url": "https://hooks.slack.com/...",
      "channel": "#alerts"
    },

    "teams": {
      "enabled": "N"
    },

    "email": {
      "enabled": "Y",
      "sender": "etl@company.com",
      "recipients": ["team@company.com"]
    },

    "preferences": {
      "on_start": "N",
      "on_success": "Y",
      "on_failure": "Y",
      "on_dq_failure": "Y",
      "on_cost_alert": "Y"
    }
  }
}
```

---

## Next Steps

After completing manual testing:

1. **Set up real webhooks** for Slack/Teams
2. **Configure SES** for email notifications
3. **Deploy API Gateway** to AWS
4. **Create DynamoDB tables** for audit
5. **Set up CloudWatch dashboards**
6. **Configure Streamlit** for visualization

Refer to `docs/COMPLETE_BABY_STEPS_GUIDE.md` for production setup instructions.
