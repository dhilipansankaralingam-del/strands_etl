# Complete End-to-End Testing Guide

This guide provides step-by-step baby steps for testing the ETL Framework component by component, followed by full E2E integration testing.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step 1: Run Component Tests](#step-1-run-component-tests)
4. [Step 2: Provision AWS Resources](#step-2-provision-aws-resources)
5. [Step 3: Setup Slack Integration](#step-3-setup-slack-integration)
6. [Step 4: Setup Teams Integration](#step-4-setup-teams-integration)
7. [Step 5: Test with Simple JSON Configs](#step-5-test-with-simple-json-configs)
8. [Step 6: Test with Complex JSON Configs](#step-6-test-with-complex-json-configs)
9. [Step 7: Run Full E2E Tests](#step-7-run-full-e2e-tests)
10. [Step 8: Verify Everything Works](#step-8-verify-everything-works)

---

## Overview

### Testing Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                    TESTING PYRAMID                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│    ┌─────────────────────────────────────────────┐              │
│    │          STEP 7: Full E2E Tests             │  ← Final     │
│    └─────────────────────────────────────────────┘              │
│                                                                  │
│    ┌─────────────────────────────────────────────┐              │
│    │    STEP 5-6: Simple & Complex JSON Tests    │  ← Integration│
│    └─────────────────────────────────────────────┘              │
│                                                                  │
│    ┌─────────────────────────────────────────────┐              │
│    │    STEP 3-4: Slack & Teams Integration      │  ← Setup     │
│    └─────────────────────────────────────────────┘              │
│                                                                  │
│    ┌─────────────────────────────────────────────┐              │
│    │      STEP 2: AWS Resource Provisioning      │  ← Infra     │
│    └─────────────────────────────────────────────┘              │
│                                                                  │
│    ┌─────────────────────────────────────────────┐              │
│    │         STEP 1: Component Tests             │  ← Unit      │
│    └─────────────────────────────────────────────┘              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Component Tests Available

| Test | File | Description |
|------|------|-------------|
| 01 | `test_01_config_parsing.py` | JSON configuration parsing |
| 02 | `test_02_notification_flags.py` | Y/N flag parsing |
| 03 | `test_03_notification_manager.py` | Notification manager |
| 04 | `test_04_teams_integration.py` | Teams integration |
| 05 | `test_05_api_gateway.py` | API Gateway handlers |
| 06 | `test_06_audit_system.py` | Audit system |

---

## Prerequisites

### 1. Clone and Setup Environment

```bash
# Navigate to project
cd /path/to/strands_etl

# Set Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Install dependencies
pip install boto3 requests
```

### 2. AWS Credentials (for provisioning)

```bash
# Option A: AWS CLI
aws configure

# Option B: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

---

## Step 1: Run Component Tests

### 1.1 Test Configuration Parsing

```bash
# Test that all JSON configs can be parsed
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
...
```

### 1.2 Test Notification Y/N Flags

```bash
# Test Y/N flag parsing (Y, N, YES, NO, true, false, 1, 0)
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
...
```

### 1.3 Test Notification Manager

```bash
python tests/component_tests/test_03_notification_manager.py
```

### 1.4 Test Teams Integration

```bash
python tests/component_tests/test_04_teams_integration.py
```

### 1.5 Test API Gateway

```bash
python tests/component_tests/test_05_api_gateway.py
```

### 1.6 Test Audit System

```bash
python tests/component_tests/test_06_audit_system.py
```

### 1.7 Run ALL Component Tests at Once

```bash
python tests/run_all_tests.py --component-only
```

---

## Step 2: Provision AWS Resources

### 2.1 Provision DynamoDB Tables

```bash
# Create DynamoDB tables for audit
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

### 2.2 Provision API Gateway

```bash
# Create API Gateway with Lambda backend
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

  Test endpoints:
    curl https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod/health
    curl https://abc123xyz.execute-api.us-east-1.amazonaws.com/prod/jobs
```

### 2.3 Test API Endpoints

```bash
# Test health endpoint
curl https://YOUR_API_ID.execute-api.us-east-1.amazonaws.com/prod/health

# Expected response:
# {"status": "healthy", "timestamp": "2024-01-15T10:30:00", "version": "1.0.0"}
```

---

## Step 3: Setup Slack Integration

### 3.1 View Setup Instructions

```bash
python scripts/provisioning/setup_slack.py --instructions
```

### 3.2 Create Slack App

1. Go to https://api.slack.com/apps
2. Create New App → From scratch
3. Name: "ETL Framework Alerts"
4. Select workspace
5. Go to "Incoming Webhooks" → Enable
6. Click "Add New Webhook to Workspace"
7. Select channel (#etl-alerts)
8. Copy webhook URL

### 3.3 Test Slack Integration

```bash
# Test webhook (without sending message)
python scripts/provisioning/setup_slack.py \
  --webhook-url "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Test with actual message
python scripts/provisioning/setup_slack.py \
  --webhook-url "https://hooks.slack.com/services/YOUR/WEBHOOK/URL" \
  --send-test
```

### 3.4 Store in Secrets Manager (Optional)

```bash
python scripts/provisioning/setup_slack.py \
  --webhook-url "https://hooks.slack.com/services/YOUR/WEBHOOK/URL" \
  --store-secret \
  --region us-east-1
```

---

## Step 4: Setup Teams Integration

### 4.1 View Setup Instructions

```bash
python scripts/provisioning/setup_teams.py --instructions
```

### 4.2 Create Teams Connector

1. Open Microsoft Teams
2. Go to your channel (e.g., "ETL Alerts")
3. Click "..." → Connectors
4. Search "Incoming Webhook" → Configure
5. Name: "ETL Framework"
6. Create and copy webhook URL

### 4.3 Test Teams Integration

```bash
# Test webhook (without sending message)
python scripts/provisioning/setup_teams.py \
  --webhook-url "https://outlook.office.com/webhook/YOUR/WEBHOOK/URL"

# Test with actual message
python scripts/provisioning/setup_teams.py \
  --webhook-url "https://outlook.office.com/webhook/YOUR/WEBHOOK/URL" \
  --send-test
```

---

## Step 5: Test with Simple JSON Configs

### 5.1 Test Config: No Notifications

```bash
cat test_configs/simple_s3_to_s3.json
```

```json
{
  "job_name": "simple_s3_copy",
  "source": { "type": "s3", "path": "s3://source/..." },
  "target": { "type": "s3", "path": "s3://target/..." },
  "notifications": { "enabled": "N" }
}
```

```python
# Test in Python
python -c "
import json
import sys
sys.path.insert(0, '.')
from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_s3_to_s3.json') as f:
    config = json.load(f)

manager = create_notification_manager(config)
print('Status:', manager.get_status())
"
```

### 5.2 Test Config: Slack Only

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')
from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_glue_catalog.json') as f:
    config = json.load(f)

manager = create_notification_manager(config)
status = manager.get_status()
print('Slack enabled:', status['channels']['slack']['enabled'])
print('Teams enabled:', status['channels']['teams']['enabled'])
print('Email enabled:', status['channels']['email']['enabled'])
"
```

### 5.3 Test Config: Teams Only

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')
from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_teams_only.json') as f:
    config = json.load(f)

manager = create_notification_manager(config)
status = manager.get_status()
print('Slack enabled:', status['channels']['slack']['enabled'])
print('Teams enabled:', status['channels']['teams']['enabled'])
print('Email enabled:', status['channels']['email']['enabled'])
"
```

### 5.4 Test Config: All Notifications

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')
from integrations.notification_manager import create_notification_manager

with open('test_configs/simple_all_notifications.json') as f:
    config = json.load(f)

manager = create_notification_manager(config)
status = manager.get_status()
print('All channels:', status['channels'])
print('Preferences:', status['preferences'])
"
```

---

## Step 6: Test with Complex JSON Configs

### 6.1 Test Config: Full Pipeline

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')
from integrations.notification_manager import create_notification_manager
from audit.etl_audit import ETLRunAudit, RunStatus
from datetime import datetime
import time

# Load complex config
with open('test_configs/complex_full_pipeline.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])
print('Source:', config['source']['type'])
print('Target:', config['target']['type'])
print('DQ Rules:', len(config.get('data_quality', {}).get('rules', [])))
print('Transformations:', len(config.get('transformations', [])))

# Create notification manager
manager = create_notification_manager(config)
status = manager.get_status()
print('\\nNotifications:')
print('  Slack:', status['channels']['slack']['enabled'])
print('  Teams:', status['channels']['teams']['enabled'])
print('  Email:', status['channels']['email']['enabled'])

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
print('\\nAudit created:', audit.run_id)
"
```

### 6.2 Test Config: EKS with Karpenter

```bash
python -c "
import json
import sys
sys.path.insert(0, '.')
from integrations.notification_manager import create_notification_manager

with open('test_configs/complex_eks_karpenter.json') as f:
    config = json.load(f)

print('Job:', config['job_name'])
print('Platform:', config['platform'])
print('EKS Config:', config.get('platform', {}).get('eks_config', {}))

manager = create_notification_manager(config)
status = manager.get_status()
print('\\nNotifications:', status['channels'])
"
```

---

## Step 7: Run Full E2E Tests

### 7.1 Run All Tests

```bash
# Run complete test suite
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
...
[PASS]

============================================================
RUNNING: test_02_notification_flags
============================================================
...
[PASS]

...

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

...

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

### 7.2 Generate Test Report

```bash
python tests/run_all_tests.py --report test_report.json
```

---

## Step 8: Verify Everything Works

### 8.1 Verification Checklist

```
□ Step 1: All 6 component tests pass
□ Step 2: DynamoDB tables created (verify in AWS Console)
□ Step 2: API Gateway created and /health returns 200
□ Step 3: Slack webhook tested (message received)
□ Step 4: Teams webhook tested (message received)
□ Step 5: All simple JSON configs parse correctly
□ Step 6: All complex JSON configs parse correctly
□ Step 7: Full E2E test suite passes (14/14)
```

### 8.2 Quick Verification Commands

```bash
# 1. Component tests
python tests/run_all_tests.py --component-only

# 2. E2E tests
python tests/run_all_tests.py --e2e-only

# 3. Full suite
python tests/run_all_tests.py

# 4. Test API (if deployed)
curl https://YOUR_API.execute-api.us-east-1.amazonaws.com/prod/health

# 5. Test DynamoDB (if deployed)
aws dynamodb list-tables --region us-east-1 | grep etl_
```

---

## Quick Reference

### JSON Notification Flags

| Value | Meaning |
|-------|---------|
| `"Y"`, `"YES"`, `"TRUE"`, `"1"`, `true` | Enabled |
| `"N"`, `"NO"`, `"FALSE"`, `"0"`, `false` | Disabled |

### Config Files Summary

| File | Slack | Teams | Email | DQ |
|------|-------|-------|-------|-----|
| `simple_s3_to_s3.json` | N | N | N | N |
| `simple_glue_catalog.json` | Y | N | N | N |
| `simple_teams_only.json` | N | Y | N | N |
| `simple_email_alerts.json` | N | N | Y | N |
| `simple_all_notifications.json` | Y | Y | Y | Y |
| `complex_full_pipeline.json` | Y | Y | Y | Y |
| `complex_streaming_pipeline.json` | Y | N | Y | Y |
| `complex_eks_karpenter.json` | N | Y | Y | Y |

### Commands Summary

```bash
# Component tests
python tests/component_tests/test_01_config_parsing.py
python tests/component_tests/test_02_notification_flags.py
python tests/component_tests/test_03_notification_manager.py
python tests/component_tests/test_04_teams_integration.py
python tests/component_tests/test_05_api_gateway.py
python tests/component_tests/test_06_audit_system.py

# Provisioning
python scripts/provisioning/provision_dynamodb.py --region us-east-1
python scripts/provisioning/provision_api_gateway.py --region us-east-1

# Setup
python scripts/provisioning/setup_slack.py --instructions
python scripts/provisioning/setup_teams.py --instructions

# Full test suite
python tests/run_all_tests.py
python tests/run_all_tests.py --report report.json
```

---

## Troubleshooting

### Import Error

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### AWS Credentials

```bash
aws sts get-caller-identity  # Verify credentials
```

### Webhook Not Working

```bash
# Slack
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test"}' \
  YOUR_SLACK_WEBHOOK_URL

# Teams
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test"}' \
  YOUR_TEAMS_WEBHOOK_URL
```
