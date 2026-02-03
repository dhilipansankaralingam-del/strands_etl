# Manual End-to-End Baby Steps Guide

## Overview

This guide walks you through **manually testing** each component of the Enterprise ETL Framework step-by-step.

---

## Prerequisites

```bash
# 1. Clone the repo and checkout branch
git clone <repo-url>
cd strands_etl
git checkout claude/fix-todo-mkviugmx560v5ibm-HTv7q

# 2. Install dependencies
pip install boto3 streamlit requests

# 3. Configure AWS credentials
export AWS_REGION=us-east-1
export AWS_PROFILE=your-profile  # or use IAM role
```

---

## STEP 1: Create Audit Tables (One-time Setup)

### 1.1 Create DynamoDB Tables

```python
# step1_create_audit_tables.py

from audit import ETLAuditManager, AuditBackend

# Initialize audit manager
audit_manager = ETLAuditManager(
    backend=AuditBackend.DYNAMODB,
    region='us-east-1',
    table_prefix='dev_'  # Use 'prod_' for production
)

# Create tables
print("Creating audit tables...")
results = audit_manager.create_tables()

for table_name, success in results.items():
    status = "✓ Created" if success else "✗ Failed"
    print(f"  {table_name}: {status}")

print("\nTables created:")
print("  - dev_etl_audit_runs        (stores all ETL run information)")
print("  - dev_etl_audit_data_quality (stores data quality results)")
print("  - dev_etl_audit_recommendations (stores platform recommendations)")
```

**Run:**
```bash
python step1_create_audit_tables.py
```

**Verify in AWS Console:**
1. Go to DynamoDB → Tables
2. You should see 3 new tables with `dev_` prefix

---

## STEP 2: Test Audit System

### 2.1 Start an Audit Run

```python
# step2_test_audit.py

from audit import ETLAuditManager, AuditBackend, RunStatus
import json

# Initialize
audit_manager = ETLAuditManager(
    backend=AuditBackend.DYNAMODB,
    region='us-east-1',
    table_prefix='dev_'
)

# Sample config (same structure as your ETL config)
config = {
    'workload': {
        'name': 'customer_360_etl',
        'data_volume': 'high',
        'complexity': 'high'
    },
    'execution': {
        'platform': 'glue',
        'worker_type': 'G.1X',
        'number_of_workers': 5
    },
    'data_sources': [
        {'database': 'ecommerce_db', 'table': 'orders'},
        {'database': 'ecommerce_db', 'table': 'customers'}
    ]
}

# START the run (this creates an audit record)
print("Starting audit run...")
audit = audit_manager.start_run('customer_360_etl', config)
print(f"  Run ID: {audit.run_id}")
print(f"  Status: {audit.status}")
print(f"  Started: {audit.started_at}")

# SIMULATE some processing metrics
import time
print("\nSimulating ETL processing...")
time.sleep(2)  # Simulate work

# Update metrics during run
audit.rows_read = 1500000
audit.rows_written = 75000
audit.bytes_read = 5 * 1024 * 1024 * 1024  # 5 GB
audit.bytes_written = 500 * 1024 * 1024    # 500 MB
audit.dpu_seconds = 3600  # 1 hour
audit.shuffle_bytes = 1024 * 1024 * 1024   # 1 GB shuffle

# Update data quality results
audit.dq_score = 0.95
audit.dq_rules_passed = 19
audit.dq_rules_failed = 1
audit.dq_details = {
    'failed_rules': [
        {'rule': 'email_format', 'table': 'customers', 'fail_rate': 0.02}
    ]
}

# Add recommendations
audit.recommendations = [
    "Consider using broadcast join for small dimension tables",
    "Enable Flex mode for 35% cost savings"
]
audit.platform_recommendation = "glue_flex"

# COMPLETE the run
print("\nCompleting audit run...")
audit_manager.complete_run(audit, RunStatus.SUCCEEDED)

print(f"  Status: {audit.status}")
print(f"  Duration: {audit.duration_seconds} seconds")
print(f"  Cost: ${audit.estimated_cost_usd}")
print(f"  DQ Score: {audit.dq_score}")

# VERIFY - Query the run back
print("\n--- Verifying stored audit record ---")
stored_audit = audit_manager.get_run('customer_360_etl', audit.run_id)
if stored_audit:
    print(f"  ✓ Run ID: {stored_audit.run_id}")
    print(f"  ✓ Rows Read: {stored_audit.rows_read:,}")
    print(f"  ✓ Rows Written: {stored_audit.rows_written:,}")
    print(f"  ✓ Cost: ${stored_audit.estimated_cost_usd}")
else:
    print("  ✗ Failed to retrieve audit record")
```

**Run:**
```bash
python step2_test_audit.py
```

**Expected Output:**
```
Starting audit run...
  Run ID: customer_360_etl_20240115_103045_a1b2c3d4
  Status: RUNNING
  Started: 2024-01-15T10:30:45.123456

Simulating ETL processing...

Completing audit run...
  Status: SUCCEEDED
  Duration: 2 seconds
  Cost: $2.2
  DQ Score: 0.95

--- Verifying stored audit record ---
  ✓ Run ID: customer_360_etl_20240115_103045_a1b2c3d4
  ✓ Rows Read: 1,500,000
  ✓ Rows Written: 75,000
  ✓ Cost: $2.2
```

---

## STEP 3: Test Dashboard Data Retrieval

### 3.1 Get Dashboard Data from Audit Tables

```python
# step3_test_dashboard_data.py

from audit import ETLAuditManager, AuditBackend
import json

audit_manager = ETLAuditManager(
    backend=AuditBackend.DYNAMODB,
    region='us-east-1',
    table_prefix='dev_'
)

# Get dashboard data (last 24 hours)
print("Fetching dashboard data from audit tables...")
dashboard_data = audit_manager.get_dashboard_data(hours=24)

print("\n--- DASHBOARD SUMMARY ---")
print(json.dumps(dashboard_data['summary'], indent=2))

print("\n--- JOBS SUMMARY ---")
for job_name, stats in dashboard_data['jobs_summary'].items():
    print(f"\n  {job_name}:")
    print(f"    Total Runs: {stats['total']}")
    print(f"    Successful: {stats['successful']}")
    print(f"    Failed: {stats['failed']}")
    print(f"    Cost: ${stats['cost']:.2f}")

print("\n--- RECENT RUNS ---")
for run in dashboard_data['recent_runs'][:5]:
    print(f"  {run['job_name']}: {run['status']} ({run['duration_seconds']}s, ${run['cost_usd']:.2f})")

if dashboard_data['recent_failures']:
    print("\n--- RECENT FAILURES ---")
    for failure in dashboard_data['recent_failures']:
        print(f"  {failure['job_name']}: {failure['error'][:50]}...")
```

**Run:**
```bash
python step3_test_dashboard_data.py
```

---

## STEP 4: Test All Agents with Audit Integration

### 4.1 Complete Agent Test with Audit

```python
# step4_test_all_agents.py

import json
from datetime import datetime

# Import audit
from audit import ETLAuditManager, AuditBackend, RunStatus, DataQualityAudit, PlatformRecommendationAudit
import uuid

# Import agents
from agents import (
    WorkloadAssessmentAgent,
    CodeAnalysisAgent,
    DataQualityAgent,
    AutoHealingAgent,
    ComplianceAgent,
    EKSOptimizer,
    AWSRecommendationsEngine
)

def run_complete_test():
    print("=" * 70)
    print("COMPLETE AGENT TEST WITH AUDIT")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print()

    # Initialize audit
    audit_manager = ETLAuditManager(
        backend=AuditBackend.DYNAMODB,
        region='us-east-1',
        table_prefix='dev_'
    )

    # Configuration
    config = {
        'workload': {
            'name': 'test_etl_job',
            'data_volume': 'high',
            'complexity': 'high',
            'criticality': 'medium',
            'schedule_type': 'daily'
        },
        'execution': {
            'platform': 'glue',
            'worker_type': 'G.1X',
            'number_of_workers': 10
        },
        'data_sources': [
            {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'orders'},
            {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'customers'}
        ],
        'compliance': {
            'enabled': True,
            'frameworks': ['GDPR', 'PCI_DSS']
        },
        'data_quality': {
            'enabled': True,
            'rules': [
                {
                    'id': 'orders_amount',
                    'name': 'Order Amount Positive',
                    'type': 'validity',
                    'table': 'ecommerce_db.orders',
                    'column': 'total_amount',
                    'expression': 'total_amount > 0',
                    'threshold': 0.99,
                    'severity': 'error'
                }
            ]
        }
    }

    sample_code = '''
orders_df = spark.table("ecommerce_db.orders")
customers_df = spark.table("ecommerce_db.customers")

# Join without broadcast hint
result = orders_df.join(customers_df, "customer_id")

# Collect to driver (anti-pattern!)
# data = result.collect()

result = result.groupBy("segment").agg({"total_amount": "sum"})
result.write.mode("overwrite").parquet("s3://bucket/output/")
'''

    # START AUDIT
    print("STEP 1: Starting Audit Run")
    print("-" * 50)
    audit = audit_manager.start_run('test_etl_job', config)
    print(f"  Run ID: {audit.run_id}")
    print()

    # WORKLOAD ASSESSMENT
    print("STEP 2: Workload Assessment")
    print("-" * 50)
    workload_agent = WorkloadAssessmentAgent()
    assessment = workload_agent.assess_workload(config, code=sample_code)

    print(f"  Category: {assessment.category.value}")
    print(f"  Recommended Platform: {assessment.recommended_platform.value}")
    print(f"  Flex Mode Suitable: {assessment.flex_mode_suitable}")
    print(f"  Karpenter Suitable: {assessment.karpenter_suitable}")
    print(f"  Estimated Cost: ${assessment.estimated_cost:.2f}")

    # Update audit with assessment
    audit.platform_recommendation = assessment.recommended_platform.value
    audit.recommendations.extend(assessment.optimization_opportunities)
    print()

    # Save recommendation audit
    rec_audit = PlatformRecommendationAudit(
        audit_id=str(uuid.uuid4()),
        run_id=audit.run_id,
        job_name=audit.job_name,
        current_platform=config['execution']['platform'],
        current_config=config['execution'],
        recommended_platform=assessment.recommended_platform.value,
        confidence_score=assessment.confidence_score,
        reasoning=assessment.reasoning,
        estimated_savings_pct=35.0 if assessment.flex_mode_suitable else 0.0
    )
    audit_manager.save_recommendation_audit(rec_audit)
    print("  ✓ Saved recommendation to audit table")
    print()

    # CODE ANALYSIS
    print("STEP 3: Code Analysis")
    print("-" * 50)
    code_agent = CodeAnalysisAgent()
    analysis = code_agent.analyze_code(sample_code)

    print(f"  Issues Found: {len(analysis.get('issues', []))}")
    for issue in analysis.get('issues', [])[:3]:
        print(f"    - {issue}")

    print(f"  Recommendations: {len(analysis.get('recommendations', []))}")
    for rec in analysis.get('recommendations', [])[:3]:
        print(f"    - {rec}")

    audit.recommendations.extend(analysis.get('recommendations', []))
    print()

    # DATA QUALITY
    print("STEP 4: Data Quality Check")
    print("-" * 50)
    dq_agent = DataQualityAgent()
    rules = dq_agent.parse_rules_from_config(config)

    print(f"  Rules Parsed: {len(rules)}")
    for rule in rules:
        print(f"    - {rule.name} ({rule.rule_type.value})")

    # Simulate DQ results
    audit.dq_score = 0.96
    audit.dq_rules_passed = len(rules)
    audit.dq_rules_failed = 0

    # Save DQ audit
    dq_audit = DataQualityAudit(
        audit_id=str(uuid.uuid4()),
        run_id=audit.run_id,
        job_name=audit.job_name,
        table_name='ecommerce_db.orders',
        overall_score=0.96,
        total_rules=len(rules),
        passed_rules=len(rules),
        failed_rules=0,
        rule_results=[{'rule': r.name, 'passed': True} for r in rules],
        recommendations=['All rules passed']
    )
    audit_manager.save_dq_audit(dq_audit)
    print("  ✓ Saved data quality results to audit table")
    print()

    # AWS RECOMMENDATIONS
    print("STEP 5: AWS Architecture Recommendations")
    print("-" * 50)
    aws_engine = AWSRecommendationsEngine()
    arch_rec = aws_engine.recommend_architecture(config)

    print(f"  Compute: {arch_rec.compute[0].service.value if arch_rec.compute else 'N/A'}")
    print(f"  Orchestration: {arch_rec.orchestration[0].service.value if arch_rec.orchestration else 'N/A'}")
    print(f"  Estimated Monthly Cost: ${arch_rec.estimated_monthly_cost}")
    print()

    # EKS COST ANALYSIS
    print("STEP 6: EKS/SPOT/Graviton Analysis")
    print("-" * 50)
    eks_optimizer = EKSOptimizer()
    migration = eks_optimizer.estimate_t_series_migration(
        current_instance="t3.large",
        current_capacity="on-demand",
        hours_per_day=8
    )

    print(f"  Current Monthly Cost: ${migration['current']['monthly_cost']}")
    print(f"  Best Option: {migration['best_option']['name']}")
    print(f"  Potential Savings: {migration['max_savings']['percentage']}%")
    print()

    # SIMULATE PROCESSING
    print("STEP 7: Simulating ETL Processing")
    print("-" * 50)
    import time
    time.sleep(1)

    audit.rows_read = 2500000
    audit.rows_written = 125000
    audit.bytes_read = 8 * 1024 * 1024 * 1024  # 8 GB
    audit.bytes_written = 800 * 1024 * 1024     # 800 MB
    audit.dpu_seconds = 5400  # 1.5 hours

    print(f"  Rows Read: {audit.rows_read:,}")
    print(f"  Rows Written: {audit.rows_written:,}")
    print()

    # COMPLETE AUDIT
    print("STEP 8: Completing Audit")
    print("-" * 50)
    audit_manager.complete_run(audit, RunStatus.SUCCEEDED)

    print(f"  Status: {audit.status}")
    print(f"  Duration: {audit.duration_seconds} seconds")
    print(f"  Estimated Cost: ${audit.estimated_cost_usd}")
    print()

    # VERIFY
    print("STEP 9: Verification")
    print("-" * 50)
    stored = audit_manager.get_run('test_etl_job', audit.run_id)
    if stored:
        print(f"  ✓ Audit record stored successfully")
        print(f"  ✓ Run ID: {stored.run_id}")
        print(f"  ✓ Status: {stored.status}")
        print(f"  ✓ Rows Processed: {stored.rows_read:,}")
        print(f"  ✓ DQ Score: {stored.dq_score}")
        print(f"  ✓ Recommendations: {len(stored.recommendations)}")
    else:
        print("  ✗ Failed to retrieve audit record")

    print()
    print("=" * 70)
    print("TEST COMPLETE!")
    print("=" * 70)
    print()
    print("Check DynamoDB tables in AWS Console:")
    print("  - dev_etl_audit_runs")
    print("  - dev_etl_audit_data_quality")
    print("  - dev_etl_audit_recommendations")

if __name__ == '__main__':
    run_complete_test()
```

**Run:**
```bash
python step4_test_all_agents.py
```

---

## STEP 5: Test Streamlit Dashboard with Audit Data

### 5.1 Update Dashboard to Read from Audit Tables

The Streamlit dashboard will now read from the audit tables. Run it with:

```bash
streamlit run integrations/streamlit_dashboard.py
```

### 5.2 Verify Dashboard Shows Audit Data

1. Open http://localhost:8501
2. Check "Overview" tab - should show runs from audit tables
3. Check "Job Status" tab - should show job details
4. Check "Data Quality" tab - should show DQ scores

---

## STEP 6: Test Slack Integration (Manual)

### 6.1 Test Without Slack (Dry Run)

```python
# step6_test_slack_dry_run.py

from integrations import SlackBot, SlackIntegration

# Test command parsing (no Slack connection needed)
bot = SlackBot()

test_commands = [
    "run job customer_360_etl",
    "status customer_360_etl",
    "list jobs",
    "cancel job failed_etl",
    "logs customer_360_etl",
    "help"
]

print("Testing Slack Bot Command Parsing")
print("-" * 50)

for cmd in test_commands:
    parsed = bot.parse_command(cmd)
    if parsed:
        print(f"✓ '{cmd}'")
        print(f"    Type: {parsed.command_type.value}")
        print(f"    Job: {parsed.job_name}")
    else:
        print(f"✗ '{cmd}' - Not recognized")
    print()
```

**Run:**
```bash
python step6_test_slack_dry_run.py
```

### 6.2 Test With Real Slack (Requires Setup)

```python
# step6_test_slack_real.py
# Only run this if you have Slack configured

import os
from integrations import SlackIntegration

# Set your webhook URL (get from Slack App settings)
# export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...

webhook_url = os.environ.get('SLACK_WEBHOOK_URL')

if not webhook_url:
    print("SLACK_WEBHOOK_URL not set. Skipping real Slack test.")
    exit(0)

slack = SlackIntegration(webhook_url=webhook_url)

# Send test notification
print("Sending test notification to Slack...")
success = slack.send_job_notification(
    job_name='test_etl_job',
    status='SUCCEEDED',
    channel='#etl-alerts',  # Change to your channel
    details={
        'Duration': '15 minutes',
        'Rows Processed': '1,500,000',
        'Cost': '$2.50'
    }
)

if success:
    print("✓ Slack notification sent!")
else:
    print("✗ Failed to send notification")
```

---

## STEP 7: Test Email Reporting

### 7.1 Generate Email Report (Without Sending)

```python
# step7_test_email.py

from integrations import EmailReporter
from audit import ETLAuditManager, AuditBackend

# Get data from audit tables
audit_manager = ETLAuditManager(
    backend=AuditBackend.DYNAMODB,
    region='us-east-1',
    table_prefix='dev_'
)

# Get recent runs for report
runs = audit_manager.get_recent_runs(hours=24, limit=20)

# Convert to format expected by reporter
jobs_data = [
    {
        'name': r.job_name,
        'status': r.status,
        'duration': f"{r.duration_seconds}s",
        'cost': float(r.estimated_cost_usd),
        'started': r.started_at,
        'error': r.error_message if r.error_message else ''
    }
    for r in runs
]

# Generate report
reporter = EmailReporter(region='us-east-1')
html = reporter.generate_job_summary_report(jobs_data, 'daily')

# Save to file for preview
with open('/tmp/etl_report.html', 'w') as f:
    f.write(html)

print(f"Generated email report: {len(html)} characters")
print(f"Preview at: file:///tmp/etl_report.html")
print("\nOpen this file in a browser to see the report.")
```

**Run:**
```bash
python step7_test_email.py
# Then open /tmp/etl_report.html in browser
```

---

## STEP 8: Full End-to-End Test

### 8.1 Complete E2E Script

```python
# step8_full_e2e_test.py
"""
FULL END-TO-END TEST
Runs all components together with audit integration
"""

import json
import time
from datetime import datetime

# Audit
from audit import ETLAuditManager, AuditBackend, RunStatus

# Agents
from agents import (
    WorkloadAssessmentAgent,
    CodeAnalysisAgent,
    DataQualityAgent,
    AWSRecommendationsEngine,
    EKSOptimizer
)

# Integrations
from integrations import EmailReporter

# Script converter
from scripts.glue_script_converter import GlueScriptConverter


def full_e2e_test():
    """Run complete end-to-end test."""
    print("=" * 70)
    print("FULL END-TO-END TEST")
    print(f"Started: {datetime.now()}")
    print("=" * 70)
    print()

    results = {}

    # ========================================
    # 1. SETUP
    # ========================================
    print("[1/8] Setting up audit manager...")
    try:
        audit_manager = ETLAuditManager(
            backend=AuditBackend.DYNAMODB,
            region='us-east-1',
            table_prefix='test_'
        )
        results['audit_setup'] = '✓ PASS'
    except Exception as e:
        results['audit_setup'] = f'✗ FAIL: {e}'
        return results

    # ========================================
    # 2. CREATE TABLES
    # ========================================
    print("[2/8] Creating/verifying audit tables...")
    try:
        table_results = audit_manager.create_tables()
        all_created = all(table_results.values())
        results['create_tables'] = '✓ PASS' if all_created else '✗ FAIL'
    except Exception as e:
        results['create_tables'] = f'✗ FAIL: {e}'

    # ========================================
    # 3. START AUDIT RUN
    # ========================================
    print("[3/8] Starting audit run...")
    config = {
        'workload': {'name': 'e2e_test_job', 'data_volume': 'medium', 'complexity': 'medium'},
        'execution': {'platform': 'glue', 'worker_type': 'G.1X', 'number_of_workers': 5},
        'data_sources': [{'database': 'test_db', 'table': 'test_table'}]
    }

    try:
        audit = audit_manager.start_run('e2e_test_job', config)
        results['start_audit'] = f'✓ PASS (Run ID: {audit.run_id[:30]}...)'
    except Exception as e:
        results['start_audit'] = f'✗ FAIL: {e}'
        return results

    # ========================================
    # 4. WORKLOAD ASSESSMENT
    # ========================================
    print("[4/8] Running workload assessment...")
    try:
        agent = WorkloadAssessmentAgent()
        assessment = agent.assess_workload(config)
        audit.platform_recommendation = assessment.recommended_platform.value
        results['workload_assessment'] = f'✓ PASS (Recommended: {assessment.recommended_platform.value})'
    except Exception as e:
        results['workload_assessment'] = f'✗ FAIL: {e}'

    # ========================================
    # 5. CODE ANALYSIS
    # ========================================
    print("[5/8] Running code analysis...")
    sample_code = 'df = spark.table("test_db.test"); result = df.groupBy("id").count()'
    try:
        agent = CodeAnalysisAgent()
        analysis = agent.analyze_code(sample_code)
        audit.recommendations.extend(analysis.get('recommendations', [])[:3])
        results['code_analysis'] = f'✓ PASS (Issues: {len(analysis.get("issues", []))})'
    except Exception as e:
        results['code_analysis'] = f'✗ FAIL: {e}'

    # ========================================
    # 6. AWS RECOMMENDATIONS
    # ========================================
    print("[6/8] Getting AWS recommendations...")
    try:
        engine = AWSRecommendationsEngine()
        arch = engine.recommend_architecture(config)
        results['aws_recommendations'] = f'✓ PASS (Cost: ${arch.estimated_monthly_cost})'
    except Exception as e:
        results['aws_recommendations'] = f'✗ FAIL: {e}'

    # ========================================
    # 7. COMPLETE AUDIT
    # ========================================
    print("[7/8] Completing audit...")
    try:
        audit.rows_read = 100000
        audit.rows_written = 50000
        audit.dpu_seconds = 600
        audit.dq_score = 0.98

        audit_manager.complete_run(audit, RunStatus.SUCCEEDED)
        results['complete_audit'] = f'✓ PASS (Cost: ${audit.estimated_cost_usd})'
    except Exception as e:
        results['complete_audit'] = f'✗ FAIL: {e}'

    # ========================================
    # 8. VERIFY DATA
    # ========================================
    print("[8/8] Verifying audit data...")
    try:
        stored = audit_manager.get_run('e2e_test_job', audit.run_id)
        if stored and stored.status == 'SUCCEEDED':
            results['verify_data'] = '✓ PASS'
        else:
            results['verify_data'] = '✗ FAIL: Data not found or status mismatch'
    except Exception as e:
        results['verify_data'] = f'✗ FAIL: {e}'

    # ========================================
    # SUMMARY
    # ========================================
    print()
    print("=" * 70)
    print("TEST RESULTS")
    print("=" * 70)

    passed = 0
    failed = 0
    for test, result in results.items():
        print(f"  {test}: {result}")
        if '✓' in result:
            passed += 1
        else:
            failed += 1

    print()
    print(f"TOTAL: {passed} passed, {failed} failed")
    print("=" * 70)

    return results


if __name__ == '__main__':
    full_e2e_test()
```

**Run:**
```bash
python step8_full_e2e_test.py
```

---

## Quick Reference: File Locations

| Component | Location |
|-----------|----------|
| Audit Module | `audit/etl_audit.py` |
| Agents | `agents/*.py` |
| Slack Integration | `integrations/slack_integration.py` |
| Email Reporter | `integrations/email_reporter.py` |
| Streamlit Dashboard | `integrations/streamlit_dashboard.py` |
| CloudWatch Dashboard | `dashboards/cloudwatch_dashboard.py` |
| Script Converter | `scripts/glue_script_converter.py` |

---

## Audit Table Schema

### etl_audit_runs

| Field | Type | Description |
|-------|------|-------------|
| job_name | String (PK) | ETL job name |
| run_id | String (SK) | Unique run identifier |
| status | String | PENDING/RUNNING/SUCCEEDED/FAILED |
| started_at | String | ISO timestamp |
| completed_at | String | ISO timestamp |
| duration_seconds | Number | Execution duration |
| rows_read | Number | Input rows count |
| rows_written | Number | Output rows count |
| estimated_cost_usd | Number | Cost estimate |
| dq_score | Number | Data quality score (0-1) |
| recommendations | List | Platform/code recommendations |
| error_message | String | Error details if failed |

### etl_audit_data_quality

| Field | Type | Description |
|-------|------|-------------|
| run_id | String (PK) | Links to etl_audit_runs |
| audit_id | String (SK) | Unique DQ audit ID |
| table_name | String | Table checked |
| overall_score | Number | DQ score (0-1) |
| passed_rules | Number | Rules that passed |
| failed_rules | Number | Rules that failed |
| rule_results | List | Detailed rule results |

### etl_audit_recommendations

| Field | Type | Description |
|-------|------|-------------|
| job_name | String (PK) | ETL job name |
| audit_id | String (SK) | Unique recommendation ID |
| recommended_platform | String | Suggested platform |
| reasoning | List | Why this recommendation |
| estimated_savings_pct | Number | Potential savings |

---

## Troubleshooting

### DynamoDB Tables Not Creating
```bash
# Check IAM permissions
aws iam get-role-policy --role-name YourRole --policy-name DynamoDBAccess
```

### Audit Records Not Saving
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Dashboard Not Showing Data
```python
# Check if data exists
from audit import ETLAuditManager, AuditBackend
audit = ETLAuditManager(backend=AuditBackend.DYNAMODB, table_prefix='dev_')
print(audit.get_recent_runs(hours=24))
```
