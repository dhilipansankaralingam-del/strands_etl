# Enterprise ETL Framework - Setup & Demo Guide

## Overview

This guide provides step-by-step instructions for setting up and testing the Enterprise ETL Framework with all its components.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Testing Each Component](#testing-each-component)
5. [End-to-End Demo](#end-to-end-demo)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### AWS Requirements
- AWS Account with appropriate permissions
- IAM Role with access to: Glue, S3, CloudWatch, SES, Athena, Bedrock
- Glue Catalog with sample databases/tables

### Python Requirements
```bash
pip install boto3 streamlit requests
```

### Optional
- Slack workspace with bot configured
- SES verified email addresses

---

## Installation

### Step 1: Clone/Update Repository
```bash
cd /path/to/strands_etl
git checkout claude/fix-todo-mkviugmx560v5ibm-HTv7q
git pull origin claude/fix-todo-mkviugmx560v5ibm-HTv7q
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Configure AWS Credentials
```bash
export AWS_REGION=us-east-1
export AWS_PROFILE=your-profile  # or use IAM role
```

---

## Configuration

### Create Enterprise Config File

Create `config/my_enterprise_config.json`:

```json
{
  "version": "2.0",
  "workload": {
    "name": "customer_360_etl",
    "description": "Customer 360 ETL Pipeline",
    "data_volume": "high",
    "complexity": "high",
    "criticality": "medium",
    "schedule_type": "daily"
  },
  "execution": {
    "platform_fallback": {
      "enabled": true,
      "order": ["glue", "emr_serverless", "lambda"],
      "retry_count": 2
    },
    "auto_healing": {
      "enabled": true,
      "max_attempts": 3,
      "strategies": ["increase_memory", "add_workers", "broadcast_join"]
    }
  },
  "data_sources": [
    {
      "name": "orders",
      "type": "glue_catalog",
      "database": "ecommerce_db",
      "table": "orders"
    },
    {
      "name": "customers",
      "type": "glue_catalog",
      "database": "ecommerce_db",
      "table": "customers"
    }
  ],
  "compliance": {
    "enabled": true,
    "frameworks": ["GDPR", "PCI_DSS"],
    "scan_for_pii": true,
    "mask_pii": true
  },
  "data_quality": {
    "enabled": true,
    "rules": [
      {
        "id": "order_amount_positive",
        "name": "Order Amount Must Be Positive",
        "type": "validity",
        "table": "ecommerce_db.orders",
        "column": "total_amount",
        "expression": "total_amount > 0",
        "threshold": 0.99,
        "severity": "error"
      },
      {
        "id": "customer_email_valid",
        "name": "Valid Customer Email",
        "type": "natural_language",
        "table": "ecommerce_db.customers",
        "column": "email",
        "expression": "email must be valid email format",
        "threshold": 0.95,
        "severity": "warning"
      }
    ]
  },
  "notifications": {
    "slack": {
      "enabled": true,
      "channel": "#etl-alerts",
      "on_failure": true,
      "on_success": false
    },
    "email": {
      "enabled": true,
      "recipients": ["team@example.com"],
      "on_failure": true,
      "daily_summary": true
    }
  }
}
```

---

## Testing Each Component

### Baby Step 1: Test Auto-Healing Agent

```python
# test_auto_healing.py
from agents import AutoHealingAgent

agent = AutoHealingAgent(region='us-east-1')

# Test error classification
error_msg = "Container killed by YARN for exceeding memory limits"
error_type = agent.classify_error(error_msg)
print(f"Error Type: {error_type}")  # Should be OUT_OF_MEMORY

# Test healing strategy
can_heal = agent.can_heal(error_type)
print(f"Can Heal: {can_heal}")  # Should be True

# Test config modification
config = {'worker_type': 'G.1X', 'number_of_workers': 5}
healed_config = agent.apply_healing(error_type, config)
print(f"Healed Config: {healed_config}")  # Should show increased memory
```

**Run:**
```bash
python test_auto_healing.py
```

---

### Baby Step 2: Test Compliance Agent

```python
# test_compliance.py
from agents import ComplianceAgent

agent = ComplianceAgent(region='us-east-1')

# Test PII detection in column names
pii_types = agent.detect_pii_in_column_name('customer_email')
print(f"PII Types: {pii_types}")  # Should detect EMAIL

# Test PII detection in values
pii_types = agent.detect_pii_in_value('john.doe@example.com')
print(f"Value PII: {pii_types}")  # Should detect EMAIL

# Test schema scan (requires Glue Catalog)
# result = agent.scan_schema('ecommerce_db', 'customers')
# print(f"Schema Scan: {result}")
```

**Run:**
```bash
python test_compliance.py
```

---

### Baby Step 3: Test Code Analysis Agent

```python
# test_code_analysis.py
from agents import CodeAnalysisAgent

agent = CodeAnalysisAgent(region='us-east-1')

sample_code = '''
orders_df = spark.table("ecommerce_db.orders")
customers_df = spark.table("ecommerce_db.customers")

# Join without broadcast hint
result = orders_df.join(customers_df, "customer_id")

# Collect to driver (anti-pattern!)
data = result.collect()
'''

analysis = agent.analyze_code(sample_code)
print(f"Issues Found: {len(analysis.get('issues', []))}")
print(f"Recommendations: {analysis.get('recommendations', [])}")
```

**Run:**
```bash
python test_code_analysis.py
```

---

### Baby Step 4: Test Workload Assessment Agent

```python
# test_workload_assessment.py
from agents import WorkloadAssessmentAgent

agent = WorkloadAssessmentAgent(region='us-east-1')

config = {
    'workload': {
        'name': 'test_job',
        'criticality': 'medium',
        'schedule_type': 'daily'
    },
    'data_sources': [
        {
            'type': 'glue_catalog',
            'database': 'ecommerce_db',
            'table': 'orders'
        }
    ]
}

assessment = agent.assess_workload(config)
print(f"Category: {assessment.category.value}")
print(f"Recommended Platform: {assessment.recommended_platform.value}")
print(f"Flex Mode Suitable: {assessment.flex_mode_suitable}")
print(agent.generate_assessment_report(assessment))
```

**Run:**
```bash
python test_workload_assessment.py
```

---

### Baby Step 5: Test Data Quality Agent

```python
# test_data_quality.py
from agents import DataQualityAgent

agent = DataQualityAgent(region='us-east-1')

config = {
    'data_quality': {
        'enabled': True,
        'rules': [
            {
                'id': 'test_rule',
                'name': 'Test Not Null',
                'type': 'completeness',
                'table': 'ecommerce_db.orders',
                'column': 'order_id',
                'threshold': 1.0,
                'severity': 'error'
            }
        ]
    }
}

rules = agent.parse_rules_from_config(config)
print(f"Parsed {len(rules)} rules")

# Generate PySpark validation code
pyspark_code = agent.generate_pyspark_validation(rules)
print(pyspark_code)
```

**Run:**
```bash
python test_data_quality.py
```

---

### Baby Step 6: Test Slack Integration

```python
# test_slack.py
from integrations import SlackIntegration, SlackBot

# Test without actually sending (dry run)
slack = SlackIntegration()
bot = SlackBot()

# Test command parsing
commands = [
    "run job customer_360_etl",
    "status customer_360_etl",
    "list jobs",
    "help"
]

for cmd in commands:
    parsed = bot.parse_command(cmd)
    if parsed:
        print(f"'{cmd}' -> {parsed.command_type.value}: {parsed.job_name}")
    else:
        print(f"'{cmd}' -> Not recognized")

# To actually send messages, set environment variables:
# export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
# slack.send_job_notification('test_job', 'SUCCEEDED', '#test-channel')
```

**Run:**
```bash
python test_slack.py
```

---

### Baby Step 7: Test Email Reporter

```python
# test_email.py
from integrations import EmailReporter

reporter = EmailReporter(region='us-east-1')

# Generate sample report (without sending)
sample_jobs = [
    {'name': 'job1', 'status': 'SUCCEEDED', 'duration': '15m', 'cost': 2.50, 'started': '2024-01-15 08:00'},
    {'name': 'job2', 'status': 'FAILED', 'duration': '3m', 'cost': 0.50, 'started': '2024-01-15 09:00', 'error': 'OOM'}
]

html = reporter.generate_job_summary_report(sample_jobs, 'daily')
print(f"Generated HTML report: {len(html)} characters")

# Save for preview
with open('/tmp/email_preview.html', 'w') as f:
    f.write(html)
print("Saved to /tmp/email_preview.html - open in browser to view")

# To actually send:
# export SES_SENDER_EMAIL=noreply@yourdomain.com
# reporter.send_job_summary(sample_jobs, ['recipient@example.com'], 'daily')
```

**Run:**
```bash
python test_email.py
```

---

### Baby Step 8: Test Streamlit Dashboard

```bash
# Install streamlit
pip install streamlit

# Run dashboard
cd /path/to/strands_etl
streamlit run integrations/streamlit_dashboard.py
```

Open browser to `http://localhost:8501`

---

### Baby Step 9: Test CloudWatch Dashboard

```python
# test_cloudwatch.py
from dashboards import CloudWatchDashboard

dashboard = CloudWatchDashboard(region='us-east-1')

# Create dashboard
result = dashboard.create_etl_dashboard(
    dashboard_name='ETL-Test-Dashboard',
    include_data_quality=True,
    include_cost=True
)

if result['success']:
    print(f"Dashboard URL: {result['dashboard_url']}")
else:
    print(f"Error: {result.get('error')}")

# Create alarms (optional)
# alarms = dashboard.create_job_alarms('customer_360_etl')
# print(f"Created {len(alarms)} alarms")
```

**Run:**
```bash
python test_cloudwatch.py
```

---

## End-to-End Demo

### Demo Script

```python
# demo_enterprise_etl.py
"""
Complete Enterprise ETL Demo
Demonstrates all agents and integrations working together
"""

import json
from datetime import datetime

# Import all components
from agents import (
    AutoHealingAgent,
    ComplianceAgent,
    CodeAnalysisAgent,
    WorkloadAssessmentAgent,
    DataQualityAgent
)
from integrations import SlackIntegration, EmailReporter
from dashboards import CloudWatchDashboard

def run_demo():
    print("=" * 60)
    print("Enterprise ETL Framework Demo")
    print("=" * 60)
    print()

    # Load config
    with open('config/my_enterprise_config.json') as f:
        config = json.load(f)

    print(f"Loaded config for: {config['workload']['name']}")
    print()

    # 1. Workload Assessment
    print("1. Running Workload Assessment...")
    workload_agent = WorkloadAssessmentAgent()
    assessment = workload_agent.assess_workload(config)
    print(f"   - Category: {assessment.category.value}")
    print(f"   - Platform: {assessment.recommended_platform.value}")
    print(f"   - Flex Suitable: {assessment.flex_mode_suitable}")
    print()

    # 2. Code Analysis
    print("2. Running Code Analysis...")
    code_agent = CodeAnalysisAgent()
    sample_code = open('scripts/customer_360_etl.py').read()
    analysis = code_agent.analyze_code(sample_code)
    print(f"   - Issues: {len(analysis.get('issues', []))}")
    print(f"   - Score: {analysis.get('overall_score', 'N/A')}")
    print()

    # 3. Compliance Check
    print("3. Running Compliance Check...")
    compliance_agent = ComplianceAgent()
    # Would scan actual tables in production
    print("   - Frameworks: GDPR, PCI-DSS")
    print("   - PII Detection: Enabled")
    print()

    # 4. Data Quality Rules
    print("4. Parsing Data Quality Rules...")
    dq_agent = DataQualityAgent()
    rules = dq_agent.parse_rules_from_config(config)
    print(f"   - Rules loaded: {len(rules)}")
    print()

    # 5. Setup Monitoring
    print("5. Setting up CloudWatch Dashboard...")
    cw_dashboard = CloudWatchDashboard()
    # In production: cw_dashboard.create_etl_dashboard(...)
    print("   - Dashboard: Ready")
    print()

    # 6. Notifications
    print("6. Notification Channels:")
    print(f"   - Slack: {config['notifications']['slack']['channel']}")
    print(f"   - Email: {config['notifications']['email']['recipients']}")
    print()

    print("=" * 60)
    print("Demo Complete! All components initialized successfully.")
    print("=" * 60)

if __name__ == '__main__':
    run_demo()
```

---

## Architecture Overview

```
Enterprise ETL Framework
├── agents/
│   ├── auto_healing_agent.py      # Error recovery & platform fallback
│   ├── compliance_agent.py        # PII detection & compliance checks
│   ├── code_analysis_agent.py     # PySpark code optimization
│   ├── workload_assessment_agent.py # Platform & resource recommendations
│   └── data_quality_agent.py      # SQL/NL data quality rules
│
├── integrations/
│   ├── slack_integration.py       # Slack bot & notifications
│   ├── email_reporter.py          # HTML email reports
│   └── streamlit_dashboard.py     # Interactive web dashboard
│
├── dashboards/
│   └── cloudwatch_dashboard.py    # AWS CloudWatch dashboards
│
├── config/
│   └── enterprise_config_schema.json  # Config schema
│
└── docs/
    └── ENTERPRISE_SETUP_GUIDE.md  # This guide
```

---

## Troubleshooting

### Common Issues

1. **AWS Credentials Error**
   ```bash
   export AWS_REGION=us-east-1
   aws sts get-caller-identity  # Verify credentials
   ```

2. **Glue Table Not Found**
   - Verify database and table exist in Glue Catalog
   - Check IAM permissions for glue:GetTable

3. **Slack Messages Not Sending**
   - Verify SLACK_WEBHOOK_URL or SLACK_BOT_TOKEN
   - Check Slack app permissions

4. **Email Not Sending**
   - Verify SES sender email is verified
   - Check SES sending limits

5. **Streamlit Not Loading**
   ```bash
   pip install streamlit --upgrade
   streamlit run integrations/streamlit_dashboard.py --server.port 8502
   ```

---

## Next Steps

1. **Customize** the config for your use case
2. **Test** each component individually
3. **Integrate** with your existing ETL jobs
4. **Monitor** via CloudWatch and Streamlit dashboards
5. **Iterate** based on agent recommendations

For questions or issues, contact the ETL team.
