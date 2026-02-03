# Enterprise ETL Framework - Complete Baby Steps Guide

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [AWS Infrastructure Setup](#aws-infrastructure-setup)
4. [Slack Integration Setup](#slack-integration-setup)
5. [Streamlit Dashboard Setup](#streamlit-dashboard-setup)
6. [Email (SES) Setup](#email-ses-setup)
7. [CloudWatch Dashboard Setup](#cloudwatch-dashboard-setup)
8. [Testing Each Agent](#testing-each-agent)
9. [End-to-End Demo](#end-to-end-demo)
10. [Platform Migration Guide](#platform-migration-guide)

---

## Overview

This guide walks you through setting up the complete Enterprise ETL Framework with all integrations.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Enterprise ETL Framework                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                        AGENTS                                 │  │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐│  │
│  │  │Auto-Healing│ │ Compliance │ │Code Analysis│ │ Workload  ││  │
│  │  │   Agent    │ │   Agent    │ │   Agent    │ │ Assessment ││  │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘│  │
│  │  ┌────────────┐ ┌────────────┐                              │  │
│  │  │Data Quality│ │    EKS     │                              │  │
│  │  │   Agent    │ │ Optimizer  │                              │  │
│  │  └────────────┘ └────────────┘                              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                               │                                      │
│                               ▼                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                     INTEGRATIONS                              │  │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────┐ ┌─────────────────┐   │  │
│  │  │  Slack  │ │ Streamlit│ │  Email  │ │   CloudWatch    │   │  │
│  │  │   Bot   │ │Dashboard │ │  (SES)  │ │   Dashboard     │   │  │
│  │  └─────────┘ └──────────┘ └─────────┘ └─────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                               │                                      │
│                               ▼                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    AWS SERVICES                               │  │
│  │  Glue │ EMR │ EKS │ Lambda │ S3 │ Athena │ Step Functions   │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

### Required
- AWS Account with admin access
- Python 3.9+
- AWS CLI configured
- Docker (for EKS deployments)

### Install Dependencies
```bash
pip install boto3 streamlit requests slack-sdk flask
```

---

## AWS Infrastructure Setup

### Step 1: Create IAM Role for ETL Framework

```bash
# Create IAM policy
cat > etl-framework-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:*",
                "logs:*",
                "cloudwatch:*",
                "ses:SendEmail",
                "ses:SendRawEmail",
                "athena:*",
                "bedrock:InvokeModel",
                "emr:*",
                "emr-serverless:*",
                "eks:DescribeCluster",
                "secretsmanager:GetSecretValue",
                "sns:Publish",
                "lambda:InvokeFunction",
                "states:*"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# Create the policy
aws iam create-policy \
    --policy-name ETLFrameworkPolicy \
    --policy-document file://etl-framework-policy.json

# Create role and attach policy
aws iam create-role \
    --role-name ETLFrameworkRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": ["glue.amazonaws.com", "lambda.amazonaws.com", "ec2.amazonaws.com"]},
            "Action": "sts:AssumeRole"
        }]
    }'

aws iam attach-role-policy \
    --role-name ETLFrameworkRole \
    --policy-arn arn:aws:iam::YOUR_ACCOUNT:policy/ETLFrameworkPolicy
```

### Step 2: Create S3 Buckets

```bash
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1

# Create buckets
aws s3 mb s3://etl-framework-${AWS_ACCOUNT}-scripts --region ${REGION}
aws s3 mb s3://etl-framework-${AWS_ACCOUNT}-data --region ${REGION}
aws s3 mb s3://etl-framework-${AWS_ACCOUNT}-logs --region ${REGION}
```

### Step 3: Store Secrets in Secrets Manager

```bash
# Store Slack credentials
aws secretsmanager create-secret \
    --name etl-framework/slack \
    --secret-string '{
        "webhook_url": "YOUR_SLACK_WEBHOOK_URL",
        "bot_token": "YOUR_SLACK_BOT_TOKEN",
        "signing_secret": "YOUR_SLACK_SIGNING_SECRET"
    }'

# Store other credentials
aws secretsmanager create-secret \
    --name etl-framework/config \
    --secret-string '{
        "ses_sender_email": "noreply@yourdomain.com",
        "alert_recipients": ["team@yourdomain.com"]
    }'
```

---

## Slack Integration Setup

### Part A: Slack Side Setup

#### Step 1: Create Slack App

1. Go to https://api.slack.com/apps
2. Click **"Create New App"**
3. Choose **"From scratch"**
4. Enter App Name: `ETL Bot`
5. Select your workspace
6. Click **"Create App"**

#### Step 2: Configure Bot Permissions

1. In left sidebar, click **"OAuth & Permissions"**
2. Scroll to **"Scopes"** → **"Bot Token Scopes"**
3. Add these scopes:
   ```
   chat:write          - Send messages
   chat:write.public   - Send to public channels
   commands            - Slash commands
   files:read          - Read voice messages
   im:history          - Read DMs
   im:read             - Access DM info
   im:write            - Send DMs
   users:read          - Read user info
   ```

#### Step 3: Install App to Workspace

1. Scroll up to **"OAuth Tokens"**
2. Click **"Install to Workspace"**
3. Authorize the app
4. Copy the **"Bot User OAuth Token"** (starts with `xoxb-`)

#### Step 4: Enable Event Subscriptions

1. In left sidebar, click **"Event Subscriptions"**
2. Toggle **"Enable Events"** ON
3. For Request URL, enter: `https://your-api-gateway-url/slack/events`
   (We'll create this in AWS side)
4. Under **"Subscribe to bot events"**, add:
   ```
   message.im           - DM messages
   message.channels     - Channel messages
   app_mention          - @mentions
   ```
5. Click **"Save Changes"**

#### Step 5: Create Slash Commands (Optional)

1. In left sidebar, click **"Slash Commands"**
2. Click **"Create New Command"**
3. Create these commands:

| Command | Request URL | Description |
|---------|-------------|-------------|
| `/etl-run` | `https://your-api/slack/commands` | Run an ETL job |
| `/etl-status` | `https://your-api/slack/commands` | Check job status |
| `/etl-list` | `https://your-api/slack/commands` | List all jobs |

#### Step 6: Get Webhook URL (for notifications)

1. In left sidebar, click **"Incoming Webhooks"**
2. Toggle **"Activate Incoming Webhooks"** ON
3. Click **"Add New Webhook to Workspace"**
4. Select a channel (e.g., `#etl-alerts`)
5. Copy the **Webhook URL**

### Part B: AWS Side Setup

#### Step 1: Create Lambda Function for Slack Events

```python
# lambda_slack_handler.py
import json
import os
import boto3
import hmac
import hashlib
import time

# Add to Lambda layer or package
from integrations.slack_integration import SlackBot, SlackIntegration

secrets_client = boto3.client('secretsmanager')

def get_secrets():
    """Get Slack credentials from Secrets Manager."""
    response = secrets_client.get_secret_value(SecretId='etl-framework/slack')
    return json.loads(response['SecretString'])

def verify_slack_signature(event, secrets):
    """Verify request is from Slack."""
    timestamp = event['headers'].get('x-slack-request-timestamp', '')
    signature = event['headers'].get('x-slack-signature', '')
    body = event.get('body', '')

    # Check timestamp (prevent replay attacks)
    if abs(time.time() - int(timestamp)) > 60 * 5:
        return False

    # Verify signature
    sig_basestring = f"v0:{timestamp}:{body}"
    my_signature = 'v0=' + hmac.new(
        secrets['signing_secret'].encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(my_signature, signature)

def lambda_handler(event, context):
    """Handle Slack events."""
    secrets = get_secrets()

    # Parse body
    if isinstance(event.get('body'), str):
        body = json.loads(event['body'])
    else:
        body = event.get('body', {})

    # Handle URL verification challenge
    if body.get('type') == 'url_verification':
        return {
            'statusCode': 200,
            'body': body.get('challenge')
        }

    # Verify signature
    if not verify_slack_signature(event, secrets):
        return {'statusCode': 401, 'body': 'Invalid signature'}

    # Handle events
    if body.get('type') == 'event_callback':
        slack_event = body.get('event', {})

        # Ignore bot messages
        if slack_event.get('bot_id'):
            return {'statusCode': 200}

        # Initialize bot
        bot = SlackBot(
            slack_integration=SlackIntegration(
                webhook_url=secrets['webhook_url'],
                bot_token=secrets['bot_token']
            )
        )

        # Handle message
        text = slack_event.get('text', '')
        channel = slack_event.get('channel')

        result = bot.handle_text_command(text, channel)

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    return {'statusCode': 200}
```

#### Step 2: Deploy Lambda Function

```bash
# Create deployment package
cd /path/to/strands_etl
zip -r lambda-slack.zip integrations/ agents/ lambda_slack_handler.py

# Create Lambda function
aws lambda create-function \
    --function-name etl-slack-handler \
    --runtime python3.9 \
    --handler lambda_slack_handler.lambda_handler \
    --role arn:aws:iam::YOUR_ACCOUNT:role/ETLFrameworkRole \
    --zip-file fileb://lambda-slack.zip \
    --timeout 30 \
    --memory-size 256

# Add boto3 layer (if needed)
aws lambda update-function-configuration \
    --function-name etl-slack-handler \
    --layers arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-boto3:10
```

#### Step 3: Create API Gateway

```bash
# Create HTTP API
aws apigatewayv2 create-api \
    --name etl-slack-api \
    --protocol-type HTTP \
    --target arn:aws:lambda:us-east-1:YOUR_ACCOUNT:function:etl-slack-handler

# Get the API endpoint
aws apigatewayv2 get-apis --query "Items[?Name=='etl-slack-api'].ApiEndpoint" --output text
```

#### Step 4: Update Slack App with API URL

1. Go back to Slack App settings
2. Update **Event Subscriptions** Request URL with your API Gateway URL
3. Update **Slash Commands** Request URLs

#### Step 5: Test Slack Integration

```bash
# Test locally first
python -c "
from integrations import SlackBot
bot = SlackBot()
commands = ['run job test', 'status test', 'list jobs', 'help']
for cmd in commands:
    parsed = bot.parse_command(cmd)
    print(f'{cmd} -> {parsed.command_type.value if parsed else \"Not recognized\"}')
"
```

In Slack, try:
```
@ETL Bot run job customer_360_etl
@ETL Bot status customer_360_etl
@ETL Bot help
```

---

## Streamlit Dashboard Setup

### Option A: Run Locally (Development)

```bash
# Install Streamlit
pip install streamlit plotly pandas

# Run dashboard
cd /path/to/strands_etl
streamlit run integrations/streamlit_dashboard.py --server.port 8501

# Access at http://localhost:8501
```

### Option B: Deploy on EC2

#### Step 1: Launch EC2 Instance

```bash
# Launch t3.small (or t4g.small for Graviton)
aws ec2 run-instances \
    --image-id ami-0c55b159cbfafe1f0 \
    --instance-type t3.small \
    --key-name your-key \
    --security-group-ids sg-xxxxx \
    --subnet-id subnet-xxxxx \
    --iam-instance-profile Name=ETLFrameworkRole \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=etl-dashboard}]' \
    --user-data '#!/bin/bash
yum update -y
yum install python3-pip git -y
pip3 install streamlit boto3 plotly pandas
git clone https://github.com/your-repo/strands_etl.git /opt/etl
cd /opt/etl
nohup streamlit run integrations/streamlit_dashboard.py --server.port 80 --server.address 0.0.0.0 &
'
```

#### Step 2: Configure Security Group

```bash
# Allow HTTP access
aws ec2 authorize-security-group-ingress \
    --group-id sg-xxxxx \
    --protocol tcp \
    --port 80 \
    --cidr 0.0.0.0/0
```

### Option C: Deploy on ECS Fargate (Production)

#### Step 1: Create Dockerfile

```dockerfile
# Dockerfile.streamlit
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY integrations/ ./integrations/
COPY agents/ ./agents/
COPY dashboards/ ./dashboards/

EXPOSE 8501

CMD ["streamlit", "run", "integrations/streamlit_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

#### Step 2: Build and Push to ECR

```bash
# Create ECR repository
aws ecr create-repository --repository-name etl-dashboard

# Login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com

# Build and push
docker build -t etl-dashboard -f Dockerfile.streamlit .
docker tag etl-dashboard:latest YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/etl-dashboard:latest
docker push YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/etl-dashboard:latest
```

#### Step 3: Create ECS Task Definition

```json
{
    "family": "etl-dashboard",
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "512",
    "memory": "1024",
    "executionRoleArn": "arn:aws:iam::YOUR_ACCOUNT:role/ecsTaskExecutionRole",
    "taskRoleArn": "arn:aws:iam::YOUR_ACCOUNT:role/ETLFrameworkRole",
    "containerDefinitions": [
        {
            "name": "dashboard",
            "image": "YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/etl-dashboard:latest",
            "portMappings": [
                {
                    "containerPort": 8501,
                    "protocol": "tcp"
                }
            ],
            "environment": [
                {"name": "AWS_REGION", "value": "us-east-1"}
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "/ecs/etl-dashboard",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "ecs"
                }
            }
        }
    ]
}
```

#### Step 4: Create ECS Service with ALB

```bash
# Create ALB, target group, and ECS service
# (Use AWS Console or CloudFormation for easier setup)
```

---

## Email (SES) Setup

### Step 1: Verify Email Domain/Address

```bash
# Verify domain (recommended for production)
aws ses verify-domain-identity --domain yourdomain.com

# Or verify single email (for testing)
aws ses verify-email-identity --email-address noreply@yourdomain.com
```

### Step 2: Check Verification Status

```bash
aws ses get-identity-verification-attributes \
    --identities noreply@yourdomain.com
```

### Step 3: Move Out of Sandbox (Production)

1. Go to AWS Console → SES → Account Dashboard
2. Click **"Request production access"**
3. Fill out the form explaining your use case

### Step 4: Test Email Sending

```python
# test_email.py
from integrations import EmailReporter

reporter = EmailReporter(
    region='us-east-1',
    sender_email='noreply@yourdomain.com'
)

# Test job summary email
sample_jobs = [
    {'name': 'customer_360_etl', 'status': 'SUCCEEDED', 'duration': '15m', 'cost': 2.50, 'started': '2024-01-15 08:00'},
    {'name': 'orders_sync', 'status': 'FAILED', 'duration': '3m', 'cost': 0.50, 'started': '2024-01-15 09:00', 'error': 'OOM'}
]

# Send email
success = reporter.send_job_summary(
    jobs=sample_jobs,
    recipients=['your-email@example.com'],
    period='daily'
)

print(f"Email sent: {success}")
```

---

## CloudWatch Dashboard Setup

### Step 1: Create Dashboard

```python
# setup_cloudwatch.py
from dashboards import CloudWatchDashboard

dashboard = CloudWatchDashboard(region='us-east-1')

# Create comprehensive ETL dashboard
result = dashboard.create_etl_dashboard(
    dashboard_name='Enterprise-ETL-Dashboard',
    job_names=['customer_360_etl', 'orders_sync', 'inventory_update'],  # Your job names
    include_data_quality=True,
    include_cost=True
)

if result['success']:
    print(f"Dashboard created!")
    print(f"URL: {result['dashboard_url']}")
else:
    print(f"Error: {result.get('error')}")
```

### Step 2: Create Alarms

```python
# Create alarms for each job
for job_name in ['customer_360_etl', 'orders_sync']:
    alarms = dashboard.create_job_alarms(
        job_name=job_name,
        failure_threshold=1,
        duration_threshold_minutes=60,
        sns_topic_arn='arn:aws:sns:us-east-1:YOUR_ACCOUNT:etl-alerts'  # Optional
    )
    print(f"Created {len(alarms)} alarms for {job_name}")
```

### Step 3: Publish Custom Metrics

```python
# In your ETL job, publish data quality metrics
from dashboards import CloudWatchDashboard

cw = CloudWatchDashboard()

# After running data quality check
cw.publish_data_quality_metrics({
    'table_name': 'ecommerce_db.orders',
    'overall_score': 0.95,
    'passed_rules': 47,
    'failed_rules': 3,
    'results': [...]
})
```

---

## Testing Each Agent

### Test 1: Auto-Healing Agent

```python
# test_auto_healing.py
from agents import AutoHealingAgent

agent = AutoHealingAgent(region='us-east-1')

# Test error classification
test_errors = [
    "Container killed by YARN for exceeding memory limits",
    "Job timed out after 2880 seconds",
    "Shuffle fetch failed",
    "Data skew detected in partition 5"
]

for error in test_errors:
    error_type = agent.classify_error(error)
    can_heal = agent.can_heal(error_type)
    print(f"Error: {error[:50]}...")
    print(f"  Type: {error_type}")
    print(f"  Can Heal: {can_heal}")
    print()
```

### Test 2: Workload Assessment Agent

```python
# test_workload.py
from agents import WorkloadAssessmentAgent

agent = WorkloadAssessmentAgent(region='us-east-1')

config = {
    'workload': {
        'name': 'test_etl',
        'criticality': 'medium',
        'schedule_type': 'daily'
    },
    'data_sources': [
        {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'orders'}
    ]
}

assessment = agent.assess_workload(config)
print(agent.generate_assessment_report(assessment))
```

### Test 3: Code Analysis Agent

```python
# test_code_analysis.py
from agents import CodeAnalysisAgent

agent = CodeAnalysisAgent(region='us-east-1')

sample_code = '''
df = spark.table("db.large_table")
result = df.collect()  # Anti-pattern!
df2 = df.join(small_df, "key")  # Missing broadcast
'''

analysis = agent.analyze_code(sample_code)
print(f"Issues found: {len(analysis.get('issues', []))}")
for issue in analysis.get('issues', []):
    print(f"  - {issue}")
```

### Test 4: EKS Optimizer

```python
# test_eks.py
from agents import EKSOptimizer

optimizer = EKSOptimizer()

# Analyze your current t3.large setup
result = optimizer.estimate_t_series_migration(
    current_instance="t3.large",
    current_capacity="on-demand",
    hours_per_day=8,
    instance_count=1
)

print(f"Current monthly cost: ${result['current']['monthly_cost']}")
print(f"Best option: {result['best_option']['name']}")
print(f"Monthly savings: ${result['max_savings']['monthly']}")
print(f"Yearly savings: ${result['max_savings']['yearly']}")
```

---

## End-to-End Demo

### Complete Demo Script

```python
# demo_complete.py
"""
Complete Enterprise ETL Framework Demo
Run this to test all components end-to-end
"""

import json
from datetime import datetime

from agents import (
    AutoHealingAgent,
    ComplianceAgent,
    CodeAnalysisAgent,
    WorkloadAssessmentAgent,
    DataQualityAgent,
    EKSOptimizer
)
from integrations import SlackIntegration, EmailReporter
from dashboards import CloudWatchDashboard
from scripts.glue_script_converter import GlueScriptConverter

def run_complete_demo():
    print("=" * 70)
    print("ENTERPRISE ETL FRAMEWORK - COMPLETE DEMO")
    print("=" * 70)
    print(f"Started at: {datetime.now()}")
    print()

    # Configuration
    config = {
        'workload': {
            'name': 'customer_360_etl',
            'criticality': 'medium',
            'schedule_type': 'daily'
        },
        'data_sources': [
            {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'orders'},
            {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'customers'}
        ],
        'data_quality': {
            'enabled': True,
            'rules': [
                {
                    'id': 'orders_not_null',
                    'name': 'Order ID Not Null',
                    'type': 'completeness',
                    'table': 'ecommerce_db.orders',
                    'column': 'order_id',
                    'threshold': 1.0,
                    'severity': 'error'
                }
            ]
        }
    }

    sample_code = '''
orders_df = spark.table("ecommerce_db.orders")
customers_df = spark.table("ecommerce_db.customers")
result = orders_df.join(customers_df, "customer_id")
result = result.groupBy("segment").agg({"total_amount": "sum"})
result.write.mode("overwrite").parquet("s3://bucket/output/")
'''

    # 1. WORKLOAD ASSESSMENT
    print("1. WORKLOAD ASSESSMENT")
    print("-" * 50)
    workload_agent = WorkloadAssessmentAgent()
    assessment = workload_agent.assess_workload(config, code=sample_code)
    print(f"   Category: {assessment.category.value}")
    print(f"   Recommended Platform: {assessment.recommended_platform.value}")
    print(f"   Flex Mode Suitable: {assessment.flex_mode_suitable}")
    print(f"   Karpenter Suitable: {assessment.karpenter_suitable}")
    print()

    # 2. CODE ANALYSIS
    print("2. CODE ANALYSIS")
    print("-" * 50)
    code_agent = CodeAnalysisAgent()
    analysis = code_agent.analyze_code(sample_code)
    print(f"   Issues Found: {len(analysis.get('issues', []))}")
    print(f"   Recommendations: {len(analysis.get('recommendations', []))}")
    for rec in analysis.get('recommendations', [])[:3]:
        print(f"   - {rec}")
    print()

    # 3. EKS COST OPTIMIZATION
    print("3. EKS COST OPTIMIZATION")
    print("-" * 50)
    eks_optimizer = EKSOptimizer()
    migration = eks_optimizer.estimate_t_series_migration(
        current_instance="t3.large",
        current_capacity="on-demand",
        hours_per_day=8
    )
    print(f"   Current Cost: ${migration['current']['monthly_cost']}/month")
    print(f"   Best Option: {migration['best_option']['name']}")
    print(f"   Potential Savings: ${migration['max_savings']['monthly']}/month ({migration['max_savings']['percentage']}%)")
    print()

    # 4. PLATFORM MIGRATION
    print("4. PLATFORM MIGRATION (if needed)")
    print("-" * 50)
    if assessment.recommended_platform.value in ['emr_serverless', 'eks_karpenter', 'emr_cluster']:
        converter = GlueScriptConverter()
        converted = converter.convert(sample_code, target='eks')
        print(f"   Changes needed: {len(converted.changes_made)}")
        for change in converted.changes_made[:3]:
            print(f"   - {change}")
    else:
        print("   No migration needed - Glue recommended")
    print()

    # 5. DATA QUALITY
    print("5. DATA QUALITY RULES")
    print("-" * 50)
    dq_agent = DataQualityAgent()
    rules = dq_agent.parse_rules_from_config(config)
    print(f"   Rules Parsed: {len(rules)}")
    print(f"   PySpark validation code generated")
    print()

    # 6. COMPLIANCE CHECK
    print("6. COMPLIANCE CHECK")
    print("-" * 50)
    compliance_agent = ComplianceAgent()
    # Test PII detection
    pii = compliance_agent.detect_pii_in_column_name('customer_email')
    print(f"   PII detected in 'customer_email': {[p.value for p in pii]}")
    print()

    # 7. AUTO-HEALING CAPABILITIES
    print("7. AUTO-HEALING CAPABILITIES")
    print("-" * 50)
    healing_agent = AutoHealingAgent()
    test_error = "Container killed by YARN for exceeding memory limits"
    error_type = healing_agent.classify_error(test_error)
    can_heal = healing_agent.can_heal(error_type)
    print(f"   Test Error: OOM")
    print(f"   Can Auto-Heal: {can_heal}")
    print(f"   Strategy: Increase memory, switch to G.2X")
    print()

    print("=" * 70)
    print("DEMO COMPLETE!")
    print("=" * 70)
    print()
    print("Next Steps:")
    print("1. Configure Slack integration (see SLACK_SETUP.md)")
    print("2. Deploy Streamlit dashboard")
    print("3. Set up CloudWatch alarms")
    print("4. Run with your actual Glue Catalog tables")

if __name__ == '__main__':
    run_complete_demo()
```

Run the demo:
```bash
python demo_complete.py
```

---

## Platform Migration Guide

When the framework recommends a different platform (EMR, EKS), use this guide:

### Glue → EMR Migration

```python
from scripts.glue_script_converter import GlueScriptConverter

converter = GlueScriptConverter()

# Read your Glue script
with open('my_glue_job.py', 'r') as f:
    glue_code = f.read()

# Convert for EMR
result = converter.convert(glue_code, target='emr')

# Save converted script
with open('my_emr_job.py', 'w') as f:
    f.write(result.converted_code)

# Get spark-submit command
print(converter.generate_spark_submit_command(
    's3://bucket/scripts/my_emr_job.py',
    result.spark_conf,
    target='emr'
))
```

### Glue → EKS Migration

```python
# Convert for EKS
result = converter.convert(glue_code, target='eks')

# Generate Kubernetes manifest
yaml = converter.generate_spark_application_yaml(
    'my-etl-job',
    's3://bucket/scripts/my_eks_job.py',
    result.spark_conf
)

# Save manifest
with open('spark-application.yaml', 'w') as f:
    f.write(yaml)

# Deploy
# kubectl apply -f spark-application.yaml
```

---

## Quick Reference

### Environment Variables

```bash
export AWS_REGION=us-east-1
export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
export SLACK_BOT_TOKEN=xoxb-...
export SES_SENDER_EMAIL=noreply@yourdomain.com
```

### Useful Commands

```bash
# Run Streamlit dashboard
streamlit run integrations/streamlit_dashboard.py

# Test Slack bot commands
python -c "from integrations import SlackBot; bot = SlackBot(); print(bot.parse_command('run job test'))"

# Convert Glue script
python scripts/glue_script_converter.py < my_glue_job.py

# Check EKS cost savings
python -c "from agents import EKSOptimizer; print(EKSOptimizer().estimate_t_series_migration('t3.large', 'on-demand', 8))"
```

### Support

For issues, check:
1. CloudWatch Logs
2. Streamlit dashboard → Agent Insights tab
3. Slack bot: `@ETL Bot help`
