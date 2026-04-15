# Enterprise ETL Framework - Complete Guide

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites & Provisioning](#prerequisites--provisioning)
3. [Configuration Guide](#configuration-guide)
4. [Agent Manual Testing](#agent-manual-testing)
5. [Component Testing](#component-testing)
6. [E2E Demo Steps](#e2e-demo-steps)
7. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ETL Framework - Enterprise Edition                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ Config-Driven│  │   Platform   │  │ Auto-Healing │  │   Learning   │    │
│  │   Execution  │──│   Fallback   │──│    Agent     │──│    Agent     │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│         │                │                  │                 │             │
│  ┌──────┴──────────────┴──────────────────┴─────────────────┴──────┐      │
│  │                     Core Framework Engine                         │      │
│  └──────┬──────────────┬──────────────────┬─────────────────┬──────┘      │
│         │              │                  │                 │             │
│  ┌──────┴────┐  ┌──────┴────┐  ┌─────────┴───┐  ┌─────────┴────┐        │
│  │   Code    │  │   Data    │  │ Compliance  │  │  Workload    │        │
│  │ Analysis  │  │  Quality  │  │   Agent     │  │  Assessment  │        │
│  └───────────┘  └───────────┘  └─────────────┘  └──────────────┘        │
│         │              │                  │                 │             │
│  ┌──────┴──────────────┴──────────────────┴─────────────────┴──────┐      │
│  │                    Integrations Layer                             │      │
│  │  ┌────────┐ ┌────────┐ ┌──────────┐ ┌───────┐ ┌──────────────┐  │      │
│  │  │ Slack  │ │ Teams  │ │Streamlit │ │ Email │ │  CloudWatch  │  │      │
│  │  │ +Voice │ │        │ │Dashboard │ │ HTML  │ │  Dashboard   │  │      │
│  │  └────────┘ └────────┘ └──────────┘ └───────┘ └──────────────┘  │      │
│  └───────────────────────────────────────────────────────────────────┘      │
│         │                                                                   │
│  ┌──────┴───────────────────────────────────────────────────────────┐      │
│  │                     DynamoDB Audit Layer                          │      │
│  │  [Start] → [Read] → [Transform] → [DQ] → [Compliance] → [Write]  │      │
│  └───────────────────────────────────────────────────────────────────┘      │
│                                                                              │
│  Platforms: Glue ─(fallback)→ EMR ─(fallback)→ EKS (with Karpenter)        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Features

| Feature | Description | Config Flag |
|---------|-------------|-------------|
| Platform Fallback | Auto-switch from Glue→EMR→EKS on failure | `platform.auto_fallback_on_error` |
| Auto-Healing | Analyze errors, fix code, rerun | `auto_healing.enabled` |
| Code Analysis | PySpark optimization recommendations | `code_analysis.enabled` |
| Data Quality | NL, SQL, Template-based rules | `data_quality.enabled` |
| Compliance | GDPR, HIPAA, PCI-DSS, SOX checks | `compliance.enabled` |
| Workload Assessment | Resource recommendations | `workload_assessment.enabled` |
| Learning Agent | Historical analysis & predictions | `learning.enabled` |
| Slack with Voice | Trigger ETL via voice commands | `integrations.slack_voice_enabled` |
| Enterprise Dashboard | Comprehensive monitoring | `dashboard.enterprise_summary_enabled` |

---

## Prerequisites & Provisioning

### Step 1: AWS Account Setup

```bash
# 1.1 Set your AWS credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1

# 1.2 Verify access
aws sts get-caller-identity
```

### Step 2: Create IAM Roles and Policies

```bash
# 2.1 Create the ETL Framework execution role
aws iam create-role \
  --role-name ETLFrameworkExecutionRole \
  --assume-role-policy-document file://iam/roles/etl_framework_execution_role.json

# 2.2 Attach Glue policy
aws iam put-role-policy \
  --role-name ETLFrameworkExecutionRole \
  --policy-name ETLFrameworkGluePolicy \
  --policy-document file://iam/policies/etl_framework_glue_policy.json

# 2.3 Attach EMR policy
aws iam put-role-policy \
  --role-name ETLFrameworkExecutionRole \
  --policy-name ETLFrameworkEMRPolicy \
  --policy-document file://iam/policies/etl_framework_emr_policy.json

# 2.4 Attach DynamoDB policy
aws iam put-role-policy \
  --role-name ETLFrameworkExecutionRole \
  --policy-name ETLFrameworkDynamoDBPolicy \
  --policy-document file://iam/policies/etl_framework_dynamodb_policy.json

# 2.5 Attach Slack/Teams/Email policy
aws iam put-role-policy \
  --role-name ETLFrameworkExecutionRole \
  --policy-name ETLFrameworkIntegrationsPolicy \
  --policy-document file://iam/policies/etl_framework_slack_teams_policy.json
```

### Step 3: Create DynamoDB Tables

```bash
# 3.1 Create audit log table
aws dynamodb create-table \
  --table-name etl_audit_log \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=timestamp,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

# 3.2 Create execution history table
aws dynamodb create-table \
  --table-name etl_execution_history \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=timestamp,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

# 3.3 Create job baselines table
aws dynamodb create-table \
  --table-name etl_job_baselines \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# 3.4 Create recommendations table
aws dynamodb create-table \
  --table-name etl_recommendations \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=recommendation_id,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=recommendation_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
```

### Step 4: Create S3 Buckets

```bash
# 4.1 Create data bucket
aws s3 mb s3://etl-framework-data-${AWS_ACCOUNT_ID}

# 4.2 Create scripts bucket
aws s3 mb s3://etl-framework-scripts-${AWS_ACCOUNT_ID}

# 4.3 Upload PySpark scripts
aws s3 sync scripts/pyspark/ s3://etl-framework-scripts-${AWS_ACCOUNT_ID}/pyspark/
```

### Step 5: Setup Slack Integration

```bash
# 5.1 Create Slack app at https://api.slack.com/apps
# 5.2 Add Bot Token Scopes: chat:write, channels:read, app_mentions:read
# 5.3 Store tokens in Secrets Manager

aws secretsmanager create-secret \
  --name etl-framework/slack/bot-token \
  --secret-string '{"SLACK_BOT_TOKEN":"xoxb-your-token"}'

aws secretsmanager create-secret \
  --name etl-framework/slack/signing-secret \
  --secret-string '{"SLACK_SIGNING_SECRET":"your-signing-secret"}'
```

### Step 6: Setup Teams Integration

```bash
# 6.1 Create Teams Incoming Webhook
# 6.2 Store webhook URL in Secrets Manager

aws secretsmanager create-secret \
  --name etl-framework/teams/webhook-url \
  --secret-string '{"TEAMS_WEBHOOK_URL":"https://outlook.office.com/webhook/..."}'
```

### Step 7: Setup SES for Email

```bash
# 7.1 Verify sender email
aws ses verify-email-identity --email-address etl-notifications@yourdomain.com

# 7.2 Verify recipient domain (for production)
aws ses verify-domain-identity --domain yourdomain.com
```

### Step 8: Create Glue Database and Catalog

```bash
# 8.1 Create databases
aws glue create-database --database-input '{"Name":"raw_data"}'
aws glue create-database --database-input '{"Name":"processed_data"}'
aws glue create-database --database-input '{"Name":"analytics"}'
aws glue create-database --database-input '{"Name":"master_data"}'

# 8.2 Create sample table
aws glue create-table \
  --database-name raw_data \
  --table-input '{
    "Name": "customers",
    "StorageDescriptor": {
      "Columns": [
        {"Name": "customer_id", "Type": "string"},
        {"Name": "customer_name", "Type": "string"},
        {"Name": "email", "Type": "string"},
        {"Name": "phone", "Type": "string"},
        {"Name": "address", "Type": "string"},
        {"Name": "age", "Type": "int"},
        {"Name": "status", "Type": "string"},
        {"Name": "country", "Type": "string"}
      ],
      "Location": "s3://etl-framework-data/raw/customers/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
    }
  }'
```

---

## Configuration Guide

### Master Configuration Schema

All features are controlled via JSON configuration. Here's how to structure your config:

```json
{
  "job_name": "your_job_name",

  "platform": {
    "primary": "glue|emr|eks",
    "fallback_chain": ["emr", "eks"],
    "force_platform": null,
    "auto_fallback_on_error": "Y"
  },

  "auto_healing": {
    "enabled": "Y|N",
    "correct_code": "Y|N",
    "auto_rerun": "Y|N",
    "max_retries": 3,
    "heal_memory_errors": "Y|N",
    "heal_shuffle_errors": "Y|N"
  },

  "data_quality": {
    "enabled": "Y|N",
    "natural_language_rules": [
      "column_name should not be null",
      "column_name should be unique"
    ],
    "sql_rules": [
      {"id": "SQL_001", "sql": "SELECT COUNT(*) FROM {table} WHERE ...", "threshold": 0}
    ]
  },

  "compliance": {
    "enabled": "Y|N",
    "check_sources": "Y|N",
    "check_targets": "Y|N",
    "frameworks": ["GDPR", "HIPAA", "PCI_DSS"]
  },

  "integrations": {
    "slack_enabled": "Y|N",
    "slack_voice_enabled": "Y|N",
    "teams_enabled": "Y|N",
    "email_enabled": "Y|N",
    "streamlit_enabled": "Y|N"
  },

  "audit": {
    "enabled": "Y|N",
    "audit_on_start": "Y|N",
    "audit_on_read": "Y|N",
    "audit_on_transform": "Y|N",
    "audit_on_write": "Y|N"
  }
}
```

---

## Agent Manual Testing

### Testing Auto-Healing Agent

```python
# test_auto_healing.py
import sys
sys.path.insert(0, '.')

from framework.agents.auto_healing_agent import AutoHealingAgent, HealingStrategy

class MockConfig:
    heal_memory_errors = True
    heal_shuffle_errors = True
    heal_timeout_errors = True
    heal_connection_errors = True
    heal_data_skew = True
    heal_partition_errors = True

# Initialize agent
config = MockConfig()
agent = AutoHealingAgent(config)

# Test 1: Memory error healing
print("="*50)
print("TEST 1: Memory Error Healing")
print("="*50)
error = Exception("java.lang.OutOfMemoryError: GC overhead limit exceeded")
result = agent.analyze_and_heal(error, config)

print(f"Can heal: {result.can_heal}")
print(f"Strategy: {result.strategy}")
print(f"Strategies: {result.strategies}")
print(f"Config changes: {result.config_changes}")
print(f"Recommendations: {result.recommendations}")

# Test 2: Shuffle error healing
print("\n" + "="*50)
print("TEST 2: Shuffle Error Healing")
print("="*50)
error = Exception("FetchFailedException: Failed to connect to executor")
result = agent.analyze_and_heal(error, config)

print(f"Can heal: {result.can_heal}")
print(f"Strategy: {result.strategy}")
print(f"Code fixes: {len(result.code_fixes)}")

# Test 3: Code fix generation
print("\n" + "="*50)
print("TEST 3: Code Fix Generation")
print("="*50)
original_code = '''
spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.read.parquet("s3://bucket/data/")
result = df.groupBy("key").agg(sum("value"))
result.write.parquet("s3://bucket/output/")
'''

fixed_code = agent.generate_fixed_code(original_code, result.code_fixes)
print("Fixed code generated")
print(fixed_code[:500])

print("\nAuto-Healing Agent tests completed!")
```

**Run the test:**
```bash
python tests/component_tests/test_auto_healing.py
```

**Expected output:**
```
TEST 1: Memory Error Healing
Can heal: True
Strategy: HealingStrategy.INCREASE_MEMORY
Strategies: [<HealingStrategy.INCREASE_MEMORY>, <HealingStrategy.ADD_CACHING>]
Config changes: {'spark.executor.memory': 'increase_50_pct', ...}
```

---

### Testing Code Analysis Agent

```python
# test_code_analysis.py
import sys
sys.path.insert(0, '.')

from framework.agents.code_analysis_agent import CodeAnalysisAgent

class MockConfig:
    check_anti_patterns = True
    check_join_optimizations = True
    recommend_aws_tools = True
    recommend_delta_optimizations = True

config = MockConfig()
agent = CodeAnalysisAgent(config)

# Test code with anti-patterns
test_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()

# Anti-pattern: UDF usage
@udf
def my_upper(s):
    return s.upper() if s else None

# Anti-pattern: collect on large data
df = spark.read.parquet("s3://bucket/large-data/")
all_data = df.collect()

# Anti-pattern: multiple withColumn
df = df.withColumn("a", col("x") + 1)
df = df.withColumn("b", col("y") * 2)
df = df.withColumn("c", upper(col("z")))

# Missing broadcast hint
result = large_df.join(small_df, "key")
'''

print("="*50)
print("Testing Code Analysis Agent")
print("="*50)

result = agent.analyze(test_code, "test_job")

print(f"\nOptimization Score: {result.optimization_score}/100")
print(f"Anti-patterns found: {len(result.anti_patterns_found)}")
print(f"Total recommendations: {len(result.recommendations)}")

print("\nAnti-patterns detected:")
for ap in result.anti_patterns_found:
    print(f"  - {ap['name']}: {ap['title']} (Line {ap.get('line_number', 'N/A')})")

print("\nTop recommendations:")
for rec in result.recommendations[:5]:
    print(f"  [{rec.severity.value}] {rec.title}")
    print(f"    {rec.description[:100]}...")

print("\nCode Analysis Agent tests completed!")
```

**Run the test:**
```bash
python tests/component_tests/test_code_analysis.py
```

---

### Testing Data Quality Agent

```python
# test_data_quality.py
import sys
sys.path.insert(0, '.')

from framework.agents.data_quality_agent import DataQualityAgent

class MockConfig:
    pass

config = MockConfig()
agent = DataQualityAgent(config)

# Test Natural Language rule parsing
print("="*50)
print("Testing DQ Natural Language Rules")
print("="*50)

nl_rules = [
    "customer_id should not be null",
    "email should be valid email",
    "age should be between 0 and 150",
    "order_id should be unique",
    "customer_name is required"
]

for nl_rule in nl_rules:
    parsed = agent.parse_natural_language_rule(nl_rule, "test_table")
    if parsed:
        print(f"\nRule: '{nl_rule}'")
        print(f"  Type: {parsed.rule_type.value}")
        print(f"  Column: {parsed.column}")
        print(f"  SQL: {parsed.sql_expression[:80]}...")
    else:
        print(f"\nRule: '{nl_rule}' - NOT PARSED")

# Test SQL rule parsing
print("\n" + "="*50)
print("Testing DQ SQL Rules")
print("="*50)

sql_rule = agent.parse_sql_rule(
    "SELECT COUNT(*) FROM orders WHERE total < 0",
    "SQL_NEG_TOTAL",
    "Check for negative order totals"
)
print(f"SQL Rule ID: {sql_rule.rule_id}")
print(f"SQL: {sql_rule.sql_expression}")

# Test Spark code generation
print("\n" + "="*50)
print("Testing Spark Code Generation")
print("="*50)

parsed_rule = agent.parse_natural_language_rule("customer_id should not be null", "customers")
spark_code = agent.generate_spark_check_code(parsed_rule)
print("Generated Spark code:")
print(spark_code)

print("\nData Quality Agent tests completed!")
```

---

### Testing Compliance Agent

```python
# test_compliance.py
import sys
sys.path.insert(0, '.')

from framework.agents.compliance_agent import ComplianceAgent, ComplianceFramework

class MockConfig:
    check_sources = True
    check_targets = True
    frameworks = ["gdpr", "pci_dss"]
    pii_columns = ["email", "ssn"]
    mask_pii = True

config = MockConfig()
agent = ComplianceAgent(config)

# Test PII detection
print("="*50)
print("Testing PII Detection")
print("="*50)

test_schema = {
    "columns": [
        {"name": "customer_id", "type": "string"},
        {"name": "customer_email", "type": "string"},
        {"name": "phone_number", "type": "string"},
        {"name": "ssn", "type": "string"},
        {"name": "credit_card_number", "type": "string"},
        {"name": "order_total", "type": "double"}
    ]
}

result = agent.analyze_compliance(test_schema, "test_customers", is_source=True)

print(f"\nCompliance Status: {result.status.value}")
print(f"PII Findings: {len(result.pii_findings)}")
print(f"Violations: {len(result.violations)}")

print("\nPII Detected:")
for finding in result.pii_findings:
    print(f"  - {finding.column_name}: {finding.pii_type.value} (Confidence: {finding.confidence:.0%})")

print("\nViolations:")
for violation in result.violations:
    print(f"  - [{violation.severity}] {violation.rule_id}: {violation.rule_description}")

# Generate masking recommendations
if result.recommendations:
    print("\nMasking Recommendations:")
    for rec in result.recommendations[:5]:
        print(f"  {rec[:100]}...")

# Generate report
print("\n" + "="*50)
print("Generating Compliance Report")
print("="*50)
report = agent.generate_compliance_report(result, "test_customers")
print(report[:1000])

print("\nCompliance Agent tests completed!")
```

---

### Testing Workload Assessment Agent

```python
# test_workload_assessment.py
import sys
sys.path.insert(0, '.')

from framework.agents.workload_assessment_agent import WorkloadAssessmentAgent

class MockConfig:
    analyze_input_size = True
    consider_weekday_weekend = True
    use_historical_trends = True
    monitor_cpu_load = True
    monitor_memory_profile = True
    detect_data_skew = True
    use_karpenter = True
    use_spot = True
    use_graviton = True

config = MockConfig()
agent = WorkloadAssessmentAgent(config)

# Test workload assessment
print("="*50)
print("Testing Workload Assessment")
print("="*50)

source_tables = [
    {
        "name": "orders",
        "size_bytes": 50 * 1024**3,  # 50 GB
        "row_count": 100_000_000,
        "partitions": [
            {"size_bytes": 5 * 1024**3},
            {"size_bytes": 50 * 1024**3},  # Skewed partition
            {"size_bytes": 3 * 1024**3}
        ],
        "partition_column": "order_date"
    },
    {
        "name": "customers",
        "size_bytes": 5 * 1024**3,  # 5 GB
        "row_count": 10_000_000
    }
]

test_code = '''
df = spark.read.parquet("s3://bucket/orders/")
result = df.join(customers, "customer_id").groupBy("region").agg(sum("total"))
result.repartition(100).write.parquet("output/")
'''

historical_runs = [
    {"duration_seconds": 1800, "cost": 25.0, "status": "SUCCEEDED", "cpu_utilization": 70, "memory_utilization": 65},
    {"duration_seconds": 1900, "cost": 28.0, "status": "SUCCEEDED", "cpu_utilization": 75, "memory_utilization": 70},
    {"duration_seconds": 2100, "cost": 30.0, "status": "FAILED", "cpu_utilization": 90, "memory_utilization": 95}
]

from datetime import datetime
assessment = agent.assess_workload(
    source_tables=source_tables,
    code=test_code,
    historical_runs=historical_runs,
    current_day=datetime.now()
)

print(f"\nComplexity: {assessment.complexity.value}")
print(f"Data Volume: {assessment.data_volume.total_bytes / (1024**3):.1f} GB")
print(f"Skew Detected: {assessment.skew_detected}")
print(f"Skew Columns: {assessment.skew_columns}")

print("\nPrimary Recommendation:")
rec = assessment.primary_recommendation
if rec:
    print(f"  Platform: {rec.platform.value}")
    print(f"  Worker Type: {rec.worker_type.value}")
    print(f"  Workers: {rec.num_workers}")
    print(f"  Executor Memory: {rec.executor_memory}")
    print(f"  Estimated Duration: {rec.estimated_duration_minutes} min")
    print(f"  Estimated Cost: ${rec.estimated_cost:.2f}")
    print(f"  Confidence: {rec.confidence:.0%}")

print("\nWarnings:")
for warning in assessment.warnings:
    print(f"  ⚠️ {warning}")

print("\nOptimizations:")
for opt in assessment.optimization_opportunities:
    print(f"  💡 {opt}")

# Generate report
print("\n" + "="*50)
print("Generating Assessment Report")
print("="*50)
report = agent.generate_assessment_report(assessment)
print(report[:1500])

print("\nWorkload Assessment Agent tests completed!")
```

---

### Testing Learning Agent

```python
# test_learning.py
import sys
sys.path.insert(0, '.')

from framework.agents.learning_agent import LearningAgent

class MockConfig:
    history_table = "etl_execution_history"
    baseline_table = "etl_job_baselines"

config = MockConfig()
agent = LearningAgent(config, dynamodb_client=None)  # No DynamoDB for testing

# Test learning from execution
print("="*50)
print("Testing Learning Agent")
print("="*50)

# Simulate execution metrics
execution_metrics = {
    "execution_id": "exec_001",
    "status": "SUCCEEDED",
    "duration_seconds": 1800,
    "cost": 25.50,
    "input_bytes": 50 * 1024**3,
    "output_bytes": 10 * 1024**3,
    "shuffle_bytes": 20 * 1024**3,
    "memory_utilization": 75,
    "cpu_utilization": 65,
    "platform": "glue",
    "worker_type": "G.2X",
    "num_workers": 10
}

result = agent.learn_from_execution("test_job", execution_metrics, store_history=False)

print(f"\nBaseline updated:")
if result.baseline:
    print(f"  Sample count: {result.baseline.sample_count}")
    print(f"  Avg duration: {result.baseline.avg_duration_seconds:.0f}s")
    print(f"  Avg cost: ${result.baseline.avg_cost:.2f}")
    print(f"  Success rate: {result.baseline.success_rate:.0%}")

# Test anomaly detection
print("\n" + "="*50)
print("Testing Anomaly Detection")
print("="*50)

# Create baseline first
for i in range(10):
    metrics = {
        "duration_seconds": 1800 + (i * 50),
        "cost": 25 + (i * 0.5),
        "memory_utilization": 70 + i,
        "status": "SUCCEEDED"
    }
    result = agent.learn_from_execution("test_job", metrics, store_history=False)

# Now test with anomalous execution
anomalous_metrics = {
    "duration_seconds": 5000,  # Much longer than normal
    "cost": 100,  # Much more expensive
    "memory_utilization": 98,  # Very high memory
    "status": "SUCCEEDED"
}

result = agent.learn_from_execution("test_job", anomalous_metrics, store_history=False)

print(f"\nAnomalies detected: {len(result.anomalies)}")
for anomaly in result.anomalies:
    print(f"  - {anomaly.anomaly_type.value}: {anomaly.description}")

print("\nLearning Agent tests completed!")
```

---

### Testing Slack Integration

```python
# test_slack.py
import sys
sys.path.insert(0, '.')

from framework.integrations.slack_integration import SlackIntegration, MessageType, VoiceCommandType

class MockConfig:
    slack_bot_token = None  # Set to test with real Slack
    slack_signing_secret = None
    slack_channel_id = "#test-channel"

config = MockConfig()
slack = SlackIntegration(config)

# Test voice command parsing
print("="*50)
print("Testing Voice Command Parsing")
print("="*50)

test_commands = [
    "run the job sales_etl",
    "trigger complex_pipeline",
    "what's the status of daily_report",
    "show me metrics for customer_analytics",
    "cancel the job failing_etl",
    "approve the deployment",
    "kick off data_quality_check"
]

for cmd in test_commands:
    result = slack.parse_voice_command(cmd, user_id="U123", channel_id="C456")
    if result:
        print(f"\n'{cmd}'")
        print(f"  → Command: {result.command_type.value}")
        print(f"  → Job: {result.job_name}")
        print(f"  → Confidence: {result.confidence:.0%}")
    else:
        print(f"\n'{cmd}' → NOT RECOGNIZED")

# Test notification building (without sending)
print("\n" + "="*50)
print("Testing Notification Building")
print("="*50)

# This would send if bot_token was set
result = slack.send_job_completed(
    job_name="test_etl_job",
    metrics={
        "duration_seconds": 1800,
        "records_processed": 1_000_000,
        "cost": 25.50,
        "output_path": "s3://bucket/output/"
    },
    channel="#test"
)

if not config.slack_bot_token:
    print("Slack notifications disabled (no bot token)")
    print("Set SLACK_BOT_TOKEN to enable real testing")

print("\nSlack Integration tests completed!")
```

---

## Component Testing

### Running All Component Tests

```bash
# Create test directory
mkdir -p tests/component_tests

# Run individual tests
python tests/component_tests/test_auto_healing.py
python tests/component_tests/test_code_analysis.py
python tests/component_tests/test_data_quality.py
python tests/component_tests/test_compliance.py
python tests/component_tests/test_workload_assessment.py
python tests/component_tests/test_learning.py
python tests/component_tests/test_slack.py

# Or run all tests
python -m pytest tests/component_tests/ -v
```

---

## E2E Demo Steps

### Demo 1: Simple ETL with All Features

```bash
# Step 1: Upload config and script
aws s3 cp test_configs/simple_etl_config.json s3://etl-framework-scripts/configs/
aws s3 cp scripts/pyspark/simple_customer_etl.py s3://etl-framework-scripts/pyspark/

# Step 2: Create Glue job
aws glue create-job \
  --name etl-simple-customer \
  --role ETLFrameworkExecutionRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://etl-framework-scripts/pyspark/simple_customer_etl.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--source_database": "raw_data",
    "--source_table": "customers",
    "--target_database": "processed_data",
    "--target_table": "customers_cleaned"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X"

# Step 3: Run the job
aws glue start-job-run --job-name etl-simple-customer

# Step 4: Monitor in CloudWatch
aws logs tail /aws-glue/jobs/etl-simple-customer --follow
```

### Demo 2: Complex Pipeline with Platform Fallback

```bash
# Step 1: Upload config
aws s3 cp test_configs/complex_pipeline_config.json s3://etl-framework-scripts/configs/

# Step 2: Trigger via Slack (if configured)
# In Slack: "run complex_sales_analytics_pipeline"

# Step 3: Or trigger via CLI
python -c "
from framework.core.engine import FrameworkEngine
from framework.core.config_schema import MasterConfig
import json

with open('test_configs/complex_pipeline_config.json') as f:
    config_dict = json.load(f)

config = MasterConfig.from_dict(config_dict)
engine = FrameworkEngine(config)

# This will use platform fallback if primary fails
with engine.execute() as ctx:
    print('Job executing...')
"

# Step 4: View enterprise dashboard
python -c "
from framework.dashboard.enterprise_dashboard import EnterpriseDashboard, DashboardData

dashboard = EnterpriseDashboard(type('Config', (), {'dashboard_output_dir': './output'})())
data = DashboardData(
    job_history=[
        {'name': 'complex_sales', 'status': 'success', 'duration_seconds': 3600, 'cost': 45.00}
    ]
)
path = dashboard.save_dashboard(data, 'demo_dashboard.html')
print(f'Dashboard saved to: {path}')
"

# Step 5: Open dashboard in browser
open output/demo_dashboard.html
```

### Demo 3: Voice-Triggered ETL via Slack

```bash
# Step 1: Deploy Slack Lambda handler
cd framework/integrations
zip -r slack_lambda.zip slack_integration.py

aws lambda create-function \
  --function-name etl-slack-handler \
  --runtime python3.9 \
  --handler slack_integration.lambda_handler \
  --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/ETLFrameworkExecutionRole \
  --zip-file fileb://slack_lambda.zip \
  --environment "Variables={SLACK_BOT_TOKEN=xoxb-...,SLACK_SIGNING_SECRET=...}"

# Step 2: Create API Gateway endpoint
aws apigateway create-rest-api --name etl-slack-api

# Step 3: Configure Slack app event subscription
# Point to: https://your-api.execute-api.region.amazonaws.com/prod/slack/events

# Step 4: Test in Slack
# Type: "run simple_customer_etl"
# The bot will respond and trigger the job
```

### Demo 4: Data Quality Report

```python
# demo_dq_report.py
from framework.agents.data_quality_agent import DataQualityAgent
import json

class Config:
    pass

agent = DataQualityAgent(Config())

# Create rules from config
with open('test_configs/simple_etl_config.json') as f:
    config = json.load(f)

rules = agent.create_rules_from_config(config['data_quality'], 'customers')

print(f"Created {len(rules)} DQ rules:")
for rule in rules:
    print(f"  - {rule.rule_id}: {rule.description}")

# Generate HTML report (mock data)
from framework.agents.data_quality_agent import DQReport, DQCheckResult, DQStatus

report = DQReport(
    table_name="customers",
    timestamp="2024-01-15T10:00:00Z",
    overall_status=DQStatus.PASSED,
    total_rules=3,
    passed_rules=2,
    failed_rules=1,
    results=[
        DQCheckResult(rule_id="NL_001", rule_description="customer_id not null", status=DQStatus.PASSED, pass_rate=1.0),
        DQCheckResult(rule_id="NL_002", rule_description="email valid format", status=DQStatus.PASSED, pass_rate=0.98),
        DQCheckResult(rule_id="SQL_001", rule_description="no duplicates", status=DQStatus.FAILED, pass_rate=0.95, records_failed=500)
    ]
)

html = agent.generate_report_html(report)
with open('output/dq_report.html', 'w') as f:
    f.write(html)

print("DQ Report saved to output/dq_report.html")
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: No module named 'framework'` | Add project root to PYTHONPATH: `export PYTHONPATH=$PYTHONPATH:.` |
| DynamoDB table not found | Create tables using provisioning steps |
| Slack notifications not sending | Verify SLACK_BOT_TOKEN is set correctly |
| Glue job fails with permission error | Check IAM role policies are attached |
| Platform fallback not working | Ensure `auto_fallback_on_error: "Y"` in config |
| PII not detected | Add column names to `pii_columns` in config |

### Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Now run your code to see detailed logs
```

### Verifying DynamoDB Writes

```bash
# Check audit logs
aws dynamodb scan --table-name etl_audit_log --limit 10

# Check execution history
aws dynamodb query \
  --table-name etl_execution_history \
  --key-condition-expression "job_name = :jn" \
  --expression-attribute-values '{":jn": {"S": "your_job_name"}}'
```

---

## Next Steps

1. **Production Deployment**: Set up CI/CD pipeline for automated deployments
2. **Monitoring**: Create CloudWatch alarms for critical metrics
3. **Scaling**: Configure EKS with Karpenter for large-scale workloads
4. **Customization**: Extend agents with domain-specific rules

For questions or support, contact the Data Engineering team.
