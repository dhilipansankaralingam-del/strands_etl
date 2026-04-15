# Demo Execution Steps

This guide walks you through executing both the simple and complex ETL demo configurations.

---

## Quick Start - Local Testing (No AWS Required)

### Test Agents Locally First

```bash
# Navigate to project root
cd /home/user/strands_etl

# Set Python path
export PYTHONPATH=$PYTHONPATH:.

# Test 1: Auto-Healing Agent
python -c "
from framework.agents.auto_healing_agent import AutoHealingAgent

class Config:
    heal_memory_errors = True
    heal_shuffle_errors = True
    heal_timeout_errors = True
    heal_connection_errors = True
    heal_data_skew = True
    heal_partition_errors = True

agent = AutoHealingAgent(Config())
error = Exception('java.lang.OutOfMemoryError: GC overhead limit exceeded')
result = agent.analyze_and_heal(error, Config())

print('=== AUTO-HEALING AGENT TEST ===')
print(f'Can heal: {result.can_heal}')
print(f'Strategy: {result.strategy}')
print(f'Config changes: {result.config_changes}')
"

# Test 2: Data Quality Agent with Natural Language Rules
python -c "
from framework.agents.data_quality_agent import DataQualityAgent
import json

with open('demo_configs/simple_demo_config.json') as f:
    config = json.load(f)

agent = DataQualityAgent(type('Config', (), {})())
rules = agent.create_rules_from_config(config['data_quality'], 'customers')

print('=== DATA QUALITY AGENT TEST ===')
print(f'Created {len(rules)} DQ rules from config:')
for rule in rules:
    print(f'  [{rule.rule_type.value}] {rule.description}')
    print(f'    SQL: {rule.sql_expression[:60]}...')
"

# Test 3: Code Analysis Agent
python -c "
from framework.agents.code_analysis_agent import CodeAnalysisAgent

class Config:
    check_anti_patterns = True
    check_join_optimizations = True
    recommend_aws_tools = True
    recommend_delta_optimizations = True

with open('scripts/pyspark/simple_customer_etl.py') as f:
    code = f.read()

agent = CodeAnalysisAgent(Config())
result = agent.analyze(code, 'simple_customer_etl')

print('=== CODE ANALYSIS AGENT TEST ===')
print(f'Optimization Score: {result.optimization_score}/100')
print(f'Anti-patterns found: {len(result.anti_patterns_found)}')
print(f'Recommendations: {len(result.recommendations)}')
for rec in result.recommendations[:3]:
    print(f'  [{rec.severity.value}] {rec.title}')
"

# Test 4: Compliance Agent
python -c "
from framework.agents.compliance_agent import ComplianceAgent

class Config:
    check_sources = True
    check_targets = True
    frameworks = ['gdpr', 'pci_dss']
    pii_columns = ['email', 'phone']
    mask_pii = True

schema = {
    'columns': [
        {'name': 'customer_id', 'type': 'string'},
        {'name': 'customer_email', 'type': 'string'},
        {'name': 'phone_number', 'type': 'string'},
        {'name': 'credit_card', 'type': 'string'}
    ]
}

agent = ComplianceAgent(Config())
result = agent.analyze_compliance(schema, 'customers', is_source=True)

print('=== COMPLIANCE AGENT TEST ===')
print(f'Status: {result.status.value}')
print(f'PII Findings: {len(result.pii_findings)}')
for finding in result.pii_findings:
    print(f'  {finding.column_name}: {finding.pii_type.value} (Confidence: {finding.confidence:.0%})')
"

# Test 5: Workload Assessment Agent
python -c "
from framework.agents.workload_assessment_agent import WorkloadAssessmentAgent

class Config:
    analyze_input_size = True
    consider_weekday_weekend = True
    use_historical_trends = True
    monitor_cpu_load = True
    monitor_memory_profile = True
    detect_data_skew = True
    use_karpenter = True
    use_spot = True
    use_graviton = True

agent = WorkloadAssessmentAgent(Config())

source_tables = [
    {'name': 'orders', 'size_bytes': 50 * 1024**3, 'row_count': 100000000},
    {'name': 'customers', 'size_bytes': 5 * 1024**3, 'row_count': 10000000}
]

with open('scripts/pyspark/complex_sales_analytics.py') as f:
    code = f.read()

assessment = agent.assess_workload(source_tables=source_tables, code=code)

print('=== WORKLOAD ASSESSMENT TEST ===')
print(f'Complexity: {assessment.complexity.value}')
print(f'Total Data Volume: {assessment.data_volume.total_bytes / (1024**3):.1f} GB')
print(f'Primary Recommendation:')
rec = assessment.primary_recommendation
print(f'  Platform: {rec.platform.value}')
print(f'  Workers: {rec.num_workers} x {rec.worker_type.value}')
print(f'  Est. Cost: \${rec.estimated_cost:.2f}')
"
```

---

## Demo 1: Simple ETL Execution

### Prerequisites Check

```bash
# Verify AWS credentials
aws sts get-caller-identity

# Verify S3 buckets exist
aws s3 ls s3://etl-framework-scripts/ 2>/dev/null || echo "Create bucket first"
aws s3 ls s3://etl-framework-data/ 2>/dev/null || echo "Create bucket first"

# Verify Glue databases exist
aws glue get-database --name raw_data 2>/dev/null || echo "Create database first"
aws glue get-database --name processed_data 2>/dev/null || echo "Create database first"
```

### Step 1: Upload Script and Config

```bash
# Upload PySpark script
aws s3 cp scripts/pyspark/simple_customer_etl.py \
  s3://etl-framework-scripts/pyspark/simple_customer_etl.py

# Upload config
aws s3 cp demo_configs/simple_demo_config.json \
  s3://etl-framework-scripts/configs/simple_demo_config.json
```

### Step 2: Create Sample Data (if needed)

```python
# generate_sample_customers.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("GenerateSampleData").getOrCreate()

# Generate 100K sample customers
customers = spark.range(100000).select(
    col("id").alias("customer_id"),
    concat(lit("Customer_"), col("id")).alias("customer_name"),
    concat(lit("customer"), col("id"), lit("@email.com")).alias("email"),
    concat(lit("+1-555-"), (col("id") % 10000000).cast("string")).alias("phone"),
    concat(lit("Address "), col("id")).alias("address"),
    (18 + (col("id") % 80)).alias("age"),
    when(col("id") % 3 == 0, "active")
     .when(col("id") % 3 == 1, "inactive")
     .otherwise("pending").alias("status"),
    when(col("id") % 4 == 0, "US")
     .when(col("id") % 4 == 1, "UK")
     .when(col("id") % 4 == 2, "CA")
     .otherwise("DE").alias("country")
)

customers.write.mode("overwrite").parquet("s3://etl-framework-data/raw/customers/")
print(f"Generated {customers.count()} customer records")
```

### Step 3: Create Glue Job

```bash
aws glue create-job \
  --name demo_simple_customer_etl \
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
    "--target_table": "customers_cleaned",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X"
```

### Step 4: Run the Job

```bash
# Start job run
RUN_ID=$(aws glue start-job-run --job-name demo_simple_customer_etl --query 'JobRunId' --output text)
echo "Job Run ID: $RUN_ID"

# Monitor progress
watch -n 5 "aws glue get-job-run --job-name demo_simple_customer_etl --run-id $RUN_ID --query 'JobRun.JobRunState' --output text"
```

### Step 5: Verify Results

```bash
# Check output data
aws s3 ls s3://etl-framework-data/processed/customers/ --recursive

# Check DynamoDB audit
aws dynamodb query \
  --table-name etl_audit_log \
  --key-condition-expression "job_name = :jn" \
  --expression-attribute-values '{":jn": {"S": "demo_simple_customer_etl"}}' \
  --limit 10

# Check CloudWatch metrics
aws cloudwatch get-metric-statistics \
  --namespace ETL/SimpleDemo \
  --metric-name duration_seconds \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 \
  --statistics Average
```

---

## Demo 2: Complex ETL Execution

### Step 1: Upload Script and Config

```bash
# Upload PySpark script
aws s3 cp scripts/pyspark/complex_sales_analytics.py \
  s3://etl-framework-scripts/pyspark/complex_sales_analytics.py

# Upload config
aws s3 cp demo_configs/complex_demo_config.json \
  s3://etl-framework-scripts/configs/complex_demo_config.json
```

### Step 2: Create Required Databases and Tables

```bash
# Create databases
aws glue create-database --database-input '{"Name":"raw_data"}' 2>/dev/null || true
aws glue create-database --database-input '{"Name":"master_data"}' 2>/dev/null || true
aws glue create-database --database-input '{"Name":"analytics"}' 2>/dev/null || true

# Create orders table
aws glue create-table \
  --database-name raw_data \
  --table-input '{
    "Name": "orders",
    "StorageDescriptor": {
      "Columns": [
        {"Name": "order_id", "Type": "string"},
        {"Name": "customer_id", "Type": "string"},
        {"Name": "order_date", "Type": "date"},
        {"Name": "order_status", "Type": "string"},
        {"Name": "shipping_region", "Type": "string"}
      ],
      "Location": "s3://etl-framework-data/raw/orders/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
    }
  }' 2>/dev/null || true

# Create order_lines table
aws glue create-table \
  --database-name raw_data \
  --table-input '{
    "Name": "order_lines",
    "StorageDescriptor": {
      "Columns": [
        {"Name": "order_id", "Type": "string"},
        {"Name": "product_id", "Type": "string"},
        {"Name": "quantity", "Type": "int"},
        {"Name": "unit_price", "Type": "double"},
        {"Name": "discount", "Type": "double"},
        {"Name": "line_total", "Type": "double"}
      ],
      "Location": "s3://etl-framework-data/raw/order_lines/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
    }
  }' 2>/dev/null || true
```

### Step 3: Create Glue Job with Delta Lake Support

```bash
aws glue create-job \
  --name demo_complex_sales_analytics \
  --role ETLFrameworkExecutionRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://etl-framework-scripts/pyspark/complex_sales_analytics.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--source_database": "raw_data",
    "--target_database": "analytics",
    "--delta_path": "s3://etl-framework-data/delta",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--additional-python-modules": "delta-spark",
    "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type "G.2X"
```

### Step 4: Run with Framework (Python)

```python
# run_complex_demo.py
import json
import sys
sys.path.insert(0, '.')

from framework.core.engine import FrameworkEngine
from framework.core.config_schema import MasterConfig
from framework.agents import (
    CodeAnalysisAgent,
    DataQualityAgent,
    ComplianceAgent,
    WorkloadAssessmentAgent,
    RecommendationAgent
)

# Load config
with open('demo_configs/complex_demo_config.json') as f:
    config_dict = json.load(f)

print("="*60)
print("COMPLEX ETL DEMO - PRE-EXECUTION ANALYSIS")
print("="*60)

# 1. Code Analysis
print("\n[1] CODE ANALYSIS")
print("-"*40)
with open('scripts/pyspark/complex_sales_analytics.py') as f:
    code = f.read()

class CAConfig:
    check_anti_patterns = True
    check_join_optimizations = True
    recommend_aws_tools = True
    recommend_delta_optimizations = True

code_agent = CodeAnalysisAgent(CAConfig())
code_result = code_agent.analyze(code, config_dict['job_name'])
print(f"Optimization Score: {code_result.optimization_score}/100")
print(f"Anti-patterns: {len(code_result.anti_patterns_found)}")
print(f"Recommendations: {len(code_result.recommendations)}")

# 2. Data Quality Rules Preview
print("\n[2] DATA QUALITY RULES")
print("-"*40)
dq_agent = DataQualityAgent(type('Config', (), {})())
dq_rules = dq_agent.create_rules_from_config(
    config_dict['data_quality'],
    'sales_fact'
)
print(f"Total DQ rules: {len(dq_rules)}")
for rule in dq_rules[:5]:
    print(f"  [{rule.rule_type.value}] {rule.description}")

# 3. Compliance Check Preview
print("\n[3] COMPLIANCE CHECK")
print("-"*40)
class CompConfig:
    check_sources = True
    check_targets = True
    frameworks = config_dict['compliance']['frameworks']
    pii_columns = config_dict['compliance']['pii_columns']
    mask_pii = True

comp_agent = ComplianceAgent(CompConfig())

# Check each source table schema
for table in config_dict['source_tables']:
    schema = {
        'columns': [
            {'name': 'customer_email', 'type': 'string'},
            {'name': 'customer_name', 'type': 'string'},
            {'name': 'phone', 'type': 'string'}
        ]
    }
    result = comp_agent.analyze_compliance(schema, table['table'], is_source=True)
    print(f"{table['table']}: {result.status.value} ({len(result.pii_findings)} PII columns)")

# 4. Workload Assessment
print("\n[4] WORKLOAD ASSESSMENT")
print("-"*40)
class WAConfig:
    analyze_input_size = True
    consider_weekday_weekend = True
    use_historical_trends = True
    monitor_cpu_load = True
    monitor_memory_profile = True
    detect_data_skew = True
    use_karpenter = True
    use_spot = True
    use_graviton = True

wa_agent = WorkloadAssessmentAgent(WAConfig())
source_tables = [
    {
        'name': t['table'],
        'size_bytes': t.get('estimated_size_gb', 1) * 1024**3,
        'row_count': 1000000
    }
    for t in config_dict['source_tables']
]

assessment = wa_agent.assess_workload(source_tables=source_tables, code=code)
print(f"Complexity: {assessment.complexity.value}")
print(f"Total Data: {assessment.data_volume.total_bytes / (1024**3):.1f} GB")
print(f"Recommended Platform: {assessment.primary_recommendation.platform.value}")
print(f"Workers: {assessment.primary_recommendation.num_workers} x {assessment.primary_recommendation.worker_type.value}")
print(f"Est. Cost: ${assessment.primary_recommendation.estimated_cost:.2f}")

# 5. Aggregate Recommendations
print("\n[5] AGGREGATED RECOMMENDATIONS")
print("-"*40)
rec_agent = RecommendationAgent(type('Config', (), {'recommendations_table': 'etl_recommendations'})())
plan = rec_agent.aggregate_recommendations(
    code_analysis_results={
        'recommendations': [
            {'title': r.title, 'severity': r.severity.value, 'description': r.description}
            for r in code_result.recommendations
        ]
    },
    workload_assessment={
        'warnings': assessment.warnings,
        'optimization_opportunities': assessment.optimization_opportunities,
        'primary_recommendation': {
            'platform': assessment.primary_recommendation.platform.value,
            'worker_type': assessment.primary_recommendation.worker_type.value,
            'num_workers': assessment.primary_recommendation.num_workers,
            'estimated_cost': assessment.primary_recommendation.estimated_cost
        }
    },
    job_name=config_dict['job_name']
)

print(f"Total Recommendations: {plan.total_recommendations}")
print(f"By Priority: {plan.by_priority}")
print(f"Quick Wins: {len(plan.quick_wins)}")
for qw in plan.quick_wins[:3]:
    print(f"  - {qw.title} ({qw.effort.value})")

print("\n" + "="*60)
print("PRE-EXECUTION ANALYSIS COMPLETE")
print("="*60)
print("\nTo execute the actual job, run:")
print("  aws glue start-job-run --job-name demo_complex_sales_analytics")
```

### Step 5: Execute and Monitor

```bash
# Start job run
RUN_ID=$(aws glue start-job-run --job-name demo_complex_sales_analytics --query 'JobRunId' --output text)
echo "Job Run ID: $RUN_ID"

# Monitor in real-time
aws logs tail /aws-glue/jobs/demo_complex_sales_analytics --follow

# Or check status periodically
watch -n 10 "aws glue get-job-run --job-name demo_complex_sales_analytics --run-id $RUN_ID"
```

### Step 6: Generate Reports

```python
# generate_reports.py
import json
from framework.agents.data_quality_agent import DataQualityAgent, DQReport, DQCheckResult, DQStatus
from framework.dashboard.enterprise_dashboard import EnterpriseDashboard, DashboardData
from framework.integrations.email_integration import EmailIntegration

# Generate DQ Report HTML
dq_agent = DataQualityAgent(type('Config', (), {})())
report = DQReport(
    table_name="sales_fact",
    timestamp="2024-01-15T10:00:00Z",
    overall_status=DQStatus.PASSED,
    total_rules=15,
    passed_rules=14,
    failed_rules=1,
    results=[
        DQCheckResult(rule_id="NL_ORDER_ID", rule_description="order_id not null", status=DQStatus.PASSED, pass_rate=1.0),
        DQCheckResult(rule_id="NL_QUANTITY", rule_description="quantity > 0", status=DQStatus.PASSED, pass_rate=0.998),
        DQCheckResult(rule_id="SQL_NEG_PROFIT", rule_description="negative profit check", status=DQStatus.WARNING, pass_rate=0.95, records_failed=500)
    ]
)

dq_html = dq_agent.generate_report_html(report)
with open('output/dq_report.html', 'w') as f:
    f.write(dq_html)
print("DQ Report saved to output/dq_report.html")

# Generate Enterprise Dashboard
class DashConfig:
    dashboard_output_dir = './output'

dashboard = EnterpriseDashboard(DashConfig())
data = DashboardData(
    job_history=[
        {'name': 'demo_complex_sales_analytics', 'status': 'success', 'duration_seconds': 3600, 'cost': 45.00, 'timestamp': '2024-01-15T06:00:00Z'},
        {'name': 'demo_complex_sales_analytics', 'status': 'success', 'duration_seconds': 3500, 'cost': 42.00, 'timestamp': '2024-01-14T06:00:00Z'}
    ],
    dq_trends={'2024-01-14': 0.98, '2024-01-15': 0.99},
    compliance_status={'GDPR': 'compliant', 'PCI_DSS': 'compliant', 'SOX': 'compliant'},
    cost_summary={'total': 87.00, 'avg_per_run': 43.50}
)
path = dashboard.save_dashboard(data, 'enterprise_dashboard.html')
print(f"Enterprise Dashboard saved to {path}")
```

---

## Demo 3: Slack Voice Trigger (Optional)

If Slack integration is configured:

```
# In Slack channel #etl-analytics, type or speak:
"run demo_complex_sales_analytics"

# The bot will respond with:
# 🚀 Starting job: demo_complex_sales_analytics
# Platform: Glue
# Config: complex_demo_config.json

# After completion:
# ✅ Job Completed: demo_complex_sales_analytics
# Duration: 45 min | Cost: $42.50 | Records: 5.2M
```

---

## Verification Checklist

### Simple Demo Verification

- [ ] Raw customer data exists in S3
- [ ] Glue job created and visible in console
- [ ] Job completed successfully
- [ ] Cleaned data written to processed layer
- [ ] DynamoDB audit entries created
- [ ] CloudWatch metrics published
- [ ] Email notification received (if configured)

### Complex Demo Verification

- [ ] All source tables exist in Glue catalog
- [ ] Delta Lake path accessible
- [ ] Job completed with all stages successful
- [ ] sales_fact Delta table created/updated
- [ ] customer_aggregates table populated
- [ ] product_performance table populated
- [ ] All agents executed (check audit log)
- [ ] Recommendations generated
- [ ] Dashboard accessible
- [ ] Notifications sent (Slack/Teams/Email)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError` | Run `export PYTHONPATH=$PYTHONPATH:.` |
| Glue job permission denied | Check IAM role has required policies |
| S3 access denied | Verify bucket policy and IAM permissions |
| DynamoDB table not found | Create tables using provisioning script |
| Delta Lake errors | Ensure `delta-spark` module is installed |
| Slack not responding | Verify bot token and channel permissions |
| Email not sending | Check SES sender verification |

---

## Next Steps

1. **Customize configs** - Modify demo configs for your tables
2. **Add more DQ rules** - Extend natural language rules
3. **Enable all integrations** - Set up Slack, Teams, Email
4. **Schedule jobs** - Use CloudWatch Events or Glue triggers
5. **Monitor dashboards** - Access enterprise dashboard regularly
