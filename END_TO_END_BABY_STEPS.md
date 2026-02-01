# Strands ETL Framework - Complete End-to-End Baby Steps Guide

This comprehensive guide walks you through the entire Strands ETL Framework from setup to dashboard reporting.

---

## Table of Contents

1. [Part 1: AWS Infrastructure Setup](#part-1-aws-infrastructure-setup)
2. [Part 2: Sample Data and Scripts Setup](#part-2-sample-data-and-scripts-setup)
3. [Part 3: Create Sample Glue Jobs (One-Time Setup)](#part-3-create-sample-glue-jobs-one-time-setup)
4. [Part 4: Configuration Files](#part-4-configuration-files)
5. [Part 5: Run Existing Jobs via Strands (99% Use Case)](#part-5-run-existing-jobs-via-strands-99-use-case)
6. [Part 6: Agent Modes - Recommend vs Decide](#part-6-agent-modes---recommend-vs-decide)
7. [Part 7: Creating New Jobs (Rare Case)](#part-7-creating-new-jobs-rare-case)
8. [Part 8: Getting Platform Recommendations](#part-8-getting-platform-recommendations)
9. [Part 9: Learning Module - Automatic Data Capture](#part-9-learning-module---automatic-data-capture)
10. [Part 10: Training on Historical Data](#part-10-training-on-historical-data)
11. [Part 11: Getting Insights](#part-11-getting-insights)
12. [Part 12: Dashboard and Reporting](#part-12-dashboard-and-reporting)
13. [Part 13: Cost Analysis](#part-13-cost-analysis)
14. [Part 14: Code Analysis and Improvement](#part-14-code-analysis-and-improvement)

---

## Part 1: AWS Infrastructure Setup

### Step 1.1: Set AWS Region

```bash
export AWS_DEFAULT_REGION=us-west-2
```

### Step 1.2: Create Required S3 Buckets

```bash
# Data bucket for input/output data
aws s3 mb s3://strands-etl-data --region us-west-2

# Scripts bucket for Glue job scripts
aws s3 mb s3://strands-etl-scripts --region us-west-2

# Learning bucket for learning module data
aws s3 mb s3://strands-etl-learning --region us-west-2

# Config bucket for configuration files
aws s3 mb s3://strands-etl-config --region us-west-2
```

### Step 1.3: Verify Buckets Created

```bash
aws s3 ls | grep strands
```

**Expected Output:**
```
2024-01-15 10:00:00 strands-etl-data
2024-01-15 10:00:01 strands-etl-scripts
2024-01-15 10:00:02 strands-etl-learning
2024-01-15 10:00:03 strands-etl-config
```

### Step 1.4: Attach Required IAM Policies

```bash
# Attach Bedrock access (for AI agents)
aws iam attach-role-policy \
    --role-name YOUR_INSTANCE_ROLE \
    --policy-arn arn:aws:iam::aws:policy/AmazonBedrockFullAccess

# Attach Glue access
aws iam attach-role-policy \
    --role-name YOUR_INSTANCE_ROLE \
    --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess

# Attach S3 access
aws iam attach-role-policy \
    --role-name YOUR_INSTANCE_ROLE \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

---

## Part 2: Sample Data and Scripts Setup

### Step 2.1: Create Sample Input Data

```bash
cat > /tmp/sample_sales_data.csv << 'EOF'
order_id,customer_id,product_name,quantity,unit_price,order_date,region
1001,C001,Laptop,2,999.99,2024-01-15,US-West
1002,C002,Mouse,5,29.99,2024-01-15,US-East
1003,C001,Keyboard,3,79.99,2024-01-16,US-West
1004,C003,Monitor,1,399.99,2024-01-16,EU-West
1005,C004,Headphones,2,149.99,2024-01-17,US-East
1006,C002,Webcam,1,89.99,2024-01-17,EU-West
1007,C005,Laptop,1,1299.99,2024-01-18,US-West
1008,C001,Mouse,10,29.99,2024-01-18,US-East
1009,C006,Keyboard,2,99.99,2024-01-19,EU-West
1010,C003,Monitor,2,449.99,2024-01-19,US-West
EOF
```

### Step 2.2: Upload Sample Data to S3

```bash
aws s3 cp /tmp/sample_sales_data.csv s3://strands-etl-data/input/sales/
```

### Step 2.3: Create Sample PySpark ETL Script

```bash
cat > /tmp/sales_etl_job.py << 'PYSPARK_EOF'
"""
Sample Sales ETL Job
Transforms raw sales data into aggregated analytics
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read source data
print("Reading source data from S3...")
input_path = "s3://strands-etl-data/input/sales/"

df = spark.read.option("header", "true").csv(input_path)

# Print schema and sample
print(f"Input records: {df.count()}")
df.show(5)

# Transform: Calculate totals and aggregates
print("Transforming data...")
df_with_total = df.withColumn(
    "total_amount",
    F.col("quantity").cast("double") * F.col("unit_price").cast("double")
)

# Aggregate by region
region_summary = df_with_total.groupBy("region").agg(
    F.count("order_id").alias("total_orders"),
    F.sum("total_amount").alias("total_revenue"),
    F.avg("total_amount").alias("avg_order_value")
)

# Aggregate by product
product_summary = df_with_total.groupBy("product_name").agg(
    F.sum("quantity").alias("total_quantity_sold"),
    F.sum("total_amount").alias("total_revenue")
)

# Write outputs
print("Writing outputs to S3...")
output_path = "s3://strands-etl-data/output/sales/"

region_summary.write.mode("overwrite").parquet(f"{output_path}region_summary/")
product_summary.write.mode("overwrite").parquet(f"{output_path}product_summary/")
df_with_total.write.mode("overwrite").parquet(f"{output_path}detailed/")

print(f"Region Summary:")
region_summary.show()

print(f"Product Summary:")
product_summary.show()

print("ETL Job completed successfully!")
job.commit()
PYSPARK_EOF
```

### Step 2.4: Upload Script to S3

```bash
aws s3 cp /tmp/sales_etl_job.py s3://strands-etl-scripts/etl/
```

### Step 2.5: Verify Files Uploaded

```bash
echo "=== Input Data ==="
aws s3 ls s3://strands-etl-data/input/sales/

echo ""
echo "=== Scripts ==="
aws s3 ls s3://strands-etl-scripts/etl/
```

---

## Part 3: Create Sample Glue Jobs (One-Time Setup)

This is done ONCE to set up the existing jobs that will be executed repeatedly.

### Step 3.1: Create IAM Role for Glue (if not exists)

```bash
# Create trust policy
cat > /tmp/glue-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

# Create role
aws iam create-role \
    --role-name AWSGlueServiceRole-StrandsETL \
    --assume-role-policy-document file:///tmp/glue-trust-policy.json

# Attach policies
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-StrandsETL \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-StrandsETL \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### Step 3.2: Create the Sample Glue Job

```bash
aws glue create-job \
    --name "sales-etl-daily" \
    --role "AWSGlueServiceRole-StrandsETL" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://strands-etl-scripts/etl/sales_etl_job.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--job-language": "python",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true"
    }' \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X"
```

### Step 3.3: Verify Job Created

```bash
aws glue get-job --job-name sales-etl-daily
```

**Expected Output:**
```json
{
    "Job": {
        "Name": "sales-etl-daily",
        "Role": "AWSGlueServiceRole-StrandsETL",
        ...
    }
}
```

### Step 3.4: Create Additional Sample Jobs (Optional)

```bash
# Create a second job for testing
aws glue create-job \
    --name "sales-etl-weekly" \
    --role "AWSGlueServiceRole-StrandsETL" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://strands-etl-scripts/etl/sales_etl_job.py",
        "PythonVersion": "3"
    }' \
    --glue-version "4.0" \
    --number-of-workers 5 \
    --worker-type "G.2X"
```

---

## Part 4: Configuration Files

### Step 4.1: Create Configuration for Running Existing Job

```bash
cat > config/run_existing_job.json << 'EOF'
{
    "pipeline_name": "sales_daily_pipeline",
    "description": "Run existing daily sales ETL job",

    "workload": {
        "name": "daily_sales_processing",
        "data_volume": "medium",
        "complexity": "medium",
        "criticality": "high",
        "time_sensitivity": "normal"
    },

    "execution": {
        "platform": "glue",
        "glue_job_name": "sales-etl-daily",
        "create_job": false,
        "job_arguments": {
            "--input_date": "2024-01-15"
        },
        "timeout": 60,
        "worker_type": "G.1X",
        "number_of_workers": 2
    }
}
EOF
```

### Step 4.2: Create Configuration for Agent-Driven Decision

```bash
cat > config/agent_decides.json << 'EOF'
{
    "pipeline_name": "smart_sales_pipeline",
    "description": "Let agent decide the optimal platform",

    "workload": {
        "name": "smart_sales_processing",
        "data_volume": "high",
        "complexity": "high",
        "criticality": "high",
        "time_sensitivity": "urgent"
    },

    "execution": {
        "available_platforms": ["glue", "emr", "lambda"],
        "glue_job_name": "sales-etl-weekly",
        "emr_cluster_id": "j-XXXXXXXXXXXXX",
        "lambda_function_name": "sales-etl-lambda"
    },

    "scripts": {
        "pyspark": "s3://strands-etl-scripts/etl/sales_etl_job.py"
    }
}
EOF
```

### Step 4.3: Create Configuration for New Job Creation (Rare)

```bash
cat > config/create_new_job.json << 'EOF'
{
    "pipeline_name": "adhoc_analysis_pipeline",
    "description": "Create new job for one-time analysis",

    "workload": {
        "name": "adhoc_customer_analysis",
        "data_volume": "low",
        "complexity": "low",
        "criticality": "low",
        "time_sensitivity": "normal"
    },

    "execution": {
        "platform": "glue",
        "create_job": true,
        "new_job_name": "adhoc-customer-analysis-2024",
        "worker_type": "G.1X",
        "number_of_workers": 2,
        "timeout": 30,
        "iam_role": "AWSGlueServiceRole-StrandsETL"
    },

    "scripts": {
        "pyspark": "s3://strands-etl-scripts/etl/sales_etl_job.py"
    }
}
EOF
```

### Step 4.4: Upload Configs to S3

```bash
aws s3 cp config/run_existing_job.json s3://strands-etl-config/
aws s3 cp config/agent_decides.json s3://strands-etl-config/
aws s3 cp config/create_new_job.json s3://strands-etl-config/
```

---

## Part 5: Run Existing Jobs via Strands (99% Use Case)

This is the primary use case - running existing Glue jobs through the Strands framework.

### Step 5.1: Dry Run (No Execution)

```bash
cd /home/user/strands_etl

python -m orchestrator.strands_orchestrator \
    --config config/run_existing_job.json \
    --request "Execute daily sales ETL job" \
    --mode recommend \
    --dry-run
```

**Expected Output:**
```json
{
    "pipeline_id": "abc-123",
    "status": "dry_run",
    "orchestration_plan": {...},
    "platform_decision": {
        "selected_platform": "glue",
        "recommendations": [...]
    }
}
```

### Step 5.2: Execute the Job

```bash
python -m orchestrator.strands_orchestrator \
    --config config/run_existing_job.json \
    --request "Execute daily sales ETL job" \
    --mode recommend
```

**Expected Output:**
```json
{
    "pipeline_id": "abc-123",
    "status": "completed",
    "execution_result": {
        "platform": "glue",
        "job_name": "sales-etl-daily",
        "run_id": "jr_xxxxx",
        "execution_type": "existing_job",
        "final_status": "SUCCEEDED"
    },
    "quality_report": {
        "overall_score": 0.95
    },
    "learning": {
        "learning_vector": {...}
    }
}
```

### Step 5.3: Verify Job Ran in AWS Console

```bash
# Check recent job runs
aws glue get-job-runs --job-name sales-etl-daily --max-results 1
```

### Step 5.4: Check Output Data

```bash
aws s3 ls s3://strands-etl-data/output/sales/ --recursive
```

---

## Part 6: Agent Modes - Recommend vs Decide

### Mode 1: RECOMMEND Mode (Default)

The agent provides recommendations but you control execution.

```bash
python -m orchestrator.strands_orchestrator \
    --config config/agent_decides.json \
    --request "Process weekly sales data" \
    --mode recommend
```

**Output includes recommendations:**
```json
{
    "platform_decision": {
        "selected_platform": "glue",
        "recommendations": [
            {
                "platform": "glue",
                "score": 0.85,
                "pros": ["Good for complex transformations", "Catalog integration"],
                "cons": ["Higher cold start time"],
                "estimated_cost": "$2.50"
            },
            {
                "platform": "emr",
                "score": 0.70,
                "pros": ["Best for very large datasets"],
                "cons": ["Higher operational overhead"],
                "estimated_cost": "$5.00"
            }
        ]
    }
}
```

### Mode 2: DECIDE Mode (Autonomous)

The agent autonomously selects the platform.

```bash
python -m orchestrator.strands_orchestrator \
    --config config/agent_decides.json \
    --request "Process weekly sales data - agent decide platform" \
    --mode decide
```

**Output:**
```json
{
    "agent_mode": "decide",
    "platform_decision": {
        "selected_platform": "glue",
        "confidence": 0.85,
        "reasoning": "Selected Glue based on high complexity workload..."
    },
    "selected_platform": "glue",
    "execution_result": {...}
}
```

---

## Part 7: Creating New Jobs (Rare Case)

Only use when you need to create a new job on-the-fly.

### Step 7.1: Create New Job Configuration

Already created in Step 4.3 (`config/create_new_job.json`).

Key setting: `"create_job": true`

### Step 7.2: Execute with Job Creation

```bash
python -m orchestrator.strands_orchestrator \
    --config config/create_new_job.json \
    --request "Create and run adhoc customer analysis" \
    --mode recommend
```

**Expected Output:**
```json
{
    "execution_result": {
        "platform": "glue",
        "job_name": "adhoc-customer-analysis-2024",
        "run_id": "jr_xxxxx",
        "execution_type": "new_job_created",
        "status": "running"
    }
}
```

### Step 7.3: Verify New Job Created

```bash
aws glue get-job --job-name adhoc-customer-analysis-2024
```

---

## Part 8: Getting Platform Recommendations

Get recommendations without executing any job.

### Step 8.1: Get Recommendation Only

```bash
python -m orchestrator.strands_orchestrator \
    --config config/agent_decides.json \
    --recommend-only
```

**Output:**
```json
{
    "RECOMMENDED_PLATFORM": "glue",
    "CONFIDENCE_SCORE": 0.85,
    "REASONING": "Based on high complexity and high data volume...",
    "CONVERSION_POSSIBLE": "yes",
    "CONVERSION_RECOMMENDATION": "Consider Lambda for simpler portions...",
    "COST_COMPARISON": {
        "glue": "$2.50/run",
        "emr": "$5.00/run",
        "lambda": "$0.50/run (if applicable)"
    }
}
```

---

## Part 9: Learning Module - Automatic Data Capture

The Learning Module automatically captures data after each execution.

### Step 9.1: Run Multiple Jobs to Generate Learning Data

```bash
# Run job 1
python -m orchestrator.strands_orchestrator \
    --config config/run_existing_job.json \
    --request "Daily sales run 1" \
    --mode recommend

# Run job 2
python -m orchestrator.strands_orchestrator \
    --config config/run_existing_job.json \
    --request "Daily sales run 2" \
    --mode recommend

# Run job 3 with different config
python -m orchestrator.strands_orchestrator \
    --config config/agent_decides.json \
    --request "Weekly sales run" \
    --mode decide
```

### Step 9.2: Verify Learning Vectors Captured

```bash
aws s3 ls s3://strands-etl-learning/learning/vectors/
```

**Expected Output:**
```
2024-01-15 10:00:00 abc-123.json
2024-01-15 10:05:00 def-456.json
2024-01-15 10:10:00 ghi-789.json
```

### Step 9.3: View a Learning Vector

```bash
aws s3 cp s3://strands-etl-learning/learning/vectors/$(aws s3 ls s3://strands-etl-learning/learning/vectors/ | head -1 | awk '{print $4}') - | python -m json.tool
```

**Sample Learning Vector:**
```json
{
    "vector_id": "abc-123",
    "timestamp": "2024-01-15T10:00:00",
    "workload": {
        "name": "daily_sales_processing",
        "data_volume": "medium",
        "complexity": "medium"
    },
    "execution": {
        "platform": "glue",
        "execution_type": "existing_job",
        "status": "completed"
    },
    "metrics": {
        "execution_time_seconds": 120,
        "estimated_cost_usd": 0.88
    },
    "quality": {
        "overall_score": 0.95
    }
}
```

---

## Part 10: Training on Historical Data

Train the prediction model on historical execution data.

### Step 10.1: Analyze Current Patterns

```bash
python learning_module.py analyze
```

**Output:**
```json
{
    "total_executions": 10,
    "summary": {
        "success_rate": 0.9,
        "avg_execution_time_seconds": 150,
        "avg_cost_per_run_usd": 0.92
    },
    "platform_analysis": {
        "glue": {
            "count": 8,
            "success_rate": 0.875,
            "avg_execution_time": 140
        }
    }
}
```

### Step 10.2: Train the Platform Predictor

```bash
python learning_module.py train
```

**Output:**
```json
{
    "model_id": "model-xyz-123",
    "trained_at": "2024-01-15T10:30:00",
    "training_samples": 10,
    "rules": {
        "high_volume": {
            "recommended_platform": "glue",
            "confidence": 0.85,
            "sample_count": 5
        },
        "medium_complexity": {
            "recommended_platform": "glue",
            "confidence": 0.75,
            "sample_count": 8
        }
    }
}
```

### Step 10.3: Predict Platform for New Workload

```bash
python learning_module.py predict --workload '{"data_volume": "high", "complexity": "high"}'
```

**Output:**
```json
{
    "recommended_platform": "glue",
    "confidence": 0.85,
    "all_scores": {
        "glue": 0.85,
        "emr": 0.15
    },
    "model_id": "model-xyz-123",
    "based_on_samples": 10
}
```

---

## Part 11: Getting Insights

Ask questions about your ETL performance using natural language.

### Step 11.1: Ask About Performance

```bash
python learning_module.py insights --question "Which platform has the best success rate?"
```

**Output:**
```json
{
    "answer": "Based on your historical data, Glue has the best success rate at 87.5%...",
    "key_findings": [
        "Glue: 87.5% success rate across 8 runs",
        "Average execution time: 140 seconds"
    ],
    "recommendations": [
        "Continue using Glue for similar workloads"
    ],
    "confidence": 0.85
}
```

### Step 11.2: Ask About Cost

```bash
python learning_module.py insights --question "What is my average cost per job and how can I reduce it?"
```

### Step 11.3: Ask About Failures

```bash
python learning_module.py insights --question "What are the main causes of job failures?"
```

---

## Part 12: Dashboard and Reporting

### Step 12.1: Generate Summary Report

```bash
python learning_module.py report --report-type summary
```

**Output:**
```json
{
    "report_type": "summary",
    "generated_at": "2024-01-15T11:00:00",
    "summary": {
        "total_runs": 10,
        "successful_runs": 9,
        "failed_runs": 1,
        "success_rate": 0.9,
        "avg_execution_time_seconds": 150,
        "total_estimated_cost_usd": 9.20
    },
    "platform_usage": {
        "glue": {"count": 8, "success_rate": 0.875},
        "lambda": {"count": 2, "success_rate": 1.0}
    }
}
```

### Step 12.2: Generate Detailed Report

```bash
python learning_module.py report --report-type detailed
```

### Step 12.3: Generate Cost Report

```bash
python learning_module.py report --report-type cost
```

**Output:**
```json
{
    "report_type": "cost",
    "cost_analysis": {
        "by_platform": {
            "glue": {
                "avg_cost": 1.05,
                "min_cost": 0.44,
                "max_cost": 2.20,
                "total_cost": 8.40
            }
        },
        "by_data_volume": {
            "low": 0.44,
            "medium": 0.88,
            "high": 1.76
        }
    }
}
```

### Step 12.4: Generate Performance Report

```bash
python learning_module.py report --report-type performance
```

### Step 12.5: Run Dashboard Script

```bash
python examples/run_dashboard.py
```

This generates an HTML dashboard at `output/dashboard.html`.

---

## Part 13: Cost Analysis

### Step 13.1: View Cost Trends

```bash
python learning_module.py insights --question "Show me cost trends over the last week and identify any anomalies"
```

### Step 13.2: Cost Optimization Recommendations

```bash
python learning_module.py insights --question "How can I reduce my ETL costs by 20%?"
```

**Sample Output:**
```json
{
    "answer": "Based on your execution history, here are recommendations to reduce costs by 20%...",
    "recommendations": [
        "Move low-complexity jobs to Lambda (potential savings: 50%)",
        "Reduce worker count for medium-volume jobs from 5 to 3",
        "Schedule non-urgent jobs during off-peak hours"
    ]
}
```

### Step 13.3: Platform Cost Comparison

```bash
python examples/cost_analysis.py
```

---

## Part 14: Code Analysis and Improvement

### Step 14.1: Get Code Improvement Recommendations

```bash
python learning_module.py insights --question "Based on execution patterns, what code improvements would increase efficiency?"
```

### Step 14.2: Platform Conversion Analysis

```bash
python -m orchestrator.strands_orchestrator \
    --config config/agent_decides.json \
    --recommend-only
```

**Look for CONVERSION_RECOMMENDATION in output:**
```json
{
    "CONVERSION_POSSIBLE": "yes",
    "CONVERSION_RECOMMENDATION": "This Glue job could be converted to Lambda for cost savings. The average execution time is under 5 minutes and data volume is low. Estimated savings: 60%"
}
```

### Step 14.3: Full Pipeline Analysis

```bash
python examples/full_analysis.py
```

This runs a complete analysis including:
- Historical pattern analysis
- Cost breakdown
- Performance trends
- Code improvement recommendations
- Platform conversion opportunities

---

## Complete End-to-End Example

Run this script for a complete end-to-end demonstration:

```bash
#!/bin/bash
# complete_e2e_demo.sh

set -e

echo "=========================================="
echo "STRANDS ETL FRAMEWORK - COMPLETE E2E DEMO"
echo "=========================================="

export AWS_DEFAULT_REGION=us-west-2

echo ""
echo "Step 1: Running existing Glue job..."
python -m orchestrator.strands_orchestrator \
    --config config/run_existing_job.json \
    --request "Execute daily sales ETL" \
    --mode recommend

echo ""
echo "Step 2: Running with agent decision mode..."
python -m orchestrator.strands_orchestrator \
    --config config/agent_decides.json \
    --request "Process data - agent decides" \
    --mode decide

echo ""
echo "Step 3: Analyzing patterns..."
python learning_module.py analyze

echo ""
echo "Step 4: Training predictor..."
python learning_module.py train

echo ""
echo "Step 5: Getting platform prediction..."
python learning_module.py predict --workload '{"data_volume": "high", "complexity": "medium"}'

echo ""
echo "Step 6: Getting insights..."
python learning_module.py insights --question "What is the best platform for high volume jobs?"

echo ""
echo "Step 7: Generating reports..."
python learning_module.py report --report-type summary
python learning_module.py report --report-type cost

echo ""
echo "Step 8: Generating dashboard..."
python examples/run_dashboard.py

echo ""
echo "=========================================="
echo "DEMO COMPLETE!"
echo "Dashboard available at: output/dashboard.html"
echo "=========================================="
```

Save and run:
```bash
chmod +x complete_e2e_demo.sh
./complete_e2e_demo.sh
```

---

## Troubleshooting

### Common Errors

1. **NoRegionError**
   ```bash
   export AWS_DEFAULT_REGION=us-west-2
   ```

2. **AccessDeniedException (Bedrock)**
   ```bash
   aws iam attach-role-policy --role-name YOUR_ROLE --policy-arn arn:aws:iam::aws:policy/AmazonBedrockFullAccess
   ```

3. **NoSuchBucket**
   ```bash
   aws s3 mb s3://BUCKET_NAME --region us-west-2
   ```

4. **Glue job not found**
   - Ensure job was created in Step 3
   - Verify job name matches config exactly

5. **Not enough training samples**
   - Run at least 5 successful jobs before training

---

## Summary

| Step | Component | Purpose |
|------|-----------|---------|
| 1-3 | AWS Setup | Create buckets, roles, sample jobs |
| 4 | Configuration | Define job configs for each use case |
| 5 | Execute Existing | Run existing jobs (99% use case) |
| 6 | Agent Modes | Recommend vs Decide |
| 7 | Create New | Create jobs on-demand (rare) |
| 8 | Recommendations | Get platform advice without execution |
| 9 | Auto-Capture | Learning data captured automatically |
| 10 | Training | Train predictive model |
| 11 | Insights | Ask questions about performance |
| 12 | Dashboard | Visual reports |
| 13 | Cost Analysis | Optimize costs |
| 14 | Code Analysis | Improvement recommendations |
