# Config-Driven ETL Framework - Complete Setup Guide

**A beginner-friendly, step-by-step guide to set up and run the Strands Config-Driven ETL Framework**

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Step 1: Clone and Setup Repository](#step-1-clone-and-setup-repository)
3. [Step 2: Install Dependencies](#step-2-install-dependencies)
4. [Step 3: AWS Setup](#step-3-aws-setup)
5. [Step 4: Create Sample Data](#step-4-create-sample-data)
6. [Step 5: Configure Your First Job](#step-5-configure-your-first-job)
7. [Step 6: Run Examples](#step-6-run-examples)
8. [Step 7: Run Your First Job](#step-7-run-your-first-job)
9. [Step 8: Monitor and Validate](#step-8-monitor-and-validate)
10. [Troubleshooting](#troubleshooting)
11. [Next Steps](#next-steps)

---

## Prerequisites

Before you begin, make sure you have:

- ✅ **Python 3.8 or higher** installed
  ```bash
  python --version  # Should show 3.8+
  ```

- ✅ **pip** (Python package manager)
  ```bash
  pip --version
  ```

- ✅ **AWS Account** with access to:
  - AWS Glue
  - AWS S3
  - AWS IAM (for creating roles)
  - (Optional) EMR, Lambda, Redshift, RDS, DynamoDB

- ✅ **AWS CLI** installed and configured
  ```bash
  aws --version
  aws configure  # Set up your credentials
  ```

- ✅ **Git** (to clone the repository)
  ```bash
  git --version
  ```

---

## Step 1: Clone and Setup Repository

### 1.1 Clone the Repository

```bash
# Clone the repository
git clone https://github.com/dhilipansankaralingam-del/strands_etl.git

# Navigate to the directory
cd strands_etl

# Checkout the config-driven framework branch
git checkout claude/config-driven-framework-mklys5sfa8e4xuqc-u0a48
```

### 1.2 Verify Files

Check that all key files are present:

```bash
ls -la

# You should see:
# - etl_config.json
# - requirements.txt
# - strands_agents/
# - example_config_driven_usage.py
# - CONFIG_DRIVEN_README.md
```

---

## Step 2: Install Dependencies

### 2.1 Create Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Linux/Mac:
source venv/bin/activate

# On Windows:
# venv\Scripts\activate

# Your prompt should now show (venv)
```

### 2.2 Install Python Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install all dependencies
pip install -r requirements.txt

# This will install:
# - strands-agents (native Strands SDK)
# - boto3 (AWS SDK)
# - pandas, numpy (data processing)
# - other required packages
```

### 2.3 Verify Installation

```bash
# Verify Strands SDK is installed
python -c "import strands; print('Strands SDK installed:', strands.__version__)"

# Verify boto3 is installed
python -c "import boto3; print('boto3 installed:', boto3.__version__)"
```

**Expected output:**
```
Strands SDK installed: 1.23.0
boto3 installed: 1.26.0
```

---

## Step 3: AWS Setup

### 3.1 Configure AWS Credentials

```bash
# Run AWS configure
aws configure

# Enter your credentials when prompted:
# AWS Access Key ID: [YOUR_ACCESS_KEY]
# AWS Secret Access Key: [YOUR_SECRET_KEY]
# Default region name: us-east-1
# Default output format: json
```

### 3.2 Verify AWS Access

```bash
# Test AWS connectivity
aws sts get-caller-identity

# You should see your AWS account info
```

### 3.3 Create S3 Bucket for Data

```bash
# Create a bucket for your ETL data
aws s3 mb s3://my-etl-data-bucket-$(date +%s)

# Note the bucket name, you'll use it in config
# Example: my-etl-data-bucket-1674567890
```

### 3.4 Create Glue Database

```bash
# Create a Glue database for your tables
aws glue create-database \
  --database-input '{
    "Name": "etl_demo_db",
    "Description": "Database for ETL demo"
  }'

# Verify it was created
aws glue get-database --name etl_demo_db
```

### 3.5 Create IAM Role for Glue

Create a file `glue-trust-policy.json`:

```json
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
```

Create the role:

```bash
# Create the IAM role
aws iam create-role \
  --role-name ETLGlueRole \
  --assume-role-policy-document file://glue-trust-policy.json

# Attach AWS managed policy for Glue
aws iam attach-role-policy \
  --role-name ETLGlueRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Attach S3 full access (for demo - use more restrictive in production)
aws iam attach-role-policy \
  --role-name ETLGlueRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Get the role ARN (you'll need this later)
aws iam get-role --role-name ETLGlueRole --query 'Role.Arn' --output text
```

**Save the role ARN**, it will look like:
```
arn:aws:iam::123456789012:role/ETLGlueRole
```

---

## Step 4: Create Sample Data

### 4.1 Create Sample Data Locally

Create a file `create_sample_data.py`:

```python
"""
Create sample data for testing the ETL framework
"""
import pandas as pd
import boto3
from datetime import datetime, timedelta
import random

# Configuration
S3_BUCKET = "my-etl-data-bucket-1674567890"  # Replace with your bucket
GLUE_DATABASE = "etl_demo_db"

def create_transactions_data():
    """Create sample transactions data"""
    num_records = 10000

    data = {
        'transaction_id': [f'TXN{i:06d}' for i in range(num_records)],
        'customer_id': [f'CUST{random.randint(1, 1000):04d}' for _ in range(num_records)],
        'product_id': [f'PROD{random.randint(1, 100):03d}' for _ in range(num_records)],
        'transaction_date': [
            (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
            for _ in range(num_records)
        ],
        'amount': [round(random.uniform(10, 1000), 2) for _ in range(num_records)],
        'quantity': [random.randint(1, 10) for _ in range(num_records)],
        'unit_price': [round(random.uniform(10, 200), 2) for _ in range(num_records)]
    }

    df = pd.DataFrame(data)
    return df

def create_customers_data():
    """Create sample customers data"""
    num_customers = 1000

    data = {
        'customer_id': [f'CUST{i:04d}' for i in range(1, num_customers + 1)],
        'customer_name': [f'Customer {i}' for i in range(1, num_customers + 1)],
        'email': [f'customer{i}@example.com' for i in range(1, num_customers + 1)],
        'phone': [f'555-{random.randint(1000, 9999)}' for _ in range(num_customers)],
        'segment_id': [f'SEG{random.randint(1, 5):02d}' for _ in range(num_customers)],
        'signup_date': [
            (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
            for _ in range(num_customers)
        ]
    }

    df = pd.DataFrame(data)
    return df

def create_products_data():
    """Create sample products data"""
    num_products = 100

    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys']

    data = {
        'product_id': [f'PROD{i:03d}' for i in range(1, num_products + 1)],
        'product_name': [f'Product {i}' for i in range(1, num_products + 1)],
        'category': [random.choice(categories) for _ in range(num_products)],
        'price': [round(random.uniform(10, 500), 2) for _ in range(num_products)],
        'stock': [random.randint(0, 1000) for _ in range(num_products)]
    }

    df = pd.DataFrame(data)
    return df

def upload_to_s3_as_parquet(df, s3_path):
    """Upload DataFrame to S3 as parquet"""
    print(f"Uploading to {s3_path}...")

    # Save locally first
    local_file = '/tmp/temp_data.parquet'
    df.to_parquet(local_file, index=False)

    # Upload to S3
    s3 = boto3.client('s3')
    bucket = S3_BUCKET
    key = s3_path.replace(f's3://{bucket}/', '')

    s3.upload_file(local_file, bucket, key)
    print(f"✓ Uploaded {len(df)} records to {s3_path}")

def create_glue_table(database, table_name, s3_location, columns):
    """Create Glue catalog table"""
    print(f"Creating Glue table {database}.{table_name}...")

    glue = boto3.client('glue')

    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'PartitionKeys': []
    }

    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"✓ Created table {database}.{table_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"⚠ Table {database}.{table_name} already exists, updating...")
        glue.update_table(DatabaseName=database, TableInput=table_input)

def main():
    print("=" * 80)
    print("CREATING SAMPLE DATA FOR ETL FRAMEWORK")
    print("=" * 80)

    # 1. Create transactions data
    print("\n1. Creating transactions data...")
    transactions_df = create_transactions_data()
    s3_txn_path = f's3://{S3_BUCKET}/raw/transactions/transactions.parquet'
    upload_to_s3_as_parquet(transactions_df, s3_txn_path)

    # Create Glue table for transactions
    create_glue_table(
        database=GLUE_DATABASE,
        table_name='fact_transactions',
        s3_location=f's3://{S3_BUCKET}/raw/transactions/',
        columns=[
            {'Name': 'transaction_id', 'Type': 'string'},
            {'Name': 'customer_id', 'Type': 'string'},
            {'Name': 'product_id', 'Type': 'string'},
            {'Name': 'transaction_date', 'Type': 'string'},
            {'Name': 'amount', 'Type': 'double'},
            {'Name': 'quantity', 'Type': 'int'},
            {'Name': 'unit_price', 'Type': 'double'}
        ]
    )

    # 2. Create customers data
    print("\n2. Creating customers data...")
    customers_df = create_customers_data()
    s3_cust_path = f's3://{S3_BUCKET}/dimensions/customers/customers.parquet'
    upload_to_s3_as_parquet(customers_df, s3_cust_path)

    # Create Glue table for customers
    create_glue_table(
        database=GLUE_DATABASE,
        table_name='dim_customers',
        s3_location=f's3://{S3_BUCKET}/dimensions/customers/',
        columns=[
            {'Name': 'customer_id', 'Type': 'string'},
            {'Name': 'customer_name', 'Type': 'string'},
            {'Name': 'email', 'Type': 'string'},
            {'Name': 'phone', 'Type': 'string'},
            {'Name': 'segment_id', 'Type': 'string'},
            {'Name': 'signup_date', 'Type': 'string'}
        ]
    )

    # 3. Create products data
    print("\n3. Creating products data...")
    products_df = create_products_data()
    s3_prod_path = f's3://{S3_BUCKET}/dimensions/products/products.parquet'
    upload_to_s3_as_parquet(products_df, s3_prod_path)

    # Create Glue table for products
    create_glue_table(
        database=GLUE_DATABASE,
        table_name='dim_products',
        s3_location=f's3://{S3_BUCKET}/dimensions/products/',
        columns=[
            {'Name': 'product_id', 'Type': 'string'},
            {'Name': 'product_name', 'Type': 'string'},
            {'Name': 'category', 'Type': 'string'},
            {'Name': 'price', 'Type': 'double'},
            {'Name': 'stock', 'Type': 'int'}
        ]
    )

    print("\n" + "=" * 80)
    print("✓ SAMPLE DATA CREATED SUCCESSFULLY")
    print("=" * 80)
    print(f"\nData locations:")
    print(f"  Transactions: s3://{S3_BUCKET}/raw/transactions/")
    print(f"  Customers: s3://{S3_BUCKET}/dimensions/customers/")
    print(f"  Products: s3://{S3_BUCKET}/dimensions/products/")
    print(f"\nGlue tables:")
    print(f"  {GLUE_DATABASE}.fact_transactions")
    print(f"  {GLUE_DATABASE}.dim_customers")
    print(f"  {GLUE_DATABASE}.dim_products")

if __name__ == '__main__':
    main()
```

### 4.2 Run the Sample Data Creation

```bash
# Update the S3_BUCKET variable in the script first!
# Then run:
python create_sample_data.py

# This will:
# - Create 10,000 transactions
# - Create 1,000 customers
# - Create 100 products
# - Upload to S3
# - Create Glue catalog tables
```

### 4.3 Verify Data in AWS Console

1. Go to **AWS S3 Console**
2. Find your bucket
3. Verify folders: `raw/transactions/`, `dimensions/customers/`, `dimensions/products/`

4. Go to **AWS Glue Console** → Databases
5. Click on `etl_demo_db`
6. Verify tables: `fact_transactions`, `dim_customers`, `dim_products`

---

## Step 5: Configure Your First Job

### 5.1 Update etl_config.json

Open `etl_config.json` and update the variables section:

```json
{
  "variables": {
    "data_bucket": "my-etl-data-bucket-1674567890",  // YOUR BUCKET
    "raw_prefix": "raw",
    "processed_prefix": "processed",
    "dimensions_prefix": "dimensions",
    "glue_database": "etl_demo_db",  // YOUR DATABASE
    "environment": "dev"
  }
}
```

### 5.2 Create a Simple PySpark Script

Create `pyscript/customer_order_summary_glue.py`:

```python
"""
Simple Customer Order Summary ETL Job
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum, count, avg

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

print("=" * 80)
print("CUSTOMER ORDER SUMMARY ETL JOB")
print("=" * 80)

# Read data from Glue Catalog
print("\n1. Reading transactions...")
transactions = glueContext.create_dynamic_frame.from_catalog(
    database="etl_demo_db",
    table_name="fact_transactions"
).toDF()

print(f"✓ Loaded {transactions.count()} transactions")

print("\n2. Reading customers...")
customers = glueContext.create_dynamic_frame.from_catalog(
    database="etl_demo_db",
    table_name="dim_customers"
).toDF()

print(f"✓ Loaded {customers.count()} customers")

print("\n3. Reading products...")
products = glueContext.create_dynamic_frame.from_catalog(
    database="etl_demo_db",
    table_name="dim_products"
).toDF()

print(f"✓ Loaded {products.count()} products")

# Join and aggregate
print("\n4. Joining and aggregating...")
result = transactions \
    .join(customers, "customer_id") \
    .join(products, "product_id") \
    .groupBy("customer_id", "customer_name", "category") \
    .agg(
        sum("amount").alias("total_amount"),
        count("transaction_id").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    )

print(f"✓ Aggregated to {result.count()} rows")

# Show sample
print("\n5. Sample results:")
result.show(10)

# Write output
output_path = "s3://my-etl-data-bucket-1674567890/processed/customer_order_summary/"  # UPDATE THIS
print(f"\n6. Writing output to {output_path}")
result.write.mode("overwrite").parquet(output_path)

print("\n✓ JOB COMPLETED SUCCESSFULLY")

job.commit()
```

**Important**: Update the `output_path` with your bucket name!

---

## Step 6: Run Examples

### 6.1 Run Configuration Validation

```bash
# Run the example to validate configuration
python -c "
from strands_agents.orchestrator.config_loader import ConfigLoader

loader = ConfigLoader('./etl_config.json')
config = loader.load()

print('✓ Configuration loaded successfully')
print(f'  Total jobs: {len(config[\"jobs\"])}')

validation = loader.validate_config()
if validation['valid']:
    print('✓ Configuration is valid')
else:
    print('✗ Configuration has errors:')
    for error in validation['errors']:
        print(f'  - {error}')
"
```

**Expected output:**
```
✓ Configuration loaded successfully
  Total jobs: 3
✓ Configuration is valid
```

### 6.2 Test Auto-Size Detection

```bash
# Test auto-detection of table sizes
python -c "
from strands_agents.tools.catalog_tools import detect_glue_table_size

result = detect_glue_table_size(
    database='etl_demo_db',
    table='fact_transactions',
    aws_region='us-east-1'
)

print('Auto-Detection Result:')
print(f'  Table: {result[\"table\"]}')
print(f'  Size: {result[\"size_gb\"]} GB')
print(f'  Files: {result[\"num_files\"]}')
print(f'  Rows: {result[\"num_rows\"]}')
print(f'  Broadcast eligible: {result[\"broadcast_eligible\"]}')
"
```

### 6.3 Run Full Example

```bash
# Run the comprehensive examples
python example_config_driven_usage.py

# This will show:
# - Configuration loading
# - Validation
# - Job inspection
# - Auto-detection examples
# - Data flow analysis examples
```

---

## Step 7: Run Your First Job

### 7.1 Create Glue Job (One-time Setup)

```bash
# Create the Glue job
aws glue create-job \
  --name customer-order-summary-etl \
  --role ETLGlueRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://my-etl-data-bucket-1674567890/scripts/customer_order_summary_glue.py",
    "PythonVersion": "3"
  }' \
  --glue-version "3.0" \
  --number-of-workers 2 \
  --worker-type "G.1X"

# Upload your script to S3
aws s3 cp pyscript/customer_order_summary_glue.py \
  s3://my-etl-data-bucket-1674567890/scripts/customer_order_summary_glue.py
```

### 7.2 Run Job Using Config-Driven Orchestrator

Create a test script `run_job.py`:

```python
"""
Run a job using the config-driven orchestrator
"""
from strands_agents.orchestrator.config_driven_orchestrator import ConfigDrivenOrchestrator

print("=" * 80)
print("RUNNING CONFIG-DRIVEN ETL JOB")
print("=" * 80)

# Initialize orchestrator
print("\n1. Initializing orchestrator...")
orchestrator = ConfigDrivenOrchestrator(
    config_path='./etl_config.json',
    enable_auto_detection=True
)

print("✓ Orchestrator initialized")

# Run the job
print("\n2. Running job: customer_order_summary")
result = orchestrator.run_job_by_id('customer_order_summary')

# Display results
print("\n" + "=" * 80)
print("EXECUTION RESULTS")
print("=" * 80)

print(f"\nJob: {result.get('job_name', 'Unknown')}")
print(f"Status: {result.get('status', 'unknown')}")

# Pre-execution analysis
pre_analysis = result.get('pre_execution_analysis', {})
print(f"\n📊 Pre-Execution Analysis:")
print(f"  Data volume: {pre_analysis.get('total_data_volume_gb', 0)} GB")
print(f"  Auto-detected: {pre_analysis.get('auto_detected', False)}")

# Platform decision
platform_decision = result.get('platform_decision', {})
print(f"\n🖥️ Platform Decision:")
print(f"  Final platform: {platform_decision.get('final_platform', 'unknown')}")
print(f"  Reason: {platform_decision.get('decision_reason', 'N/A')}")

print("\n✓ Job execution initiated")
```

Run it:

```bash
python run_job.py
```

### 7.3 Alternative: Run Job Manually (Simpler for Testing)

```bash
# Just start the Glue job manually
aws glue start-job-run --job-name customer-order-summary-etl

# Get the job run ID from output
# Then monitor it:
aws glue get-job-run \
  --job-name customer-order-summary-etl \
  --run-id <JOB_RUN_ID>
```

---

## Step 8: Monitor and Validate

### 8.1 Monitor Glue Job

```bash
# List all runs of your job
aws glue get-job-runs --job-name customer-order-summary-etl

# Get status of specific run
aws glue get-job-run \
  --job-name customer-order-summary-etl \
  --run-id <JOB_RUN_ID> \
  --query 'JobRun.JobRunState' \
  --output text
```

### 8.2 Check Output Data

```bash
# List output files
aws s3 ls s3://my-etl-data-bucket-1674567890/processed/customer_order_summary/ --recursive

# Download a sample output file
aws s3 cp \
  s3://my-etl-data-bucket-1674567890/processed/customer_order_summary/part-00000.parquet \
  /tmp/output.parquet

# Read it with pandas
python -c "
import pandas as pd
df = pd.read_parquet('/tmp/output.parquet')
print('Output data:')
print(df.head())
print(f'\nTotal rows: {len(df)}')
"
```

### 8.3 Validate Data Quality

The framework automatically runs quality checks defined in your config. Check the logs for quality validation results.

---

## Troubleshooting

### Issue 1: "strands module not found"

**Solution:**
```bash
# Make sure you activated the virtual environment
source venv/bin/activate

# Reinstall strands
pip install strands-agents
```

### Issue 2: "Access Denied" when accessing S3

**Solution:**
```bash
# Check your AWS credentials
aws sts get-caller-identity

# Make sure your IAM user/role has S3 permissions
# Or use:
aws s3 ls s3://your-bucket-name/
```

### Issue 3: "Table not found in Glue Catalog"

**Solution:**
```bash
# Verify table exists
aws glue get-table \
  --database-name etl_demo_db \
  --name fact_transactions

# If not, run create_sample_data.py again
```

### Issue 4: "No module named awsglue"

This is **NORMAL** when running locally. The `awsglue` module only exists in the AWS Glue environment. Your PySpark script will work when run in Glue.

For local testing, you can mock it:
```python
try:
    from awsglue.context import GlueContext
except ImportError:
    print("Running locally, awsglue not available")
    # Use regular PySpark instead
```

### Issue 5: Glue Job Fails

**Check CloudWatch Logs:**
```bash
# Go to AWS CloudWatch Console
# → Log groups
# → /aws-glue/jobs/output or /aws-glue/jobs/error
# → Find your job run logs
```

Common fixes:
- Check script path is correct
- Verify S3 bucket names in script
- Ensure IAM role has necessary permissions
- Check Glue table names match your database

---

## Next Steps

### 1. Customize Your Config

Edit `etl_config.json` to add your own jobs:

```json
{
  "job_id": "my_custom_job",
  "job_name": "My Custom ETL Job",
  "enabled": true,
  "data_sources": [
    {
      "name": "my_table",
      "source_type": "glue_catalog",
      "database": "my_db",
      "table": "my_table",
      "auto_detect_size": true
    }
  ]
}
```

### 2. Add More Data Sources

The framework supports:
- AWS Glue Catalog tables
- S3 paths
- Redshift tables
- RDS tables
- DynamoDB tables
- Kinesis streams

See `CONFIG_DRIVEN_README.md` for examples of each.

### 3. Configure Data Quality Rules

Add custom validation rules:

```json
{
  "data_quality": {
    "completeness_checks": [
      {
        "field": "my_field",
        "required": true,
        "null_threshold_percent": 0,
        "severity": "critical"
      }
    ],
    "accuracy_rules": [
      {
        "field": "email",
        "rule_type": "regex",
        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
      }
    ]
  }
}
```

### 4. Set Up Cost Tracking

Configure cost budgets and alerts:

```json
{
  "cost": {
    "budget_per_run_usd": 10.0,
    "cost_alerts": {
      "enabled": true,
      "threshold_percent": 90,
      "alert_channels": ["email"]
    }
  }
}
```

### 5. Enable Learning Agent

The framework learns from previous runs and optimizes automatically:

```json
{
  "global_settings": {
    "enable_agent_learning": true,
    "learning_storage": {
      "type": "s3",
      "bucket": "my-learning-bucket",
      "prefix": "execution-history/"
    }
  }
}
```

### 6. Explore Advanced Features

- **Data Flow Analysis**: Automatic join optimization
- **Auto-Platform Selection**: Let agents choose the best platform
- **Template Inheritance**: Reuse common configurations
- **Variable Substitution**: Dynamic configs with `${variables.key}`

See `CONFIG_DRIVEN_README.md` for complete documentation.

---

## Summary

You've now set up the complete config-driven ETL framework! 🎉

**What you've accomplished:**
1. ✅ Installed all dependencies
2. ✅ Set up AWS resources (S3, Glue, IAM)
3. ✅ Created sample data
4. ✅ Configured your first job
5. ✅ Run examples and validated setup
6. ✅ Executed your first ETL job

**Next steps:**
- Add your own data sources
- Customize configurations
- Set up data quality rules
- Enable cost tracking
- Deploy to production

**Need help?**
- See `CONFIG_DRIVEN_README.md` for detailed documentation
- Check `example_config_driven_usage.py` for code examples
- Review `STRANDS_NATIVE_README.md` for agent details

Happy ETL-ing! 🚀
