# Complete Manual Setup Guide - Strands ETL Framework V2

**Estimated Time:** 2-3 hours
**Prerequisites:** AWS Account, Python 3.8+, AWS CLI configured

---

## Table of Contents

1. [Prerequisites & Access Verification](#step-1-prerequisites--access-verification)
2. [Create S3 Buckets](#step-2-create-s3-buckets)
3. [Create Glue Database](#step-3-create-glue-database)
4. [Create Sample Data](#step-4-create-sample-data)
5. [Create IAM Roles](#step-5-create-iam-roles)
6. [Enable Bedrock Model Access](#step-6-enable-bedrock-model-access)
7. [Setup Python Environment](#step-7-setup-python-environment)
8. [Validate Configuration](#step-8-validate-configuration)
9. [Test the Orchestrator](#step-9-test-the-orchestrator)
10. [Run End-to-End Demo](#step-10-run-end-to-end-demo)
11. [Verification & Validation](#step-11-verification--validation)
12. [Troubleshooting](#step-12-troubleshooting)

---

## Step 1: Prerequisites & Access Verification

### 1.1 Verify AWS CLI Installation

```bash
aws --version
```

**Expected Output:**
```
aws-cli/2.x.x Python/3.x.x Linux/x.x.x
```

### 1.2 Verify AWS Credentials

```bash
aws sts get-caller-identity
```

**Expected Output:**
```json
{
    "UserId": "AIDAXXXXXXXXXXXXXXXXX",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-username"
}
```

### 1.3 Verify Bedrock Access

```bash
aws bedrock list-foundation-models --query "modelSummaries[?contains(modelId, 'claude')].[modelId]" --output table
```

**Expected Output:**
```
--------------------------------------------------
|             ListFoundationModels               |
+------------------------------------------------+
|  anthropic.claude-3-sonnet-20240229-v1:0       |
|  anthropic.claude-3-haiku-20240307-v1:0        |
+------------------------------------------------+
```

---

## Step 2: Create S3 Buckets

### 2.1 Create Required Buckets

Replace `YOUR_UNIQUE_PREFIX` with your own prefix (e.g., `mycompany-dev`):

```bash
# Set your unique prefix
export BUCKET_PREFIX="strands-etl-demo"

# Create buckets
aws s3 mb s3://${BUCKET_PREFIX}-data --region us-east-1
aws s3 mb s3://${BUCKET_PREFIX}-scripts --region us-east-1
aws s3 mb s3://${BUCKET_PREFIX}-config --region us-east-1
aws s3 mb s3://${BUCKET_PREFIX}-learning --region us-east-1
```

**Expected Output (for each):**
```
make_bucket: strands-etl-demo-data
```

### 2.2 Verify Buckets

```bash
aws s3 ls | grep ${BUCKET_PREFIX}
```

**Expected Output:**
```
2024-01-15 10:30:00 strands-etl-demo-data
2024-01-15 10:30:01 strands-etl-demo-scripts
2024-01-15 10:30:02 strands-etl-demo-config
2024-01-15 10:30:03 strands-etl-demo-learning
```

---

## Step 3: Create Glue Database

### 3.1 Create Database

```bash
aws glue create-database --database-input '{"Name": "ecommerce_db", "Description": "E-commerce data for Strands ETL demo"}'
```

### 3.2 Create Analytics Database

```bash
aws glue create-database --database-input '{"Name": "analytics_db", "Description": "Analytics output for Strands ETL demo"}'
```

### 3.3 Verify Databases

```bash
aws glue get-databases --query "DatabaseList[*].Name" --output table
```

**Expected Output:**
```
-----------------
|  GetDatabases |
+---------------+
|  ecommerce_db |
|  analytics_db |
+---------------+
```

---

## Step 4: Create Sample Data

### 4.1 Create Sample Data Script

Create file `create_sample_data.py`:

```python
"""
Sample Data Generator for Strands ETL Demo
Creates customers, orders, and products tables in Glue
"""

import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import random

# Configuration - UPDATE THESE VALUES
BUCKET_NAME = "strands-etl-demo-data"  # Your S3 bucket name
DATABASE_NAME = "ecommerce_db"          # Your Glue database name
REGION = "us-east-1"

# Initialize clients
s3 = boto3.client('s3', region_name=REGION)
glue = boto3.client('glue', region_name=REGION)

def create_customers_data(num_records=1000):
    """Generate sample customer data."""
    regions = ['North America', 'Europe', 'Asia', 'South America', 'Australia']

    customers = pd.DataFrame({
        'customer_id': [f'CUST{str(i).zfill(6)}' for i in range(1, num_records + 1)],
        'name': [f'Customer {i}' for i in range(1, num_records + 1)],
        'email': [f'customer{i}@example.com' for i in range(1, num_records + 1)],
        'region': [random.choice(regions) for _ in range(num_records)],
        'signup_date': [(datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d') for _ in range(num_records)],
        'active': [random.choice([True, True, True, False]) for _ in range(num_records)]  # 75% active
    })
    return customers

def create_products_data(num_records=100):
    """Generate sample product data."""
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports']

    products = pd.DataFrame({
        'product_id': [f'PROD{str(i).zfill(4)}' for i in range(1, num_records + 1)],
        'product_name': [f'Product {i}' for i in range(1, num_records + 1)],
        'category': [random.choice(categories) for _ in range(num_records)],
        'price': [round(random.uniform(10, 500), 2) for _ in range(num_records)],
        'cost': [round(random.uniform(5, 250), 2) for _ in range(num_records)],
        'supplier_id': [f'SUP{str(random.randint(1, 20)).zfill(3)}' for _ in range(num_records)],
        'active': [random.choice([True, True, True, False]) for _ in range(num_records)]
    })
    return products

def create_orders_data(num_records=10000, num_customers=1000, num_products=100):
    """Generate sample order data."""
    statuses = ['completed', 'completed', 'completed', 'pending', 'cancelled']  # Mostly completed

    orders = pd.DataFrame({
        'order_id': [f'ORD{str(i).zfill(8)}' for i in range(1, num_records + 1)],
        'customer_id': [f'CUST{str(random.randint(1, num_customers)).zfill(6)}' for _ in range(num_records)],
        'product_id': [f'PROD{str(random.randint(1, num_products)).zfill(4)}' for _ in range(num_records)],
        'order_date': [(datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d') for _ in range(num_records)],
        'quantity': [random.randint(1, 10) for _ in range(num_records)],
        'total_amount': [round(random.uniform(20, 1000), 2) for _ in range(num_records)],
        'status': [random.choice(statuses) for _ in range(num_records)]
    })
    return orders

def upload_to_s3(df, table_name):
    """Upload DataFrame to S3 as Parquet."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_key = f"raw/{table_name}/{table_name}.parquet"
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
    print(f"✓ Uploaded {table_name} to s3://{BUCKET_NAME}/{s3_key}")
    return s3_key

def create_glue_table(table_name, columns, s3_location):
    """Create Glue table."""
    try:
        glue.delete_table(DatabaseName=DATABASE_NAME, Name=table_name)
        print(f"  Deleted existing table: {table_name}")
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_table(
        DatabaseName=DATABASE_NAME,
        TableInput={
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
            'TableType': 'EXTERNAL_TABLE'
        }
    )
    print(f"✓ Created Glue table: {DATABASE_NAME}.{table_name}")

def main():
    print("=" * 60)
    print("STRANDS ETL - Sample Data Generator")
    print("=" * 60)
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Database: {DATABASE_NAME}")
    print("-" * 60)

    # Generate data
    print("\n[1/6] Generating customers data...")
    customers = create_customers_data(1000)
    print(f"  Generated {len(customers)} customers")

    print("\n[2/6] Generating products data...")
    products = create_products_data(100)
    print(f"  Generated {len(products)} products")

    print("\n[3/6] Generating orders data...")
    orders = create_orders_data(10000)
    print(f"  Generated {len(orders)} orders")

    # Upload to S3
    print("\n[4/6] Uploading to S3...")
    upload_to_s3(customers, 'customers')
    upload_to_s3(products, 'products')
    upload_to_s3(orders, 'orders')

    # Create Glue tables
    print("\n[5/6] Creating Glue tables...")

    create_glue_table('customers', [
        {'Name': 'customer_id', 'Type': 'string'},
        {'Name': 'name', 'Type': 'string'},
        {'Name': 'email', 'Type': 'string'},
        {'Name': 'region', 'Type': 'string'},
        {'Name': 'signup_date', 'Type': 'string'},
        {'Name': 'active', 'Type': 'boolean'}
    ], f's3://{BUCKET_NAME}/raw/customers/')

    create_glue_table('products', [
        {'Name': 'product_id', 'Type': 'string'},
        {'Name': 'product_name', 'Type': 'string'},
        {'Name': 'category', 'Type': 'string'},
        {'Name': 'price', 'Type': 'double'},
        {'Name': 'cost', 'Type': 'double'},
        {'Name': 'supplier_id', 'Type': 'string'},
        {'Name': 'active', 'Type': 'boolean'}
    ], f's3://{BUCKET_NAME}/raw/products/')

    create_glue_table('orders', [
        {'Name': 'order_id', 'Type': 'string'},
        {'Name': 'customer_id', 'Type': 'string'},
        {'Name': 'product_id', 'Type': 'string'},
        {'Name': 'order_date', 'Type': 'string'},
        {'Name': 'quantity', 'Type': 'int'},
        {'Name': 'total_amount', 'Type': 'double'},
        {'Name': 'status', 'Type': 'string'}
    ], f's3://{BUCKET_NAME}/raw/orders/')

    # Verify
    print("\n[6/6] Verification...")
    tables = glue.get_tables(DatabaseName=DATABASE_NAME)
    print(f"✓ Tables in {DATABASE_NAME}:")
    for table in tables['TableList']:
        print(f"  - {table['Name']}")

    print("\n" + "=" * 60)
    print("✓ Sample data creation complete!")
    print("=" * 60)

if __name__ == '__main__':
    main()
```

### 4.2 Run the Script

```bash
python create_sample_data.py
```

**Expected Output:**
```
============================================================
STRANDS ETL - Sample Data Generator
============================================================
Bucket: strands-etl-demo-data
Database: ecommerce_db
------------------------------------------------------------

[1/6] Generating customers data...
  Generated 1000 customers

[2/6] Generating products data...
  Generated 100 products

[3/6] Generating orders data...
  Generated 10000 orders

[4/6] Uploading to S3...
✓ Uploaded customers to s3://strands-etl-demo-data/raw/customers/customers.parquet
✓ Uploaded products to s3://strands-etl-demo-data/raw/products/products.parquet
✓ Uploaded orders to s3://strands-etl-demo-data/raw/orders/orders.parquet

[5/6] Creating Glue tables...
✓ Created Glue table: ecommerce_db.customers
✓ Created Glue table: ecommerce_db.products
✓ Created Glue table: ecommerce_db.orders

[6/6] Verification...
✓ Tables in ecommerce_db:
  - customers
  - products
  - orders

============================================================
✓ Sample data creation complete!
============================================================
```

---

## Step 5: Create IAM Roles

### 5.1 Create Glue Service Role

```bash
# Create trust policy file
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

# Create the role
aws iam create-role \
    --role-name StrandsETLDemoRole \
    --assume-role-policy-document file:///tmp/glue-trust-policy.json

# Attach policies
aws iam attach-role-policy \
    --role-name StrandsETLDemoRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --role-name StrandsETLDemoRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

**Expected Output:**
```json
{
    "Role": {
        "RoleName": "StrandsETLDemoRole",
        "Arn": "arn:aws:iam::123456789012:role/StrandsETLDemoRole"
    }
}
```

### 5.2 Verify Role

```bash
aws iam get-role --role-name StrandsETLDemoRole --query "Role.Arn" --output text
```

---

## Step 6: Enable Bedrock Model Access

### 6.1 Request Model Access (AWS Console)

1. Go to AWS Console → Amazon Bedrock
2. Click "Model access" in the left sidebar
3. Click "Manage model access"
4. Select **Anthropic Claude** models
5. Click "Request model access"
6. Wait for approval (usually instant)

### 6.2 Verify Model Access

```bash
aws bedrock list-foundation-models \
    --query "modelSummaries[?contains(modelId, 'claude-3')].modelId" \
    --output table
```

**Expected Output:**
```
----------------------------------------------------
|              ListFoundationModels                |
+--------------------------------------------------+
|  anthropic.claude-3-sonnet-20240229-v1:0         |
|  anthropic.claude-3-haiku-20240307-v1:0          |
+--------------------------------------------------+
```

---

## Step 7: Setup Python Environment

### 7.1 Create Virtual Environment

```bash
cd /path/to/strands_etl

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 7.2 Install Dependencies

```bash
pip install --upgrade pip
pip install boto3 pandas pyarrow
```

### 7.3 Verify Installation

```python
python -c "
import boto3
import pandas
import json
print('✓ boto3:', boto3.__version__)
print('✓ pandas:', pandas.__version__)
print('✓ All dependencies installed successfully')
"
```

**Expected Output:**
```
✓ boto3: 1.34.x
✓ pandas: 2.x.x
✓ All dependencies installed successfully
```

---

## Step 8: Validate Configuration

### 8.1 Test Config Loader

```bash
python -c "
from config_loader import ConfigLoader

loader = ConfigLoader('./config/test_config.json')
config = loader.load()

print('✓ Configuration loaded successfully')
print(f'  Jobs defined: {len(config[\"jobs\"])}')

validation = loader.validate_config()
if validation['valid']:
    print('✓ Configuration is valid')
    print(f'  Enabled jobs: {validation[\"enabled_jobs\"]}')
else:
    print('✗ Configuration has errors:')
    for error in validation['errors']:
        print(f'  - {error}')
"
```

**Expected Output:**
```
✓ Configuration loaded successfully
  Jobs defined: 3
✓ Configuration is valid
  Enabled jobs: 2
```

### 8.2 List Jobs in Config

```bash
python -c "
from config_loader import ConfigLoader
import json

loader = ConfigLoader('./config/test_config.json')
config = loader.load()

print('Jobs in configuration:')
print('-' * 50)
for job in config['jobs']:
    status = '✓ Enabled' if job.get('enabled', True) else '✗ Disabled'
    print(f\"{status}: {job['name']}\")
    print(f\"   Description: {job.get('description', 'N/A')}\")
    print(f\"   Platform: {job.get('platform', {}).get('preferred', 'N/A')}\")
    print()
"
```

**Expected Output:**
```
Jobs in configuration:
--------------------------------------------------
✓ Enabled: customer_order_summary_etl
   Description: Multi-table join ETL: customers + orders + products to customer_order_summary
   Platform: glue

✓ Enabled: sales_analytics_etl
   Description: Sales data aggregation and analytics pipeline
   Platform: glue

✗ Disabled: inventory_sync_etl
   Description: Inventory synchronization pipeline (disabled)
   Platform: lambda
```

---

## Step 9: Test the Orchestrator

### 9.1 Test Orchestrator Import

```bash
python -c "
from orchestrator.strands_orchestrator import StrandsOrchestrator

orchestrator = StrandsOrchestrator()
print('✓ StrandsOrchestrator initialized successfully')
print('  Available agents:', list(orchestrator.agents.keys()))
"
```

**Expected Output:**
```
✓ StrandsOrchestrator initialized successfully
  Available agents: ['orchestrator', 'decision', 'execution', 'quality', 'optimization', 'learning']
```

### 9.2 Test Config Loading

```bash
python -c "
from orchestrator.strands_orchestrator import StrandsOrchestrator
import json

orchestrator = StrandsOrchestrator()
config = orchestrator.load_config('./config/demo_config.json')

print('✓ Demo config loaded successfully')
print(f\"  Workload: {config['workload']['name']}\")
print(f\"  Platform: {config['platform']['preferred']}\")
print(f\"  Data sources: {len(config['workload'].get('data_sources', []))}\")
"
```

**Expected Output:**
```
✓ Demo config loaded successfully
  Workload: customer_order_summary_etl
  Platform: glue
  Data sources: 3
```

---

## Step 10: Run End-to-End Demo

### 10.1 Run ETL Wrapper (Validation Only)

```bash
python etl_wrapper.py ./config/test_config.json
```

**Expected Output:**
```
============================================================
STRANDS ETL FRAMEWORK
============================================================

[Step 1] Loading and validating configuration...
✓ Configuration loaded successfully
  Jobs defined: 3
  Enabled jobs: 2

[Step 2] Enabled jobs:
  - customer_order_summary_etl: Multi-table join ETL: customers + orders + products to customer_order_summary
  - sales_analytics_etl: Sales data aggregation and analytics pipeline

[Step 3] Ready to execute jobs.
Use '--run' flag to execute jobs: python etl_wrapper.py ./config/test_config.json --run
```

### 10.2 Run Full Pipeline (with AWS resources)

> **Note:** This will create actual AWS Glue jobs. Ensure IAM roles and S3 buckets are set up.

```bash
python etl_wrapper.py ./config/test_config.json --run
```

### 10.3 Run Orchestrator Directly

```bash
python -c "
from orchestrator.strands_orchestrator import StrandsOrchestrator, DateTimeEncoder
import json

orchestrator = StrandsOrchestrator()

print('Starting pipeline orchestration...')
print('-' * 50)

result = orchestrator.orchestrate_pipeline(
    user_request='Process customer data with quality checks and compliance masking',
    config_path='./config/demo_config.json'
)

print('Pipeline Result:')
print(f\"  Status: {result.get('status')}\")
print(f\"  Pipeline ID: {result.get('pipeline_id')}\")

if result.get('error'):
    print(f\"  Error: {result.get('error')}\")
"
```

---

## Step 11: Verification & Validation

### 11.1 Verify S3 Data

```bash
aws s3 ls s3://strands-etl-demo-data/raw/ --recursive
```

**Expected Output:**
```
2024-01-15 10:30:00     125000 raw/customers/customers.parquet
2024-01-15 10:30:01      12000 raw/products/products.parquet
2024-01-15 10:30:02    1250000 raw/orders/orders.parquet
```

### 11.2 Verify Glue Tables

```bash
aws glue get-tables --database-name ecommerce_db --query "TableList[*].[Name,StorageDescriptor.Location]" --output table
```

**Expected Output:**
```
-----------------------------------------------------------------
|                          GetTables                            |
+------------+--------------------------------------------------+
|  customers |  s3://strands-etl-demo-data/raw/customers/       |
|  products  |  s3://strands-etl-demo-data/raw/products/        |
|  orders    |  s3://strands-etl-demo-data/raw/orders/          |
+------------+--------------------------------------------------+
```

### 11.3 Query Sample Data with Athena

```bash
aws athena start-query-execution \
    --query-string "SELECT COUNT(*) as total_orders FROM ecommerce_db.orders" \
    --result-configuration "OutputLocation=s3://strands-etl-demo-data/athena-results/"
```

---

## Step 12: Troubleshooting

### Common Errors and Solutions

#### Error: "NoSuchBucket"
```
botocore.exceptions.ClientError: An error occurred (NoSuchBucket)
```
**Solution:** Create the S3 bucket first:
```bash
aws s3 mb s3://your-bucket-name
```

#### Error: "Database not found"
```
glue.exceptions.EntityNotFoundException: Database not found
```
**Solution:** Create the Glue database:
```bash
aws glue create-database --database-input '{"Name": "ecommerce_db"}'
```

#### Error: "Access Denied"
```
botocore.exceptions.ClientError: An error occurred (AccessDenied)
```
**Solution:** Check IAM permissions and ensure your credentials have access to:
- S3 buckets
- Glue databases
- Bedrock models

#### Error: "ModuleNotFoundError"
```
ModuleNotFoundError: No module named 'xxx'
```
**Solution:** Install missing dependencies:
```bash
pip install boto3 pandas pyarrow
```

#### Error: "Configuration validation failed"
```
✗ Configuration has errors
```
**Solution:** Check your config file has all required fields:
- `jobs` array with at least one job
- Each job needs: `name`, `workload`, `platform`, `data_sources`, `target`

---

## File Reference

| File | Purpose |
|------|---------|
| `config/test_config.json` | Config for validation (Step 8) - has `jobs` array |
| `config/demo_config.json` | Config for orchestrator demo (Step 10) - has `workload` at root |
| `config_loader.py` | Config loading and validation utility |
| `etl_wrapper.py` | Integrated ETL runner |
| `orchestrator/strands_orchestrator.py` | Main orchestration engine |

---

## Quick Command Reference

```bash
# Validate config
python -c "from config_loader import ConfigLoader; l=ConfigLoader('./config/test_config.json'); l.load(); print(l.validate_config())"

# List jobs
python etl_wrapper.py ./config/test_config.json

# Run jobs
python etl_wrapper.py ./config/test_config.json --run

# Direct orchestrator
python -c "from orchestrator.strands_orchestrator import StrandsOrchestrator; o=StrandsOrchestrator(); print(o.orchestrate_pipeline('Test', './config/demo_config.json'))"
```

---

## Success Checklist

- [ ] AWS CLI configured and working
- [ ] S3 buckets created
- [ ] Glue databases created
- [ ] Sample data uploaded to S3
- [ ] Glue tables created
- [ ] IAM roles configured
- [ ] Bedrock model access enabled
- [ ] Python environment set up
- [ ] Config validation passing
- [ ] Orchestrator import working
- [ ] ETL wrapper running

---

**Congratulations!** You have successfully set up the Strands ETL Framework.
