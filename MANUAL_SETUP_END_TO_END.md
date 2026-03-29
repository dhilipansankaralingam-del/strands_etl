# Complete Manual Setup Guide - Config-Driven ETL Framework
## Step-by-Step Instructions for End-to-End Implementation

**Last Updated:** 2026-01-27
**Estimated Time:** 2-3 hours for complete setup
**Difficulty:** Beginner-Friendly

---

## Table of Contents

1. [Prerequisites & Access Verification](#step-1-prerequisites--access-verification)
2. [AWS S3 Bucket Setup](#step-2-aws-s3-bucket-setup)
3. [AWS Glue Database Creation](#step-3-aws-glue-database-creation)
4. [Sample Data Creation & Upload](#step-4-sample-data-creation--upload)
5. [IAM Roles & Permissions](#step-5-iam-roles--permissions)
6. [Bedrock Agents Setup](#step-6-bedrock-agents-setup)
7. [Python Environment Setup](#step-7-python-environment-setup)
8. [Configuration File Setup](#step-8-configuration-file-setup)
9. [Lambda Functions (Optional)](#step-9-lambda-functions-optional)
10. [First End-to-End Execution](#step-10-first-end-to-end-execution)
11. [Validation & Testing](#step-11-validation--testing)
12. [Troubleshooting](#step-12-troubleshooting)

---

## Step 1: Prerequisites & Access Verification

### 1.1 Check AWS CLI Installation

Open your terminal and run:

```bash
aws --version
```

**Expected output:**
```
aws-cli/2.x.x Python/3.x.x ...
```

**If not installed:**
- Download from: https://aws.amazon.com/cli/
- Install and run `aws configure`

### 1.2 Configure AWS Credentials

```bash
aws configure
```

**Enter when prompted:**
```
AWS Access Key ID: [YOUR_ACCESS_KEY]
AWS Secret Access Key: [YOUR_SECRET_KEY]
Default region name: us-east-1
Default output format: json
```

### 1.3 Verify AWS Access

```bash
# Check your identity
aws sts get-caller-identity

# Should show:
# {
#   "UserId": "...",
#   "Account": "123456789012",
#   "Arn": "arn:aws:iam::123456789012:user/your-user"
# }
```

**✅ Copy your Account ID** - you'll need it later: `123456789012`

### 1.4 Verify Bedrock Access

```bash
# List Bedrock models (if you have access)
aws bedrock list-foundation-models --region us-east-1

# If you get an error about Bedrock not being available, you need to:
# 1. Go to AWS Console → Bedrock
# 2. Request model access
```

### 1.5 Check Python Installation

```bash
python3 --version

# Expected: Python 3.8 or higher
```

---

## Step 2: AWS S3 Bucket Setup

### 2.1 Choose a Bucket Name

**Rules:**
- Must be globally unique
- Only lowercase letters, numbers, hyphens
- 3-63 characters

**Suggestion:**
```
etl-demo-<your-name>-<random-number>
Example: etl-demo-john-2024
```

### 2.2 Create S3 Bucket via AWS Console

**Option A: AWS Console (Easiest)**

1. Go to **AWS Console** → **S3**
2. Click **"Create bucket"**
3. **Bucket name:** `etl-demo-yourname-2024` (replace with your name)
4. **AWS Region:** `us-east-1`
5. **Block Public Access:** Keep all boxes checked (default)
6. **Bucket Versioning:** Disabled (default)
7. **Tags:** (optional)
   - Key: `Project`, Value: `ETL-Demo`
8. Click **"Create bucket"**

**Option B: AWS CLI**

```bash
# Set your bucket name
export BUCKET_NAME="etl-demo-yourname-2024"

# Create bucket
aws s3 mb s3://$BUCKET_NAME --region us-east-1

# Verify
aws s3 ls | grep $BUCKET_NAME
```

### 2.3 Create Folder Structure

```bash
# Create folders in your bucket
aws s3api put-object --bucket $BUCKET_NAME --key raw/transactions/
aws s3api put-object --bucket $BUCKET_NAME --key raw/customers/
aws s3api put-object --bucket $BUCKET_NAME --key dimensions/customers/
aws s3api put-object --bucket $BUCKET_NAME --key dimensions/products/
aws s3api put-object --bucket $BUCKET_NAME --key processed/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/

# Verify folders were created
aws s3 ls s3://$BUCKET_NAME/
```

**Expected output:**
```
                           PRE dimensions/
                           PRE processed/
                           PRE raw/
                           PRE scripts/
```

**✅ Save your bucket name:** `etl-demo-yourname-2024`

---

## Step 3: AWS Glue Database Creation

### 3.1 Create Glue Database via AWS Console

1. Go to **AWS Console** → **AWS Glue**
2. In left menu, click **"Databases"**
3. Click **"Add database"**
4. **Database name:** `etl_demo_db`
5. **Description:** `ETL Demo Database for Config-Driven Framework`
6. **Location:** Leave empty
7. Click **"Create"**

### 3.2 Create Glue Database via AWS CLI

```bash
# Create database
aws glue create-database \
  --database-input '{
    "Name": "etl_demo_db",
    "Description": "ETL Demo Database for Config-Driven Framework"
  }' \
  --region us-east-1

# Verify
aws glue get-database --name etl_demo_db --region us-east-1
```

**Expected output:**
```json
{
    "Database": {
        "Name": "etl_demo_db",
        "Description": "ETL Demo Database...",
        "CreateTime": "..."
    }
}
```

**✅ Save your database name:** `etl_demo_db`

---

## Step 4: Sample Data Creation & Upload

### 4.1 Create Local Sample Data Script

Create a file named `create_sample_data_local.py` in your project directory:

```python
"""
Create sample data locally and upload to S3
"""
import pandas as pd
import boto3
from datetime import datetime, timedelta
import random
import os

# ===== CONFIGURATION - UPDATE THESE =====
S3_BUCKET = "etl-demo-yourname-2024"  # YOUR BUCKET NAME
GLUE_DATABASE = "etl_demo_db"
AWS_REGION = "us-east-1"
# ========================================

def create_transactions_data(num_records=10000):
    """Create sample transactions"""
    print(f"Creating {num_records} transactions...")

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
    print(f"✓ Created {len(df)} transaction records")
    return df

def create_customers_data(num_customers=1000):
    """Create sample customers"""
    print(f"Creating {num_customers} customers...")

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
    print(f"✓ Created {len(df)} customer records")
    return df

def create_products_data(num_products=100):
    """Create sample products"""
    print(f"Creating {num_products} products...")

    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys']

    data = {
        'product_id': [f'PROD{i:03d}' for i in range(1, num_products + 1)],
        'product_name': [f'Product {i}' for i in range(1, num_products + 1)],
        'category': [random.choice(categories) for _ in range(num_products)],
        'price': [round(random.uniform(10, 500), 2) for _ in range(num_products)],
        'stock': [random.randint(0, 1000) for _ in range(num_products)]
    }

    df = pd.DataFrame(data)
    print(f"✓ Created {len(df)} product records")
    return df

def save_and_upload_parquet(df, local_path, s3_key):
    """Save DataFrame as parquet and upload to S3"""
    # Create local directory if needed
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Save locally as parquet
    df.to_parquet(local_path, index=False, engine='pyarrow')
    print(f"  Saved to {local_path}")

    # Upload to S3
    s3 = boto3.client('s3', region_name=AWS_REGION)
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"  ✓ Uploaded to s3://{S3_BUCKET}/{s3_key}")

def create_glue_table(table_name, s3_location, columns):
    """Create Glue catalog table"""
    print(f"Creating Glue table: {table_name}...")

    glue = boto3.client('glue', region_name=AWS_REGION)

    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {}
            }
        },
        'PartitionKeys': []
    }

    try:
        glue.create_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
        print(f"  ✓ Created table {GLUE_DATABASE}.{table_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"  ⚠ Table already exists, updating...")
        glue.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
        print(f"  ✓ Updated table {GLUE_DATABASE}.{table_name}")

def main():
    print("=" * 80)
    print("SAMPLE DATA CREATION FOR ETL FRAMEWORK")
    print("=" * 80)
    print(f"Bucket: {S3_BUCKET}")
    print(f"Database: {GLUE_DATABASE}")
    print(f"Region: {AWS_REGION}")
    print("=" * 80)

    # 1. Create transactions
    print("\n[1/3] TRANSACTIONS")
    print("-" * 40)
    transactions = create_transactions_data(10000)
    save_and_upload_parquet(
        transactions,
        './tmp/transactions.parquet',
        'raw/transactions/transactions.parquet'
    )
    create_glue_table(
        'fact_transactions',
        f's3://{S3_BUCKET}/raw/transactions/',
        [
            {'Name': 'transaction_id', 'Type': 'string'},
            {'Name': 'customer_id', 'Type': 'string'},
            {'Name': 'product_id', 'Type': 'string'},
            {'Name': 'transaction_date', 'Type': 'string'},
            {'Name': 'amount', 'Type': 'double'},
            {'Name': 'quantity', 'Type': 'int'},
            {'Name': 'unit_price', 'Type': 'double'}
        ]
    )

    # 2. Create customers
    print("\n[2/3] CUSTOMERS")
    print("-" * 40)
    customers = create_customers_data(1000)
    save_and_upload_parquet(
        customers,
        './tmp/customers.parquet',
        'dimensions/customers/customers.parquet'
    )
    create_glue_table(
        'dim_customers',
        f's3://{S3_BUCKET}/dimensions/customers/',
        [
            {'Name': 'customer_id', 'Type': 'string'},
            {'Name': 'customer_name', 'Type': 'string'},
            {'Name': 'email', 'Type': 'string'},
            {'Name': 'phone', 'Type': 'string'},
            {'Name': 'segment_id', 'Type': 'string'},
            {'Name': 'signup_date', 'Type': 'string'}
        ]
    )

    # 3. Create products
    print("\n[3/3] PRODUCTS")
    print("-" * 40)
    products = create_products_data(100)
    save_and_upload_parquet(
        products,
        './tmp/products.parquet',
        'dimensions/products/products.parquet'
    )
    create_glue_table(
        'dim_products',
        f's3://{S3_BUCKET}/dimensions/products/',
        [
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
    print(f"\nS3 Locations:")
    print(f"  Transactions: s3://{S3_BUCKET}/raw/transactions/")
    print(f"  Customers: s3://{S3_BUCKET}/dimensions/customers/")
    print(f"  Products: s3://{S3_BUCKET}/dimensions/products/")
    print(f"\nGlue Tables:")
    print(f"  {GLUE_DATABASE}.fact_transactions (10,000 rows)")
    print(f"  {GLUE_DATABASE}.dim_customers (1,000 rows)")
    print(f"  {GLUE_DATABASE}.dim_products (100 rows)")
    print("\nNext steps:")
    print("  1. Verify data in S3 Console")
    print("  2. Verify tables in Glue Console")
    print("  3. Continue with Step 5 (IAM Roles)")

if __name__ == '__main__':
    main()
```

### 4.2 Update Configuration in Script

**IMPORTANT:** Open the file and update these lines at the top:

```python
S3_BUCKET = "etl-demo-yourname-2024"  # YOUR BUCKET NAME FROM STEP 2
GLUE_DATABASE = "etl_demo_db"         # YOUR DATABASE NAME FROM STEP 3
```

### 4.3 Install Required Python Libraries

```bash
# Install pandas and pyarrow for parquet support
pip install pandas pyarrow boto3
```

### 4.4 Run the Sample Data Creation Script

```bash
# Run the script
python create_sample_data_local.py
```

**Expected output:**
```
================================================================================
SAMPLE DATA CREATION FOR ETL FRAMEWORK
================================================================================
Bucket: etl-demo-yourname-2024
Database: etl_demo_db
Region: us-east-1
================================================================================

[1/3] TRANSACTIONS
----------------------------------------
Creating 10000 transactions...
✓ Created 10000 transaction records
  Saved to ./tmp/transactions.parquet
  ✓ Uploaded to s3://etl-demo-yourname-2024/raw/transactions/transactions.parquet
Creating Glue table: fact_transactions...
  ✓ Created table etl_demo_db.fact_transactions

[2/3] CUSTOMERS
----------------------------------------
Creating 1000 customers...
✓ Created 1000 customer records
  Saved to ./tmp/customers.parquet
  ✓ Uploaded to s3://etl-demo-yourname-2024/dimensions/customers/customers.parquet
Creating Glue table: dim_customers...
  ✓ Created table etl_demo_db.dim_customers

[3/3] PRODUCTS
----------------------------------------
Creating 100 products...
✓ Created 100 product records
  Saved to ./tmp/products.parquet
  ✓ Uploaded to s3://etl-demo-yourname-2024/dimensions/products/products.parquet
Creating Glue table: dim_products...
  ✓ Created table etl_demo_db.dim_products

================================================================================
✓ SAMPLE DATA CREATED SUCCESSFULLY
================================================================================
```

### 4.5 Verify Data in AWS Console

**Verify S3:**
1. Go to **AWS Console** → **S3**
2. Click your bucket: `etl-demo-yourname-2024`
3. Navigate to `raw/transactions/` - you should see `transactions.parquet`
4. Navigate to `dimensions/customers/` - you should see `customers.parquet`
5. Navigate to `dimensions/products/` - you should see `products.parquet`

**Verify Glue Tables:**
1. Go to **AWS Console** → **AWS Glue** → **Databases**
2. Click `etl_demo_db`
3. You should see 3 tables:
   - `fact_transactions`
   - `dim_customers`
   - `dim_products`
4. Click on each table to see schema

**Verify via CLI:**
```bash
# List S3 files
aws s3 ls s3://$BUCKET_NAME/raw/transactions/
aws s3 ls s3://$BUCKET_NAME/dimensions/customers/
aws s3 ls s3://$BUCKET_NAME/dimensions/products/

# List Glue tables
aws glue get-tables --database-name etl_demo_db --query 'TableList[].Name'
```

---

## Step 5: IAM Roles & Permissions

### 5.1 Create IAM Role for AWS Glue

**5.1.1 Create Trust Policy**

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

**5.1.2 Create the IAM Role via AWS CLI**

```bash
# Create role
aws iam create-role \
  --role-name ETLGlueServiceRole \
  --assume-role-policy-document file://glue-trust-policy.json \
  --description "Service role for AWS Glue ETL jobs"

# Attach AWS managed policy for Glue
aws iam attach-role-policy \
  --role-name ETLGlueServiceRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Attach S3 access policy
aws iam attach-role-policy \
  --role-name ETLGlueServiceRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Get role ARN (save this!)
aws iam get-role \
  --role-name ETLGlueServiceRole \
  --query 'Role.Arn' \
  --output text
```

**Expected output (Role ARN):**
```
arn:aws:iam::123456789012:role/ETLGlueServiceRole
```

**✅ Save this ARN** - you'll need it for Glue jobs

**5.1.3 Create IAM Role via AWS Console (Alternative)**

1. Go to **AWS Console** → **IAM** → **Roles**
2. Click **"Create role"**
3. **Trusted entity type:** AWS service
4. **Use case:** Glue
5. Click **"Next"**
6. **Add permissions:**
   - Search and select: `AWSGlueServiceRole`
   - Search and select: `AmazonS3FullAccess`
7. Click **"Next"**
8. **Role name:** `ETLGlueServiceRole`
9. **Description:** Service role for AWS Glue ETL jobs
10. Click **"Create role"**
11. Click on the role and copy the **Role ARN**

### 5.2 Create IAM Role for Bedrock Agents (Optional)

```bash
# Create trust policy for Bedrock
cat > bedrock-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "bedrock.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role
aws iam create-role \
  --role-name BedrockAgentRole \
  --assume-role-policy-document file://bedrock-trust-policy.json

# Attach Bedrock policy
aws iam attach-role-policy \
  --role-name BedrockAgentRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonBedrockFullAccess

# Get ARN
aws iam get-role \
  --role-name BedrockAgentRole \
  --query 'Role.Arn' \
  --output text
```

---

## Step 6: Bedrock Agents Setup

### 6.1 Enable Bedrock Model Access

**6.1.1 Via AWS Console**

1. Go to **AWS Console** → **Amazon Bedrock**
2. In left menu, click **"Model access"**
3. Click **"Manage model access"** (top right)
4. Select models you want to enable:
   - ✅ **Anthropic - Claude 3 Sonnet**
   - ✅ **Anthropic - Claude 3 Haiku**
   - ✅ **Anthropic - Claude 3.5 Sonnet** (if available)
5. Click **"Request model access"**
6. Wait for approval (usually instant for Claude models)

**6.1.2 Verify Model Access**

```bash
# List available models
aws bedrock list-foundation-models \
  --region us-east-1 \
  --query 'modelSummaries[?contains(modelId, `anthropic`)].modelId'
```

**Expected output:**
```json
[
    "anthropic.claude-3-sonnet-20240229-v1:0",
    "anthropic.claude-3-haiku-20240307-v1:0",
    "anthropic.claude-3-5-sonnet-20240620-v1:0"
]
```

### 6.2 Create Bedrock Agent for Decision Making

**Note:** For this config-driven framework, we're using the **native Strands SDK** instead of Bedrock Agents. Bedrock Agents are optional for additional orchestration.

**If you want to create a Bedrock Agent (Optional):**

1. Go to **AWS Console** → **Amazon Bedrock** → **Agents**
2. Click **"Create Agent"**
3. **Agent details:**
   - Agent name: `ETL-Decision-Agent`
   - Description: `Decision agent for ETL platform selection`
   - Agent resource role: Select `BedrockAgentRole` (created in 5.2)
4. **Agent instruction:**
```
You are an ETL decision agent. Your role is to analyze workload characteristics
and recommend the optimal processing platform (AWS Glue, EMR, Lambda, or Batch)
based on data volume, complexity, and cost considerations.
```
5. **Model:** Select `Anthropic Claude 3 Sonnet`
6. Click **"Create"**

**For this demo, Bedrock Agents are OPTIONAL.** The framework uses native Strands SDK which works locally without Bedrock Agents.

---

## Step 7: Python Environment Setup

### 7.1 Clone Repository (If Not Already Done)

```bash
# Clone the repository
git clone https://github.com/dhilipansankaralingam-del/strands_etl.git

# Navigate to directory
cd strands_etl

# Checkout the config-driven framework branch
git checkout claude/config-driven-framework-mklys5sfa8e4xuqc-u0a48
```

### 7.2 Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
# On Linux/Mac:
source venv/bin/activate

# On Windows:
# venv\Scripts\activate

# Your prompt should now show (venv)
```

### 7.3 Install Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install all requirements
pip install -r requirements.txt

# This installs:
# - strands-agents (native Strands SDK)
# - boto3 (AWS SDK)
# - pandas, numpy
# - pyarrow (for parquet)
# - and other dependencies
```

**Expected output:**
```
Successfully installed strands-agents-1.23.0 boto3-1.26.0 ...
```

### 7.4 Verify Installation

```bash
# Check Strands SDK
python -c "import strands; print('Strands version:', strands.__version__)"

# Check boto3
python -c "import boto3; print('boto3 version:', boto3.__version__)"

# Check pandas
python -c "import pandas; print('pandas version:', pandas.__version__)"
```

**Expected output:**
```
Strands version: 1.23.0
boto3 version: 1.26.0
pandas version: 2.0.0
```

---

## Step 8: Configuration File Setup

### 8.1 Update etl_config.json with Your Settings

Open `etl_config.json` and update the `variables` section:

```json
{
  "variables": {
    "data_bucket": "etl-demo-yourname-2024",  // YOUR BUCKET NAME
    "raw_prefix": "raw",
    "processed_prefix": "processed",
    "dimensions_prefix": "dimensions",
    "glue_database": "etl_demo_db",  // YOUR DATABASE NAME
    "environment": "dev"
  }
}
```

**File location:** `/home/user/strands_etl/etl_config.json`

### 8.2 Validate Configuration

```bash
# Validate the configuration
python -c "
from strands_agents.orchestrator.config_loader import ConfigLoader

loader = ConfigLoader('./etl_config.json')
config = loader.load()

print('✓ Configuration loaded successfully')
print(f'  Jobs defined: {len(config[\"jobs\"])}')

# Validate
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

**Expected output:**
```
✓ Configuration loaded successfully
  Jobs defined: 3
✓ Configuration is valid
  Enabled jobs: 2
```

### 8.3 Test Auto-Detection

```bash
# Test auto-detection of table sizes
python -c "
from strands_agents.tools.catalog_tools import detect_glue_table_size

result = detect_glue_table_size(
    database='etl_demo_db',
    table='fact_transactions',
    aws_region='us-east-1'
)

print('Auto-Detection Test:')
print(f'  Table: {result[\"table\"]}')
print(f'  Size: {result[\"size_gb\"]} GB')
print(f'  Files: {result[\"num_files\"]}')
print(f'  Rows: {result.get(\"num_rows\", \"unknown\")}')
print(f'  Broadcast eligible: {result[\"broadcast_eligible\"]}')
print(f'  Recommended role: {result[\"recommended_role\"]}')
"
```

**Expected output:**
```
Auto-Detection Test:
  Table: fact_transactions
  Size: 0.45 GB
  Files: 1
  Rows: 10000
  Broadcast eligible: True
  Recommended role: dimension
```

---

## Step 9: Lambda Functions (Optional)

**Note:** Lambda functions are only needed if you're running streaming jobs or want serverless execution. For batch Glue jobs, skip this section.

### 9.1 Create Lambda Function for Clickstream Processing

**9.1.1 Create Lambda Function Code**

Create file `lambda/clickstream_processor.py`:

```python
import json
import boto3
import base64
from datetime import datetime

def lambda_handler(event, context):
    """
    Process clickstream events from Kinesis
    """
    print(f"Processing {len(event['Records'])} records")

    processed = 0
    for record in event['Records']:
        # Decode Kinesis data
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)

        # Process event
        print(f"Processing event: {data.get('event_type', 'unknown')}")

        # Your processing logic here
        # Example: Enrich with user profile, validate, transform

        processed += 1

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed,
            'timestamp': datetime.utcnow().isoformat()
        })
    }
```

**9.1.2 Create Deployment Package**

```bash
# Create deployment directory
mkdir -p lambda_deploy

# Copy lambda code
cp lambda/clickstream_processor.py lambda_deploy/

# Create zip
cd lambda_deploy
zip -r ../clickstream_function.zip .
cd ..
```

**9.1.3 Create Lambda Function via AWS CLI**

```bash
# Create Lambda execution role first
cat > lambda-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role
aws iam create-role \
  --role-name LambdaETLExecutionRole \
  --assume-role-policy-document file://lambda-trust-policy.json

# Attach basic execution policy
aws iam attach-role-policy \
  --role-name LambdaETLExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create Lambda function
aws lambda create-function \
  --function-name clickstream-processor \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/LambdaETLExecutionRole \
  --handler clickstream_processor.lambda_handler \
  --zip-file fileb://clickstream_function.zip \
  --timeout 60 \
  --memory-size 1024
```

**Replace `YOUR_ACCOUNT_ID`** with your AWS Account ID from Step 1.3

---

## Step 10: First End-to-End Execution

### 10.1 Run Configuration Examples

```bash
# Make sure virtual environment is activated
source venv/bin/activate

# Run examples
python example_config_driven_usage.py
```

**This will show:**
- Configuration loading and validation
- Job inspection
- Table name usage examples
- Data flow analysis examples

**Expected output:**
```
================================================================================
CONFIG-DRIVEN ETL FRAMEWORK - COMPREHENSIVE EXAMPLES
================================================================================

================================================================================
EXAMPLE 1: Configuration Loading and Validation
================================================================================

📂 Loading configuration...
✓ Loaded config version: 2.0
✓ Total jobs defined: 3

🔍 Validating configuration...
✓ Configuration is valid

⚠ 1 warnings:
  - Job customer_order_summary: No cost budget set

📋 Enabled jobs:
  - customer_order_summary: Customer Order Summary ETL
    Data sources: 4
    Platform preference: glue
  - real_time_clickstream: Real-Time Clickstream Processing
    Data sources: 2
    Platform preference: lambda
...
```

### 10.2 Test Individual Components

**10.2.1 Test Config Loader**

```bash
python -c "
from strands_agents.orchestrator.config_loader import ConfigLoader

loader = ConfigLoader('./etl_config.json')
loader.load()

# Get specific job
job = loader.get_job_by_id('customer_order_summary')
print(f'Job: {job[\"job_name\"]}')
print(f'Data sources: {len(job[\"data_sources\"])}')
print(f'Platform: {job[\"platform\"][\"user_preference\"]}')
"
```

**10.2.2 Test Auto-Detection for All Tables**

```bash
python -c "
from strands_agents.tools.catalog_tools import auto_detect_all_table_sizes

data_sources = [
    {
        'name': 'transactions',
        'source_type': 'glue_catalog',
        'database': 'etl_demo_db',
        'table': 'fact_transactions'
    },
    {
        'name': 'customers',
        'source_type': 'glue_catalog',
        'database': 'etl_demo_db',
        'table': 'dim_customers'
    },
    {
        'name': 'products',
        'source_type': 'glue_catalog',
        'database': 'etl_demo_db',
        'table': 'dim_products'
    }
]

result = auto_detect_all_table_sizes(data_sources, 'us-east-1')

print('Auto-Detection Results:')
print(f'  Total sources: {result[\"total_sources\"]}')
print(f'  Detected: {result[\"detected_sources\"]}')
print(f'  Total volume: {result[\"total_data_volume_gb\"]} GB')
print()
print('Individual sources:')
for r in result['results']:
    if r.get('success'):
        print(f'  - {r[\"name\"]}: {r[\"size_gb\"]} GB ({r[\"num_files\"]} files)')
"
```

**10.2.3 Test Data Flow Analysis**

```bash
python -c "
from strands_agents.tools.catalog_tools import analyze_data_flow_relationships

data_sources = [
    {
        'name': 'transactions',
        'role_in_flow': 'fact',
        'size_gb': 0.45,
        'joins_with': ['customers', 'products'],
        'join_keys': {'customers': 'customer_id', 'products': 'product_id'}
    },
    {
        'name': 'customers',
        'role_in_flow': 'dimension',
        'size_gb': 0.05,
        'joins_with': ['transactions']
    },
    {
        'name': 'products',
        'role_in_flow': 'dimension',
        'size_gb': 0.02,
        'joins_with': ['transactions']
    }
]

result = analyze_data_flow_relationships(data_sources)

print('Data Flow Analysis:')
print(f'  Fact tables: {result[\"fact_tables\"]}')
print(f'  Dimension tables: {result[\"dimension_tables\"]}')
print(f'  Broadcast recommendations: {len(result[\"broadcast_recommendations\"])}')
print()
for rec in result['broadcast_recommendations']:
    print(f'  - {rec[\"table\"]}: {rec[\"reason\"]}')
"
```

### 10.3 Run Complete Job Orchestration

Create a test script `run_demo_job.py`:

```python
"""
Run a demo job using the config-driven orchestrator
"""
from strands_agents.orchestrator.config_driven_orchestrator import ConfigDrivenOrchestrator
import json

print("=" * 80)
print("CONFIG-DRIVEN ETL JOB EXECUTION - DEMO")
print("=" * 80)

# Initialize orchestrator
print("\n[1] Initializing ConfigDrivenOrchestrator...")
orchestrator = ConfigDrivenOrchestrator(
    config_path='./etl_config.json',
    enable_auto_detection=True
)
print("✓ Orchestrator initialized")

# Get job config
print("\n[2] Loading job configuration...")
job = orchestrator.config_loader.get_job_by_id('customer_order_summary')
print(f"✓ Loaded job: {job['job_name']}")
print(f"  Data sources: {len(job['data_sources'])}")
print(f"  Platform preference: {job['platform']['user_preference']}")

# Run pre-execution analysis only (don't actually run the job)
print("\n[3] Running pre-execution analysis...")

# This is a dry-run - we'll analyze but not execute
from strands_agents.tools.catalog_tools import auto_detect_all_table_sizes, analyze_data_flow_relationships

# Auto-detect sizes
print("\n  Auto-detecting table sizes...")
size_result = auto_detect_all_table_sizes(job['data_sources'], 'us-east-1')
print(f"  ✓ Total data volume: {size_result['total_data_volume_gb']} GB")

# Analyze data flow
print("\n  Analyzing data flow...")
flow_result = analyze_data_flow_relationships(job['data_sources'])
print(f"  ✓ Fact tables: {flow_result['fact_tables']}")
print(f"  ✓ Dimension tables: {flow_result['dimension_tables']}")
print(f"  ✓ Broadcast recommendations: {len(flow_result['broadcast_recommendations'])}")

# Platform recommendation
from strands_agents.tools.catalog_tools import recommend_platform_based_on_data_flow
platform_rec = recommend_platform_based_on_data_flow(
    flow_result,
    size_result['total_data_volume_gb'],
    job['workload']['complexity']
)

print("\n[4] Platform Recommendation:")
print(f"  User preference: {job['platform']['user_preference']}")
print(f"  Agent recommendation: {platform_rec['recommended_platform']}")
print(f"  Reason: {platform_rec['reason']}")
print(f"  DPU count: {platform_rec['dpu_count']}")

print("\n" + "=" * 80)
print("✓ DRY RUN COMPLETE")
print("=" * 80)
print("\nResults:")
print(f"  • Total data: {size_result['total_data_volume_gb']} GB")
print(f"  • Tables: {size_result['total_sources']}")
print(f"  • Platform: {platform_rec['recommended_platform']}")
print(f"  • Broadcast candidates: {len(flow_result['broadcast_recommendations'])}")
print("\nNote: This was a dry-run (analysis only).")
print("To actually run a Glue job, you need to:")
print("  1. Create a Glue job in AWS Console")
print("  2. Upload a PySpark script to S3")
print("  3. Run orchestrator.run_job_by_id('customer_order_summary')")
```

Run it:

```bash
python run_demo_job.py
```

**Expected output:**
```
================================================================================
CONFIG-DRIVEN ETL JOB EXECUTION - DEMO
================================================================================

[1] Initializing ConfigDrivenOrchestrator...
✓ Orchestrator initialized

[2] Loading job configuration...
✓ Loaded job: Customer Order Summary ETL
  Data sources: 4
  Platform preference: glue

[3] Running pre-execution analysis...

  Auto-detecting table sizes...
  ✓ Total data volume: 0.52 GB

  Analyzing data flow...
  ✓ Fact tables: ['transactions']
  ✓ Dimension tables: ['customers', 'products', 'customer_segments']
  ✓ Broadcast recommendations: 3

[4] Platform Recommendation:
  User preference: glue
  Agent recommendation: glue
  Reason: Moderate data volume with few joins, Glue is cost-effective
  DPU count: 5

================================================================================
✓ DRY RUN COMPLETE
================================================================================

Results:
  • Total data: 0.52 GB
  • Tables: 4
  • Platform: glue
  • Broadcast candidates: 3

Note: This was a dry-run (analysis only).
```

---

## Step 11: Validation & Testing

### 11.1 Verify S3 Data

```bash
# List all data in your bucket
aws s3 ls s3://$BUCKET_NAME/ --recursive --human-readable

# Download and inspect a sample file
aws s3 cp s3://$BUCKET_NAME/raw/transactions/transactions.parquet /tmp/

# Read with pandas
python -c "
import pandas as pd
df = pd.read_parquet('/tmp/transactions.parquet')
print('Sample data:')
print(df.head())
print(f'\nTotal rows: {len(df)}')
print(f'Columns: {list(df.columns)}')
"
```

### 11.2 Verify Glue Tables

```bash
# Get table info
aws glue get-table \
  --database-name etl_demo_db \
  --name fact_transactions \
  --query 'Table.[Name,StorageDescriptor.Location,StorageDescriptor.Columns[].Name]'

# Count rows (requires Athena or Glue job)
# We'll use a simple check
aws glue get-table \
  --database-name etl_demo_db \
  --name fact_transactions
```

### 11.3 Test Strands Agents

```bash
# Test Decision Agent
python -c "
from strands_agents.agents.etl_agents import create_decision_agent

agent = create_decision_agent()
response = agent('I have 500 GB of data with 5 table joins. Which platform should I use?')
print('Decision Agent Response:')
print(response)
"
```

### 11.4 Run All Examples

```bash
# Run comprehensive examples
python example_config_driven_usage.py > example_output.txt 2>&1

# Check output
cat example_output.txt
```

---

## Step 12: Troubleshooting

### Issue 1: "NoSuchBucket" Error

**Error:**
```
An error occurred (NoSuchBucket) when calling the PutObject operation
```

**Solution:**
```bash
# Verify bucket exists
aws s3 ls | grep your-bucket-name

# If not, create it
aws s3 mb s3://your-bucket-name --region us-east-1
```

### Issue 2: "Database not found" in Glue

**Error:**
```
EntityNotFoundException: Database etl_demo_db not found
```

**Solution:**
```bash
# Create database
aws glue create-database \
  --database-input '{"Name": "etl_demo_db"}'
```

### Issue 3: "Access Denied" when accessing S3

**Error:**
```
An error occurred (AccessDenied) when calling the PutObject operation
```

**Solution:**
```bash
# Check your AWS credentials
aws sts get-caller-identity

# Verify IAM permissions
# Your user needs s3:PutObject, s3:GetObject permissions
```

### Issue 4: "Module not found: strands"

**Error:**
```
ModuleNotFoundError: No module named 'strands'
```

**Solution:**
```bash
# Activate virtual environment
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### Issue 5: Pandas cannot read parquet

**Error:**
```
ValueError: Parquet engine not available
```

**Solution:**
```bash
# Install pyarrow
pip install pyarrow
```

### Issue 6: AWS credentials not configured

**Error:**
```
NoCredentialsError: Unable to locate credentials
```

**Solution:**
```bash
# Configure AWS CLI
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### Issue 7: Bedrock model access denied

**Error:**
```
AccessDeniedException: You don't have access to the model
```

**Solution:**
1. Go to AWS Console → Bedrock → Model access
2. Request access to Anthropic Claude models
3. Wait for approval (usually instant)

---

## Summary Checklist

Use this checklist to track your progress:

- [ ] **Step 1:** AWS CLI configured and tested
- [ ] **Step 2:** S3 bucket created with folder structure
- [ ] **Step 3:** Glue database created
- [ ] **Step 4:** Sample data created and uploaded
- [ ] **Step 5:** IAM roles created (Glue, Bedrock, Lambda)
- [ ] **Step 6:** Bedrock model access enabled
- [ ] **Step 7:** Python environment set up
- [ ] **Step 8:** Configuration file updated and validated
- [ ] **Step 9:** Lambda functions created (if needed)
- [ ] **Step 10:** End-to-end execution tested
- [ ] **Step 11:** All validations passed
- [ ] **Step 12:** Troubleshooting guide reviewed

---

## Next Steps

Now that you have everything set up:

1. **Customize Configuration**
   - Edit `etl_config.json` to add your own jobs
   - Add your data sources
   - Configure quality rules

2. **Create PySpark Scripts**
   - Write ETL logic in `pyscript/` directory
   - Upload to S3
   - Reference in config

3. **Run Production Jobs**
   - Create Glue jobs in AWS Console
   - Run via orchestrator
   - Monitor execution

4. **Set Up Monitoring**
   - CloudWatch dashboards
   - Cost tracking
   - Quality metrics

5. **Deploy to Production**
   - Use separate environments (dev/staging/prod)
   - Implement CI/CD
   - Set up alerting

---

## Support & Resources

**Documentation:**
- `CONFIG_DRIVEN_README.md` - Framework documentation
- `STRANDS_NATIVE_README.md` - Agent reference
- `SETUP_GUIDE.md` - Alternative setup guide

**Examples:**
- `example_config_driven_usage.py` - Code examples
- `end_to_end_flow_example.py` - Orchestration patterns

**AWS Documentation:**
- AWS Glue: https://docs.aws.amazon.com/glue/
- Amazon Bedrock: https://docs.aws.amazon.com/bedrock/
- AWS Lambda: https://docs.aws.amazon.com/lambda/

---

**You're all set!** 🎉

Everything is configured and ready to run end-to-end. The framework will:
- Auto-detect table sizes from Glue Catalog
- Analyze data flow and recommend join strategies
- Respect your platform preference while providing agent recommendations
- Validate data quality from config rules
- Track costs and optimize resources

Happy ETL-ing! 🚀
