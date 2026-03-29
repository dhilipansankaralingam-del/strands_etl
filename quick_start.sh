#!/bin/bash

# Quick Start Script for Config-Driven ETL Framework
# This script automates the initial setup

set -e  # Exit on error

echo "================================================================================"
echo "CONFIG-DRIVEN ETL FRAMEWORK - QUICK START SETUP"
echo "================================================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Step 1: Check Prerequisites
echo ""
echo "Step 1: Checking Prerequisites..."
echo "=================================="

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    print_success "Python found: $PYTHON_VERSION"
else
    print_error "Python 3 not found. Please install Python 3.8 or higher."
    exit 1
fi

# Check pip
if command -v pip3 &> /dev/null; then
    print_success "pip found"
else
    print_error "pip not found. Please install pip."
    exit 1
fi

# Check AWS CLI
if command -v aws &> /dev/null; then
    print_success "AWS CLI found"
else
    print_error "AWS CLI not found. Please install AWS CLI."
    exit 1
fi

# Check AWS credentials
if aws sts get-caller-identity &> /dev/null; then
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    print_success "AWS credentials configured (Account: $ACCOUNT_ID)"
else
    print_error "AWS credentials not configured. Run 'aws configure' first."
    exit 1
fi

# Step 2: Create Virtual Environment
echo ""
echo "Step 2: Creating Virtual Environment..."
echo "========================================"

if [ -d "venv" ]; then
    print_info "Virtual environment already exists, skipping..."
else
    print_info "Creating virtual environment..."
    python3 -m venv venv
    print_success "Virtual environment created"
fi

# Activate virtual environment
print_info "Activating virtual environment..."
source venv/bin/activate
print_success "Virtual environment activated"

# Step 3: Install Dependencies
echo ""
echo "Step 3: Installing Dependencies..."
echo "==================================="

print_info "Upgrading pip..."
pip install --upgrade pip --quiet

print_info "Installing requirements..."
pip install -r requirements.txt --quiet

print_success "Dependencies installed"

# Verify installations
print_info "Verifying installations..."
python -c "import strands; print('  Strands SDK:', strands.__version__)" 2>/dev/null || print_error "Strands SDK not installed correctly"
python -c "import boto3; print('  boto3:', boto3.__version__)" 2>/dev/null || print_error "boto3 not installed correctly"

# Step 4: AWS Resource Setup
echo ""
echo "Step 4: Setting Up AWS Resources..."
echo "===================================="

# Prompt for bucket name
echo ""
read -p "Enter S3 bucket name (or press Enter for auto-generated): " BUCKET_NAME

if [ -z "$BUCKET_NAME" ]; then
    TIMESTAMP=$(date +%s)
    BUCKET_NAME="etl-demo-bucket-$TIMESTAMP"
    print_info "Using auto-generated bucket name: $BUCKET_NAME"
fi

# Create S3 bucket
print_info "Creating S3 bucket: $BUCKET_NAME..."
if aws s3 mb s3://$BUCKET_NAME 2>/dev/null; then
    print_success "S3 bucket created: $BUCKET_NAME"
else
    if aws s3 ls s3://$BUCKET_NAME &>/dev/null; then
        print_info "Bucket already exists, using existing bucket"
    else
        print_error "Failed to create bucket. It may already exist in another account."
        exit 1
    fi
fi

# Prompt for Glue database name
echo ""
read -p "Enter Glue database name (default: etl_demo_db): " DB_NAME
DB_NAME=${DB_NAME:-etl_demo_db}

# Create Glue database
print_info "Creating Glue database: $DB_NAME..."
if aws glue create-database --database-input "{\"Name\": \"$DB_NAME\", \"Description\": \"ETL Demo Database\"}" 2>/dev/null; then
    print_success "Glue database created: $DB_NAME"
else
    if aws glue get-database --name $DB_NAME &>/dev/null; then
        print_info "Database already exists, using existing database"
    else
        print_error "Failed to create database"
        exit 1
    fi
fi

# Create IAM role for Glue
echo ""
print_info "Creating IAM role for Glue..."

# Create trust policy file
cat > /tmp/glue-trust-policy.json <<EOF
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

ROLE_NAME="ETLGlueRole"

# Create role
if aws iam create-role \
    --role-name $ROLE_NAME \
    --assume-role-policy-document file:///tmp/glue-trust-policy.json &>/dev/null; then
    print_success "IAM role created: $ROLE_NAME"
else
    if aws iam get-role --role-name $ROLE_NAME &>/dev/null; then
        print_info "Role already exists, using existing role"
    else
        print_error "Failed to create IAM role"
        exit 1
    fi
fi

# Attach policies
print_info "Attaching policies to IAM role..."
aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole &>/dev/null || true

aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess &>/dev/null || true

print_success "Policies attached"

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --query 'Role.Arn' --output text)
print_success "Role ARN: $ROLE_ARN"

# Step 5: Update Configuration
echo ""
echo "Step 5: Updating Configuration..."
echo "=================================="

print_info "Updating etl_config.json with your settings..."

# Backup original config
cp etl_config.json etl_config.json.backup

# Update variables in config using python
python3 <<EOF
import json

with open('etl_config.json', 'r') as f:
    config = json.load(f)

# Update variables
config['variables']['data_bucket'] = '$BUCKET_NAME'
config['variables']['glue_database'] = '$DB_NAME'

with open('etl_config.json', 'w') as f:
    json.dump(config, f, indent=2)

print('Configuration updated successfully')
EOF

print_success "Configuration updated"
print_info "Backup saved to: etl_config.json.backup"

# Step 6: Create Sample Data Script
echo ""
echo "Step 6: Preparing Sample Data Script..."
echo "========================================"

cat > create_sample_data.py <<'EOFPY'
"""
Create sample data for testing the ETL framework
"""
import pandas as pd
import boto3
import sys
from datetime import datetime, timedelta
import random

# Get configuration from command line
S3_BUCKET = sys.argv[1] if len(sys.argv) > 1 else "my-bucket"
GLUE_DATABASE = sys.argv[2] if len(sys.argv) > 2 else "etl_demo_db"

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
    return pd.DataFrame(data)

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
    return pd.DataFrame(data)

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
    return pd.DataFrame(data)

def upload_to_s3_as_parquet(df, s3_path):
    """Upload DataFrame to S3 as parquet"""
    print(f"  Uploading to {s3_path}...")
    local_file = '/tmp/temp_data.parquet'
    df.to_parquet(local_file, index=False)
    s3 = boto3.client('s3')
    bucket = S3_BUCKET
    key = s3_path.replace(f's3://{bucket}/', '')
    s3.upload_file(local_file, bucket, key)
    print(f"  ✓ Uploaded {len(df)} records")

def create_glue_table(database, table_name, s3_location, columns):
    """Create Glue catalog table"""
    print(f"  Creating table {database}.{table_name}...")
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
        print(f"  ✓ Created table")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"  ✓ Updated existing table")

print("Creating sample data...")
print()

# Transactions
print("1. Creating transactions...")
df = create_transactions_data()
upload_to_s3_as_parquet(df, f's3://{S3_BUCKET}/raw/transactions/transactions.parquet')
create_glue_table(GLUE_DATABASE, 'fact_transactions', f's3://{S3_BUCKET}/raw/transactions/',
    [{'Name': 'transaction_id', 'Type': 'string'}, {'Name': 'customer_id', 'Type': 'string'},
     {'Name': 'product_id', 'Type': 'string'}, {'Name': 'transaction_date', 'Type': 'string'},
     {'Name': 'amount', 'Type': 'double'}, {'Name': 'quantity', 'Type': 'int'},
     {'Name': 'unit_price', 'Type': 'double'}])

# Customers
print("\n2. Creating customers...")
df = create_customers_data()
upload_to_s3_as_parquet(df, f's3://{S3_BUCKET}/dimensions/customers/customers.parquet')
create_glue_table(GLUE_DATABASE, 'dim_customers', f's3://{S3_BUCKET}/dimensions/customers/',
    [{'Name': 'customer_id', 'Type': 'string'}, {'Name': 'customer_name', 'Type': 'string'},
     {'Name': 'email', 'Type': 'string'}, {'Name': 'phone', 'Type': 'string'},
     {'Name': 'segment_id', 'Type': 'string'}, {'Name': 'signup_date', 'Type': 'string'}])

# Products
print("\n3. Creating products...")
df = create_products_data()
upload_to_s3_as_parquet(df, f's3://{S3_BUCKET}/dimensions/products/products.parquet')
create_glue_table(GLUE_DATABASE, 'dim_products', f's3://{S3_BUCKET}/dimensions/products/',
    [{'Name': 'product_id', 'Type': 'string'}, {'Name': 'product_name', 'Type': 'string'},
     {'Name': 'category', 'Type': 'string'}, {'Name': 'price', 'Type': 'double'},
     {'Name': 'stock', 'Type': 'int'}])

print("\n✓ Sample data created successfully!")
EOFPY

print_success "Sample data script created"

# Step 7: Run Sample Data Creation
echo ""
read -p "Do you want to create sample data now? (y/n): " CREATE_DATA

if [ "$CREATE_DATA" = "y" ] || [ "$CREATE_DATA" = "Y" ]; then
    echo ""
    echo "Step 7: Creating Sample Data..."
    echo "================================"

    print_info "Running sample data creation script..."
    python3 create_sample_data.py $BUCKET_NAME $DB_NAME

    print_success "Sample data created"
else
    print_info "Skipping sample data creation"
    print_info "Run 'python create_sample_data.py $BUCKET_NAME $DB_NAME' later to create data"
fi

# Step 8: Validate Setup
echo ""
echo "Step 8: Validating Setup..."
echo "==========================="

print_info "Running configuration validation..."
python3 -c "
from strands_agents.orchestrator.config_loader import ConfigLoader

loader = ConfigLoader('./etl_config.json')
config = loader.load()
validation = loader.validate_config()

if validation['valid']:
    print('  ✓ Configuration is valid')
    print(f'  ✓ Total jobs: {len(config[\"jobs\"])}')
    print(f'  ✓ Enabled jobs: {validation[\"enabled_jobs\"]}')
else:
    print('  ✗ Configuration has errors')
    for error in validation['errors']:
        print(f'    - {error}')
"

print_success "Setup validation complete"

# Final Summary
echo ""
echo "================================================================================"
echo "SETUP COMPLETE!"
echo "================================================================================"
echo ""
print_success "Your config-driven ETL framework is ready to use!"
echo ""
echo "Resources created:"
echo "  • S3 Bucket: $BUCKET_NAME"
echo "  • Glue Database: $DB_NAME"
echo "  • IAM Role: $ROLE_NAME ($ROLE_ARN)"
echo ""
echo "Configuration:"
echo "  • etl_config.json - Updated with your settings"
echo "  • Backup saved to: etl_config.json.backup"
echo ""
echo "Next steps:"
echo "  1. Review the configuration:"
echo "     $ cat etl_config.json"
echo ""
echo "  2. Run examples:"
echo "     $ python example_config_driven_usage.py"
echo ""
echo "  3. Test auto-detection:"
echo "     $ python -c 'from strands_agents.tools.catalog_tools import detect_glue_table_size; print(detect_glue_table_size(\"$DB_NAME\", \"fact_transactions\"))'"
echo ""
echo "  4. See full documentation:"
echo "     $ cat SETUP_GUIDE.md"
echo "     $ cat CONFIG_DRIVEN_README.md"
echo ""
echo "  5. Remember to activate venv in new terminals:"
echo "     $ source venv/bin/activate"
echo ""
print_success "Happy ETL-ing! 🚀"
echo ""
