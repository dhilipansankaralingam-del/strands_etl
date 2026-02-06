# Enterprise ETL Framework - Complete Provisioning Guide

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [AWS Core Infrastructure](#2-aws-core-infrastructure)
3. [IAM Roles and Policies](#3-iam-roles-and-policies)
4. [S3 Buckets](#4-s3-buckets)
5. [DynamoDB Tables](#5-dynamodb-tables)
6. [Glue Setup](#6-glue-setup)
7. [EMR Setup](#7-emr-setup)
8. [EKS with Karpenter](#8-eks-with-karpenter)
9. [Slack Integration](#9-slack-integration)
10. [Microsoft Teams Integration](#10-microsoft-teams-integration)
11. [Email (SES) Integration](#11-email-ses-integration)
12. [Streamlit Dashboard](#12-streamlit-dashboard)
13. [CloudWatch Dashboard](#13-cloudwatch-dashboard)
14. [Secrets Manager](#14-secrets-manager)
15. [Lambda Functions](#15-lambda-functions)
16. [API Gateway](#16-api-gateway)
17. [Verification Steps](#17-verification-steps)

---

## 1. Prerequisites

### 1.1 Required Tools

```bash
# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Install eksctl (for EKS)
curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
sudo mv /tmp/eksctl /usr/local/bin

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install Helm (for Karpenter)
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Install Terraform (optional, for IaC)
wget https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
unzip terraform_1.6.0_linux_amd64.zip
sudo mv terraform /usr/local/bin/

# Install Python dependencies
pip install boto3 slack-sdk streamlit pandas plotly
```

### 1.2 AWS Account Setup

```bash
# Configure AWS credentials
aws configure
# Enter:
# - AWS Access Key ID
# - AWS Secret Access Key
# - Default region (e.g., us-east-1)
# - Default output format (json)

# Verify setup
aws sts get-caller-identity

# Set environment variables
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1
echo "Account ID: $AWS_ACCOUNT_ID"
echo "Region: $AWS_REGION"
```

---

## 2. AWS Core Infrastructure

### 2.1 VPC Setup (if not using default)

```bash
# Create VPC
VPC_ID=$(aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=etl-framework-vpc}]' \
  --query 'Vpc.VpcId' --output text)

echo "VPC ID: $VPC_ID"

# Enable DNS hostnames
aws ec2 modify-vpc-attribute --vpc-id $VPC_ID --enable-dns-hostnames

# Create subnets (2 AZs for high availability)
SUBNET_1=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.1.0/24 \
  --availability-zone ${AWS_REGION}a \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=etl-subnet-1}]' \
  --query 'Subnet.SubnetId' --output text)

SUBNET_2=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --cidr-block 10.0.2.0/24 \
  --availability-zone ${AWS_REGION}b \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=etl-subnet-2}]' \
  --query 'Subnet.SubnetId' --output text)

echo "Subnet 1: $SUBNET_1"
echo "Subnet 2: $SUBNET_2"

# Create Internet Gateway
IGW_ID=$(aws ec2 create-internet-gateway \
  --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=etl-igw}]' \
  --query 'InternetGateway.InternetGatewayId' --output text)

aws ec2 attach-internet-gateway --vpc-id $VPC_ID --internet-gateway-id $IGW_ID

# Create route table
RT_ID=$(aws ec2 create-route-table \
  --vpc-id $VPC_ID \
  --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=etl-rt}]' \
  --query 'RouteTable.RouteTableId' --output text)

aws ec2 create-route --route-table-id $RT_ID --destination-cidr-block 0.0.0.0/0 --gateway-id $IGW_ID
aws ec2 associate-route-table --route-table-id $RT_ID --subnet-id $SUBNET_1
aws ec2 associate-route-table --route-table-id $RT_ID --subnet-id $SUBNET_2
```

---

## 3. IAM Roles and Policies

### 3.1 Create Trust Policy File

```bash
# Create directory
mkdir -p /tmp/iam-policies

# Glue service trust policy
cat > /tmp/iam-policies/glue-trust-policy.json << 'EOF'
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

# Lambda service trust policy
cat > /tmp/iam-policies/lambda-trust-policy.json << 'EOF'
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

# EMR service trust policy
cat > /tmp/iam-policies/emr-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "elasticmapreduce.amazonaws.com",
          "ec2.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
```

### 3.2 Create IAM Policies

```bash
# ETL Framework Full Policy
cat > /tmp/iam-policies/etl-framework-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueAccess",
      "Effect": "Allow",
      "Action": [
        "glue:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::etl-framework-*",
        "arn:aws:s3:::etl-framework-*/*"
      ]
    },
    {
      "Sid": "DynamoDBAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchWriteItem",
        "dynamodb:BatchGetItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/etl_*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchMetrics",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SecretsManager",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:etl-framework/*"
    },
    {
      "Sid": "SES",
      "Effect": "Allow",
      "Action": [
        "ses:SendEmail",
        "ses:SendRawEmail"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EMRAccess",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EC2ForEMR",
      "Effect": "Allow",
      "Action": [
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:AuthorizeSecurityGroupEgress",
        "ec2:CreateSecurityGroup",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceTypes",
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:CreateTags"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": [
            "glue.amazonaws.com",
            "elasticmapreduce.amazonaws.com",
            "eks.amazonaws.com"
          ]
        }
      }
    }
  ]
}
EOF

# Create the policy
aws iam create-policy \
  --policy-name ETLFrameworkFullPolicy \
  --policy-document file:///tmp/iam-policies/etl-framework-policy.json
```

### 3.3 Create IAM Roles

```bash
# Create Glue execution role
aws iam create-role \
  --role-name ETLFrameworkGlueRole \
  --assume-role-policy-document file:///tmp/iam-policies/glue-trust-policy.json

# Attach policies to Glue role
aws iam attach-role-policy \
  --role-name ETLFrameworkGlueRole \
  --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/ETLFrameworkFullPolicy

aws iam attach-role-policy \
  --role-name ETLFrameworkGlueRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Create Lambda execution role
aws iam create-role \
  --role-name ETLFrameworkLambdaRole \
  --assume-role-policy-document file:///tmp/iam-policies/lambda-trust-policy.json

aws iam attach-role-policy \
  --role-name ETLFrameworkLambdaRole \
  --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/ETLFrameworkFullPolicy

aws iam attach-role-policy \
  --role-name ETLFrameworkLambdaRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create EMR execution role
aws iam create-role \
  --role-name ETLFrameworkEMRRole \
  --assume-role-policy-document file:///tmp/iam-policies/emr-trust-policy.json

aws iam attach-role-policy \
  --role-name ETLFrameworkEMRRole \
  --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/ETLFrameworkFullPolicy

aws iam attach-role-policy \
  --role-name ETLFrameworkEMRRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

# Create EMR EC2 instance profile
aws iam create-role \
  --role-name ETLFrameworkEMREC2Role \
  --assume-role-policy-document file:///tmp/iam-policies/emr-trust-policy.json

aws iam attach-role-policy \
  --role-name ETLFrameworkEMREC2Role \
  --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/ETLFrameworkFullPolicy

aws iam attach-role-policy \
  --role-name ETLFrameworkEMREC2Role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role

aws iam create-instance-profile --instance-profile-name ETLFrameworkEMREC2Profile
aws iam add-role-to-instance-profile \
  --instance-profile-name ETLFrameworkEMREC2Profile \
  --role-name ETLFrameworkEMREC2Role

echo "IAM roles created successfully!"
```

---

## 4. S3 Buckets

### 4.1 Create Required Buckets

```bash
# Create buckets (names must be globally unique)
BUCKET_PREFIX="etl-framework-${AWS_ACCOUNT_ID}"

# Data bucket (raw, processed, analytics)
aws s3 mb s3://${BUCKET_PREFIX}-data --region $AWS_REGION

# Scripts bucket (PySpark, configs)
aws s3 mb s3://${BUCKET_PREFIX}-scripts --region $AWS_REGION

# Reports bucket (DQ reports, dashboards)
aws s3 mb s3://${BUCKET_PREFIX}-reports --region $AWS_REGION

# Delta Lake bucket
aws s3 mb s3://${BUCKET_PREFIX}-delta --region $AWS_REGION

# Logs bucket
aws s3 mb s3://${BUCKET_PREFIX}-logs --region $AWS_REGION

echo "Created buckets with prefix: $BUCKET_PREFIX"
```

### 4.2 Configure Bucket Policies

```bash
# Enable versioning for data bucket
aws s3api put-bucket-versioning \
  --bucket ${BUCKET_PREFIX}-data \
  --versioning-configuration Status=Enabled

# Enable lifecycle policy for logs (delete after 90 days)
cat > /tmp/lifecycle-policy.json << EOF
{
  "Rules": [
    {
      "ID": "DeleteOldLogs",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "Expiration": {"Days": 90}
    }
  ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
  --bucket ${BUCKET_PREFIX}-logs \
  --lifecycle-configuration file:///tmp/lifecycle-policy.json
```

### 4.3 Create Directory Structure

```bash
# Create directory structure in data bucket
aws s3api put-object --bucket ${BUCKET_PREFIX}-data --key raw/
aws s3api put-object --bucket ${BUCKET_PREFIX}-data --key processed/
aws s3api put-object --bucket ${BUCKET_PREFIX}-data --key analytics/
aws s3api put-object --bucket ${BUCKET_PREFIX}-data --key master/

# Upload scripts
aws s3 sync scripts/pyspark/ s3://${BUCKET_PREFIX}-scripts/pyspark/
aws s3 sync demo_configs/ s3://${BUCKET_PREFIX}-scripts/configs/
```

---

## 5. DynamoDB Tables

### 5.1 Create Audit Log Table

```bash
aws dynamodb create-table \
  --table-name etl_audit_log \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=timestamp,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --tags Key=Project,Value=ETLFramework

echo "Created etl_audit_log table"
```

### 5.2 Create Execution History Table

```bash
aws dynamodb create-table \
  --table-name etl_execution_history \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=execution_id,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=execution_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --tags Key=Project,Value=ETLFramework

echo "Created etl_execution_history table"
```

### 5.3 Create Job Baselines Table

```bash
aws dynamodb create-table \
  --table-name etl_job_baselines \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --tags Key=Project,Value=ETLFramework

echo "Created etl_job_baselines table"
```

### 5.4 Create Recommendations Table

```bash
aws dynamodb create-table \
  --table-name etl_recommendations \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=recommendation_id,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=recommendation_id,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --tags Key=Project,Value=ETLFramework

echo "Created etl_recommendations table"
```

### 5.5 Create Learning Data Table

```bash
aws dynamodb create-table \
  --table-name etl_learning_data \
  --attribute-definitions \
    AttributeName=job_name,AttributeType=S \
    AttributeName=data_type,AttributeType=S \
  --key-schema \
    AttributeName=job_name,KeyType=HASH \
    AttributeName=data_type,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --tags Key=Project,Value=ETLFramework

echo "Created etl_learning_data table"
```

### 5.6 Verify Tables

```bash
# List all ETL tables
aws dynamodb list-tables --query "TableNames[?starts_with(@, 'etl_')]"
```

---

## 6. Glue Setup

### 6.1 Create Glue Databases

```bash
# Raw data database
aws glue create-database --database-input '{
  "Name": "raw_data",
  "Description": "Raw data ingestion layer"
}'

# Processed data database
aws glue create-database --database-input '{
  "Name": "processed_data",
  "Description": "Cleaned and transformed data"
}'

# Analytics database
aws glue create-database --database-input '{
  "Name": "analytics",
  "Description": "Analytics and aggregated data"
}'

# Master data database
aws glue create-database --database-input '{
  "Name": "master_data",
  "Description": "Reference and master data"
}'

echo "Created Glue databases"
```

### 6.2 Create Sample Tables

```bash
# Customers table
aws glue create-table \
  --database-name raw_data \
  --table-input '{
    "Name": "customers",
    "Description": "Customer master data",
    "StorageDescriptor": {
      "Columns": [
        {"Name": "customer_id", "Type": "string", "Comment": "Unique customer identifier"},
        {"Name": "customer_name", "Type": "string", "Comment": "Full name"},
        {"Name": "email", "Type": "string", "Comment": "Email address"},
        {"Name": "phone", "Type": "string", "Comment": "Phone number"},
        {"Name": "address", "Type": "string", "Comment": "Street address"},
        {"Name": "age", "Type": "int", "Comment": "Customer age"},
        {"Name": "status", "Type": "string", "Comment": "Account status"},
        {"Name": "country", "Type": "string", "Comment": "Country code"}
      ],
      "Location": "s3://'${BUCKET_PREFIX}'-data/raw/customers/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    },
    "TableType": "EXTERNAL_TABLE"
  }'

# Orders table
aws glue create-table \
  --database-name raw_data \
  --table-input '{
    "Name": "orders",
    "Description": "Order transactions",
    "StorageDescriptor": {
      "Columns": [
        {"Name": "order_id", "Type": "string"},
        {"Name": "customer_id", "Type": "string"},
        {"Name": "order_date", "Type": "date"},
        {"Name": "order_status", "Type": "string"},
        {"Name": "shipping_region", "Type": "string"},
        {"Name": "total_amount", "Type": "double"}
      ],
      "Location": "s3://'${BUCKET_PREFIX}'-data/raw/orders/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    },
    "TableType": "EXTERNAL_TABLE",
    "PartitionKeys": [
      {"Name": "year", "Type": "int"},
      {"Name": "month", "Type": "int"}
    ]
  }'

echo "Created sample Glue tables"
```

### 6.3 Create Glue Jobs

```bash
# Simple ETL Job
aws glue create-job \
  --name demo_simple_customer_etl \
  --role ETLFrameworkGlueRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'${BUCKET_PREFIX}'-scripts/pyspark/simple_customer_etl.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--source_database": "raw_data",
    "--source_table": "customers",
    "--target_database": "processed_data",
    "--target_table": "customers_cleaned",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-spark-ui": "true",
    "--spark-event-logs-path": "s3://'${BUCKET_PREFIX}'-logs/spark-ui/"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X" \
  --timeout 60

# Complex Analytics Job
aws glue create-job \
  --name demo_complex_sales_analytics \
  --role ETLFrameworkGlueRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'${BUCKET_PREFIX}'-scripts/pyspark/complex_sales_analytics.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--source_database": "raw_data",
    "--target_database": "analytics",
    "--delta_path": "s3://'${BUCKET_PREFIX}'-delta",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--additional-python-modules": "delta-spark",
    "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type "G.2X" \
  --timeout 180

echo "Created Glue jobs"
```

---

## 7. EMR Setup

### 7.1 Create EMR Security Configuration

```bash
# Create security configuration for encryption
aws emr create-security-configuration \
  --name ETLFrameworkSecurityConfig \
  --security-configuration '{
    "EncryptionConfiguration": {
      "EnableInTransitEncryption": true,
      "EnableAtRestEncryption": true,
      "InTransitEncryptionConfiguration": {
        "TLSCertificateConfiguration": {
          "CertificateProviderType": "PEM",
          "S3Object": "s3://'${BUCKET_PREFIX}'-scripts/certs/emr-certs.zip"
        }
      },
      "AtRestEncryptionConfiguration": {
        "S3EncryptionConfiguration": {
          "EncryptionMode": "SSE-S3"
        },
        "LocalDiskEncryptionConfiguration": {
          "EncryptionKeyProviderType": "AwsKms",
          "AwsKmsKey": "alias/aws/emr"
        }
      }
    }
  }'
```

### 7.2 Create EMR Cluster Configuration Template

```bash
cat > /tmp/emr-cluster-config.json << 'EOF'
{
  "Name": "ETL-Framework-EMR-Cluster",
  "ReleaseLabel": "emr-6.10.0",
  "Applications": [
    {"Name": "Spark"},
    {"Name": "Hadoop"},
    {"Name": "Hive"}
  ],
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Master",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Core",
        "InstanceRole": "CORE",
        "InstanceType": "m5.2xlarge",
        "InstanceCount": 3
      }
    ],
    "Ec2KeyName": "your-key-pair",
    "KeepJobFlowAliveWhenNoSteps": true,
    "TerminationProtected": false
  },
  "ServiceRole": "ETLFrameworkEMRRole",
  "JobFlowRole": "ETLFrameworkEMREC2Profile",
  "LogUri": "s3://BUCKET_PREFIX-logs/emr/",
  "Configurations": [
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4"
      }
    }
  ],
  "Tags": [
    {"Key": "Project", "Value": "ETLFramework"},
    {"Key": "Environment", "Value": "Production"}
  ]
}
EOF

# Update with actual bucket name
sed -i "s/BUCKET_PREFIX/${BUCKET_PREFIX}/g" /tmp/emr-cluster-config.json

echo "EMR configuration template created at /tmp/emr-cluster-config.json"
echo "To create cluster: aws emr create-cluster --cli-input-json file:///tmp/emr-cluster-config.json"
```

---

## 8. EKS with Karpenter

### 8.1 Create EKS Cluster

```bash
# Create EKS cluster using eksctl
cat > /tmp/eks-cluster-config.yaml << EOF
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: etl-framework-eks
  region: ${AWS_REGION}
  version: "1.28"

iam:
  withOIDC: true

managedNodeGroups:
  - name: system
    instanceType: m5.large
    minSize: 2
    maxSize: 4
    desiredCapacity: 2
    labels:
      role: system
    tags:
      Project: ETLFramework

vpc:
  cidr: 10.0.0.0/16
  nat:
    gateway: Single

cloudWatch:
  clusterLogging:
    enableTypes: ["audit", "authenticator", "controllerManager"]

addons:
  - name: vpc-cni
  - name: coredns
  - name: kube-proxy
EOF

# Create the cluster
eksctl create cluster -f /tmp/eks-cluster-config.yaml

# Update kubeconfig
aws eks update-kubeconfig --name etl-framework-eks --region $AWS_REGION
```

### 8.2 Install Karpenter

```bash
# Set environment variables for Karpenter
export KARPENTER_VERSION=v0.32.0
export CLUSTER_NAME=etl-framework-eks
export CLUSTER_ENDPOINT=$(aws eks describe-cluster --name $CLUSTER_NAME --query "cluster.endpoint" --output text)

# Create Karpenter IAM resources
eksctl create iamidentitymapping \
  --username system:node:{{EC2PrivateDNSName}} \
  --cluster $CLUSTER_NAME \
  --arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/KarpenterNodeRole-${CLUSTER_NAME}" \
  --group system:bootstrappers \
  --group system:nodes

# Create Karpenter controller IAM role
eksctl create iamserviceaccount \
  --cluster $CLUSTER_NAME \
  --name karpenter \
  --namespace karpenter \
  --role-name "KarpenterControllerRole-${CLUSTER_NAME}" \
  --attach-policy-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:policy/KarpenterControllerPolicy-${CLUSTER_NAME}" \
  --approve

# Install Karpenter using Helm
helm upgrade --install karpenter oci://public.ecr.aws/karpenter/karpenter \
  --version ${KARPENTER_VERSION} \
  --namespace karpenter \
  --create-namespace \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="arn:aws:iam::${AWS_ACCOUNT_ID}:role/KarpenterControllerRole-${CLUSTER_NAME}" \
  --set settings.clusterName=$CLUSTER_NAME \
  --set settings.clusterEndpoint=$CLUSTER_ENDPOINT \
  --wait
```

### 8.3 Create Karpenter NodePool for Spark

```bash
cat > /tmp/karpenter-nodepool.yaml << 'EOF'
apiVersion: karpenter.sh/v1beta1
kind: NodePool
metadata:
  name: spark-workers
spec:
  template:
    spec:
      requirements:
        - key: kubernetes.io/arch
          operator: In
          values: ["amd64", "arm64"]
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["spot", "on-demand"]
        - key: karpenter.k8s.aws/instance-category
          operator: In
          values: ["m", "r"]
        - key: karpenter.k8s.aws/instance-size
          operator: In
          values: ["xlarge", "2xlarge", "4xlarge"]
      nodeClassRef:
        name: spark-nodes
  limits:
    cpu: 1000
    memory: 2000Gi
  disruption:
    consolidationPolicy: WhenEmpty
    consolidateAfter: 30s
---
apiVersion: karpenter.k8s.aws/v1beta1
kind: EC2NodeClass
metadata:
  name: spark-nodes
spec:
  amiFamily: AL2
  subnetSelectorTerms:
    - tags:
        karpenter.sh/discovery: etl-framework-eks
  securityGroupSelectorTerms:
    - tags:
        karpenter.sh/discovery: etl-framework-eks
  role: KarpenterNodeRole-etl-framework-eks
  tags:
    Project: ETLFramework
    Environment: Production
EOF

kubectl apply -f /tmp/karpenter-nodepool.yaml
```

### 8.4 Install Spark Operator

```bash
# Add Spark Operator Helm repo
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# Install Spark Operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true \
  --set sparkJobNamespace=spark-jobs

# Create namespace for Spark jobs
kubectl create namespace spark-jobs

# Create service account for Spark
kubectl create serviceaccount spark -n spark-jobs
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark-jobs:spark -n spark-jobs
```

---

## 9. Slack Integration

### 9.1 Create Slack App

1. Go to https://api.slack.com/apps
2. Click "Create New App" → "From scratch"
3. Name: `ETL Framework Bot`
4. Select your workspace

### 9.2 Configure Bot Token Scopes

In "OAuth & Permissions", add these Bot Token Scopes:

```
channels:history
channels:read
chat:write
chat:write.public
files:write
groups:history
groups:read
im:history
im:read
im:write
mpim:history
mpim:read
reactions:write
users:read
app_mentions:read
```

### 9.3 Enable Event Subscriptions

1. Go to "Event Subscriptions" → Enable Events
2. Set Request URL to your Lambda endpoint (after creating it)
3. Subscribe to these Bot Events:
   - `app_mention`
   - `message.channels`
   - `message.groups`
   - `message.im`

### 9.4 Install App to Workspace

1. Go to "Install App"
2. Click "Install to Workspace"
3. Authorize the permissions
4. Copy the "Bot User OAuth Token" (starts with `xoxb-`)

### 9.5 Store Slack Credentials

```bash
# Store bot token
aws secretsmanager create-secret \
  --name etl-framework/slack/bot-token \
  --secret-string '{"SLACK_BOT_TOKEN":"xoxb-your-token-here"}'

# Store signing secret (from Basic Information page)
aws secretsmanager create-secret \
  --name etl-framework/slack/signing-secret \
  --secret-string '{"SLACK_SIGNING_SECRET":"your-signing-secret-here"}'

# Store app token (for Socket Mode, optional)
aws secretsmanager create-secret \
  --name etl-framework/slack/app-token \
  --secret-string '{"SLACK_APP_TOKEN":"xapp-your-app-token"}'
```

### 9.6 Create Slack Channel

```bash
# Create a channel for ETL notifications (do this in Slack UI or via API)
# Then invite the bot to the channel:
# /invite @ETL Framework Bot
```

---

## 10. Microsoft Teams Integration

### 10.1 Create Incoming Webhook

1. Open Microsoft Teams
2. Go to the channel where you want notifications
3. Click "..." → "Connectors"
4. Search for "Incoming Webhook"
5. Click "Configure"
6. Name: `ETL Framework`
7. Upload an icon (optional)
8. Click "Create"
9. Copy the webhook URL

### 10.2 Store Teams Webhook URL

```bash
aws secretsmanager create-secret \
  --name etl-framework/teams/webhook-url \
  --secret-string '{"TEAMS_WEBHOOK_URL":"https://outlook.office.com/webhook/xxx/IncomingWebhook/yyy/zzz"}'
```

### 10.3 Test Teams Integration

```bash
# Test sending a message
curl -H "Content-Type: application/json" -d '{
  "@type": "MessageCard",
  "@context": "http://schema.org/extensions",
  "summary": "ETL Framework Test",
  "themeColor": "0076D7",
  "title": "ETL Framework Connected",
  "text": "Teams integration is working!"
}' "YOUR_WEBHOOK_URL"
```

---

## 11. Email (SES) Integration

### 11.1 Verify Sender Email

```bash
# Verify email address (sender)
aws ses verify-email-identity --email-address etl-notifications@yourdomain.com

# Check verification status
aws ses get-identity-verification-attributes \
  --identities etl-notifications@yourdomain.com
```

### 11.2 Verify Domain (for production)

```bash
# Verify domain
aws ses verify-domain-identity --domain yourdomain.com

# Get DKIM tokens
aws ses verify-domain-dkim --domain yourdomain.com

# Add the returned DKIM records to your DNS
```

### 11.3 Request Production Access

```bash
# Check if still in sandbox
aws ses get-account-sending-enabled

# If in sandbox, request production access via AWS Console:
# SES → Account Dashboard → Request Production Access
```

### 11.4 Store Email Configuration

```bash
aws secretsmanager create-secret \
  --name etl-framework/email/config \
  --secret-string '{
    "EMAIL_SENDER": "etl-notifications@yourdomain.com",
    "EMAIL_RECIPIENTS": "data-team@yourdomain.com,analytics@yourdomain.com"
  }'
```

### 11.5 Test Email

```bash
aws ses send-email \
  --from etl-notifications@yourdomain.com \
  --destination 'ToAddresses=your-email@example.com' \
  --message 'Subject={Data="ETL Framework Test"},Body={Text={Data="Email integration is working!"}}'
```

---

## 12. Streamlit Dashboard

### 12.1 Create EC2 Instance for Streamlit

```bash
# Create security group
SG_ID=$(aws ec2 create-security-group \
  --group-name streamlit-sg \
  --description "Streamlit Dashboard Security Group" \
  --vpc-id $VPC_ID \
  --query 'GroupId' --output text)

# Allow HTTP (8501) and SSH
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 8501 --cidr 0.0.0.0/0
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 22 --cidr 0.0.0.0/0

# Launch EC2 instance
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id ami-0c55b159cbfafe1f0 \
  --instance-type t3.medium \
  --key-name your-key-pair \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_1 \
  --associate-public-ip-address \
  --iam-instance-profile Name=ETLFrameworkEMREC2Profile \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=streamlit-dashboard}]' \
  --user-data '#!/bin/bash
    yum update -y
    yum install python3 python3-pip -y
    pip3 install streamlit boto3 pandas plotly
    mkdir -p /opt/streamlit
  ' \
  --query 'Instances[0].InstanceId' --output text)

echo "Streamlit EC2 Instance: $INSTANCE_ID"
```

### 12.2 Deploy Streamlit App

```bash
# Get public IP
PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

# Copy Streamlit app to instance
scp -i your-key.pem framework/integrations/streamlit_app.py ec2-user@$PUBLIC_IP:/opt/streamlit/

# SSH and start Streamlit
ssh -i your-key.pem ec2-user@$PUBLIC_IP << 'EOF'
cd /opt/streamlit
nohup streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0 &
EOF

echo "Streamlit Dashboard: http://$PUBLIC_IP:8501"
```

### 12.3 Alternative: ECS Fargate Deployment

```bash
# Create ECR repository
aws ecr create-repository --repository-name etl-streamlit

# Create Dockerfile
cat > /tmp/Dockerfile.streamlit << 'EOF'
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY streamlit_app.py .
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
EOF

# Build and push (example commands)
# docker build -t etl-streamlit -f /tmp/Dockerfile.streamlit .
# docker tag etl-streamlit:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/etl-streamlit:latest
# docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/etl-streamlit:latest
```

---

## 13. CloudWatch Dashboard

### 13.1 Create CloudWatch Dashboard

```bash
cat > /tmp/cloudwatch-dashboard.json << 'EOF'
{
  "widgets": [
    {
      "type": "metric",
      "x": 0,
      "y": 0,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["ETL/Framework", "JobDuration", "JobName", "demo_simple_customer_etl"],
          ["ETL/Framework", "JobDuration", "JobName", "demo_complex_sales_analytics"]
        ],
        "title": "Job Duration (seconds)",
        "period": 300,
        "stat": "Average"
      }
    },
    {
      "type": "metric",
      "x": 12,
      "y": 0,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["ETL/Framework", "JobSuccess", "JobName", "demo_simple_customer_etl"],
          ["ETL/Framework", "JobSuccess", "JobName", "demo_complex_sales_analytics"]
        ],
        "title": "Job Success Rate",
        "period": 3600,
        "stat": "Average"
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 6,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["ETL/Framework", "RecordsProcessed", "JobName", "demo_simple_customer_etl"],
          ["ETL/Framework", "RecordsProcessed", "JobName", "demo_complex_sales_analytics"]
        ],
        "title": "Records Processed",
        "period": 300,
        "stat": "Sum"
      }
    },
    {
      "type": "metric",
      "x": 12,
      "y": 6,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["ETL/Framework", "DQPassRate", "JobName", "demo_simple_customer_etl"],
          ["ETL/Framework", "DQPassRate", "JobName", "demo_complex_sales_analytics"]
        ],
        "title": "Data Quality Pass Rate",
        "period": 300,
        "stat": "Average"
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 12,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": [
          ["ETL/Framework", "EstimatedCost", "JobName", "demo_simple_customer_etl"],
          ["ETL/Framework", "EstimatedCost", "JobName", "demo_complex_sales_analytics"]
        ],
        "title": "Estimated Cost ($)",
        "period": 3600,
        "stat": "Sum"
      }
    }
  ]
}
EOF

aws cloudwatch put-dashboard \
  --dashboard-name ETL-Framework-Dashboard \
  --dashboard-body file:///tmp/cloudwatch-dashboard.json

echo "CloudWatch Dashboard created: ETL-Framework-Dashboard"
```

### 13.2 Create CloudWatch Alarms

```bash
# Alarm for job failure
aws cloudwatch put-metric-alarm \
  --alarm-name ETL-Job-Failure \
  --alarm-description "Alert when ETL job fails" \
  --metric-name JobSuccess \
  --namespace ETL/Framework \
  --statistic Average \
  --period 300 \
  --threshold 1 \
  --comparison-operator LessThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:etl-alerts

# Alarm for long-running jobs
aws cloudwatch put-metric-alarm \
  --alarm-name ETL-Long-Running-Job \
  --alarm-description "Alert when job exceeds expected duration" \
  --metric-name JobDuration \
  --namespace ETL/Framework \
  --statistic Maximum \
  --period 300 \
  --threshold 7200 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:etl-alerts

# Alarm for low DQ pass rate
aws cloudwatch put-metric-alarm \
  --alarm-name ETL-Low-DQ-PassRate \
  --alarm-description "Alert when data quality drops below threshold" \
  --metric-name DQPassRate \
  --namespace ETL/Framework \
  --statistic Average \
  --period 300 \
  --threshold 0.95 \
  --comparison-operator LessThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:etl-alerts

echo "CloudWatch alarms created"
```

### 13.3 Create SNS Topic for Alerts

```bash
# Create SNS topic
aws sns create-topic --name etl-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:etl-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

echo "SNS topic created and email subscription pending confirmation"
```

---

## 14. Secrets Manager

### 14.1 Summary of All Secrets

```bash
# List all ETL framework secrets
aws secretsmanager list-secrets \
  --filter Key="name",Values="etl-framework"

# The following secrets should exist:
# - etl-framework/slack/bot-token
# - etl-framework/slack/signing-secret
# - etl-framework/teams/webhook-url
# - etl-framework/email/config
```

### 14.2 Create Configuration Secret

```bash
aws secretsmanager create-secret \
  --name etl-framework/config/master \
  --secret-string '{
    "AWS_REGION": "'${AWS_REGION}'",
    "S3_BUCKET_DATA": "'${BUCKET_PREFIX}'-data",
    "S3_BUCKET_SCRIPTS": "'${BUCKET_PREFIX}'-scripts",
    "S3_BUCKET_REPORTS": "'${BUCKET_PREFIX}'-reports",
    "DYNAMODB_AUDIT_TABLE": "etl_audit_log",
    "DYNAMODB_HISTORY_TABLE": "etl_execution_history",
    "GLUE_ROLE": "ETLFrameworkGlueRole",
    "EMR_ROLE": "ETLFrameworkEMRRole"
  }'
```

---

## 15. Lambda Functions

### 15.1 Create Slack Event Handler Lambda

```bash
# Create deployment package
cd framework/integrations
zip -r /tmp/slack-lambda.zip slack_integration.py

# Create Lambda function
aws lambda create-function \
  --function-name etl-slack-handler \
  --runtime python3.9 \
  --handler slack_integration.lambda_handler \
  --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/ETLFrameworkLambdaRole \
  --zip-file fileb:///tmp/slack-lambda.zip \
  --timeout 30 \
  --memory-size 256 \
  --environment "Variables={
    SECRETS_PREFIX=etl-framework,
    AWS_REGION_NAME=${AWS_REGION}
  }"

echo "Slack Lambda function created"
```

### 15.2 Create ETL Trigger Lambda

```bash
# Create trigger Lambda for scheduled/event-based execution
cat > /tmp/etl_trigger.py << 'EOF'
import json
import boto3
import os

glue = boto3.client('glue')

def lambda_handler(event, context):
    job_name = event.get('job_name', os.environ.get('DEFAULT_JOB_NAME'))

    response = glue.start_job_run(
        JobName=job_name,
        Arguments=event.get('arguments', {})
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Started job {job_name}',
            'run_id': response['JobRunId']
        })
    }
EOF

cd /tmp
zip etl-trigger.zip etl_trigger.py

aws lambda create-function \
  --function-name etl-job-trigger \
  --runtime python3.9 \
  --handler etl_trigger.lambda_handler \
  --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/ETLFrameworkLambdaRole \
  --zip-file fileb:///tmp/etl-trigger.zip \
  --timeout 30 \
  --environment "Variables={DEFAULT_JOB_NAME=demo_simple_customer_etl}"

echo "ETL Trigger Lambda created"
```

### 15.3 Create Scheduled Trigger

```bash
# Create EventBridge rule for daily execution
aws events put-rule \
  --name etl-daily-trigger \
  --schedule-expression "cron(0 6 * * ? *)" \
  --description "Daily ETL trigger at 6 AM UTC"

# Add Lambda as target
aws events put-targets \
  --rule etl-daily-trigger \
  --targets '[{
    "Id": "1",
    "Arn": "arn:aws:lambda:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':function:etl-job-trigger",
    "Input": "{\"job_name\": \"demo_complex_sales_analytics\"}"
  }]'

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission \
  --function-name etl-job-trigger \
  --statement-id eventbridge-daily \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/etl-daily-trigger

echo "Scheduled trigger created for daily execution at 6 AM UTC"
```

---

## 16. API Gateway

### 16.1 Create API for Slack Events

```bash
# Create REST API
API_ID=$(aws apigateway create-rest-api \
  --name etl-slack-api \
  --description "API for Slack ETL integration" \
  --query 'id' --output text)

# Get root resource ID
ROOT_ID=$(aws apigateway get-resources \
  --rest-api-id $API_ID \
  --query 'items[0].id' --output text)

# Create /slack resource
SLACK_ID=$(aws apigateway create-resource \
  --rest-api-id $API_ID \
  --parent-id $ROOT_ID \
  --path-part slack \
  --query 'id' --output text)

# Create /slack/events resource
EVENTS_ID=$(aws apigateway create-resource \
  --rest-api-id $API_ID \
  --parent-id $SLACK_ID \
  --path-part events \
  --query 'id' --output text)

# Create POST method
aws apigateway put-method \
  --rest-api-id $API_ID \
  --resource-id $EVENTS_ID \
  --http-method POST \
  --authorization-type NONE

# Set up Lambda integration
aws apigateway put-integration \
  --rest-api-id $API_ID \
  --resource-id $EVENTS_ID \
  --http-method POST \
  --type AWS_PROXY \
  --integration-http-method POST \
  --uri "arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:etl-slack-handler/invocations"

# Deploy API
aws apigateway create-deployment \
  --rest-api-id $API_ID \
  --stage-name prod

# Grant API Gateway permission to invoke Lambda
aws lambda add-permission \
  --function-name etl-slack-handler \
  --statement-id apigateway-slack \
  --action lambda:InvokeFunction \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:${AWS_REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*/POST/slack/events"

echo "API Gateway endpoint: https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/prod/slack/events"
echo "Update this URL in your Slack app's Event Subscriptions"
```

---

## 17. Verification Steps

### 17.1 Verify IAM

```bash
echo "=== IAM Verification ==="

# Check roles exist
aws iam get-role --role-name ETLFrameworkGlueRole --query 'Role.RoleName' 2>/dev/null && echo "✓ Glue role exists" || echo "✗ Glue role missing"
aws iam get-role --role-name ETLFrameworkLambdaRole --query 'Role.RoleName' 2>/dev/null && echo "✓ Lambda role exists" || echo "✗ Lambda role missing"
aws iam get-role --role-name ETLFrameworkEMRRole --query 'Role.RoleName' 2>/dev/null && echo "✓ EMR role exists" || echo "✗ EMR role missing"
```

### 17.2 Verify S3

```bash
echo "=== S3 Verification ==="

aws s3 ls s3://${BUCKET_PREFIX}-data/ 2>/dev/null && echo "✓ Data bucket exists" || echo "✗ Data bucket missing"
aws s3 ls s3://${BUCKET_PREFIX}-scripts/ 2>/dev/null && echo "✓ Scripts bucket exists" || echo "✗ Scripts bucket missing"
aws s3 ls s3://${BUCKET_PREFIX}-reports/ 2>/dev/null && echo "✓ Reports bucket exists" || echo "✗ Reports bucket missing"
```

### 17.3 Verify DynamoDB

```bash
echo "=== DynamoDB Verification ==="

for table in etl_audit_log etl_execution_history etl_job_baselines etl_recommendations etl_learning_data; do
  aws dynamodb describe-table --table-name $table --query 'Table.TableName' 2>/dev/null && echo "✓ $table exists" || echo "✗ $table missing"
done
```

### 17.4 Verify Glue

```bash
echo "=== Glue Verification ==="

# Check databases
for db in raw_data processed_data analytics master_data; do
  aws glue get-database --name $db --query 'Database.Name' 2>/dev/null && echo "✓ Database $db exists" || echo "✗ Database $db missing"
done

# Check jobs
for job in demo_simple_customer_etl demo_complex_sales_analytics; do
  aws glue get-job --job-name $job --query 'Job.Name' 2>/dev/null && echo "✓ Job $job exists" || echo "✗ Job $job missing"
done
```

### 17.5 Verify Secrets

```bash
echo "=== Secrets Manager Verification ==="

for secret in etl-framework/slack/bot-token etl-framework/teams/webhook-url etl-framework/email/config; do
  aws secretsmanager describe-secret --secret-id $secret --query 'Name' 2>/dev/null && echo "✓ Secret $secret exists" || echo "✗ Secret $secret missing"
done
```

### 17.6 Verify Lambda

```bash
echo "=== Lambda Verification ==="

for func in etl-slack-handler etl-job-trigger; do
  aws lambda get-function --function-name $func --query 'Configuration.FunctionName' 2>/dev/null && echo "✓ Lambda $func exists" || echo "✗ Lambda $func missing"
done
```

### 17.7 Full Verification Script

```bash
#!/bin/bash
# save as verify_provisioning.sh

echo "================================================"
echo "ETL Framework Provisioning Verification"
echo "================================================"
echo ""

PASS=0
FAIL=0

check() {
  if eval "$2" > /dev/null 2>&1; then
    echo "✓ $1"
    ((PASS++))
  else
    echo "✗ $1"
    ((FAIL++))
  fi
}

echo "--- IAM ---"
check "Glue Role" "aws iam get-role --role-name ETLFrameworkGlueRole"
check "Lambda Role" "aws iam get-role --role-name ETLFrameworkLambdaRole"
check "EMR Role" "aws iam get-role --role-name ETLFrameworkEMRRole"

echo ""
echo "--- S3 ---"
check "Data Bucket" "aws s3 ls s3://${BUCKET_PREFIX}-data/"
check "Scripts Bucket" "aws s3 ls s3://${BUCKET_PREFIX}-scripts/"
check "Reports Bucket" "aws s3 ls s3://${BUCKET_PREFIX}-reports/"

echo ""
echo "--- DynamoDB ---"
check "Audit Log Table" "aws dynamodb describe-table --table-name etl_audit_log"
check "Execution History Table" "aws dynamodb describe-table --table-name etl_execution_history"
check "Job Baselines Table" "aws dynamodb describe-table --table-name etl_job_baselines"
check "Recommendations Table" "aws dynamodb describe-table --table-name etl_recommendations"

echo ""
echo "--- Glue ---"
check "raw_data Database" "aws glue get-database --name raw_data"
check "processed_data Database" "aws glue get-database --name processed_data"
check "analytics Database" "aws glue get-database --name analytics"
check "Simple ETL Job" "aws glue get-job --job-name demo_simple_customer_etl"
check "Complex ETL Job" "aws glue get-job --job-name demo_complex_sales_analytics"

echo ""
echo "--- Lambda ---"
check "Slack Handler" "aws lambda get-function --function-name etl-slack-handler"
check "Job Trigger" "aws lambda get-function --function-name etl-job-trigger"

echo ""
echo "--- CloudWatch ---"
check "Dashboard" "aws cloudwatch get-dashboard --dashboard-name ETL-Framework-Dashboard"

echo ""
echo "================================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================================"
```

---

## Quick Reference

### Environment Variables

```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1
export BUCKET_PREFIX=etl-framework-${AWS_ACCOUNT_ID}
export CLUSTER_NAME=etl-framework-eks
```

### Key ARNs

```
Glue Role:    arn:aws:iam::${AWS_ACCOUNT_ID}:role/ETLFrameworkGlueRole
Lambda Role:  arn:aws:iam::${AWS_ACCOUNT_ID}:role/ETLFrameworkLambdaRole
EMR Role:     arn:aws:iam::${AWS_ACCOUNT_ID}:role/ETLFrameworkEMRRole
SNS Topic:    arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:etl-alerts
```

### Useful Commands

```bash
# Start simple job
aws glue start-job-run --job-name demo_simple_customer_etl

# Start complex job
aws glue start-job-run --job-name demo_complex_sales_analytics

# Check job status
aws glue get-job-runs --job-name demo_simple_customer_etl --max-results 1

# View CloudWatch logs
aws logs tail /aws-glue/jobs/demo_simple_customer_etl --follow

# Query audit log
aws dynamodb query \
  --table-name etl_audit_log \
  --key-condition-expression "job_name = :jn" \
  --expression-attribute-values '{":jn": {"S": "demo_simple_customer_etl"}}'
```

---

## Next Steps

After completing provisioning:

1. **Generate sample data** - Use the data generation scripts
2. **Run simple demo** - Execute `demo_simple_customer_etl`
3. **Verify integrations** - Check Slack/Teams/Email notifications
4. **Run complex demo** - Execute `demo_complex_sales_analytics`
5. **Review dashboards** - Check CloudWatch and Streamlit dashboards
6. **Configure scheduling** - Set up EventBridge rules for automation

For troubleshooting, see `docs/ENTERPRISE_FRAMEWORK_GUIDE.md`.
