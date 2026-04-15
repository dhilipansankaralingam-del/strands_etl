# EKS + Karpenter Setup Guide

## Overview

This guide explains how to provision EKS with Karpenter for running large-scale PySpark ETL jobs. When data exceeds 500GB, the ETL framework automatically converts Glue jobs to run on EKS with Karpenter for dynamic scaling.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EKS Cluster                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │  System Nodes   │  │ Spark Operator  │  │   Karpenter     │     │
│  │  (m5.large)     │  │  (Controller)   │  │  (Controller)   │     │
│  │  Always-on      │  │                 │  │                 │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Karpenter-Managed Spark Nodes                   │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │   │
│  │  │Executor │ │Executor │ │Executor │ │Executor │  ...       │   │
│  │  │ Pod 1   │ │ Pod 2   │ │ Pod 3   │ │ Pod N   │            │   │
│  │  │ m5.2xl  │ │ m6i.xl  │ │ r5.2xl  │ │ c5.4xl  │            │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘            │   │
│  │  (Spot + On-Demand, AMD64 + ARM64, auto-scaled)              │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────┐                                               │
│  │   Driver Pod     │                                               │
│  │   (Spark App)    │                                               │
│  └──────────────────┘                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌──────────────────────────┐
              │          S3              │
              │  - Source Data           │
              │  - Target Data           │
              │  - PySpark Scripts       │
              └──────────────────────────┘
```

### Where Converted Scripts Reside

When the ETL framework converts Glue code to EKS:

```
Original:  scripts/pyspark/complex_enterprise_sales.py  (GlueContext)
           ↓ (code_conversion_agent)
Converted: converted/complex_enterprise_sales_eks.py    (SparkSession)
```

The converted script is also uploaded to S3:
```
s3://etl-framework-scripts/converted/complex_enterprise_sales_eks.py
```

---

## Prerequisites

### On Your EC2 Instance

```bash
# 1. AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# 2. Configure AWS credentials
aws configure
# OR use instance IAM role (recommended)

# 3. Docker (for building Spark image)
sudo yum install -y docker
sudo systemctl start docker
sudo usermod -aG docker $USER
# Log out and log back in

# 4. Git (if not installed)
sudo yum install -y git
```

### Required IAM Permissions

Your EC2 instance role or IAM user needs:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "eks:*",
                "ec2:*",
                "ecr:*",
                "iam:CreateRole",
                "iam:AttachRolePolicy",
                "iam:PutRolePolicy",
                "iam:PassRole",
                "iam:GetRole",
                "iam:CreateServiceLinkedRole",
                "cloudformation:*",
                "autoscaling:*",
                "elasticloadbalancing:*",
                "s3:*",
                "logs:*",
                "sts:GetCallerIdentity",
                "kms:CreateGrant",
                "kms:DescribeKey"
            ],
            "Resource": "*"
        }
    ]
}
```

---

## Step-by-Step Installation

### Step 1: Clone and Navigate to Provisioning

```bash
cd /home/user/strands_etl
chmod +x scripts/provisioning/*.sh
```

### Step 2: Run EKS + Karpenter Provisioning

```bash
# Dry-run first (see what will be created)
./scripts/provisioning/provision_eks_karpenter.sh --dry-run

# Actual provisioning (takes 15-20 minutes)
./scripts/provisioning/provision_eks_karpenter.sh \
    --cluster-name enterprise-etl-eks \
    --region us-east-1
```

This creates:
- EKS cluster with managed node group (2x m5.large for system pods)
- Karpenter controller with NodePool for Spark (spot + on-demand)
- Spark Operator for submitting SparkApplications
- RBAC for Spark service account
- ECR repository for Spark image

### Step 3: Build and Push Spark Image

```bash
./scripts/provisioning/build_spark_image.sh
```

This creates a Spark 3.5.0 image with:
- Hadoop-AWS for S3 access
- Delta Lake support
- AWS SDK bundle

### Step 4: Verify Installation

```bash
# Check cluster
kubectl get nodes
kubectl get pods -n karpenter
kubectl get pods -n spark-operator

# Check Karpenter NodePool
kubectl get nodepool
kubectl get ec2nodeclass

# Verify Spark Operator
kubectl get crd | grep spark
```

### Step 5: Test with Sample Job

```bash
# Submit test SparkPi job
kubectl apply -f scripts/provisioning/test_spark_job.yaml

# Watch job status
kubectl get sparkapplication -n spark-jobs -w

# Check driver logs
kubectl logs -f $(kubectl get pods -n spark-jobs -l spark-role=driver -o name | head -1) -n spark-jobs

# Watch Karpenter provision nodes
kubectl get nodes -w
```

---

## Running ETL Framework with EKS

### Option 1: Let Framework Auto-Convert

When data exceeds 500GB, the framework automatically converts to EKS:

```bash
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json
```

If `total_size_gb > 500`:
1. `platform_conversion_agent` sets `target_platform = "eks"`
2. `code_conversion_agent` converts GlueContext → SparkSession
3. Generates SparkApplication YAML
4. `execution_agent` submits to EKS

### Option 2: Force EKS Platform

```bash
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json --platform eks
```

### Option 3: Configure in JSON

```json
"platform": {
    "force_platform": "eks",
    "auto_convert": {
        "enabled": "Y",
        "convert_to_eks_threshold_gb": 500
    }
}
```

---

## Configuration Reference

### EKS Config in enterprise_sales_config.json

```json
"eks_config": {
    "cluster_name": "enterprise-etl-eks",
    "namespace": "spark-jobs",
    "use_karpenter": "Y",
    "use_spot": "Y",
    "use_graviton": "Y",
    "executor_instances": 20,
    "executor_memory": "12g",
    "executor_cores": 4,
    "spark_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/spark-etl:latest",
    "karpenter_provisioner": {
        "name": "spark-executors",
        "capacity_types": ["spot", "on-demand"],
        "architectures": ["amd64", "arm64"],
        "instance_families": ["m5", "m6i", "m6g", "r5", "r6i"]
    }
}
```

### Karpenter NodePool Mapping

| Glue Worker Type | CPU/Memory | Karpenter Instance |
|-----------------|------------|-------------------|
| G.1X | 4 vCPU / 16 GB | m5.xlarge, m6i.xlarge |
| G.2X | 8 vCPU / 32 GB | m5.2xlarge, m6i.2xlarge |
| G.4X | 16 vCPU / 64 GB | m5.4xlarge, r5.2xlarge |
| G.8X | 32 vCPU / 128 GB | m5.8xlarge, r5.4xlarge |

---

## Code Conversion Details

### What Gets Converted

| Glue Pattern | EKS Replacement |
|-------------|-----------------|
| `from awsglue.context import GlueContext` | `from pyspark.sql import SparkSession` |
| `GlueContext(spark_context)` | `SparkSession.builder.config("spark.kubernetes.container.image", "...")` |
| `glueContext.create_dynamic_frame.from_catalog(database=X, table_name=Y)` | `spark.table("X.Y")` |
| `dynamic_frame.toDF()` | Direct DataFrame |
| `Job(glueContext); job.init(); job.commit()` | Removed |

### Example Conversion

**Before (Glue):**
```python
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = glueContext.create_dynamic_frame.from_catalog(
    database="finance_db",
    table_name="transactions"
).toDF()

# ... transformations ...

job.commit()
```

**After (EKS):**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("EKS-Spark-Job") \
    .config("spark.kubernetes.container.image", "spark:3.5.0") \
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.table("finance_db.transactions")

# ... transformations ...

print('Job completed')
```

---

## Monitoring

### View Running Jobs

```bash
# List all SparkApplications
kubectl get sparkapplication -n spark-jobs

# Describe specific job
kubectl describe sparkapplication enterprise-etl-job -n spark-jobs

# View driver logs
kubectl logs -f -l spark-role=driver -n spark-jobs

# View executor logs
kubectl logs -f -l spark-role=executor -n spark-jobs
```

### View Karpenter Scaling

```bash
# Watch nodes being provisioned
kubectl get nodes -w

# Check Karpenter logs
kubectl logs -f -n karpenter -l app.kubernetes.io/name=karpenter

# View NodePool status
kubectl describe nodepool spark-executors
```

### Spark UI

```bash
# Port-forward to Spark UI
kubectl port-forward svc/enterprise-etl-job-ui-svc 4040:4040 -n spark-jobs

# Open in browser: http://localhost:4040
```

---

## Cost Optimization

### Spot Instances

Karpenter automatically uses Spot instances (up to 90% savings):

```yaml
# In NodePool
spec:
  template:
    spec:
      requirements:
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["spot", "on-demand"]  # Spot preferred
```

### Graviton (ARM64)

Enable ARM64 for ~20% cost savings:

```yaml
requirements:
  - key: kubernetes.io/arch
    operator: In
    values: ["amd64", "arm64"]  # Graviton included
```

### Consolidation

Karpenter automatically consolidates underutilized nodes:

```yaml
disruption:
  consolidationPolicy: WhenEmpty
  consolidateAfter: 30s
```

---

## Troubleshooting

### Job Stuck in PENDING

```bash
# Check if nodes are being provisioned
kubectl get nodes
kubectl logs -n karpenter -l app.kubernetes.io/name=karpenter | tail -50

# Check pod events
kubectl describe pod <driver-pod> -n spark-jobs
```

### S3 Access Denied

```bash
# Verify IRSA is configured
kubectl describe sa spark-s3 -n spark-jobs

# Check service account annotation
# Should show: eks.amazonaws.com/role-arn: arn:aws:iam::...:role/SparkS3AccessRole-...
```

### Executor OOM

Increase memory in config:
```json
"eks_config": {
    "executor_memory": "16g",
    "executor_cores": 4
}
```

### Image Pull Errors

```bash
# Check ECR login
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com

# Verify image exists
aws ecr list-images --repository-name spark-etl
```

---

## Cleanup

```bash
# Delete SparkApplications
kubectl delete sparkapplication --all -n spark-jobs

# Delete cluster (removes everything)
eksctl delete cluster --name enterprise-etl-eks --region us-east-1
```
