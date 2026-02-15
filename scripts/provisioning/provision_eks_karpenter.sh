#!/bin/bash
#===============================================================================
# EKS + Karpenter + Spark Provisioning Script
#===============================================================================
# This script provisions:
#   1. EKS Cluster with managed node group
#   2. Karpenter for dynamic node provisioning
#   3. Spark Operator for running Spark jobs on Kubernetes
#
# Prerequisites:
#   - AWS CLI configured with appropriate permissions
#   - kubectl, eksctl, helm installed
#   - EC2 instance with IAM role or credentials
#
# Usage:
#   ./provision_eks_karpenter.sh [--cluster-name NAME] [--region REGION]
#===============================================================================

set -e

# Default configuration
CLUSTER_NAME="${CLUSTER_NAME:-enterprise-etl-eks}"
AWS_REGION="${AWS_REGION:-us-east-1}"
K8S_VERSION="1.29"
KARPENTER_VERSION="v0.33.0"
SPARK_OPERATOR_VERSION="1.1.27"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster-name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--cluster-name NAME] [--region REGION] [--dry-run]"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

echo "========================================================================"
echo " EKS + Karpenter Provisioning"
echo "========================================================================"
echo " Cluster Name: $CLUSTER_NAME"
echo " Region:       $AWS_REGION"
echo " K8s Version:  $K8S_VERSION"
echo " Karpenter:    $KARPENTER_VERSION"
echo "========================================================================"

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account: $AWS_ACCOUNT_ID"

#===============================================================================
# Step 1: Install Prerequisites
#===============================================================================
echo ""
echo "Step 1: Checking prerequisites..."

# Check eksctl
if ! command -v eksctl &> /dev/null; then
    echo "Installing eksctl..."
    curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
    sudo mv /tmp/eksctl /usr/local/bin
fi
echo "  ✓ eksctl $(eksctl version)"

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    echo "Installing kubectl..."
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin
fi
echo "  ✓ kubectl $(kubectl version --client --short 2>/dev/null || kubectl version --client)"

# Check helm
if ! command -v helm &> /dev/null; then
    echo "Installing helm..."
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi
echo "  ✓ helm $(helm version --short)"

#===============================================================================
# Step 2: Create EKS Cluster
#===============================================================================
echo ""
echo "Step 2: Creating EKS Cluster..."

# Check if cluster exists
if eksctl get cluster --name "$CLUSTER_NAME" --region "$AWS_REGION" 2>/dev/null; then
    echo "  Cluster $CLUSTER_NAME already exists"
else
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Would create cluster $CLUSTER_NAME"
    else
        echo "  Creating cluster $CLUSTER_NAME (this takes 15-20 minutes)..."

        eksctl create cluster \
            --name "$CLUSTER_NAME" \
            --region "$AWS_REGION" \
            --version "$K8S_VERSION" \
            --nodegroup-name "system-nodes" \
            --node-type "m5.large" \
            --nodes 2 \
            --nodes-min 1 \
            --nodes-max 3 \
            --managed \
            --with-oidc \
            --tags "Project=ETL-Framework,Environment=Production"

        echo "  ✓ Cluster created"
    fi
fi

# Update kubeconfig
aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"
echo "  ✓ kubeconfig updated"

#===============================================================================
# Step 3: Install Karpenter
#===============================================================================
echo ""
echo "Step 3: Installing Karpenter..."

# Create Karpenter IAM Role
KARPENTER_ROLE_NAME="KarpenterControllerRole-${CLUSTER_NAME}"

# Check if IAM role exists
if aws iam get-role --role-name "$KARPENTER_ROLE_NAME" 2>/dev/null; then
    echo "  IAM role $KARPENTER_ROLE_NAME already exists"
else
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Would create IAM role $KARPENTER_ROLE_NAME"
    else
        echo "  Creating Karpenter IAM resources..."

        # Create the Karpenter node role
        eksctl create iamidentitymapping \
            --username system:node:{{EC2PrivateDNSName}} \
            --cluster "$CLUSTER_NAME" \
            --arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/KarpenterNodeRole-${CLUSTER_NAME}" \
            --group system:bootstrappers \
            --group system:nodes \
            --region "$AWS_REGION" 2>/dev/null || true
    fi
fi

# Tag subnets for Karpenter
echo "  Tagging subnets for Karpenter..."
for SUBNET_ID in $(aws ec2 describe-subnets \
    --filters "Name=tag:eksctl.cluster.k8s.io/v1alpha1/cluster-name,Values=$CLUSTER_NAME" \
    --query 'Subnets[].SubnetId' --output text --region "$AWS_REGION"); do

    aws ec2 create-tags \
        --resources "$SUBNET_ID" \
        --tags "Key=karpenter.sh/discovery,Value=$CLUSTER_NAME" \
        --region "$AWS_REGION" 2>/dev/null || true
done

# Tag security groups
CLUSTER_SG=$(aws eks describe-cluster \
    --name "$CLUSTER_NAME" \
    --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' \
    --output text --region "$AWS_REGION")

aws ec2 create-tags \
    --resources "$CLUSTER_SG" \
    --tags "Key=karpenter.sh/discovery,Value=$CLUSTER_NAME" \
    --region "$AWS_REGION" 2>/dev/null || true

# Install Karpenter via Helm
echo "  Installing Karpenter via Helm..."

helm repo add karpenter https://charts.karpenter.sh/ 2>/dev/null || true
helm repo update

if helm status karpenter -n karpenter 2>/dev/null; then
    echo "  Karpenter already installed"
else
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Would install Karpenter $KARPENTER_VERSION"
    else
        kubectl create namespace karpenter 2>/dev/null || true

        helm upgrade --install karpenter oci://public.ecr.aws/karpenter/karpenter \
            --version "$KARPENTER_VERSION" \
            --namespace karpenter \
            --set "settings.clusterName=$CLUSTER_NAME" \
            --set "settings.clusterEndpoint=$(aws eks describe-cluster --name $CLUSTER_NAME --region $AWS_REGION --query 'cluster.endpoint' --output text)" \
            --set "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::${AWS_ACCOUNT_ID}:role/${KARPENTER_ROLE_NAME}" \
            --wait

        echo "  ✓ Karpenter installed"
    fi
fi

#===============================================================================
# Step 4: Create Karpenter NodePool for Spark
#===============================================================================
echo ""
echo "Step 4: Creating Karpenter NodePool for Spark..."

if [[ "$DRY_RUN" == "true" ]]; then
    echo "  [DRY-RUN] Would create Spark NodePool"
else
    cat <<EOF | kubectl apply -f -
apiVersion: karpenter.sh/v1beta1
kind: NodePool
metadata:
  name: spark-executors
spec:
  template:
    metadata:
      labels:
        workload-type: spark
    spec:
      requirements:
        - key: kubernetes.io/arch
          operator: In
          values: ["amd64", "arm64"]
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["spot", "on-demand"]
        - key: karpenter.k8s.aws/instance-family
          operator: In
          values: ["m5", "m6i", "m6g", "r5", "r6i", "c5", "c6i"]
        - key: karpenter.k8s.aws/instance-size
          operator: In
          values: ["large", "xlarge", "2xlarge", "4xlarge"]
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
        karpenter.sh/discovery: "$CLUSTER_NAME"
  securityGroupSelectorTerms:
    - tags:
        karpenter.sh/discovery: "$CLUSTER_NAME"
  role: "KarpenterNodeRole-${CLUSTER_NAME}"
  tags:
    Project: ETL-Framework
    ManagedBy: Karpenter
EOF
    echo "  ✓ NodePool 'spark-executors' created"
fi

#===============================================================================
# Step 5: Install Spark Operator
#===============================================================================
echo ""
echo "Step 5: Installing Spark Operator..."

helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator 2>/dev/null || true
helm repo update

if helm status spark-operator -n spark-operator 2>/dev/null; then
    echo "  Spark Operator already installed"
else
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Would install Spark Operator"
    else
        kubectl create namespace spark-operator 2>/dev/null || true
        kubectl create namespace spark-jobs 2>/dev/null || true

        helm upgrade --install spark-operator spark-operator/spark-operator \
            --version "$SPARK_OPERATOR_VERSION" \
            --namespace spark-operator \
            --set webhook.enable=true \
            --set webhook.port=443 \
            --set sparkJobNamespace=spark-jobs \
            --set serviceAccounts.spark.create=true \
            --set serviceAccounts.spark.name=spark \
            --wait

        echo "  ✓ Spark Operator installed"
    fi
fi

#===============================================================================
# Step 6: Create Spark Service Account and RBAC
#===============================================================================
echo ""
echo "Step 6: Creating Spark RBAC..."

if [[ "$DRY_RUN" == "true" ]]; then
    echo "  [DRY-RUN] Would create Spark RBAC"
else
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-jobs
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "persistentvolumeclaims"]
  verbs: ["*"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications", "scheduledsparkapplications"]
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: spark-jobs
subjects:
- kind: ServiceAccount
  name: spark
  namespace: spark-jobs
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-cluster-role
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch", "create", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-cluster-role-binding
subjects:
- kind: ServiceAccount
  name: spark
  namespace: spark-jobs
roleRef:
  kind: ClusterRole
  name: spark-cluster-role
  apiGroup: rbac.authorization.k8s.io
EOF
    echo "  ✓ Spark RBAC created"
fi

#===============================================================================
# Step 7: Create S3 Access for Spark
#===============================================================================
echo ""
echo "Step 7: Creating S3 access for Spark..."

SPARK_S3_ROLE_NAME="SparkS3AccessRole-${CLUSTER_NAME}"

if aws iam get-role --role-name "$SPARK_S3_ROLE_NAME" 2>/dev/null; then
    echo "  IAM role $SPARK_S3_ROLE_NAME already exists"
else
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Would create S3 access role"
    else
        # Create IRSA for Spark to access S3
        eksctl create iamserviceaccount \
            --name spark-s3 \
            --namespace spark-jobs \
            --cluster "$CLUSTER_NAME" \
            --region "$AWS_REGION" \
            --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
            --attach-policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess \
            --approve \
            --override-existing-serviceaccounts

        echo "  ✓ S3 access configured via IRSA"
    fi
fi

#===============================================================================
# Step 8: Build and Push Spark Docker Image
#===============================================================================
echo ""
echo "Step 8: Setting up Spark container image..."

ECR_REPO_NAME="spark-etl"
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

# Create ECR repository
if aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" --region "$AWS_REGION" 2>/dev/null; then
    echo "  ECR repository $ECR_REPO_NAME exists"
else
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY-RUN] Would create ECR repository $ECR_REPO_NAME"
    else
        aws ecr create-repository \
            --repository-name "$ECR_REPO_NAME" \
            --region "$AWS_REGION" \
            --image-scanning-configuration scanOnPush=true
        echo "  ✓ ECR repository created"
    fi
fi

echo "  ECR URI: $ECR_REPO_URI"

#===============================================================================
# Summary
#===============================================================================
echo ""
echo "========================================================================"
echo " EKS + Karpenter Setup Complete!"
echo "========================================================================"
echo ""
echo " Cluster:       $CLUSTER_NAME"
echo " Region:        $AWS_REGION"
echo " K8s Version:   $K8S_VERSION"
echo " Karpenter:     $KARPENTER_VERSION"
echo " ECR Repo:      $ECR_REPO_URI"
echo ""
echo " Namespaces:"
echo "   - karpenter:     Karpenter controller"
echo "   - spark-operator: Spark Operator"
echo "   - spark-jobs:     Your Spark applications"
echo ""
echo " Next steps:"
echo "   1. Build and push Spark image:"
echo "      ./build_spark_image.sh"
echo ""
echo "   2. Submit a test Spark job:"
echo "      kubectl apply -f test_spark_job.yaml"
echo ""
echo "   3. Run ETL framework with EKS:"
echo "      python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json --platform eks"
echo ""
echo "========================================================================"
