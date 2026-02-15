#!/bin/bash
#===============================================================================
# Build and Push Spark Docker Image to ECR
#===============================================================================

set -e

CLUSTER_NAME="${CLUSTER_NAME:-enterprise-etl-eks}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SPARK_VERSION="${SPARK_VERSION:-3.5.0}"
HADOOP_VERSION="3"
SCALA_VERSION="2.12"

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/spark-etl"

echo "Building Spark image with S3 and Glue support..."
echo "  Spark Version: $SPARK_VERSION"
echo "  ECR URI: $ECR_REPO_URI"

# Create Dockerfile
cat > /tmp/Dockerfile.spark <<'DOCKERFILE'
FROM apache/spark:3.5.0

USER root

# Install AWS SDK and Hadoop-AWS
RUN apt-get update && apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Download Hadoop-AWS and AWS SDK JARs
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o /opt/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -o /opt/spark/jars/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Add Glue Catalog integration
RUN curl -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# Create spark user
RUN useradd -u 185 -g root spark || true

USER 185

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

ENTRYPOINT ["/opt/entrypoint.sh"]
DOCKERFILE

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "$ECR_REPO_URI"

# Build image
echo "Building Docker image..."
docker build -t spark-etl:latest -f /tmp/Dockerfile.spark /tmp

# Tag and push
echo "Pushing to ECR..."
docker tag spark-etl:latest "$ECR_REPO_URI:latest"
docker tag spark-etl:latest "$ECR_REPO_URI:$SPARK_VERSION"
docker push "$ECR_REPO_URI:latest"
docker push "$ECR_REPO_URI:$SPARK_VERSION"

echo ""
echo "========================================================================"
echo " Spark image pushed successfully!"
echo "========================================================================"
echo " Image URI: $ECR_REPO_URI:latest"
echo ""
echo " Update your eks_config in JSON:"
echo '   "spark_image": "'$ECR_REPO_URI':latest"'
echo "========================================================================"
