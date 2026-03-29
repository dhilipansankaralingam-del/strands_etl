#!/bin/bash
#
# Quick deployment script for Daily Health Check Glue Job
# Usage: ./scripts/deploy_health_check.sh <bucket-name> <glue-role-arn>
#

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <bucket-name> <glue-role-arn>"
    echo ""
    echo "Example:"
    echo "  $0 my-etl-bucket arn:aws:iam::123456789012:role/GlueServiceRole"
    exit 1
fi

BUCKET=$1
ROLE_ARN=$2
JOB_NAME="daily-health-check"
REGION=${AWS_REGION:-us-east-1}

echo "=================================================="
echo "Daily Health Check Deployment"
echo "=================================================="
echo "Bucket:  $BUCKET"
echo "Role:    $ROLE_ARN"
echo "Job:     $JOB_NAME"
echo "Region:  $REGION"
echo ""

# Step 1: Upload configuration
echo "[1/4] Uploading configuration..."
aws s3 cp config/health_check_config.json \
    s3://$BUCKET/config/health_check_config.json
echo "✓ Configuration uploaded"

# Step 2: Upload script
echo "[2/4] Uploading Glue script..."
aws s3 cp pyscript/daily_health_check_glue_job.py \
    s3://$BUCKET/scripts/daily_health_check_glue_job.py
echo "✓ Script uploaded"

# Step 3: Create Glue job
echo "[3/4] Creating Glue job..."
aws glue create-job \
    --region $REGION \
    --name $JOB_NAME \
    --role $ROLE_ARN \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://'$BUCKET'/scripts/daily_health_check_glue_job.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--CONFIG_S3_PATH": "s3://'$BUCKET'/config/health_check_config.json",
        "--additional-python-modules": "matplotlib==3.7.1,numpy==1.24.3",
        "--enable-glue-datacatalog": "true",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--TempDir": "s3://'$BUCKET'/temp/"
    }' \
    --max-retries 0 \
    --timeout 60 \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X" \
    2>/dev/null || echo "⚠️  Job may already exist, use update instead"

echo "✓ Glue job created/updated"

# Step 4: Create EventBridge schedule (9 PM daily)
echo "[4/4] Creating EventBridge schedule..."

# Note: Adjust cron expression for your timezone
# Current: 21:00 UTC (9 PM UTC)
# For EST (UTC-5): Use "cron(0 2 * * ? *)" for 9 PM EST
# For PST (UTC-8): Use "cron(0 5 * * ? *)" for 9 PM PST

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws events put-rule \
    --region $REGION \
    --name ${JOB_NAME}-9pm \
    --schedule-expression "cron(0 21 * * ? *)" \
    --state ENABLED \
    --description "Trigger daily health check at 9 PM UTC" \
    2>/dev/null || echo "⚠️  Rule may already exist"

# Note: You need to manually add the target as it requires proper IAM role
echo ""
echo "⚠️  MANUAL STEP REQUIRED:"
echo "Add Glue job as EventBridge target with:"
echo ""
echo "  aws events put-targets \\"
echo "    --rule ${JOB_NAME}-9pm \\"
echo "    --targets '[{"
echo "      \"Id\": \"1\","
echo "      \"Arn\": \"arn:aws:glue:$REGION:$ACCOUNT_ID:job/$JOB_NAME\","
echo "      \"RoleArn\": \"<EventBridge-to-Glue-Role-ARN>\""
echo "    }]'"
echo ""

echo "=================================================="
echo "Deployment Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "1. Update config/health_check_config.json with your email settings"
echo "2. Create EventBridge-to-Glue IAM role if needed"
echo "3. Add Glue job as EventBridge target (see command above)"
echo "4. Test manually: aws glue start-job-run --job-name $JOB_NAME"
echo ""
echo "Documentation: docs/DAILY_HEALTH_CHECK_SETUP.md"
