#!/bin/bash
#
# AWS Setup Script for Sales Analytics ETL
# =========================================
#
# This script sets up all AWS resources needed for the ETL framework:
# 1. S3 buckets for data and scripts
# 2. Sample data generation and upload
# 3. Glue databases and tables
# 4. Glue ETL jobs
# 5. IAM roles (optional)
#
# Usage:
#   ./scripts/setup/aws_setup.sh --region us-east-1 --bucket my-etl-bucket
#   ./scripts/setup/aws_setup.sh --full-setup --records 100000
#   ./scripts/setup/aws_setup.sh --dry-run
#

set -e

# =============================================================================
# Configuration
# =============================================================================

REGION="${AWS_REGION:-us-east-1}"
DATA_BUCKET="etl-framework-data"
SCRIPTS_BUCKET="etl-framework-scripts"
LOGS_BUCKET="etl-framework-logs"
GLUE_ROLE_NAME="GlueETLRole"
RECORDS=100000
DRY_RUN=false
FULL_SETUP=false
SKIP_DATA=false
SKIP_CATALOG=false
SKIP_JOBS=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Functions
# =============================================================================

print_header() {
    echo ""
    echo -e "${BLUE}======================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}======================================================================${NC}"
}

print_step() {
    echo -e "\n${GREEN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI not found. Please install it first."
        exit 1
    fi

    # Verify credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured. Run 'aws configure' first."
        exit 1
    fi

    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    print_info "AWS Account: $ACCOUNT_ID"
    print_info "Region: $REGION"
}

check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 not found. Please install it first."
        exit 1
    fi
}

create_s3_buckets() {
    print_step "Creating S3 Buckets"

    for BUCKET in "$DATA_BUCKET" "$SCRIPTS_BUCKET" "$LOGS_BUCKET"; do
        if $DRY_RUN; then
            print_info "[DRY RUN] Would create bucket: $BUCKET"
        else
            if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
                print_info "Bucket already exists: $BUCKET"
            else
                print_info "Creating bucket: $BUCKET"
                if [ "$REGION" = "us-east-1" ]; then
                    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
                else
                    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
                        --create-bucket-configuration LocationConstraint="$REGION"
                fi
                print_success "Created bucket: $BUCKET"
            fi

            # Enable versioning
            aws s3api put-bucket-versioning --bucket "$BUCKET" \
                --versioning-configuration Status=Enabled
        fi
    done
}

create_iam_role() {
    print_step "Creating IAM Role for Glue"

    ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE_NAME}"

    if $DRY_RUN; then
        print_info "[DRY RUN] Would create IAM role: $GLUE_ROLE_NAME"
        return
    fi

    # Check if role exists
    if aws iam get-role --role-name "$GLUE_ROLE_NAME" &>/dev/null; then
        print_info "Role already exists: $GLUE_ROLE_NAME"
        return
    fi

    # Create trust policy
    cat > /tmp/glue-trust-policy.json << EOF
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
        --role-name "$GLUE_ROLE_NAME" \
        --assume-role-policy-document file:///tmp/glue-trust-policy.json \
        --description "IAM role for Glue ETL jobs"

    # Attach policies
    aws iam attach-role-policy \
        --role-name "$GLUE_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

    aws iam attach-role-policy \
        --role-name "$GLUE_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

    aws iam attach-role-policy \
        --role-name "$GLUE_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess

    print_success "Created IAM role: $GLUE_ROLE_NAME"
    print_info "Role ARN: $ROLE_ARN"

    # Wait for role to propagate
    sleep 10
}

generate_sample_data() {
    print_step "Generating Sample Data ($RECORDS records)"

    if $DRY_RUN; then
        print_info "[DRY RUN] Would generate $RECORDS order records"
        return
    fi

    # Install dependencies if needed
    pip install pyarrow boto3 --quiet 2>/dev/null || true

    # Generate data
    python3 scripts/setup/generate_sample_data.py \
        --records "$RECORDS" \
        --output ./sample_data \
        --format parquet

    print_success "Generated sample data in ./sample_data/"
}

upload_data_to_s3() {
    print_step "Uploading Data to S3"

    if $DRY_RUN; then
        print_info "[DRY RUN] Would upload data to s3://$DATA_BUCKET/"
        return
    fi

    # Upload master data
    aws s3 sync ./sample_data/master/ "s3://$DATA_BUCKET/master/" --quiet
    print_success "Uploaded master data"

    # Upload raw data
    aws s3 sync ./sample_data/raw/ "s3://$DATA_BUCKET/raw/" --quiet
    print_success "Uploaded raw data"

    print_info "Data location: s3://$DATA_BUCKET/"
}

upload_scripts_to_s3() {
    print_step "Uploading PySpark Scripts to S3"

    if $DRY_RUN; then
        print_info "[DRY RUN] Would upload scripts to s3://$SCRIPTS_BUCKET/"
        return
    fi

    # Create scripts if they don't exist
    python3 scripts/setup/create_glue_job.py --create-scripts

    # Upload scripts
    aws s3 sync ./scripts/pyspark/ "s3://$SCRIPTS_BUCKET/pyspark/" --quiet
    print_success "Uploaded PySpark scripts"

    print_info "Scripts location: s3://$SCRIPTS_BUCKET/pyspark/"
}

create_glue_catalog() {
    print_step "Creating Glue Catalog (Databases & Tables)"

    if $DRY_RUN; then
        python3 scripts/setup/create_glue_tables.py --region "$REGION" --bucket "$DATA_BUCKET" --dry-run
    else
        python3 scripts/setup/create_glue_tables.py --region "$REGION" --bucket "$DATA_BUCKET"
    fi

    print_success "Glue catalog created"
}

repair_table_partitions() {
    print_step "Repairing Table Partitions"

    if $DRY_RUN; then
        print_info "[DRY RUN] Would repair table partitions"
        return
    fi

    # Use Athena to repair partitions
    print_info "Repairing partitions for raw_data.orders..."

    QUERY_ID=$(aws athena start-query-execution \
        --query-string "MSCK REPAIR TABLE raw_data.orders" \
        --result-configuration "OutputLocation=s3://$LOGS_BUCKET/athena-results/" \
        --region "$REGION" \
        --query 'QueryExecutionId' --output text 2>/dev/null) || true

    if [ -n "$QUERY_ID" ]; then
        print_info "Query ID: $QUERY_ID"
    fi

    QUERY_ID=$(aws athena start-query-execution \
        --query-string "MSCK REPAIR TABLE raw_data.order_lines" \
        --result-configuration "OutputLocation=s3://$LOGS_BUCKET/athena-results/" \
        --region "$REGION" \
        --query 'QueryExecutionId' --output text 2>/dev/null) || true

    print_success "Partition repair initiated"
}

create_glue_jobs() {
    print_step "Creating Glue ETL Jobs"

    ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE_NAME}"

    if $DRY_RUN; then
        python3 scripts/setup/create_glue_job.py \
            --region "$REGION" \
            --bucket "$SCRIPTS_BUCKET" \
            --role "$ROLE_ARN" \
            --dry-run
    else
        python3 scripts/setup/create_glue_job.py \
            --region "$REGION" \
            --bucket "$SCRIPTS_BUCKET" \
            --role "$ROLE_ARN" \
            --upload-scripts
    fi

    print_success "Glue jobs created"
}

verify_setup() {
    print_step "Verifying Setup"

    echo ""
    echo "S3 Buckets:"
    for BUCKET in "$DATA_BUCKET" "$SCRIPTS_BUCKET" "$LOGS_BUCKET"; do
        if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
            echo "  ✓ $BUCKET"
        else
            echo "  ✗ $BUCKET (not found)"
        fi
    done

    echo ""
    echo "Glue Databases:"
    for DB in "raw_data" "master_data" "analytics"; do
        if aws glue get-database --name "$DB" --region "$REGION" &>/dev/null; then
            echo "  ✓ $DB"
        else
            echo "  ✗ $DB (not found)"
        fi
    done

    echo ""
    echo "Glue Jobs:"
    for JOB in "demo_complex_sales_analytics" "demo_simple_customer_etl"; do
        if aws glue get-job --job-name "$JOB" --region "$REGION" &>/dev/null; then
            echo "  ✓ $JOB"
        else
            echo "  ✗ $JOB (not found)"
        fi
    done

    echo ""
    echo "IAM Role:"
    if aws iam get-role --role-name "$GLUE_ROLE_NAME" &>/dev/null; then
        echo "  ✓ $GLUE_ROLE_NAME"
    else
        echo "  ✗ $GLUE_ROLE_NAME (not found)"
    fi
}

print_next_steps() {
    print_header "SETUP COMPLETE - NEXT STEPS"

    echo ""
    echo "1. Run the ETL job:"
    echo "   aws glue start-job-run --job-name demo_complex_sales_analytics --region $REGION"
    echo ""
    echo "2. Or use the framework CLI:"
    echo "   ./run_etl.sh --complex"
    echo ""
    echo "3. Interactive agent predictions:"
    echo "   python scripts/agent_cli.py --config demo_configs/complex_demo_config.json"
    echo ""
    echo "4. Query results in Athena:"
    echo "   SELECT * FROM analytics.sales_fact LIMIT 10;"
    echo ""
    echo "5. Monitor in Glue Console:"
    echo "   https://$REGION.console.aws.amazon.com/glue/home?region=$REGION"
}

# =============================================================================
# Parse Arguments
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --region|-r)
            REGION="$2"
            shift 2
            ;;
        --data-bucket)
            DATA_BUCKET="$2"
            shift 2
            ;;
        --scripts-bucket)
            SCRIPTS_BUCKET="$2"
            shift 2
            ;;
        --bucket|-b)
            DATA_BUCKET="$2-data"
            SCRIPTS_BUCKET="$2-scripts"
            LOGS_BUCKET="$2-logs"
            shift 2
            ;;
        --records|-n)
            RECORDS="$2"
            shift 2
            ;;
        --role-name)
            GLUE_ROLE_NAME="$2"
            shift 2
            ;;
        --dry-run|-d)
            DRY_RUN=true
            shift
            ;;
        --full-setup|-f)
            FULL_SETUP=true
            shift
            ;;
        --skip-data)
            SKIP_DATA=true
            shift
            ;;
        --skip-catalog)
            SKIP_CATALOG=true
            shift
            ;;
        --skip-jobs)
            SKIP_JOBS=true
            shift
            ;;
        --verify)
            check_aws_cli
            verify_setup
            exit 0
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --region, -r REGION      AWS region (default: us-east-1)"
            echo "  --bucket, -b PREFIX      Bucket prefix (creates PREFIX-data, PREFIX-scripts, PREFIX-logs)"
            echo "  --records, -n COUNT      Number of order records to generate (default: 100000)"
            echo "  --dry-run, -d            Show what would be done without making changes"
            echo "  --full-setup, -f         Run complete setup including IAM role"
            echo "  --skip-data              Skip data generation and upload"
            echo "  --skip-catalog           Skip Glue catalog creation"
            echo "  --skip-jobs              Skip Glue job creation"
            echo "  --verify                 Verify existing setup"
            echo "  --help, -h               Show this help"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# =============================================================================
# Main
# =============================================================================

print_header "AWS SETUP - SALES ANALYTICS ETL"

if $DRY_RUN; then
    print_info "DRY RUN MODE - No changes will be made"
fi

echo ""
echo "Configuration:"
echo "  Region:          $REGION"
echo "  Data Bucket:     $DATA_BUCKET"
echo "  Scripts Bucket:  $SCRIPTS_BUCKET"
echo "  Logs Bucket:     $LOGS_BUCKET"
echo "  Records:         $RECORDS"
echo "  IAM Role:        $GLUE_ROLE_NAME"

# Check prerequisites
check_aws_cli
check_python

# Create S3 buckets
create_s3_buckets

# Create IAM role (if full setup)
if $FULL_SETUP; then
    create_iam_role
fi

# Generate and upload data
if ! $SKIP_DATA; then
    generate_sample_data
    upload_data_to_s3
fi

# Upload PySpark scripts
upload_scripts_to_s3

# Create Glue catalog
if ! $SKIP_CATALOG; then
    create_glue_catalog
    repair_table_partitions
fi

# Create Glue jobs
if ! $SKIP_JOBS; then
    create_glue_jobs
fi

# Verify setup
verify_setup

# Print next steps
print_next_steps

echo ""
print_success "Setup complete!"
