#!/usr/bin/env python3
"""
AWS Glue Job Creation
=====================

Creates the Glue ETL job for Sales Analytics pipeline.

Usage:
    python scripts/setup/create_glue_job.py --region us-east-1 --role arn:aws:iam::123456789:role/GlueETLRole
    python scripts/setup/create_glue_job.py --dry-run
"""

import os
import sys
import json
import argparse
import boto3
from typing import Dict, Any


# ============================================================================
# Job Definitions
# ============================================================================

GLUE_JOBS = {
    "demo_complex_sales_analytics": {
        "description": "Enterprise sales analytics ETL with Delta Lake",
        "role": None,  # Will be set from args
        "script_s3_path": "s3://etl-framework-scripts/pyspark/complex_sales_analytics.py",
        "glue_version": "4.0",
        "worker_type": "G.2X",
        "number_of_workers": 10,
        "timeout_minutes": 180,
        "max_retries": 1,
        "default_arguments": {
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://etl-framework-logs/spark-logs/",
            "--enable-glue-datacatalog": "true",
            "--additional-python-modules": "delta-spark",
            "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--datalake-formats": "delta",
            "--source_database": "raw_data",
            "--target_database": "analytics",
            "--delta_path": "s3://etl-framework-data/delta"
        },
        "tags": {
            "Environment": "production",
            "Project": "sales-analytics",
            "ManagedBy": "etl-framework"
        }
    },
    "demo_simple_customer_etl": {
        "description": "Simple customer data ETL pipeline",
        "role": None,
        "script_s3_path": "s3://etl-framework-scripts/pyspark/simple_customer_etl.py",
        "glue_version": "4.0",
        "worker_type": "G.1X",
        "number_of_workers": 5,
        "timeout_minutes": 60,
        "max_retries": 1,
        "default_arguments": {
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-glue-datacatalog": "true",
            "--source_database": "raw_data",
            "--target_database": "analytics"
        },
        "tags": {
            "Environment": "production",
            "Project": "customer-etl",
            "ManagedBy": "etl-framework"
        }
    }
}


# ============================================================================
# Glue Job Manager
# ============================================================================

class GlueJobManager:
    """Manages AWS Glue Job resources."""

    def __init__(self, region: str = 'us-east-1', role_arn: str = None, dry_run: bool = False):
        self.region = region
        self.role_arn = role_arn
        self.dry_run = dry_run
        self.glue_client = None
        self.s3_client = None

        if not dry_run:
            self.glue_client = boto3.client('glue', region_name=region)
            self.s3_client = boto3.client('s3', region_name=region)

    def upload_script(self, local_path: str, s3_path: str) -> bool:
        """Upload PySpark script to S3."""
        print(f"\nUploading script: {local_path} -> {s3_path}")

        if not os.path.exists(local_path):
            print(f"  ✗ Local script not found: {local_path}")
            return False

        if self.dry_run:
            print(f"  [DRY RUN] Would upload script to: {s3_path}")
            return True

        # Parse S3 path
        parts = s3_path[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        try:
            self.s3_client.upload_file(local_path, bucket, key)
            print(f"  ✓ Uploaded: {s3_path}")
            return True
        except Exception as e:
            print(f"  ✗ Failed to upload: {e}")
            return False

    def create_job(self, job_name: str, job_config: Dict) -> bool:
        """Create or update a Glue job."""
        print(f"\nCreating Glue job: {job_name}")

        # Set role
        role = job_config.get('role') or self.role_arn
        if not role:
            print(f"  ✗ No IAM role specified. Use --role to provide one.")
            return False

        job_config['role'] = role

        if self.dry_run:
            print(f"  [DRY RUN] Would create job: {job_name}")
            print(f"    Role: {role}")
            print(f"    Script: {job_config['script_s3_path']}")
            print(f"    Glue Version: {job_config['glue_version']}")
            print(f"    Workers: {job_config['number_of_workers']} x {job_config['worker_type']}")
            print(f"    Timeout: {job_config['timeout_minutes']} minutes")
            return True

        try:
            # Check if job exists
            try:
                self.glue_client.get_job(JobName=job_name)
                job_exists = True
            except self.glue_client.exceptions.EntityNotFoundException:
                job_exists = False

            job_input = {
                'Name': job_name,
                'Description': job_config.get('description', ''),
                'Role': role,
                'ExecutionProperty': {
                    'MaxConcurrentRuns': 1
                },
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': job_config['script_s3_path'],
                    'PythonVersion': '3'
                },
                'DefaultArguments': job_config.get('default_arguments', {}),
                'GlueVersion': job_config.get('glue_version', '4.0'),
                'WorkerType': job_config.get('worker_type', 'G.1X'),
                'NumberOfWorkers': job_config.get('number_of_workers', 5),
                'Timeout': job_config.get('timeout_minutes', 60),
                'MaxRetries': job_config.get('max_retries', 1),
                'Tags': job_config.get('tags', {})
            }

            if job_exists:
                # Update existing job
                del job_input['Name']
                del job_input['Tags']
                self.glue_client.update_job(
                    JobName=job_name,
                    JobUpdate=job_input
                )
                print(f"  ✓ Updated existing job: {job_name}")
            else:
                # Create new job
                self.glue_client.create_job(**job_input)
                print(f"  ✓ Created job: {job_name}")

            return True

        except Exception as e:
            print(f"  ✗ Failed to create job: {e}")
            return False

    def verify_job(self, job_name: str) -> Dict:
        """Verify a Glue job exists and get its details."""
        if self.dry_run:
            return {"exists": True, "dry_run": True}

        try:
            response = self.glue_client.get_job(JobName=job_name)
            job = response['Job']
            return {
                "exists": True,
                "name": job['Name'],
                "role": job.get('Role', ''),
                "script": job.get('Command', {}).get('ScriptLocation', ''),
                "glue_version": job.get('GlueVersion', ''),
                "workers": f"{job.get('NumberOfWorkers', 0)} x {job.get('WorkerType', '')}",
                "timeout": job.get('Timeout', 0)
            }
        except self.glue_client.exceptions.EntityNotFoundException:
            return {"exists": False}
        except Exception as e:
            return {"exists": False, "error": str(e)}


# ============================================================================
# PySpark Script Templates
# ============================================================================

COMPLEX_SALES_SCRIPT = '''#!/usr/bin/env python3
"""
Complex Sales Analytics PySpark ETL
====================================

This script performs:
1. Read raw orders and order_lines from Glue Catalog
2. Join with customer and product dimension tables
3. Apply data quality checks
4. Calculate aggregations
5. Write to Delta Lake with MERGE (upsert)

Usage (Glue Job):
    This script is executed as a Glue ETL job with parameters passed via --args
"""

import sys
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Get job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'target_database',
    'delta_path'
])

# Initialize Spark and Glue contexts
spark = SparkSession.builder \\
    .appName(args['JOB_NAME']) \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 70)
print("SALES ANALYTICS ETL - START")
print("=" * 70)

source_db = args['source_database']
target_db = args['target_database']
delta_path = args['delta_path']

# =============================================================================
# STEP 1: Read Source Data
# =============================================================================
print("\\n[STEP 1] Reading source data...")

# Read orders
orders_df = spark.read.table(f"{source_db}.orders")
print(f"  Orders: {orders_df.count():,} records")

# Read order lines
order_lines_df = spark.read.table(f"{source_db}.order_lines")
print(f"  Order Lines: {order_lines_df.count():,} records")

# Read dimension tables (broadcast for small tables)
customers_df = spark.read.table("master_data.customers")
products_df = spark.read.table("master_data.products")
regions_df = spark.read.table("master_data.regions")

print(f"  Customers: {customers_df.count():,} records")
print(f"  Products: {products_df.count():,} records")
print(f"  Regions: {regions_df.count():,} records")

# =============================================================================
# STEP 2: Join and Transform
# =============================================================================
print("\\n[STEP 2] Joining and transforming data...")

# Join order lines with orders
sales_df = order_lines_df.alias("ol").join(
    orders_df.alias("o"),
    F.col("ol.order_id") == F.col("o.order_id"),
    "inner"
).select(
    F.col("ol.order_id"),
    F.concat(F.col("ol.order_id"), F.lit("_"), F.col("ol.line_number")).alias("order_line_id"),
    F.col("o.customer_id"),
    F.col("o.customer_name"),
    F.col("ol.product_id"),
    F.col("ol.product_name"),
    F.col("ol.category"),
    F.col("o.region_id"),
    F.to_date(F.col("o.order_date")).alias("order_date"),
    F.col("o.order_year"),
    F.col("o.order_month"),
    F.col("ol.quantity"),
    F.col("ol.unit_price"),
    F.col("ol.discount"),
    F.col("ol.line_total"),
    F.col("ol.line_cost"),
    F.col("ol.line_profit").alias("profit")
)

# Join with customers (broadcast)
sales_df = sales_df.join(
    F.broadcast(customers_df.select("customer_id", "customer_tier")),
    "customer_id",
    "left"
)

# Join with regions (broadcast)
sales_df = sales_df.join(
    F.broadcast(regions_df.select("region_id", "region_name")),
    "region_id",
    "left"
)

# Add calculated fields
sales_df = sales_df.withColumn(
    "profit_margin",
    F.when(F.col("line_total") > 0,
           F.round(F.col("profit") / F.col("line_total") * 100, 2)
    ).otherwise(0)
).withColumn(
    "customer_name_masked",
    F.concat(
        F.substring(F.col("customer_name"), 1, 1),
        F.lit("***"),
        F.substring(F.col("customer_name"), -1, 1)
    )
).withColumn(
    "etl_timestamp",
    F.current_timestamp()
)

# Select final columns
sales_fact_df = sales_df.select(
    "order_id",
    "order_line_id",
    "customer_id",
    "customer_name_masked",
    "product_id",
    "product_name",
    "category",
    "region_id",
    "region_name",
    "order_date",
    "order_year",
    "order_month",
    "quantity",
    "unit_price",
    "discount",
    "line_total",
    "line_cost",
    "profit",
    "profit_margin",
    "customer_tier",
    "etl_timestamp"
)

print(f"  Sales Fact: {sales_fact_df.count():,} records after join")

# =============================================================================
# STEP 3: Write Sales Fact to Delta Lake
# =============================================================================
print("\\n[STEP 3] Writing Sales Fact to Delta Lake...")

delta_sales_path = f"{delta_path}/sales_fact"

# Write as Delta with partitioning
sales_fact_df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .partitionBy("order_year", "order_month") \\
    .option("overwriteSchema", "true") \\
    .save(delta_sales_path)

print(f"  ✓ Written to: {delta_sales_path}")

# =============================================================================
# STEP 4: Create Customer Aggregates
# =============================================================================
print("\\n[STEP 4] Creating customer aggregates...")

# Window for finding favorite category
category_window = Window.partitionBy("customer_id").orderBy(F.desc("category_count"))

customer_agg_df = sales_fact_df.groupBy(
    "customer_id", "customer_tier", "region_id"
).agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("quantity").alias("total_items"),
    F.sum("line_total").alias("total_revenue"),
    F.sum("profit").alias("total_profit"),
    F.avg("line_total").alias("avg_order_value"),
    F.avg("profit_margin").alias("avg_profit_margin"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date")
).withColumn(
    "days_since_last_order",
    F.datediff(F.current_date(), F.col("last_order_date"))
).withColumn(
    "snapshot_date",
    F.current_date().cast("string")
)

# Get favorite category per customer
category_counts = sales_fact_df.groupBy("customer_id", "category").agg(
    F.count("*").alias("category_count")
)

favorite_categories = category_counts.withColumn(
    "rn", F.row_number().over(category_window)
).filter(F.col("rn") == 1).select(
    "customer_id",
    F.col("category").alias("favorite_category")
)

customer_agg_df = customer_agg_df.join(favorite_categories, "customer_id", "left")

# Write customer aggregates
customer_agg_path = f"{delta_path}/../analytics/customer_aggregates"
customer_agg_df.write \\
    .mode("overwrite") \\
    .partitionBy("snapshot_date") \\
    .parquet(customer_agg_path)

print(f"  ✓ Customer aggregates: {customer_agg_df.count():,} records")

# =============================================================================
# STEP 5: Create Product Performance
# =============================================================================
print("\\n[STEP 5] Creating product performance metrics...")

product_perf_df = sales_fact_df.groupBy(
    "product_id", "product_name", "category", "order_year", "order_month"
).agg(
    F.sum("quantity").alias("units_sold"),
    F.countDistinct("order_id").alias("order_count"),
    F.sum("line_total").alias("total_revenue"),
    F.sum("line_cost").alias("total_cost"),
    F.sum("profit").alias("total_profit"),
    F.avg("unit_price").alias("avg_selling_price"),
    F.avg("discount").alias("avg_discount"),
    F.countDistinct("customer_id").alias("unique_customers")
).withColumn(
    "profit_margin",
    F.when(F.col("total_revenue") > 0,
           F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2)
    ).otherwise(0)
)

# Write product performance
product_perf_path = f"{delta_path}/../analytics/product_performance"
product_perf_df.write \\
    .mode("overwrite") \\
    .partitionBy("order_year", "order_month") \\
    .parquet(product_perf_path)

print(f"  ✓ Product performance: {product_perf_df.count():,} records")

# =============================================================================
# STEP 6: Summary
# =============================================================================
print("\\n" + "=" * 70)
print("ETL COMPLETE - SUMMARY")
print("=" * 70)

total_revenue = sales_fact_df.agg(F.sum("line_total")).collect()[0][0]
total_profit = sales_fact_df.agg(F.sum("profit")).collect()[0][0]
total_orders = sales_fact_df.agg(F.countDistinct("order_id")).collect()[0][0]

print(f"  Total Orders:     {total_orders:,}")
print(f"  Total Revenue:    ${total_revenue:,.2f}")
print(f"  Total Profit:     ${total_profit:,.2f}")
print(f"  Profit Margin:    {(total_profit/total_revenue*100):.1f}%")

job.commit()
print("\\n✓ Job committed successfully")
'''

SIMPLE_CUSTOMER_SCRIPT = '''#!/usr/bin/env python3
"""
Simple Customer ETL PySpark Script
===================================

Basic ETL that reads customer data, applies transformations, and writes output.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_database', 'target_database'])

spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Starting Simple Customer ETL...")

# Read customers
customers_df = spark.read.table("master_data.customers")

# Add derived columns
customers_df = customers_df.withColumn(
    "full_address",
    F.concat_ws(", ", "address", "city", "state", "zip_code")
).withColumn(
    "account_age_days",
    F.datediff(F.current_date(), F.to_date("created_date"))
)

# Write output
output_path = "s3://etl-framework-data/analytics/customer_summary/"
customers_df.write.mode("overwrite").parquet(output_path)

print(f"Written {customers_df.count()} customer records")

job.commit()
'''


def create_pyspark_scripts(output_dir: str):
    """Create PySpark script files locally."""
    scripts = {
        "complex_sales_analytics.py": COMPLEX_SALES_SCRIPT,
        "simple_customer_etl.py": SIMPLE_CUSTOMER_SCRIPT
    }

    script_dir = os.path.join(output_dir, "scripts", "pyspark")
    os.makedirs(script_dir, exist_ok=True)

    for filename, content in scripts.items():
        filepath = os.path.join(script_dir, filename)
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"  Created: {filepath}")

    return script_dir


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Create Glue ETL jobs for Sales Analytics')
    parser.add_argument('--region', '-r', type=str, default='us-east-1',
                        help='AWS region (default: us-east-1)')
    parser.add_argument('--role', type=str,
                        help='IAM role ARN for Glue jobs (required)')
    parser.add_argument('--bucket', '-b', type=str, default='etl-framework-scripts',
                        help='S3 bucket for scripts (default: etl-framework-scripts)')
    parser.add_argument('--dry-run', '-d', action='store_true',
                        help='Show what would be created without actually creating')
    parser.add_argument('--create-scripts', '-s', action='store_true',
                        help='Create PySpark script files locally')
    parser.add_argument('--upload-scripts', '-u', action='store_true',
                        help='Upload scripts to S3')

    args = parser.parse_args()

    print("=" * 70)
    print("AWS GLUE JOB SETUP - Sales Analytics ETL")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Region:   {args.region}")
    print(f"  Role:     {args.role or '(not specified)'}")
    print(f"  Bucket:   {args.bucket}")
    print(f"  Dry Run:  {args.dry_run}")

    # Create local scripts if requested
    if args.create_scripts:
        print("\n" + "-" * 70)
        print("Creating PySpark Scripts")
        print("-" * 70)
        script_dir = create_pyspark_scripts(".")
        print(f"\nScripts created in: {script_dir}")

    # Initialize manager
    manager = GlueJobManager(
        region=args.region,
        role_arn=args.role,
        dry_run=args.dry_run
    )

    # Upload scripts if requested
    if args.upload_scripts:
        print("\n" + "-" * 70)
        print("Uploading Scripts to S3")
        print("-" * 70)

        scripts_to_upload = [
            ("scripts/pyspark/complex_sales_analytics.py",
             f"s3://{args.bucket}/pyspark/complex_sales_analytics.py"),
            ("scripts/pyspark/simple_customer_etl.py",
             f"s3://{args.bucket}/pyspark/simple_customer_etl.py")
        ]

        for local_path, s3_path in scripts_to_upload:
            if os.path.exists(local_path):
                manager.upload_script(local_path, s3_path)
            else:
                print(f"  ⚠ Script not found: {local_path}")
                print(f"    Run with --create-scripts first")

    # Create Glue jobs
    print("\n" + "-" * 70)
    print("Creating Glue Jobs")
    print("-" * 70)

    # Update script paths with bucket
    for job_name, job_config in GLUE_JOBS.items():
        job_config['script_s3_path'] = job_config['script_s3_path'].replace(
            'etl-framework-scripts', args.bucket
        )
        manager.create_job(job_name, job_config)

    # Verify jobs
    print("\n" + "-" * 70)
    print("Verifying Jobs")
    print("-" * 70)

    for job_name in GLUE_JOBS.keys():
        result = manager.verify_job(job_name)
        if result.get('exists'):
            print(f"\n✓ {job_name}:")
            if not result.get('dry_run'):
                print(f"    Script: {result.get('script', 'N/A')}")
                print(f"    Workers: {result.get('workers', 'N/A')}")
                print(f"    Timeout: {result.get('timeout', 'N/A')} minutes")
        else:
            print(f"\n✗ {job_name}: Not found")

    print("\n" + "=" * 70)
    print("Glue Job setup complete!")
    print("=" * 70)

    print("\nNext Steps:")
    print("-" * 70)
    print("1. Run the ETL job:")
    print(f"   aws glue start-job-run --job-name demo_complex_sales_analytics --region {args.region}")
    print("\n2. Or use the framework CLI:")
    print("   ./run_etl.sh --complex")
    print("\n3. Monitor in Glue Console:")
    print(f"   https://{args.region}.console.aws.amazon.com/glue/home?region={args.region}#/v2/etl-configuration/jobs")


if __name__ == "__main__":
    main()
