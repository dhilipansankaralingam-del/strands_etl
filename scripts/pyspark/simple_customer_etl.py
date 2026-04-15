#!/usr/bin/env python3
"""
Simple Customer ETL Script
==========================

This script demonstrates a simple ETL pipeline that:
1. Reads customer data from Glue Catalog
2. Applies basic transformations
3. Performs data quality checks
4. Writes to target table

Compatible with the ETL Framework for:
- Auto-healing on failures
- Data quality validation
- Compliance checking
- Workload assessment
"""

import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, when, trim, lower, upper, regexp_replace,
    current_date, current_timestamp, lit, coalesce
)
from pyspark.sql.types import StringType, IntegerType


def main():
    """Main ETL function."""

    # Initialize Glue context
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_database',
        'source_table',
        'target_database',
        'target_table'
    ])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    print(f"Starting ETL job: {args['JOB_NAME']}")
    print(f"Source: {args['source_database']}.{args['source_table']}")
    print(f"Target: {args['target_database']}.{args['target_table']}")

    # =========================================================================
    # STEP 1: Read source data from Glue Catalog
    # =========================================================================
    print("Step 1: Reading source data...")

    source_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=args['source_database'],
        table_name=args['source_table'],
        transformation_ctx="source_dyf"
    )

    source_df = source_dyf.toDF()
    source_count = source_df.count()
    print(f"Source records read: {source_count}")

    # =========================================================================
    # STEP 2: Apply transformations
    # =========================================================================
    print("Step 2: Applying transformations...")

    # Clean and standardize customer data
    transformed_df = source_df \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) \
        .withColumn("address", trim(col("address"))) \
        .withColumn("status", when(col("status").isNull(), "ACTIVE").otherwise(upper(col("status")))) \
        .withColumn("age", when(col("age") < 0, None).otherwise(col("age")))

    # Add audit columns
    transformed_df = transformed_df \
        .withColumn("load_date", current_date()) \
        .withColumn("etl_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("CRM"))

    # Handle nulls with default values
    transformed_df = transformed_df \
        .withColumn("customer_name", coalesce(col("customer_name"), lit("Unknown"))) \
        .withColumn("country", coalesce(col("country"), lit("US")))

    print(f"Transformations applied")

    # =========================================================================
    # STEP 3: Data quality checks (basic - framework will do comprehensive)
    # =========================================================================
    print("Step 3: Running basic data quality checks...")

    # Count nulls in critical columns
    null_customer_id = transformed_df.filter(col("customer_id").isNull()).count()
    null_email = transformed_df.filter(col("email").isNull()).count()

    print(f"Null customer_id count: {null_customer_id}")
    print(f"Null email count: {null_email}")

    if null_customer_id > 0:
        print("WARNING: Found null customer_id values")
        # Framework will handle this based on DQ rules

    # =========================================================================
    # STEP 4: Write to target
    # =========================================================================
    print("Step 4: Writing to target...")

    # Convert back to DynamicFrame for Glue optimizations
    output_dyf = DynamicFrame.fromDF(transformed_df, glueContext, "output_dyf")

    # Write to Glue Catalog table
    glueContext.write_dynamic_frame.from_catalog(
        frame=output_dyf,
        database=args['target_database'],
        table_name=args['target_table'],
        transformation_ctx="output_dyf"
    )

    target_count = transformed_df.count()
    print(f"Target records written: {target_count}")

    # =========================================================================
    # STEP 5: Job completion
    # =========================================================================
    print("Step 5: Job completion...")

    # Log summary metrics
    print("=" * 50)
    print("ETL JOB SUMMARY")
    print("=" * 50)
    print(f"Job Name: {args['JOB_NAME']}")
    print(f"Source Records: {source_count}")
    print(f"Target Records: {target_count}")
    print(f"Records Processed: {target_count - source_count if target_count != source_count else source_count}")
    print(f"Completion Time: {datetime.now().isoformat()}")
    print("=" * 50)

    # Commit the job (important for bookmarks)
    job.commit()

    print("ETL job completed successfully!")


if __name__ == "__main__":
    main()
