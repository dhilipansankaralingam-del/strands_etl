#!/usr/bin/env python3
"""
Simple Glue ETL: Customer Daily Processing
==========================================

This script demonstrates how to wrap your existing Glue code
with the ETL Framework for all agent capabilities.

OPTION 1: Using Framework Wrapper (Recommended)
-----------------------------------------------
Wrap your existing code with the framework to get:
- Auto-Healing on failures
- Data Quality validation
- Compliance checks
- Cost recommendations
- Performance analysis

OPTION 2: Standalone with Framework Analysis
---------------------------------------------
Keep your existing code but call framework agents directly
for analysis and recommendations.

Usage:
    Glue Job Parameters:
    --JOB_NAME=glue_customer_daily_etl
    --CONFIG_PATH=s3://your-bucket/configs/glue_simple_use_case.json
    --YEAR=2025
    --MONTH=01
"""

import sys
from datetime import datetime

# Glue imports
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, split, current_timestamp, current_date,
    year, month, when, coalesce, trim, lower, upper
)

# Framework imports (add to extra-py-files)
try:
    from framework.glue_wrapper import GlueETLFramework
    FRAMEWORK_AVAILABLE = True
except ImportError:
    FRAMEWORK_AVAILABLE = False
    print("Framework not available - running standalone")


def run_with_framework():
    """
    OPTION 1: Run your ETL wrapped with the framework.
    This gives you all agent capabilities automatically.
    """
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONFIG_PATH', 'YEAR', 'MONTH'])

    # Initialize framework
    framework = GlueETLFramework(config_path=args['CONFIG_PATH'])

    # Your ETL code runs inside the framework context
    with framework.run() as ctx:
        print(f"\n{'='*60}")
        print(f"Running: {args['JOB_NAME']}")
        print(f"Year: {args['YEAR']}, Month: {args['MONTH']}")
        print(f"{'='*60}\n")

        # ============================================================
        # YOUR EXISTING GLUE CODE GOES HERE
        # Just replace spark.table() with ctx.read_catalog()
        # ============================================================

        # Read from Glue Catalog
        customers_df = ctx.read_catalog("raw_db", "customers")

        print(f"Source records: {customers_df.count()}")

        # Apply transformations (your existing logic)
        customers_transformed = (customers_df
            # Filter active customers with email
            .filter(
                (col("status") == "ACTIVE") &
                (col("email").isNotNull())
            )
            # Clean and transform data
            .withColumn("first_name", trim(col("first_name")))
            .withColumn("last_name", trim(col("last_name")))
            .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("email_domain", split(col("email"), "@").getItem(1))
            # Add metadata columns
            .withColumn("year", lit(int(args['YEAR'])))
            .withColumn("month", lit(int(args['MONTH'])))
            .withColumn("etl_timestamp", current_timestamp())
            # Select final columns
            .select(
                "customer_id",
                "full_name",
                "email",
                "email_domain",
                "phone",
                "city",
                "state",
                "country",
                "registration_date",
                "status",
                "year",
                "month",
                "etl_timestamp"
            )
        )

        print(f"Transformed records: {customers_transformed.count()}")

        # Register for Data Quality validation
        ctx.register_dataframe("customers_output", customers_transformed)

        # Write to Glue Catalog
        ctx.write_to_catalog(
            customers_transformed,
            database="curated_db",
            table="customers_curated",
            mode="overwrite",
            partition_by=["year", "month"]
        )

        print("ETL completed successfully!")

    # Print agent results
    print("\n" + "=" * 60)
    print("AGENT RESULTS SUMMARY")
    print("=" * 60)
    for agent_type, result in framework.get_results_summary().items():
        print(f"{agent_type}: {result['status']} ({result['findings_count']} findings)")


def run_standalone():
    """
    OPTION 2: Run your existing Glue code standalone.
    You can still use framework agents for analysis.
    """
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR', 'MONTH'])

    # Standard Glue setup
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    print(f"\n{'='*60}")
    print(f"Running (Standalone): {args['JOB_NAME']}")
    print(f"{'='*60}\n")

    # ============================================================
    # YOUR EXISTING GLUE CODE - NO CHANGES NEEDED
    # ============================================================

    # Read from Glue Catalog
    customers_df = spark.table("raw_db.customers")

    print(f"Source records: {customers_df.count()}")

    # Apply transformations
    customers_transformed = (customers_df
        .filter(
            (col("status") == "ACTIVE") &
            (col("email").isNotNull())
        )
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
        .withColumn("email_domain", split(col("email"), "@").getItem(1))
        .withColumn("year", lit(int(args['YEAR'])))
        .withColumn("month", lit(int(args['MONTH'])))
        .withColumn("etl_timestamp", current_timestamp())
    )

    print(f"Transformed records: {customers_transformed.count()}")

    # ============================================================
    # OPTIONAL: Run Data Quality checks using framework agent
    # ============================================================
    try:
        from agents.data_quality_nl_agent import DataQualityNLAgent

        dq_agent = DataQualityNLAgent(spark)

        # Add rules using natural language
        dq_agent.add_rule_nl("customer_id should not be null")
        dq_agent.add_rule_nl("customer_id should be unique")
        dq_agent.add_rule_nl("email should not be null")
        dq_agent.add_rule_nl("email should be a valid email")

        # Validate
        dq_results = dq_agent.validate(customers_transformed)

        print("\nData Quality Results:")
        for result in dq_results:
            status = "PASS" if result.passed else "FAIL"
            print(f"  [{status}] {result.rule_name}: {result.pass_rate*100:.1f}%")

    except ImportError:
        print("DQ Agent not available - skipping DQ checks")

    # Write output
    (customers_transformed
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .saveAsTable("curated_db.customers_curated"))

    print("ETL completed!")
    job.commit()


# ============================================================
# MAIN ENTRY POINT
# ============================================================
if __name__ == "__main__":
    if FRAMEWORK_AVAILABLE:
        # Use framework wrapper for full capabilities
        run_with_framework()
    else:
        # Fall back to standalone execution
        run_standalone()
