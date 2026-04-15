#!/usr/bin/env python3
"""
Complex Glue ETL: Sales Analytics Pipeline
==========================================

This script demonstrates a complex ETL pipeline with:
- Multi-table joins from Glue Catalog
- Aggregations and window functions
- Multiple output tables
- Full framework integration with all agents

Tables Read (from Glue Catalog):
- sales_db.transactions
- crm_db.customers
- master_db.products
- master_db.stores
- marketing_db.promotions

Tables Written (to Glue Catalog):
- analytics_db.sales_facts (main fact table)
- analytics_db.sales_daily_summary (daily aggregates)
- analytics_db.customer_sales_metrics (customer RFM scores)

Agents Used:
- Code Analysis: Analyzes PySpark code for optimizations
- Data Quality: Validates data with NL rules
- Compliance: GDPR/PCI-DSS checks
- Workload Assessment: Resource recommendations
- AWS Recommendations: Cost optimization
- Auto-Healing: Retry on failures

Usage:
    Glue Job Parameters:
    --JOB_NAME=glue_sales_analytics_pipeline
    --CONFIG_PATH=s3://your-bucket/configs/glue_complex_use_case.json
    --START_DATE=2025-01-01
    --END_DATE=2025-02-01
"""

import sys
from datetime import datetime, timedelta

# Glue imports
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, concat, concat_ws, split, current_timestamp, current_date,
    year, month, dayofmonth, dayofweek, datediff,
    when, coalesce, trim, lower, upper, abs as spark_abs,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    countDistinct, first, collect_list,
    row_number, rank, dense_rank, ntile, percent_rank,
    broadcast, sha2
)
from pyspark.sql.types import *

# Framework imports
try:
    from framework.glue_wrapper import GlueETLFramework
    from agents.data_quality_nl_agent import DataQualityNLAgent
    from agents.pyspark_code_analysis_agent import PySparkCodeAnalysisAgent
    FRAMEWORK_AVAILABLE = True
except ImportError:
    FRAMEWORK_AVAILABLE = False
    print("Framework not available - running standalone")


class SalesAnalyticsPipeline:
    """
    Complex sales analytics ETL pipeline.
    Processes transactions with customers, products, stores, and promotions.
    """

    def __init__(self, spark: SparkSession, config: dict, args: dict):
        self.spark = spark
        self.config = config
        self.args = args
        self.dataframes = {}

    def read_source_tables(self):
        """Read all source tables from Glue Catalog with push-down predicates."""
        print("\n" + "=" * 60)
        print("READING SOURCE TABLES")
        print("=" * 60)

        # Transactions (large table - with predicate pushdown)
        start_date = self.args['START_DATE']
        end_date = self.args['END_DATE']

        print(f"Reading transactions: {start_date} to {end_date}")
        self.dataframes['transactions'] = (self.spark
            .table("sales_db.transactions")
            .filter(
                (col("transaction_date") >= start_date) &
                (col("transaction_date") < end_date)
            ))
        print(f"  Transactions: {self.dataframes['transactions'].count()} rows")

        # Customers (medium table - will broadcast)
        print("Reading customers...")
        self.dataframes['customers'] = self.spark.table("crm_db.customers")
        print(f"  Customers: {self.dataframes['customers'].count()} rows")

        # Products (small table - will broadcast)
        print("Reading products...")
        self.dataframes['products'] = self.spark.table("master_db.products")
        print(f"  Products: {self.dataframes['products'].count()} rows")

        # Stores (small table - will broadcast)
        print("Reading stores...")
        self.dataframes['stores'] = self.spark.table("master_db.stores")
        print(f"  Stores: {self.dataframes['stores'].count()} rows")

        # Promotions (small table - with filter)
        print("Reading active promotions...")
        self.dataframes['promotions'] = (self.spark
            .table("marketing_db.promotions")
            .filter(col("is_active") == True))
        print(f"  Promotions: {self.dataframes['promotions'].count()} rows")

    def join_tables(self):
        """
        Join all tables using optimal join strategies.
        Uses broadcast joins for small tables to avoid shuffle.
        """
        print("\n" + "=" * 60)
        print("JOINING TABLES")
        print("=" * 60)

        # Start with transactions (largest table)
        df = self.dataframes['transactions']

        # Join with customers (broadcast - medium size)
        print("Joining transactions + customers (broadcast)...")
        customers_cols = ["customer_id", "customer_name", "customer_email",
                         "customer_phone", "customer_segment", "customer_registration_date"]
        df = df.join(
            broadcast(self.dataframes['customers'].select(customers_cols)),
            "customer_id",
            "left"
        )

        # Join with products (broadcast - small)
        print("Joining + products (broadcast)...")
        products_cols = ["product_id", "product_name", "product_category",
                        "product_subcategory", "unit_cost"]
        df = df.join(
            broadcast(self.dataframes['products'].select(products_cols)),
            "product_id",
            "left"
        )

        # Join with stores (broadcast - small)
        print("Joining + stores (broadcast)...")
        stores_cols = ["store_id", "store_name", "store_region", "store_city", "tax_rate"]
        df = df.join(
            broadcast(self.dataframes['stores'].select(stores_cols)),
            "store_id",
            "left"
        )

        # Join with promotions (broadcast - small, left join for nulls)
        print("Joining + promotions (broadcast)...")
        promotions_cols = ["promotion_id", "promotion_name", "promotion_discount_pct"]
        df = df.join(
            broadcast(self.dataframes['promotions'].select(promotions_cols)),
            "promotion_id",
            "left"
        )

        self.dataframes['joined'] = df
        print(f"Joined result: {df.count()} rows")

    def apply_transformations(self):
        """Apply business logic transformations and calculate metrics."""
        print("\n" + "=" * 60)
        print("APPLYING TRANSFORMATIONS")
        print("=" * 60)

        df = self.dataframes['joined']

        # Filter valid transactions
        print("Filtering valid transactions...")
        df = df.filter(
            (col("transaction_status").isin("COMPLETED", "REFUNDED")) &
            (col("quantity") > 0) &
            (col("unit_price") > 0)
        )

        # Calculate amounts
        print("Calculating amounts...")
        df = (df
            .withColumn("gross_amount", col("quantity") * col("unit_price"))
            .withColumn("discount_amount",
                coalesce(col("quantity") * col("unit_price") * col("promotion_discount_pct") / 100, lit(0)))
            .withColumn("net_amount",
                col("gross_amount") - col("discount_amount"))
            .withColumn("tax_amount",
                col("net_amount") * coalesce(col("tax_rate"), lit(0)))
            .withColumn("total_amount",
                col("net_amount") + col("tax_amount"))
        )

        # Add date dimensions
        print("Adding date dimensions...")
        df = (df
            .withColumn("year", year(col("transaction_date")))
            .withColumn("month", month(col("transaction_date")))
            .withColumn("day", dayofmonth(col("transaction_date")))
            .withColumn("day_of_week", dayofweek(col("transaction_date")))
            .withColumn("is_weekend",
                when(dayofweek(col("transaction_date")).isin(1, 7), lit(True))
                .otherwise(lit(False)))
        )

        # Add derived columns
        print("Adding derived columns...")
        df = (df
            .withColumn("region", coalesce(col("store_region"), lit("UNKNOWN")))
            .withColumn("customer_tenure_days",
                datediff(col("transaction_date"), col("customer_registration_date")))
            .withColumn("profit_margin",
                (col("unit_price") - coalesce(col("unit_cost"), lit(0))) / col("unit_price"))
            .withColumn("etl_load_timestamp", current_timestamp())
        )

        self.dataframes['transformed'] = df
        print(f"Transformed: {df.count()} rows")

    def create_aggregations(self):
        """Create aggregated output tables."""
        print("\n" + "=" * 60)
        print("CREATING AGGREGATIONS")
        print("=" * 60)

        df = self.dataframes['transformed']

        # Daily Summary
        print("Creating daily summary...")
        daily_summary = (df
            .groupBy("year", "month", "day", "region", "product_category")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity"),
                spark_sum("gross_amount").alias("gross_revenue"),
                spark_sum("discount_amount").alias("total_discounts"),
                spark_sum("net_amount").alias("net_revenue"),
                spark_sum("tax_amount").alias("total_tax"),
                spark_sum("total_amount").alias("total_revenue"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("product_id").alias("unique_products"),
                avg("total_amount").alias("avg_transaction_value"),
                spark_max("total_amount").alias("max_transaction_value")
            )
            .withColumn("etl_load_timestamp", current_timestamp())
        )
        self.dataframes['daily_summary'] = daily_summary
        print(f"Daily summary: {daily_summary.count()} rows")

        # Customer Metrics with RFM
        print("Creating customer metrics with RFM scores...")
        customer_metrics = (df
            .groupBy("customer_id", "customer_name", "customer_email", "customer_segment")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_transaction_value"),
                spark_min("transaction_date").alias("first_transaction_date"),
                spark_max("transaction_date").alias("last_transaction_date"),
                countDistinct("product_id").alias("unique_products_purchased"),
                countDistinct("store_id").alias("unique_stores_visited"),
                spark_sum("discount_amount").alias("total_discounts_received")
            )
        )

        # Add RFM scores using window functions
        customer_metrics = (customer_metrics
            .withColumn("recency_score",
                ntile(5).over(Window.orderBy(col("last_transaction_date").desc())))
            .withColumn("frequency_score",
                ntile(5).over(Window.orderBy("total_transactions")))
            .withColumn("monetary_score",
                ntile(5).over(Window.orderBy("total_spent")))
            .withColumn("rfm_score",
                col("recency_score") + col("frequency_score") + col("monetary_score"))
            .withColumn("customer_segment_rfm",
                when(col("rfm_score") >= 13, "Champions")
                .when(col("rfm_score") >= 10, "Loyal")
                .when(col("rfm_score") >= 7, "Potential")
                .when(col("rfm_score") >= 4, "At Risk")
                .otherwise("Lost"))
            .withColumn("etl_load_timestamp", current_timestamp())
        )
        self.dataframes['customer_metrics'] = customer_metrics
        print(f"Customer metrics: {customer_metrics.count()} rows")

    def mask_pii_columns(self):
        """Apply PII masking based on compliance config."""
        print("\n" + "=" * 60)
        print("APPLYING PII MASKING")
        print("=" * 60)

        compliance = self.config.get("compliance", {})
        if not compliance.get("mask_pii") == "Y":
            print("PII masking disabled - skipping")
            return

        masking_rules = compliance.get("masking_rules", {})

        for df_name in ['transformed', 'customer_metrics']:
            if df_name not in self.dataframes:
                continue

            df = self.dataframes[df_name]

            for col_name, rule in masking_rules.items():
                if col_name not in df.columns:
                    continue

                print(f"  Masking {col_name} with {rule}...")

                if rule == "SHA256":
                    df = df.withColumn(col_name, sha2(col(col_name), 256))
                elif rule == "PARTIAL":
                    # Show last 4 chars only
                    df = df.withColumn(col_name,
                        concat(lit("***"), col(col_name).substr(-4, 4)))
                elif rule == "REDACT":
                    df = df.withColumn(col_name, lit("[REDACTED]"))
                # KEEP = no change

            self.dataframes[df_name] = df

    def write_outputs(self, ctx=None):
        """Write all output tables to Glue Catalog."""
        print("\n" + "=" * 60)
        print("WRITING OUTPUT TABLES")
        print("=" * 60)

        # Sales Facts (main output)
        print("Writing sales_facts...")
        sales_facts = self.dataframes['transformed'].select(
            "transaction_id", "customer_id", "product_id", "store_id", "promotion_id",
            "transaction_date", "transaction_status",
            "quantity", "unit_price", "gross_amount", "discount_amount",
            "net_amount", "tax_amount", "total_amount",
            "year", "month", "day", "region",
            "product_category", "customer_segment",
            "etl_load_timestamp"
        )

        if ctx:
            ctx.write_to_catalog(sales_facts, "analytics_db", "sales_facts",
                               mode="overwrite", partition_by=["year", "month", "region"])
        else:
            (sales_facts.write.mode("overwrite")
             .partitionBy("year", "month", "region")
             .saveAsTable("analytics_db.sales_facts"))

        # Daily Summary
        print("Writing sales_daily_summary...")
        if ctx:
            ctx.write_to_catalog(self.dataframes['daily_summary'],
                               "analytics_db", "sales_daily_summary", mode="overwrite")
        else:
            (self.dataframes['daily_summary'].write.mode("overwrite")
             .saveAsTable("analytics_db.sales_daily_summary"))

        # Customer Metrics
        print("Writing customer_sales_metrics...")
        if ctx:
            ctx.write_to_catalog(self.dataframes['customer_metrics'],
                               "analytics_db", "customer_sales_metrics", mode="overwrite")
        else:
            (self.dataframes['customer_metrics'].write.mode("overwrite")
             .saveAsTable("analytics_db.customer_sales_metrics"))

        print("All outputs written successfully!")

    def run(self, ctx=None):
        """Execute the full pipeline."""
        self.read_source_tables()
        self.join_tables()
        self.apply_transformations()
        self.create_aggregations()
        self.mask_pii_columns()

        # Register DataFrames for DQ validation
        if ctx:
            ctx.register_dataframe("sales_facts", self.dataframes['transformed'])
            ctx.register_dataframe("daily_summary", self.dataframes['daily_summary'])
            ctx.register_dataframe("customer_metrics", self.dataframes['customer_metrics'])

        self.write_outputs(ctx)


def run_with_framework():
    """Run pipeline with full framework integration."""
    args = getResolvedOptions(sys.argv,
        ['JOB_NAME', 'CONFIG_PATH', 'START_DATE', 'END_DATE'])

    framework = GlueETLFramework(config_path=args['CONFIG_PATH'])

    with framework.run() as ctx:
        print(f"\n{'='*70}")
        print(f"SALES ANALYTICS PIPELINE: {args['JOB_NAME']}")
        print(f"Date Range: {args['START_DATE']} to {args['END_DATE']}")
        print(f"Run ID: {ctx.run_id}")
        print(f"{'='*70}\n")

        pipeline = SalesAnalyticsPipeline(ctx.spark, framework.config, args)
        pipeline.run(ctx)

    # Print comprehensive summary
    print("\n" + "=" * 70)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 70)

    summary = framework.get_results_summary()
    for agent, result in summary.items():
        print(f"\n{agent.upper()}:")
        print(f"  Status: {result['status']}")
        print(f"  Findings: {result['findings_count']}")
        print(f"  Recommendations: {result['recommendations_count']}")

    # Save audit
    framework.save_audit()


def run_standalone():
    """Run pipeline without framework (fallback)."""
    args = getResolvedOptions(sys.argv,
        ['JOB_NAME', 'START_DATE', 'END_DATE'])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    # Enable optimizations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    print(f"\n{'='*70}")
    print(f"SALES ANALYTICS PIPELINE (Standalone): {args['JOB_NAME']}")
    print(f"{'='*70}\n")

    pipeline = SalesAnalyticsPipeline(spark, {}, args)
    pipeline.run()

    job.commit()


if __name__ == "__main__":
    if FRAMEWORK_AVAILABLE:
        run_with_framework()
    else:
        run_standalone()
