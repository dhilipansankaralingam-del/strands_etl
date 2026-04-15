#!/usr/bin/env python3
"""
Customer Data Transformation ETL Script
========================================

This script demonstrates:
1. Reading customer data from multiple sources
2. Data cleansing and standardization
3. Aggregations and window functions
4. Data quality checks
5. Writing to partitioned output

Usage:
    spark-submit customer_transform.py \
        --customers s3://etl-raw-data/customers/ \
        --transactions s3://etl-raw-data/transactions/ \
        --output s3://etl-curated-data/customer_360/
"""

import sys
import argparse
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, lower, upper, coalesce,
    to_date, year, month, dayofmonth, date_format,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    row_number, dense_rank, lead, lag,
    regexp_replace, concat, concat_ws, split,
    first, last, collect_list, collect_set, size,
    expr, datediff, current_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)


def create_spark_session(app_name: str = "CustomerTransform") -> SparkSession:
    """Create and configure Spark session with optimizations."""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.broadcastTimeout", "600")
            .getOrCreate())


def read_customers(spark: SparkSession, path: str):
    """Read customer master data."""
    print(f"Reading customer data from: {path}")

    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("is_active", StringType(), True)
    ])

    df = spark.read.schema(schema).parquet(path)
    print(f"Loaded {df.count()} customer records")
    return df


def read_transactions(spark: SparkSession, path: str):
    """Read transaction data."""
    print(f"Reading transaction data from: {path}")

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("transaction_date", DateType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("product_category", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("discount_applied", DoubleType(), True)
    ])

    df = spark.read.schema(schema).parquet(path)
    print(f"Loaded {df.count()} transaction records")
    return df


def clean_customer_data(df):
    """Clean and standardize customer data."""
    print("Cleaning customer data...")

    df_clean = (df
        # Trim and standardize names
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

        # Standardize email (lowercase)
        .withColumn("email", lower(trim(col("email"))))

        # Clean phone (remove non-digits)
        .withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))

        # Standardize state (uppercase)
        .withColumn("state", upper(trim(col("state"))))

        # Handle null values
        .withColumn("customer_segment", coalesce(col("customer_segment"), lit("UNKNOWN")))
        .withColumn("is_active", coalesce(col("is_active"), lit("Y")))

        # Convert is_active to boolean-like
        .withColumn("is_active_flag",
                   when(upper(col("is_active")).isin("Y", "YES", "TRUE", "1"), lit(1))
                   .otherwise(lit(0)))

        # Calculate customer tenure
        .withColumn("tenure_days",
                   datediff(current_date(), col("registration_date")))
        .withColumn("tenure_years",
                   (col("tenure_days") / 365).cast("int"))
    )

    return df_clean


def calculate_customer_metrics(customers_df, transactions_df):
    """Calculate customer-level metrics from transactions."""
    print("Calculating customer metrics...")

    # Aggregate transaction metrics per customer
    customer_metrics = (transactions_df
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("transaction_amount").alias("total_spend"),
            avg("transaction_amount").alias("avg_transaction_value"),
            spark_max("transaction_amount").alias("max_transaction"),
            spark_min("transaction_amount").alias("min_transaction"),
            spark_max("transaction_date").alias("last_transaction_date"),
            spark_min("transaction_date").alias("first_transaction_date"),
            collect_set("product_category").alias("product_categories_purchased"),
            collect_set("payment_method").alias("payment_methods_used")
        )
        .withColumn("distinct_categories", size(col("product_categories_purchased")))
        .withColumn("distinct_payment_methods", size(col("payment_methods_used")))
        .withColumn("days_since_last_purchase",
                   datediff(current_date(), col("last_transaction_date")))
        .withColumn("customer_lifetime_days",
                   datediff(col("last_transaction_date"), col("first_transaction_date")))
    )

    return customer_metrics


def calculate_rfm_scores(df):
    """Calculate RFM (Recency, Frequency, Monetary) scores."""
    print("Calculating RFM scores...")

    # Define windows for percentile-based scoring
    recency_window = Window.orderBy(col("days_since_last_purchase").desc())
    frequency_window = Window.orderBy("total_transactions")
    monetary_window = Window.orderBy("total_spend")

    df_rfm = (df
        # Calculate percentile ranks (1-5 scale)
        .withColumn("recency_rank", dense_rank().over(recency_window))
        .withColumn("frequency_rank", dense_rank().over(frequency_window))
        .withColumn("monetary_rank", dense_rank().over(monetary_window))

        # Convert to 1-5 scores using ntile approach
        .withColumn("recency_score",
                   when(col("days_since_last_purchase") <= 30, 5)
                   .when(col("days_since_last_purchase") <= 60, 4)
                   .when(col("days_since_last_purchase") <= 90, 3)
                   .when(col("days_since_last_purchase") <= 180, 2)
                   .otherwise(1))
        .withColumn("frequency_score",
                   when(col("total_transactions") >= 20, 5)
                   .when(col("total_transactions") >= 10, 4)
                   .when(col("total_transactions") >= 5, 3)
                   .when(col("total_transactions") >= 2, 2)
                   .otherwise(1))
        .withColumn("monetary_score",
                   when(col("total_spend") >= 10000, 5)
                   .when(col("total_spend") >= 5000, 4)
                   .when(col("total_spend") >= 1000, 3)
                   .when(col("total_spend") >= 500, 2)
                   .otherwise(1))

        # Calculate combined RFM score and segment
        .withColumn("rfm_score",
                   col("recency_score") + col("frequency_score") + col("monetary_score"))
        .withColumn("rfm_segment",
                   when(col("rfm_score") >= 13, "Champions")
                   .when(col("rfm_score") >= 10, "Loyal Customers")
                   .when(col("rfm_score") >= 7, "Potential Loyalists")
                   .when(col("rfm_score") >= 4, "At Risk")
                   .otherwise("Need Attention"))
    )

    return df_rfm


def create_customer_360(customers_df, metrics_df):
    """Create unified customer 360 view."""
    print("Creating Customer 360 view...")

    customer_360 = (customers_df
        .join(metrics_df, "customer_id", "left")
        .withColumn("total_transactions", coalesce(col("total_transactions"), lit(0)))
        .withColumn("total_spend", coalesce(col("total_spend"), lit(0.0)))
        .withColumn("avg_transaction_value", coalesce(col("avg_transaction_value"), lit(0.0)))

        # Add customer value tier
        .withColumn("customer_value_tier",
                   when(col("total_spend") >= 10000, "Platinum")
                   .when(col("total_spend") >= 5000, "Gold")
                   .when(col("total_spend") >= 1000, "Silver")
                   .otherwise("Bronze"))

        # Add churn risk indicator
        .withColumn("churn_risk",
                   when(col("days_since_last_purchase") > 180, "High")
                   .when(col("days_since_last_purchase") > 90, "Medium")
                   .otherwise("Low"))

        # Add ETL metadata
        .withColumn("etl_load_timestamp", current_timestamp())
        .withColumn("etl_source", lit("customer_transform"))

        # Add partition columns
        .withColumn("process_year", year(current_date()))
        .withColumn("process_month", month(current_date()))
    )

    return customer_360


def run_data_quality_checks(df, df_name: str):
    """Run data quality checks and print results."""
    print(f"\nData Quality Checks for {df_name}:")
    print("-" * 40)

    total_records = df.count()
    print(f"Total Records: {total_records}")

    # Check for null customer_ids
    null_ids = df.filter(col("customer_id").isNull()).count()
    print(f"Null Customer IDs: {null_ids} ({100*null_ids/total_records:.2f}%)")

    # Check for duplicates
    distinct_ids = df.select("customer_id").distinct().count()
    duplicates = total_records - distinct_ids
    print(f"Duplicate Customer IDs: {duplicates}")

    # Data quality pass/fail
    if null_ids == 0 and duplicates == 0:
        print("Data Quality Check: PASSED")
        return True
    else:
        print("Data Quality Check: FAILED")
        return False


def write_output(df, output_path: str):
    """Write customer 360 data to S3."""
    print(f"\nWriting output to: {output_path}")

    (df.write
     .mode("overwrite")
     .partitionBy("process_year", "process_month")
     .parquet(output_path))

    print("Write complete")


def main():
    parser = argparse.ArgumentParser(description="Customer Data Transformation ETL")
    parser.add_argument("--customers", required=True, help="Customer data S3 path")
    parser.add_argument("--transactions", required=True, help="Transaction data S3 path")
    parser.add_argument("--output", required=True, help="Output S3 path")

    args = parser.parse_args()

    print("=" * 60)
    print("Customer Transformation ETL Job")
    print("=" * 60)
    print(f"Customers: {args.customers}")
    print(f"Transactions: {args.transactions}")
    print(f"Output: {args.output}")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Read source data
        customers_df = read_customers(spark, args.customers)
        transactions_df = read_transactions(spark, args.transactions)

        # Clean customer data
        customers_clean = clean_customer_data(customers_df)

        # Calculate customer metrics
        customer_metrics = calculate_customer_metrics(customers_clean, transactions_df)

        # Add RFM scores
        customer_metrics_rfm = calculate_rfm_scores(customer_metrics)

        # Create Customer 360 view
        customer_360 = create_customer_360(customers_clean, customer_metrics_rfm)

        # Run data quality checks
        dq_passed = run_data_quality_checks(customer_360, "Customer 360")

        if not dq_passed:
            print("WARNING: Data quality checks failed, but continuing...")

        # Write output
        write_output(customer_360, args.output)

        # Print summary
        print("\n" + "=" * 60)
        print("Job Summary")
        print("=" * 60)
        print(f"Input customers: {customers_df.count()}")
        print(f"Input transactions: {transactions_df.count()}")
        print(f"Output records: {customer_360.count()}")
        print(f"Finished: {datetime.now().isoformat()}")
        print("=" * 60)

    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
