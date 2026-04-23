#!/usr/bin/env python3
"""
Sample Sales Analytics ETL Job
This script has intentional anti-patterns for testing code analysis.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Initialize Spark
spark = SparkSession.builder.appName("SalesAnalytics").getOrCreate()

# Read source tables
orders_df = spark.read.parquet("s3://bucket/raw/orders/")
customers_df = spark.read.parquet("s3://bucket/raw/customers/")
products_df = spark.read.parquet("s3://bucket/raw/products/")

# ANTI-PATTERN: UDF without type hint (slow)
@udf
def clean_string(s):
    if s:
        return s.strip().upper()
    return None

# Apply UDF
orders_df = orders_df.withColumn("status_clean", clean_string(col("status")))

# Join orders with customers
# ANTI-PATTERN: No broadcast hint for small table
order_customer_df = orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "left"
)

# Join with products
order_full_df = order_customer_df.join(
    products_df,
    order_customer_df.product_id == products_df.product_id,
    "left"
)

# ANTI-PATTERN: repartition without partition key
order_full_df = order_full_df.repartition(200)

# Calculate daily aggregates
daily_summary = order_full_df.groupBy("order_date", "region").agg(
    count("order_id").alias("order_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    count(when(col("status") == "COMPLETED", 1)).alias("completed_orders")
)

# ANTI-PATTERN: collect() - loads all data to driver
all_regions = order_full_df.select("region").distinct().collect()
print(f"Found {len(all_regions)} regions")

# ANTI-PATTERN: Loop over collected data
for region_row in all_regions:
    region = region_row["region"]
    region_total = order_full_df.filter(col("region") == region).agg(sum("amount")).collect()[0][0]
    print(f"Region {region}: ${region_total}")

# ANTI-PATTERN: toPandas() for large dataset
# summary_pd = daily_summary.toPandas()

# GOOD: Using coalesce before write
daily_summary = daily_summary.coalesce(10)

# Write output
daily_summary.write.mode("overwrite").partitionBy("order_date").parquet(
    "s3://bucket/curated/daily_sales_summary/"
)

# GOOD: Cache intermediate result for reuse
order_full_df.cache()

# Additional aggregations using cached data
monthly_summary = order_full_df.groupBy("year_month", "region").agg(
    sum("amount").alias("monthly_total")
)

monthly_summary.write.mode("overwrite").parquet(
    "s3://bucket/curated/monthly_sales_summary/"
)

spark.stop()
