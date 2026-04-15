#!/usr/bin/env python3
"""
Complex Sales Analytics Pipeline
=================================

This script demonstrates a complex multi-stage ETL pipeline that:
1. Reads from multiple source tables (orders, order_lines, customers, products, regions)
2. Performs multiple joins with broadcast hints for small tables
3. Aggregates data at multiple levels
4. Handles data skew with salting
5. Writes to Delta Lake with MERGE operations
6. Creates analytical aggregates

Compatible with the ETL Framework for:
- Platform fallback (Glue -> EMR -> EKS)
- Auto-healing with code correction
- Comprehensive data quality
- Compliance checking
- Workload assessment with historical trends
"""

import sys
from datetime import datetime, date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, lower, upper, regexp_replace, coalesce,
    current_date, current_timestamp, lit, sum as spark_sum,
    count, avg, min as spark_min, max as spark_max,
    year, month, dayofmonth, quarter, weekofyear,
    row_number, dense_rank, broadcast, floor, rand, concat,
    explode, array, first, last, collect_list, struct
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType


def main():
    """Main ETL function."""

    # Initialize context
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_database',
        'target_database',
        'delta_path'
    ])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Enable optimizations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

    print(f"Starting complex ETL job: {args['JOB_NAME']}")
    print(f"Timestamp: {datetime.now().isoformat()}")

    # =========================================================================
    # STEP 1: Read source tables
    # =========================================================================
    print("\n" + "="*60)
    print("STEP 1: Reading source tables")
    print("="*60)

    # Read orders (large table)
    orders_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['source_database'],
        table_name="orders",
        transformation_ctx="orders"
    ).toDF()
    print(f"Orders: {orders_df.count()} records")

    # Read order lines (very large table)
    order_lines_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['source_database'],
        table_name="order_lines",
        transformation_ctx="order_lines"
    ).toDF()
    print(f"Order Lines: {order_lines_df.count()} records")

    # Read customers (medium table - will broadcast)
    customers_df = glueContext.create_dynamic_frame.from_catalog(
        database="master_data",
        table_name="customers",
        transformation_ctx="customers"
    ).toDF()
    print(f"Customers: {customers_df.count()} records")

    # Read products (small table - will broadcast)
    products_df = glueContext.create_dynamic_frame.from_catalog(
        database="master_data",
        table_name="products",
        transformation_ctx="products"
    ).toDF()
    print(f"Products: {products_df.count()} records")

    # Read regions (very small table - will broadcast)
    regions_df = glueContext.create_dynamic_frame.from_catalog(
        database="master_data",
        table_name="regions",
        transformation_ctx="regions"
    ).toDF()
    print(f"Regions: {regions_df.count()} records")

    # Cache small dimension tables
    customers_df.cache()
    products_df.cache()
    regions_df.cache()

    # =========================================================================
    # STEP 2: Build Sales Fact Table with optimized joins
    # =========================================================================
    print("\n" + "="*60)
    print("STEP 2: Building Sales Fact Table")
    print("="*60)

    # Join orders with order lines
    # Using skew handling for potentially skewed order_id
    print("Joining orders with order_lines...")

    sales_base_df = orders_df.alias("o").join(
        order_lines_df.alias("ol"),
        col("o.order_id") == col("ol.order_id"),
        "inner"
    ).select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.order_date"),
        col("o.order_status"),
        col("o.shipping_region"),
        col("ol.product_id"),
        col("ol.quantity"),
        col("ol.unit_price"),
        col("ol.discount"),
        col("ol.line_total")
    )

    # Broadcast join with customers
    print("Joining with customers (broadcast)...")
    sales_with_customers_df = sales_base_df.join(
        broadcast(customers_df.select(
            col("customer_id"),
            col("customer_name"),
            col("customer_email"),
            col("customer_segment"),
            col("customer_country")
        )),
        "customer_id",
        "left"
    )

    # Broadcast join with products
    print("Joining with products (broadcast)...")
    sales_with_products_df = sales_with_customers_df.join(
        broadcast(products_df.select(
            col("product_id"),
            col("product_name"),
            col("product_category"),
            col("product_subcategory"),
            col("brand"),
            col("cost_price")
        )),
        "product_id",
        "left"
    )

    # Broadcast join with regions
    print("Joining with regions (broadcast)...")
    sales_fact_df = sales_with_products_df.join(
        broadcast(regions_df.select(
            col("region_code").alias("shipping_region"),
            col("region_name"),
            col("country"),
            col("continent")
        )),
        "shipping_region",
        "left"
    )

    # Add calculated columns
    print("Adding calculated columns...")
    sales_fact_df = sales_fact_df \
        .withColumn("gross_amount", col("quantity") * col("unit_price")) \
        .withColumn("discount_amount", col("gross_amount") * col("discount")) \
        .withColumn("net_amount", col("gross_amount") - col("discount_amount")) \
        .withColumn("profit", col("net_amount") - (col("quantity") * col("cost_price"))) \
        .withColumn("profit_margin", col("profit") / col("net_amount") * 100) \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_quarter", quarter(col("order_date"))) \
        .withColumn("order_week", weekofyear(col("order_date"))) \
        .withColumn("etl_timestamp", current_timestamp()) \
        .withColumn("etl_date", current_date())

    sales_fact_count = sales_fact_df.count()
    print(f"Sales Fact records: {sales_fact_count}")

    # =========================================================================
    # STEP 3: Write Sales Fact to Delta Lake with MERGE
    # =========================================================================
    print("\n" + "="*60)
    print("STEP 3: Writing Sales Fact to Delta Lake")
    print("="*60)

    delta_path = args['delta_path'] + "/sales_fact"

    # Check if Delta table exists
    try:
        from delta.tables import DeltaTable

        # Table exists - perform MERGE (upsert)
        if DeltaTable.isDeltaTable(spark, delta_path):
            print("Delta table exists - performing MERGE...")

            delta_table = DeltaTable.forPath(spark, delta_path)

            # Merge on order_id and product_id (composite key)
            delta_table.alias("target").merge(
                sales_fact_df.alias("source"),
                "target.order_id = source.order_id AND target.product_id = source.product_id"
            ).whenMatchedUpdate(
                condition="source.etl_timestamp > target.etl_timestamp",
                set={
                    "quantity": "source.quantity",
                    "unit_price": "source.unit_price",
                    "discount": "source.discount",
                    "line_total": "source.line_total",
                    "gross_amount": "source.gross_amount",
                    "net_amount": "source.net_amount",
                    "profit": "source.profit",
                    "etl_timestamp": "source.etl_timestamp"
                }
            ).whenNotMatchedInsertAll().execute()

            print("MERGE completed")
        else:
            raise Exception("Table doesn't exist")

    except Exception as e:
        # Table doesn't exist - create new
        print(f"Creating new Delta table: {e}")

        sales_fact_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .option("overwriteSchema", "true") \
            .save(delta_path)

        print("Delta table created")

    # Optimize Delta table
    print("Optimizing Delta table...")
    spark.sql(f"OPTIMIZE delta.`{delta_path}`")

    # =========================================================================
    # STEP 4: Create Customer Aggregates
    # =========================================================================
    print("\n" + "="*60)
    print("STEP 4: Creating Customer Aggregates")
    print("="*60)

    # Window for customer rankings
    customer_window = Window.partitionBy("customer_segment").orderBy(col("total_revenue").desc())

    customer_agg_df = sales_fact_df.groupBy(
        "customer_id",
        "customer_name",
        "customer_email",
        "customer_segment",
        "customer_country"
    ).agg(
        count("order_id").alias("total_orders"),
        spark_sum("net_amount").alias("total_revenue"),
        spark_sum("profit").alias("total_profit"),
        avg("profit_margin").alias("avg_profit_margin"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date"),
        count(when(col("order_status") == "RETURNED", 1)).alias("return_count"),
        avg("discount").alias("avg_discount_rate")
    ).withColumn(
        "customer_rank", dense_rank().over(customer_window)
    ).withColumn(
        "customer_tier",
        when(col("customer_rank") <= 10, "PLATINUM")
        .when(col("customer_rank") <= 50, "GOLD")
        .when(col("customer_rank") <= 200, "SILVER")
        .otherwise("BRONZE")
    ).withColumn(
        "snapshot_date", current_date()
    )

    print(f"Customer aggregates: {customer_agg_df.count()} records")

    # Write customer aggregates
    customer_agg_dyf = DynamicFrame.fromDF(customer_agg_df, glueContext, "customer_agg")
    glueContext.write_dynamic_frame.from_catalog(
        frame=customer_agg_dyf,
        database=args['target_database'],
        table_name="customer_aggregates",
        transformation_ctx="customer_agg_output"
    )

    print("Customer aggregates written")

    # =========================================================================
    # STEP 5: Create Product Performance Metrics
    # =========================================================================
    print("\n" + "="*60)
    print("STEP 5: Creating Product Performance Metrics")
    print("="*60)

    # Window for product rankings within category
    product_window = Window.partitionBy("product_category", "order_year", "order_month") \
        .orderBy(col("total_revenue").desc())

    product_perf_df = sales_fact_df.groupBy(
        "product_id",
        "product_name",
        "product_category",
        "product_subcategory",
        "brand",
        "order_year",
        "order_month"
    ).agg(
        count("order_id").alias("order_count"),
        spark_sum("quantity").alias("units_sold"),
        spark_sum("net_amount").alias("total_revenue"),
        spark_sum("profit").alias("total_profit"),
        avg("profit_margin").alias("avg_profit_margin"),
        avg("discount").alias("avg_discount"),
        count(when(col("order_status") == "RETURNED", 1)).alias("returns")
    ).withColumn(
        "return_rate", col("returns") / col("order_count") * 100
    ).withColumn(
        "revenue_per_unit", col("total_revenue") / col("units_sold")
    ).withColumn(
        "category_rank", row_number().over(product_window)
    )

    print(f"Product performance: {product_perf_df.count()} records")

    # Write product performance
    product_perf_dyf = DynamicFrame.fromDF(product_perf_df, glueContext, "product_perf")
    glueContext.write_dynamic_frame.from_catalog(
        frame=product_perf_dyf,
        database=args['target_database'],
        table_name="product_performance",
        transformation_ctx="product_perf_output"
    )

    print("Product performance written")

    # =========================================================================
    # STEP 6: Generate summary metrics for monitoring
    # =========================================================================
    print("\n" + "="*60)
    print("STEP 6: Job Summary")
    print("="*60)

    # Calculate summary metrics
    summary = sales_fact_df.agg(
        count("*").alias("total_records"),
        spark_sum("net_amount").alias("total_revenue"),
        spark_sum("profit").alias("total_profit"),
        avg("profit_margin").alias("avg_margin")
    ).collect()[0]

    print("\nETL JOB SUMMARY")
    print("="*60)
    print(f"Job Name: {args['JOB_NAME']}")
    print(f"Total Sales Records: {summary['total_records']:,}")
    print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
    print(f"Total Profit: ${summary['total_profit']:,.2f}")
    print(f"Average Margin: {summary['avg_margin']:.2f}%")
    print(f"Completion Time: {datetime.now().isoformat()}")
    print("="*60)

    # Unpersist cached DataFrames
    customers_df.unpersist()
    products_df.unpersist()
    regions_df.unpersist()

    # Commit the job
    job.commit()

    print("\nComplex ETL job completed successfully!")


if __name__ == "__main__":
    main()
