import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_DB = "ecommerce_db"
TARGET_DB = "analytics_db"

print("=== script_master.py : orders enrichment pipeline ===")

# ── Read source tables ────────────────────────────────────────────────────────

orders_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB, table_name="orders"
).toDF()

order_items_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB, table_name="order_items"
).toDF()

customers_df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB, table_name="customers"
).toDF()

# ── Anti-pattern 1: collect() on large table ─────────────────────────────────
# Pulls entire orders table to driver to build a Python dict — OOM on large datasets
print("Building order lookup dict...")
order_lookup = orders_df.collect()                           # ANTI-PATTERN: collect()
order_dict   = {row["order_id"]: row["status"] for row in order_lookup}
print(f"Loaded {len(order_dict)} orders into driver memory")

# ── Anti-pattern 2: UDF instead of native Spark functions ───────────────────
from pyspark.sql.functions import udf

@udf("string")
def categorize_order_value(amount):
    if amount is None:
        return "unknown"
    if amount < 50:
        return "low"
    elif amount < 200:
        return "medium"
    else:
        return "high"

orders_df = orders_df.withColumn(
    "value_category",
    categorize_order_value(F.col("total_amount"))             # ANTI-PATTERN: Python UDF
)

# ── Anti-pattern 3: no broadcast hint on small table ─────────────────────────
# customers is ~4 GB — should be broadcast, but no hint given → shuffle join
enriched_df = orders_df.join(
    customers_df,                                             # ANTI-PATTERN: missing broadcast()
    on="customer_id",
    how="left"
)

# ── Anti-pattern 4: explode without repartition causes skew ──────────────────
# order_items has 5.4B rows; joining on order_id causes severe partition skew
# because popular orders (electronics, flash sales) have 1000s of line items
items_enriched = enriched_df.join(
    order_items_df,
    on="order_id",
    how="left"
)

# ── Anti-pattern 5: toPandas() on a wide intermediate result ─────────────────
print("Computing summary statistics...")
summary_stats = items_enriched \
    .groupBy("value_category", "region") \
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
    )

summary_pd = summary_stats.toPandas()                        # ANTI-PATTERN: toPandas()
print(summary_pd.describe())

# ── Anti-pattern 6: iterative collect inside a loop ──────────────────────────
regions = [row["region"] for row in enriched_df.select("region").distinct().collect()]
regional_results = []
for region in regions:
    region_df  = items_enriched.filter(F.col("region") == region)
    region_agg = region_df.agg(
        F.sum("total_amount").alias("revenue"),
        F.count("*").alias("orders"),
    ).collect()                                               # ANTI-PATTERN: collect() in loop
    regional_results.append({"region": region, **region_agg[0].asDict()})

print(f"Processed {len(regions)} regions")

# ── Window function without partitionBy (full shuffle) ───────────────────────
window_all = Window.orderBy("order_date")
orders_df = orders_df.withColumn(
    "running_total",
    F.sum("total_amount").over(window_all)                   # ANTI-PATTERN: window without partitionBy
)

# ── Write output ──────────────────────────────────────────────────────────────
print("Writing enriched output...")
items_enriched.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .save("s3://analytics-lake/enriched/order_master/")

job.commit()
print("=== script_master.py complete ===")
