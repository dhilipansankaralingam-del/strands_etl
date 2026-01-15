import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define databases
source_db = "ecommerce_db"
target_db = "analytics_db"

# Step 1: Read source tables from Glue Catalog
print("Reading source tables from Glue Catalog...")

# Read customers table
customers_df = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="customers",
    transformation_ctx="customers_df"
).toDF()

# Read orders table
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="orders",
    transformation_ctx="orders_df"
).toDF()

# Read products table
products_df = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name="products",
    transformation_ctx="products_df"
).toDF()

print(f"Customers: {customers_df.count()} records")
print(f"Orders: {orders_df.count()} records")
print(f"Products: {products_df.count()} records")

# Step 2: Apply filters as per config
print("Applying data filters...")
customers_df.printSchema()
customers_filtered = customers_df.filter(col("active.boolean") == True)

orders_filtered = orders_df.filter(
    (col("status") == "completed") &
    (col("order_date") >= "2023-01-01")
)

products_filtered = products_df.filter(col("active.boolean") == True)

# Step 3: Perform joins
print("Performing table joins...")

# Join customers with orders
customer_orders = customers_filtered.join(
    orders_filtered,
    customers_filtered.customer_id == orders_filtered.customer_id,
    "inner"
).select(
    customers_filtered.customer_id,
    customers_filtered.name.alias("customer_name"),
    customers_filtered.email,
    customers_filtered.region,
    customers_filtered.signup_date,
    orders_filtered.order_id,
    orders_filtered.product_id,
    orders_filtered.order_date,
    orders_filtered.quantity,
    orders_filtered.total_amount,
    orders_filtered.status
)

# Join with products
full_orders = customer_orders.join(
    products_filtered,
    customer_orders.product_id == products_filtered.product_id,
    "left"
).select(
    customer_orders.customer_id,
    customer_orders.customer_name,
    customer_orders.region,
    customer_orders.order_id,
    customer_orders.product_id,
    customer_orders.order_date,
    customer_orders.quantity,
    customer_orders.total_amount,
    products_filtered.product_name,
    products_filtered.category,
    products_filtered.price,
    products_filtered.cost
)

# Step 4: Calculate derived columns
print("Calculating derived columns...")
full_orders.printSchema()

# Calculate profit margin
full_orders = full_orders.withColumn(
    "profit_margin_pct",
    ((col("price.decimal") - col("cost.decimal")) / col("price.decimal")) * 100
)

# Step 5: Perform aggregations
print("Performing customer-level aggregations...")

customer_summary = full_orders.groupBy(
    col("customer_id.int").alias("customer_id"),
    "customer_name",
    "region"
).agg(
    count("order_id.int").alias("total_orders"),
    sum("quantity.int").alias("total_quantity"),
    sum("total_amount.decimal").alias("total_revenue"),
    avg("total_amount.decimal").alias("avg_order_value"),
    max("order_date").alias("last_order_date"),
    avg("profit_margin_pct").alias("avg_profit_margin")
)

# Step 6: Calculate customer lifetime value
customer_summary = customer_summary.withColumn(
    "customer_lifetime_value",
    col("total_revenue") * (1 + col("avg_profit_margin") / 100)
)

# Step 7: Find top category per customer
print("Finding top category per customer...")

# First, get category counts per customer
category_counts = full_orders.groupBy(
    col("customer_id.int").alias("customer_id"), "category"
).agg(
    count("*").alias("category_count")
)

# Use window function to rank categories
window_spec = Window.partitionBy("customer_id").orderBy(desc("category_count"))

category_ranked = category_counts.withColumn(
    "rank",
    row_number().over(window_spec)
)

# Get top category for each customer
top_categories = category_ranked.filter(col("rank") == 1).select(
    "customer_id",
    "category"
)

# Join back to customer summary
customer_summary = customer_summary.join(
    top_categories,
    "customer_id",
    "left"
).withColumnRenamed("category", "top_category")

# Step 8: Add processing timestamp
customer_summary = customer_summary.withColumn(
    "processed_at",
    current_timestamp()
)

# Step 9: Write to target table
print("Writing results to target table...")

# Convert back to DynamicFrame for Glue catalog operations
output_dynamic_frame = DynamicFrame.fromDF(
    customer_summary,
    glueContext,
    "output_dynamic_frame"
)

S3_OUTPUT_PATH = "s3://strands-etl-data/ecommerce/target1/"
OUTPUT_FORMAT = "parquet"
# Write to Glue catalog table
# glueContext.write_dynamic_frame.from_catalog(
#     frame=output_dynamic_frame,
#     database=target_db,
#     table_name="customer_order_summary",
#     transformation_ctx="output_dynamic_frame"
# )

glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="s3",  # Tells Glue the sink is S3
    connection_options={
        "path": S3_OUTPUT_PATH,
        
        # --- KEY SETTINGS TO CREATE THE TABLE ---
        "enableUpdateCatalog": True,  # This flag enables catalog interaction
        "database": target_db,
        "table": "customer_order_summary"
    },
    format=OUTPUT_FORMAT, # Specifies the file format
    transformation_ctx="output_dynamic_frame"
)

print("ETL job completed successfully!")
print(f"Processed {customer_summary.count()} customer records")

job.commit()