###################################################################################################
# SCRIPT        : trigger_glue.py
# PURPOSE       : Config-driven Glue job that reads CSV files from S3 landing,
#                 loads them into Iceberg tables, and archives processed files.
#
# KEY BEHAVIOUR :
#   - When --stale_folders is provided (comma-separated S3 prefixes), ONLY
#     files under those subfolders are read and loaded.
#   - When --stale_folders is empty/missing, falls back to processing the
#     full landing_loc from the JSON config (original behaviour).
#   - After a successful load, processed files are copied to the archive
#     path (--archive_s3_path or config's archive_s3_path) and then deleted
#     from the landing location.
#
# ARGUMENTS     :
#   --JOB_NAME          (Glue built-in)
#   --json_file_name    S3 key of the JSON config file
#   --bucket_name       S3 bucket where the JSON config resides
#   --stale_folders     Comma-separated S3 prefixes of stale subfolders
#   --archive_s3_path   Override archive destination (optional)
#   --orchestrator_run_id  Run ID from the orchestrator (optional, for tracing)
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 10/2025       Dhilipan        Initial version
# 02/2026       Dhilipan        Selective stale-folder processing & archiving
#
#=================================================================================================
###################################################################################################

import sys
import json
import logging
from datetime import datetime

import pytz
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.session import SparkSession

# ---------------------------------------------------------------------------
# Initialise Glue / Spark
# ---------------------------------------------------------------------------
logger = logging.getLogger()

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "json_file_name", "bucket_name", "stale_folders",
     "archive_s3_path", "orchestrator_run_id", "source_bucket"],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

job_run_id = args["JOB_RUN_ID"]
job_name = args["JOB_NAME"]
job_start_time = str(datetime.now())

spark = (
    SparkSession.builder
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .config("spark.sql.catalog.glue_catalog.warehouse",
            "ace-da-mem-qlik-landing-us-west-2-847515726144")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")


def json_to_structtype(dtype):
    """Convert a {col: type_str} dict to a PySpark StructType."""
    type_mapping = {
        "str": StringType(),
        "int": IntegerType(),
    }
    fields = []
    for col_name, col_type in dtype.items():
        if col_type in type_mapping:
            fields.append(StructField(col_name, type_mapping[col_type], True))
        else:
            raise ValueError(f"Unsupported data type: {col_type}")
    return StructType(fields)


def list_csv_files(bucket, prefix):
    """Return a list of s3:// paths for all .csv files under *prefix*."""
    paginator = s3_client.get_paginator("list_objects_v2")
    csv_paths = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                csv_paths.append(key)
    return csv_paths


def archive_files(bucket, keys, archive_prefix):
    """
    Copy each key to the archive prefix (preserving the subfolder structure
    relative to the landing root) and delete the original.
    """
    for key in keys:
        # Build the archive key: archive_prefix + filename-part after last '/'
        # We keep the subfolder structure by appending everything after the
        # common landing root prefix.
        archive_key = archive_prefix.rstrip("/") + "/" + key.split("/", 1)[-1]
        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=archive_key,
            )
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"Archived: s3://{bucket}/{key} -> s3://{bucket}/{archive_key}")
        except Exception as exc:
            print(f"ERROR archiving {key}: {exc}")
            raise


def delete_subfolder(bucket, prefix):
    """
    Delete all remaining objects under a subfolder prefix, effectively
    removing the subfolder from S3.  S3 has no real "directory" — once
    all objects (including 0-byte folder markers) are gone, the folder
    disappears.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    objects_to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

    if not objects_to_delete:
        print(f"  Subfolder already empty: s3://{bucket}/{prefix}")
        return

    # S3 delete_objects supports up to 1000 keys per call
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i : i + 1000]
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    print(f"  Deleted subfolder ({len(objects_to_delete)} objects): s3://{bucket}/{prefix}")


# ---------------------------------------------------------------------------
# Read job config from S3
# ---------------------------------------------------------------------------
bucket_name = args["bucket_name"]
json_file_name = args["json_file_name"]

response = s3_client.get_object(Bucket=bucket_name, Key=json_file_name)
file_content = response["Body"].read().decode("utf-8")
jobs = json.loads(file_content)

# Stale-folder list passed by the orchestrator (may be empty)
stale_folders_arg = args.get("stale_folders", "")
stale_folders = [f.strip() for f in stale_folders_arg.split(",") if f.strip()]

# Archive path override from the orchestrator (falls back to config value)
archive_override = args.get("archive_s3_path", "")

# Source data bucket (from the orchestrator config, NOT the config-file bucket).
# Used to delete stale subfolders after processing.
source_bucket = args.get("source_bucket", "")

print("=" * 60)
print(f"Job: {job_name}  Run: {job_run_id}")
print(f"Source bucket : {source_bucket if source_bucket else '(will use job config s3_bucket)'}")
print(f"Stale folders received: {stale_folders if stale_folders else '(all – full landing)')}")
print(f"Archive override: {archive_override if archive_override else '(use config value)'}")
print("=" * 60)

# ---------------------------------------------------------------------------
# Process each job entry in the config
# ---------------------------------------------------------------------------
for job_cfg in jobs:
    print("*" * 50)
    bucket_name1 = job_cfg["s3_bucket"]
    landing_loc = job_cfg["landing_loc"]
    file_pattern_name = job_cfg["file_pattern_name"]
    database_name = job_cfg["database_name"]
    table_name = job_cfg["landing_table"]
    dtype = job_cfg["dtype"]
    archive_path = archive_override or job_cfg.get("archive_s3_path", "")
    temp_path = job_cfg.get("temp_s3_path", "")

    print(f"Source bucket : {bucket_name1}")
    print(f"Landing loc   : {landing_loc}")
    print(f"Pattern       : {file_pattern_name}")
    print(f"Target table  : {database_name}.{table_name}")
    print(f"Archive path  : {archive_path}")

    schema = json_to_structtype(dtype)
    print(f"Schema        : {schema}")

    # ------------------------------------------------------------------
    # Collect CSV files – only from stale subfolders when provided
    # ------------------------------------------------------------------
    csv_keys = []

    if stale_folders:
        # Only process CSV files that live under one of the stale subfolders
        for sf_prefix in stale_folders:
            # Ensure we only include subfolders that belong to this landing_loc
            if not sf_prefix.startswith(landing_loc):
                continue
            sf_csvs = list_csv_files(bucket_name1, sf_prefix)
            print(f"  Stale subfolder {sf_prefix}: {len(sf_csvs)} CSV file(s)")
            csv_keys.extend(sf_csvs)
    else:
        # Fallback: process everything under landing_loc (original behaviour)
        csv_keys = list_csv_files(bucket_name1, landing_loc)
        print(f"  Full landing scan: {len(csv_keys)} CSV file(s)")

    if not csv_keys:
        print(f"  No CSV files to process for {table_name} – skipping load")
        # Even with no CSVs, we still need to clean up stale subfolders below
        loaded_successfully = False
    else:
        csv_paths = [f"s3://{bucket_name1}/{k}" for k in csv_keys]
        for p in csv_paths:
            print(f"  -> {p}")

        # ------------------------------------------------------------------
        # Read, enrich, and load
        # ------------------------------------------------------------------
        # Read CSV with header=true but WITHOUT applying the schema yet.
        # CSV files may have UPPERCASE column names while the config schema
        # uses lowercase — Spark matches these case-sensitively which causes
        # "Field ... not found in source schema" errors.
        df = spark.read.option("header", "true").csv(csv_paths)

        # Lowercase all column names so they match the schema definition
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())

        # Now cast to the target schema types
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))

        # Add processing indicator columns
        df = df.withColumn("hourly_processed_ind", lit("N"))
        df = df.withColumn("daily_processed_ind", lit("N"))

        # Timestamps
        pst = pytz.timezone("US/Pacific")
        current_timestamp = datetime.now(pst).strftime("%Y-%m-%d %H:%M:%S")
        current_timestamp_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        df = df.withColumn("insert_timestamp", lit(current_timestamp))
        df = df.withColumn("inserted_by", lit("Qlik Staging Lambda"))
        df = df.withColumn("updated_timestamp", lit(current_timestamp))
        df = df.withColumn("updated_by", lit("Qlik Staging Lambda"))
        df = df.withColumn("load_timestamp", lit(current_timestamp_utc))

        df.cache()
        df.createOrReplaceTempView("mem_staging")

        row_count = df.count()
        print(f"  Rows to load: {row_count}")
        df.show(5, truncate=False)

        # Insert into Iceberg table
        query = f"INSERT INTO glue_catalog.{database_name}.{table_name} SELECT * FROM mem_staging"
        print(f"  Executing: {query}")
        spark.sql(query)
        print(f"  Loaded {row_count} rows into {database_name}.{table_name}")

        # Archive the processed CSV files
        if archive_path:
            print(f"  Archiving {len(csv_keys)} file(s) to {archive_path}")
            archive_files(bucket_name1, csv_keys, archive_path)
            print(f"  Archive complete for {table_name}")
        else:
            print(f"  WARNING: No archive path configured – files left in landing")

        # Clean up cached dataframe
        df.unpersist()
        loaded_successfully = True

print("=" * 60)

# ===========================================================================
# Delete stale subfolders from the source landing bucket.
#
# This runs AFTER all job configs have been processed so that:
#   1) It is independent of any landing_loc prefix matching
#   2) It uses source_bucket (from the orchestrator) — the actual bucket
#      where the stale subfolders live
#   3) It always executes, even if no CSVs were found inside a subfolder
#
# The orchestrator already filtered the stale_folders list to belong to
# this source, so no additional filtering is needed here.
# ===========================================================================
if stale_folders:
    delete_bucket = source_bucket or bucket_name
    print(f"Deleting {len(stale_folders)} stale subfolder(s) from s3://{delete_bucket}/")
    for sf_prefix in stale_folders:
        print(f"  Removing: s3://{delete_bucket}/{sf_prefix}")
        delete_subfolder(delete_bucket, sf_prefix)
    print(f"Cleanup complete – {len(stale_folders)} subfolder(s) removed")
else:
    print("No stale subfolders to delete")

print("=" * 60)
print("trigger_glue.py completed successfully")
print("=" * 60)

job.commit()
