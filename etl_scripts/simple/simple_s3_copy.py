#!/usr/bin/env python3
"""
Simple S3 to S3 Copy ETL Script
================================

This is a basic ETL script that:
1. Reads data from S3 source
2. Applies basic transformations (filtering, column selection)
3. Writes to S3 destination

Usage:
    spark-submit simple_s3_copy.py \
        --source s3://etl-raw-data/customers/ \
        --destination s3://etl-curated-data/customers/ \
        --format parquet

For Glue:
    Referenced in job parameters as --script-location
"""

import sys
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, lower, upper,
    to_date, year, month, dayofmonth
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def create_spark_session(app_name: str = "SimpleS3Copy") -> SparkSession:
    """Create and configure Spark session."""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())


def read_source_data(spark: SparkSession, source_path: str, format: str = "parquet"):
    """Read data from source S3 path."""
    print(f"Reading data from: {source_path}")

    if format.lower() == "csv":
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(source_path))
    elif format.lower() == "json":
        df = spark.read.json(source_path)
    elif format.lower() == "parquet":
        df = spark.read.parquet(source_path)
    else:
        raise ValueError(f"Unsupported format: {format}")

    print(f"Read {df.count()} records")
    return df


def apply_basic_transformations(df):
    """Apply basic data transformations."""
    print("Applying transformations...")

    # Add metadata columns
    df_transformed = df.withColumn("etl_load_timestamp", current_timestamp()) \
                       .withColumn("etl_source", lit("simple_s3_copy"))

    # Add partition columns if date column exists
    if "created_date" in df.columns:
        df_transformed = (df_transformed
                         .withColumn("year", year(col("created_date")))
                         .withColumn("month", month(col("created_date")))
                         .withColumn("day", dayofmonth(col("created_date"))))

    # Clean string columns (trim whitespace)
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df_transformed = df_transformed.withColumn(col_name, trim(col(col_name)))

    print(f"Transformation complete. Schema: {df_transformed.schema.simpleString()}")
    return df_transformed


def write_output(df, destination_path: str, format: str = "parquet", mode: str = "overwrite"):
    """Write data to destination S3 path."""
    print(f"Writing data to: {destination_path}")

    writer = df.write.mode(mode)

    # Partition by date columns if they exist
    partition_cols = [c for c in ["year", "month", "day"] if c in df.columns]
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if format.lower() == "csv":
        writer.option("header", "true").csv(destination_path)
    elif format.lower() == "json":
        writer.json(destination_path)
    elif format.lower() == "parquet":
        writer.parquet(destination_path)

    print("Write complete")


def main():
    parser = argparse.ArgumentParser(description="Simple S3 to S3 ETL")
    parser.add_argument("--source", required=True, help="Source S3 path")
    parser.add_argument("--destination", required=True, help="Destination S3 path")
    parser.add_argument("--format", default="parquet", help="Data format (csv, json, parquet)")
    parser.add_argument("--mode", default="overwrite", help="Write mode (overwrite, append)")

    args = parser.parse_args()

    print("=" * 60)
    print("Simple S3 Copy ETL Job")
    print("=" * 60)
    print(f"Source: {args.source}")
    print(f"Destination: {args.destination}")
    print(f"Format: {args.format}")
    print(f"Mode: {args.mode}")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Read source data
        df = read_source_data(spark, args.source, args.format)

        # Apply transformations
        df_transformed = apply_basic_transformations(df)

        # Write output
        write_output(df_transformed, args.destination, args.format, args.mode)

        print("=" * 60)
        print("Job completed successfully!")
        print(f"Finished: {datetime.now().isoformat()}")
        print("=" * 60)

    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
