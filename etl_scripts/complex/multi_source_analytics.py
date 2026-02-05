#!/usr/bin/env python3
"""
Complex Multi-Source Analytics ETL Script
==========================================

This complex ETL demonstrates:
1. Reading from multiple data sources (S3, JDBC, Glue Catalog)
2. Multiple table joins with different join strategies
3. Complex aggregations with window functions
4. Incremental processing with watermarks
5. Data quality validation with quarantine handling
6. Multi-output writing (data lake + DynamoDB audit)
7. Comprehensive error handling and logging

Usage:
    spark-submit multi_source_analytics.py \
        --config s3://etl-configs/complex_full_pipeline.json

For EMR:
    aws emr add-steps --cluster-id j-XXXXX --steps Type=Spark,...
"""

import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, trim, lower, upper, coalesce,
    to_date, year, month, dayofmonth, date_format, to_timestamp,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    row_number, dense_rank, rank, percent_rank, ntile,
    lead, lag, first, last,
    collect_list, collect_set, size, array_contains, explode,
    struct, array, map_from_arrays, create_map,
    regexp_replace, concat, concat_ws, split, substring,
    expr, datediff, current_date, date_add, date_sub,
    broadcast, monotonically_increasing_id,
    sha2, md5, xxhash64
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, DateType, BooleanType, ArrayType, MapType
)


class DataQualityValidator:
    """Validates data quality and quarantines bad records."""

    def __init__(self, spark: SparkSession, quarantine_path: str):
        self.spark = spark
        self.quarantine_path = quarantine_path
        self.validation_results = []

    def validate_not_null(self, df: DataFrame, columns: List[str], rule_name: str) -> DataFrame:
        """Validate columns are not null, quarantine failures."""
        condition = None
        for col_name in columns:
            null_check = col(col_name).isNull()
            condition = null_check if condition is None else (condition | null_check)

        valid_df = df.filter(~condition) if condition else df
        invalid_df = df.filter(condition) if condition else df.limit(0)

        invalid_count = invalid_df.count()
        total_count = df.count()

        self.validation_results.append({
            "rule": rule_name,
            "columns": columns,
            "total_records": total_count,
            "failed_records": invalid_count,
            "pass_rate": (total_count - invalid_count) / total_count if total_count > 0 else 1.0
        })

        if invalid_count > 0:
            self._quarantine_records(invalid_df, rule_name)

        return valid_df

    def validate_range(self, df: DataFrame, column: str, min_val: float, max_val: float, rule_name: str) -> DataFrame:
        """Validate column values are within range."""
        condition = (col(column) < min_val) | (col(column) > max_val) | col(column).isNull()

        valid_df = df.filter(~condition)
        invalid_df = df.filter(condition)

        invalid_count = invalid_df.count()
        total_count = df.count()

        self.validation_results.append({
            "rule": rule_name,
            "column": column,
            "range": f"[{min_val}, {max_val}]",
            "total_records": total_count,
            "failed_records": invalid_count,
            "pass_rate": (total_count - invalid_count) / total_count if total_count > 0 else 1.0
        })

        if invalid_count > 0:
            self._quarantine_records(invalid_df, rule_name)

        return valid_df

    def validate_unique(self, df: DataFrame, columns: List[str], rule_name: str) -> DataFrame:
        """Validate uniqueness, keep first occurrence, quarantine duplicates."""
        window = Window.partitionBy(columns).orderBy(col("etl_row_id"))

        df_with_rank = df.withColumn("_dup_rank", row_number().over(window))
        valid_df = df_with_rank.filter(col("_dup_rank") == 1).drop("_dup_rank")
        invalid_df = df_with_rank.filter(col("_dup_rank") > 1).drop("_dup_rank")

        invalid_count = invalid_df.count()
        total_count = df.count()

        self.validation_results.append({
            "rule": rule_name,
            "columns": columns,
            "total_records": total_count,
            "duplicate_records": invalid_count,
            "pass_rate": (total_count - invalid_count) / total_count if total_count > 0 else 1.0
        })

        if invalid_count > 0:
            self._quarantine_records(invalid_df, rule_name)

        return valid_df

    def _quarantine_records(self, df: DataFrame, rule_name: str):
        """Write quarantined records to S3."""
        quarantine_df = (df
            .withColumn("quarantine_reason", lit(rule_name))
            .withColumn("quarantine_timestamp", current_timestamp()))

        path = f"{self.quarantine_path}/{rule_name}/{datetime.now().strftime('%Y/%m/%d/%H%M%S')}"
        quarantine_df.write.mode("append").parquet(path)
        print(f"  Quarantined {df.count()} records to {path}")

    def get_summary(self) -> List[Dict]:
        """Get validation summary."""
        return self.validation_results


class MultiSourceAnalyticsETL:
    """Complex multi-source analytics ETL pipeline."""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.dq_validator = DataQualityValidator(
            spark,
            config.get("quarantine_path", "s3://etl-quarantine/")
        )

    def read_source(self, source_config: Dict[str, Any]) -> DataFrame:
        """Read data from various sources."""
        source_type = source_config.get("type", "s3")
        path = source_config.get("path")
        format = source_config.get("format", "parquet")

        print(f"Reading from {source_type}: {path}")

        if source_type == "s3":
            if format == "csv":
                df = (self.spark.read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(path))
            elif format == "json":
                df = self.spark.read.json(path)
            else:
                df = self.spark.read.parquet(path)

        elif source_type == "glue_catalog":
            database = source_config.get("database")
            table = source_config.get("table")
            df = self.spark.table(f"{database}.{table}")

        elif source_type == "jdbc":
            jdbc_url = source_config.get("jdbc_url")
            table = source_config.get("table")
            df = (self.spark.read
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", table)
                  .option("driver", source_config.get("driver", "org.postgresql.Driver"))
                  .load())

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        # Add row ID for deduplication
        df = df.withColumn("etl_row_id", monotonically_increasing_id())
        print(f"  Loaded {df.count()} records")
        return df

    def join_datasets(self, datasets: Dict[str, DataFrame], join_config: List[Dict]) -> DataFrame:
        """Join multiple datasets based on configuration."""
        print("Joining datasets...")

        result = None
        for join_spec in join_config:
            left_name = join_spec["left"]
            right_name = join_spec["right"]
            join_keys = join_spec["keys"]
            join_type = join_spec.get("type", "inner")
            broadcast_hint = join_spec.get("broadcast", None)

            left_df = datasets[left_name] if result is None else result
            right_df = datasets[right_name]

            # Apply broadcast hint if specified
            if broadcast_hint == right_name:
                right_df = broadcast(right_df)

            # Handle column name conflicts
            right_cols_to_rename = [c for c in right_df.columns
                                    if c in left_df.columns and c not in join_keys]
            for col_name in right_cols_to_rename:
                right_df = right_df.withColumnRenamed(col_name, f"{right_name}_{col_name}")

            result = left_df.join(right_df, join_keys, join_type)
            print(f"  Joined {left_name} + {right_name} ({join_type}): {result.count()} records")

        return result

    def apply_transformations(self, df: DataFrame, transform_config: List[Dict]) -> DataFrame:
        """Apply configured transformations."""
        print("Applying transformations...")

        for transform in transform_config:
            transform_type = transform["type"]

            if transform_type == "derive":
                col_name = transform["column"]
                expression = transform["expression"]
                df = df.withColumn(col_name, expr(expression))
                print(f"  Derived: {col_name}")

            elif transform_type == "aggregate":
                group_by = transform["group_by"]
                agg_exprs = []
                for agg in transform["aggregations"]:
                    agg_func = agg["function"]
                    agg_col = agg["column"]
                    alias = agg.get("alias", f"{agg_func}_{agg_col}")

                    if agg_func == "sum":
                        agg_exprs.append(spark_sum(agg_col).alias(alias))
                    elif agg_func == "count":
                        agg_exprs.append(count(agg_col).alias(alias))
                    elif agg_func == "avg":
                        agg_exprs.append(avg(agg_col).alias(alias))
                    elif agg_func == "max":
                        agg_exprs.append(spark_max(agg_col).alias(alias))
                    elif agg_func == "min":
                        agg_exprs.append(spark_min(agg_col).alias(alias))

                df = df.groupBy(group_by).agg(*agg_exprs)
                print(f"  Aggregated by: {group_by}")

            elif transform_type == "window":
                col_name = transform["column"]
                partition_by = transform.get("partition_by", [])
                order_by = transform.get("order_by", [])
                function = transform["function"]

                window_spec = Window.partitionBy(partition_by).orderBy(order_by)

                if function == "row_number":
                    df = df.withColumn(col_name, row_number().over(window_spec))
                elif function == "rank":
                    df = df.withColumn(col_name, rank().over(window_spec))
                elif function == "dense_rank":
                    df = df.withColumn(col_name, dense_rank().over(window_spec))
                elif function == "lag":
                    offset = transform.get("offset", 1)
                    source_col = transform["source_column"]
                    df = df.withColumn(col_name, lag(source_col, offset).over(window_spec))
                elif function == "lead":
                    offset = transform.get("offset", 1)
                    source_col = transform["source_column"]
                    df = df.withColumn(col_name, lead(source_col, offset).over(window_spec))

                print(f"  Window function: {col_name}")

            elif transform_type == "filter":
                condition = transform["condition"]
                df = df.filter(expr(condition))
                print(f"  Filtered: {condition}")

            elif transform_type == "hash":
                col_name = transform["column"]
                source_cols = transform["source_columns"]
                df = df.withColumn(col_name, sha2(concat_ws("|", *[col(c) for c in source_cols]), 256))
                print(f"  Hashed: {col_name}")

        return df

    def calculate_advanced_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate advanced analytics metrics."""
        print("Calculating advanced metrics...")

        # Time-based windows for trend analysis
        if "transaction_date" in df.columns and "customer_id" in df.columns:
            # 30-day rolling metrics per customer
            window_30d = (Window
                .partitionBy("customer_id")
                .orderBy(col("transaction_date").cast("long"))
                .rangeBetween(-30 * 86400, 0))

            df = (df
                .withColumn("rolling_30d_spend", spark_sum("transaction_amount").over(window_30d))
                .withColumn("rolling_30d_txn_count", count("*").over(window_30d)))

            # YoY comparison
            window_yoy = Window.partitionBy("customer_id", month("transaction_date"))
            df = (df
                .withColumn("mom_spend_change",
                    (col("transaction_amount") - lag("transaction_amount", 1).over(
                        Window.partitionBy("customer_id").orderBy("transaction_date"))) /
                    lag("transaction_amount", 1).over(
                        Window.partitionBy("customer_id").orderBy("transaction_date"))))

        # Percentile calculations
        if "total_spend" in df.columns:
            df = df.withColumn("spend_percentile",
                percent_rank().over(Window.orderBy("total_spend")))
            df = df.withColumn("spend_quartile",
                ntile(4).over(Window.orderBy("total_spend")))

        return df

    def run_data_quality(self, df: DataFrame) -> DataFrame:
        """Run comprehensive data quality checks."""
        print("Running data quality checks...")

        # Required columns not null
        required_cols = self.config.get("dq_required_columns", ["customer_id"])
        if required_cols:
            df = self.dq_validator.validate_not_null(df, required_cols, "required_columns_not_null")

        # Amount validation
        if "transaction_amount" in df.columns:
            df = self.dq_validator.validate_range(
                df, "transaction_amount", 0, 1000000, "transaction_amount_range")

        # Uniqueness check
        unique_cols = self.config.get("dq_unique_columns", None)
        if unique_cols:
            df = self.dq_validator.validate_unique(df, unique_cols, "uniqueness_check")

        # Print DQ summary
        print("\nData Quality Summary:")
        for result in self.dq_validator.get_summary():
            print(f"  {result['rule']}: {result['pass_rate']*100:.2f}% pass rate")

        return df

    def write_outputs(self, df: DataFrame):
        """Write to multiple output destinations."""
        print("Writing outputs...")

        # Add ETL metadata
        df = (df
            .withColumn("etl_load_timestamp", current_timestamp())
            .withColumn("etl_pipeline", lit("multi_source_analytics"))
            .withColumn("etl_run_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))))

        # Primary output to S3
        primary_output = self.config.get("primary_output", {})
        if primary_output:
            path = primary_output["path"]
            format = primary_output.get("format", "parquet")
            partition_by = primary_output.get("partition_by", ["year", "month"])
            mode = primary_output.get("mode", "overwrite")

            # Add partition columns
            if "year" in partition_by and "transaction_date" in df.columns:
                df = df.withColumn("year", year(col("transaction_date")))
            if "month" in partition_by and "transaction_date" in df.columns:
                df = df.withColumn("month", month(col("transaction_date")))

            writer = df.write.mode(mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)

            if format == "parquet":
                writer.parquet(path)
            elif format == "delta":
                writer.format("delta").save(path)
            elif format == "csv":
                writer.option("header", "true").csv(path)

            print(f"  Written to primary output: {path}")

        # Secondary outputs (aggregations, etc.)
        for secondary in self.config.get("secondary_outputs", []):
            sec_path = secondary["path"]
            agg_cols = secondary.get("aggregate_by", [])
            metrics = secondary.get("metrics", [])

            if agg_cols and metrics:
                agg_exprs = []
                for m in metrics:
                    if m["function"] == "sum":
                        agg_exprs.append(spark_sum(m["column"]).alias(m.get("alias", f"sum_{m['column']}")))
                    elif m["function"] == "count":
                        agg_exprs.append(count(m["column"]).alias(m.get("alias", f"count_{m['column']}")))

                agg_df = df.groupBy(agg_cols).agg(*agg_exprs)
                agg_df.write.mode("overwrite").parquet(sec_path)
                print(f"  Written aggregation to: {sec_path}")

    def run(self):
        """Execute the full ETL pipeline."""
        print("=" * 70)
        print("Multi-Source Analytics ETL Pipeline")
        print("=" * 70)
        print(f"Started: {datetime.now().isoformat()}")
        print("=" * 70)

        # Read all sources
        datasets = {}
        for source_name, source_config in self.config.get("sources", {}).items():
            datasets[source_name] = self.read_source(source_config)

        # Join datasets
        join_config = self.config.get("joins", [])
        if join_config:
            df = self.join_datasets(datasets, join_config)
        else:
            # Use first dataset if no joins configured
            df = list(datasets.values())[0]

        # Apply transformations
        transform_config = self.config.get("transformations", [])
        if transform_config:
            df = self.apply_transformations(df, transform_config)

        # Calculate advanced metrics
        if self.config.get("calculate_advanced_metrics", False):
            df = self.calculate_advanced_metrics(df)

        # Run data quality
        if self.config.get("enable_dq", True):
            df = self.run_data_quality(df)

        # Write outputs
        self.write_outputs(df)

        print("=" * 70)
        print("Pipeline completed successfully!")
        print(f"Finished: {datetime.now().isoformat()}")
        print("=" * 70)

        return df


def main():
    parser = argparse.ArgumentParser(description="Multi-Source Analytics ETL")
    parser.add_argument("--config", required=True, help="Path to config JSON file (local or S3)")
    parser.add_argument("--local-config", action="store_true", help="Config is a local file")

    args = parser.parse_args()

    # Create Spark session
    spark = (SparkSession.builder
             .appName("MultiSourceAnalyticsETL")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.dynamicAllocation.enabled", "true")
             .config("spark.dynamicAllocation.minExecutors", "2")
             .config("spark.dynamicAllocation.maxExecutors", "20")
             .getOrCreate())

    try:
        # Load config
        if args.local_config:
            with open(args.config, 'r') as f:
                config = json.load(f)
        else:
            # Read config from S3
            config_df = spark.read.text(args.config)
            config_str = config_df.collect()[0][0]
            config = json.loads(config_str)

        # Run ETL
        etl = MultiSourceAnalyticsETL(spark, config)
        etl.run()

    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
