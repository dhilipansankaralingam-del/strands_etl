#!/usr/bin/env python3
"""
Interactive PySpark Cost Optimizer
==================================

Conversational interface for cost optimization analysis with specialized
features for S3 small files detection and large historical table optimization.

Interactive Mode:
    python scripts/optimize_costs_interactive.py

With Initial Script:
    python scripts/optimize_costs_interactive.py --script my_job.py

Conversation Mode (LLM):
    python scripts/optimize_costs_interactive.py --chat

JSON Input:
    python scripts/optimize_costs_interactive.py --input analysis_request.json

S3 Small Files Scanner:
    # Scan S3 path for small files (KB-sized) that impact Spark performance
    python scripts/optimize_costs_interactive.py --scan-s3 s3://bucket/prefix/
    python scripts/optimize_costs_interactive.py --scan-s3 s3://bucket/prefix/ --max-files 50000 --output report.json

Large Historical Table Optimizer:
    # Analyze billion-row tables with wide columns
    python scripts/optimize_costs_interactive.py --large-table db.historical_transactions --rows 1600000000 --cols 150
    python scripts/optimize_costs_interactive.py --large-table db.events --rows 2000000000 --cols 200 --partitions date region

Features:
    - S3 Small Files Scanner: Identifies KB-sized files that cause Spark performance issues
    - Large Table Optimizer: Strategies for billion-row tables with wide columns
    - Code pattern analysis
    - Resource allocation recommendations
    - Platform cost comparison (Glue, EMR, EKS)
    - LLM-powered conversation mode for follow-up questions
"""

import sys
import json
import argparse
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cost_optimizer import CostOptimizationOrchestrator, BatchAnalyzer

# Optional boto3 for S3 operations
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


@dataclass
class FileInfo:
    """Information about a file in S3."""
    key: str
    size_bytes: int
    last_modified: str

    @property
    def size_kb(self) -> float:
        return self.size_bytes / 1024

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)

    @property
    def size_category(self) -> str:
        """Categorize file by size."""
        if self.size_bytes < 1024:  # < 1 KB
            return "tiny"
        elif self.size_bytes < 128 * 1024:  # < 128 KB
            return "very_small"
        elif self.size_bytes < 1024 * 1024:  # < 1 MB
            return "small"
        elif self.size_bytes < 128 * 1024 * 1024:  # < 128 MB
            return "optimal"
        elif self.size_bytes < 1024 * 1024 * 1024:  # < 1 GB
            return "large"
        else:
            return "very_large"


class S3SmallFilesScanner:
    """Scanner for detecting small files in S3 paths."""

    # Optimal file sizes for Spark (in bytes)
    OPTIMAL_MIN_SIZE = 128 * 1024 * 1024  # 128 MB
    OPTIMAL_MAX_SIZE = 512 * 1024 * 1024  # 512 MB
    SMALL_FILE_THRESHOLD = 1024 * 1024    # 1 MB - files below this are "small"

    def __init__(self):
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for S3 scanning. Install with: pip install boto3")
        self.s3_client = boto3.client('s3')

    def parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and prefix."""
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path: {s3_path}. Must start with s3://")

        path = s3_path[5:]  # Remove 's3://'
        parts = path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        return bucket, prefix

    def scan_path(self, s3_path: str, max_files: int = 10000) -> Dict[str, Any]:
        """
        Scan an S3 path for small files.

        Returns analysis with file size distribution and recommendations.
        """
        bucket, prefix = self.parse_s3_path(s3_path)

        files: List[FileInfo] = []
        continuation_token = None

        print(f"   Scanning s3://{bucket}/{prefix}...")

        try:
            while len(files) < max_files:
                kwargs = {
                    'Bucket': bucket,
                    'Prefix': prefix,
                    'MaxKeys': 1000
                }
                if continuation_token:
                    kwargs['ContinuationToken'] = continuation_token

                response = self.s3_client.list_objects_v2(**kwargs)

                for obj in response.get('Contents', []):
                    # Skip directories (size 0 keys ending with /)
                    if obj['Size'] == 0 and obj['Key'].endswith('/'):
                        continue
                    # Skip metadata files
                    if any(x in obj['Key'] for x in ['_SUCCESS', '_metadata', '.crc', '_committed_', '_started_']):
                        continue

                    files.append(FileInfo(
                        key=obj['Key'],
                        size_bytes=obj['Size'],
                        last_modified=str(obj['LastModified'])
                    ))

                if not response.get('IsTruncated'):
                    break
                continuation_token = response.get('NextContinuationToken')

                # Progress indicator
                print(f"   Found {len(files)} files...", end='\r')

        except NoCredentialsError:
            return {
                'success': False,
                'error': 'AWS credentials not configured. Run: aws configure'
            }
        except ClientError as e:
            return {
                'success': False,
                'error': f"S3 error: {e.response['Error']['Message']}"
            }

        print(f"   Found {len(files)} data files.     ")

        return self._analyze_files(files, s3_path)

    def _analyze_files(self, files: List[FileInfo], s3_path: str) -> Dict[str, Any]:
        """Analyze file distribution and generate recommendations."""
        if not files:
            return {
                'success': True,
                's3_path': s3_path,
                'total_files': 0,
                'message': 'No data files found in path'
            }

        # Categorize files
        categories = defaultdict(list)
        for f in files:
            categories[f.size_category].append(f)

        # Calculate statistics
        total_size = sum(f.size_bytes for f in files)
        avg_size = total_size / len(files) if files else 0
        small_files = [f for f in files if f.size_bytes < self.SMALL_FILE_THRESHOLD]
        kb_files = [f for f in files if f.size_bytes < 1024 * 1024]  # < 1MB (KB range)

        # File format analysis
        formats = defaultdict(int)
        for f in files:
            ext = Path(f.key).suffix.lower()
            if ext:
                formats[ext] += 1
            else:
                # Check for partitioned parquet/orc
                if '/part-' in f.key or f.key.endswith('.parquet') or 'parquet' in f.key:
                    formats['.parquet'] += 1
                elif f.key.endswith('.orc') or 'orc' in f.key:
                    formats['.orc'] += 1
                else:
                    formats['unknown'] += 1

        # Generate recommendations
        recommendations = self._generate_recommendations(
            files, small_files, kb_files, avg_size, total_size, categories
        )

        # Size distribution
        size_distribution = {
            'tiny_under_1kb': len(categories['tiny']),
            'very_small_1kb_128kb': len(categories['very_small']),
            'small_128kb_1mb': len(categories['small']),
            'optimal_1mb_128mb': len(categories['optimal']),
            'large_128mb_1gb': len(categories['large']),
            'very_large_over_1gb': len(categories['very_large'])
        }

        # Sample small files for display
        sample_small_files = sorted(kb_files, key=lambda x: x.size_bytes)[:20]

        return {
            'success': True,
            's3_path': s3_path,
            'total_files': len(files),
            'total_size_bytes': total_size,
            'total_size_gb': total_size / (1024**3),
            'average_file_size_bytes': avg_size,
            'average_file_size_mb': avg_size / (1024**2),
            'small_files_count': len(small_files),
            'small_files_percent': (len(small_files) / len(files)) * 100,
            'kb_files_count': len(kb_files),
            'kb_files_percent': (len(kb_files) / len(files)) * 100,
            'size_distribution': size_distribution,
            'file_formats': dict(formats),
            'sample_small_files': [
                {'key': f.key, 'size_kb': round(f.size_kb, 2)}
                for f in sample_small_files
            ],
            'recommendations': recommendations,
            'severity': 'critical' if len(kb_files) > len(files) * 0.5 else
                       'high' if len(kb_files) > len(files) * 0.3 else
                       'medium' if len(small_files) > len(files) * 0.2 else 'low'
        }

    def _generate_recommendations(
        self,
        files: List[FileInfo],
        small_files: List[FileInfo],
        kb_files: List[FileInfo],
        avg_size: float,
        total_size: float,
        categories: Dict
    ) -> List[Dict]:
        """Generate recommendations based on file analysis."""
        recommendations = []

        # Critical: Many KB-sized files
        if len(kb_files) > len(files) * 0.3:
            optimal_file_count = max(1, int(total_size / (128 * 1024 * 1024)))  # 128MB target

            recommendations.append({
                'priority': 'P0',
                'category': 'small_files',
                'title': 'Critical: Too Many Small Files (KB-sized)',
                'description': f'{len(kb_files)} files ({len(kb_files)/len(files)*100:.1f}%) are in KB range. '
                              f'This severely impacts Spark performance.',
                'impact': 'High task overhead, poor parallelism, excessive driver memory',
                'current_state': f'{len(files)} files, avg {avg_size/(1024*1024):.2f} MB',
                'target_state': f'~{optimal_file_count} files, avg 128 MB each',
                'solutions': [
                    {
                        'name': 'Compaction with coalesce()',
                        'code': f'''# Compact small files during write
df.coalesce({optimal_file_count}).write.mode("overwrite").parquet("output_path")''',
                        'when_to_use': 'When rewriting the dataset'
                    },
                    {
                        'name': 'Use repartition() for better distribution',
                        'code': f'''# Repartition for even file sizes
df.repartition({optimal_file_count}).write.mode("overwrite").parquet("output_path")''',
                        'when_to_use': 'When data is skewed and needs redistribution'
                    },
                    {
                        'name': 'Set maxRecordsPerFile',
                        'code': '''# Control records per file
df.write.option("maxRecordsPerFile", 1000000).parquet("output_path")''',
                        'when_to_use': 'When row counts vary significantly'
                    },
                    {
                        'name': 'Use Delta Lake OPTIMIZE',
                        'code': '''-- Run bin-packing compaction
OPTIMIZE delta.`s3://bucket/path`''',
                        'when_to_use': 'If using Delta Lake format'
                    },
                    {
                        'name': 'AWS Glue built-in grouping',
                        'code': '''# Use Glue's automatic grouping
glueContext.create_dynamic_frame.from_catalog(
    database="db",
    table_name="table",
    additional_options={
        "groupFiles": "inPartition",
        "groupSize": "134217728"  # 128MB
    }
)''',
                        'when_to_use': 'When reading in AWS Glue jobs'
                    }
                ]
            })

        # High: Small files (1KB - 1MB)
        elif len(small_files) > len(files) * 0.2:
            recommendations.append({
                'priority': 'P1',
                'category': 'small_files',
                'title': 'High: Significant Small File Problem',
                'description': f'{len(small_files)} files ({len(small_files)/len(files)*100:.1f}%) are under 1MB',
                'impact': 'Reduced read performance, increased task scheduling overhead',
                'solutions': [
                    {
                        'name': 'Enable adaptive file coalescing',
                        'code': '''spark.conf.set("spark.sql.files.minPartitionNum", "1")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB''',
                        'when_to_use': 'During read operations'
                    },
                    {
                        'name': 'Periodic compaction job',
                        'code': '''# Scheduled compaction
df = spark.read.parquet("s3://input/")
df.repartition(optimal_partitions).write.mode("overwrite").parquet("s3://output/")''',
                        'when_to_use': 'As a maintenance job'
                    }
                ]
            })

        # Spark read configurations for small files
        if len(kb_files) > 0:
            recommendations.append({
                'priority': 'P1',
                'category': 'spark_config',
                'title': 'Optimize Spark Configurations for Small Files',
                'description': 'Adjust Spark settings to handle small files more efficiently',
                'spark_configs': {
                    'spark.sql.files.maxPartitionBytes': '134217728',  # 128MB
                    'spark.sql.files.openCostInBytes': '4194304',     # 4MB
                    'spark.hadoop.mapreduce.input.fileinputformat.split.minsize': '134217728',
                    'spark.sql.adaptive.enabled': 'true',
                    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                    'spark.sql.adaptive.coalescePartitions.minPartitionSize': '64MB'
                }
            })

        # Very large files
        if len(categories['very_large']) > 0:
            recommendations.append({
                'priority': 'P2',
                'category': 'large_files',
                'title': 'Very Large Files Detected',
                'description': f'{len(categories["very_large"])} files exceed 1GB',
                'impact': 'May cause memory pressure, limits parallelism',
                'solutions': [
                    {
                        'name': 'Split large files',
                        'code': '''df.repartition(num_partitions).write.parquet("output")''',
                        'when_to_use': 'When rewriting data'
                    }
                ]
            })

        return recommendations


class LargeTableOptimizer:
    """Optimizer for large historical tables (billions of rows)."""

    def __init__(self):
        self.recommendations = []

    def analyze_large_table_scenario(
        self,
        table_name: str,
        row_count: int,
        column_count: int,
        avg_row_size_bytes: int = None,
        partition_columns: List[str] = None,
        query_patterns: List[str] = None,
        current_read_approach: str = None
    ) -> Dict[str, Any]:
        """
        Analyze a large historical table and provide optimization strategies.

        Args:
            table_name: Name of the table
            row_count: Number of rows (e.g., 1.6 billion)
            column_count: Number of columns
            avg_row_size_bytes: Average size per row
            partition_columns: Current partition columns if any
            query_patterns: How the data is typically queried
            current_read_approach: Current reading strategy
        """
        # Estimate sizes if not provided
        if avg_row_size_bytes is None:
            # Estimate: ~50 bytes per column on average for wide tables
            avg_row_size_bytes = column_count * 50

        total_size_bytes = row_count * avg_row_size_bytes
        total_size_gb = total_size_bytes / (1024**3)
        total_size_tb = total_size_gb / 1024

        # Calculate optimal configurations
        optimal_partitions = self._calculate_optimal_partitions(total_size_bytes)
        memory_requirements = self._calculate_memory_requirements(total_size_bytes, column_count)

        analysis = {
            'table_info': {
                'name': table_name,
                'row_count': row_count,
                'row_count_formatted': f"{row_count/1e9:.2f}B",
                'column_count': column_count,
                'estimated_size_gb': round(total_size_gb, 2),
                'estimated_size_tb': round(total_size_tb, 3),
                'avg_row_size_bytes': avg_row_size_bytes,
                'is_wide_table': column_count > 100,
                'is_billion_scale': row_count >= 1e9
            },
            'classification': self._classify_table(row_count, column_count, total_size_gb),
            'optimal_config': {
                'partitions': optimal_partitions,
                'memory_per_executor': memory_requirements['executor_memory'],
                'executor_cores': memory_requirements['executor_cores'],
                'recommended_workers': memory_requirements['recommended_workers'],
                'worker_type': memory_requirements['worker_type']
            },
            'strategies': self._generate_strategies(
                row_count, column_count, total_size_gb, partition_columns, query_patterns
            ),
            'spark_configs': self._generate_spark_configs(total_size_gb, row_count),
            'anti_patterns_to_avoid': self._get_anti_patterns(),
            'implementation_examples': self._get_implementation_examples(
                table_name, column_count, partition_columns
            )
        }

        return analysis

    def _classify_table(self, row_count: int, column_count: int, size_gb: float) -> Dict:
        """Classify the table based on characteristics."""
        classifications = []

        if row_count >= 1e9:
            classifications.append("billion_scale")
        if column_count > 200:
            classifications.append("very_wide")
        elif column_count > 100:
            classifications.append("wide")
        if size_gb > 1000:
            classifications.append("terabyte_scale")

        # Determine complexity
        complexity = 'low'
        if row_count >= 1e9 and column_count > 100:
            complexity = 'very_high'
        elif row_count >= 1e9 or (column_count > 100 and size_gb > 500):
            complexity = 'high'
        elif row_count >= 1e8 or size_gb > 100:
            complexity = 'medium'

        return {
            'classifications': classifications,
            'complexity': complexity,
            'description': f"{'Very wide' if column_count > 200 else 'Wide' if column_count > 100 else 'Standard'} "
                          f"table with {row_count/1e9:.1f}B rows, estimated {size_gb:.0f} GB"
        }

    def _calculate_optimal_partitions(self, total_size_bytes: int) -> int:
        """Calculate optimal number of partitions."""
        # Target: 128MB per partition
        target_partition_size = 128 * 1024 * 1024
        optimal = max(200, int(total_size_bytes / target_partition_size))

        # Round to a nice number
        if optimal > 10000:
            optimal = (optimal // 1000) * 1000
        elif optimal > 1000:
            optimal = (optimal // 100) * 100

        return optimal

    def _calculate_memory_requirements(self, total_size_bytes: int, column_count: int) -> Dict:
        """Calculate memory and resource requirements."""
        size_gb = total_size_bytes / (1024**3)

        # Wide tables need more memory due to schema overhead
        memory_multiplier = 1.5 if column_count > 100 else 1.0

        if size_gb > 1000:  # > 1TB
            return {
                'executor_memory': '64g',
                'executor_cores': 8,
                'recommended_workers': max(50, int(size_gb / 50)),
                'worker_type': 'G.8X',
                'driver_memory': '32g'
            }
        elif size_gb > 500:
            return {
                'executor_memory': '32g',
                'executor_cores': 8,
                'recommended_workers': max(30, int(size_gb / 30)),
                'worker_type': 'G.4X',
                'driver_memory': '16g'
            }
        elif size_gb > 100:
            return {
                'executor_memory': '16g',
                'executor_cores': 4,
                'recommended_workers': max(20, int(size_gb / 20)),
                'worker_type': 'G.2X',
                'driver_memory': '8g'
            }
        else:
            return {
                'executor_memory': '8g',
                'executor_cores': 4,
                'recommended_workers': max(10, int(size_gb / 10)),
                'worker_type': 'G.1X',
                'driver_memory': '4g'
            }

    def _generate_strategies(
        self,
        row_count: int,
        column_count: int,
        size_gb: float,
        partition_columns: List[str],
        query_patterns: List[str]
    ) -> List[Dict]:
        """Generate optimization strategies for large table."""
        strategies = []

        # Strategy 1: Column Pruning (Critical for wide tables)
        if column_count > 50:
            strategies.append({
                'priority': 'P0',
                'name': 'Column Pruning',
                'description': 'Select only required columns instead of SELECT *',
                'impact': f'Can reduce data read by {(1 - 20/column_count)*100:.0f}%+ if only 20 columns needed',
                'implementation': '''# BAD: Reading all columns
df = spark.read.parquet("s3://bucket/large_table/")

# GOOD: Read only needed columns with predicate pushdown
df = spark.read.parquet("s3://bucket/large_table/") \\
    .select("col1", "col2", "col3")  # Pushes down to Parquet reader

# BEST: Use schema pruning from catalog
spark.conf.set("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")
df = spark.table("database.large_table").select("col1", "col2", "col3")'''
            })

        # Strategy 2: Partition Pruning
        strategies.append({
            'priority': 'P0',
            'name': 'Partition Pruning',
            'description': 'Filter on partition columns to avoid full table scan',
            'impact': 'Can reduce data scanned by 90%+ with proper partitioning',
            'implementation': f'''# Ensure filter on partition columns comes FIRST
# Current partitions: {partition_columns or "None detected - consider adding!"}

# GOOD: Filter on partition column first
df = spark.read.parquet("s3://bucket/large_table/") \\
    .filter(col("date") >= "2024-01-01") \\  # Partition filter FIRST
    .filter(col("status") == "active") \\     # Then other filters
    .select("col1", "col2")

# For date-partitioned tables, use date range
df = spark.table("db.table") \\
    .where("year = 2024 AND month >= 6")  # Partition pruning'''
        })

        # Strategy 3: Incremental Processing
        strategies.append({
            'priority': 'P0',
            'name': 'Incremental Processing',
            'description': 'Process only new/changed data instead of full historical table',
            'impact': f'For 1.6B rows, processing 1% incrementally = 16M rows vs 1.6B',
            'implementation': '''# Track watermark for incremental reads
last_processed = get_last_watermark()  # From your state store

# Read only new data
df = spark.read.parquet("s3://bucket/large_table/") \\
    .filter(col("updated_at") > last_processed) \\
    .filter(col("date") >= date_sub(current_date(), 7))  # Safety window

# For Delta Lake - use time travel or changes
df = spark.read.format("delta") \\
    .option("readChangeFeed", "true") \\
    .option("startingVersion", last_version) \\
    .load("s3://bucket/delta_table/")'''
        })

        # Strategy 4: Bucketing/Z-Ordering for joins
        strategies.append({
            'priority': 'P1',
            'name': 'Pre-sorted/Bucketed Data',
            'description': 'Avoid shuffle by pre-organizing data on join keys',
            'impact': 'Eliminates shuffle for joins - major savings for billion-row joins',
            'implementation': '''# Create bucketed table (one-time setup)
df.write \\
    .bucketBy(256, "customer_id") \\
    .sortBy("customer_id") \\
    .saveAsTable("db.large_table_bucketed")

# Join two bucketed tables - NO SHUFFLE!
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")

df1 = spark.table("db.large_table_bucketed")
df2 = spark.table("db.lookup_bucketed")
result = df1.join(df2, "customer_id")  # Sort-merge join, no shuffle

# For Delta Lake - use Z-ORDER
# OPTIMIZE table ZORDER BY (customer_id, date)'''
        })

        # Strategy 5: Broadcast for dimension tables
        strategies.append({
            'priority': 'P1',
            'name': 'Broadcast Dimension Tables',
            'description': 'Broadcast smaller lookup tables to avoid shuffle on large table',
            'impact': 'Eliminates shuffle of the 1.6B row table during joins',
            'implementation': '''from pyspark.sql.functions import broadcast

# Large fact table (1.6B rows) - DO NOT move this
large_df = spark.table("db.large_historical_table") \\
    .filter(col("date") >= "2024-01-01")  # Reduce with partition pruning

# Small dimension table (< 100MB) - BROADCAST this
dim_df = spark.table("db.dimension_table")

# Join with broadcast hint
result = large_df.join(
    broadcast(dim_df),  # Broadcast the small table
    "join_key"
)

# Or use SQL hint
spark.sql("""
    SELECT /*+ BROADCAST(dim) */ *
    FROM large_table l
    JOIN dimension_table dim ON l.key = dim.key
""")'''
        })

        # Strategy 6: Sampling for development/testing
        strategies.append({
            'priority': 'P2',
            'name': 'Sampling for Development',
            'description': 'Use sampling during development to speed up iterations',
            'impact': 'Development cycles 100x faster',
            'implementation': '''# For development/testing - sample the data
df = spark.read.parquet("s3://bucket/large_table/") \\
    .sample(fraction=0.001, seed=42)  # 0.1% = 1.6M rows

# Or limit to recent partitions for dev
df = spark.read.parquet("s3://bucket/large_table/") \\
    .filter(col("date") >= date_sub(current_date(), 7))

# Use environment variable to control
import os
is_dev = os.getenv("ENVIRONMENT") == "dev"
df = spark.read.parquet("s3://bucket/large_table/")
if is_dev:
    df = df.sample(0.01)'''
        })

        # Strategy 7: Caching strategy
        strategies.append({
            'priority': 'P2',
            'name': 'Strategic Caching',
            'description': 'Cache filtered/aggregated intermediate results, not raw data',
            'impact': 'Avoids re-reading TB of data for iterative operations',
            'implementation': '''# WRONG: Caching full large table (will fail or be slow)
# df.cache()  # DON'T DO THIS for 1.6B rows

# RIGHT: Cache AFTER filtering and aggregation
filtered_df = spark.read.parquet("s3://bucket/large_table/") \\
    .filter(col("date") >= "2024-01-01") \\
    .filter(col("region") == "US") \\
    .select("customer_id", "amount", "date")

# Only cache the reduced dataset if reused multiple times
if filtered_df.count() < 100_000_000:  # < 100M rows
    filtered_df = filtered_df.cache()
    filtered_df.count()  # Trigger cache

# For multiple aggregations on same base
daily_agg = filtered_df.groupBy("date").agg(...)
daily_agg.cache()  # Cache small aggregated result'''
        })

        return strategies

    def _generate_spark_configs(self, size_gb: float, row_count: int) -> Dict:
        """Generate Spark configurations for large table processing."""
        configs = {
            'shuffle_and_memory': {
                'spark.sql.shuffle.partitions': str(min(10000, max(200, int(size_gb * 2)))),
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
                'spark.sql.adaptive.skewJoin.skewedPartitionFactor': '5',
                'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes': '256MB',
                'spark.sql.autoBroadcastJoinThreshold': '100MB',
            },
            'memory_management': {
                'spark.memory.fraction': '0.8',
                'spark.memory.storageFraction': '0.3',
                'spark.sql.windowExec.buffer.spill.threshold': '4096',
                'spark.executor.memoryOverhead': '2g' if size_gb > 500 else '1g',
            },
            'io_optimization': {
                'spark.sql.parquet.enableVectorizedReader': 'true',
                'spark.sql.parquet.filterPushdown': 'true',
                'spark.sql.parquet.aggregatePushdown': 'true',
                'spark.hadoop.parquet.enable.summary-metadata': 'false',
                'spark.sql.files.maxPartitionBytes': '128MB',
                'spark.sql.files.openCostInBytes': '4MB',
            },
            'serialization': {
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.kryoserializer.buffer.max': '1024m',
            },
            'gc_tuning': {
                'spark.executor.extraJavaOptions': '-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35',
            }
        }

        return configs

    def _get_anti_patterns(self) -> List[Dict]:
        """Get anti-patterns to avoid with large tables."""
        return [
            {
                'pattern': 'SELECT * on wide tables',
                'issue': 'Reads all columns even if only few needed',
                'fix': 'Always specify column list explicitly'
            },
            {
                'pattern': 'collect() on large datasets',
                'issue': 'Pulls all data to driver - will OOM',
                'fix': 'Use take(), head(), or write to storage'
            },
            {
                'pattern': 'Full table scan without partition filters',
                'issue': 'Scans entire 1.6B rows unnecessarily',
                'fix': 'Always filter on partition columns first'
            },
            {
                'pattern': 'Cartesian joins (cross join)',
                'issue': '1.6B x N = explosion',
                'fix': 'Ensure join conditions are specified'
            },
            {
                'pattern': 'UDFs on full dataset',
                'issue': 'Serialization overhead on billions of rows',
                'fix': 'Use built-in functions or Pandas UDFs'
            },
            {
                'pattern': 'Multiple shuffles in sequence',
                'issue': 'Each shuffle moves TB of data',
                'fix': 'Combine operations, use bucketing'
            },
            {
                'pattern': '.cache() on full large table',
                'issue': 'Cannot fit in memory',
                'fix': 'Cache filtered/aggregated subsets only'
            },
            {
                'pattern': 'count() just to check if data exists',
                'issue': 'Full scan for simple check',
                'fix': 'Use df.head(1) or df.isEmpty()'
            }
        ]

    def _get_implementation_examples(
        self,
        table_name: str,
        column_count: int,
        partition_columns: List[str]
    ) -> Dict:
        """Get implementation examples for the specific table."""
        partition_filter = ""
        if partition_columns:
            partition_filter = f'.filter(col("{partition_columns[0]}") >= "2024-01-01")'

        return {
            'optimal_read_pattern': f'''from pyspark.sql.functions import col, broadcast

# Step 1: Set optimal configurations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# Step 2: Read with column and partition pruning
df = spark.table("{table_name}") \\
    {partition_filter} \\
    .select("col1", "col2", "col3")  # Only needed columns

# Step 3: Join with smaller tables using broadcast
result = df.join(
    broadcast(dimension_df),
    "join_key"
)

# Step 4: Write with optimal partitioning
result.repartition(200) \\
    .write \\
    .mode("overwrite") \\
    .partitionBy("date") \\
    .parquet("s3://output/")''',

            'incremental_pattern': f'''# Incremental processing pattern
from pyspark.sql.functions import col, max as spark_max

# Get last processed watermark
watermark_df = spark.table("control.watermarks") \\
    .filter(col("table_name") == "{table_name}")
last_watermark = watermark_df.select("last_value").collect()[0][0]

# Read only new data
new_data = spark.table("{table_name}") \\
    .filter(col("updated_at") > last_watermark) \\
    .select("col1", "col2", "col3")

# Process new data
processed = transform(new_data)
processed.write.mode("append").parquet("s3://output/")

# Update watermark
new_watermark = new_data.agg(spark_max("updated_at")).collect()[0][0]
update_watermark("{table_name}", new_watermark)''',

            'chunked_processing': f'''# Process in chunks for very large operations
from pyspark.sql.functions import col

# Get partition values
partitions = spark.sql("""
    SELECT DISTINCT date FROM {table_name}
    WHERE date >= '2024-01-01'
    ORDER BY date
""").collect()

# Process each partition separately
for row in partitions:
    partition_date = row["date"]

    chunk = spark.table("{table_name}") \\
        .filter(col("date") == partition_date) \\
        .select("col1", "col2", "col3")

    # Process this chunk
    result = transform(chunk)

    # Write incrementally
    result.write \\
        .mode("append") \\
        .partitionBy("date") \\
        .parquet("s3://output/")

    # Clear cache if used
    spark.catalog.clearCache()'''
        }


class InteractiveCostOptimizer:
    """Interactive CLI for cost optimization with conversation support."""

    def __init__(self, use_llm: bool = False, model_id: str = None):
        self.use_llm = use_llm
        self.model_id = model_id or "us.anthropic.claude-sonnet-4-20250514-v1:0"
        self.orchestrator = CostOptimizationOrchestrator(use_llm, model_id)
        self.current_analysis = None
        self.conversation_history = []

        # LLM agent for conversation (lazy loaded)
        self._chat_agent = None

    def _get_chat_agent(self):
        """Get or create chat agent for conversation mode."""
        if self._chat_agent is None and self.use_llm:
            try:
                from strands import Agent
                from cost_optimizer.prompts.super_prompts import ORCHESTRATOR_PROMPT

                self._chat_agent = Agent(
                    model=self.model_id,
                    system_prompt=ORCHESTRATOR_PROMPT + """

You have access to the following analysis results from the cost optimization agents.
Use this data to answer questions and provide recommendations.

When the user asks about:
- Cost savings: Reference the resource_allocator findings
- Code issues: Reference the code_analyzer findings
- Data size: Reference the size_analyzer findings
- Recommendations: Reference the recommendations agent findings

Be specific with numbers, line references, and actionable advice.
"""
                )
            except ImportError:
                print("Warning: strands-agents not installed. Chat mode unavailable.")
                print("Install with: pip install strands-agents")
                return None
        return self._chat_agent

    def run_interactive(self):
        """Run interactive prompt-based input."""
        print("\n" + "="*70)
        print(" 🔍 PySpark Cost Optimization Analyzer")
        print("="*70)
        print(" Comprehensive cost and performance optimization for PySpark jobs")
        print(" (Press Ctrl+C to exit at any time)")
        print("="*70 + "\n")

        # Main menu
        print("   SELECT AN OPTION:")
        print("   1. Analyze PySpark script for cost optimization")
        print("   2. Scan S3 path for small files (performance issue)")
        print("   3. Optimize large historical table (billion+ rows)")
        print("   4. Exit")

        choice = input("\n   Select [1-4]: ").strip()

        if choice == '2':
            self._run_s3_small_files_scan()
            return
        elif choice == '3':
            self._run_large_table_analysis()
            return
        elif choice == '4':
            print("\n👋 Goodbye!")
            return
        elif choice != '1':
            print("   Invalid choice, defaulting to script analysis...")

        print("\n" + "-"*70)
        print(" 📄 PYSPARK SCRIPT ANALYSIS")
        print("-"*70 + "\n")

        # Collect inputs
        script_path = self._prompt_script_path()
        source_tables = self._prompt_source_tables()
        processing_mode = self._prompt_processing_mode()
        current_config = self._prompt_current_config()
        complexity_override = self._prompt_complexity_override()

        # Build additional context
        additional_context = {}
        if complexity_override:
            additional_context['complexity_override'] = complexity_override

        runs_per_day = self._prompt_int("Runs per day", default=1)
        additional_context['runs_per_day'] = runs_per_day
        additional_context['runs_per_year'] = runs_per_day * 365

        # Confirm and run
        print("\n" + "-"*70)
        print(" ANALYSIS SUMMARY")
        print("-"*70)
        print(f" Script:          {script_path}")
        print(f" Source Tables:   {len(source_tables)}")
        print(f" Processing Mode: {processing_mode}")
        print(f" Current Workers: {current_config.get('number_of_workers', 'N/A')}")
        print(f" Worker Type:     {current_config.get('worker_type', 'N/A')}")
        print(f" Complexity:      {complexity_override or 'Auto-detect from code'}")
        print("-"*70)

        confirm = input("\nProceed with analysis? [Y/n]: ").strip().lower()
        if confirm == 'n':
            print("Analysis cancelled.")
            return

        # Run analysis
        print("\n⏳ Running analysis...")
        result = self.orchestrator.analyze_script(
            script_path=script_path,
            source_tables=source_tables,
            processing_mode=processing_mode,
            current_config=current_config,
            additional_context=additional_context
        )

        self.current_analysis = result

        # Display results
        self._display_results(result)

        # Offer conversation mode
        if self.use_llm:
            self._offer_conversation_mode(result)
        else:
            self._offer_followup_questions(result)

    def _prompt_script_path(self) -> str:
        """Prompt for script path."""
        while True:
            path = input("📄 PySpark script path: ").strip()
            if Path(path).exists():
                return path
            print(f"   ❌ File not found: {path}")
            print("   Please enter a valid path.")

    def _prompt_source_tables(self) -> List[Dict]:
        """Prompt for source tables."""
        print("\n📊 SOURCE TABLES")
        print("   Enter details for each input table")
        print("   (Leave table name empty to finish)\n")

        tables = []
        table_num = 1

        while True:
            print(f"   --- Table {table_num} ---")
            table_name = input(f"   Table name (or Enter to finish): ").strip()

            if not table_name:
                if not tables:
                    print("   ⚠️  At least one table is required.")
                    continue
                break

            # Get record count
            record_count = self._prompt_int(f"   Record count for {table_name}", default=1000000)

            # Get column count
            column_count = self._prompt_int(f"   Column count", default=30)

            # Get format
            format_type = input(f"   Format [parquet/orc/delta/csv] (default: parquet): ").strip().lower()
            if not format_type:
                format_type = 'parquet'

            # Check if broadcast candidate
            is_small = record_count < 1000000
            broadcast = False
            if is_small:
                broadcast_input = input(f"   Use as broadcast join? [y/N]: ").strip().lower()
                broadcast = broadcast_input == 'y'

            # Check for skew
            has_skew = False
            skew_input = input(f"   Has data skew? [y/N]: ").strip().lower()
            has_skew = skew_input == 'y'

            table = {
                'table': table_name,
                'record_count': record_count,
                'column_count': column_count,
                'format': format_type,
                'broadcast': broadcast,
                'has_skew': has_skew
            }

            tables.append(table)
            table_num += 1
            print()

        return tables

    def _prompt_processing_mode(self) -> str:
        """Prompt for processing mode."""
        print("\n⚙️  PROCESSING MODE")
        print("   1. Full - Process entire dataset")
        print("   2. Delta - Process only new/changed records")

        while True:
            choice = input("   Select [1/2] (default: 1): ").strip()
            if choice == '' or choice == '1':
                return 'full'
            elif choice == '2':
                delta_ratio = self._prompt_float("   Delta ratio (0.01-0.5)", default=0.05)
                return 'delta'
            print("   Please enter 1 or 2")

    def _prompt_current_config(self) -> Dict:
        """Prompt for current job configuration."""
        print("\n🔧 CURRENT CONFIGURATION")
        print("   Enter your current job settings")

        platform = input("   Platform [glue/emr/eks] (default: glue): ").strip().lower()
        if not platform:
            platform = 'glue'

        workers = self._prompt_int("   Number of workers", default=10)

        print("   Worker types: G.1X (16GB), G.2X (32GB), G.4X (64GB), G.8X (128GB)")
        worker_type = input("   Worker type (default: G.2X): ").strip().upper()
        if not worker_type:
            worker_type = 'G.2X'

        timeout = self._prompt_int("   Timeout minutes", default=120)

        return {
            'platform': platform,
            'number_of_workers': workers,
            'worker_type': worker_type,
            'timeout_minutes': timeout
        }

    def _prompt_complexity_override(self) -> Optional[str]:
        """Prompt for optional complexity override."""
        print("\n📈 COMPLEXITY (Optional)")
        print("   1. Auto-detect from code analysis")
        print("   2. Low - Simple transformations, few joins")
        print("   3. Medium - Multiple joins, aggregations")
        print("   4. High - Complex joins, window functions, UDFs")
        print("   5. Very High - Multiple large joins, heavy shuffles")

        choice = input("   Select [1-5] (default: 1 = auto): ").strip()

        complexity_map = {
            '2': 'low',
            '3': 'medium',
            '4': 'high',
            '5': 'very_high'
        }

        return complexity_map.get(choice)

    def _prompt_int(self, prompt: str, default: int = 0) -> int:
        """Prompt for integer input."""
        while True:
            value = input(f"   {prompt} (default: {default}): ").strip()
            if not value:
                return default
            try:
                return int(value.replace(',', ''))
            except ValueError:
                print(f"   Please enter a valid number")

    def _prompt_float(self, prompt: str, default: float = 0.0) -> float:
        """Prompt for float input."""
        while True:
            value = input(f"   {prompt} (default: {default}): ").strip()
            if not value:
                return default
            try:
                return float(value)
            except ValueError:
                print(f"   Please enter a valid number")

    def _display_results(self, result: Dict):
        """Display analysis results."""
        if not result.get('success'):
            print(f"\n❌ Analysis failed: {result.get('error')}")
            return

        summary = result.get('summary', {})
        exec_summary = result.get('executive_summary', {})

        print("\n" + "="*70)
        print(" 📊 ANALYSIS RESULTS")
        print("="*70)

        print(f"\n {exec_summary.get('headline', 'Analysis complete')}")

        print("\n 💰 COST ANALYSIS")
        print(f"    Current Cost/Run:      ${summary.get('current_cost_per_run', 0):.2f}")
        print(f"    Optimal Cost/Run:      ${summary.get('optimal_cost_per_run', 0):.2f}")
        print(f"    Savings Potential:     {summary.get('potential_savings_percent', 0):.0f}%")
        print(f"    Annual Savings:        ${summary.get('potential_annual_savings', 0):,.0f}")

        print("\n 🔍 CODE QUALITY")
        print(f"    Anti-patterns Found:   {summary.get('anti_patterns_found', 0)}")
        print(f"    Critical Issues:       {summary.get('critical_issues', 0)}")
        print(f"    Recommendations:       {summary.get('total_recommendations', 0)}")
        print(f"    Quick Wins:            {summary.get('quick_wins', 0)}")

        print("\n 📈 DATA")
        print(f"    Effective Size:        {summary.get('effective_data_size_gb', 0):.1f} GB")

        # Top recommendations
        recommendations = result.get('all_recommendations', [])[:5]
        if recommendations:
            print("\n 🎯 TOP RECOMMENDATIONS")
            for i, rec in enumerate(recommendations, 1):
                priority = rec.get('priority', 'P3')
                title = rec.get('title', 'Unknown')
                quick = " ⚡" if rec.get('quick_win') else ""
                print(f"    {i}. [{priority}] {title}{quick}")

        print("\n" + "="*70)

    def _offer_conversation_mode(self, result: Dict):
        """Offer LLM conversation mode."""
        print("\n💬 CONVERSATION MODE")
        print("   You can now ask questions about the analysis.")
        print("   Type 'exit' or 'quit' to end the conversation.")
        print("   Type 'save' to save the analysis report.")
        print("-"*70)

        agent = self._get_chat_agent()
        if not agent:
            self._offer_followup_questions(result)
            return

        # Provide context to agent
        context_message = f"""
Here are the analysis results to reference:

SUMMARY:
- Effective Data Size: {result['summary']['effective_data_size_gb']:.1f} GB
- Current Cost/Run: ${result['summary']['current_cost_per_run']:.2f}
- Optimal Cost/Run: ${result['summary']['optimal_cost_per_run']:.2f}
- Potential Savings: {result['summary']['potential_savings_percent']:.0f}%
- Annual Savings: ${result['summary']['potential_annual_savings']:,.0f}
- Anti-patterns Found: {result['summary']['anti_patterns_found']}
- Critical Issues: {result['summary']['critical_issues']}

DETAILED ANALYSIS:
{json.dumps(result['agents'], indent=2, default=str)[:5000]}

Ready to answer questions about this analysis.
"""

        # Initialize conversation
        agent(context_message)

        while True:
            try:
                user_input = input("\n🧑 You: ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ('exit', 'quit', 'q'):
                    print("\n👋 Ending conversation. Goodbye!")
                    break

                if user_input.lower() == 'save':
                    self._save_report(result)
                    continue

                if user_input.lower() == 'help':
                    self._show_conversation_help()
                    continue

                # Get response from agent
                print("\n🤖 Agent: ", end="", flush=True)
                response = agent(user_input)
                print(response)

            except KeyboardInterrupt:
                print("\n\n👋 Conversation ended.")
                break

    def _offer_followup_questions(self, result: Dict):
        """Offer predefined followup questions (non-LLM mode)."""
        print("\n📝 FOLLOWUP OPTIONS")
        print("   1. Show all anti-patterns with line numbers")
        print("   2. Show recommended Spark configurations")
        print("   3. Show implementation roadmap")
        print("   4. Show platform comparison (Glue vs EMR vs EKS)")
        print("   5. Scan S3 path for small files")
        print("   6. Analyze large historical table optimization")
        print("   7. Save report to JSON")
        print("   8. Exit")

        while True:
            choice = input("\n   Select [1-8]: ").strip()

            if choice == '1':
                self._show_anti_patterns(result)
            elif choice == '2':
                self._show_spark_configs(result)
            elif choice == '3':
                self._show_roadmap(result)
            elif choice == '4':
                self._show_platform_comparison(result)
            elif choice == '5':
                self._run_s3_small_files_scan()
            elif choice == '6':
                self._run_large_table_analysis()
            elif choice == '7':
                self._save_report(result)
            elif choice == '8':
                print("\n👋 Goodbye!")
                break
            else:
                print("   Please select 1-8")

    def _run_s3_small_files_scan(self):
        """Run S3 small files scanner."""
        print("\n" + "="*70)
        print(" 🔍 S3 SMALL FILES SCANNER")
        print("="*70)
        print(" Scan S3 paths to identify small files (KB-sized) that impact performance")
        print("="*70 + "\n")

        if not HAS_BOTO3:
            print("   ❌ boto3 is required for S3 scanning.")
            print("   Install with: pip install boto3")
            return

        s3_path = input("   Enter S3 path (e.g., s3://bucket/prefix/): ").strip()
        if not s3_path:
            print("   ❌ S3 path is required")
            return

        if not s3_path.startswith('s3://'):
            print("   ❌ Invalid S3 path. Must start with s3://")
            return

        max_files = self._prompt_int("Maximum files to scan", default=10000)

        try:
            scanner = S3SmallFilesScanner()
            result = scanner.scan_path(s3_path, max_files=max_files)

            if not result.get('success'):
                print(f"\n   ❌ Scan failed: {result.get('error')}")
                return

            self._display_s3_scan_results(result)

        except Exception as e:
            print(f"\n   ❌ Error during scan: {e}")

    def _display_s3_scan_results(self, result: Dict):
        """Display S3 small files scan results."""
        print("\n" + "="*70)
        print(" 📊 S3 SMALL FILES ANALYSIS RESULTS")
        print("="*70)

        print(f"\n   📁 Path: {result['s3_path']}")
        print(f"   📄 Total Files: {result['total_files']:,}")
        print(f"   💾 Total Size: {result['total_size_gb']:.2f} GB")
        print(f"   📏 Average File Size: {result['average_file_size_mb']:.2f} MB")

        # Severity indicator
        severity = result.get('severity', 'low')
        severity_icons = {'critical': '🔴', 'high': '🟠', 'medium': '🟡', 'low': '🟢'}
        print(f"\n   {severity_icons.get(severity, '⚪')} Severity: {severity.upper()}")

        # Small files summary
        print(f"\n   ⚠️  SMALL FILES SUMMARY")
        print(f"   Files in KB range (< 1MB): {result['kb_files_count']:,} ({result['kb_files_percent']:.1f}%)")
        print(f"   Files < 1MB total:         {result['small_files_count']:,} ({result['small_files_percent']:.1f}%)")

        # Size distribution
        print(f"\n   📊 SIZE DISTRIBUTION")
        dist = result['size_distribution']
        total = result['total_files']
        for category, count in dist.items():
            if count > 0:
                bar_len = int((count / total) * 40)
                bar = "█" * bar_len
                pct = (count / total) * 100
                label = category.replace('_', ' ').title()
                print(f"   {label:25} {count:8,} ({pct:5.1f}%) {bar}")

        # File formats
        print(f"\n   📋 FILE FORMATS")
        for fmt, count in result['file_formats'].items():
            print(f"   {fmt}: {count:,}")

        # Sample small files
        if result.get('sample_small_files'):
            print(f"\n   📝 SAMPLE SMALL FILES (KB-sized)")
            for f in result['sample_small_files'][:10]:
                key_short = f['key'][-60:] if len(f['key']) > 60 else f['key']
                print(f"   {f['size_kb']:8.1f} KB  ...{key_short}")

        # Recommendations
        print("\n" + "-"*70)
        print(" 🎯 RECOMMENDATIONS")
        print("-"*70)

        for rec in result.get('recommendations', []):
            print(f"\n   [{rec['priority']}] {rec['title']}")
            print(f"   {rec['description']}")

            if rec.get('impact'):
                print(f"   Impact: {rec['impact']}")

            if rec.get('current_state'):
                print(f"   Current: {rec['current_state']}")
                print(f"   Target: {rec['target_state']}")

            # Show solutions
            if rec.get('solutions'):
                print(f"\n   Solutions:")
                for sol in rec['solutions'][:3]:
                    print(f"\n   📌 {sol['name']}")
                    print(f"   When: {sol['when_to_use']}")
                    print(f"   Code:")
                    for line in sol['code'].split('\n'):
                        print(f"      {line}")

            # Show spark configs
            if rec.get('spark_configs'):
                print(f"\n   Spark Configurations:")
                for config, value in rec['spark_configs'].items():
                    print(f"      {config} = {value}")

        print("\n" + "="*70)

    def _run_large_table_analysis(self):
        """Run large historical table optimization analysis."""
        print("\n" + "="*70)
        print(" 📊 LARGE HISTORICAL TABLE OPTIMIZER")
        print("="*70)
        print(" Analyze billion-row tables with wide columns for optimal processing")
        print("="*70 + "\n")

        # Collect table information
        table_name = input("   Table name (e.g., db.historical_transactions): ").strip()
        if not table_name:
            table_name = "historical_table"

        row_count = self._prompt_int("Number of rows (e.g., 1600000000 for 1.6B)", default=1_600_000_000)
        column_count = self._prompt_int("Number of columns", default=150)

        print("\n   Partition columns (comma-separated, or Enter for none):")
        partition_input = input("   ").strip()
        partition_columns = [p.strip() for p in partition_input.split(',')] if partition_input else None

        print("\n   Common query patterns:")
        print("   1. Point lookups (WHERE id = ...)")
        print("   2. Range queries (WHERE date BETWEEN ...)")
        print("   3. Aggregations (GROUP BY ...)")
        print("   4. Full table joins")
        query_choice = input("   Select patterns [1,2,3,4] (comma-separated): ").strip()

        query_patterns = []
        pattern_map = {
            '1': 'point_lookup',
            '2': 'range_query',
            '3': 'aggregation',
            '4': 'full_join'
        }
        for c in query_choice.split(','):
            if c.strip() in pattern_map:
                query_patterns.append(pattern_map[c.strip()])

        # Run analysis
        print("\n   ⏳ Analyzing large table optimization strategies...")

        optimizer = LargeTableOptimizer()
        result = optimizer.analyze_large_table_scenario(
            table_name=table_name,
            row_count=row_count,
            column_count=column_count,
            partition_columns=partition_columns,
            query_patterns=query_patterns
        )

        self._display_large_table_results(result)

    def _display_large_table_results(self, result: Dict):
        """Display large table optimization results."""
        print("\n" + "="*70)
        print(" 📊 LARGE TABLE OPTIMIZATION ANALYSIS")
        print("="*70)

        info = result['table_info']
        classification = result['classification']

        print(f"\n   📋 TABLE INFORMATION")
        print(f"   Name:         {info['name']}")
        print(f"   Rows:         {info['row_count_formatted']} ({info['row_count']:,})")
        print(f"   Columns:      {info['column_count']}")
        print(f"   Est. Size:    {info['estimated_size_gb']:.1f} GB ({info['estimated_size_tb']:.2f} TB)")
        print(f"   Wide Table:   {'Yes' if info['is_wide_table'] else 'No'}")
        print(f"   Billion+:     {'Yes' if info['is_billion_scale'] else 'No'}")

        print(f"\n   🏷️  CLASSIFICATION")
        print(f"   Type:         {', '.join(classification['classifications'])}")
        print(f"   Complexity:   {classification['complexity'].upper()}")
        print(f"   Description:  {classification['description']}")

        # Optimal configuration
        config = result['optimal_config']
        print(f"\n   ⚙️  RECOMMENDED CONFIGURATION")
        print(f"   Partitions:       {config['partitions']:,}")
        print(f"   Worker Type:      {config['worker_type']}")
        print(f"   Workers:          {config['recommended_workers']}")
        print(f"   Executor Memory:  {config['memory_per_executor']}")
        print(f"   Executor Cores:   {config['executor_cores']}")

        # Strategies
        print("\n" + "-"*70)
        print(" 🎯 OPTIMIZATION STRATEGIES")
        print("-"*70)

        for strategy in result['strategies']:
            print(f"\n   [{strategy['priority']}] {strategy['name']}")
            print(f"   {strategy['description']}")
            print(f"   Impact: {strategy['impact']}")
            print(f"\n   Implementation:")
            for line in strategy['implementation'].split('\n'):
                print(f"   {line}")

        # Anti-patterns
        print("\n" + "-"*70)
        print(" ⚠️  ANTI-PATTERNS TO AVOID")
        print("-"*70)

        for ap in result['anti_patterns_to_avoid']:
            print(f"\n   ❌ {ap['pattern']}")
            print(f"      Issue: {ap['issue']}")
            print(f"      Fix: {ap['fix']}")

        # Spark configurations
        print("\n" + "-"*70)
        print(" 🔧 SPARK CONFIGURATIONS")
        print("-"*70)

        for category, configs in result['spark_configs'].items():
            print(f"\n   {category.replace('_', ' ').title()}:")
            for config, value in configs.items():
                print(f"      {config} = {value}")

        # Implementation examples
        print("\n" + "-"*70)
        print(" 📝 IMPLEMENTATION EXAMPLES")
        print("-"*70)

        examples = result['implementation_examples']

        print("\n   OPTIMAL READ PATTERN:")
        for line in examples['optimal_read_pattern'].split('\n'):
            print(f"   {line}")

        print("\n   INCREMENTAL PROCESSING:")
        for line in examples['incremental_pattern'].split('\n'):
            print(f"   {line}")

        print("\n   CHUNKED PROCESSING (for very large operations):")
        for line in examples['chunked_processing'].split('\n'):
            print(f"   {line}")

        print("\n" + "="*70)

    def _show_anti_patterns(self, result: Dict):
        """Show anti-patterns with details."""
        code_analysis = result.get('agents', {}).get('code_analyzer', {}).get('analysis', {})
        anti_patterns = code_analysis.get('anti_patterns', [])

        if not anti_patterns:
            print("\n   ✅ No anti-patterns detected!")
            return

        print(f"\n   ⚠️  ANTI-PATTERNS ({len(anti_patterns)} found)")
        print("   " + "-"*60)

        for pattern in anti_patterns:
            severity = pattern.get('severity', 'unknown').upper()
            name = pattern.get('pattern', 'unknown')
            lines = pattern.get('line_numbers', [])

            print(f"\n   [{severity}] {name}")
            print(f"   Lines: {lines}")
            print(f"   Issue: {pattern.get('description', '')}")
            print(f"   Fix: {pattern.get('fix', '')}")

    def _show_spark_configs(self, result: Dict):
        """Show recommended Spark configurations."""
        code_analysis = result.get('agents', {}).get('code_analyzer', {}).get('analysis', {})
        configs = code_analysis.get('spark_configs', [])

        print("\n   🔧 RECOMMENDED SPARK CONFIGURATIONS")
        print("   " + "-"*60)

        for config in configs:
            print(f"\n   {config.get('config')}")
            print(f"   Current:     {config.get('current_value')}")
            print(f"   Recommended: {config.get('recommended_value')}")
            print(f"   Reason:      {config.get('reason')}")

    def _show_roadmap(self, result: Dict):
        """Show implementation roadmap."""
        roadmap = result.get('implementation_roadmap', {})

        print("\n   📋 IMPLEMENTATION ROADMAP")
        print("   " + "-"*60)

        for phase_key in ['phase_1', 'phase_2', 'phase_3', 'phase_4']:
            phase = roadmap.get(phase_key, {})
            if phase.get('actions'):
                print(f"\n   {phase.get('name', phase_key)}")
                print(f"   Duration: {phase.get('duration')}")
                print(f"   Expected Savings: {phase.get('expected_savings_percent', 0):.0f}%")
                print("   Actions:")
                for action in phase.get('actions', []):
                    print(f"     • {action}")

    def _show_platform_comparison(self, result: Dict):
        """Show platform cost comparison."""
        resource = result.get('agents', {}).get('resource_allocator', {}).get('analysis', {})
        platforms = resource.get('platform_comparison', [])

        print("\n   💵 PLATFORM COST COMPARISON")
        print("   " + "-"*60)
        print(f"   {'Platform':<20} {'Cost/Run':<12} {'Annual':<15} {'Savings'}")
        print("   " + "-"*60)

        for p in platforms:
            name = p.get('platform', 'unknown')
            cost = p.get('cost_per_run', 0)
            annual = p.get('annual_cost', 0)
            savings = p.get('savings_vs_current_percent', 0)
            print(f"   {name:<20} ${cost:<11.2f} ${annual:<14,.0f} {savings:.0f}%")

    def _save_report(self, result: Dict):
        """Save report to JSON file."""
        default_name = f"cost_analysis_{result.get('job_name', 'report')}.json"
        filename = input(f"   Filename (default: {default_name}): ").strip()
        if not filename:
            filename = default_name

        output_path = Path('cost_optimizer/reports') / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)

        print(f"   ✅ Report saved to: {output_path}")

    def _show_conversation_help(self):
        """Show conversation help."""
        print("""
   💡 CONVERSATION TIPS

   Ask questions like:
   • "What are the most impactful changes I can make?"
   • "Explain the anti-pattern on line 45"
   • "How much would I save by switching to EKS?"
   • "What Spark configs should I change?"
   • "How do I implement key salting for the skewed join?"
   • "Compare my options for reducing shuffle"
   • "What's the ROI if I fix just the UDF issues?"

   Commands:
   • 'save' - Save report to JSON
   • 'help' - Show this help
   • 'exit' - End conversation
""")

    def run_from_json(self, json_path: str):
        """Run analysis from JSON input file."""
        with open(json_path) as f:
            input_data = json.load(f)

        print(f"\n📄 Loading analysis request from: {json_path}")

        # Extract inputs
        script_path = input_data.get('script_path')
        if not script_path:
            print("Error: script_path is required in JSON")
            return

        source_tables = input_data.get('source_tables', [])
        if not source_tables:
            print("Error: source_tables is required in JSON")
            return

        processing_mode = input_data.get('processing_mode', 'full')
        current_config = input_data.get('current_config', {})
        additional_context = input_data.get('additional_context', {})

        # Run analysis
        print("\n⏳ Running analysis...")
        result = self.orchestrator.analyze_script(
            script_path=script_path,
            source_tables=source_tables,
            processing_mode=processing_mode,
            current_config=current_config,
            additional_context=additional_context
        )

        self.current_analysis = result
        self._display_results(result)

        # Offer conversation or followup
        if self.use_llm:
            self._offer_conversation_mode(result)
        else:
            self._offer_followup_questions(result)


def run_s3_scan_standalone(s3_path: str, max_files: int = 10000, output_json: str = None):
    """Run S3 small files scan as standalone operation."""
    print("\n" + "="*70)
    print(" 🔍 S3 SMALL FILES SCANNER")
    print("="*70)

    if not HAS_BOTO3:
        print("   ❌ boto3 is required for S3 scanning.")
        print("   Install with: pip install boto3")
        return

    try:
        scanner = S3SmallFilesScanner()
        result = scanner.scan_path(s3_path, max_files=max_files)

        if not result.get('success'):
            print(f"\n   ❌ Scan failed: {result.get('error')}")
            return

        # Display results
        optimizer = InteractiveCostOptimizer()
        optimizer._display_s3_scan_results(result)

        # Save to JSON if requested
        if output_json:
            with open(output_json, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"\n   ✅ Results saved to: {output_json}")

    except Exception as e:
        print(f"\n   ❌ Error during scan: {e}")


def run_large_table_standalone(
    table_name: str,
    row_count: int,
    column_count: int,
    partition_columns: List[str] = None,
    output_json: str = None
):
    """Run large table analysis as standalone operation."""
    print("\n" + "="*70)
    print(" 📊 LARGE HISTORICAL TABLE OPTIMIZER")
    print("="*70)

    optimizer_obj = LargeTableOptimizer()
    result = optimizer_obj.analyze_large_table_scenario(
        table_name=table_name,
        row_count=row_count,
        column_count=column_count,
        partition_columns=partition_columns
    )

    # Display results
    interactive = InteractiveCostOptimizer()
    interactive._display_large_table_results(result)

    # Save to JSON if requested
    if output_json:
        with open(output_json, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n   ✅ Results saved to: {output_json}")


def main():
    parser = argparse.ArgumentParser(
        description='Interactive PySpark Cost Optimizer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python scripts/optimize_costs_interactive.py

  # With LLM conversation
  python scripts/optimize_costs_interactive.py --chat

  # Scan S3 for small files
  python scripts/optimize_costs_interactive.py --scan-s3 s3://bucket/prefix/

  # Analyze large table
  python scripts/optimize_costs_interactive.py --large-table my_db.table --rows 1600000000 --cols 150

  # From JSON input
  python scripts/optimize_costs_interactive.py --input analysis_request.json
"""
    )
    parser.add_argument('--script', '-s', help='Initial script to analyze')
    parser.add_argument('--input', '-i', help='JSON input file with analysis request')
    parser.add_argument('--chat', action='store_true', help='Enable LLM conversation mode')
    parser.add_argument('--model', default='us.anthropic.claude-sonnet-4-20250514-v1:0',
                        help='Bedrock model ID for chat mode')

    # S3 small files scanner
    parser.add_argument('--scan-s3', metavar='S3_PATH',
                        help='Scan S3 path for small files (e.g., s3://bucket/prefix/)')
    parser.add_argument('--max-files', type=int, default=10000,
                        help='Maximum files to scan (default: 10000)')

    # Large table analyzer
    parser.add_argument('--large-table', metavar='TABLE_NAME',
                        help='Analyze optimization for large historical table')
    parser.add_argument('--rows', type=int, default=1_600_000_000,
                        help='Number of rows (default: 1.6 billion)')
    parser.add_argument('--cols', type=int, default=150,
                        help='Number of columns (default: 150)')
    parser.add_argument('--partitions', nargs='+',
                        help='Partition columns (space-separated)')

    # Output
    parser.add_argument('--output', '-o', help='Output JSON file for results')

    args = parser.parse_args()

    try:
        # S3 scan mode
        if args.scan_s3:
            run_s3_scan_standalone(
                s3_path=args.scan_s3,
                max_files=args.max_files,
                output_json=args.output
            )
            return

        # Large table mode
        if args.large_table:
            run_large_table_standalone(
                table_name=args.large_table,
                row_count=args.rows,
                column_count=args.cols,
                partition_columns=args.partitions,
                output_json=args.output
            )
            return

        # Create optimizer
        optimizer = InteractiveCostOptimizer(
            use_llm=args.chat,
            model_id=args.model
        )

        if args.input:
            # Run from JSON
            optimizer.run_from_json(args.input)
        else:
            # Run interactive mode
            optimizer.run_interactive()

    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        sys.exit(0)


if __name__ == '__main__':
    main()
