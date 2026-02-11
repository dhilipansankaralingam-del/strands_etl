#!/usr/bin/env python3
"""
Source Size Detector
====================

Automatically detects source table sizes for intelligent resource allocation.

Features:
1. Auto-detect from Glue Catalog (table statistics)
2. Auto-detect from S3 (actual file sizes)
3. Delta mode: detect only today's partition size
4. Weekend/weekday awareness
5. Caching with TTL
6. Complexity-based adjustment

Usage:
    detector = SourceSizeDetector()

    # Get sizes for all source tables
    sizes = detector.detect_all_sizes(config)

    # Get total size for resource calculation
    total_gb = detector.get_total_source_size(config)

    # Get recommended platform based on size
    platform = detector.recommend_platform(total_gb)
"""

import os
import sys
import json
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

# Add framework to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@dataclass
class TableSize:
    """Size information for a source table."""
    database: str
    table: str
    full_name: str
    total_size_bytes: int
    total_size_gb: float
    partition_size_bytes: Optional[int] = None
    partition_size_gb: Optional[float] = None
    row_count: Optional[int] = None
    partition_count: Optional[int] = None
    last_updated: Optional[str] = None
    detection_method: str = "unknown"  # glue_catalog, s3_scan, hardcoded, cached
    is_estimate: bool = False


@dataclass
class SizingResult:
    """Result of source sizing analysis."""
    tables: List[TableSize]
    total_size_gb: float
    delta_size_gb: float  # Size for incremental/delta run
    is_weekend: bool
    is_month_end: bool
    recommended_workers: int
    recommended_worker_type: str
    recommended_platform: str
    sizing_details: Dict[str, Any]
    warnings: List[str]


class SourceSizeDetector:
    """
    Detects source table sizes from multiple sources.
    """

    # Size thresholds for platform recommendation
    GLUE_MAX_EFFICIENT_GB = 100      # Glue efficient up to 100 GB
    EMR_MIN_GB = 50                   # Consider EMR above 50 GB
    EMR_MAX_EFFICIENT_GB = 500        # EMR efficient up to 500 GB
    EKS_MIN_GB = 200                  # Consider EKS above 200 GB

    # Worker sizing constants
    GB_PER_WORKER_G1X = 10           # G.1X can handle ~10 GB efficiently
    GB_PER_WORKER_G2X = 25           # G.2X can handle ~25 GB efficiently
    GB_PER_WORKER_G4X = 50           # G.4X can handle ~50 GB efficiently

    # Weekend scale factor (less data on weekends)
    WEEKEND_SCALE = 0.6
    MONTH_END_SCALE = 1.5
    QUARTER_END_SCALE = 2.0

    def __init__(self, config: Dict = None, cache_path: str = None):
        self.config = config or {}
        self.cache_path = cache_path or "data/agent_store/source_sizes.json"
        self._ensure_cache()

        # AWS clients (lazy initialization)
        self._glue_client = None
        self._s3_client = None

    def _ensure_cache(self):
        """Ensure cache directory and file exist."""
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        if not os.path.exists(self.cache_path):
            with open(self.cache_path, 'w') as f:
                json.dump({"sizes": {}, "last_updated": {}}, f)

    @property
    def glue_client(self):
        if self._glue_client is None:
            self._glue_client = boto3.client('glue')
        return self._glue_client

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')
        return self._s3_client

    def detect_all_sizes(
        self,
        config: Dict,
        run_date: Optional[datetime] = None,
        delta_mode: bool = False
    ) -> SizingResult:
        """
        Detect sizes for all source tables in config.

        Args:
            config: ETL configuration with source_tables
            run_date: Date of the run (for weekend/month-end detection)
            delta_mode: If True, detect only partition/delta size

        Returns:
            SizingResult with all table sizes and recommendations
        """
        run_date = run_date or datetime.now()
        source_tables = config.get('source_tables', [])
        sizing_config = config.get('source_sizing', {})

        # Determine sizing mode
        mode = sizing_config.get('mode', 'hybrid')  # auto_detect, hardcoded, hybrid

        # Detect day type
        is_weekend = run_date.weekday() >= 5
        is_month_end = run_date.day >= 28
        is_quarter_end = run_date.month in [3, 6, 9, 12] and run_date.day >= 28

        tables = []
        warnings = []
        total_size_gb = 0
        delta_size_gb = 0

        for table_config in source_tables:
            database = table_config.get('database', '')
            table = table_config.get('table', '')
            full_name = f"{database}.{table}"

            # Determine how to get size
            auto_detect = table_config.get('auto_detect_size', mode in ['auto_detect', 'hybrid'])
            hardcoded_size = table_config.get('estimated_size_gb')

            table_size = None

            # Try to get cached size first
            if sizing_config.get('cache_sizes', True):
                cached = self._get_cached_size(full_name)
                if cached:
                    table_size = cached

            # Auto-detect if needed
            if table_size is None and auto_detect:
                try:
                    table_size = self._detect_table_size(
                        database, table,
                        table_config.get('location'),
                        table_config.get('partition_column'),
                        run_date if delta_mode else None
                    )
                except Exception as e:
                    warnings.append(f"Auto-detect failed for {full_name}: {str(e)}")

            # Fall back to hardcoded
            if table_size is None and hardcoded_size is not None:
                table_size = TableSize(
                    database=database,
                    table=table,
                    full_name=full_name,
                    total_size_bytes=int(hardcoded_size * 1024 * 1024 * 1024),
                    total_size_gb=hardcoded_size,
                    detection_method="hardcoded",
                    is_estimate=True
                )

            # Fall back to estimate
            if table_size is None:
                # Estimate based on table type
                if table_config.get('is_dimension') or table_config.get('broadcast'):
                    estimated_gb = 0.1  # Small dimension table
                elif table_config.get('high_volume'):
                    estimated_gb = 50  # Large fact table
                else:
                    estimated_gb = 5  # Medium table

                table_size = TableSize(
                    database=database,
                    table=table,
                    full_name=full_name,
                    total_size_bytes=int(estimated_gb * 1024 * 1024 * 1024),
                    total_size_gb=estimated_gb,
                    detection_method="estimate",
                    is_estimate=True
                )
                warnings.append(f"Using estimate for {full_name}: {estimated_gb} GB")

            # Cache the size
            if sizing_config.get('cache_sizes', True):
                self._cache_size(full_name, table_size)

            tables.append(table_size)

            # Calculate totals
            total_size_gb += table_size.total_size_gb

            # Delta size (partition or scaled)
            if delta_mode and table_size.partition_size_gb:
                delta_size_gb += table_size.partition_size_gb
            elif delta_mode:
                # Estimate delta as 1/30th of total (one day's worth)
                delta_size_gb += table_size.total_size_gb / 30
            else:
                delta_size_gb = total_size_gb

        # Apply day-type scaling
        effective_size = delta_size_gb if delta_mode else total_size_gb

        if is_weekend:
            effective_size *= self.WEEKEND_SCALE
        if is_quarter_end:
            effective_size *= self.QUARTER_END_SCALE
        elif is_month_end:
            effective_size *= self.MONTH_END_SCALE

        # Calculate recommendations
        workers, worker_type = self._recommend_workers(effective_size, config)
        platform = self._recommend_platform(effective_size, config)

        return SizingResult(
            tables=tables,
            total_size_gb=total_size_gb,
            delta_size_gb=delta_size_gb,
            is_weekend=is_weekend,
            is_month_end=is_month_end,
            recommended_workers=workers,
            recommended_worker_type=worker_type,
            recommended_platform=platform,
            sizing_details={
                "effective_size_gb": effective_size,
                "scale_factor": self.WEEKEND_SCALE if is_weekend else (
                    self.QUARTER_END_SCALE if is_quarter_end else (
                        self.MONTH_END_SCALE if is_month_end else 1.0
                    )
                ),
                "tables_detected": len(tables),
                "tables_estimated": sum(1 for t in tables if t.is_estimate)
            },
            warnings=warnings
        )

    def _detect_table_size(
        self,
        database: str,
        table: str,
        s3_location: Optional[str] = None,
        partition_column: Optional[str] = None,
        partition_date: Optional[datetime] = None
    ) -> Optional[TableSize]:
        """
        Detect table size from Glue Catalog or S3.
        """
        full_name = f"{database}.{table}"
        total_bytes = 0
        partition_bytes = None
        row_count = None
        partition_count = None
        method = "unknown"

        try:
            # Try Glue Catalog first
            response = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )

            glue_table = response.get('Table', {})
            storage_descriptor = glue_table.get('StorageDescriptor', {})
            parameters = glue_table.get('Parameters', {})

            # Get size from table parameters
            if 'sizeInBytes' in parameters:
                total_bytes = int(parameters['sizeInBytes'])
                method = "glue_catalog"

            if 'recordCount' in parameters:
                row_count = int(parameters['recordCount'])

            # Get location for S3 scan if needed
            if not s3_location:
                s3_location = storage_descriptor.get('Location', '')

            # Try to get partition info
            try:
                partitions_response = self.glue_client.get_partitions(
                    DatabaseName=database,
                    TableName=table,
                    MaxResults=1000
                )
                partition_count = len(partitions_response.get('Partitions', []))

                # If partition date specified, get that partition's size
                if partition_date and partition_column:
                    date_str = partition_date.strftime('%Y-%m-%d')
                    for partition in partitions_response.get('Partitions', []):
                        values = partition.get('Values', [])
                        if date_str in values:
                            part_params = partition.get('Parameters', {})
                            if 'sizeInBytes' in part_params:
                                partition_bytes = int(part_params['sizeInBytes'])
                            break

            except Exception:
                pass  # Partitions not available

        except Exception as e:
            # Glue catalog failed, try S3 scan
            if s3_location:
                total_bytes, method = self._scan_s3_size(s3_location)

        # If still no size, return None
        if total_bytes == 0:
            return None

        total_gb = total_bytes / (1024 * 1024 * 1024)
        partition_gb = partition_bytes / (1024 * 1024 * 1024) if partition_bytes else None

        return TableSize(
            database=database,
            table=table,
            full_name=full_name,
            total_size_bytes=total_bytes,
            total_size_gb=total_gb,
            partition_size_bytes=partition_bytes,
            partition_size_gb=partition_gb,
            row_count=row_count,
            partition_count=partition_count,
            last_updated=datetime.now().isoformat(),
            detection_method=method,
            is_estimate=False
        )

    def _scan_s3_size(self, s3_location: str) -> Tuple[int, str]:
        """Scan S3 to get total size of all objects."""
        if not s3_location.startswith('s3://'):
            return 0, "unknown"

        # Parse S3 path
        path = s3_location.replace('s3://', '')
        bucket = path.split('/')[0]
        prefix = '/'.join(path.split('/')[1:])

        total_bytes = 0
        paginator = self.s3_client.get_paginator('list_objects_v2')

        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    total_bytes += obj.get('Size', 0)
        except Exception:
            return 0, "unknown"

        return total_bytes, "s3_scan"

    def _recommend_workers(
        self,
        effective_size_gb: float,
        config: Dict
    ) -> Tuple[int, str]:
        """Recommend worker count and type based on data size."""

        # Get complexity multiplier if available
        complexity = config.get('complexity_analysis', {})
        multiplier = complexity.get('complexity_multiplier', 1.0)

        adjusted_size = effective_size_gb * multiplier

        # Determine worker type
        if adjusted_size > 200:
            worker_type = "G.4X"
            gb_per_worker = self.GB_PER_WORKER_G4X
        elif adjusted_size > 50:
            worker_type = "G.2X"
            gb_per_worker = self.GB_PER_WORKER_G2X
        else:
            worker_type = "G.1X"
            gb_per_worker = self.GB_PER_WORKER_G1X

        # Calculate workers
        workers = max(2, int(adjusted_size / gb_per_worker) + 1)

        # Apply limits
        workers = min(workers, 100)

        return workers, worker_type

    def _recommend_platform(
        self,
        effective_size_gb: float,
        config: Dict
    ) -> str:
        """Recommend platform based on data size."""

        auto_convert = config.get('platform', {}).get('auto_convert', {})

        if auto_convert.get('enabled'):
            emr_threshold = auto_convert.get('convert_to_emr_threshold_gb', self.GLUE_MAX_EFFICIENT_GB)
            eks_threshold = auto_convert.get('convert_to_eks_threshold_gb', self.EMR_MAX_EFFICIENT_GB)

            if effective_size_gb > eks_threshold:
                return "eks"
            elif effective_size_gb > emr_threshold:
                return "emr"

        # Default thresholds
        if effective_size_gb > self.EMR_MAX_EFFICIENT_GB:
            return "eks"
        elif effective_size_gb > self.GLUE_MAX_EFFICIENT_GB:
            return "emr"

        return "glue"

    def _get_cached_size(self, full_name: str) -> Optional[TableSize]:
        """Get cached size if valid."""
        try:
            with open(self.cache_path, 'r') as f:
                cache = json.load(f)

            if full_name in cache.get('sizes', {}):
                cached = cache['sizes'][full_name]
                last_updated = cache.get('last_updated', {}).get(full_name)

                # Check TTL (default 24 hours)
                if last_updated:
                    updated_time = datetime.fromisoformat(last_updated)
                    ttl_hours = self.config.get('source_sizing', {}).get('cache_ttl_hours', 24)
                    if datetime.now() - updated_time < timedelta(hours=ttl_hours):
                        return TableSize(
                            database=cached['database'],
                            table=cached['table'],
                            full_name=full_name,
                            total_size_bytes=cached['total_size_bytes'],
                            total_size_gb=cached['total_size_gb'],
                            partition_size_bytes=cached.get('partition_size_bytes'),
                            partition_size_gb=cached.get('partition_size_gb'),
                            row_count=cached.get('row_count'),
                            partition_count=cached.get('partition_count'),
                            last_updated=last_updated,
                            detection_method="cached",
                            is_estimate=cached.get('is_estimate', False)
                        )
        except Exception:
            pass

        return None

    def _cache_size(self, full_name: str, table_size: TableSize):
        """Cache table size."""
        try:
            with open(self.cache_path, 'r') as f:
                cache = json.load(f)
        except Exception:
            cache = {"sizes": {}, "last_updated": {}}

        cache['sizes'][full_name] = {
            "database": table_size.database,
            "table": table_size.table,
            "total_size_bytes": table_size.total_size_bytes,
            "total_size_gb": table_size.total_size_gb,
            "partition_size_bytes": table_size.partition_size_bytes,
            "partition_size_gb": table_size.partition_size_gb,
            "row_count": table_size.row_count,
            "partition_count": table_size.partition_count,
            "detection_method": table_size.detection_method,
            "is_estimate": table_size.is_estimate
        }
        cache['last_updated'][full_name] = datetime.now().isoformat()

        with open(self.cache_path, 'w') as f:
            json.dump(cache, f, indent=2)

    def get_total_source_size(self, config: Dict, delta_mode: bool = False) -> float:
        """Get total source size in GB."""
        result = self.detect_all_sizes(config, delta_mode=delta_mode)
        return result.delta_size_gb if delta_mode else result.total_size_gb

    def generate_sizing_report(self, result: SizingResult) -> str:
        """Generate human-readable sizing report."""
        report = []
        report.append("=" * 70)
        report.append("SOURCE SIZE ANALYSIS REPORT")
        report.append("=" * 70)

        report.append(f"\nTotal Tables: {len(result.tables)}")
        report.append(f"Total Size: {result.total_size_gb:.2f} GB")
        report.append(f"Delta Size: {result.delta_size_gb:.2f} GB")

        report.append(f"\nDay Type:")
        report.append(f"  Weekend: {'Yes' if result.is_weekend else 'No'}")
        report.append(f"  Month End: {'Yes' if result.is_month_end else 'No'}")

        report.append(f"\nEffective Size: {result.sizing_details['effective_size_gb']:.2f} GB")
        report.append(f"Scale Factor: {result.sizing_details['scale_factor']:.2f}")

        report.append(f"\nRecommendations:")
        report.append(f"  Platform: {result.recommended_platform.upper()}")
        report.append(f"  Workers: {result.recommended_workers} x {result.recommended_worker_type}")

        report.append(f"\nTable Details:")
        report.append(f"  {'Table':<50} {'Size (GB)':<12} {'Method'}")
        report.append(f"  {'-'*50} {'-'*12} {'-'*15}")

        for table in sorted(result.tables, key=lambda t: t.total_size_gb, reverse=True)[:10]:
            estimate_flag = "*" if table.is_estimate else ""
            report.append(f"  {table.full_name:<50} {table.total_size_gb:<12.2f} {table.detection_method}{estimate_flag}")

        if len(result.tables) > 10:
            report.append(f"  ... and {len(result.tables) - 10} more tables")

        if result.warnings:
            report.append(f"\nWarnings:")
            for warning in result.warnings:
                report.append(f"  ⚠ {warning}")

        report.append("\n" + "=" * 70)

        return "\n".join(report)
