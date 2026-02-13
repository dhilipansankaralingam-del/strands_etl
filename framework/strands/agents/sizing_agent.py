#!/usr/bin/env python3
"""
Strands Sizing Agent
====================

Detects actual source table sizes from:
1. AWS Glue Data Catalog (sizeInBytes, recordCount)
2. S3 list_objects_v2 scan
3. Config fallback (estimated_size_gb)

Provides accurate sizing for resource allocation and platform decisions.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import json

from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..tools import tool
from ..storage import StrandsStorage


@dataclass
class TableSizeInfo:
    """Size information for a table."""
    database: str
    table: str
    size_bytes: int
    size_gb: float
    row_count: Optional[int]
    detection_method: str  # "glue_catalog", "s3_scan", "config_fallback", "estimate"
    partition_count: Optional[int] = None
    last_modified: Optional[datetime] = None
    s3_location: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'database': self.database,
            'table': self.table,
            'size_bytes': self.size_bytes,
            'size_gb': self.size_gb,
            'row_count': self.row_count,
            'detection_method': self.detection_method,
            'partition_count': self.partition_count,
            'last_modified': self.last_modified.isoformat() if self.last_modified else None,
            's3_location': self.s3_location
        }


@register_agent
class SizingAgent(StrandsAgent):
    """
    Agent that detects actual sizes of source tables.

    Provides accurate sizing for:
    - Resource allocation decisions
    - Platform conversion thresholds
    - Cost estimation
    """

    AGENT_NAME = "sizing_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Detects actual source table sizes from Glue Catalog or S3"

    DEPENDENCIES = []  # No dependencies - runs first
    PARALLEL_SAFE = True

    # Size estimation defaults
    DEFAULT_BYTES_PER_ROW = 500
    DEFAULT_DIMENSION_TABLE_GB = 0.1
    DEFAULT_FACT_TABLE_GB = 50.0

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.logger = logging.getLogger("strands.agents.sizing")
        self._glue_client = None
        self._s3_client = None
        self._size_cache: Dict[str, TableSizeInfo] = {}
        self.storage = StrandsStorage(config)

    @property
    def glue_client(self):
        if self._glue_client is None:
            try:
                import boto3
                self._glue_client = boto3.client('glue')
            except Exception as e:
                self.logger.warning(f"Could not create Glue client: {e}")
        return self._glue_client

    @property
    def s3_client(self):
        if self._s3_client is None:
            try:
                import boto3
                self._s3_client = boto3.client('s3')
            except Exception as e:
                self.logger.warning(f"Could not create S3 client: {e}")
        return self._s3_client

    def execute(self, context: AgentContext) -> AgentResult:
        """Execute sizing detection for all source tables."""
        sizing_config = context.config.get('source_sizing', {})
        mode = sizing_config.get('mode', 'auto_detect')
        delta_mode = sizing_config.get('delta_mode', {})
        is_delta = delta_mode.get('enabled') in ('Y', 'y', True)
        partition_column = delta_mode.get('partition_column', 'process_date')

        source_tables = context.config.get('source_tables', [])

        self.logger.info(f"Sizing {len(source_tables)} source tables (mode={mode}, delta={is_delta})")

        # Load cache if enabled
        if sizing_config.get('cache_sizes') in ('Y', 'y', True):
            self._load_cache(sizing_config.get('size_cache_path'))

        table_sizes: List[TableSizeInfo] = []
        total_size_gb = 0.0
        detection_stats = {
            'glue_catalog': 0,
            's3_scan': 0,
            'config_fallback': 0,
            'estimate': 0,
            'cache_hit': 0
        }

        for table_config in source_tables:
            database = table_config.get('database', '')
            table = table_config.get('table', '')
            auto_detect = table_config.get('auto_detect_size') in ('Y', 'y', True)
            s3_location = table_config.get('location', '')

            cache_key = f"{database}.{table}"

            # Check cache first
            if cache_key in self._size_cache:
                cached = self._size_cache[cache_key]
                table_sizes.append(cached)
                total_size_gb += cached.size_gb
                detection_stats['cache_hit'] += 1
                continue

            size_info = None

            # Determine detection method
            if mode == 'auto_detect' or (mode == 'hybrid' and auto_detect):
                # Try Glue Catalog first
                size_info = self._detect_from_glue_catalog(
                    database, table, s3_location,
                    partition_column if is_delta else None,
                    context.run_date if is_delta else None
                )
                if size_info:
                    detection_stats['glue_catalog'] += 1
                else:
                    # Fallback to S3 scan
                    size_info = self._detect_from_s3(
                        s3_location,
                        partition_column if is_delta else None,
                        context.run_date if is_delta else None
                    )
                    if size_info:
                        size_info.database = database
                        size_info.table = table
                        detection_stats['s3_scan'] += 1

            # Fallback to config or estimate
            if not size_info:
                estimated_gb = table_config.get('estimated_size_gb')
                if estimated_gb is not None:
                    size_info = TableSizeInfo(
                        database=database,
                        table=table,
                        size_bytes=int(estimated_gb * 1024 * 1024 * 1024),
                        size_gb=estimated_gb,
                        row_count=None,
                        detection_method='config_fallback',
                        s3_location=s3_location
                    )
                    detection_stats['config_fallback'] += 1
                else:
                    # Use smart estimate based on table type
                    estimated_gb = self._estimate_table_size(table_config)
                    size_info = TableSizeInfo(
                        database=database,
                        table=table,
                        size_bytes=int(estimated_gb * 1024 * 1024 * 1024),
                        size_gb=estimated_gb,
                        row_count=None,
                        detection_method='estimate',
                        s3_location=s3_location
                    )
                    detection_stats['estimate'] += 1

            if size_info:
                table_sizes.append(size_info)
                total_size_gb += size_info.size_gb
                self._size_cache[cache_key] = size_info

        # Save cache
        if sizing_config.get('cache_sizes') in ('Y', 'y', True):
            self._save_cache(sizing_config.get('size_cache_path'))

        # Build recommendations
        recommendations = []
        if total_size_gb > 500:
            recommendations.append(f"Total data size ({total_size_gb:.0f} GB) exceeds EKS threshold - consider EKS with Karpenter")
        elif total_size_gb > 100:
            recommendations.append(f"Total data size ({total_size_gb:.0f} GB) exceeds EMR threshold - consider EMR")

        if detection_stats['estimate'] > 0:
            recommendations.append(f"{detection_stats['estimate']} tables used size estimation - run MSCK REPAIR TABLE or Glue Crawler to get accurate sizes")

        # Store sizing data
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'sizing_results',
            [s.to_dict() for s in table_sizes],
            use_pipe_delimited=True
        )

        # Share sizing results with other agents
        context.set_shared('total_size_gb', total_size_gb)
        context.set_shared('table_sizes', [s.to_dict() for s in table_sizes])
        context.set_shared('sizing_detection_stats', detection_stats)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'total_size_gb': total_size_gb,
                'total_size_bytes': int(total_size_gb * 1024 * 1024 * 1024),
                'table_count': len(table_sizes),
                'tables': [s.to_dict() for s in table_sizes],
                'detection_stats': detection_stats
            },
            metrics={
                'total_size_gb': total_size_gb,
                'tables_detected': len(table_sizes),
                'glue_catalog_hits': detection_stats['glue_catalog'],
                's3_scan_hits': detection_stats['s3_scan'],
                'cache_hits': detection_stats['cache_hit']
            },
            recommendations=recommendations
        )

    @tool(name="detect_table_size", description="Detect size of a specific table")
    def detect_table_size(
        self,
        database: str,
        table: str,
        s3_location: str = None
    ) -> Optional[TableSizeInfo]:
        """Detect size of a specific table."""
        # Try Glue Catalog
        size_info = self._detect_from_glue_catalog(database, table, s3_location)
        if size_info:
            return size_info

        # Fallback to S3
        if s3_location:
            size_info = self._detect_from_s3(s3_location)
            if size_info:
                size_info.database = database
                size_info.table = table
                return size_info

        return None

    def _detect_from_glue_catalog(
        self,
        database: str,
        table: str,
        s3_location: str = None,
        partition_column: str = None,
        partition_date: datetime = None
    ) -> Optional[TableSizeInfo]:
        """Detect table size from Glue Data Catalog."""
        if not self.glue_client:
            return None

        try:
            # Get table metadata
            response = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )

            table_info = response.get('Table', {})
            parameters = table_info.get('Parameters', {})
            storage_descriptor = table_info.get('StorageDescriptor', {})

            # Get size from table parameters
            size_bytes = 0
            row_count = None
            method = "glue_catalog"

            if 'sizeInBytes' in parameters:
                size_bytes = int(parameters['sizeInBytes'])
                self.logger.debug(f"Found sizeInBytes for {database}.{table}: {size_bytes}")

            if 'recordCount' in parameters:
                row_count = int(parameters['recordCount'])

            # If partition date specified, try to get partition-level size
            if partition_date and partition_column:
                partition_size = self._get_partition_size(
                    database, table, partition_column, partition_date
                )
                if partition_size:
                    size_bytes = partition_size
                    method = "glue_catalog_partition"

            # Get S3 location
            location = s3_location or storage_descriptor.get('Location', '')

            if size_bytes > 0:
                return TableSizeInfo(
                    database=database,
                    table=table,
                    size_bytes=size_bytes,
                    size_gb=size_bytes / (1024 ** 3),
                    row_count=row_count,
                    detection_method=method,
                    s3_location=location
                )

            # If sizeInBytes not available, scan S3
            if location:
                return self._detect_from_s3(
                    location, partition_column, partition_date,
                    database=database, table_name=table
                )

        except Exception as e:
            self.logger.warning(f"Glue Catalog lookup failed for {database}.{table}: {e}")

        return None

    def _get_partition_size(
        self,
        database: str,
        table: str,
        partition_column: str,
        partition_date: datetime
    ) -> Optional[int]:
        """Get size of a specific partition."""
        if not self.glue_client:
            return None

        try:
            # Try different date formats
            date_formats = [
                partition_date.strftime('%Y-%m-%d'),
                partition_date.strftime('%Y%m%d'),
                partition_date.strftime('%Y/%m/%d')
            ]

            response = self.glue_client.get_partitions(
                DatabaseName=database,
                TableName=table,
                MaxResults=100
            )

            for partition in response.get('Partitions', []):
                values = partition.get('Values', [])
                for date_str in date_formats:
                    if date_str in values:
                        params = partition.get('Parameters', {})
                        if 'sizeInBytes' in params:
                            return int(params['sizeInBytes'])

        except Exception as e:
            self.logger.warning(f"Partition lookup failed: {e}")

        return None

    def _detect_from_s3(
        self,
        s3_location: str,
        partition_column: str = None,
        partition_date: datetime = None,
        database: str = None,
        table_name: str = None
    ) -> Optional[TableSizeInfo]:
        """Detect table size by scanning S3."""
        if not self.s3_client or not s3_location:
            return None

        try:
            # Parse S3 location
            if s3_location.startswith('s3://'):
                path = s3_location[5:]
            else:
                path = s3_location

            parts = path.split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ''

            # If partition date specified, narrow the prefix
            if partition_date and partition_column:
                date_prefixes = [
                    partition_date.strftime('%Y-%m-%d'),
                    partition_date.strftime('%Y/%m/%d'),
                    f"{partition_column}={partition_date.strftime('%Y-%m-%d')}"
                ]
                # Try each date format
                for date_prefix in date_prefixes:
                    test_prefix = f"{prefix.rstrip('/')}/{date_prefix}"
                    size = self._scan_s3_prefix(bucket, test_prefix)
                    if size > 0:
                        return TableSizeInfo(
                            database=database or '',
                            table=table_name or '',
                            size_bytes=size,
                            size_gb=size / (1024 ** 3),
                            row_count=None,
                            detection_method='s3_scan_partition',
                            s3_location=f"s3://{bucket}/{test_prefix}"
                        )

            # Scan full prefix
            size = self._scan_s3_prefix(bucket, prefix)
            if size > 0:
                return TableSizeInfo(
                    database=database or '',
                    table=table_name or '',
                    size_bytes=size,
                    size_gb=size / (1024 ** 3),
                    row_count=None,
                    detection_method='s3_scan',
                    s3_location=s3_location
                )

        except Exception as e:
            self.logger.warning(f"S3 scan failed for {s3_location}: {e}")

        return None

    def _scan_s3_prefix(self, bucket: str, prefix: str) -> int:
        """Scan S3 prefix and sum all object sizes."""
        total_bytes = 0

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    total_bytes += obj.get('Size', 0)

        except Exception as e:
            self.logger.warning(f"S3 scan error for s3://{bucket}/{prefix}: {e}")

        return total_bytes

    def _estimate_table_size(self, table_config: Dict[str, Any]) -> float:
        """Estimate table size based on table characteristics."""
        table_name = table_config.get('table', '').lower()

        # Check if it's a dimension table (typically small)
        dimension_keywords = ['dim_', 'lookup', 'reference', 'code', 'type', 'status', 'zip', 'gis']
        for keyword in dimension_keywords:
            if keyword in table_name:
                return self.DEFAULT_DIMENSION_TABLE_GB

        # Check if it's a broadcast table
        if table_config.get('broadcast'):
            return self.DEFAULT_DIMENSION_TABLE_GB

        # Check for large fact table indicators
        fact_keywords = ['fact_', 'transaction', 'history', 'master', 'clickstream', 'event', 'log']
        for keyword in fact_keywords:
            if keyword in table_name:
                return self.DEFAULT_FACT_TABLE_GB

        # Default to medium size
        return 10.0

    def _load_cache(self, cache_path: str = None) -> None:
        """Load size cache from local storage."""
        try:
            cache_file = Path(cache_path or 'data/agent_store/source_sizes.json')
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)

                for key, data in cached_data.items():
                    self._size_cache[key] = TableSizeInfo(
                        database=data['database'],
                        table=data['table'],
                        size_bytes=data['size_bytes'],
                        size_gb=data['size_gb'],
                        row_count=data.get('row_count'),
                        detection_method=data['detection_method'],
                        s3_location=data.get('s3_location')
                    )

                self.logger.info(f"Loaded {len(self._size_cache)} cached table sizes")
        except Exception as e:
            self.logger.warning(f"Failed to load cache: {e}")

    def _save_cache(self, cache_path: str = None) -> None:
        """Save size cache to local storage."""
        try:
            cache_file = Path(cache_path or 'data/agent_store/source_sizes.json')
            cache_file.parent.mkdir(parents=True, exist_ok=True)

            cache_data = {
                key: info.to_dict()
                for key, info in self._size_cache.items()
            }

            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)

            self.logger.info(f"Saved {len(self._size_cache)} table sizes to cache")
        except Exception as e:
            self.logger.warning(f"Failed to save cache: {e}")
