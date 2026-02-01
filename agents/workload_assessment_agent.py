"""
Workload Assessment Agent - Intelligent workload analysis for optimal platform selection
Analyzes table sizes, data patterns, complexity, and historical metrics
"""

import json
import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
from datetime import datetime, timedelta
from dataclasses import dataclass, field

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkloadCategory(Enum):
    """Workload categories based on complexity and resource needs."""
    LIGHTWEIGHT = "lightweight"      # < 1GB, simple transforms
    MODERATE = "moderate"            # 1-10GB, standard ETL
    HEAVY = "heavy"                  # 10-100GB, complex joins
    MASSIVE = "massive"              # > 100GB, distributed processing
    STREAMING = "streaming"          # Real-time/near-real-time


class SchedulePattern(Enum):
    """Schedule patterns for workload."""
    WEEKDAY_PEAK = "weekday_peak"        # Mon-Fri business hours
    WEEKDAY_OFF_PEAK = "weekday_off_peak"  # Mon-Fri nights
    WEEKEND = "weekend"
    DAILY = "daily"
    HOURLY = "hourly"
    AD_HOC = "ad_hoc"


class PlatformRecommendation(Enum):
    """Platform recommendations."""
    GLUE_STANDARD = "glue_standard"
    GLUE_FLEX = "glue_flex"
    GLUE_STREAMING = "glue_streaming"
    EMR_SERVERLESS = "emr_serverless"
    EMR_CLUSTER = "emr_cluster"
    LAMBDA = "lambda"
    EKS_SPARK = "eks_spark"
    EKS_KARPENTER = "eks_karpenter"


@dataclass
class TableMetrics:
    """Metrics for a single table."""
    table_name: str
    database: str
    row_count: int = 0
    size_bytes: int = 0
    partition_count: int = 0
    column_count: int = 0
    avg_row_size: int = 0
    last_modified: Optional[datetime] = None
    data_skew_score: float = 0.0  # 0 = no skew, 1 = high skew
    complex_types: List[str] = field(default_factory=list)


@dataclass
class HistoricalMetrics:
    """Historical execution metrics."""
    avg_execution_time_sec: float = 0.0
    avg_s3_bytes_read: int = 0
    avg_s3_bytes_written: int = 0
    avg_shuffle_bytes: int = 0
    avg_cpu_utilization: float = 0.0
    avg_memory_utilization: float = 0.0
    failure_rate: float = 0.0
    avg_data_skewness: float = 0.0
    avg_workers_used: int = 0
    weekday_volume_gb: float = 0.0
    weekend_volume_gb: float = 0.0
    cost_per_run: float = 0.0


@dataclass
class WorkloadAssessment:
    """Complete workload assessment result."""
    category: WorkloadCategory
    recommended_platform: PlatformRecommendation
    confidence_score: float
    resource_config: Dict[str, Any]
    reasoning: List[str]
    warnings: List[str]
    estimated_cost: float
    estimated_duration_minutes: int
    flex_mode_suitable: bool
    karpenter_suitable: bool
    optimization_opportunities: List[str]


class WorkloadAssessmentAgent:
    """
    Intelligent workload assessment agent that analyzes:
    - Input table sizes and schemas
    - Data volume patterns (weekday vs weekend)
    - Complex operations (joins, aggregations)
    - Historical execution metrics
    - Flex mode suitability
    - Shuffle and data skew patterns
    - CPU/memory utilization profiles
    - Karpenter/EKS opportunities
    """

    # Size thresholds in bytes
    SIZE_THRESHOLDS = {
        'lightweight': 1 * 1024**3,     # 1 GB
        'moderate': 10 * 1024**3,       # 10 GB
        'heavy': 100 * 1024**3,         # 100 GB
        'massive': float('inf')
    }

    # Worker type configurations
    WORKER_CONFIGS = {
        'G.1X': {'memory_gb': 16, 'vcpu': 4, 'cost_per_dpu_hour': 0.44},
        'G.2X': {'memory_gb': 32, 'vcpu': 8, 'cost_per_dpu_hour': 0.44},
        'G.4X': {'memory_gb': 64, 'vcpu': 16, 'cost_per_dpu_hour': 0.44},
        'G.8X': {'memory_gb': 128, 'vcpu': 32, 'cost_per_dpu_hour': 0.44},
        'Z.2X': {'memory_gb': 64, 'vcpu': 8, 'cost_per_dpu_hour': 0.44},  # Memory optimized
    }

    # Complex operations patterns
    COMPLEX_OPERATIONS = {
        'window_functions': r'\b(OVER|PARTITION BY|ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\b',
        'heavy_aggregations': r'\b(GROUP BY|CUBE|ROLLUP|GROUPING SETS)\b',
        'large_joins': r'\bJOIN\b.*\bJOIN\b.*\bJOIN\b',  # 3+ joins
        'subqueries': r'\(\s*SELECT\b',
        'distinct_operations': r'\bDISTINCT\b',
        'array_operations': r'\b(EXPLODE|FLATTEN|ARRAY_AGG|COLLECT_LIST)\b',
        'udf_usage': r'\b(UDF|UDAF|UDTF)\b',
    }

    def __init__(self, region: str = 'us-east-1'):
        """Initialize the workload assessment agent."""
        self.region = region
        self.glue_client = boto3.client('glue', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)

    def get_table_metrics(self, database: str, table_name: str) -> TableMetrics:
        """
        Get metrics for a table from Glue Catalog.

        Args:
            database: Glue database name
            table_name: Table name

        Returns:
            TableMetrics with size, partitions, schema info
        """
        metrics = TableMetrics(table_name=table_name, database=database)

        try:
            # Get table metadata
            response = self.glue_client.get_table(
                DatabaseName=database,
                Name=table_name
            )
            table = response['Table']

            # Get column info
            columns = table.get('StorageDescriptor', {}).get('Columns', [])
            partition_keys = table.get('PartitionKeys', [])

            metrics.column_count = len(columns) + len(partition_keys)

            # Detect complex types
            for col in columns:
                col_type = col.get('Type', '').lower()
                if any(ct in col_type for ct in ['array', 'map', 'struct']):
                    metrics.complex_types.append(f"{col['Name']}: {col_type}")

            # Get partition count
            try:
                partitions = self.glue_client.get_partitions(
                    DatabaseName=database,
                    Name=table_name,
                    MaxResults=1000
                )
                metrics.partition_count = len(partitions.get('Partitions', []))
            except ClientError:
                metrics.partition_count = 0

            # Estimate size from S3
            location = table.get('StorageDescriptor', {}).get('Location', '')
            if location.startswith('s3://'):
                metrics.size_bytes = self._estimate_s3_size(location)

            # Get last modified time
            if 'UpdateTime' in table:
                metrics.last_modified = table['UpdateTime']

            logger.info(f"Retrieved metrics for {database}.{table_name}: "
                       f"{metrics.size_bytes / 1024**3:.2f} GB, "
                       f"{metrics.partition_count} partitions")

        except ClientError as e:
            logger.warning(f"Could not get table metrics: {e}")

        return metrics

    def _estimate_s3_size(self, s3_path: str, sample_limit: int = 100) -> int:
        """
        Estimate S3 path size by sampling objects.

        Args:
            s3_path: S3 path (s3://bucket/prefix)
            sample_limit: Max objects to sample

        Returns:
            Estimated total size in bytes
        """
        try:
            # Parse S3 path
            path_parts = s3_path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''

            # List and sample objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            total_size = 0
            object_count = 0

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    total_size += obj['Size']
                    object_count += 1
                    if object_count >= sample_limit:
                        # Extrapolate if we hit sample limit
                        estimated_total_objects = page.get('KeyCount', object_count)
                        return int(total_size * (estimated_total_objects / object_count))

            return total_size

        except ClientError as e:
            logger.warning(f"Could not estimate S3 size: {e}")
            return 0

    def get_historical_metrics(self, job_name: str, days: int = 30) -> HistoricalMetrics:
        """
        Get historical execution metrics from CloudWatch and Glue.

        Args:
            job_name: Glue job name
            days: Number of days to analyze

        Returns:
            HistoricalMetrics with averages and patterns
        """
        metrics = HistoricalMetrics()
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)

        try:
            # Get job run history
            runs = []
            paginator = self.glue_client.get_paginator('get_job_runs')

            for page in paginator.paginate(JobName=job_name, MaxResults=100):
                for run in page.get('JobRuns', []):
                    if run.get('StartedOn', datetime.min) >= start_time:
                        runs.append(run)

            if not runs:
                logger.info(f"No historical runs found for {job_name}")
                return metrics

            # Calculate metrics
            execution_times = []
            s3_reads = []
            s3_writes = []
            weekday_sizes = []
            weekend_sizes = []
            failures = 0

            for run in runs:
                # Execution time
                if run.get('ExecutionTime'):
                    execution_times.append(run['ExecutionTime'])

                # S3 metrics from job run details
                if 'ExecutionClass' in run:
                    metrics_data = run.get('ExecutionClass', '')

                # Check day of week for volume patterns
                start_date = run.get('StartedOn')
                if start_date:
                    # Estimate data volume from DPU hours
                    dpu_seconds = run.get('DPUSeconds', 0)
                    estimated_gb = dpu_seconds / 3600 * 5  # Rough estimate

                    if start_date.weekday() < 5:  # Weekday
                        weekday_sizes.append(estimated_gb)
                    else:
                        weekend_sizes.append(estimated_gb)

                # Track failures
                if run.get('JobRunState') == 'FAILED':
                    failures += 1

            # Compute averages
            if execution_times:
                metrics.avg_execution_time_sec = sum(execution_times) / len(execution_times)
            if weekday_sizes:
                metrics.weekday_volume_gb = sum(weekday_sizes) / len(weekday_sizes)
            if weekend_sizes:
                metrics.weekend_volume_gb = sum(weekend_sizes) / len(weekend_sizes)

            metrics.failure_rate = failures / len(runs) if runs else 0.0

            # Get CloudWatch metrics for more detail
            cw_metrics = self._get_cloudwatch_metrics(job_name, start_time, end_time)
            metrics.avg_cpu_utilization = cw_metrics.get('cpu', 0.0)
            metrics.avg_memory_utilization = cw_metrics.get('memory', 0.0)
            metrics.avg_shuffle_bytes = cw_metrics.get('shuffle', 0)

            logger.info(f"Historical metrics for {job_name}: "
                       f"avg exec time {metrics.avg_execution_time_sec:.0f}s, "
                       f"failure rate {metrics.failure_rate:.1%}")

        except ClientError as e:
            logger.warning(f"Could not get historical metrics: {e}")

        return metrics

    def _get_cloudwatch_metrics(self, job_name: str,
                                 start_time: datetime,
                                 end_time: datetime) -> Dict[str, float]:
        """Get CloudWatch metrics for a Glue job."""
        metrics = {'cpu': 0.0, 'memory': 0.0, 'shuffle': 0}

        try:
            # Query CloudWatch for Glue metrics
            for metric_name, key in [
                ('glue.driver.aggregate.cpuTimeMs', 'cpu'),
                ('glue.driver.jvm.heap.usage', 'memory'),
                ('glue.driver.aggregate.shuffleBytesWritten', 'shuffle')
            ]:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='Glue',
                    MetricName=metric_name,
                    Dimensions=[
                        {'Name': 'JobName', 'Value': job_name},
                        {'Name': 'Type', 'Value': 'gauge'}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,  # Daily
                    Statistics=['Average']
                )

                datapoints = response.get('Datapoints', [])
                if datapoints:
                    metrics[key] = sum(d['Average'] for d in datapoints) / len(datapoints)

        except ClientError as e:
            logger.debug(f"CloudWatch metrics not available: {e}")

        return metrics

    def analyze_code_complexity(self, code: str) -> Dict[str, Any]:
        """
        Analyze PySpark/SQL code for complexity indicators.

        Args:
            code: Source code to analyze

        Returns:
            Dict with complexity metrics
        """
        complexity = {
            'score': 0.0,  # 0-1 scale
            'operations': [],
            'join_count': 0,
            'has_shuffle_heavy_ops': False,
            'has_data_skew_risk': False,
            'estimated_stages': 1,
            'recommendations': []
        }

        code_upper = code.upper()

        # Count joins
        join_count = len(re.findall(r'\bJOIN\b', code_upper))
        complexity['join_count'] = join_count
        if join_count > 3:
            complexity['recommendations'].append(
                f"High join count ({join_count}). Consider breaking into stages or using broadcast joins."
            )

        # Check for complex operations
        score = 0.0
        for op_name, pattern in self.COMPLEX_OPERATIONS.items():
            if re.search(pattern, code_upper):
                complexity['operations'].append(op_name)
                score += 0.15

        # Check for shuffle-heavy operations
        shuffle_patterns = [
            r'\brepartition\s*\(',
            r'\bcoalesce\s*\(',
            r'\bshuffle\s*=\s*True',
            r'\bsortWithinPartitions\b',
            r'\bgroupBy\s*\(',
        ]
        for pattern in shuffle_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                complexity['has_shuffle_heavy_ops'] = True
                score += 0.1
                break

        # Check for data skew risk
        skew_patterns = [
            r'GROUP BY.*\bnull\b',
            r'GROUP BY.*\bid\b',
            r'\.groupByKey\s*\(',
            r'reduceByKey.*\bcount\b',
        ]
        for pattern in skew_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                complexity['has_data_skew_risk'] = True
                complexity['recommendations'].append(
                    "Potential data skew detected. Consider salting keys or using broadcast joins."
                )
                score += 0.15
                break

        # Estimate stages (rough)
        complexity['estimated_stages'] = max(1, join_count + len(complexity['operations']))

        complexity['score'] = min(1.0, score)
        return complexity

    def assess_workload(self,
                        config: Dict[str, Any],
                        code: Optional[str] = None,
                        job_name: Optional[str] = None) -> WorkloadAssessment:
        """
        Perform comprehensive workload assessment.

        Args:
            config: Pipeline configuration
            code: Optional PySpark code to analyze
            job_name: Optional existing job name for historical analysis

        Returns:
            WorkloadAssessment with recommendations
        """
        reasoning = []
        warnings = []
        optimization_opportunities = []

        # 1. Analyze data sources
        total_size_bytes = 0
        table_metrics = []
        data_sources = config.get('data_sources', [])

        for source in data_sources:
            if source.get('type') == 'glue_catalog':
                db = source.get('database', '')
                table = source.get('table', '')
                if db and table:
                    metrics = self.get_table_metrics(db, table)
                    table_metrics.append(metrics)
                    total_size_bytes += metrics.size_bytes

                    if metrics.data_skew_score > 0.5:
                        warnings.append(f"Table {table} has high data skew ({metrics.data_skew_score:.2f})")

                    if metrics.complex_types:
                        reasoning.append(f"Table {table} has complex types: {metrics.complex_types}")

        # 2. Categorize by size
        if total_size_bytes < self.SIZE_THRESHOLDS['lightweight']:
            category = WorkloadCategory.LIGHTWEIGHT
            reasoning.append(f"Data volume is lightweight ({total_size_bytes / 1024**3:.2f} GB)")
        elif total_size_bytes < self.SIZE_THRESHOLDS['moderate']:
            category = WorkloadCategory.MODERATE
            reasoning.append(f"Data volume is moderate ({total_size_bytes / 1024**3:.2f} GB)")
        elif total_size_bytes < self.SIZE_THRESHOLDS['heavy']:
            category = WorkloadCategory.HEAVY
            reasoning.append(f"Data volume is heavy ({total_size_bytes / 1024**3:.2f} GB)")
        else:
            category = WorkloadCategory.MASSIVE
            reasoning.append(f"Data volume is massive ({total_size_bytes / 1024**3:.2f} GB)")

        # 3. Analyze code complexity
        code_complexity = {'score': 0.0, 'join_count': 0}
        if code:
            code_complexity = self.analyze_code_complexity(code)
            if code_complexity['score'] > 0.5:
                reasoning.append(f"Code complexity is high (score: {code_complexity['score']:.2f})")
                if category == WorkloadCategory.LIGHTWEIGHT:
                    category = WorkloadCategory.MODERATE

        # 4. Get historical metrics if available
        historical = HistoricalMetrics()
        if job_name:
            historical = self.get_historical_metrics(job_name)
            if historical.failure_rate > 0.1:
                warnings.append(f"High historical failure rate: {historical.failure_rate:.1%}")
            if historical.weekday_volume_gb > historical.weekend_volume_gb * 2:
                reasoning.append("Weekday volumes significantly higher than weekend")

        # 5. Determine schedule pattern
        workload_config = config.get('workload', {})
        schedule_type = workload_config.get('schedule_type', 'daily')
        schedule_pattern = SchedulePattern.DAILY

        if schedule_type == 'weekday':
            schedule_pattern = SchedulePattern.WEEKDAY_PEAK
        elif schedule_type == 'weekend':
            schedule_pattern = SchedulePattern.WEEKEND
        elif schedule_type == 'hourly':
            schedule_pattern = SchedulePattern.HOURLY
        elif schedule_type == 'adhoc':
            schedule_pattern = SchedulePattern.AD_HOC

        # 6. Determine Flex mode suitability
        flex_suitable = self._is_flex_suitable(
            category, schedule_pattern, workload_config, historical
        )
        if flex_suitable:
            optimization_opportunities.append(
                "Flex execution mode can reduce costs by up to 35% for this workload"
            )

        # 7. Determine Karpenter/EKS suitability
        karpenter_suitable = self._is_karpenter_suitable(
            category, total_size_bytes, code_complexity, historical
        )
        if karpenter_suitable:
            optimization_opportunities.append(
                "EKS with Karpenter can provide better resource optimization for large workloads"
            )

        # 8. Select platform and configuration
        platform, resource_config, confidence = self._select_platform(
            category=category,
            total_size_bytes=total_size_bytes,
            code_complexity=code_complexity,
            historical=historical,
            schedule_pattern=schedule_pattern,
            flex_suitable=flex_suitable,
            karpenter_suitable=karpenter_suitable,
            workload_config=workload_config
        )

        reasoning.append(f"Recommended platform: {platform.value} (confidence: {confidence:.1%})")

        # 9. Estimate cost and duration
        estimated_cost = self._estimate_cost(platform, resource_config, total_size_bytes)
        estimated_duration = self._estimate_duration(
            total_size_bytes, code_complexity, resource_config
        )

        # 10. Additional optimization opportunities
        if code_complexity.get('has_shuffle_heavy_ops'):
            optimization_opportunities.append(
                "Consider optimizing shuffle operations to reduce data movement"
            )
        if len(table_metrics) > 3:
            optimization_opportunities.append(
                "Multiple data sources detected. Consider staging intermediate results"
            )

        return WorkloadAssessment(
            category=category,
            recommended_platform=platform,
            confidence_score=confidence,
            resource_config=resource_config,
            reasoning=reasoning,
            warnings=warnings,
            estimated_cost=estimated_cost,
            estimated_duration_minutes=estimated_duration,
            flex_mode_suitable=flex_suitable,
            karpenter_suitable=karpenter_suitable,
            optimization_opportunities=optimization_opportunities
        )

    def _is_flex_suitable(self,
                          category: WorkloadCategory,
                          schedule: SchedulePattern,
                          workload_config: Dict[str, Any],
                          historical: HistoricalMetrics) -> bool:
        """Determine if Flex execution mode is suitable."""
        # Flex mode is good for:
        # - Non-urgent workloads
        # - Off-peak scheduling
        # - Lower criticality jobs
        # - Jobs that can tolerate longer startup times

        criticality = workload_config.get('criticality', 'medium')
        time_sensitivity = workload_config.get('time_sensitivity', 'medium')

        if criticality in ['high', 'critical']:
            return False
        if time_sensitivity in ['high', 'urgent']:
            return False
        if schedule in [SchedulePattern.HOURLY, SchedulePattern.STREAMING]:
            return False

        # Prefer Flex for off-peak and weekend jobs
        if schedule in [SchedulePattern.WEEKDAY_OFF_PEAK, SchedulePattern.WEEKEND]:
            return True

        # Also suitable for moderate workloads with good historical performance
        if category in [WorkloadCategory.LIGHTWEIGHT, WorkloadCategory.MODERATE]:
            if historical.failure_rate < 0.05:
                return True

        return False

    def _is_karpenter_suitable(self,
                               category: WorkloadCategory,
                               total_size_bytes: int,
                               code_complexity: Dict[str, Any],
                               historical: HistoricalMetrics) -> bool:
        """Determine if EKS with Karpenter is suitable."""
        # Karpenter is beneficial for:
        # - Large/massive workloads
        # - Complex processing with varying resource needs
        # - Workloads with bursty patterns
        # - When fine-grained resource control is needed

        if category in [WorkloadCategory.HEAVY, WorkloadCategory.MASSIVE]:
            return True

        # High complexity jobs benefit from custom resource allocation
        if code_complexity.get('score', 0) > 0.6:
            return True

        # Large shuffle operations benefit from optimized networking
        if historical.avg_shuffle_bytes > 50 * 1024**3:  # 50 GB
            return True

        return False

    def _select_platform(self,
                         category: WorkloadCategory,
                         total_size_bytes: int,
                         code_complexity: Dict[str, Any],
                         historical: HistoricalMetrics,
                         schedule_pattern: SchedulePattern,
                         flex_suitable: bool,
                         karpenter_suitable: bool,
                         workload_config: Dict[str, Any]) -> Tuple[PlatformRecommendation, Dict[str, Any], float]:
        """
        Select optimal platform and configuration.

        Returns:
            Tuple of (platform, resource_config, confidence_score)
        """
        confidence = 0.8

        # Default resource config
        resource_config = {
            'worker_type': 'G.1X',
            'number_of_workers': 2,
            'timeout_minutes': 60,
            'flex_mode': False,
            'auto_scaling': False
        }

        # Lightweight workloads
        if category == WorkloadCategory.LIGHTWEIGHT:
            if code_complexity.get('score', 0) < 0.3:
                # Simple lightweight jobs can use Lambda
                return (
                    PlatformRecommendation.LAMBDA,
                    {'memory_mb': 1024, 'timeout_seconds': 300},
                    0.9
                )
            else:
                resource_config['number_of_workers'] = 2
                if flex_suitable:
                    resource_config['flex_mode'] = True
                    return (PlatformRecommendation.GLUE_FLEX, resource_config, 0.85)
                return (PlatformRecommendation.GLUE_STANDARD, resource_config, 0.85)

        # Moderate workloads
        elif category == WorkloadCategory.MODERATE:
            resource_config['number_of_workers'] = max(5, min(10, int(total_size_bytes / (2 * 1024**3))))
            resource_config['worker_type'] = 'G.1X'

            if flex_suitable:
                resource_config['flex_mode'] = True
                return (PlatformRecommendation.GLUE_FLEX, resource_config, 0.85)
            return (PlatformRecommendation.GLUE_STANDARD, resource_config, 0.85)

        # Heavy workloads
        elif category == WorkloadCategory.HEAVY:
            resource_config['number_of_workers'] = max(10, min(50, int(total_size_bytes / (5 * 1024**3))))
            resource_config['worker_type'] = 'G.2X'
            resource_config['auto_scaling'] = True
            resource_config['min_workers'] = 10
            resource_config['max_workers'] = 50

            if karpenter_suitable and workload_config.get('prefer_eks', False):
                return (
                    PlatformRecommendation.EKS_KARPENTER,
                    {
                        **resource_config,
                        'node_pool': 'spark-workers',
                        'instance_types': ['r5.2xlarge', 'r5.4xlarge', 'm5.4xlarge'],
                        'spot_enabled': True
                    },
                    0.75
                )

            # EMR Serverless for heavy workloads
            if code_complexity.get('has_shuffle_heavy_ops'):
                return (
                    PlatformRecommendation.EMR_SERVERLESS,
                    {
                        'release_label': 'emr-6.10.0',
                        'max_capacity_cpu': resource_config['number_of_workers'] * 4,
                        'max_capacity_memory': f"{resource_config['number_of_workers'] * 16}GB"
                    },
                    0.8
                )

            return (PlatformRecommendation.GLUE_STANDARD, resource_config, 0.8)

        # Massive workloads
        else:  # MASSIVE
            if karpenter_suitable:
                return (
                    PlatformRecommendation.EKS_KARPENTER,
                    {
                        'node_pool': 'spark-workers-large',
                        'instance_types': ['r5.4xlarge', 'r5.8xlarge', 'r5.12xlarge'],
                        'min_nodes': 10,
                        'max_nodes': 100,
                        'spot_enabled': True,
                        'spark_config': {
                            'spark.executor.memory': '28g',
                            'spark.executor.cores': '4',
                            'spark.dynamicAllocation.enabled': 'true'
                        }
                    },
                    0.75
                )

            # EMR cluster for massive workloads
            return (
                PlatformRecommendation.EMR_CLUSTER,
                {
                    'release_label': 'emr-6.10.0',
                    'master_instance_type': 'r5.4xlarge',
                    'core_instance_type': 'r5.4xlarge',
                    'core_instance_count': max(20, int(total_size_bytes / (10 * 1024**3))),
                    'spot_enabled': True,
                    'auto_scaling': True
                },
                0.8
            )

    def _estimate_cost(self,
                       platform: PlatformRecommendation,
                       resource_config: Dict[str, Any],
                       total_size_bytes: int) -> float:
        """Estimate execution cost in USD."""
        # Rough cost estimation based on platform

        if platform == PlatformRecommendation.LAMBDA:
            # Lambda pricing: $0.0000166667 per GB-second
            memory_gb = resource_config.get('memory_mb', 1024) / 1024
            estimated_seconds = 60  # Assume 1 minute
            return memory_gb * estimated_seconds * 0.0000166667

        elif platform in [PlatformRecommendation.GLUE_STANDARD, PlatformRecommendation.GLUE_FLEX]:
            workers = resource_config.get('number_of_workers', 2)
            worker_type = resource_config.get('worker_type', 'G.1X')
            dpu_per_worker = 1 if worker_type == 'G.1X' else 2

            # Estimate duration in hours
            hours = max(0.1, (total_size_bytes / (1024**3)) / (workers * 10))  # 10GB per worker per hour

            cost_per_dpu_hour = 0.44
            if platform == PlatformRecommendation.GLUE_FLEX:
                cost_per_dpu_hour = 0.29  # ~35% discount

            return workers * dpu_per_worker * hours * cost_per_dpu_hour

        elif platform in [PlatformRecommendation.EMR_SERVERLESS, PlatformRecommendation.EMR_CLUSTER]:
            # EMR pricing varies, use approximation
            return (total_size_bytes / (1024**3)) * 0.05  # $0.05 per GB processed

        elif platform in [PlatformRecommendation.EKS_SPARK, PlatformRecommendation.EKS_KARPENTER]:
            # EKS with Spot instances
            return (total_size_bytes / (1024**3)) * 0.03  # $0.03 per GB with spot

        return 0.0

    def _estimate_duration(self,
                           total_size_bytes: int,
                           code_complexity: Dict[str, Any],
                           resource_config: Dict[str, Any]) -> int:
        """Estimate execution duration in minutes."""
        # Base processing rate: 1 GB per minute per worker
        workers = resource_config.get('number_of_workers', 2)
        size_gb = total_size_bytes / (1024**3)

        base_minutes = max(5, size_gb / workers)

        # Adjust for complexity
        complexity_multiplier = 1 + code_complexity.get('score', 0)
        join_multiplier = 1 + (code_complexity.get('join_count', 0) * 0.1)

        estimated = base_minutes * complexity_multiplier * join_multiplier

        return int(min(estimated, 480))  # Cap at 8 hours

    def generate_assessment_report(self, assessment: WorkloadAssessment) -> str:
        """Generate a human-readable assessment report."""
        report = []
        report.append("=" * 60)
        report.append("WORKLOAD ASSESSMENT REPORT")
        report.append("=" * 60)
        report.append("")

        report.append(f"Category: {assessment.category.value.upper()}")
        report.append(f"Recommended Platform: {assessment.recommended_platform.value}")
        report.append(f"Confidence Score: {assessment.confidence_score:.1%}")
        report.append("")

        report.append("--- Reasoning ---")
        for reason in assessment.reasoning:
            report.append(f"  • {reason}")
        report.append("")

        if assessment.warnings:
            report.append("--- Warnings ---")
            for warning in assessment.warnings:
                report.append(f"  ⚠ {warning}")
            report.append("")

        report.append("--- Resource Configuration ---")
        for key, value in assessment.resource_config.items():
            report.append(f"  {key}: {value}")
        report.append("")

        report.append("--- Estimates ---")
        report.append(f"  Estimated Cost: ${assessment.estimated_cost:.2f}")
        report.append(f"  Estimated Duration: {assessment.estimated_duration_minutes} minutes")
        report.append(f"  Flex Mode Suitable: {'Yes' if assessment.flex_mode_suitable else 'No'}")
        report.append(f"  Karpenter Suitable: {'Yes' if assessment.karpenter_suitable else 'No'}")
        report.append("")

        if assessment.optimization_opportunities:
            report.append("--- Optimization Opportunities ---")
            for opp in assessment.optimization_opportunities:
                report.append(f"  ✓ {opp}")
            report.append("")

        report.append("=" * 60)

        return "\n".join(report)


# Example usage
if __name__ == '__main__':
    # Test with sample config
    sample_config = {
        'workload': {
            'name': 'customer_360_etl',
            'data_volume': 'high',
            'complexity': 'high',
            'criticality': 'medium',
            'time_sensitivity': 'medium',
            'schedule_type': 'weekday'
        },
        'data_sources': [
            {
                'type': 'glue_catalog',
                'database': 'ecommerce_db',
                'table': 'orders'
            },
            {
                'type': 'glue_catalog',
                'database': 'ecommerce_db',
                'table': 'customers'
            }
        ]
    }

    sample_code = """
    orders_df = spark.table("ecommerce_db.orders")
    customers_df = spark.table("ecommerce_db.customers")
    products_df = spark.table("ecommerce_db.products")

    result = orders_df \\
        .join(customers_df, "customer_id") \\
        .join(products_df, "product_id") \\
        .groupBy("customer_id", "segment") \\
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_spent")
        ) \\
        .withColumn("rank", row_number().over(Window.partitionBy("segment").orderBy(desc("total_spent"))))
    """

    agent = WorkloadAssessmentAgent()
    assessment = agent.assess_workload(sample_config, sample_code, job_name='customer_360_job')
    print(agent.generate_assessment_report(assessment))
