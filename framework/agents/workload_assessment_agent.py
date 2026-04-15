#!/usr/bin/env python3
"""
Workload Assessment Agent
=========================

Intelligent agent that analyzes workload characteristics to recommend:
1. Optimal platform (Glue, EMR, EKS)
2. Resource configuration (memory, executors, workers)
3. Cost estimation and optimization
4. Historical trend analysis
5. Data skew detection
6. Performance prediction

Considers multiple factors:
- Input table sizes
- Inbound data volume (weekday vs weekend patterns)
- Complex operations (joins, aggregations)
- Join metrics and strategies
- Flex mode vs standard capacity
- Shuffle operations
- S3 byte read/write
- CPU load and memory profile
- Executor utilization
- Data skewness
- Historical execution trends
- Cost per GB processed
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import math


class Platform(Enum):
    """Available processing platforms."""
    GLUE = "glue"
    EMR = "emr"
    EKS = "eks"
    LAMBDA = "lambda"


class WorkerType(Enum):
    """Worker types for different platforms."""
    GLUE_G1X = "G.1X"  # 4 vCPU, 16GB memory
    GLUE_G2X = "G.2X"  # 8 vCPU, 32GB memory
    GLUE_G4X = "G.4X"  # 16 vCPU, 64GB memory
    GLUE_G8X = "G.8X"  # 32 vCPU, 128GB memory
    EMR_M5_XLARGE = "m5.xlarge"  # 4 vCPU, 16GB
    EMR_M5_2XLARGE = "m5.2xlarge"  # 8 vCPU, 32GB
    EMR_R5_XLARGE = "r5.xlarge"  # 4 vCPU, 32GB (memory optimized)
    EMR_R5_2XLARGE = "r5.2xlarge"  # 8 vCPU, 64GB
    EKS_SPOT = "spot"
    EKS_GRAVITON = "graviton"


class ComplexityLevel(Enum):
    """Workload complexity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


@dataclass
class DataVolumeMetrics:
    """Metrics about data volume."""
    total_bytes: int = 0
    total_rows: int = 0
    avg_row_size_bytes: int = 0
    num_partitions: int = 0
    partition_skew_factor: float = 1.0
    max_partition_bytes: int = 0
    min_partition_bytes: int = 0


@dataclass
class OperationMetrics:
    """Metrics about operations in the job."""
    num_joins: int = 0
    join_types: List[str] = field(default_factory=list)
    num_aggregations: int = 0
    num_window_functions: int = 0
    num_udfs: int = 0
    num_shuffles: int = 0
    has_broadcast_join: bool = False
    has_sort_merge_join: bool = False
    estimated_shuffle_bytes: int = 0


@dataclass
class HistoricalMetrics:
    """Historical execution metrics."""
    avg_duration_seconds: int = 0
    avg_cpu_utilization: float = 0.0
    avg_memory_utilization: float = 0.0
    avg_shuffle_read_bytes: int = 0
    avg_shuffle_write_bytes: int = 0
    avg_s3_read_bytes: int = 0
    avg_s3_write_bytes: int = 0
    failure_rate: float = 0.0
    avg_cost: float = 0.0
    trend_direction: str = "stable"  # increasing, decreasing, stable
    weekday_volume_multiplier: float = 1.0
    weekend_volume_multiplier: float = 1.0


@dataclass
class ResourceRecommendation:
    """Recommended resource configuration."""
    platform: Platform
    worker_type: WorkerType
    num_workers: int
    executor_memory: str
    executor_cores: int
    driver_memory: str
    spark_configs: Dict[str, str] = field(default_factory=dict)
    estimated_duration_minutes: int = 0
    estimated_cost: float = 0.0
    use_spot: bool = False
    use_graviton: bool = False
    flex_mode: bool = False
    confidence: float = 0.0
    reasoning: List[str] = field(default_factory=list)


@dataclass
class WorkloadAssessment:
    """Complete workload assessment result."""
    complexity: ComplexityLevel
    data_volume: DataVolumeMetrics
    operations: OperationMetrics
    historical: Optional[HistoricalMetrics] = None
    primary_recommendation: Optional[ResourceRecommendation] = None
    alternative_recommendations: List[ResourceRecommendation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    optimization_opportunities: List[str] = field(default_factory=list)
    skew_detected: bool = False
    skew_columns: List[str] = field(default_factory=list)


class WorkloadAssessmentAgent:
    """
    Agent that analyzes workload characteristics and recommends optimal configurations.
    """

    def __init__(self, config):
        self.config = config

        # Cost per hour (USD) for different worker types
        self.cost_per_hour = {
            WorkerType.GLUE_G1X: 0.44,
            WorkerType.GLUE_G2X: 0.88,
            WorkerType.GLUE_G4X: 1.76,
            WorkerType.GLUE_G8X: 3.52,
            WorkerType.EMR_M5_XLARGE: 0.192,
            WorkerType.EMR_M5_2XLARGE: 0.384,
            WorkerType.EMR_R5_XLARGE: 0.252,
            WorkerType.EMR_R5_2XLARGE: 0.504,
            WorkerType.EKS_SPOT: 0.10,  # Average spot price
            WorkerType.EKS_GRAVITON: 0.08,  # Graviton pricing
        }

        # Memory per worker type (GB)
        self.memory_per_worker = {
            WorkerType.GLUE_G1X: 16,
            WorkerType.GLUE_G2X: 32,
            WorkerType.GLUE_G4X: 64,
            WorkerType.GLUE_G8X: 128,
            WorkerType.EMR_M5_XLARGE: 16,
            WorkerType.EMR_M5_2XLARGE: 32,
            WorkerType.EMR_R5_XLARGE: 32,
            WorkerType.EMR_R5_2XLARGE: 64,
            WorkerType.EKS_SPOT: 16,
            WorkerType.EKS_GRAVITON: 16,
        }

    def assess_workload(
        self,
        source_tables: List[Dict[str, Any]],
        code: Optional[str] = None,
        historical_runs: Optional[List[Dict[str, Any]]] = None,
        current_day: Optional[datetime] = None
    ) -> WorkloadAssessment:
        """
        Perform comprehensive workload assessment.

        Args:
            source_tables: List of source table metadata with size info
            code: Optional PySpark code for operation analysis
            historical_runs: Optional list of previous execution metrics
            current_day: Current day for weekday/weekend analysis

        Returns:
            WorkloadAssessment with recommendations
        """
        assessment = WorkloadAssessment(
            complexity=ComplexityLevel.LOW,
            data_volume=DataVolumeMetrics(),
            operations=OperationMetrics()
        )

        # Analyze data volume
        if self.config.analyze_input_size:
            assessment.data_volume = self._analyze_data_volume(source_tables)

        # Analyze operations in code
        if code:
            assessment.operations = self._analyze_operations(code)

        # Analyze historical metrics
        if historical_runs and self.config.use_historical_trends:
            assessment.historical = self._analyze_historical(historical_runs)

        # Detect data skew
        if self.config.detect_data_skew:
            assessment.skew_detected, assessment.skew_columns = self._detect_skew(source_tables)

        # Determine complexity
        assessment.complexity = self._determine_complexity(assessment)

        # Generate recommendations
        assessment.primary_recommendation = self._generate_recommendation(assessment, current_day)
        assessment.alternative_recommendations = self._generate_alternatives(assessment)

        # Generate warnings and optimization opportunities
        assessment.warnings = self._generate_warnings(assessment)
        assessment.optimization_opportunities = self._generate_optimizations(assessment)

        return assessment

    def _analyze_data_volume(self, source_tables: List[Dict[str, Any]]) -> DataVolumeMetrics:
        """Analyze total data volume from source tables."""
        metrics = DataVolumeMetrics()

        total_bytes = 0
        total_rows = 0
        partition_sizes = []

        for table in source_tables:
            table_bytes = table.get("size_bytes", 0)
            table_rows = table.get("row_count", 0)

            total_bytes += table_bytes
            total_rows += table_rows

            # Get partition info if available
            partitions = table.get("partitions", [])
            for p in partitions:
                partition_sizes.append(p.get("size_bytes", 0))

        metrics.total_bytes = total_bytes
        metrics.total_rows = total_rows
        metrics.avg_row_size_bytes = total_bytes // total_rows if total_rows > 0 else 0

        if partition_sizes:
            metrics.num_partitions = len(partition_sizes)
            metrics.max_partition_bytes = max(partition_sizes)
            metrics.min_partition_bytes = min(partition_sizes)
            avg_partition = sum(partition_sizes) / len(partition_sizes)
            if avg_partition > 0:
                metrics.partition_skew_factor = metrics.max_partition_bytes / avg_partition

        return metrics

    def _analyze_operations(self, code: str) -> OperationMetrics:
        """Analyze operations in PySpark code."""
        metrics = OperationMetrics()

        # Count joins
        join_matches = re.findall(r'\.join\([^)]*,\s*[\'"]?(\w+)[\'"]?\s*\)', code, re.IGNORECASE)
        metrics.num_joins = len(re.findall(r'\.join\(', code, re.IGNORECASE))
        metrics.join_types = list(set(join_matches)) if join_matches else ["inner"]

        # Check for broadcast
        metrics.has_broadcast_join = "broadcast(" in code.lower()

        # Count aggregations
        metrics.num_aggregations = len(re.findall(r'\.(?:groupBy|agg|count|sum|avg|min|max)\(', code, re.IGNORECASE))

        # Count window functions
        metrics.num_window_functions = len(re.findall(r'Window\.|over\(|partitionBy\(', code, re.IGNORECASE))

        # Count UDFs
        metrics.num_udfs = len(re.findall(r'@udf|udf\(|UserDefinedFunction', code, re.IGNORECASE))

        # Estimate shuffles
        shuffle_operations = len(re.findall(r'\.(?:groupBy|join|repartition|distinct|orderBy|sort)\(', code, re.IGNORECASE))
        metrics.num_shuffles = shuffle_operations

        return metrics

    def _analyze_historical(self, historical_runs: List[Dict[str, Any]]) -> HistoricalMetrics:
        """Analyze historical execution metrics."""
        metrics = HistoricalMetrics()

        if not historical_runs:
            return metrics

        # Calculate averages
        durations = [r.get("duration_seconds", 0) for r in historical_runs]
        cpu_utils = [r.get("cpu_utilization", 0) for r in historical_runs]
        mem_utils = [r.get("memory_utilization", 0) for r in historical_runs]
        shuffle_reads = [r.get("shuffle_read_bytes", 0) for r in historical_runs]
        shuffle_writes = [r.get("shuffle_write_bytes", 0) for r in historical_runs]
        s3_reads = [r.get("s3_read_bytes", 0) for r in historical_runs]
        s3_writes = [r.get("s3_write_bytes", 0) for r in historical_runs]
        costs = [r.get("cost", 0) for r in historical_runs]
        failures = [1 if r.get("status") == "FAILED" else 0 for r in historical_runs]

        metrics.avg_duration_seconds = int(sum(durations) / len(durations)) if durations else 0
        metrics.avg_cpu_utilization = sum(cpu_utils) / len(cpu_utils) if cpu_utils else 0
        metrics.avg_memory_utilization = sum(mem_utils) / len(mem_utils) if mem_utils else 0
        metrics.avg_shuffle_read_bytes = int(sum(shuffle_reads) / len(shuffle_reads)) if shuffle_reads else 0
        metrics.avg_shuffle_write_bytes = int(sum(shuffle_writes) / len(shuffle_writes)) if shuffle_writes else 0
        metrics.avg_s3_read_bytes = int(sum(s3_reads) / len(s3_reads)) if s3_reads else 0
        metrics.avg_s3_write_bytes = int(sum(s3_writes) / len(s3_writes)) if s3_writes else 0
        metrics.avg_cost = sum(costs) / len(costs) if costs else 0
        metrics.failure_rate = sum(failures) / len(failures) if failures else 0

        # Determine trend
        if len(durations) >= 3:
            recent_avg = sum(durations[-3:]) / 3
            older_avg = sum(durations[:-3]) / max(len(durations) - 3, 1)
            if recent_avg > older_avg * 1.2:
                metrics.trend_direction = "increasing"
            elif recent_avg < older_avg * 0.8:
                metrics.trend_direction = "decreasing"

        # Analyze weekday vs weekend patterns
        weekday_runs = [r for r in historical_runs if r.get("day_of_week", 0) < 5]
        weekend_runs = [r for r in historical_runs if r.get("day_of_week", 0) >= 5]

        if weekday_runs:
            weekday_volume = sum(r.get("input_bytes", 0) for r in weekday_runs) / len(weekday_runs)
        else:
            weekday_volume = 1

        if weekend_runs:
            weekend_volume = sum(r.get("input_bytes", 0) for r in weekend_runs) / len(weekend_runs)
        else:
            weekend_volume = 1

        avg_volume = (weekday_volume + weekend_volume) / 2 if (weekday_volume + weekend_volume) > 0 else 1
        metrics.weekday_volume_multiplier = weekday_volume / avg_volume if avg_volume > 0 else 1.0
        metrics.weekend_volume_multiplier = weekend_volume / avg_volume if avg_volume > 0 else 1.0

        return metrics

    def _detect_skew(self, source_tables: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Detect data skew in source tables."""
        skew_detected = False
        skew_columns = []

        for table in source_tables:
            # Check partition skew
            partitions = table.get("partitions", [])
            if len(partitions) >= 2:
                sizes = [p.get("size_bytes", 0) for p in partitions]
                avg_size = sum(sizes) / len(sizes)
                max_size = max(sizes)

                if avg_size > 0 and max_size / avg_size > 5:  # 5x skew factor
                    skew_detected = True
                    skew_columns.append(table.get("partition_column", "unknown"))

            # Check column value distribution if available
            column_stats = table.get("column_stats", {})
            for col_name, stats in column_stats.items():
                if stats.get("distinct_ratio", 1.0) < 0.01:  # Low cardinality
                    skew_detected = True
                    skew_columns.append(col_name)

        return skew_detected, list(set(skew_columns))

    def _determine_complexity(self, assessment: WorkloadAssessment) -> ComplexityLevel:
        """Determine workload complexity level."""
        score = 0

        # Data volume scoring
        data_gb = assessment.data_volume.total_bytes / (1024 ** 3)
        if data_gb > 1000:
            score += 4
        elif data_gb > 100:
            score += 3
        elif data_gb > 10:
            score += 2
        elif data_gb > 1:
            score += 1

        # Operations scoring
        if assessment.operations.num_joins > 5:
            score += 3
        elif assessment.operations.num_joins > 2:
            score += 2
        elif assessment.operations.num_joins > 0:
            score += 1

        if assessment.operations.num_window_functions > 3:
            score += 2
        elif assessment.operations.num_window_functions > 0:
            score += 1

        if assessment.operations.num_udfs > 0:
            score += 2

        if assessment.operations.num_shuffles > 5:
            score += 2
        elif assessment.operations.num_shuffles > 2:
            score += 1

        # Skew scoring
        if assessment.skew_detected:
            score += 2

        # Historical failure scoring
        if assessment.historical and assessment.historical.failure_rate > 0.1:
            score += 2

        # Determine level
        if score >= 10:
            return ComplexityLevel.VERY_HIGH
        elif score >= 6:
            return ComplexityLevel.HIGH
        elif score >= 3:
            return ComplexityLevel.MEDIUM
        else:
            return ComplexityLevel.LOW

    def _generate_recommendation(
        self,
        assessment: WorkloadAssessment,
        current_day: Optional[datetime] = None
    ) -> ResourceRecommendation:
        """Generate primary resource recommendation."""
        data_gb = assessment.data_volume.total_bytes / (1024 ** 3)

        # Determine volume multiplier based on day
        volume_multiplier = 1.0
        if current_day and assessment.historical:
            if self.config.consider_weekday_weekend:
                if current_day.weekday() < 5:  # Weekday
                    volume_multiplier = assessment.historical.weekday_volume_multiplier
                else:  # Weekend
                    volume_multiplier = assessment.historical.weekend_volume_multiplier

        adjusted_data_gb = data_gb * volume_multiplier

        # Choose platform based on complexity and data size
        if assessment.complexity == ComplexityLevel.LOW and adjusted_data_gb < 10:
            platform = Platform.GLUE
            worker_type = WorkerType.GLUE_G1X
            num_workers = max(2, int(adjusted_data_gb / 2) + 1)
        elif assessment.complexity == ComplexityLevel.MEDIUM or adjusted_data_gb < 100:
            platform = Platform.GLUE
            worker_type = WorkerType.GLUE_G2X
            num_workers = max(5, int(adjusted_data_gb / 10) + 2)
        elif assessment.complexity == ComplexityLevel.HIGH or adjusted_data_gb < 500:
            platform = Platform.EMR
            worker_type = WorkerType.EMR_R5_2XLARGE
            num_workers = max(10, int(adjusted_data_gb / 50) + 5)
        else:
            platform = Platform.EKS
            worker_type = WorkerType.EKS_SPOT
            num_workers = max(20, int(adjusted_data_gb / 100) + 10)

        # Adjust for skew
        if assessment.skew_detected:
            num_workers = int(num_workers * 1.5)

        # Adjust for many joins
        if assessment.operations.num_joins > 3:
            num_workers = int(num_workers * 1.3)

        # Calculate memory and cores
        memory_gb = self.memory_per_worker.get(worker_type, 16)
        executor_memory = f"{int(memory_gb * 0.75)}g"
        executor_cores = 4 if memory_gb >= 32 else 2

        # Estimate duration and cost
        if assessment.historical and assessment.historical.avg_duration_seconds > 0:
            estimated_duration = int(assessment.historical.avg_duration_seconds / 60 * volume_multiplier)
        else:
            # Rough estimate: 1 minute per GB with complexity factor
            complexity_factor = {
                ComplexityLevel.LOW: 1.0,
                ComplexityLevel.MEDIUM: 1.5,
                ComplexityLevel.HIGH: 2.0,
                ComplexityLevel.VERY_HIGH: 3.0
            }
            estimated_duration = max(5, int(adjusted_data_gb * complexity_factor[assessment.complexity]))

        cost_per_hour = self.cost_per_hour.get(worker_type, 0.5) * num_workers
        estimated_cost = cost_per_hour * (estimated_duration / 60)

        # Generate Spark configs
        spark_configs = self._generate_spark_configs(assessment, num_workers, memory_gb)

        # Determine if flex mode / spot should be used
        use_spot = self.config.use_spot if hasattr(self.config, 'use_spot') else (platform == Platform.EKS)
        flex_mode = adjusted_data_gb > 50 and assessment.historical is not None and assessment.historical.failure_rate < 0.1

        # Generate reasoning
        reasoning = self._generate_reasoning(assessment, platform, worker_type, num_workers)

        return ResourceRecommendation(
            platform=platform,
            worker_type=worker_type,
            num_workers=num_workers,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            driver_memory=f"{int(memory_gb * 0.5)}g",
            spark_configs=spark_configs,
            estimated_duration_minutes=estimated_duration,
            estimated_cost=round(estimated_cost, 2),
            use_spot=use_spot,
            use_graviton=self.config.use_graviton if hasattr(self.config, 'use_graviton') else False,
            flex_mode=flex_mode,
            confidence=0.8 if assessment.historical else 0.6,
            reasoning=reasoning
        )

    def _generate_spark_configs(
        self,
        assessment: WorkloadAssessment,
        num_workers: int,
        memory_gb: int
    ) -> Dict[str, str]:
        """Generate optimized Spark configurations."""
        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": str(max(2, num_workers // 2)),
            "spark.dynamicAllocation.maxExecutors": str(num_workers * 2),
        }

        # Shuffle optimization
        if assessment.operations.num_shuffles > 2:
            shuffle_partitions = max(200, num_workers * 4)
            configs["spark.sql.shuffle.partitions"] = str(shuffle_partitions)
            configs["spark.sql.adaptive.advisoryPartitionSizeInBytes"] = "128m"

        # Memory optimization
        if assessment.complexity in [ComplexityLevel.HIGH, ComplexityLevel.VERY_HIGH]:
            configs["spark.memory.fraction"] = "0.8"
            configs["spark.memory.storageFraction"] = "0.3"

        # Skew handling
        if assessment.skew_detected:
            configs["spark.sql.adaptive.skewJoin.enabled"] = "true"
            configs["spark.sql.adaptive.skewJoin.skewedPartitionFactor"] = "5"
            configs["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"] = "256m"

        # Broadcast threshold
        if assessment.operations.num_joins > 0:
            configs["spark.sql.autoBroadcastJoinThreshold"] = "52428800"  # 50MB

        # Speculation for long-running jobs
        if assessment.historical and assessment.historical.avg_duration_seconds > 1800:
            configs["spark.speculation"] = "true"
            configs["spark.speculation.multiplier"] = "1.5"

        return configs

    def _generate_alternatives(self, assessment: WorkloadAssessment) -> List[ResourceRecommendation]:
        """Generate alternative recommendations."""
        alternatives = []

        primary = assessment.primary_recommendation
        if not primary:
            return alternatives

        # Cost-optimized alternative
        if primary.platform == Platform.EMR:
            cost_opt = ResourceRecommendation(
                platform=Platform.GLUE,
                worker_type=WorkerType.GLUE_G4X,
                num_workers=max(5, primary.num_workers // 2),
                executor_memory="48g",
                executor_cores=8,
                driver_memory="24g",
                estimated_duration_minutes=int(primary.estimated_duration_minutes * 1.3),
                estimated_cost=round(primary.estimated_cost * 0.7, 2),
                use_spot=False,
                flex_mode=True,
                confidence=0.7,
                reasoning=["Cost-optimized: Using Glue with flex mode for lower cost"]
            )
            alternatives.append(cost_opt)

        # Performance-optimized alternative
        if primary.platform == Platform.GLUE:
            perf_opt = ResourceRecommendation(
                platform=Platform.EMR,
                worker_type=WorkerType.EMR_R5_2XLARGE,
                num_workers=max(10, primary.num_workers * 2),
                executor_memory="48g",
                executor_cores=8,
                driver_memory="32g",
                estimated_duration_minutes=int(primary.estimated_duration_minutes * 0.6),
                estimated_cost=round(primary.estimated_cost * 1.5, 2),
                use_spot=True,
                confidence=0.7,
                reasoning=["Performance-optimized: Using EMR with spot instances for faster execution"]
            )
            alternatives.append(perf_opt)

        # EKS with Karpenter alternative
        if self.config.use_karpenter if hasattr(self.config, 'use_karpenter') else False:
            eks_opt = ResourceRecommendation(
                platform=Platform.EKS,
                worker_type=WorkerType.EKS_GRAVITON,
                num_workers=max(10, primary.num_workers),
                executor_memory="14g",
                executor_cores=4,
                driver_memory="8g",
                estimated_duration_minutes=int(primary.estimated_duration_minutes * 0.8),
                estimated_cost=round(primary.estimated_cost * 0.5, 2),
                use_spot=True,
                use_graviton=True,
                confidence=0.65,
                reasoning=[
                    "EKS with Karpenter: Auto-scaling with spot and graviton for cost efficiency",
                    "Requires EKS cluster with Karpenter configured"
                ]
            )
            alternatives.append(eks_opt)

        return alternatives

    def _generate_warnings(self, assessment: WorkloadAssessment) -> List[str]:
        """Generate warnings based on assessment."""
        warnings = []

        # Data volume warnings
        data_gb = assessment.data_volume.total_bytes / (1024 ** 3)
        if data_gb > 1000:
            warnings.append(f"Very large dataset ({data_gb:.0f} GB). Consider incremental processing.")

        # Skew warnings
        if assessment.skew_detected:
            warnings.append(
                f"Data skew detected in columns: {', '.join(assessment.skew_columns)}. "
                "May cause uneven task distribution."
            )

        # Partition skew
        if assessment.data_volume.partition_skew_factor > 10:
            warnings.append(
                f"High partition skew factor ({assessment.data_volume.partition_skew_factor:.1f}x). "
                "Consider repartitioning data."
            )

        # Historical failure rate
        if assessment.historical and assessment.historical.failure_rate > 0.2:
            warnings.append(
                f"High historical failure rate ({assessment.historical.failure_rate:.0%}). "
                "Review error logs for root cause."
            )

        # Trend warning
        if assessment.historical and assessment.historical.trend_direction == "increasing":
            warnings.append(
                "Execution time trend is increasing. Consider performance optimization."
            )

        # UDF warning
        if assessment.operations.num_udfs > 0:
            warnings.append(
                f"Found {assessment.operations.num_udfs} UDFs. Consider replacing with built-in functions."
            )

        return warnings

    def _generate_optimizations(self, assessment: WorkloadAssessment) -> List[str]:
        """Generate optimization opportunities."""
        opportunities = []

        # Join optimizations
        if assessment.operations.num_joins > 0 and not assessment.operations.has_broadcast_join:
            opportunities.append(
                "Add broadcast hints for small dimension tables in joins."
            )

        # Caching opportunity
        if assessment.operations.num_aggregations > 2:
            opportunities.append(
                "Consider caching intermediate DataFrames used in multiple aggregations."
            )

        # Partition optimization
        data_gb = assessment.data_volume.total_bytes / (1024 ** 3)
        if data_gb > 10 and assessment.data_volume.num_partitions < 100:
            opportunities.append(
                "Increase partition count for better parallelism with large datasets."
            )

        # AQE recommendation
        opportunities.append(
            "Ensure Adaptive Query Execution (AQE) is enabled for runtime optimization."
        )

        # Skew handling
        if assessment.skew_detected:
            opportunities.append(
                "Enable skew join optimization or implement salting for skewed keys."
            )

        return opportunities

    def _generate_reasoning(
        self,
        assessment: WorkloadAssessment,
        platform: Platform,
        worker_type: WorkerType,
        num_workers: int
    ) -> List[str]:
        """Generate reasoning for the recommendation."""
        reasoning = []

        data_gb = assessment.data_volume.total_bytes / (1024 ** 3)

        # Platform reasoning
        if platform == Platform.GLUE:
            reasoning.append(f"Glue selected: Managed service suitable for {data_gb:.1f} GB workload")
        elif platform == Platform.EMR:
            reasoning.append(f"EMR selected: Better control for complex {assessment.complexity.value} workload")
        elif platform == Platform.EKS:
            reasoning.append("EKS selected: Kubernetes flexibility for very large scale")

        # Worker type reasoning
        reasoning.append(
            f"Worker type {worker_type.value}: "
            f"{self.memory_per_worker.get(worker_type, 16)} GB memory per worker"
        )

        # Worker count reasoning
        if assessment.historical:
            reasoning.append(
                f"{num_workers} workers based on historical performance "
                f"(avg duration: {assessment.historical.avg_duration_seconds // 60} min)"
            )
        else:
            reasoning.append(f"{num_workers} workers estimated for {data_gb:.1f} GB input")

        # Complexity reasoning
        reasoning.append(f"Complexity: {assessment.complexity.value}")

        return reasoning

    def generate_assessment_report(self, assessment: WorkloadAssessment) -> str:
        """Generate a markdown report of the assessment."""
        report = []
        report.append("# Workload Assessment Report")
        report.append(f"\n**Complexity Level:** {assessment.complexity.value.upper()}")

        # Data Volume
        report.append("\n## Data Volume")
        dv = assessment.data_volume
        report.append(f"- Total Size: {dv.total_bytes / (1024**3):.2f} GB")
        report.append(f"- Total Rows: {dv.total_rows:,}")
        report.append(f"- Partitions: {dv.num_partitions}")
        if dv.partition_skew_factor > 1:
            report.append(f"- Partition Skew Factor: {dv.partition_skew_factor:.1f}x")

        # Operations
        report.append("\n## Operations Analysis")
        ops = assessment.operations
        report.append(f"- Joins: {ops.num_joins}")
        report.append(f"- Aggregations: {ops.num_aggregations}")
        report.append(f"- Window Functions: {ops.num_window_functions}")
        report.append(f"- UDFs: {ops.num_udfs}")
        report.append(f"- Estimated Shuffles: {ops.num_shuffles}")

        # Recommendation
        if assessment.primary_recommendation:
            rec = assessment.primary_recommendation
            report.append("\n## Primary Recommendation")
            report.append(f"- **Platform:** {rec.platform.value}")
            report.append(f"- **Worker Type:** {rec.worker_type.value}")
            report.append(f"- **Workers:** {rec.num_workers}")
            report.append(f"- **Executor Memory:** {rec.executor_memory}")
            report.append(f"- **Estimated Duration:** {rec.estimated_duration_minutes} minutes")
            report.append(f"- **Estimated Cost:** ${rec.estimated_cost:.2f}")
            report.append(f"- **Confidence:** {rec.confidence:.0%}")

            report.append("\n### Reasoning")
            for r in rec.reasoning:
                report.append(f"- {r}")

            report.append("\n### Spark Configurations")
            report.append("```")
            for k, v in rec.spark_configs.items():
                report.append(f'{k}={v}')
            report.append("```")

        # Warnings
        if assessment.warnings:
            report.append("\n## Warnings")
            for w in assessment.warnings:
                report.append(f"- ⚠️ {w}")

        # Optimizations
        if assessment.optimization_opportunities:
            report.append("\n## Optimization Opportunities")
            for o in assessment.optimization_opportunities:
                report.append(f"- 💡 {o}")

        return "\n".join(report)
