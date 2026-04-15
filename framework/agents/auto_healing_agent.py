#!/usr/bin/env python3
"""
Auto-Healing Agent
==================

Intelligent agent that:
1. Analyzes errors and determines if they can be healed
2. Generates code fixes for common PySpark issues
3. Provides healing strategies for different error types
4. Can modify and rerun jobs automatically

Healable Error Categories:
- Memory errors (OOM, GC overhead)
- Shuffle errors (fetch failed, disk space)
- Timeout errors
- Connection errors
- Data skew issues
- Partition errors

Code Fixes Applied:
- Add broadcast hints for small tables
- Apply repartitioning to fix skew
- Add caching for repeated computations
- Optimize join strategies
- Increase memory/executor configs
"""

import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class HealingStrategy(Enum):
    """Available healing strategies."""
    INCREASE_MEMORY = "increase_memory"
    INCREASE_EXECUTORS = "increase_executors"
    ADD_BROADCAST = "add_broadcast"
    REPARTITION = "repartition"
    ADD_CACHING = "add_caching"
    OPTIMIZE_JOINS = "optimize_joins"
    FIX_SKEW = "fix_skew"
    RETRY = "simple_retry"
    PLATFORM_SWITCH = "platform_switch"
    REDUCE_PARALLELISM = "reduce_parallelism"
    INCREASE_TIMEOUT = "increase_timeout"


@dataclass
class HealingResult:
    """Result of healing analysis."""
    can_heal: bool = False
    strategy: Optional[HealingStrategy] = None
    strategies: List[HealingStrategy] = field(default_factory=list)
    code_fixes: List[Dict[str, Any]] = field(default_factory=list)
    config_changes: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    confidence: float = 0.0


class AutoHealingAgent:
    """
    Agent that analyzes errors and provides healing strategies with code fixes.
    """

    def __init__(self, config):
        self.config = config

        # Error patterns and their healing strategies
        self.error_patterns = self._init_error_patterns()

    def _init_error_patterns(self) -> List[Dict]:
        """Initialize error patterns with healing strategies."""
        return [
            # Memory errors
            {
                "pattern": r"java\.lang\.OutOfMemoryError|GC overhead limit exceeded",
                "category": "memory",
                "strategies": [HealingStrategy.INCREASE_MEMORY, HealingStrategy.ADD_CACHING],
                "config_changes": {
                    "spark.executor.memory": "increase_50_pct",
                    "spark.driver.memory": "increase_50_pct",
                    "spark.memory.fraction": "0.8"
                },
                "code_fixes": [
                    {
                        "type": "add_config",
                        "description": "Increase executor memory and tune GC",
                        "code": """
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.sql.adaptive.enabled", "true")
"""
                    },
                    {
                        "type": "add_caching",
                        "description": "Cache intermediate results to reduce recomputation",
                        "pattern": r"(df\s*=\s*[^.]+\.(?:join|groupBy|agg)[^;]+)",
                        "replacement": r"\1.cache()"
                    }
                ],
                "recommendations": [
                    "Consider using G.2X workers instead of G.1X for memory-intensive operations",
                    "Add .cache() before operations that reuse the same DataFrame",
                    "Use broadcast joins for small lookup tables"
                ]
            },

            # Shuffle errors
            {
                "pattern": r"FetchFailedException|shuffle.*failed|disk space|No space left",
                "category": "shuffle",
                "strategies": [HealingStrategy.REPARTITION, HealingStrategy.REDUCE_PARALLELISM],
                "config_changes": {
                    "spark.sql.shuffle.partitions": "increase_double",
                    "spark.shuffle.file.buffer": "64k",
                    "spark.reducer.maxSizeInFlight": "96m"
                },
                "code_fixes": [
                    {
                        "type": "add_config",
                        "description": "Optimize shuffle configuration",
                        "code": """
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
"""
                    },
                    {
                        "type": "add_repartition",
                        "description": "Add repartition before heavy operations",
                        "pattern": r"(\.groupBy\([^)]+\)\.agg)",
                        "replacement": r".repartition(200)\1"
                    }
                ],
                "recommendations": [
                    "Increase shuffle partitions for large datasets",
                    "Enable adaptive query execution (AQE)",
                    "Consider increasing temp storage on workers"
                ]
            },

            # Data skew
            {
                "pattern": r"skew|single task.*slow|one task.*failed|stage.*taking too long",
                "category": "skew",
                "strategies": [HealingStrategy.FIX_SKEW, HealingStrategy.ADD_BROADCAST],
                "config_changes": {
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
                    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256m"
                },
                "code_fixes": [
                    {
                        "type": "add_config",
                        "description": "Enable skew join optimization",
                        "code": """
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
"""
                    },
                    {
                        "type": "add_salting",
                        "description": "Add salting for skewed join keys",
                        "code_template": """
# Salt the skewed key
from pyspark.sql.functions import concat, lit, floor, rand

# Add salt to the larger table
df_salted = df.withColumn("salt", floor(rand() * 10))
df_salted = df_salted.withColumn("salted_key", concat(col("{key}"), lit("_"), col("salt")))

# Explode salt on the smaller table
from pyspark.sql.functions import explode, array
df_small_exploded = df_small.withColumn("salt", explode(array([lit(i) for i in range(10)])))
df_small_exploded = df_small_exploded.withColumn("salted_key", concat(col("{key}"), lit("_"), col("salt")))

# Join on salted key
result = df_salted.join(df_small_exploded, "salted_key")
"""
                    }
                ],
                "recommendations": [
                    "Enable adaptive skew join in Spark",
                    "Consider salting heavily skewed keys",
                    "Use broadcast join for small dimension tables",
                    "Analyze data distribution with df.groupBy(key).count().orderBy(desc('count'))"
                ]
            },

            # Timeout errors
            {
                "pattern": r"timeout|timed out|deadline exceeded|connection reset",
                "category": "timeout",
                "strategies": [HealingStrategy.INCREASE_TIMEOUT, HealingStrategy.RETRY],
                "config_changes": {
                    "spark.network.timeout": "800s",
                    "spark.sql.broadcastTimeout": "600",
                    "spark.rpc.askTimeout": "600s"
                },
                "code_fixes": [
                    {
                        "type": "add_config",
                        "description": "Increase timeout configurations",
                        "code": """
spark.conf.set("spark.network.timeout", "800s")
spark.conf.set("spark.sql.broadcastTimeout", "600")
spark.conf.set("spark.executor.heartbeatInterval", "60s")
"""
                    }
                ],
                "recommendations": [
                    "Increase network and broadcast timeouts",
                    "Check for network issues between driver and executors",
                    "Consider reducing broadcast threshold if tables are large"
                ]
            },

            # Connection errors
            {
                "pattern": r"connection refused|unable to connect|socket.*exception|network unreachable",
                "category": "connection",
                "strategies": [HealingStrategy.RETRY],
                "config_changes": {
                    "spark.sql.files.maxPartitionBytes": "134217728",
                    "spark.hadoop.fs.s3a.connection.maximum": "200"
                },
                "code_fixes": [
                    {
                        "type": "add_retry",
                        "description": "Add retry logic for transient failures",
                        "code_template": """
import time
from functools import wraps

def retry_on_failure(max_retries=3, delay=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        time.sleep(delay * (attempt + 1))
                    else:
                        raise
        return wrapper
    return decorator
"""
                    }
                ],
                "recommendations": [
                    "Check VPC and security group configurations",
                    "Verify S3 endpoint connectivity",
                    "Add retry logic for transient network failures"
                ]
            },

            # Partition errors
            {
                "pattern": r"too many partitions|partition.*overflow|max.*partitions",
                "category": "partition",
                "strategies": [HealingStrategy.REPARTITION, HealingStrategy.REDUCE_PARALLELISM],
                "config_changes": {
                    "spark.sql.files.maxPartitionBytes": "268435456",
                    "spark.sql.shuffle.partitions": "200"
                },
                "code_fixes": [
                    {
                        "type": "add_coalesce",
                        "description": "Reduce partition count with coalesce",
                        "pattern": r"(\.write\.)",
                        "replacement": r".coalesce(100)\1"
                    }
                ],
                "recommendations": [
                    "Use coalesce() instead of repartition() when reducing partitions",
                    "Set appropriate partition size (128MB-256MB recommended)",
                    "Avoid partitioning by high-cardinality columns"
                ]
            },

            # Join optimization opportunities
            {
                "pattern": r"broadcast.*too large|auto broadcast.*disabled|SortMergeJoin",
                "category": "join",
                "strategies": [HealingStrategy.OPTIMIZE_JOINS, HealingStrategy.ADD_BROADCAST],
                "config_changes": {
                    "spark.sql.autoBroadcastJoinThreshold": "52428800"
                },
                "code_fixes": [
                    {
                        "type": "add_broadcast",
                        "description": "Add explicit broadcast hints for small tables",
                        "code_template": """
from pyspark.sql.functions import broadcast

# Broadcast small dimension tables
result = large_df.join(broadcast(small_df), "key")
"""
                    }
                ],
                "recommendations": [
                    "Use broadcast() for tables < 10MB",
                    "Consider increasing autoBroadcastJoinThreshold",
                    "Pre-filter before joining to reduce data volume"
                ]
            }
        ]

    def analyze_and_heal(self, error: Exception, config) -> HealingResult:
        """
        Analyze an error and determine if/how it can be healed.
        Returns healing strategies and code fixes.
        """
        error_str = str(error).lower()
        error_full = f"{type(error).__name__}: {error}"

        result = HealingResult()

        # Match against known patterns
        for pattern_info in self.error_patterns:
            if re.search(pattern_info["pattern"], error_full, re.IGNORECASE):
                # Check if this error type is healable by config
                category = pattern_info["category"]

                if category == "memory" and not self.config.heal_memory_errors:
                    continue
                if category == "shuffle" and not self.config.heal_shuffle_errors:
                    continue
                if category == "timeout" and not self.config.heal_timeout_errors:
                    continue
                if category == "connection" and not self.config.heal_connection_errors:
                    continue
                if category == "skew" and not self.config.heal_data_skew:
                    continue
                if category == "partition" and not self.config.heal_partition_errors:
                    continue

                result.can_heal = True
                result.strategies.extend(pattern_info["strategies"])
                result.strategy = pattern_info["strategies"][0]
                result.config_changes.update(pattern_info.get("config_changes", {}))
                result.code_fixes.extend(pattern_info.get("code_fixes", []))
                result.recommendations.extend(pattern_info.get("recommendations", []))
                result.confidence = 0.8

                break

        if not result.can_heal:
            result.recommendations.append(
                f"Unknown error type: {type(error).__name__}. Manual investigation required."
            )

        return result

    def generate_fixed_code(self, original_code: str, fixes: List[Dict]) -> str:
        """
        Apply fixes to the original code and return modified version.
        """
        modified_code = original_code

        for fix in fixes:
            fix_type = fix.get("type")

            if fix_type == "add_config":
                # Add config statements at the beginning after SparkSession creation
                config_code = fix.get("code", "")
                # Find spark session creation and add after it
                pattern = r"(spark\s*=\s*SparkSession\.builder[^.]+\.getOrCreate\(\))"
                replacement = rf"\1\n\n# Auto-healing: {fix.get('description', 'Config fix')}\n{config_code}"
                modified_code = re.sub(pattern, replacement, modified_code, flags=re.DOTALL)

            elif fix_type == "add_caching":
                # Add .cache() to heavy operations
                pattern = fix.get("pattern")
                replacement = fix.get("replacement")
                if pattern and replacement:
                    modified_code = re.sub(pattern, replacement, modified_code)

            elif fix_type == "add_repartition":
                # Add repartition before groupBy
                pattern = fix.get("pattern")
                replacement = fix.get("replacement")
                if pattern and replacement:
                    modified_code = re.sub(pattern, replacement, modified_code)

            elif fix_type == "add_coalesce":
                # Add coalesce before write
                pattern = fix.get("pattern")
                replacement = fix.get("replacement")
                if pattern and replacement:
                    modified_code = re.sub(pattern, replacement, modified_code)

        return modified_code

    def get_platform_switch_recommendation(self, error: Exception, current_platform: str) -> Dict:
        """
        Recommend platform switch based on error type.
        """
        error_str = str(error).lower()

        recommendations = {
            "should_switch": False,
            "recommended_platform": current_platform,
            "reason": ""
        }

        # Memory issues on Glue -> suggest EMR with more memory
        if "outofmemory" in error_str and current_platform == "glue":
            recommendations["should_switch"] = True
            recommendations["recommended_platform"] = "emr"
            recommendations["reason"] = "Memory issues on Glue. EMR allows custom memory configuration."

        # Long-running job on Lambda -> suggest Glue
        if "timeout" in error_str and current_platform == "lambda":
            recommendations["should_switch"] = True
            recommendations["recommended_platform"] = "glue"
            recommendations["reason"] = "Lambda timeout. Glue supports longer-running jobs."

        # Complex shuffle on Glue -> suggest EMR
        if "shuffle" in error_str and current_platform == "glue":
            recommendations["should_switch"] = True
            recommendations["recommended_platform"] = "emr"
            recommendations["reason"] = "Shuffle issues on Glue. EMR provides better shuffle handling."

        return recommendations

    def estimate_healing_time(self, strategy: HealingStrategy) -> int:
        """Estimate time needed for healing in seconds."""
        estimates = {
            HealingStrategy.RETRY: 30,
            HealingStrategy.INCREASE_MEMORY: 120,
            HealingStrategy.INCREASE_EXECUTORS: 180,
            HealingStrategy.ADD_BROADCAST: 60,
            HealingStrategy.REPARTITION: 90,
            HealingStrategy.ADD_CACHING: 60,
            HealingStrategy.OPTIMIZE_JOINS: 120,
            HealingStrategy.FIX_SKEW: 180,
            HealingStrategy.PLATFORM_SWITCH: 300,
            HealingStrategy.REDUCE_PARALLELISM: 60,
            HealingStrategy.INCREASE_TIMEOUT: 30
        }
        return estimates.get(strategy, 60)
