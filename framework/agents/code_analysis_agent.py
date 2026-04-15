#!/usr/bin/env python3
"""
Code Analysis Agent
===================

Intelligent agent that analyzes PySpark code for:
1. Anti-patterns and performance issues
2. Join optimization opportunities
3. Data handling best practices
4. Delta Lake operations optimization
5. AWS service recommendations
6. Memory and shuffle optimization
7. Partition strategy recommendations

Provides actionable recommendations with code snippets.
"""

import re
import ast
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class RecommendationSeverity(Enum):
    """Severity levels for recommendations."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class RecommendationCategory(Enum):
    """Categories of recommendations."""
    PERFORMANCE = "performance"
    MEMORY = "memory"
    SHUFFLE = "shuffle"
    JOIN = "join"
    PARTITION = "partition"
    CACHING = "caching"
    DATA_HANDLING = "data_handling"
    DELTA = "delta"
    AWS_SERVICE = "aws_service"
    SECURITY = "security"
    BEST_PRACTICE = "best_practice"


@dataclass
class CodeRecommendation:
    """A single code recommendation."""
    category: RecommendationCategory
    severity: RecommendationSeverity
    title: str
    description: str
    current_code: str = ""
    suggested_code: str = ""
    line_number: Optional[int] = None
    estimated_impact: str = ""
    references: List[str] = field(default_factory=list)


@dataclass
class AnalysisResult:
    """Result of code analysis."""
    recommendations: List[CodeRecommendation] = field(default_factory=list)
    anti_patterns_found: List[Dict[str, Any]] = field(default_factory=list)
    optimization_score: float = 0.0
    summary: Dict[str, Any] = field(default_factory=dict)


class CodeAnalysisAgent:
    """
    Agent that analyzes PySpark code and provides optimization recommendations.
    """

    def __init__(self, config):
        self.config = config
        self.anti_patterns = self._init_anti_patterns()
        self.optimization_patterns = self._init_optimization_patterns()

    def _init_anti_patterns(self) -> List[Dict]:
        """Initialize anti-pattern detection rules."""
        return [
            # Collect on large datasets
            {
                "pattern": r"\.collect\(\)",
                "name": "collect_on_large_data",
                "severity": RecommendationSeverity.CRITICAL,
                "category": RecommendationCategory.MEMORY,
                "title": "Avoid collect() on large datasets",
                "description": "collect() brings all data to driver memory, causing OOM on large datasets",
                "suggestion": "Use take(n), head(n), or write to storage instead",
                "suggested_code": """# Instead of:
# data = df.collect()

# Use take() for samples:
sample_data = df.take(100)

# Or write to storage:
df.write.mode("overwrite").parquet("s3://bucket/output/")

# Or use toPandas() with limit:
pdf = df.limit(10000).toPandas()"""
            },

            # UDF instead of built-in functions
            {
                "pattern": r"@udf|udf\(|UserDefinedFunction",
                "name": "udf_usage",
                "severity": RecommendationSeverity.HIGH,
                "category": RecommendationCategory.PERFORMANCE,
                "title": "UDF detected - consider built-in functions",
                "description": "UDFs prevent Catalyst optimizer and cause serialization overhead",
                "suggestion": "Replace UDFs with built-in Spark SQL functions when possible",
                "suggested_code": """# Instead of UDF:
# @udf(StringType())
# def upper_case(s):
#     return s.upper() if s else None
# df.withColumn("upper", upper_case(col("name")))

# Use built-in function:
from pyspark.sql.functions import upper
df.withColumn("upper", upper(col("name")))"""
            },

            # Multiple withColumn calls
            {
                "pattern": r"(\.withColumn\([^)]+\)\s*\n?\s*\.withColumn\([^)]+\)\s*\n?\s*\.withColumn\([^)]+\))",
                "name": "chained_withcolumn",
                "severity": RecommendationSeverity.MEDIUM,
                "category": RecommendationCategory.PERFORMANCE,
                "title": "Multiple withColumn calls detected",
                "description": "Chained withColumn calls create multiple DataFrame copies",
                "suggestion": "Use select with multiple columns or withColumns (Spark 3.3+)",
                "suggested_code": """# Instead of multiple withColumn:
# df.withColumn("a", col("x") + 1)
#   .withColumn("b", col("y") * 2)
#   .withColumn("c", upper(col("z")))

# Use select:
df.select(
    "*",
    (col("x") + 1).alias("a"),
    (col("y") * 2).alias("b"),
    upper(col("z")).alias("c")
)

# Or withColumns (Spark 3.3+):
df.withColumns({
    "a": col("x") + 1,
    "b": col("y") * 2,
    "c": upper(col("z"))
})"""
            },

            # Count followed by filter
            {
                "pattern": r"\.count\(\).*\.filter\(|\.filter\(.*\.count\(\)",
                "name": "count_filter_pattern",
                "severity": RecommendationSeverity.MEDIUM,
                "category": RecommendationCategory.PERFORMANCE,
                "title": "Separate count and filter operations",
                "description": "If you need filtered count, filter first then count",
                "suggestion": "Filter before counting to avoid scanning entire dataset twice",
                "suggested_code": """# Inefficient:
# total = df.count()
# filtered_count = df.filter(condition).count()

# Efficient - single pass:
from pyspark.sql.functions import sum, when

counts = df.agg(
    count("*").alias("total"),
    sum(when(condition, 1).otherwise(0)).alias("filtered_count")
).collect()[0]"""
            },

            # Cartesian join / cross join
            {
                "pattern": r"\.crossJoin\(|\.join\([^,]+\)\s*$",
                "name": "cartesian_join",
                "severity": RecommendationSeverity.CRITICAL,
                "category": RecommendationCategory.JOIN,
                "title": "Potential Cartesian join detected",
                "description": "Cartesian joins can explode data volume exponentially",
                "suggestion": "Always specify join conditions explicitly",
                "suggested_code": """# Avoid cartesian join:
# result = df1.crossJoin(df2)

# Always use explicit join conditions:
result = df1.join(df2, df1["key"] == df2["key"], "inner")

# If cross join is needed, make it explicit and limit:
small_df2 = df2.limit(100)
result = df1.crossJoin(small_df2)"""
            },

            # repartition before write without coalesce consideration
            {
                "pattern": r"\.repartition\(\d+\)\.write\.",
                "name": "repartition_before_write",
                "severity": RecommendationSeverity.MEDIUM,
                "category": RecommendationCategory.SHUFFLE,
                "title": "repartition() before write causes full shuffle",
                "description": "Use coalesce() for reducing partitions to avoid full shuffle",
                "suggestion": "Use coalesce() when reducing partitions before write",
                "suggested_code": """# Instead of repartition (causes shuffle):
# df.repartition(10).write.parquet("output/")

# Use coalesce (no shuffle when reducing):
df.coalesce(10).write.parquet("output/")

# Use repartition only when:
# 1. Increasing partition count
# 2. Need even distribution
# 3. Partitioning by column
df.repartition(100, "date_column").write.partitionBy("date_column").parquet("output/")"""
            },

            # Reading without schema
            {
                "pattern": r"spark\.read\.(csv|json|parquet)\([^)]+\)(?!.*schema)",
                "name": "read_without_schema",
                "severity": RecommendationSeverity.MEDIUM,
                "category": RecommendationCategory.DATA_HANDLING,
                "title": "Reading data without explicit schema",
                "description": "Schema inference is expensive and can lead to type issues",
                "suggestion": "Define explicit schema for better performance and reliability",
                "suggested_code": """# Instead of schema inference:
# df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Define explicit schema:
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True)
])

df = spark.read.schema(schema).csv("data.csv", header=True)"""
            },

            # groupBy without aggregation
            {
                "pattern": r"\.groupBy\([^)]+\)\.count\(\)",
                "name": "groupby_count_pattern",
                "severity": RecommendationSeverity.LOW,
                "category": RecommendationCategory.PERFORMANCE,
                "title": "Consider using countDistinct for unique counts",
                "description": "groupBy().count() may be optimizable",
                "suggestion": "For distinct counts, use approx_count_distinct() for better performance",
                "suggested_code": """# For exact distinct count:
df.select(countDistinct("column")).collect()

# For approximate (faster) distinct count:
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("column", 0.05)).collect()  # 5% error margin"""
            },

            # toPandas without limit
            {
                "pattern": r"\.toPandas\(\)(?!\s*#.*limit)",
                "name": "topandas_without_limit",
                "severity": RecommendationSeverity.HIGH,
                "category": RecommendationCategory.MEMORY,
                "title": "toPandas() without limit",
                "description": "toPandas() loads entire DataFrame to driver memory",
                "suggestion": "Always limit data before converting to Pandas",
                "suggested_code": """# Dangerous - loads all data:
# pdf = df.toPandas()

# Safe - limit first:
pdf = df.limit(100000).toPandas()

# Or sample:
pdf = df.sample(fraction=0.01).toPandas()"""
            },

            # Inefficient null handling
            {
                "pattern": r"\.filter\([^)]*==\s*None|\.filter\([^)]*is\s+None",
                "name": "inefficient_null_check",
                "severity": RecommendationSeverity.LOW,
                "category": RecommendationCategory.DATA_HANDLING,
                "title": "Use isNull() for null checks",
                "description": "Python None comparisons may not work correctly in Spark",
                "suggestion": "Use isNull() or isNotNull() functions",
                "suggested_code": """# Instead of Python None check:
# df.filter(col("field") == None)

# Use Spark null functions:
from pyspark.sql.functions import col

df.filter(col("field").isNull())
df.filter(col("field").isNotNull())"""
            },

            # Missing broadcast hint for small tables
            {
                "pattern": r"\.join\([^)]+\)(?!.*broadcast)",
                "name": "missing_broadcast",
                "severity": RecommendationSeverity.MEDIUM,
                "category": RecommendationCategory.JOIN,
                "title": "Consider broadcast hint for small tables",
                "description": "Small dimension tables should be broadcast to avoid shuffle",
                "suggestion": "Use broadcast() for tables under 10MB",
                "suggested_code": """# Instead of regular join:
# result = large_df.join(small_df, "key")

# Broadcast small table:
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "key")

# Or use hint:
result = large_df.join(small_df.hint("broadcast"), "key")"""
            }
        ]

    def _init_optimization_patterns(self) -> List[Dict]:
        """Initialize optimization opportunity patterns."""
        return [
            # Delta Lake optimizations
            {
                "pattern": r"\.write\.format\(['\"]delta['\"]\)",
                "category": RecommendationCategory.DELTA,
                "optimizations": [
                    {
                        "title": "Enable Delta optimizations",
                        "description": "Configure Delta Lake for optimal performance",
                        "code": """# Delta optimization configurations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

# Write with optimization
df.write.format("delta") \\
    .option("optimizeWrite", "true") \\
    .option("mergeSchema", "true") \\
    .mode("overwrite") \\
    .save("s3://bucket/delta-table/")

# Optimize table periodically
spark.sql("OPTIMIZE delta.`s3://bucket/delta-table/`")
spark.sql("VACUUM delta.`s3://bucket/delta-table/` RETAIN 168 HOURS")"""
                    },
                    {
                        "title": "Use Z-ORDER for query optimization",
                        "description": "Z-ORDER columns used frequently in filters",
                        "code": """# Z-ORDER by frequently filtered columns
spark.sql(\"\"\"
    OPTIMIZE delta.`s3://bucket/delta-table/`
    ZORDER BY (date_column, region)
\"\"\")"""
                    }
                ]
            },

            # AWS Glue specific
            {
                "pattern": r"GlueContext|from awsglue",
                "category": RecommendationCategory.AWS_SERVICE,
                "optimizations": [
                    {
                        "title": "Use Glue DynamicFrame for schema flexibility",
                        "description": "DynamicFrame handles schema variations better",
                        "code": """from awsglue.dynamicframe import DynamicFrame

# Convert DataFrame to DynamicFrame
dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# Use relationalize for nested data
dyf_flat = dyf.relationalize("root", "s3://temp-bucket/relationalize/")

# Write with job bookmarks for incremental processing
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://bucket/output/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet"
)"""
                    },
                    {
                        "title": "Enable Glue job bookmarks",
                        "description": "Process only new data in incremental jobs",
                        "code": """# In job parameters, set:
# --job-bookmark-option: job-bookmark-enable

# In code:
from awsglue.context import GlueContext
from awsglue.job import Job

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ... your ETL logic ...

job.commit()  # Important: commits bookmark"""
                    }
                ]
            },

            # Adaptive Query Execution
            {
                "pattern": r"spark\.sql\.|\.join\(|\.groupBy\(",
                "category": RecommendationCategory.PERFORMANCE,
                "optimizations": [
                    {
                        "title": "Enable Adaptive Query Execution (AQE)",
                        "description": "AQE optimizes queries at runtime",
                        "code": """# Enable AQE (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Set advisory partition size
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")"""
                    }
                ]
            },

            # Caching opportunities
            {
                "pattern": r"(df\w*)\s*=.*\n(?:.*\1.*\n){2,}",
                "category": RecommendationCategory.CACHING,
                "optimizations": [
                    {
                        "title": "Cache reused DataFrames",
                        "description": "Cache DataFrames that are used multiple times",
                        "code": """# Cache DataFrame used multiple times
df_cached = df.cache()

# Or use persist for more control
from pyspark import StorageLevel

df_persisted = df.persist(StorageLevel.MEMORY_AND_DISK)

# Remember to unpersist when done
df_cached.unpersist()"""
                    }
                ]
            },

            # Partitioning strategy
            {
                "pattern": r"\.write\.(parquet|orc|json)\(",
                "category": RecommendationCategory.PARTITION,
                "optimizations": [
                    {
                        "title": "Use efficient partitioning strategy",
                        "description": "Partition by low-cardinality columns for better query performance",
                        "code": """# Partition by date for time-series data
df.write \\
    .partitionBy("year", "month", "day") \\
    .mode("overwrite") \\
    .parquet("s3://bucket/output/")

# For high-cardinality, use bucketing
df.write \\
    .bucketBy(100, "customer_id") \\
    .sortBy("order_date") \\
    .saveAsTable("orders_bucketed")

# Control partition file size
spark.conf.set("spark.sql.files.maxRecordsPerFile", "1000000")"""
                    }
                ]
            }
        ]

    def analyze(self, code: str, job_name: str = "") -> AnalysisResult:
        """
        Analyze PySpark code and return recommendations.
        """
        result = AnalysisResult()

        # Check anti-patterns
        if self.config.check_anti_patterns:
            result.anti_patterns_found = self._detect_anti_patterns(code)
            for ap in result.anti_patterns_found:
                result.recommendations.append(CodeRecommendation(
                    category=ap["category"],
                    severity=ap["severity"],
                    title=ap["title"],
                    description=ap["description"],
                    current_code=ap.get("matched_code", ""),
                    suggested_code=ap["suggested_code"],
                    line_number=ap.get("line_number"),
                    estimated_impact=self._estimate_impact(ap["severity"])
                ))

        # Check join optimizations
        if self.config.check_join_optimizations:
            join_recommendations = self._analyze_joins(code)
            result.recommendations.extend(join_recommendations)

        # Check for AWS service recommendations
        if self.config.recommend_aws_tools:
            aws_recommendations = self._recommend_aws_services(code)
            result.recommendations.extend(aws_recommendations)

        # Check Delta optimizations
        if self.config.recommend_delta_optimizations:
            delta_recommendations = self._analyze_delta_operations(code)
            result.recommendations.extend(delta_recommendations)

        # Calculate optimization score
        result.optimization_score = self._calculate_score(result)

        # Generate summary
        result.summary = self._generate_summary(result)

        return result

    def _detect_anti_patterns(self, code: str) -> List[Dict]:
        """Detect anti-patterns in the code."""
        found = []
        lines = code.split('\n')

        for pattern_info in self.anti_patterns:
            for i, line in enumerate(lines):
                if re.search(pattern_info["pattern"], line, re.IGNORECASE):
                    found.append({
                        "name": pattern_info["name"],
                        "category": pattern_info["category"],
                        "severity": pattern_info["severity"],
                        "title": pattern_info["title"],
                        "description": pattern_info["description"],
                        "suggested_code": pattern_info["suggested_code"],
                        "matched_code": line.strip(),
                        "line_number": i + 1
                    })

        return found

    def _analyze_joins(self, code: str) -> List[CodeRecommendation]:
        """Analyze join operations for optimization opportunities."""
        recommendations = []

        # Find all joins
        join_pattern = r"\.join\(([^,]+),\s*([^,]+)(?:,\s*['\"](\w+)['\"])?\)"
        joins = re.findall(join_pattern, code)

        for join in joins:
            right_df, condition, join_type = join[0], join[1], join[2] if len(join) > 2 else "inner"

            # Check for potential broadcast opportunity
            if "broadcast" not in code.lower() or f"broadcast({right_df})" not in code:
                recommendations.append(CodeRecommendation(
                    category=RecommendationCategory.JOIN,
                    severity=RecommendationSeverity.MEDIUM,
                    title=f"Consider broadcast for {right_df.strip()}",
                    description=f"If {right_df.strip()} is a small table (<10MB), broadcast it",
                    suggested_code=f"""from pyspark.sql.functions import broadcast

# If {right_df.strip()} is small, use broadcast:
result = left_df.join(broadcast({right_df.strip()}), {condition})"""
                ))

        # Check for skew join hints
        if ".join(" in code and "skew" not in code.lower():
            recommendations.append(CodeRecommendation(
                category=RecommendationCategory.JOIN,
                severity=RecommendationSeverity.LOW,
                title="Consider skew join handling",
                description="If join keys have skewed distribution, enable AQE skew join",
                suggested_code="""# Enable skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Or use skew hint (Spark 3.0+):
result = df1.hint("skew", "key_column").join(df2, "key_column")"""
            ))

        return recommendations

    def _recommend_aws_services(self, code: str) -> List[CodeRecommendation]:
        """Recommend AWS services based on code patterns."""
        recommendations = []

        # Check for S3 reads - recommend Glue Data Catalog
        if "s3://" in code and "glue" not in code.lower():
            recommendations.append(CodeRecommendation(
                category=RecommendationCategory.AWS_SERVICE,
                severity=RecommendationSeverity.INFO,
                title="Consider AWS Glue Data Catalog",
                description="Use Glue Data Catalog for schema management and discovery",
                suggested_code="""# Instead of direct S3 reads:
# df = spark.read.parquet("s3://bucket/data/")

# Use Glue Catalog:
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://bucket/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")

df = spark.table("glue_catalog.database.table")"""
            ))

        # Check for large data processing - recommend EMR
        if re.search(r"\.repartition\(\d{3,}\)|shuffle.*partitions.*['\"]?\d{3,}", code):
            recommendations.append(CodeRecommendation(
                category=RecommendationCategory.AWS_SERVICE,
                severity=RecommendationSeverity.INFO,
                title="Consider EMR for large-scale processing",
                description="High partition counts suggest large data - EMR may be more cost-effective",
                suggested_code="""# EMR cluster configuration for large workloads:
# Master: m5.xlarge
# Core: r5.2xlarge x 10 (SPOT)
# Task: r5.xlarge x 20 (SPOT with Karpenter)

# EMR-specific optimizations:
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.speculation", "true")"""
            ))

        # Check for ML workloads - recommend SageMaker
        if re.search(r"MLlib|ml\.|VectorAssembler|Pipeline", code):
            recommendations.append(CodeRecommendation(
                category=RecommendationCategory.AWS_SERVICE,
                severity=RecommendationSeverity.INFO,
                title="Consider SageMaker for ML workloads",
                description="For ML training and inference, SageMaker provides managed infrastructure",
                suggested_code="""# Export processed data for SageMaker:
df.write.parquet("s3://bucket/sagemaker-input/")

# Or use SageMaker Processing with Spark:
from sagemaker.spark.processing import PySparkProcessor

spark_processor = PySparkProcessor(
    base_job_name="spark-processor",
    framework_version="3.1",
    role="arn:aws:iam::account:role/SageMakerRole",
    instance_count=2,
    instance_type="ml.m5.xlarge"
)"""
            ))

        return recommendations

    def _analyze_delta_operations(self, code: str) -> List[CodeRecommendation]:
        """Analyze Delta Lake operations for optimization."""
        recommendations = []

        if "delta" in code.lower():
            # Check for MERGE optimization
            if "merge" in code.lower() and "matchedCondition" not in code:
                recommendations.append(CodeRecommendation(
                    category=RecommendationCategory.DELTA,
                    severity=RecommendationSeverity.MEDIUM,
                    title="Optimize Delta MERGE operation",
                    description="Use matched conditions and partitioning for faster MERGE",
                    suggested_code="""from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "s3://bucket/delta-table/")

# Optimized MERGE with conditions
deltaTable.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id AND target.partition_col = source.partition_col"  # Include partition
).whenMatchedUpdate(
    condition="source.updated_at > target.updated_at",  # Only update if newer
    set={"col1": "source.col1", "col2": "source.col2"}
).whenNotMatchedInsert(
    values={"id": "source.id", "col1": "source.col1"}
).execute()"""
                ))

            # Check for vacuum and optimize
            if "vacuum" not in code.lower():
                recommendations.append(CodeRecommendation(
                    category=RecommendationCategory.DELTA,
                    severity=RecommendationSeverity.LOW,
                    title="Add Delta table maintenance",
                    description="Regular VACUUM and OPTIMIZE improve query performance",
                    suggested_code="""# Add to your ETL pipeline or as scheduled job:

# Optimize table (compacts small files)
spark.sql("OPTIMIZE delta.`s3://bucket/delta-table/`")

# Z-ORDER for query optimization
spark.sql("OPTIMIZE delta.`s3://bucket/delta-table/` ZORDER BY (date, region)")

# Vacuum old versions (retain 7 days minimum)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM delta.`s3://bucket/delta-table/` RETAIN 168 HOURS")"""
                ))

        return recommendations

    def _estimate_impact(self, severity: RecommendationSeverity) -> str:
        """Estimate performance impact based on severity."""
        impacts = {
            RecommendationSeverity.CRITICAL: "High - Can cause job failure or 10x+ slowdown",
            RecommendationSeverity.HIGH: "Significant - 2-5x performance improvement possible",
            RecommendationSeverity.MEDIUM: "Moderate - 20-50% performance improvement possible",
            RecommendationSeverity.LOW: "Minor - 5-20% performance improvement possible",
            RecommendationSeverity.INFO: "Informational - Best practice recommendation"
        }
        return impacts.get(severity, "Unknown")

    def _calculate_score(self, result: AnalysisResult) -> float:
        """Calculate optimization score (0-100)."""
        base_score = 100.0

        for rec in result.recommendations:
            if rec.severity == RecommendationSeverity.CRITICAL:
                base_score -= 25
            elif rec.severity == RecommendationSeverity.HIGH:
                base_score -= 15
            elif rec.severity == RecommendationSeverity.MEDIUM:
                base_score -= 10
            elif rec.severity == RecommendationSeverity.LOW:
                base_score -= 5

        return max(0.0, min(100.0, base_score))

    def _generate_summary(self, result: AnalysisResult) -> Dict[str, Any]:
        """Generate analysis summary."""
        severity_counts = {}
        category_counts = {}

        for rec in result.recommendations:
            sev = rec.severity.value
            cat = rec.category.value
            severity_counts[sev] = severity_counts.get(sev, 0) + 1
            category_counts[cat] = category_counts.get(cat, 0) + 1

        return {
            "total_recommendations": len(result.recommendations),
            "anti_patterns_found": len(result.anti_patterns_found),
            "optimization_score": result.optimization_score,
            "by_severity": severity_counts,
            "by_category": category_counts,
            "top_priority": [
                r.title for r in result.recommendations
                if r.severity in [RecommendationSeverity.CRITICAL, RecommendationSeverity.HIGH]
            ][:5]
        }

    def get_fix_for_pattern(self, pattern_name: str) -> Optional[str]:
        """Get suggested fix code for a specific pattern."""
        for pattern in self.anti_patterns:
            if pattern["name"] == pattern_name:
                return pattern["suggested_code"]
        return None
