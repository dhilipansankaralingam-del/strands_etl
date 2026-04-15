#!/usr/bin/env python3
"""
PySpark Code Analysis Agent
============================

Comprehensive static analysis of PySpark code for:
- Performance optimizations
- Anti-patterns detection
- Memory management issues
- Shuffle optimization opportunities
- Join strategy recommendations
- Caching/persistence analysis
- Partition optimization
- Serialization improvements
- UDF optimization
- Catalyst optimizer hints

This agent provides actionable recommendations with code examples.

Usage:
    from agents.pyspark_code_analysis_agent import PySparkCodeAnalysisAgent

    agent = PySparkCodeAnalysisAgent()
    results = agent.analyze_file("path/to/script.py")
    # or
    results = agent.analyze(code_string, "script_name")
"""

import re
import ast
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass, field
from enum import Enum


class IssueSeverity(Enum):
    """Severity levels for code issues."""
    CRITICAL = "critical"  # Will cause failures or severe performance issues
    HIGH = "high"          # Significant performance impact
    MEDIUM = "medium"      # Moderate impact, should fix
    LOW = "low"            # Minor optimization opportunity
    INFO = "info"          # Best practice suggestion


class IssueCategory(Enum):
    """Categories of code issues."""
    PERFORMANCE = "performance"
    MEMORY = "memory"
    SHUFFLE = "shuffle"
    JOIN = "join"
    CACHING = "caching"
    PARTITION = "partition"
    SERIALIZATION = "serialization"
    UDF = "udf"
    CATALYST = "catalyst"
    ANTI_PATTERN = "anti_pattern"
    BEST_PRACTICE = "best_practice"


@dataclass
class CodeIssue:
    """Represents a code issue found during analysis."""
    category: IssueCategory
    severity: IssueSeverity
    line_number: int
    code_snippet: str
    issue: str
    recommendation: str
    fix_example: str = ""
    performance_impact: str = ""


@dataclass
class AnalysisResult:
    """Results from code analysis."""
    agent_type: str = "CODE_ANALYSIS"
    status: str = "success"
    findings: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0


class PySparkCodeAnalysisAgent:
    """
    Comprehensive PySpark code analyzer with optimization recommendations.
    """

    def __init__(self):
        self.issues: List[CodeIssue] = []
        self.code_lines: List[str] = []
        self.file_name: str = ""

        # Initialize all pattern detectors
        self._init_patterns()

    def _init_patterns(self):
        """Initialize regex patterns for code analysis."""

        # ============================================================
        # ANTI-PATTERNS
        # ============================================================
        self.anti_patterns = [
            # Collect on large datasets
            {
                "pattern": r"\.collect\(\)",
                "category": IssueCategory.ANTI_PATTERN,
                "severity": IssueSeverity.CRITICAL,
                "issue": "Using collect() brings all data to driver memory",
                "recommendation": "Use take(n), show(), or write to storage instead",
                "fix_example": "# Instead of:\ndata = df.collect()\n\n# Use:\ndf.show(100)  # View sample\ndf.write.parquet('output/')  # Save results\ndf.take(1000)  # Get limited rows",
                "performance_impact": "Can cause OutOfMemoryError on driver with large datasets"
            },
            # toPandas on large data
            {
                "pattern": r"\.toPandas\(\)",
                "category": IssueCategory.ANTI_PATTERN,
                "severity": IssueSeverity.HIGH,
                "issue": "toPandas() collects all data to driver as Pandas DataFrame",
                "recommendation": "Use Spark operations or limit data before toPandas()",
                "fix_example": "# Instead of:\npdf = df.toPandas()\n\n# Use:\npdf = df.limit(10000).toPandas()  # Limit first\n# Or use Spark operations directly",
                "performance_impact": "Driver memory exhaustion with large datasets"
            },
            # Count inside loops
            {
                "pattern": r"(for|while).*\.count\(\)",
                "category": IssueCategory.ANTI_PATTERN,
                "severity": IssueSeverity.HIGH,
                "issue": "Calling count() inside loops triggers multiple job executions",
                "recommendation": "Cache DataFrame and count once before loop",
                "fix_example": "# Instead of:\nfor i in range(10):\n    if df.count() > 0:  # Triggers job each iteration\n\n# Use:\ndf_cached = df.cache()\nrecord_count = df_cached.count()  # Count once\nfor i in range(10):\n    if record_count > 0:",
                "performance_impact": "N times slower where N = loop iterations"
            },
            # UDF with Python types
            {
                "pattern": r"@udf\s*\(",
                "category": IssueCategory.UDF,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Python UDFs disable Catalyst optimization and require serialization",
                "recommendation": "Use Spark built-in functions or Pandas UDFs instead",
                "fix_example": "# Instead of:\n@udf(StringType())\ndef my_upper(s):\n    return s.upper()\n\n# Use built-in:\nfrom pyspark.sql.functions import upper\ndf.withColumn('upper_col', upper(col('col')))\n\n# Or Pandas UDF (vectorized):\n@pandas_udf(StringType())\ndef my_upper(s: pd.Series) -> pd.Series:\n    return s.str.upper()",
                "performance_impact": "10-100x slower than native Spark functions"
            },
            # Repeated show() calls
            {
                "pattern": r"\.show\(\).*\.show\(\)",
                "category": IssueCategory.ANTI_PATTERN,
                "severity": IssueSeverity.LOW,
                "issue": "Multiple show() calls on same DataFrame trigger repeated computations",
                "recommendation": "Cache DataFrame before multiple show() calls",
                "fix_example": "# Instead of:\ndf.show()\ndf.show(100, truncate=False)\n\n# Use:\ndf_cached = df.cache()\ndf_cached.show()\ndf_cached.show(100, truncate=False)",
                "performance_impact": "Duplicated computation overhead"
            },
        ]

        # ============================================================
        # JOIN OPTIMIZATIONS
        # ============================================================
        self.join_patterns = [
            # Cartesian join detection
            {
                "pattern": r"\.crossJoin\(",
                "category": IssueCategory.JOIN,
                "severity": IssueSeverity.CRITICAL,
                "issue": "CrossJoin produces cartesian product (N*M rows)",
                "recommendation": "Verify this is intentional. Use explicit join conditions if possible.",
                "fix_example": "# Cartesian join produces rows = left_rows * right_rows\n# If unintentional, use:\ndf1.join(df2, df1['key'] == df2['key'], 'inner')",
                "performance_impact": "Memory and computation explosion with large datasets"
            },
            # Join without broadcast hint for small tables
            {
                "pattern": r"\.join\([^)]+\)\s*(?!.*broadcast)",
                "category": IssueCategory.JOIN,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Join without broadcast hint may cause unnecessary shuffle",
                "recommendation": "Use broadcast() for small tables (<10MB) to avoid shuffle",
                "fix_example": "# Instead of:\nlarge_df.join(small_df, 'key')\n\n# Use:\nfrom pyspark.sql.functions import broadcast\nlarge_df.join(broadcast(small_df), 'key')",
                "performance_impact": "Eliminates shuffle for small table joins"
            },
            # Self-join detection
            {
                "pattern": r"(\w+)\.join\(\1[,\)]",
                "category": IssueCategory.JOIN,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Self-join detected - ensure this is intentional",
                "recommendation": "Self-joins can often be replaced with window functions",
                "fix_example": "# Instead of self-join:\ndf.alias('a').join(df.alias('b'), ...)\n\n# Consider window function:\nfrom pyspark.sql.window import Window\nwindow = Window.partitionBy('key').orderBy('date')\ndf.withColumn('prev_value', lag('value').over(window))",
                "performance_impact": "Window functions avoid shuffle overhead of self-joins"
            },
            # Join key null handling
            {
                "pattern": r"\.join\([^)]+==",
                "category": IssueCategory.JOIN,
                "severity": IssueSeverity.LOW,
                "issue": "Join equality condition - nulls in join keys won't match",
                "recommendation": "Handle nulls in join keys if data may contain nulls",
                "fix_example": "# Null-safe join:\ndf1.join(df2, df1['key'].eqNullSafe(df2['key']), 'inner')\n\n# Or filter nulls first:\ndf1.filter(col('key').isNotNull()).join(...)",
                "performance_impact": "Prevents silent data loss from null keys"
            },
        ]

        # ============================================================
        # SHUFFLE OPTIMIZATIONS
        # ============================================================
        self.shuffle_patterns = [
            # groupByKey (inefficient)
            {
                "pattern": r"\.groupByKey\(",
                "category": IssueCategory.SHUFFLE,
                "severity": IssueSeverity.HIGH,
                "issue": "groupByKey() shuffles all data before aggregation",
                "recommendation": "Use reduceByKey() or aggregateByKey() for better performance",
                "fix_example": "# Instead of:\nrdd.groupByKey().mapValues(sum)\n\n# Use:\nrdd.reduceByKey(lambda a, b: a + b)\n# Or for DataFrames:\ndf.groupBy('key').agg(sum('value'))",
                "performance_impact": "reduceByKey can be 2-10x faster due to map-side aggregation"
            },
            # Repartition instead of coalesce
            {
                "pattern": r"\.repartition\(\d+\)",
                "category": IssueCategory.SHUFFLE,
                "severity": IssueSeverity.MEDIUM,
                "issue": "repartition() causes full shuffle even when reducing partitions",
                "recommendation": "Use coalesce() when reducing partitions to avoid shuffle",
                "fix_example": "# Instead of:\ndf.repartition(10)  # Full shuffle\n\n# Use when reducing partitions:\ndf.coalesce(10)  # No shuffle\n\n# Use repartition() only when:\n# 1. Increasing partitions\n# 2. Need even distribution\n# 3. Repartitioning by column: df.repartition('col')",
                "performance_impact": "coalesce avoids shuffle when reducing partitions"
            },
            # Sort before groupBy
            {
                "pattern": r"\.sort\([^)]+\)\.groupBy\(",
                "category": IssueCategory.SHUFFLE,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Sorting before groupBy is often unnecessary and expensive",
                "recommendation": "groupBy doesn't preserve sort order. Sort after aggregation if needed.",
                "fix_example": "# Instead of:\ndf.sort('col').groupBy('key').agg(...)\n\n# Use:\ndf.groupBy('key').agg(...).sort('col')",
                "performance_impact": "Eliminates unnecessary shuffle from premature sorting"
            },
            # Distinct before join
            {
                "pattern": r"\.distinct\(\)\.join\(",
                "category": IssueCategory.SHUFFLE,
                "severity": IssueSeverity.LOW,
                "issue": "distinct() before join adds extra shuffle stage",
                "recommendation": "Consider if distinct is necessary, or use dropDuplicates with subset",
                "fix_example": "# Instead of:\ndf.distinct().join(other_df, 'key')\n\n# If only key needs to be unique:\ndf.dropDuplicates(['key']).join(other_df, 'key')",
                "performance_impact": "dropDuplicates with subset is more efficient"
            },
        ]

        # ============================================================
        # CACHING/PERSISTENCE
        # ============================================================
        self.caching_patterns = [
            # Cache without unpersist
            {
                "pattern": r"\.cache\(\)|\.persist\(",
                "category": IssueCategory.CACHING,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Cached DataFrame should be unpersisted when no longer needed",
                "recommendation": "Call unpersist() after you're done with cached data",
                "fix_example": "df_cached = df.cache()\n# ... use df_cached multiple times ...\ndf_cached.unpersist()  # Release memory",
                "performance_impact": "Prevents memory leaks in long-running jobs"
            },
            # Cache immediately after read
            {
                "pattern": r"\.read[^)]+\)\.cache\(",
                "category": IssueCategory.CACHING,
                "severity": IssueSeverity.LOW,
                "issue": "Caching immediately after read may cache raw data",
                "recommendation": "Apply filters and projections before caching to reduce memory",
                "fix_example": "# Instead of:\ndf = spark.read.parquet('...').cache()\n\n# Use:\ndf = spark.read.parquet('...').filter(...).select(...).cache()",
                "performance_impact": "Caches only needed data, reducing memory usage"
            },
            # Multiple actions without cache
            {
                "pattern": r"(df\.\w+\([^)]*\).*){3,}",
                "category": IssueCategory.CACHING,
                "severity": IssueSeverity.INFO,
                "issue": "Multiple actions on same DataFrame without caching",
                "recommendation": "Consider caching if DataFrame is used in multiple actions",
                "fix_example": "# If df is used multiple times:\ndf_cached = df.cache()\ndf_cached.count()\ndf_cached.show()\ndf_cached.write.parquet('...')\ndf_cached.unpersist()",
                "performance_impact": "Avoids recomputation for repeated actions"
            },
        ]

        # ============================================================
        # PARTITION OPTIMIZATIONS
        # ============================================================
        self.partition_patterns = [
            # Static partition value
            {
                "pattern": r"\.partitionBy\(['\"][^'\"]+['\"]\)",
                "category": IssueCategory.PARTITION,
                "severity": IssueSeverity.INFO,
                "issue": "Partitioning by column - ensure cardinality is appropriate",
                "recommendation": "Ideal partition count: 100-10000. Avoid high cardinality columns.",
                "fix_example": "# Good: Low-medium cardinality\ndf.write.partitionBy('year', 'month').parquet('...')\n\n# Bad: High cardinality (creates millions of files)\ndf.write.partitionBy('user_id').parquet('...')",
                "performance_impact": "Too many partitions = small files = slow reads"
            },
            # Hardcoded partition count
            {
                "pattern": r"\.repartition\((?:200|1000|2000)\)",
                "category": IssueCategory.PARTITION,
                "severity": IssueSeverity.LOW,
                "issue": "Hardcoded partition count may not be optimal for all data sizes",
                "recommendation": "Calculate optimal partitions based on data size (128MB-256MB per partition)",
                "fix_example": "# Calculate optimal partitions:\ndata_size_mb = df.cache().count() * avg_row_size_mb\noptimal_partitions = max(1, int(data_size_mb / 128))\ndf.repartition(optimal_partitions)",
                "performance_impact": "Right-sized partitions improve parallelism"
            },
        ]

        # ============================================================
        # MEMORY OPTIMIZATIONS
        # ============================================================
        self.memory_patterns = [
            # Select * pattern
            {
                "pattern": r"spark\.sql\(['\"]SELECT \*",
                "category": IssueCategory.MEMORY,
                "severity": IssueSeverity.MEDIUM,
                "issue": "SELECT * reads all columns which may not be needed",
                "recommendation": "Select only required columns to reduce memory and I/O",
                "fix_example": "# Instead of:\ndf = spark.sql('SELECT * FROM table')\n\n# Use:\ndf = spark.sql('SELECT col1, col2 FROM table')\n# Or:\ndf = spark.table('table').select('col1', 'col2')",
                "performance_impact": "Reduces memory usage and network I/O"
            },
            # Creating large arrays/lists
            {
                "pattern": r"collect_list\([^)]+\)",
                "category": IssueCategory.MEMORY,
                "severity": IssueSeverity.MEDIUM,
                "issue": "collect_list() creates arrays that can grow very large",
                "recommendation": "Set size limit or use alternative aggregations",
                "fix_example": "# Instead of:\ndf.groupBy('key').agg(collect_list('value'))\n\n# Limit size:\ndf.groupBy('key').agg(slice(collect_list('value'), 1, 1000))\n\n# Or use array_agg with limit in SQL:\nspark.sql('SELECT key, array_agg(value) FILTER (WHERE rn <= 1000) ...')",
                "performance_impact": "Prevents memory explosion from large arrays"
            },
            # Explode followed by aggregation
            {
                "pattern": r"\.explode\([^)]+\).*\.groupBy\(",
                "category": IssueCategory.MEMORY,
                "severity": IssueSeverity.LOW,
                "issue": "explode() can significantly increase row count before aggregation",
                "recommendation": "Consider if aggregation can be done before explode",
                "fix_example": "# Consider data multiplication factor from explode\n# Original: 1M rows with avg 10 items = 10M rows after explode",
                "performance_impact": "Row multiplication affects memory and shuffle"
            },
        ]

        # ============================================================
        # CATALYST OPTIMIZER
        # ============================================================
        self.catalyst_patterns = [
            # Filter after join
            {
                "pattern": r"\.join\([^)]+\)\.filter\(",
                "category": IssueCategory.CATALYST,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Filter after join may process more data than necessary",
                "recommendation": "Apply filter before join when possible (predicate pushdown)",
                "fix_example": "# Instead of:\ndf1.join(df2, 'key').filter(df1['status'] == 'ACTIVE')\n\n# Use:\ndf1.filter(col('status') == 'ACTIVE').join(df2, 'key')",
                "performance_impact": "Reduces data shuffled during join"
            },
            # Column expressions that prevent optimization
            {
                "pattern": r"\.filter\([^)]*\.astype\(",
                "category": IssueCategory.CATALYST,
                "severity": IssueSeverity.LOW,
                "issue": "Type casting in filter may prevent predicate pushdown",
                "recommendation": "Cast the comparison value instead of the column",
                "fix_example": "# Instead of:\ndf.filter(col('date').cast('string') == '2024-01-01')\n\n# Use:\nfrom datetime import date\ndf.filter(col('date') == date(2024, 1, 1))",
                "performance_impact": "Enables predicate pushdown to data source"
            },
            # Adaptive query execution hint
            {
                "pattern": r"spark\.conf\.set.*adaptive.*false",
                "category": IssueCategory.CATALYST,
                "severity": IssueSeverity.HIGH,
                "issue": "Adaptive Query Execution (AQE) is disabled",
                "recommendation": "Enable AQE for automatic runtime optimizations",
                "fix_example": "spark.conf.set('spark.sql.adaptive.enabled', 'true')\nspark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')\nspark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')",
                "performance_impact": "AQE provides 10-50% improvement on many workloads"
            },
        ]

        # ============================================================
        # SERIALIZATION
        # ============================================================
        self.serialization_patterns = [
            # Using pickle serializer
            {
                "pattern": r"serializer.*=.*PickleSerializer",
                "category": IssueCategory.SERIALIZATION,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Pickle serialization is slower than Kryo",
                "recommendation": "Use Kryo serialization for better performance",
                "fix_example": "spark.conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\nspark.conf.set('spark.kryo.registrationRequired', 'false')",
                "performance_impact": "Kryo is 2-10x faster than Pickle"
            },
        ]

        # ============================================================
        # GLUE-SPECIFIC OPTIMIZATIONS
        # ============================================================
        self.glue_patterns = [
            # DynamicFrame without optimization
            {
                "pattern": r"from_catalog\([^)]+\)(?!.*push_down_predicate)",
                "category": IssueCategory.PERFORMANCE,
                "severity": IssueSeverity.MEDIUM,
                "issue": "Reading from Glue Catalog without predicate pushdown",
                "recommendation": "Use push_down_predicate for partition filtering",
                "fix_example": "# Instead of:\ndyf = glue_context.create_dynamic_frame.from_catalog(\n    database='db', table_name='table'\n)\n\n# Use:\ndyf = glue_context.create_dynamic_frame.from_catalog(\n    database='db', table_name='table',\n    push_down_predicate=\"year='2024' AND month='01'\"\n)",
                "performance_impact": "Reads only required partitions"
            },
            # DynamicFrame to DataFrame conversion
            {
                "pattern": r"\.toDF\(\)\.toDF\(",
                "category": IssueCategory.ANTI_PATTERN,
                "severity": IssueSeverity.LOW,
                "issue": "Redundant DataFrame conversions",
                "recommendation": "Convert once and reuse",
                "fix_example": "# Instead of:\ndyf.toDF().toDF('new_schema')\n\n# Use:\ndf = dyf.toDF()\ndf_renamed = df.toDF('col1', 'col2', ...)",
                "performance_impact": "Avoids unnecessary conversions"
            },
            # Glue bookmark not used
            {
                "pattern": r"Job\(glue_context\)\.init\([^)]+\)(?!.*bookmark)",
                "category": IssueCategory.BEST_PRACTICE,
                "severity": IssueSeverity.INFO,
                "issue": "Job bookmark not enabled for incremental processing",
                "recommendation": "Enable job bookmarks for incremental ETL",
                "fix_example": "# Enable bookmarks:\njob.init(args['JOB_NAME'], args)\n\n# In job parameters:\n--job-bookmark-option job-bookmark-enable",
                "performance_impact": "Process only new data in subsequent runs"
            },
        ]

    def analyze(self, code: str, file_name: str = "script.py") -> AnalysisResult:
        """
        Analyze PySpark code and return findings.

        Args:
            code: Source code as string
            file_name: Name of the file being analyzed

        Returns:
            AnalysisResult with findings and recommendations
        """
        self.issues = []
        self.code_lines = code.split('\n')
        self.file_name = file_name

        print(f"[Code Analysis] Analyzing: {file_name}")
        print(f"[Code Analysis] Lines of code: {len(self.code_lines)}")

        # Run all pattern detectors
        all_patterns = (
            self.anti_patterns +
            self.join_patterns +
            self.shuffle_patterns +
            self.caching_patterns +
            self.partition_patterns +
            self.memory_patterns +
            self.catalyst_patterns +
            self.serialization_patterns +
            self.glue_patterns
        )

        for pattern_config in all_patterns:
            self._check_pattern(code, pattern_config)

        # Calculate metrics
        total_issues = len(self.issues)
        critical_issues = len([i for i in self.issues if i.severity == IssueSeverity.CRITICAL])
        high_issues = len([i for i in self.issues if i.severity == IssueSeverity.HIGH])

        # Calculate optimization score (100 = perfect, 0 = needs work)
        # Deduct points based on severity
        score = 100
        score -= critical_issues * 20
        score -= high_issues * 10
        score -= len([i for i in self.issues if i.severity == IssueSeverity.MEDIUM]) * 5
        score -= len([i for i in self.issues if i.severity == IssueSeverity.LOW]) * 2
        score = max(0, score)

        # Convert issues to findings
        findings = [
            {
                "category": issue.category.value,
                "severity": issue.severity.value,
                "line": issue.line_number,
                "code": issue.code_snippet,
                "issue": issue.issue,
                "recommendation": issue.recommendation,
                "fix_example": issue.fix_example,
                "performance_impact": issue.performance_impact
            }
            for issue in self.issues
        ]

        # Generate recommendations summary
        recommendations = []
        if critical_issues > 0:
            recommendations.append(f"FIX IMMEDIATELY: {critical_issues} critical issues found")
        if high_issues > 0:
            recommendations.append(f"HIGH PRIORITY: {high_issues} high-severity issues to address")

        # Category-specific recommendations
        categories_found = set(i.category for i in self.issues)
        if IssueCategory.JOIN in categories_found:
            recommendations.append("Review join strategies - consider broadcast joins for small tables")
        if IssueCategory.SHUFFLE in categories_found:
            recommendations.append("Optimize shuffle operations - reduce data movement")
        if IssueCategory.CACHING in categories_found:
            recommendations.append("Review caching strategy - cache reused DataFrames, unpersist when done")
        if IssueCategory.UDF in categories_found:
            recommendations.append("Replace Python UDFs with Spark built-in functions or Pandas UDFs")

        return AnalysisResult(
            agent_type="CODE_ANALYSIS",
            status="warning" if total_issues > 0 else "success",
            findings=findings,
            recommendations=recommendations,
            metrics={
                "optimization_score": score,
                "total_issues": total_issues,
                "critical_issues": critical_issues,
                "high_issues": high_issues,
                "medium_issues": len([i for i in self.issues if i.severity == IssueSeverity.MEDIUM]),
                "low_issues": len([i for i in self.issues if i.severity == IssueSeverity.LOW]),
                "lines_analyzed": len(self.code_lines),
                "categories_flagged": [c.value for c in categories_found]
            }
        )

    def analyze_file(self, file_path: str) -> AnalysisResult:
        """Analyze a PySpark script file."""
        with open(file_path, 'r') as f:
            code = f.read()
        return self.analyze(code, file_path)

    def _check_pattern(self, code: str, pattern_config: Dict):
        """Check code against a single pattern."""
        pattern = pattern_config["pattern"]

        for i, line in enumerate(self.code_lines, 1):
            if re.search(pattern, line, re.IGNORECASE):
                # Get context (line before and after)
                start = max(0, i - 2)
                end = min(len(self.code_lines), i + 1)
                snippet = '\n'.join(self.code_lines[start:end])

                issue = CodeIssue(
                    category=pattern_config["category"],
                    severity=pattern_config["severity"],
                    line_number=i,
                    code_snippet=snippet,
                    issue=pattern_config["issue"],
                    recommendation=pattern_config["recommendation"],
                    fix_example=pattern_config.get("fix_example", ""),
                    performance_impact=pattern_config.get("performance_impact", "")
                )
                self.issues.append(issue)

    def print_report(self, result: AnalysisResult):
        """Print formatted analysis report."""
        print("\n" + "=" * 70)
        print(f"PYSPARK CODE ANALYSIS REPORT: {self.file_name}")
        print("=" * 70)
        print(f"Optimization Score: {result.metrics['optimization_score']}/100")
        print(f"Total Issues: {result.metrics['total_issues']}")
        print(f"  Critical: {result.metrics['critical_issues']}")
        print(f"  High: {result.metrics['high_issues']}")
        print(f"  Medium: {result.metrics['medium_issues']}")
        print(f"  Low: {result.metrics['low_issues']}")
        print("=" * 70)

        if result.findings:
            print("\nDETAILED FINDINGS:")
            print("-" * 70)

            for i, finding in enumerate(result.findings, 1):
                severity_color = {
                    "critical": "🔴",
                    "high": "🟠",
                    "medium": "🟡",
                    "low": "🟢",
                    "info": "🔵"
                }
                icon = severity_color.get(finding['severity'], "⚪")

                print(f"\n{icon} Issue #{i} [{finding['severity'].upper()}] - {finding['category']}")
                print(f"   Line {finding['line']}: {finding['issue']}")
                print(f"   Recommendation: {finding['recommendation']}")
                if finding['performance_impact']:
                    print(f"   Impact: {finding['performance_impact']}")
                if finding['fix_example']:
                    print(f"   Fix Example:\n{self._indent(finding['fix_example'], 6)}")

        if result.recommendations:
            print("\n" + "=" * 70)
            print("SUMMARY RECOMMENDATIONS:")
            print("-" * 70)
            for rec in result.recommendations:
                print(f"  • {rec}")

        print("\n" + "=" * 70)

    def _indent(self, text: str, spaces: int) -> str:
        """Indent text by given spaces."""
        indent = " " * spaces
        return '\n'.join(indent + line for line in text.split('\n'))


# Example usage
if __name__ == "__main__":
    # Sample code with various issues
    sample_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("test").getOrCreate()

# Anti-pattern: collect on large data
data = spark.table("large_table").collect()

# Anti-pattern: Python UDF
@udf(StringType())
def my_upper(s):
    return s.upper() if s else None

df = spark.table("users")
df = df.withColumn("upper_name", my_upper(col("name")))

# Inefficient: join without broadcast
large_df = spark.table("transactions")
small_df = spark.table("categories")  # Small lookup table
result = large_df.join(small_df, "category_id")

# Shuffle: groupByKey
rdd = spark.sparkContext.parallelize([(1, 2), (1, 3), (2, 4)])
grouped = rdd.groupByKey()

# Memory: SELECT *
all_data = spark.sql("SELECT * FROM huge_table")

# Repartition when coalesce would work
df = df.repartition(10)

df.show()
df.show()  # Repeated show without cache
'''

    agent = PySparkCodeAnalysisAgent()
    result = agent.analyze(sample_code, "sample_script.py")
    agent.print_report(result)
