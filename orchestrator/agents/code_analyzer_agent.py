"""
Code Analyzer Agent
===================
Line-by-line PySpark anti-pattern detection, complexity scoring, Spark config
recommendations, skew mitigations, and AQE guidance.

Based on the updated CodeAnalyzerAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import re
from typing import Any, Dict, List

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Senior PySpark Performance Engineer** with deep expertise in Apache Spark internals,
AWS Glue optimisation, and cost reduction.

Your task is to analyse PySpark code LINE BY LINE and produce:
1. Anti-pattern findings — line number, severity (critical/high/medium/low), description, fix
2. Complexity metrics — join count, window functions, UDFs, aggregations
3. Optimisation opportunities — broadcast hints, caching, predicate pushdown, column pruning
4. Spark configuration recommendations — AQE, shuffle partitions, broadcast threshold
5. Skew mitigation strategies — salting, skew-join hints
6. Overall optimisation score (0-100) and estimated cost-reduction %

Return structured JSON:
{
  "anti_patterns": [{ "line": N, "pattern": "...", "severity": "...", "description": "...", "fix": "..." }],
  "complexity": { "join_count": N, "udf_count": N, "window_count": N, "complexity_score": 0-100 },
  "optimizations": [{ "category": "...", "recommendation": "...", "implementation": "...", "savings_pct": N }],
  "spark_configs": [{ "config": "...", "recommended_value": "...", "reason": "..." }],
  "skew_mitigations": [{ "technique": "...", "implementation": "..." }],
  "optimization_score": 0-100,
  "estimated_cost_reduction_percent": N,
  "recommendations": []
}

Return ONLY valid JSON.
"""

# ---------------------------------------------------------------------------
# Anti-pattern catalogue (PR-11 CodeAnalyzerAgent)
# ---------------------------------------------------------------------------
_ANTI_PATTERNS = {
    "collect_large": {
        "pattern":     r"\.collect\(\)",
        "severity":    "critical",
        "cost_impact": "high",
        "description": "collect() pulls all data to driver — causes OOM on large datasets",
        "fix":         "Use .take(n), .first(), or write results to storage instead",
    },
    "toPandas_large": {
        "pattern":     r"\.toPandas\(\)",
        "severity":    "critical",
        "cost_impact": "high",
        "description": "toPandas() loads entire DataFrame into driver memory",
        "fix":         "Process in Spark; use .limit(n) before toPandas() if sample needed",
    },
    "crossJoin": {
        "pattern":     r"\.crossJoin\(",
        "severity":    "critical",
        "cost_impact": "critical",
        "description": "crossJoin creates cartesian product — exponential data explosion",
        "fix":         "Add explicit join conditions or use window functions instead",
    },
    "for_loop_collect": {
        "pattern":     r"for\s+\w+\s+in\s+\w+\.collect\(\)",
        "severity":    "critical",
        "cost_impact": "critical",
        "description": "Python for-loop over collect() defeats Spark parallelism entirely",
        "fix":         "Replace with Spark transformations: map, flatMap, withColumn, etc.",
    },
    "udf_usage": {
        "pattern":     r"@udf|udf\(|\.udf\.",
        "severity":    "high",
        "cost_impact": "high",
        "description": "Python UDFs bypass Catalyst optimiser — 10–100× slower than built-ins",
        "fix":         "Replace with Spark SQL functions (concat, when, regexp_extract, etc.)",
    },
    "repartition_1": {
        "pattern":     r"\.repartition\(\s*1\s*\)",
        "severity":    "high",
        "cost_impact": "high",
        "description": "repartition(1) forces all data to one partition — kills parallelism",
        "fix":         "Use .coalesce(n) to reduce partitions; keep parallelism for transforms",
    },
    "select_star": {
        "pattern":     r'\.select\(\s*["\']?\*["\']?\s*\)',
        "severity":    "medium",
        "cost_impact": "medium",
        "description": "SELECT * reads unnecessary columns — increases I/O and shuffle",
        "fix":         'Select only required columns: .select("col1", "col2")',
    },
    "multiple_count": {
        "pattern":     r"\.count\(\)[\s\S]{0,200}\.count\(\)",
        "severity":    "medium",
        "cost_impact": "medium",
        "description": "Multiple .count() calls recompute the full lineage each time",
        "fix":         "Cache the DataFrame before multiple actions: df.cache(); df.count()",
    },
    "show_production": {
        "pattern":     r"\.show\(",
        "severity":    "low",
        "cost_impact": "low",
        "description": ".show() triggers an action — unnecessary overhead in production",
        "fix":         "Remove .show() or guard with: if os.getenv('DEBUG'): df.show()",
    },
    "repeated_read": {
        "pattern":     r"spark\.read[\s\S]{1,300}?spark\.read",
        "severity":    "medium",
        "cost_impact": "medium",
        "description": "Multiple reads of the same data source — wastes I/O",
        "fix":         "Read once, cache if reused: df = spark.read.parquet(path).cache()",
    },
    "string_concat_lambda": {
        "pattern":     r"lambda.*\+.*str|lambda.*\.format\(",
        "severity":    "medium",
        "cost_impact": "medium",
        "description": "String concat in Python lambda — should use Spark concat()/concat_ws()",
        "fix":         "from pyspark.sql.functions import concat, concat_ws",
    },
    "persist_no_unpersist": {
        "pattern":     r"\.(cache|persist)\(",
        "severity":    "low",
        "cost_impact": "low",
        "description": "cache/persist without unpersist — may waste cluster memory",
        "fix":         "Add df.unpersist() when DataFrame is no longer needed",
    },
}

_SPARK_CONFIGS = [
    {
        "config":            "spark.sql.adaptive.enabled",
        "recommended_value": "true",
        "reason":            "Enables AQE — auto-tunes joins, skew handling, and partition counts",
    },
    {
        "config":            "spark.sql.adaptive.coalescePartitions.enabled",
        "recommended_value": "true",
        "reason":            "Auto-coalesces small post-shuffle partitions to reduce task overhead",
    },
    {
        "config":            "spark.sql.adaptive.skewJoin.enabled",
        "recommended_value": "true",
        "reason":            "Automatically detects and handles skewed join partitions",
    },
    {
        "config":            "spark.sql.autoBroadcastJoinThreshold",
        "recommended_value": "104857600",  # 100 MB
        "reason":            "Increase from 10 MB default to broadcast more small tables, avoiding shuffles",
    },
]


def _detect_anti_patterns(code: str, lines: List[str]) -> List[Dict]:
    """Line-by-line anti-pattern detection."""
    found = []
    for name, info in _ANTI_PATTERNS.items():
        occurrences = []
        for i, line in enumerate(lines, 1):
            if re.search(info["pattern"], line, re.IGNORECASE):
                occurrences.append({"line": i, "content": line.strip()[:120]})

        if occurrences:
            # Skip persist warnings when unpersist is present
            if name == "persist_no_unpersist" and "unpersist" in code.lower():
                continue
            found.append({
                "pattern":     name,
                "severity":    info["severity"],
                "cost_impact": info["cost_impact"],
                "description": info["description"],
                "fix":         info["fix"],
                "occurrences": occurrences,
                "line_numbers": [o["line"] for o in occurrences],
            })
    return found


def _analyse_complexity(code: str) -> Dict:
    join_count   = len(re.findall(r"\.join\(", code, re.IGNORECASE))
    window_count = len(re.findall(r"\bWindow\b|\bover\(", code, re.IGNORECASE))
    udf_count    = len(re.findall(r"@udf|udf\(", code, re.IGNORECASE))
    agg_count    = len(re.findall(r"\.agg\(|\.groupBy\(", code, re.IGNORECASE))
    distinct_cnt = len(re.findall(r"\.distinct\(", code, re.IGNORECASE))
    sort_count   = len(re.findall(r"\.sort\(|\.orderBy\(", code, re.IGNORECASE))

    score = (20
             + join_count   * 8
             + window_count * 10
             + udf_count    * 15
             + agg_count    * 3
             + distinct_cnt * 5
             + sort_count   * 5)
    return {
        "join_count":            join_count,
        "window_function_count": window_count,
        "udf_count":             udf_count,
        "aggregation_count":     agg_count,
        "distinct_count":        distinct_cnt,
        "sort_count":            sort_count,
        "complexity_score":      min(100, score),
    }


def _detect_optimizations(code: str, complexity: Dict) -> List[Dict]:
    opts = []
    join_count      = complexity["join_count"]
    broadcast_count = len(re.findall(r"broadcast\(", code, re.IGNORECASE))
    action_count    = len(re.findall(r"\.(count|collect|show|write|save)\(", code, re.IGNORECASE))
    cache_count     = len(re.findall(r"\.(cache|persist)\(", code, re.IGNORECASE))

    if join_count > 0 and broadcast_count == 0:
        opts.append({
            "category":       "join_optimization",
            "recommendation": "Add broadcast hints for small dimension tables",
            "implementation": "from pyspark.sql.functions import broadcast\ndf.join(broadcast(small_df), 'key')",
            "savings_pct":    15,
            "effort":         "low",
        })

    if action_count > 1 and cache_count == 0:
        opts.append({
            "category":       "caching",
            "recommendation": "Cache DataFrames accessed by multiple downstream actions",
            "implementation": "df = df.cache()\n# ... multiple actions ...\ndf.unpersist()",
            "savings_pct":    30,
            "effort":         "low",
        })

    if re.search(r"\.read[\s\S]{0,200}\.filter\(", code, re.IGNORECASE):
        opts.append({
            "category":       "io_optimization",
            "recommendation": "Move filter predicates before/into read for predicate pushdown",
            "implementation": "spark.read.parquet(path).filter(\"date = '2024-01-01'\")",
            "savings_pct":    20,
            "effort":         "low",
        })

    if ".read" in code and not re.search(r"\.read[\s\S]{0,80}\.select\(", code, re.IGNORECASE):
        opts.append({
            "category":       "io_optimization",
            "recommendation": "Select only required columns immediately after read (column pruning)",
            "implementation": 'df = spark.read.parquet(path).select("col1", "col2", "col3")',
            "savings_pct":    10,
            "effort":         "low",
        })

    if (re.search(r"\.write[\s\S]{0,60}\.save|\.write[\s\S]{0,60}\.parquet", code, re.IGNORECASE)
            and not re.search(r"\.coalesce\(\d+\)[\s\S]{0,60}\.write", code, re.IGNORECASE)):
        opts.append({
            "category":       "io_optimization",
            "recommendation": "Add .coalesce(n) before write to control output file count",
            "implementation": "df.coalesce(num_partitions).write.parquet(output_path)",
            "savings_pct":    5,
            "effort":         "low",
        })

    if "spark.sql.adaptive.enabled" not in code and join_count > 0:
        opts.append({
            "category":       "config_optimization",
            "recommendation": "Enable Adaptive Query Execution (AQE)",
            "implementation": '.config("spark.sql.adaptive.enabled", "true")',
            "savings_pct":    20,
            "effort":         "low",
        })

    return opts


def _detect_skew_mitigations(code: str, complexity: Dict) -> List[Dict]:
    mitigations = []
    has_salting = "salt" in code.lower() or "rand()" in code.lower()
    has_skew_hint = "skew" in code.lower()

    if complexity["join_count"] > 1 and not has_salting:
        mitigations.append({
            "technique":     "Key Salting",
            "applicable":    True,
            "implemented":   False,
            "implementation": (
                "num_salt = 10\n"
                "df_large = df_large.withColumn('salt', floor(rand() * num_salt))\n"
                "df_large = df_large.withColumn('salted_key', concat(col('join_key'), lit('_'), col('salt')))\n"
                "df_small = df_small.crossJoin(spark.range(num_salt).withColumnRenamed('id', 'salt'))\n"
                "df_small = df_small.withColumn('salted_key', concat(col('join_key'), lit('_'), col('salt')))\n"
                "result = df_large.join(df_small, 'salted_key')"
            ),
            "estimated_improvement": "50-80% for heavily skewed joins",
        })

    if complexity["join_count"] > 2 and not has_skew_hint:
        mitigations.append({
            "technique":     "AQE Skew Join Hint",
            "applicable":    True,
            "implemented":   False,
            "implementation": (
                'spark.conf.set("spark.sql.adaptive.enabled", "true")\n'
                'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")\n'
                'spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")\n'
                'spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")'
            ),
            "estimated_improvement": "30-50% for skewed joins",
        })
    return mitigations


def _calc_score(anti_patterns: List[Dict], optimizations: List[Dict]) -> int:
    score = 100
    for p in anti_patterns:
        score -= {"critical": 20, "high": 10, "medium": 5, "low": 2}.get(p["severity"], 2)
    score -= len(optimizations) * 3
    return max(0, score)


def _estimate_savings(anti_patterns: List[Dict], optimizations: List[Dict]) -> int:
    impact = {"critical": 25, "high": 15, "medium": 8, "low": 3}
    total = sum(impact.get(p["cost_impact"], 5) for p in anti_patterns)
    total += sum(o.get("savings_pct", 5) for o in optimizations)
    return min(80, total)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@tool
def analyse_pyspark_code(script_content: str, effective_size_gb: float = 100.0) -> str:
    """
    Perform line-by-line analysis of a PySpark script.

    Args:
        script_content:    Full PySpark script source code.
        effective_size_gb: Effective data size (from Sizing Agent) for config tuning.

    Returns:
        JSON analysis report with anti_patterns, complexity, optimizations, spark_configs,
        skew_mitigations, optimization_score, and estimated_cost_reduction_percent.
    """
    try:
        lines       = script_content.split("\n")
        anti_pats   = _detect_anti_patterns(script_content, lines)
        complexity  = _analyse_complexity(script_content)
        opts        = _detect_optimizations(script_content, complexity)
        skew_mits   = _detect_skew_mitigations(script_content, complexity)
        score       = _calc_score(anti_pats, opts)
        savings     = _estimate_savings(anti_pats, opts)

        # Tune shuffle partitions to data size
        shuffle_partitions = (50 if effective_size_gb < 10 else
                              200 if effective_size_gb < 100 else
                              400 if effective_size_gb < 500 else 800)
        configs = [
            {"config": "spark.sql.shuffle.partitions",
             "recommended_value": str(shuffle_partitions),
             "reason": f"Tuned for {effective_size_gb:.0f} GB effective data size"},
            *_SPARK_CONFIGS,
        ]

        recs = []
        for p in anti_pats:
            if p["severity"] in ("critical", "high"):
                recs.append({
                    "priority":       "P0" if p["severity"] == "critical" else "P1",
                    "category":       "code",
                    "title":          f"Fix: {p['pattern'].replace('_', ' ').title()}",
                    "description":    p["description"],
                    "implementation": p["fix"],
                    "lines":          p["line_numbers"],
                    "savings_pct":    20 if p["severity"] == "critical" else 10,
                })
        for o in opts:
            recs.append({
                "priority":       "P1" if o.get("effort") == "low" else "P2",
                "category":       o["category"],
                "title":          o["recommendation"],
                "implementation": o["implementation"],
                "savings_pct":    o["savings_pct"],
            })
        if configs:
            recs.append({
                "priority":       "P0",
                "category":       "config",
                "title":          "Apply Optimal Spark Configurations",
                "configs":        configs,
                "savings_pct":    20,
            })

        return json.dumps({
            "anti_patterns":                     anti_pats,
            "anti_pattern_count":                len(anti_pats),
            "critical_issues":                   sum(1 for p in anti_pats if p["severity"] == "critical"),
            "complexity":                        complexity,
            "optimizations":                     opts,
            "spark_configs":                     configs,
            "skew_mitigations":                  skew_mits,
            "optimization_score":                score,
            "estimated_cost_reduction_percent":  savings,
            "lines_of_code":                     len(lines),
            "code_quality_score":                max(0, 100 - len(anti_pats) * 10),
            "recommendations":                   recs,
        })
    except Exception as exc:
        logger.error("Code analysis failed: %s", exc)
        return json.dumps({"error": str(exc), "optimization_score": 0})


@tool
def analyse_pyspark_file(script_path: str, effective_size_gb: float = 100.0) -> str:
    """
    Read a local PySpark script and analyse it.

    Args:
        script_path:       Absolute path to the .py file.
        effective_size_gb: Effective data size for Spark config tuning.

    Returns:
        Same JSON report as analyse_pyspark_code.
    """
    try:
        with open(script_path, "r") as f:
            content = f.read()
        return analyse_pyspark_code.__wrapped__(content, effective_size_gb)
    except Exception as exc:
        logger.error("File read failed: %s", exc)
        return json.dumps({"error": str(exc)})


# ---------------------------------------------------------------------------
# Compound cross-agent issue detection (8 patterns from KEYJa branch)
# ---------------------------------------------------------------------------
_COMPOUND_PATTERNS = [
    {
        "id": "SIZE_CODE_UDF_LARGE",
        "title": "Large dataset processed with Python UDFs",
        "severity": "critical",
        "condition": lambda s, m: s.get("effective_size_gb", 0) > 50 and m.get("udf_count", 0) > 0,
        "description": "UDFs on large datasets combine worst of two worlds: full data movement + Python serialisation overhead",
        "recommendation": "Replace UDFs with Spark built-ins (F.regexp_extract, F.when, F.concat) before scaling",
        "savings_pct": 40,
    },
    {
        "id": "METRICS_SIZE_OVER_PROVISION",
        "title": "High worker count but small effective data size",
        "severity": "high",
        "condition": lambda s, m: s.get("effective_size_gb", 999) < 20 and m.get("worker_count", 0) > 10,
        "description": "Workers are idle — effective data size doesn't justify the cluster",
        "recommendation": "Reduce worker count; enable auto-scaling; consider G.1X instead of G.2X",
        "savings_pct": 30,
    },
    {
        "id": "METRICS_CODE_SKEW_NO_AQE",
        "title": "Skewed join detected without AQE enabled",
        "severity": "critical",
        "condition": lambda s, m: m.get("skew_risk", False) and not m.get("aqe_enabled", False),
        "description": "Skew causes stragglers; without AQE the job hangs on the largest partition",
        "recommendation": "Enable spark.sql.adaptive.enabled=true and skewJoin.enabled=true",
        "savings_pct": 35,
    },
    {
        "id": "SIZE_CODE_NO_PREDICATE",
        "title": "Full table scan on large table without predicate pushdown",
        "severity": "high",
        "condition": lambda s, m: s.get("effective_size_gb", 0) > 100 and m.get("select_star_count", 0) > 0,
        "description": "Reading all columns on 100+ GB table wastes I/O budget",
        "recommendation": "Add column projection and partition filters at read time",
        "savings_pct": 20,
    },
    {
        "id": "METRICS_CODE_COLLECT_LOOP",
        "title": "collect() inside processing loop",
        "severity": "critical",
        "condition": lambda s, m: m.get("for_loop_collect_count", 0) > 0,
        "description": "Each loop iteration triggers a full Spark job — N×overhead",
        "recommendation": "Vectorise with Spark transformations; use .rdd.map() or join instead of loop",
        "savings_pct": 50,
    },
    {
        "id": "SIZE_CODE_REPARTITION_SMALL",
        "title": "repartition(1) on a large dataset",
        "severity": "high",
        "condition": lambda s, m: s.get("effective_size_gb", 0) > 10 and m.get("repartition_1_count", 0) > 0,
        "description": "Single partition on large data creates write bottleneck and disables parallelism",
        "recommendation": "Use .coalesce(optimal_n) or remove repartition and let AQE manage partition count",
        "savings_pct": 25,
    },
    {
        "id": "METRICS_HEAP_UDF",
        "title": "High heap pressure with Python UDFs",
        "severity": "high",
        "condition": lambda s, m: m.get("heap_peak_pct", 0) > 80 and m.get("udf_count", 0) > 0,
        "description": "UDFs hold Python objects in off-heap; combined with high JVM heap → GC storms",
        "recommendation": "Replace UDFs first; then consider increasing executor memory or G.2X workers",
        "savings_pct": 20,
    },
    {
        "id": "SIZE_CODE_CROSS_JOIN",
        "title": "Cartesian product on non-trivial dataset",
        "severity": "critical",
        "condition": lambda s, m: s.get("effective_size_gb", 0) > 1 and m.get("crossjoin_count", 0) > 0,
        "description": "crossJoin scales quadratically — even 1 GB × 1 GB = 1 TB shuffle",
        "recommendation": "Replace crossJoin with broadcast + inequality join or window function",
        "savings_pct": 60,
    },
]


def _extract_code_metrics(script_content: str, base_analysis: dict) -> dict:
    complexity = base_analysis.get("complexity", {})
    return {
        "udf_count":            complexity.get("udf_count", 0),
        "for_loop_collect_count": len(re.findall(r"for\s+\w+\s+in\s+\w+\.collect\(\)", script_content)),
        "repartition_1_count":  len(re.findall(r"\.repartition\(\s*1\s*\)", script_content)),
        "crossjoin_count":      len(re.findall(r"\.crossJoin\(", script_content, re.IGNORECASE)),
        "select_star_count":    len(re.findall(r'\.select\(\s*["\']?\*["\']?\s*\)', script_content)),
        "skew_risk":            base_analysis.get("complexity", {}).get("join_count", 0) > 2,
        "aqe_enabled":          "spark.sql.adaptive.enabled" in script_content,
        "worker_count":         0,  # filled from metrics_json if provided
        "heap_peak_pct":        0,  # filled from metrics_json if provided
    }


@tool
def analyse_pyspark_code_compound(
    script_content: str,
    sizing_results_json: str = "{}",
    metrics_json: str = "{}",
) -> str:
    """
    Detect compound cross-agent issues by combining code analysis with sizing and
    Glue CloudWatch metrics. Surfaces 8 cross-agent anti-patterns.

    Args:
        script_content:      Full PySpark script source code.
        sizing_results_json: JSON output from the Sizing Agent (effective_size_gb, etc.).
        metrics_json:        JSON Glue CloudWatch metrics (heap_peak_pct, worker_count, etc.).

    Returns:
        JSON with compound_issues list, each with severity, savings_pct, and recommendation.
    """
    try:
        sizing  = json.loads(sizing_results_json) if sizing_results_json else {}
        metrics = json.loads(metrics_json) if metrics_json else {}

        base   = json.loads(analyse_pyspark_code.__wrapped__(script_content))
        code_m = _extract_code_metrics(script_content, base)
        # Merge explicit metrics
        code_m.update({
            "worker_count":  metrics.get("worker_count", metrics.get("num_workers", 0)),
            "heap_peak_pct": metrics.get("heap_peak_pct", metrics.get("heap_memory_pct", 0)),
        })

        triggered = []
        for cp in _COMPOUND_PATTERNS:
            try:
                if cp["condition"](sizing, code_m):
                    triggered.append({
                        "id":             cp["id"],
                        "title":          cp["title"],
                        "severity":       cp["severity"],
                        "description":    cp["description"],
                        "recommendation": cp["recommendation"],
                        "savings_pct":    cp["savings_pct"],
                        "priority":       "P0" if cp["severity"] == "critical" else "P1",
                    })
            except Exception:
                pass

        total_savings = min(80, sum(c["savings_pct"] for c in triggered))
        return json.dumps({
            "compound_issues":        triggered,
            "compound_issue_count":   len(triggered),
            "critical_compound":      sum(1 for c in triggered if c["severity"] == "critical"),
            "estimated_savings_pct":  total_savings,
            "base_analysis_summary": {
                "anti_pattern_count":   base.get("anti_pattern_count", 0),
                "optimization_score":   base.get("optimization_score", 0),
                "complexity_score":     base.get("complexity", {}).get("complexity_score", 0),
            },
        })
    except Exception as exc:
        logger.error("Compound analysis failed: %s", exc)
        return json.dumps({"error": str(exc)})


@tool
def detect_skew_patterns(
    script_content: str,
    partition_sizes_json: str = "[]",
) -> str:
    """
    Detect data skew in PySpark scripts, quantified using Zipf's law model.
    Identifies skew-prone join keys and recommends salting / AQE strategies.

    Args:
        script_content:       Full PySpark script source code.
        partition_sizes_json: JSON array of partition sizes in MB (optional, for Zipf fit).

    Returns:
        JSON with skew_risk_level, zipf_alpha, skewed_keys, and mitigation strategies.
    """
    import math

    def _zipf_alpha(sizes: list) -> float:
        """Estimate Zipf exponent via Hill estimator (MLE)."""
        n = len(sizes)
        if n < 2:
            return 1.0
        mn = min(sizes)
        if mn <= 0:
            mn = 1e-6
        return 1 + n / sum(math.log(s / mn) for s in sizes if s > mn) if any(s > mn for s in sizes) else 1.0

    try:
        partition_sizes = json.loads(partition_sizes_json) if partition_sizes_json else []
        lines = script_content.split("\n")

        skewed_keys = []
        for i, line in enumerate(lines, 1):
            for kw in ["status", "type", "flag", "category", "region", "country"]:
                if re.search(rf'\.join\(.*["\']?{kw}["\']?', line, re.IGNORECASE):
                    skewed_keys.append({"line": i, "key": kw, "reason": "low-cardinality key"})

        alpha = _zipf_alpha([float(s) for s in partition_sizes]) if partition_sizes else None
        skew_level = "unknown"
        if alpha is not None:
            skew_level = ("severe" if alpha > 2.5 else
                          "high"   if alpha > 1.8 else
                          "moderate" if alpha > 1.2 else "low")

        join_count = len(re.findall(r"\.join\(", script_content, re.IGNORECASE))
        has_aqe    = "spark.sql.adaptive.enabled" in script_content
        has_salt   = "salt" in script_content.lower()

        mitigations = []
        if skew_level in ("severe", "high") or skewed_keys:
            if not has_salt:
                mitigations.append({
                    "technique": "Key Salting",
                    "priority": "P0",
                    "code": (
                        "num_salt = 10\n"
                        "df = df.withColumn('salt_key', concat(col('join_key'), lit('_'), (rand()*num_salt).cast('int')))"
                    ),
                })
            if not has_aqe:
                mitigations.append({
                    "technique": "Enable AQE skewJoin",
                    "priority": "P0",
                    "code": (
                        'spark.conf.set("spark.sql.adaptive.enabled", "true")\n'
                        'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")'
                    ),
                })

        return json.dumps({
            "skew_risk_level":   skew_level,
            "zipf_alpha":        round(alpha, 4) if alpha else None,
            "zipf_interpretation": (
                "α > 2.5 → extreme skew (few partitions hold almost all data)"
                if alpha and alpha > 2.5 else
                "α > 1.5 → moderate skew — AQE skewJoin recommended"
                if alpha and alpha > 1.5 else
                "α ≈ 1.0 → uniform distribution — low skew risk"
                if alpha else "No partition size data provided"
            ),
            "join_count":        join_count,
            "skewed_keys":       skewed_keys,
            "aqe_enabled":       has_aqe,
            "salting_present":   has_salt,
            "mitigations":       mitigations,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_code_analyzer_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for PySpark code analysis."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[
            analyse_pyspark_code,
            analyse_pyspark_file,
            analyse_pyspark_code_compound,
            detect_skew_patterns,
        ],
    )
