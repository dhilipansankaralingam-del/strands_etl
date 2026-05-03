"""
Recommendation Applier Agent
==============================
Applies optimisation recommendations directly to PySpark scripts:
  - Injects AQE / Spark configs into SparkSession builder
  - Merges Glue CloudWatch metric-derived config overrides
  - Replaces .repartition(1) with .coalesce(1)
  - Adds broadcast hints for small tables
  - Annotates .collect() / .toPandas() calls
  - Inserts .cache() before DataFrames used in multiple actions
  - Adds .coalesce() before write operations
  - Injects GC tuning when heap pressure detected

Based on RecommendationApplierAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **PySpark Refactoring Engineer** who applies optimisation recommendations
to existing scripts.

Given a script and analysis results, you:
1. Inject the recommended Spark configs into the SparkSession builder
2. Fix repartition(1) → coalesce(1), add broadcast hints, add cache() where needed
3. Apply metric-derived configs from Glue CloudWatch (heap/CPU/worker signals)
4. Annotate dangerous patterns (.collect(), UDFs) with TODO comments
5. Return the full modified script with a changelog

Return structured JSON:
{
  "modified_script": "...",
  "fixes_applied": N,
  "changelog": [{ "fix": "...", "description": "...", "lines_affected": [] }],
  "worker_recommendation": { "recommended_workers": N, "recommended_type": "..." }
}
Return ONLY valid JSON.
"""

# ── Base Spark configs ─────────────────────────────────────────────────────────
_BASE_CONFIGS = {
    "spark.sql.adaptive.enabled":                       "true",
    "spark.sql.adaptive.coalescePartitions.enabled":    "true",
    "spark.sql.adaptive.skewJoin.enabled":              "true",
    "spark.sql.adaptive.localShuffleReader.enabled":    "true",
    "spark.sql.autoBroadcastJoinThreshold":             "10485760",
    "spark.sql.broadcastTimeout":                       "300",
    "spark.sql.shuffle.partitions":                     "auto",
    "spark.sql.files.maxPartitionBytes":                "134217728",
    "spark.serializer":     "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max":                  "512m",
}

_SHUFFLE_CONFIGS = {
    "spark.shuffle.service.enabled":                    "true",
    "spark.reducer.maxSizeInFlight":                    "96m",
    "spark.shuffle.compress":                           "true",
    "spark.shuffle.spill.compress":                     "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "268435456",
}

_GC_OPTS = (
    "-XX:+UseG1GC -XX:G1HeapRegionSize=32M "
    "-XX:InitiatingHeapOccupancyPercent=35 "
    "-XX:+G1SummarizeConcMark"
)

_BROADCAST_ROW_THRESHOLD = 500_000


def _build_config_block(configs: Dict[str, str], overrides: Dict[str, str]) -> str:
    merged = {**configs, **overrides}
    lines  = [f'    .config("{k}", "{v}")' for k, v in sorted(merged.items())]
    return "\n".join(lines)


def _inject_spark_configs(code: str, extra_overrides: Dict[str, str]) -> tuple:
    """Inject config calls into existing SparkSession.builder chain."""
    changelog = []
    merged    = {**_BASE_CONFIGS, **extra_overrides}

    # Find .getOrCreate() / .enableHiveSupport()
    pattern = r'(SparkSession\s*\.\s*builder(?:.*?))(\s*\.(?:getOrCreate|enableHiveSupport)\s*\(\s*\))'
    match   = re.search(pattern, code, re.DOTALL)
    if match:
        config_block = "\n".join(f'    .config("{k}", "{v}")' for k, v in sorted(merged.items()))
        # Check which configs are already present
        missing = {k: v for k, v in merged.items() if k not in code}
        if missing:
            block = "\n".join(f'    .config("{k}", "{v}")' for k, v in sorted(missing.items()))
            new_code = code[:match.start(2)] + f"\n{block}" + code[match.start(2):]
            changelog.append({
                "fix":         "inject_spark_configs",
                "description": f"Injected {len(missing)} Spark config(s) into SparkSession.builder",
                "configs":     missing,
            })
            return new_code, changelog
    return code, changelog


def _fix_repartition_1(code: str) -> tuple:
    changelog = []
    new_code, n = re.subn(r'\.repartition\(\s*1\s*\)', '.coalesce(1)', code)
    if n:
        changelog.append({
            "fix":         "repartition_1_to_coalesce",
            "description": f"Replaced {n}× .repartition(1) with .coalesce(1) to preserve upstream parallelism",
        })
    return new_code, changelog


def _add_coalesce_before_write(code: str) -> tuple:
    changelog = []
    # Add coalesce hint comment before write calls that don't have one
    new_code, n = re.subn(
        r'(?<!\# TODO: coalesce)\n(\s*)([\w_]+\.write\b)',
        r'\n\1# TODO: consider .coalesce(num_partitions) here to control output file count\n\1\2',
        code,
    )
    if n:
        changelog.append({
            "fix":         "coalesce_before_write_hint",
            "description": f"Added {n} coalesce TODO comments before write operations",
        })
    return new_code, changelog


def _add_broadcast_hints(code: str, tables: List[Dict]) -> tuple:
    changelog = []
    small_tables = [t.get("table", t.get("name", "")) for t in tables
                    if int(t.get("record_count", t.get("records", 0))) < _BROADCAST_ROW_THRESHOLD]
    for tbl in small_tables:
        if tbl and f"broadcast({tbl}" not in code:
            pattern = rf'\.join\(\s*({re.escape(tbl)})\s*,'
            new_code, n = re.subn(pattern, r'.join(broadcast(\1),', code)
            if n:
                code = new_code
                # Ensure broadcast import
                if "from pyspark.sql.functions import broadcast" not in code and \
                   "broadcast" not in code:
                    code = "from pyspark.sql.functions import broadcast\n" + code
                changelog.append({
                    "fix":         "broadcast_hint",
                    "description": f"Added broadcast() hint for small table '{tbl}' ({_BROADCAST_ROW_THRESHOLD:,} rows)",
                })
    return code, changelog


def _annotate_collect(code: str) -> tuple:
    changelog = []
    new_code, n = re.subn(
        r'(?<!# WARNING: )\.collect\(\)',
        '# WARNING: .collect() pulls ALL data to driver — ensure dataset is small\n.collect()',
        code,
    )
    if n:
        changelog.append({
            "fix":         "annotate_collect",
            "description": f"Added {n} safety warning comments on .collect() calls",
        })
    return new_code, changelog


def _annotate_udf(code: str) -> tuple:
    changelog = []
    new_code, n = re.subn(
        r'(@udf)',
        r'# TODO: replace with built-in Spark SQL functions (concat, when, regexp_extract) for better performance\n\1',
        code,
    )
    if n:
        changelog.append({
            "fix":         "annotate_udf",
            "description": f"Added {n} TODO annotations on UDF definitions",
        })
    return new_code, changelog


def _inject_gc_tuning(code: str, heap_pressure: str) -> tuple:
    changelog = []
    if heap_pressure in ("high", "critical") and "UseG1GC" not in code:
        gc_config = (
            '    .config("spark.executor.extraJavaOptions", '
            f'"{_GC_OPTS}")\n'
            '    .config("spark.driver.extraJavaOptions", '
            f'"{_GC_OPTS}")'
        )
        pattern = r'(SparkSession\s*\.\s*builder(?:.*?))(\s*\.(?:getOrCreate|enableHiveSupport)\s*\(\s*\))'
        match = re.search(pattern, code, re.DOTALL)
        if match:
            code = code[:match.start(2)] + f"\n{gc_config}" + code[match.start(2):]
            changelog.append({"fix": "inject_gc_tuning",
                               "description": f"Injected G1GC JVM options (heap pressure: {heap_pressure})"})
    return code, changelog


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def apply_spark_config_recommendations(
    script_content: str,
    code_analysis_json: str = "{}",
    glue_metrics_json: str = "{}",
) -> str:
    """
    Inject recommended Spark configurations into a PySpark script's SparkSession builder.

    Args:
        script_content:     Full PySpark source code.
        code_analysis_json: JSON from CodeAnalyzerAgent (spark_configs list).
        glue_metrics_json:  JSON from GlueMetricsAgent (spark_config_overrides dict).

    Returns:
        JSON with modified_script, fixes_applied, and changelog.
    """
    try:
        code_analysis = json.loads(code_analysis_json) if code_analysis_json else {}
        glue_metrics  = json.loads(glue_metrics_json)  if glue_metrics_json  else {}

        # Build config overrides from code analysis
        extra: Dict[str, str] = {}
        for cfg in code_analysis.get("spark_configs", []):
            if isinstance(cfg, dict) and "config" in cfg and "recommended_value" in cfg:
                extra[cfg["config"]] = str(cfg["recommended_value"])

        # Merge metric overrides (higher priority)
        metric_overrides = glue_metrics.get("spark_config_overrides", {})
        extra.update(metric_overrides)

        heap_pressure = glue_metrics.get("heap_pressure", "none")

        code, cl1 = _inject_spark_configs(script_content, extra)
        code, cl2 = _inject_gc_tuning(code, heap_pressure)
        changelog = cl1 + cl2

        return json.dumps({
            "modified_script": code,
            "fixes_applied":   len(changelog),
            "changelog":       changelog,
        })
    except Exception as exc:
        logger.error("Config injection failed: %s", exc)
        return json.dumps({"error": str(exc), "modified_script": script_content})


@tool
def fix_code_anti_patterns(
    script_content: str,
    source_tables_json: str = "[]",
) -> str:
    """
    Apply rule-based code fixes: repartition(1)→coalesce, broadcast hints,
    .collect() annotations, UDF annotations, coalesce-before-write hints.

    Args:
        script_content:     Full PySpark source code.
        source_tables_json: JSON list of table descriptors (for broadcast detection).

    Returns:
        JSON with modified_script, fixes_applied, and changelog.
    """
    try:
        tables = json.loads(source_tables_json) if source_tables_json else []
        code   = script_content
        total_cl: List[Dict] = []

        code, cl = _fix_repartition_1(code);             total_cl.extend(cl)
        code, cl = _add_coalesce_before_write(code);     total_cl.extend(cl)
        code, cl = _add_broadcast_hints(code, tables);   total_cl.extend(cl)
        code, cl = _annotate_collect(code);              total_cl.extend(cl)
        code, cl = _annotate_udf(code);                  total_cl.extend(cl)

        return json.dumps({
            "modified_script": code,
            "fixes_applied":   len(total_cl),
            "changelog":       total_cl,
        })
    except Exception as exc:
        logger.error("Anti-pattern fix failed: %s", exc)
        return json.dumps({"error": str(exc), "modified_script": script_content})


@tool
def apply_all_recommendations(
    script_content: str,
    code_analysis_json: str = "{}",
    glue_metrics_json: str = "{}",
    source_tables_json: str = "[]",
) -> str:
    """
    Apply ALL recommendations in one shot: Spark configs + GC tuning + code fixes.

    Args:
        script_content:     Full PySpark source code.
        code_analysis_json: JSON from CodeAnalyzerAgent.
        glue_metrics_json:  JSON from GlueMetricsAgent.
        source_tables_json: JSON list of source table descriptors.

    Returns:
        JSON with modified_script, total fixes_applied, full changelog,
        and worker_recommendation (if glue_metrics provided).
    """
    try:
        # Step 1: config injection
        cfg_result = json.loads(
            apply_spark_config_recommendations.__wrapped__(
                script_content, code_analysis_json, glue_metrics_json
            )
        )
        code     = cfg_result.get("modified_script", script_content)
        changelog = cfg_result.get("changelog", [])

        # Step 2: code pattern fixes
        fix_result = json.loads(fix_code_anti_patterns.__wrapped__(code, source_tables_json))
        code     = fix_result.get("modified_script", code)
        changelog += fix_result.get("changelog", [])

        # Worker recommendation from metrics
        glue = json.loads(glue_metrics_json) if glue_metrics_json else {}
        w_rec = glue.get("worker_recommendation")

        return json.dumps({
            "modified_script":       code,
            "original_script":       script_content,
            "fixes_applied":         len(changelog),
            "changelog":             changelog,
            "worker_recommendation": w_rec,
        })
    except Exception as exc:
        logger.error("apply_all_recommendations failed: %s", exc)
        return json.dumps({"error": str(exc), "modified_script": script_content})


@tool
def diff_scripts(original_script: str, modified_script: str) -> str:
    """
    Generate a unified diff between original and modified scripts.

    Args:
        original_script: Original PySpark source.
        modified_script: Modified PySpark source.

    Returns:
        JSON with unified_diff string and change_line_count.
    """
    try:
        import difflib
        orig_lines = original_script.splitlines(keepends=True)
        mod_lines  = modified_script.splitlines(keepends=True)
        diff = list(difflib.unified_diff(orig_lines, mod_lines,
                                          fromfile="original.py", tofile="optimised.py"))
        added   = sum(1 for l in diff if l.startswith("+") and not l.startswith("+++"))
        removed = sum(1 for l in diff if l.startswith("-") and not l.startswith("---"))
        return json.dumps({
            "unified_diff":      "".join(diff[:200]),  # cap for readability
            "lines_added":       added,
            "lines_removed":     removed,
            "change_line_count": added + removed,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_recommendation_applier_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                         region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[apply_spark_config_recommendations, fix_code_anti_patterns,
                        apply_all_recommendations, diff_scripts])
