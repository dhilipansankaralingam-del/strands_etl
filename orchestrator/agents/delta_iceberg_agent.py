"""
Delta / Iceberg Detector Agent
================================
Detects Delta Lake or Apache Iceberg usage in PySpark scripts and emits
tailored maintenance + optimisation commands.

Based on DeltaIcebergDetectorAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Lakehouse Architect** specialising in Delta Lake and Apache Iceberg.

Detect which open-table format is used and produce:
1. Maintenance commands (OPTIMIZE, VACUUM, ZORDER / rewrite_data_files, expire_snapshots)
2. Performance configuration recommendations (auto-optimize, liquid clustering, etc.)
3. Partition evolution opportunities
4. Change-data-feed / incremental pipeline patterns

Return structured JSON:
{
  "format_detected": "delta|iceberg|both|none",
  "delta_detected": bool,
  "iceberg_detected": bool,
  "recommendations": [],
  "maintenance_commands": [],
  "code_snippets": {}
}
Return ONLY valid JSON.
"""

_DELTA_PATS = [
    r'from\s+delta\.tables\s+import',
    r'\.format\s*\(\s*["\']delta["\']\s*\)',
    r'DeltaTable\s*\.',
    r'USING\s+delta\b',
    r'spark\.read\.delta\b',
]
_ICEBERG_PATS = [
    r'from\s+pyiceberg',
    r'\.format\s*\(\s*["\']iceberg["\']\s*\)',
    r'USING\s+iceberg\b',
    r'catalog\.system\.',
]
_WRITE_PATS = [
    r'\.write\b.*\.format\s*\(\s*["\']delta["\']\s*\)',
    r'\.saveAsTable\s*\(',
    r'mode\s*\(\s*["\']overwrite["\']',
    r'mode\s*\(\s*["\']append["\']',
]


def _match(code: str, pats: List[str]) -> bool:
    return any(re.search(p, code, re.IGNORECASE | re.MULTILINE) for p in pats)


def _write_ops(code: str) -> List[str]:
    out = []
    for p in _WRITE_PATS:
        for m in re.finditer(p, code, re.IGNORECASE):
            out.append(m.group()[:80])
    return out


def _table_hint(code: str) -> str:
    for m in re.finditer(r'DeltaTable\.(?:forPath|forName)\s*\(\s*(?:spark\s*,\s*)?["\']([^"\']+)["\']', code):
        return m.group(1)
    for m in re.finditer(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        return m.group(1)
    return "your_table"


def _delta_recs(code: str, hint: str) -> Tuple[List[Dict], List[str], Dict[str, str]]:
    recs, cmds, snips = [], [], {}

    recs.append({
        "priority": "P0",
        "title":    "Schedule OPTIMIZE for small-file compaction",
        "description": "Delta accumulates small files from incremental writes — OPTIMIZE merges them.",
        "impact":   "50–80% I/O reduction for downstream queries",
    })
    cmds.append(f"OPTIMIZE {hint}")

    if ".withColumn(" in code or "withColumn(" in code:
        recs.append({
            "priority": "P1",
            "title":    "Add ZORDER on high-cardinality filter columns",
            "description": "ZORDER co-locates data by column values for faster predicate pushdown.",
            "impact":   "2–10× query speedup for selective filters",
        })
        cmds.append(f"OPTIMIZE {hint} ZORDER BY (/* high-cardinality filter col */)")

    recs.append({
        "priority": "P1",
        "title":    "Schedule VACUUM to remove stale data files",
        "description": "VACUUM deletes files older than the retention period (default 7 days).",
        "impact":   "S3 storage cost reduction",
    })
    cmds.append(f"VACUUM {hint} RETAIN 168 HOURS")

    recs.append({
        "priority": "P2",
        "title":    "Enable auto-optimize for append workloads",
        "description": "Auto-compaction + optimized writes reduce small-file problem automatically.",
    })
    snips["delta_auto_optimize"] = (
        f'spark.sql("ALTER TABLE {hint} SET TBLPROPERTIES ('
        "'delta.autoOptimize.optimizeWrite=true', "
        "'delta.autoOptimize.autoCompact=true')\")"
    )

    if "append" in code.lower():
        recs.append({
            "priority": "P2",
            "title":    "Enable Change Data Feed for incremental consumers",
            "description": "CDF lets downstream jobs read only changed rows.",
        })
        snips["delta_cdf"] = (
            f"spark.sql(\"ALTER TABLE {hint} SET TBLPROPERTIES "
            "('delta.enableChangeDataFeed' = 'true')\")"
        )

    return recs, cmds, snips


def _iceberg_recs(code: str, hint: str) -> Tuple[List[Dict], List[str], Dict[str, str]]:
    recs, cmds, snips = [], [], {}

    recs.append({
        "priority": "P0",
        "title":    "Schedule rewrite_data_files for compaction",
        "description": "Iceberg's rewrite_data_files consolidates small files and rewrites them optimally.",
        "impact":   "Faster scans, reduced metadata overhead",
    })
    cmds.append(f"CALL spark_catalog.system.rewrite_data_files('{hint}')")

    recs.append({
        "priority": "P1",
        "title":    "Schedule expire_snapshots to reclaim S3 storage",
        "description": "Old snapshots retain references to deleted files — expiring them frees storage.",
    })
    cmds.append(f"CALL spark_catalog.system.expire_snapshots('{hint}', TIMESTAMP '{'{older_than}'}', 100)")

    recs.append({
        "priority": "P1",
        "title":    "Schedule remove_orphan_files",
        "description": "Cleans up files that are no longer referenced by any snapshot.",
    })
    cmds.append(f"CALL spark_catalog.system.remove_orphan_files('{hint}')")

    if "partitionBy" in code.lower() or "partition_by" in code.lower():
        recs.append({
            "priority": "P2",
            "title":    "Use hidden partitioning for time-based columns",
            "description": "Iceberg hidden partitions (months(ts), days(ts)) avoid over-partitioning.",
        })
        snips["iceberg_hidden_partition"] = (
            "from pyiceberg.expressions import months\n"
            "schema = Schema(NestedField(1, 'ts', TimestampType()))\n"
            "spec = PartitionSpec(PartitionField(1, 1000, MonthTransform(), 'ts_month'))"
        )

    return recs, cmds, snips


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def detect_table_format(script_content: str) -> str:
    """
    Detect Delta Lake or Apache Iceberg usage in a PySpark script.

    Args:
        script_content: Full PySpark script source code.

    Returns:
        JSON with format_detected, delta_detected, iceberg_detected, write_operations,
        recommendations, maintenance_commands, and runnable code_snippets.
    """
    try:
        delta_hit   = _match(script_content, _DELTA_PATS)
        iceberg_hit = _match(script_content, _ICEBERG_PATS)
        fmt         = ("both" if delta_hit and iceberg_hit else
                       "delta" if delta_hit else
                       "iceberg" if iceberg_hit else "none")
        hint        = _table_hint(script_content)
        write_ops   = _write_ops(script_content)

        recs, cmds, snips = [], [], {}
        if delta_hit:
            dr, dc, ds = _delta_recs(script_content, hint)
            recs.extend(dr); cmds.extend(dc); snips.update(ds)
        if iceberg_hit:
            ir, ic, iss = _iceberg_recs(script_content, hint)
            recs.extend(ir); cmds.extend(ic); snips.update(iss)

        return json.dumps({
            "format_detected":      fmt,
            "delta_detected":       delta_hit,
            "iceberg_detected":     iceberg_hit,
            "inferred_table_hint":  hint,
            "write_operations":     write_ops,
            "recommendations":      recs,
            "maintenance_commands": cmds,
            "code_snippets":        snips,
        })
    except Exception as exc:
        logger.error("Format detection failed: %s", exc)
        return json.dumps({"error": str(exc), "format_detected": "none"})


@tool
def get_maintenance_schedule(script_content: str, format_hint: str = "auto") -> str:
    """
    Generate a recommended maintenance schedule for the detected table format.

    Args:
        script_content: PySpark script source code.
        format_hint:    "delta", "iceberg", or "auto" (detect automatically).

    Returns:
        JSON with daily, weekly, monthly maintenance tasks.
    """
    try:
        if format_hint == "auto":
            delta_hit   = _match(script_content, _DELTA_PATS)
            iceberg_hit = _match(script_content, _ICEBERG_PATS)
        else:
            delta_hit   = format_hint == "delta"
            iceberg_hit = format_hint == "iceberg"

        hint = _table_hint(script_content)
        schedule: Dict[str, List[str]] = {"daily": [], "weekly": [], "monthly": []}

        if delta_hit:
            schedule["daily"].append(f"OPTIMIZE {hint}  -- compact small files")
            schedule["weekly"].append(f"VACUUM {hint} RETAIN 168 HOURS  -- cleanup stale files")
            schedule["monthly"].append(f"ANALYZE TABLE {hint} COMPUTE STATISTICS  -- refresh stats")

        if iceberg_hit:
            schedule["daily"].append(f"CALL spark_catalog.system.rewrite_data_files('{hint}')")
            schedule["weekly"].append(f"CALL spark_catalog.system.expire_snapshots('{hint}', ...)")
            schedule["monthly"].append(f"CALL spark_catalog.system.remove_orphan_files('{hint}')")

        return json.dumps({"maintenance_schedule": schedule, "table_hint": hint})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def check_partition_strategy(script_content: str) -> str:
    """
    Analyse the partition strategy in the script and recommend improvements.

    Args:
        script_content: Full PySpark script source code.

    Returns:
        JSON with current partition columns, issues, and recommendations.
    """
    try:
        issues, recs = [], []
        part_cols = re.findall(r'partitionBy\s*\(\s*["\']([^"\']+)["\']\s*\)', script_content)

        if not part_cols:
            issues.append("No partitionBy found — output may produce too many or too few files")
            recs.append("Add .partitionBy('date_column') before .write for better query pruning")
        else:
            low_card = ["status", "flag", "type", "active", "boolean"]
            for col in part_cols:
                if any(kw in col.lower() for kw in low_card):
                    issues.append(f"Partition column '{col}' may have very low cardinality → too few partitions")
                    recs.append(f"Consider partitioning by a higher-cardinality column instead of '{col}'")

        has_repartition = bool(re.search(r'\.repartition\(', script_content))
        has_coalesce    = bool(re.search(r'\.coalesce\(', script_content))

        return json.dumps({
            "partition_columns": part_cols,
            "issues":            issues,
            "recommendations":   recs,
            "has_repartition":   has_repartition,
            "has_coalesce":      has_coalesce,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_delta_iceberg_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[detect_table_format, get_maintenance_schedule, check_partition_strategy])
