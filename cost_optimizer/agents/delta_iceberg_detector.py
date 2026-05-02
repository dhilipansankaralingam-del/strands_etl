"""
Delta / Iceberg Detector Agent
================================
Detects whether a PySpark script reads or writes Delta Lake or Apache Iceberg
tables and emits tailored maintenance + optimization recommendations.

Delta Lake patterns detected
-----------------------------
  - from delta.tables import DeltaTable
  - .format("delta")
  - DeltaTable.forPath / forName
  - USING delta (SQL DDL)
  - spark.read.delta / spark.write.delta

Apache Iceberg patterns detected
----------------------------------
  - .format("iceberg")
  - from pyiceberg import ...
  - USING iceberg (SQL DDL)
  - spark.read.format("iceberg")
  - catalog.system.* calls

Recommendations emitted
-----------------------
Delta
  - OPTIMIZE (file compaction)
  - ZORDER (column-level clustering)
  - VACUUM (remove stale data files)
  - Auto-optimize settings
  - Change data feed for incremental pipelines
  - Z-order column selection guidance

Iceberg
  - rewrite_data_files (compaction procedure)
  - expire_snapshots
  - remove_orphan_files
  - Partition evolution guidance
  - Hidden partitioning recommendations
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ─── Detection patterns ────────────────────────────────────────────────────────

_DELTA_PATTERNS = [
    r'from\s+delta\.tables\s+import',
    r'import\s+delta',
    r'\.format\s*\(\s*["\']delta["\']\s*\)',
    r'DeltaTable\s*\.',
    r'USING\s+delta\b',
    r'spark\.read\.delta\b',
    r'spark\.write\.delta\b',
    r'delta\.`',
]

_ICEBERG_PATTERNS = [
    r'from\s+pyiceberg',
    r'import\s+pyiceberg',
    r'\.format\s*\(\s*["\']iceberg["\']\s*\)',
    r'USING\s+iceberg\b',
    r'catalog\.system\.',
    r'spark\.read\.format\s*\(\s*["\']iceberg["\']\s*\)',
    r'catalog\.create_table\b',
    r'IcebergTable\b',
]

_DELTA_WRITE_PATTERNS = [
    r'\.write\b.*\.format\s*\(\s*["\']delta["\']\s*\)',
    r'\.saveAsTable\s*\(',
    r'DeltaTable\.\w+',
    r'mode\s*\(\s*["\']overwrite["\']',
    r'mode\s*\(\s*["\']append["\']',
]


class DeltaIcebergDetectorAgent:
    """Detect Delta/Iceberg table usage and produce maintenance recommendations."""

    # ─── Public entry point ────────────────────────────────────────────────────

    def detect(
        self,
        script_path: str,
        script_content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Detect Delta / Iceberg usage in a PySpark script.

        Returns
        -------
        success              – bool
        format_detected      – "delta" | "iceberg" | "both" | "none"
        delta_detected       – bool
        iceberg_detected     – bool
        write_operations     – list of detected write patterns
        recommendations      – list of recommendation dicts
        maintenance_commands – list of ready-to-run SQL / PySpark commands
        code_snippets        – dict of platform → runnable code snippet
        errors               – list[str]
        """
        if script_content is None:
            p = Path(script_path)
            if not p.exists():
                return {"success": False, "errors": [f"File not found: {script_path}"]}
            script_content = p.read_text(errors="replace")

        try:
            delta_hit   = _match_patterns(script_content, _DELTA_PATTERNS)
            iceberg_hit = _match_patterns(script_content, _ICEBERG_PATTERNS)

            if delta_hit and iceberg_hit:
                fmt = "both"
            elif delta_hit:
                fmt = "delta"
            elif iceberg_hit:
                fmt = "iceberg"
            else:
                fmt = "none"

            write_ops = _match_patterns(script_content, _DELTA_WRITE_PATTERNS, return_matches=True)

            recs:      List[Dict] = []
            maint_cmds: List[str] = []
            snippets:   Dict[str, str] = {}

            # Infer table name/path hint from the script
            table_hint = _infer_table_hint(script_content)

            if delta_hit:
                dr, dc, ds = _delta_recommendations(script_content, table_hint)
                recs.extend(dr)
                maint_cmds.extend(dc)
                snippets.update(ds)

            if iceberg_hit:
                ir, ic, iss = _iceberg_recommendations(script_content, table_hint)
                recs.extend(ir)
                maint_cmds.extend(ic)
                snippets.update(iss)

            return {
                "success":              True,
                "format_detected":      fmt,
                "delta_detected":       delta_hit,
                "iceberg_detected":     iceberg_hit,
                "write_operations":     write_ops,
                "recommendations":      recs,
                "maintenance_commands": maint_cmds,
                "code_snippets":        snippets,
                "errors":               [],
            }
        except Exception as exc:
            return {"success": False, "errors": [str(exc)], "format_detected": "none"}


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _match_patterns(
    code: str,
    patterns: List[str],
    return_matches: bool = False,
) -> Any:
    """Return True/False or list of matched strings."""
    matched = []
    for pat in patterns:
        for m in re.finditer(pat, code, re.IGNORECASE | re.MULTILINE):
            if return_matches:
                matched.append(m.group())
            else:
                return True
    return matched if return_matches else False


def _infer_table_hint(code: str) -> str:
    """Try to extract a table name/path from the script for use in examples."""
    for m in re.finditer(
        r'DeltaTable\.(?:forPath|forName)\s*\(\s*(?:spark\s*,\s*)?["\']([^"\']+)["\']', code
    ):
        return m.group(1)
    for m in re.finditer(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        return m.group(1)
    for m in re.finditer(r'\.format\s*\(\s*["\'](?:delta|iceberg)["\']\s*\).*\.save\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        return m.group(1)
    return "your_table"


# ─── Delta recommendations ─────────────────────────────────────────────────────

def _delta_recommendations(
    code: str, table_hint: str
) -> Tuple[List[Dict], List[str], Dict[str, str]]:
    """Return (recommendations, maintenance_commands, code_snippets) for Delta."""
    recs:   List[Dict] = []
    cmds:   List[str]  = []
    snips:  Dict[str, str] = {}

    # Determine if there are frequent small writes
    has_append    = bool(re.search(r'mode.*append', code, re.I))
    has_overwrite = bool(re.search(r'mode.*overwrite', code, re.I))
    has_streaming = bool(re.search(r'readStream|writeStream', code, re.I))
    has_merge     = bool(re.search(r'DeltaTable\.\w+.*merge|\.merge\(', code, re.I))
    has_zorder    = bool(re.search(r'zorder|z_order|ZORDER', code, re.I))

    # ── OPTIMIZE (compaction) ──────────────────────────────────────────────────
    recs.append({
        "priority":    "P1",
        "category":    "delta_optimize",
        "title":       "Run OPTIMIZE to compact small files",
        "description": (
            "Delta tables accumulate small files from frequent writes / appends. "
            "Run OPTIMIZE periodically (daily or after large loads) to merge them "
            "into larger Parquet files (~256 MB), improving scan performance."
        ),
        "impact": "Reduces scan latency by 2-10× for tables with many small files",
    })
    cmds.append(f'spark.sql("OPTIMIZE {table_hint}")')
    cmds.append(
        f"# Or via Python API:\n"
        f"from delta.tables import DeltaTable\n"
        f'dt = DeltaTable.forName(spark, "{table_hint}")\n'
        f"dt.optimize().executeCompaction()"
    )

    # ── ZORDER ────────────────────────────────────────────────────────────────
    if not has_zorder:
        # Try to infer common filter columns
        filter_cols = _infer_filter_columns(code)
        z_cols = ", ".join(filter_cols[:3]) if filter_cols else "partition_col, id_col"
        recs.append({
            "priority":    "P1",
            "category":    "delta_zorder",
            "title":       "Add Z-ORDER clustering for common query predicates",
            "description": (
                "Z-ORDER co-locates related data in the same files, enabling "
                "aggressive data skipping. Apply on columns used in WHERE / JOIN conditions."
            ),
            "suggested_columns": filter_cols[:3],
            "impact": "Can reduce data scanned by 50-99% for selective queries",
        })
        cmds.append(f'spark.sql("OPTIMIZE {table_hint} ZORDER BY ({z_cols})")')
        snips["delta_zorder_python"] = (
            f"from delta.tables import DeltaTable\n"
            f'dt = DeltaTable.forName(spark, "{table_hint}")\n'
            f"dt.optimize()\\\n"
            f'   .zorder("{filter_cols[0] if filter_cols else "id"}")\\\n'
            f"   .executeCompaction()"
        )

    # ── VACUUM ─────────────────────────────────────────────────────────────────
    recs.append({
        "priority":    "P2",
        "category":    "delta_vacuum",
        "title":       "Schedule VACUUM to remove stale data files",
        "description": (
            "Delta retains old versions of data files for time-travel. "
            "Run VACUUM regularly (e.g. weekly) to reclaim S3 storage. "
            "Default retention is 7 days (168 hours)."
        ),
        "impact": "Reduces S3 storage costs and LIST API calls",
    })
    cmds.append(f'spark.sql("VACUUM {table_hint} RETAIN 168 HOURS")')

    # ── Auto-optimize ──────────────────────────────────────────────────────────
    if has_append and not has_streaming:
        recs.append({
            "priority":    "P2",
            "category":    "delta_auto_optimize",
            "title":       "Enable auto-optimize for frequent append workloads",
            "description": (
                "For tables that receive frequent small appends, "
                "delta.autoOptimize reduces the need for manual OPTIMIZE runs."
            ),
            "impact": "Reduces small-file accumulation automatically",
        })
        cmds.append(
            f'spark.sql("ALTER TABLE {table_hint} '
            f'SET TBLPROPERTIES (\'delta.autoOptimize.optimizeWrite\' = \'true\', '
            f'\'delta.autoOptimize.autoCompact\' = \'true\')")'
        )
        snips["delta_auto_optimize_spark_conf"] = (
            'spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")\n'
            'spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")'
        )

    # ── Change Data Feed ──────────────────────────────────────────────────────
    if has_merge or has_append:
        recs.append({
            "priority":    "P3",
            "category":    "delta_cdf",
            "title":       "Enable Change Data Feed for incremental downstream pipelines",
            "description": (
                "Delta Change Data Feed captures row-level changes (INSERT/UPDATE/DELETE). "
                "Downstream jobs can read only the changed rows instead of full table scans."
            ),
            "impact": "Eliminates full-table re-scans for downstream ETL",
        })
        cmds.append(
            f'spark.sql("ALTER TABLE {table_hint} '
            f'SET TBLPROPERTIES (\'delta.enableChangeDataFeed\' = \'true\')")'
        )
        snips["delta_cdf_read"] = (
            '# Read changed rows since version N\n'
            f'changes = spark.read.format("delta")\\\n'
            f'    .option("readChangeFeed", "true")\\\n'
            f'    .option("startingVersion", 0)\\\n'
            f'    .table("{table_hint}")'
        )

    return recs, cmds, snips


# ─── Iceberg recommendations ───────────────────────────────────────────────────

def _iceberg_recommendations(
    code: str, table_hint: str
) -> Tuple[List[Dict], List[str], Dict[str, str]]:
    """Return (recommendations, maintenance_commands, code_snippets) for Iceberg."""
    recs:  List[Dict] = []
    cmds:  List[str]  = []
    snips: Dict[str, str] = {}

    has_partition = bool(re.search(r'partitionBy|partition_by|partitioned', code, re.I))

    # ── rewrite_data_files (compaction) ───────────────────────────────────────
    recs.append({
        "priority":    "P1",
        "category":    "iceberg_compact",
        "title":       "Run rewrite_data_files to compact small files",
        "description": (
            "Iceberg's rewrite_data_files procedure merges small files into larger ones. "
            "Schedule daily/weekly or trigger after batch loads."
        ),
        "impact": "Reduces scan latency; improves reader throughput",
    })
    cmds.append(
        f"spark.sql(\"CALL spark_catalog.system.rewrite_data_files"
        f"(table => '{table_hint}', strategy => 'binpack', "
        f"options => map('target-file-size-bytes','268435456'))\")"
    )
    snips["iceberg_compact_python"] = (
        "from pyiceberg.catalog import load_catalog\n"
        "catalog = load_catalog('default')\n"
        f"table   = catalog.load_table('{table_hint}')\n"
        "table.rewrite_data_files()"
    )

    # ── expire_snapshots ──────────────────────────────────────────────────────
    recs.append({
        "priority":    "P2",
        "category":    "iceberg_expire_snapshots",
        "title":       "Expire old snapshots to reclaim storage",
        "description": (
            "Iceberg retains all historical snapshots by default. "
            "Expire snapshots older than your time-travel window to reduce S3 costs."
        ),
        "impact": "Reduces S3 storage and metadata overhead",
    })
    cmds.append(
        f"spark.sql(\"CALL spark_catalog.system.expire_snapshots"
        f"(table => '{table_hint}', older_than => TIMESTAMP '2024-01-01 00:00:00.000', "
        f"retain_last => 5)\")"
    )

    # ── remove_orphan_files ───────────────────────────────────────────────────
    recs.append({
        "priority":    "P3",
        "category":    "iceberg_orphan_files",
        "title":       "Remove orphan files to clean up unreferenced data",
        "description": (
            "Orphan files accumulate from failed/partial writes or job retries. "
            "Run remove_orphan_files periodically to reclaim space."
        ),
        "impact": "Reclaims S3 storage from partial writes and job failures",
    })
    cmds.append(
        f"spark.sql(\"CALL spark_catalog.system.remove_orphan_files"
        f"(table => '{table_hint}')\")"
    )

    # ── Hidden partitioning ───────────────────────────────────────────────────
    if not has_partition:
        recs.append({
            "priority":    "P2",
            "category":    "iceberg_partitioning",
            "title":       "Consider hidden partitioning for time-based queries",
            "description": (
                "Iceberg supports hidden partitioning (YEAR, MONTH, DAY, HOUR transforms) "
                "that automatically partition data without exposing partition columns to "
                "the user. This avoids partition-column type mismatches."
            ),
            "impact": "Enables partition pruning without explicit partition columns in queries",
        })
        snips["iceberg_partition_spec"] = (
            "# Create an Iceberg table with hidden partitioning\n"
            "spark.sql(\"\"\"\n"
            "  CREATE TABLE catalog.db.events (\n"
            "    event_id STRING,\n"
            "    event_ts TIMESTAMP,\n"
            "    user_id  STRING\n"
            "  ) USING iceberg\n"
            "  PARTITIONED BY (days(event_ts))\n"
            "\"\"\")"
        )

    # ── Partition evolution ────────────────────────────────────────────────────
    recs.append({
        "priority":    "P3",
        "category":    "iceberg_partition_evolution",
        "title":       "Use partition evolution to change partitioning without rewrites",
        "description": (
            "Iceberg supports adding or replacing partition specs on existing data "
            "without rewriting files. New data uses the new spec; old data stays as-is."
        ),
        "impact": "Enables zero-downtime partition scheme changes",
    })
    cmds.append(
        f"spark.sql(\"ALTER TABLE {table_hint} "
        f"REPLACE PARTITION FIELD event_date WITH days(event_ts)\")"
    )

    return recs, cmds, snips


# ─── Utility ──────────────────────────────────────────────────────────────────

def _infer_filter_columns(code: str) -> List[str]:
    """Extract column names commonly used in .filter() or WHERE clauses."""
    cols: List[str] = []

    # .filter("col = value") or .filter(F.col("col") == ...)
    for m in re.finditer(r'\.filter\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        # Simple equality: "col = val" or "col > val"
        for cm in re.finditer(r'(\w+)\s*[=><!]', m.group(1)):
            col = cm.group(1)
            if col.lower() not in ("select", "where", "from", "join", "on", "true", "false"):
                cols.append(col)
    for m in re.finditer(r'F\.col\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        cols.append(m.group(1))

    # Deduplicate while preserving order
    seen: set = set()
    result = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result[:5]
