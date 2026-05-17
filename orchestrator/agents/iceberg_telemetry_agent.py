"""
Iceberg Telemetry Agent
=======================
Queries Apache Iceberg system tables via Athena to gather REAL table telemetry:

  $files       → file count, avg/min/max size, tiny-file count, CV, record count
  $snapshots   → snapshot count, oldest/newest, added/deleted files & records
  $partitions  → partition count, skew_ratio, max/min/avg records per partition
  $manifests   → manifest count, avg files per manifest, empty manifests

This is the data-collection layer that feeds the SizingAgent and CodeAnalyzerAgent
with ground-truth numbers instead of heuristic estimates.

Ported from cost_optimizer/agents/size_analyzer.py (KEYJa branch) — specifically
the _build_llm_prompt Athena telemetry block.

Design:
  - Every tool runs Athena SQL via boto3, polls for completion, parses CSV result
  - All tools are stateless (no class state, no caching)
  - query_iceberg_table_stats() is the primary entry-point — runs all 4 queries
    and returns a combined iceberg_stats dict ready for SizingAgent + CodeAnalyzerAgent
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TINY_FILE_MB   = 10        # files below this are "tiny"
_SMALL_FILE_MB  = 128       # files below this are "small"
_ATHENA_POLL_S  = 2         # seconds between completion polls
_ATHENA_TIMEOUT = 120       # max seconds to wait for a query

_SYSTEM_PROMPT = """
You are an **Iceberg Table Health Analyst**.

Use the available tools to query Iceberg system tables via Athena and produce a
comprehensive telemetry report covering:
  1. File size distribution and tiny-file pathologies
  2. Snapshot bloat and write amplification
  3. Partition skew ratio and straggler risk
  4. Manifest explosion and query-planning overhead
  5. Growth rate from snapshot timestamps
  6. Overall health grade (A–F) and prioritised remediation actions

Always call query_iceberg_table_stats first. For tables with skew_ratio > 3,
additionally call query_iceberg_partition_details. Return structured JSON only.
"""

# ---------------------------------------------------------------------------
# Table reference parsing helper
# ---------------------------------------------------------------------------

def _parse_table_ref(ref: str) -> Tuple[str, str]:
    """
    Parse a table reference that may be 'DB_prd.tablename' or just 'tablename'.
    Returns (database, table) tuple.
    """
    if "." in ref:
        parts = ref.split(".", 1)
        return parts[0].strip(), parts[1].strip()
    return "default", ref.strip()


# ---------------------------------------------------------------------------
# Athena execution helpers
# ---------------------------------------------------------------------------

def _run_athena_query(
    sql: str,
    database: str,
    output_s3: str,
    region: str = "us-west-2",
    timeout_sec: int = _ATHENA_TIMEOUT,
) -> List[Dict]:
    """
    Execute a SQL query on Athena, poll for completion, and return rows as list
    of {column_name: value} dicts.  Raises RuntimeError on failure/timeout.
    """
    client = boto3.client("athena", region_name=region)
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_s3},
    )
    qid = resp["QueryExecutionId"]

    elapsed = 0
    while elapsed < timeout_sec:
        status = client.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", state)
            raise RuntimeError(f"Athena query {qid} {state}: {reason}")
        time.sleep(_ATHENA_POLL_S)
        elapsed += _ATHENA_POLL_S
    else:
        client.stop_query_execution(QueryExecutionId=qid)
        raise TimeoutError(f"Athena query {qid} timed out after {timeout_sec}s")

    # Fetch results
    pages   = client.get_paginator("get_query_results").paginate(QueryExecutionId=qid)
    rows    = []
    headers = None
    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        for row in result_rows:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            if headers is None:
                headers = values
            else:
                rows.append(dict(zip(headers, values)))
    return rows


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val not in (None, "", "null", "NULL") else default
    except (TypeError, ValueError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(float(val)) if val not in (None, "", "null", "NULL") else default
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Individual query tools
# ---------------------------------------------------------------------------

@tool
def query_iceberg_files(
    database: str,
    table: str,
    athena_output_s3: str,
    region: str = "us-west-2",
) -> str:
    """
    Query the Iceberg $files system table to get file-level statistics.

    Retrieves: file count, total/avg/min/max size, tiny/small file counts,
    file size coefficient of variation (CV), partition count, and total records.

    Args:
        database:          Glue catalog database name.
        table:             Table name (without $files suffix).
        athena_output_s3:  S3 URI for Athena query results (e.g. s3://bucket/prefix/).
        region:            AWS region (default us-west-2).

    Returns:
        JSON with file_cnt, total_size_gb, avg_file_size_mb, tiny_file_cnt,
        small_file_cnt, file_size_cv, partition_count, total_records.
    """
    sql = f"""
SELECT
    COUNT(*)                                                              AS file_cnt,
    ROUND(SUM(file_size_in_bytes) / 1073741824.0, 3)                     AS total_size_gb,
    ROUND(SUM(CAST(record_count AS DOUBLE)), 0)                          AS total_records,
    ROUND(AVG(file_size_in_bytes) / 1048576.0, 2)                        AS avg_file_size_mb,
    ROUND(MIN(file_size_in_bytes) / 1024.0,    2)                        AS min_file_size_kb,
    ROUND(MAX(file_size_in_bytes) / 1048576.0, 2)                        AS max_file_size_mb,
    SUM(CASE WHEN file_size_in_bytes < {_TINY_FILE_MB  * 1048576} THEN 1 ELSE 0 END) AS tiny_file_cnt,
    SUM(CASE WHEN file_size_in_bytes < {_SMALL_FILE_MB * 1048576} THEN 1 ELSE 0 END) AS small_file_cnt,
    COUNT(DISTINCT partition)                                            AS partition_count,
    ROUND(
        STDDEV(CAST(file_size_in_bytes AS DOUBLE)) /
        NULLIF(AVG(CAST(file_size_in_bytes AS DOUBLE)), 0),
        3
    )                                                                    AS file_size_cv
FROM "{database}"."{table}$files"
""".strip()

    try:
        rows = _run_athena_query(sql, database, athena_output_s3, region)
        if not rows:
            return json.dumps({"error": "No rows returned from $files query"})
        row = rows[0]
        return json.dumps({
            "source":          "iceberg_$files",
            "database":        database,
            "table":           table,
            "file_cnt":           _safe_int(row.get("file_cnt")),
            "total_size_gb":      _safe_float(row.get("total_size_gb")),
            "total_records":      _safe_int(row.get("total_records")),
            "avg_file_size_mb":   _safe_float(row.get("avg_file_size_mb")),
            "min_file_size_kb":   _safe_float(row.get("min_file_size_kb")),
            "max_file_size_mb":   _safe_float(row.get("max_file_size_mb")),
            "tiny_file_cnt":      _safe_int(row.get("tiny_file_cnt")),
            "small_file_cnt":     _safe_int(row.get("small_file_cnt")),
            "partition_count":    _safe_int(row.get("partition_count")),
            "file_size_cv":       _safe_float(row.get("file_size_cv")),
        })
    except Exception as exc:
        logger.error("$files query failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc), "database": database, "table": table})


@tool
def query_iceberg_snapshots(
    database: str,
    table: str,
    athena_output_s3: str,
    region: str = "us-west-2",
) -> str:
    """
    Query the Iceberg $snapshots system table for snapshot health.

    Retrieves: snapshot count, oldest/newest timestamps, total added/deleted
    files and records across all snapshots (for write amplification calculation).

    Args:
        database:          Glue catalog database name.
        table:             Table name.
        athena_output_s3:  S3 URI for Athena query results.
        region:            AWS region.

    Returns:
        JSON with snapshot_count, oldest_snapshot_ts, newest_snapshot_ts,
        total_added_files, total_deleted_files, total_added_records,
        total_deleted_records, write_amplification.
    """
    sql = f"""
SELECT
    COUNT(*)                          AS snapshot_count,
    MIN(committed_at)                 AS oldest_snapshot_ts,
    MAX(committed_at)                 AS newest_snapshot_ts,
    SUM(added_files_count)            AS total_added_files,
    SUM(deleted_files_count)          AS total_deleted_files,
    SUM(added_records_count)          AS total_added_records,
    SUM(deleted_records_count)        AS total_deleted_records
FROM "{database}"."{table}$snapshots"
""".strip()

    try:
        rows = _run_athena_query(sql, database, athena_output_s3, region)
        if not rows:
            return json.dumps({"error": "No rows returned from $snapshots query"})
        row  = rows[0]
        added = _safe_int(row.get("total_added_files"))
        # write_amplification requires current file count — will be merged later
        return json.dumps({
            "source":                "iceberg_$snapshots",
            "snapshot_count":         _safe_int(row.get("snapshot_count")),
            "oldest_snapshot_ts":     row.get("oldest_snapshot_ts", ""),
            "newest_snapshot_ts":     row.get("newest_snapshot_ts", ""),
            "total_added_files":      added,
            "total_deleted_files":    _safe_int(row.get("total_deleted_files")),
            "total_added_records":    _safe_int(row.get("total_added_records")),
            "total_deleted_records":  _safe_int(row.get("total_deleted_records")),
        })
    except Exception as exc:
        logger.error("$snapshots query failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc), "database": database, "table": table})


@tool
def query_iceberg_partitions(
    database: str,
    table: str,
    athena_output_s3: str,
    region: str = "us-west-2",
) -> str:
    """
    Query the Iceberg $partitions system table for partition-skew statistics.

    Retrieves: partition count, max/min/avg records per partition, skew ratio
    (max/avg), and the maximum number of files in a single partition.

    Args:
        database:          Glue catalog database name.
        table:             Table name.
        athena_output_s3:  S3 URI for Athena query results.
        region:            AWS region.

    Returns:
        JSON with partition_count, max/min/avg_partition_records,
        skew_ratio, max_files_in_partition.
    """
    sql = f"""
SELECT
    COUNT(*)                                                              AS partition_count,
    MAX(record_count)                                                    AS max_partition_records,
    MIN(record_count)                                                    AS min_partition_records,
    ROUND(AVG(CAST(record_count AS DOUBLE)), 0)                          AS avg_partition_records,
    ROUND(
        MAX(record_count) /
        NULLIF(AVG(CAST(record_count AS DOUBLE)), 0),
        2
    )                                                                    AS skew_ratio,
    MAX(file_count)                                                      AS max_files_in_partition
FROM "{database}"."{table}$partitions"
""".strip()

    try:
        rows = _run_athena_query(sql, database, athena_output_s3, region)
        if not rows:
            return json.dumps({"error": "No rows returned from $partitions query"})
        row = rows[0]
        return json.dumps({
            "source":                  "iceberg_$partitions",
            "partition_count":          _safe_int(row.get("partition_count")),
            "max_partition_records":    _safe_int(row.get("max_partition_records")),
            "min_partition_records":    _safe_int(row.get("min_partition_records")),
            "avg_partition_records":    _safe_float(row.get("avg_partition_records")),
            "skew_ratio":               _safe_float(row.get("skew_ratio"), default=1.0),
            "max_files_in_partition":   _safe_int(row.get("max_files_in_partition")),
        })
    except Exception as exc:
        logger.error("$partitions query failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc), "database": database, "table": table})


@tool
def query_iceberg_manifests(
    database: str,
    table: str,
    athena_output_s3: str,
    region: str = "us-west-2",
) -> str:
    """
    Query the Iceberg $manifests system table for manifest health.

    Retrieves: manifest count, average files per manifest, and empty manifests.
    High manifest counts (> 1000) cause query-planning latency in Athena/Spark.

    Args:
        database:          Glue catalog database name.
        table:             Table name.
        athena_output_s3:  S3 URI for Athena query results.
        region:            AWS region.

    Returns:
        JSON with manifest_count, avg_files_per_manifest, empty_manifests.
    """
    sql = f"""
SELECT
    COUNT(*)                                                              AS manifest_count,
    ROUND(AVG(CAST(added_files_count + existing_files_count AS DOUBLE)), 2)
                                                                         AS avg_files_per_manifest,
    SUM(
        CASE WHEN added_files_count + existing_files_count = 0 THEN 1 ELSE 0 END
    )                                                                    AS empty_manifests
FROM "{database}"."{table}$manifests"
""".strip()

    try:
        rows = _run_athena_query(sql, database, athena_output_s3, region)
        if not rows:
            return json.dumps({"error": "No rows returned from $manifests query"})
        row = rows[0]
        return json.dumps({
            "source":                "iceberg_$manifests",
            "manifest_count":         _safe_int(row.get("manifest_count")),
            "avg_files_per_manifest": _safe_float(row.get("avg_files_per_manifest")),
            "empty_manifests":        _safe_int(row.get("empty_manifests")),
        })
    except Exception as exc:
        logger.error("$manifests query failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc), "database": database, "table": table})


_FATAL_ERROR_KEYWORDS = ("catalog", "not found", "does not exist", "table_not_found")


def _is_fatal_iceberg_error(error_msg: str) -> bool:
    """Return True if the error indicates the table/catalog does not exist."""
    lower = error_msg.lower()
    return any(kw in lower for kw in _FATAL_ERROR_KEYWORDS)


@tool
def query_iceberg_table_stats(
    database: str,
    table: str,
    athena_output_s3: str,
    region: str = "us-west-2",
) -> str:
    """
    Run all four Iceberg system table queries and return a merged iceberg_stats dict.

    This is the primary entry-point for gathering real Iceberg telemetry.
    Runs $files, $snapshots, $partitions, and $manifests queries in sequence
    and merges results into a single flat dict with a health assessment.

    Supports 'DB_prd.tablename' dot-notation in the database or table argument.

    Args:
        database:          Glue catalog database name, or 'DB_prd.tablename' combined.
        table:             Table name (e.g. "sales_events").
        athena_output_s3:  S3 URI for Athena query results (e.g. s3://bucket/athena/).
        region:            AWS region (default us-west-2).

    Returns:
        JSON iceberg_stats dict with all telemetry fields plus:
        - write_amplification:    total_added_files / current file_cnt
        - health_grade:           A / B / C / D / F
        - health_issues:          list of {severity, description, recommendation} dicts
        - growth_gb_per_day:      estimated daily growth from snapshot timestamps

    Raises:
        RuntimeError: if a fatal catalog/table-not-found error is encountered.
    """
    # Support DB_prd.tablename notation
    if not table and "." in database:
        database, table = _parse_table_ref(database)
    elif "." in table:
        database, table = _parse_table_ref(table)
    elif "." in database:
        database, table = _parse_table_ref(database)

    stats: Dict[str, Any] = {
        "database": database,
        "table":    table,
        "source":   "athena_iceberg_system_tables",
    }
    errors = []

    # ── $files ────────────────────────────────────────────────────────────────
    try:
        files_raw = json.loads(query_iceberg_files.__wrapped__(database, table, athena_output_s3, region))
        if "error" in files_raw:
            err_msg = files_raw["error"]
            if _is_fatal_iceberg_error(err_msg):
                raise RuntimeError(
                    f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                    f"Check database/table name and Glue catalog."
                )
            errors.append(f"$files: {err_msg}")
        else:
            stats.update({k: v for k, v in files_raw.items() if k != "source"})
    except RuntimeError:
        raise
    except Exception as e:
        err_msg = str(e)
        if _is_fatal_iceberg_error(err_msg):
            raise RuntimeError(
                f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                f"Check database/table name and Glue catalog."
            )
        errors.append(f"$files: {e}")

    # ── $snapshots ────────────────────────────────────────────────────────────
    try:
        snap_raw = json.loads(query_iceberg_snapshots.__wrapped__(database, table, athena_output_s3, region))
        if "error" in snap_raw:
            err_msg = snap_raw["error"]
            if _is_fatal_iceberg_error(err_msg):
                raise RuntimeError(
                    f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                    f"Check database/table name and Glue catalog."
                )
            errors.append(f"$snapshots: {err_msg}")
        else:
            stats.update({k: v for k, v in snap_raw.items() if k != "source"})
    except RuntimeError:
        raise
    except Exception as e:
        err_msg = str(e)
        if _is_fatal_iceberg_error(err_msg):
            raise RuntimeError(
                f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                f"Check database/table name and Glue catalog."
            )
        errors.append(f"$snapshots: {e}")

    # ── $partitions ───────────────────────────────────────────────────────────
    try:
        part_raw = json.loads(query_iceberg_partitions.__wrapped__(database, table, athena_output_s3, region))
        if "error" in part_raw:
            err_msg = part_raw["error"]
            if _is_fatal_iceberg_error(err_msg):
                raise RuntimeError(
                    f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                    f"Check database/table name and Glue catalog."
                )
            errors.append(f"$partitions: {err_msg}")
        else:
            stats.update({k: v for k, v in part_raw.items() if k != "source"})
    except RuntimeError:
        raise
    except Exception as e:
        err_msg = str(e)
        if _is_fatal_iceberg_error(err_msg):
            raise RuntimeError(
                f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                f"Check database/table name and Glue catalog."
            )
        errors.append(f"$partitions: {e}")

    # ── $manifests ────────────────────────────────────────────────────────────
    try:
        mani_raw = json.loads(query_iceberg_manifests.__wrapped__(database, table, athena_output_s3, region))
        if "error" in mani_raw:
            err_msg = mani_raw["error"]
            if _is_fatal_iceberg_error(err_msg):
                raise RuntimeError(
                    f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                    f"Check database/table name and Glue catalog."
                )
            errors.append(f"$manifests: {err_msg}")
        else:
            stats.update({k: v for k, v in mani_raw.items() if k != "source"})
    except RuntimeError:
        raise
    except Exception as e:
        err_msg = str(e)
        if _is_fatal_iceberg_error(err_msg):
            raise RuntimeError(
                f"Iceberg telemetry FATAL: {database}.{table} — {err_msg}. "
                f"Check database/table name and Glue catalog."
            )
        errors.append(f"$manifests: {e}")

    # ── Derived metrics ────────────────────────────────────────────────────────
    file_cnt   = stats.get("file_cnt", 0)
    added      = stats.get("total_added_files", 0)
    stats["write_amplification"] = round(added / max(file_cnt, 1), 2)

    # Growth rate from snapshot timestamps
    oldest = stats.get("oldest_snapshot_ts", "")
    newest = stats.get("newest_snapshot_ts", "")
    total_gb = stats.get("total_size_gb", 0.0)
    if oldest and newest and total_gb > 0:
        try:
            from datetime import datetime
            fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in oldest else "%Y-%m-%d %H:%M:%S"
            t0  = datetime.strptime(oldest[:26], fmt)
            t1  = datetime.strptime(newest[:26], fmt)
            days = max((t1 - t0).total_seconds() / 86400, 1)
            stats["growth_gb_per_day"]    = round(total_gb / days, 4)
            stats["days_of_snapshot_history"] = round(days, 1)
        except Exception:
            stats["growth_gb_per_day"] = 0.0

    # ── Health assessment ──────────────────────────────────────────────────────
    issues = []
    sc     = stats.get("snapshot_count", 0)
    tiny   = stats.get("tiny_file_cnt", 0)
    mc     = stats.get("manifest_count", 0)
    wa     = stats.get("write_amplification", 1.0)
    skew   = stats.get("skew_ratio", 1.0)
    avg_mb = stats.get("avg_file_size_mb", 0.0)

    if tiny > 10_000:
        issues.append({"severity": "P0", "description": f"{tiny:,} tiny files (< 10 MB) — read overhead critical",
                        "recommendation": "Run OPTIMIZE table REWRITE DATA USING bin-pack in Athena immediately"})
    elif tiny > 1_000:
        issues.append({"severity": "P1", "description": f"{tiny:,} tiny files detected",
                        "recommendation": "Schedule weekly OPTIMIZE REWRITE_DATA_FILES"})
    if sc > 100:
        issues.append({"severity": "P0", "description": f"{sc} snapshots — query-planning latency critical",
                        "recommendation": "expire_snapshots(table, older_than=timedelta(days=7))"})
    elif sc > 30:
        issues.append({"severity": "P1", "description": f"{sc} snapshots exceed 30 recommended",
                        "recommendation": "Schedule weekly snapshot expiry"})
    if mc > 10_000:
        issues.append({"severity": "P0", "description": f"{mc:,} manifests — queries will time out",
                        "recommendation": "rewrite_manifests(table) immediately"})
    elif mc > 1_000:
        issues.append({"severity": "P1", "description": f"{mc:,} manifests > 1 000",
                        "recommendation": "Schedule weekly rewrite_manifests"})
    if wa > 10:
        issues.append({"severity": "P0", "description": f"Write amplification {wa:.1f}× — no compaction has run",
                        "recommendation": "Enable auto-compaction; run REWRITE_DATA_FILES immediately"})
    elif wa > 5:
        issues.append({"severity": "P1", "description": f"Write amplification {wa:.1f}× exceeds 5×",
                        "recommendation": "Enable auto-compaction or schedule weekly REWRITE_DATA_FILES"})
    if skew > 10:
        issues.append({"severity": "P0", "description": f"Partition skew {skew:.1f}× — stragglers guaranteed",
                        "recommendation": "Salt the join key or enable AQE skewJoin; check partition column"})
    elif skew > 3:
        issues.append({"severity": "P1", "description": f"Partition skew {skew:.1f}× — enable AQE skewJoin",
                        "recommendation": "spark.sql.adaptive.skewJoin.enabled=true"})
    if avg_mb > 1024:
        issues.append({"severity": "P2", "description": f"Avg file size {avg_mb:.0f} MB is oversized",
                        "recommendation": "Set write.target-file-size-bytes=268435456 (256 MB)"})

    penalty   = sum({"P0": 30, "P1": 15, "P2": 5}.get(i["severity"], 2) for i in issues)
    raw_score = max(0, 100 - penalty)
    grade = "A" if raw_score >= 90 else "B" if raw_score >= 75 else "C" if raw_score >= 55 else "D" if raw_score >= 35 else "F"

    stats["health_grade"]  = grade
    stats["health_score"]  = raw_score
    stats["health_issues"] = issues
    if errors:
        stats["query_errors"] = errors

    return json.dumps(stats)


@tool
def query_iceberg_partition_details(
    database: str,
    table: str,
    athena_output_s3: str,
    top_n: int = 20,
    region: str = "us-west-2",
) -> str:
    """
    Query the top N heaviest partitions to identify which specific partition
    values are causing skew. Use this when skew_ratio > 3.

    Args:
        database:          Glue catalog database name.
        table:             Table name.
        athena_output_s3:  S3 URI for Athena query results.
        top_n:             Number of top partitions to return (default 20).
        region:            AWS region.

    Returns:
        JSON with top_heavy_partitions list, skew_partition_values, and
        recommended_salt_factor.
    """
    sql = f"""
SELECT
    partition,
    record_count,
    file_count,
    ROUND(CAST(record_count AS DOUBLE) /
          NULLIF(AVG(CAST(record_count AS DOUBLE)) OVER (), 0), 2)  AS relative_size
FROM "{database}"."{table}$partitions"
ORDER BY record_count DESC
LIMIT {top_n}
""".strip()

    try:
        rows = _run_athena_query(sql, database, athena_output_s3, region)
        if not rows:
            return json.dumps({"error": "No partition rows returned"})

        partitions = [
            {
                "partition":     r.get("partition", ""),
                "record_count":  _safe_int(r.get("record_count")),
                "file_count":    _safe_int(r.get("file_count")),
                "relative_size": _safe_float(r.get("relative_size")),
            }
            for r in rows
        ]

        heavy = [p for p in partitions if p["relative_size"] > 3.0]
        max_rs = max((p["relative_size"] for p in partitions), default=1.0)

        import math
        salt = max(2, min(50, round(max_rs ** 0.5)))

        return json.dumps({
            "top_heavy_partitions":   partitions,
            "skewed_partition_count": len(heavy),
            "recommended_salt_factor": salt,
            "salt_rationale":          f"salt_factor = ceil(√max_skew_ratio) = ceil(√{max_rs:.1f}) = {salt}",
            "spark_salting_snippet": (
                f"num_salt = {salt}\n"
                f"df = df.withColumn('salt', (rand() * num_salt).cast('int'))\n"
                f"df = df.withColumn('salted_key', concat(col('join_key'), lit('_'), col('salt')))"
            ),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_iceberg_telemetry_agent(
    model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
    region: str = "us-west-2",
) -> Agent:
    """Return a Strands Agent that queries Iceberg system tables via Athena."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=_SYSTEM_PROMPT,
        tools=[
            query_iceberg_files,
            query_iceberg_snapshots,
            query_iceberg_partitions,
            query_iceberg_manifests,
            query_iceberg_table_stats,
            query_iceberg_partition_details,
        ],
    )
