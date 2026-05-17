"""
Sizing Agent
============
Estimates data volumes, processing requirements, skew risk, and partition efficiency
for source tables. Feeds results to the Resource Allocator.
"""

import json
import logging
import re
from typing import Any, Dict

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """
You are a **Senior Data Platform Architect** with 15+ years experience in big-data systems.
Specialties: Parquet / ORC / Delta Lake, partition strategies, capacity planning for PySpark.

Your job is to analyse source table metadata and produce a structured sizing report:
- total_raw_size_gb          – estimated uncompressed size
- total_compressed_size_gb   – compressed on-disk size
- effective_size_gb          – size that will be *processed* (delta vs full, joins)
- processing_mode            – "delta" | "full"
- delta_ratio                – fraction processed for delta (0.01–1.0)
- join_amplification_factor  – estimated data expansion from joins (≥ 1.0)
- skew_risk_score            – 0–100 (0 = no skew)
- skew_risk_factors[]        – list of skew risks found
- partition_efficiency_score – 0–100
- partition_recommendations[]
- tables_detail[]            – per-table breakdown
- size_confidence            – "high" | "medium" | "low"

Row-size heuristics (compressed):
  narrow  (< 20 cols) → 200 B/row
  medium  (20–50)     → 500 B/row
  wide    (50–100)    → 1 000 B/row
  very_wide (100+)    → 2 000 B/row
Parquet/ORC/Delta compression ratio ≈ 0.25; JSON/CSV ≈ 0.6–0.7.

Delta processing: typically 1–10 % of full table per run.
Always be conservative (slightly over-estimate) to prevent job failures.

Return ONLY valid JSON — no prose outside the JSON object.
"""

# ---------------------------------------------------------------------------
# Bytes-per-row and compression constants (mirrors PR-11 SizeAnalyzerAgent)
# ---------------------------------------------------------------------------
_BYTES_PER_ROW = {"narrow": 200, "medium": 500, "wide": 1_000, "very_wide": 2_000}
_COMPRESSION   = {"parquet": 0.25, "orc": 0.25, "delta": 0.25, "avro": 0.5, "json": 0.7, "csv": 0.6}


def _bytes_to_gb(b: float) -> float:
    return round(b / (1024 ** 3), 3)


def _analyse_table(table: Dict) -> Dict:
    """Rule-based single-table sizing."""
    name     = table.get("table", table.get("name", "unknown"))
    records  = int(table.get("record_count", table.get("records", 0)))
    columns  = int(table.get("column_count", table.get("columns", 30)))
    fmt      = table.get("format", "parquet").lower()

    width = ("narrow" if columns < 20 else
             "medium" if columns < 50 else
             "wide"   if columns < 100 else "very_wide")

    bpr            = _BYTES_PER_ROW[width]
    raw_bytes      = records * bpr
    compression    = _COMPRESSION.get(fmt, 0.3)
    comp_bytes     = raw_bytes * compression

    if "size_gb" in table:
        comp_bytes = table["size_gb"] * (1024 ** 3)
        raw_bytes  = comp_bytes / compression

    # Skew assessment
    skew_risk = "low"
    skew_reason = ""
    join_key = table.get("join_key", "")
    if join_key:
        low_card = ["status", "type", "flag", "category", "region", "country"]
        if any(kw in join_key.lower() for kw in low_card):
            skew_risk, skew_reason = "high", f"Low-cardinality join key '{join_key}'"
        elif records > 1_000_000_000:
            skew_risk, skew_reason = "medium", "Very large table — power-law distribution likely"

    return {
        "table": name,
        "database": table.get("database", ""),
        "record_count": records,
        "column_count": columns,
        "format": fmt,
        "width_category": width,
        "raw_size_bytes": raw_bytes,
        "compressed_size_bytes": comp_bytes,
        "raw_size_gb": _bytes_to_gb(raw_bytes),
        "compressed_size_gb": _bytes_to_gb(comp_bytes),
        "skew_risk": skew_risk,
        "skew_reason": skew_reason,
    }


def _rule_based_sizing(tables: list, processing_mode: str, joins: int) -> Dict:
    """Full rule-based sizing analysis."""
    table_details     = [_analyse_table(t) for t in tables]
    total_raw_bytes   = sum(t["raw_size_bytes"]        for t in table_details)
    total_comp_bytes  = sum(t["compressed_size_bytes"] for t in table_details)

    delta_ratio = 0.05 if processing_mode == "delta" else 1.0
    effective_bytes = total_comp_bytes * delta_ratio

    join_factor = max(1.0, 1.0 + (joins - 1) * 0.3) if joins > 1 else 1.0
    final_bytes = effective_bytes * join_factor

    skew_risks = [
        {"table": t["table"], "risk": t["skew_risk"], "reason": t["skew_reason"]}
        for t in table_details if t["skew_risk"] != "low"
    ]
    skew_score = min(100, len(skew_risks) * 35)

    # Partition efficiency: penalise tables without partition columns
    partitioned = sum(1 for t in tables if t.get("partition_column"))
    partition_eff = int((partitioned / max(len(tables), 1)) * 100)
    partition_recs = []
    if partition_eff < 80:
        partition_recs.append("Add partition columns (e.g. date, region) to unpartitioned tables")
    if any(t.get("record_count", 0) > 500_000_000 for t in tables):
        partition_recs.append("Consider Z-order / liquid clustering for tables > 500M rows")

    confidence = "high" if all("size_gb" in t or "record_count" in t for t in tables) else "medium"

    return {
        "total_raw_size_gb":          _bytes_to_gb(total_raw_bytes),
        "total_compressed_size_gb":   _bytes_to_gb(total_comp_bytes),
        "effective_size_gb":          _bytes_to_gb(final_bytes),
        "processing_mode":            processing_mode,
        "delta_ratio":                delta_ratio,
        "join_amplification_factor":  round(join_factor, 2),
        "skew_risk_score":            skew_score,
        "skew_risk_factors":          skew_risks,
        "partition_efficiency_score": partition_eff,
        "partition_recommendations":  partition_recs,
        "tables_analyzed":            len(table_details),
        "tables_detail":              table_details,
        "size_confidence":            confidence,
    }


# ---------------------------------------------------------------------------
# Strands @tool
# ---------------------------------------------------------------------------
@tool
def analyse_data_sizing(
    tables_json: str,
    processing_mode: str = "full",
    join_count: int = 1,
) -> str:
    """
    Perform rule-based data-size analysis for ETL source tables.

    Args:
        tables_json:     JSON array of table descriptors (name, record_count, columns, format, …).
        processing_mode: "delta" for incremental; "full" for complete refresh.
        join_count:      Number of join operations in the pipeline.

    Returns:
        JSON string with sizing analysis including effective_size_gb, skew_risk_score, etc.
    """
    try:
        tables = json.loads(tables_json)
        result = _rule_based_sizing(tables, processing_mode, join_count)
        return json.dumps(result)
    except Exception as exc:
        logger.error("Sizing analysis failed: %s", exc)
        return json.dumps({"error": str(exc), "effective_size_gb": 100, "skew_risk_score": 20})


@tool
def estimate_shuffle_size(
    tables_json: str,
    join_keys_json: str = "[]",
    processing_mode: str = "full",
) -> str:
    """
    Estimate shuffle data volume and recommend spark.sql.shuffle.partitions.

    Args:
        tables_json:     JSON array of table descriptors.
        join_keys_json:  JSON list of join key column names (for cardinality hints).
        processing_mode: "full" or "delta".

    Returns:
        JSON with estimated_shuffle_gb, recommended_shuffle_partitions, skew_warning.
    """
    try:
        tables     = json.loads(tables_json)
        join_keys  = json.loads(join_keys_json) if join_keys_json else []
        sizing     = _rule_based_sizing(tables, processing_mode, len(join_keys) or 1)
        eff_gb     = sizing["effective_size_gb"]

        # Shuffle ≈ 2× effective for sort-merge joins; broadcast saves it
        shuffle_gb = round(eff_gb * 2.0, 2)
        # Target ~128 MB per partition
        target_mb  = 128
        partitions = max(200, min(2000, int((shuffle_gb * 1024) / target_mb)))

        skew_warning = None
        low_card = ["status", "type", "flag", "category", "region"]
        for key in join_keys:
            if any(kw in key.lower() for kw in low_card):
                skew_warning = f"Join key '{key}' appears low-cardinality — consider AQE skewJoin"
                break

        return json.dumps({
            "estimated_shuffle_gb":             shuffle_gb,
            "recommended_shuffle_partitions":   partitions,
            "target_partition_size_mb":         target_mb,
            "skew_warning":                     skew_warning,
            "aqe_recommendation":               "Enable spark.sql.adaptive.coalescePartitions to auto-tune",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def analyse_partition_efficiency(tables_json: str) -> str:
    """
    Evaluate partition strategy of each source table and recommend improvements.

    Args:
        tables_json: JSON array of table descriptors with optional partition_column,
                     record_count, and format fields.

    Returns:
        JSON with per-table partition scores, issues, and recommended actions.
    """
    try:
        tables = json.loads(tables_json)
        results = []
        for t in tables:
            name    = t.get("table", t.get("name", "unknown"))
            records = int(t.get("record_count", t.get("records", 0)))
            part    = t.get("partition_column", "")
            issues, recs = [], []

            if not part:
                issues.append("No partition column defined")
                recs.append("Add a date/region partition column for pruning")
                score = 20
            else:
                low_card = ["status", "flag", "type", "active", "boolean"]
                if any(kw in part.lower() for kw in low_card):
                    issues.append(f"Partition column '{part}' has low cardinality")
                    recs.append("Use a higher-cardinality column (e.g. event_date, customer_id range)")
                    score = 50
                else:
                    score = 90

            if records > 1_000_000_000 and score >= 80:
                recs.append("Consider Z-order or liquid clustering for 1B+ row tables")
                score = min(score, 80)

            results.append({
                "table": name, "record_count": records,
                "partition_column": part, "score": score,
                "issues": issues, "recommendations": recs,
            })

        avg_score = int(sum(r["score"] for r in results) / max(len(results), 1))
        return json.dumps({
            "tables": results,
            "average_partition_score": avg_score,
            "overall_rating": ("excellent" if avg_score >= 80 else
                               "good"      if avg_score >= 60 else
                               "needs_improvement"),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ---------------------------------------------------------------------------
# Scientific sizing tools
# ---------------------------------------------------------------------------

def _gaussian_bimodality_coefficient(values: list) -> float:
    """Sarle's bimodality coefficient — values > 0.555 suggest bimodal distribution."""
    import math
    n = len(values)
    if n < 4:
        return 0.0
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / max(n - 1, 1)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    skewness = sum((x - mean) ** 3 for x in values) / (n * std ** 3)
    kurtosis = sum((x - mean) ** 4 for x in values) / (n * std ** 4) - 3
    return (skewness ** 2 + 1) / (kurtosis + 3 * (n - 1) ** 2 / max((n - 2) * (n - 3), 1))


@tool
def analyse_file_size_distribution(file_sizes_mb_json: str) -> str:
    """
    Analyse the distribution of Iceberg/Delta table file sizes using Gaussian
    bimodality detection to identify tiny-file pathologies.

    Args:
        file_sizes_mb_json: JSON array of file sizes in MB (e.g. from $files system table).

    Returns:
        JSON with bimodality_coefficient, tiny_file_count, large_file_count,
        p50_mb, p95_mb, pathology, and compaction_recommendation.
    """
    try:
        sizes = json.loads(file_sizes_mb_json)
        if not sizes:
            return json.dumps({"error": "empty file list"})

        sizes_sorted = sorted(float(s) for s in sizes)
        n = len(sizes_sorted)
        p50 = sizes_sorted[int(n * 0.50)]
        p95 = sizes_sorted[int(n * 0.95)]

        tiny = sum(1 for s in sizes_sorted if s < 10)
        large = sum(1 for s in sizes_sorted if s > 512)
        bmc = _gaussian_bimodality_coefficient(sizes_sorted)

        if bmc > 0.555 and tiny > n * 0.3:
            pathology = "bimodal_tiny_file"
            rec = "Run OPTIMIZE / REWRITE_DATA_FILES to compact small files into 128–256 MB targets"
        elif tiny > n * 0.5:
            pathology = "tiny_file_dominated"
            rec = "Enable auto-compaction or schedule periodic OPTIMIZE jobs"
        elif large > n * 0.3:
            pathology = "oversized_files"
            rec = "Reduce write batch size or enable file size control in writer options"
        else:
            pathology = "healthy"
            rec = "File size distribution is within acceptable range"

        return json.dumps({
            "file_count":              n,
            "bimodality_coefficient":  round(bmc, 4),
            "bimodal_threshold":       0.555,
            "is_bimodal":              bmc > 0.555,
            "tiny_file_count":         tiny,
            "tiny_file_pct":           round(100 * tiny / max(n, 1), 1),
            "large_file_count":        large,
            "p50_mb":                  round(p50, 2),
            "p95_mb":                  round(p95, 2),
            "pathology":               pathology,
            "compaction_recommendation": rec,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def forecast_table_growth(tables_json: str, forecast_days: int = 90) -> str:
    """
    Project table size and storage cost growth using Euler exponential growth model
    (compound continuous growth: S(t) = S₀ · e^(r·t)).

    Args:
        tables_json:   JSON array of {table, size_gb, daily_growth_rate_pct} objects.
        forecast_days: Number of days to project forward (default 90).

    Returns:
        JSON with per-table projections and total_projected_gb, total_cost_usd.
    """
    import math
    STORAGE_COST_PER_GB_MONTH = 0.023  # S3 standard

    try:
        tables = json.loads(tables_json)
        projections = []
        for t in tables:
            name   = t.get("table", t.get("name", "unknown"))
            s0     = float(t.get("size_gb", 0))
            rate   = float(t.get("daily_growth_rate_pct", t.get("growth_rate", 1.0))) / 100.0
            st     = s0 * math.exp(rate * forecast_days)
            growth = st - s0
            months = forecast_days / 30.0
            cost   = st * STORAGE_COST_PER_GB_MONTH * months
            projections.append({
                "table":                name,
                "current_size_gb":      round(s0, 2),
                "projected_size_gb":    round(st, 2),
                "growth_gb":            round(growth, 2),
                "growth_pct":           round((st / max(s0, 0.001) - 1) * 100, 1),
                "storage_cost_usd":     round(cost, 2),
                "daily_growth_rate_pct": t.get("daily_growth_rate_pct", 1.0),
            })

        total_current  = sum(p["current_size_gb"]   for p in projections)
        total_proj     = sum(p["projected_size_gb"]  for p in projections)
        total_cost     = sum(p["storage_cost_usd"]   for p in projections)

        high_growth = [p for p in projections if p["growth_pct"] > 100]
        rec = (f"{len(high_growth)} table(s) will double in {forecast_days} days — consider lifecycle policies"
               if high_growth else "Growth rates are within normal bounds")

        return json.dumps({
            "forecast_days":        forecast_days,
            "projections":          projections,
            "total_current_gb":     round(total_current, 2),
            "total_projected_gb":   round(total_proj, 2),
            "total_storage_cost_usd": round(total_cost, 2),
            "high_growth_tables":   [p["table"] for p in high_growth],
            "recommendation":       rec,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def assess_iceberg_snapshot_health(
    snapshot_count: int,
    file_count: int,
    added_file_count: int,
    table_name: str = "unknown",
) -> str:
    """
    Assess Iceberg table snapshot health: detect snapshot bloat, write amplification,
    and orphan file accumulation. Recommends VACUUM / expire_snapshots timing.

    Args:
        snapshot_count:    Total number of snapshots (from $snapshots system table).
        file_count:        Total live files (from $files system table).
        added_file_count:  Total files added across all snapshots (from $manifests).
        table_name:        Table identifier for labelling.

    Returns:
        JSON with bloat_score, write_amplification, health_grade, and recommendations.
    """
    try:
        write_amp = round(added_file_count / max(file_count, 1), 2)

        # Bloat score 0–100
        snap_penalty  = min(50, max(0, (snapshot_count - 10) * 2))
        amp_penalty   = min(50, max(0, (write_amp - 1.5) * 10))
        bloat_score   = int(snap_penalty + amp_penalty)

        recs = []
        if snapshot_count > 30:
            recs.append(f"expire_snapshots(older_than=7d) — {snapshot_count} snapshots is excessive (target ≤ 30)")
        if write_amp > 5:
            recs.append(f"Write amplification {write_amp}× exceeds 5× threshold; switch to merge-on-read or increase write batch size")
        if write_amp > 2 and snapshot_count > 10:
            recs.append("Schedule REWRITE_DATA_FILES weekly to compact overlapping data files")
        if bloat_score < 20:
            recs.append("Snapshot health is good; maintain current VACUUM cadence")

        health_grade = ("A" if bloat_score < 20 else
                        "B" if bloat_score < 40 else
                        "C" if bloat_score < 60 else
                        "D" if bloat_score < 80 else "F")

        return json.dumps({
            "table":               table_name,
            "snapshot_count":      snapshot_count,
            "live_file_count":     file_count,
            "added_file_count":    added_file_count,
            "write_amplification": write_amp,
            "bloat_score":         bloat_score,
            "health_grade":        health_grade,
            "requires_vacuum":     snapshot_count > 30 or write_amp > 5,
            "recommendations":     recs,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ---------------------------------------------------------------------------
# Athena-driven Iceberg sizing (KEYJa pattern)
# ---------------------------------------------------------------------------

def _run_athena_query(client, query: str, output_s3: str, db: str = "default") -> list:
    """Execute Athena query and return rows as list of dicts."""
    import time
    resp = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": db},
        ResultConfiguration={"OutputLocation": output_s3},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(60):
        status = client.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {state}: {status['QueryExecution']['Status'].get('StateChangeReason', '')}")
        time.sleep(2)
    else:
        raise TimeoutError("Athena query timed out after 120s")

    paginator = client.get_paginator("get_query_results")
    rows, headers = [], []
    for page in paginator.paginate(QueryExecutionId=qid):
        result_rows = page["ResultSet"]["Rows"]
        if not headers:
            headers = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append({headers[i]: c.get("VarCharValue", "") for i, c in enumerate(row["Data"])})
    return rows


def _safe_float(val, default=0.0):
    try:
        return float(val) if val not in (None, "", "null") else default
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=0):
    try:
        return int(float(val)) if val not in (None, "", "null") else default
    except (TypeError, ValueError):
        return default


def _iceberg_stats_from_athena(client, database: str, table: str, output_s3: str) -> dict:
    """Query all 4 Iceberg system tables and merge into iceberg_stats dict."""
    db_table = f'"{database}"."{table}"'
    stats: dict = {"database": database, "table": table, "source": "athena_iceberg_system_tables"}

    # $files
    try:
        rows = _run_athena_query(client, f"""
            SELECT COUNT(*) AS file_cnt,
              ROUND(SUM(file_size_in_bytes)/1073741824.0,3) AS total_size_gb,
              ROUND(SUM(CAST(record_count AS DOUBLE)),0) AS total_records,
              ROUND(AVG(file_size_in_bytes)/1048576.0,2) AS avg_file_size_mb,
              ROUND(MIN(file_size_in_bytes)/1024.0,2) AS min_file_size_kb,
              ROUND(MAX(file_size_in_bytes)/1048576.0,2) AS max_file_size_mb,
              SUM(CASE WHEN file_size_in_bytes < 10485760 THEN 1 ELSE 0 END) AS tiny_file_cnt,
              SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_file_cnt,
              COUNT(DISTINCT partition) AS partition_count,
              ROUND(STDDEV(CAST(file_size_in_bytes AS DOUBLE))/
                NULLIF(AVG(CAST(file_size_in_bytes AS DOUBLE)),0),3) AS file_size_cv
            FROM {db_table}."$files"
        """, output_s3, database)
        if rows:
            r = rows[0]
            stats.update({
                "file_cnt": _safe_int(r.get("file_cnt")),
                "total_size_gb": _safe_float(r.get("total_size_gb")),
                "total_records": _safe_int(r.get("total_records")),
                "avg_file_size_mb": _safe_float(r.get("avg_file_size_mb")),
                "min_file_size_kb": _safe_float(r.get("min_file_size_kb")),
                "max_file_size_mb": _safe_float(r.get("max_file_size_mb")),
                "tiny_file_cnt": _safe_int(r.get("tiny_file_cnt")),
                "small_file_cnt": _safe_int(r.get("small_file_cnt")),
                "partition_count": _safe_int(r.get("partition_count")),
                "file_size_cv": _safe_float(r.get("file_size_cv")),
            })
    except Exception as e:
        stats["files_error"] = str(e)

    # $snapshots
    try:
        rows = _run_athena_query(client, f"""
            SELECT COUNT(*) AS snapshot_count,
              MIN(committed_at) AS oldest_snapshot_ts,
              MAX(committed_at) AS newest_snapshot_ts,
              SUM(added_files_count) AS total_added_files,
              SUM(deleted_files_count) AS total_deleted_files,
              SUM(added_records_count) AS total_added_records,
              SUM(deleted_records_count) AS total_deleted_records
            FROM {db_table}."$snapshots"
        """, output_s3, database)
        if rows:
            r = rows[0]
            stats.update({
                "snapshot_count": _safe_int(r.get("snapshot_count")),
                "oldest_snapshot_ts": r.get("oldest_snapshot_ts", ""),
                "newest_snapshot_ts": r.get("newest_snapshot_ts", ""),
                "total_added_files": _safe_int(r.get("total_added_files")),
                "total_deleted_files": _safe_int(r.get("total_deleted_files")),
                "total_added_records": _safe_int(r.get("total_added_records")),
                "total_deleted_records": _safe_int(r.get("total_deleted_records")),
            })
    except Exception as e:
        stats["snapshots_error"] = str(e)

    # $partitions
    try:
        rows = _run_athena_query(client, f"""
            SELECT COUNT(*) AS partition_count,
              MAX(record_count) AS max_partition_records,
              MIN(record_count) AS min_partition_records,
              ROUND(AVG(CAST(record_count AS DOUBLE)),0) AS avg_partition_records,
              ROUND(MAX(record_count)/NULLIF(AVG(CAST(record_count AS DOUBLE)),0),2) AS skew_ratio,
              MAX(file_count) AS max_files_in_partition
            FROM {db_table}."$partitions"
        """, output_s3, database)
        if rows:
            r = rows[0]
            stats.update({
                "max_partition_records": _safe_int(r.get("max_partition_records")),
                "min_partition_records": _safe_int(r.get("min_partition_records")),
                "avg_partition_records": _safe_float(r.get("avg_partition_records")),
                "skew_ratio": _safe_float(r.get("skew_ratio")),
                "max_files_in_partition": _safe_int(r.get("max_files_in_partition")),
            })
    except Exception as e:
        stats["partitions_error"] = str(e)

    # $manifests
    try:
        rows = _run_athena_query(client, f"""
            SELECT COUNT(*) AS manifest_count,
              ROUND(AVG(CAST(added_files_count + existing_files_count AS DOUBLE)),2) AS avg_files_per_manifest,
              SUM(CASE WHEN added_files_count + existing_files_count = 0 THEN 1 ELSE 0 END) AS empty_manifests
            FROM {db_table}."$manifests"
        """, output_s3, database)
        if rows:
            r = rows[0]
            stats.update({
                "manifest_count": _safe_int(r.get("manifest_count")),
                "avg_files_per_manifest": _safe_float(r.get("avg_files_per_manifest")),
                "empty_manifests": _safe_int(r.get("empty_manifests")),
            })
    except Exception as e:
        stats["manifests_error"] = str(e)

    # Derived metrics
    file_cnt = stats.get("file_cnt", 1)
    stats["write_amplification"] = round(stats.get("total_added_files", 0) / max(file_cnt, 1), 2)

    try:
        from datetime import datetime
        fmt = "%Y-%m-%d %H:%M:%S.%f"
        oldest = stats.get("oldest_snapshot_ts", "")
        newest = stats.get("newest_snapshot_ts", "")
        if oldest and newest:
            d0 = datetime.strptime(oldest[:26], fmt[:len(fmt)])
            d1 = datetime.strptime(newest[:26], fmt[:len(fmt)])
            days = max((d1 - d0).total_seconds() / 86400, 0.01)
            size_gb = stats.get("total_size_gb", 0)
            stats["growth_gb_per_day"] = round(size_gb / days, 4)
            stats["days_of_snapshot_history"] = round(days, 1)
        else:
            stats["growth_gb_per_day"] = 0.0
            stats["days_of_snapshot_history"] = 0.0
    except Exception:
        stats["growth_gb_per_day"] = 0.0
        stats["days_of_snapshot_history"] = 0.0

    # Health grading
    score = 100
    issues = []
    tiny_cnt = stats.get("tiny_file_cnt", 0)
    snap_cnt = stats.get("snapshot_count", 0)
    man_cnt  = stats.get("manifest_count", 0)
    w_amp    = stats.get("write_amplification", 0)
    skew     = stats.get("skew_ratio", 0)
    cv       = stats.get("file_size_cv", 0)

    if tiny_cnt > 10000:
        score -= 30; issues.append({"severity": "P0", "description": f"{tiny_cnt:,} tiny files (<10MB) — task scheduling overhead", "recommendation": "Run REWRITE_DATA_FILES with target-file-size-bytes=134217728"})
    elif tiny_cnt > 1000:
        score -= 15; issues.append({"severity": "P1", "description": f"{tiny_cnt:,} tiny files — compaction recommended", "recommendation": "Schedule OPTIMIZE weekly"})
    if snap_cnt > 100:
        score -= 30; issues.append({"severity": "P0", "description": f"{snap_cnt} snapshots — history table bloat", "recommendation": "expire_snapshots(older_than=timedelta(days=7))"})
    elif snap_cnt > 30:
        score -= 15; issues.append({"severity": "P1", "description": f"{snap_cnt} snapshots exceeds recommended 30", "recommendation": "expire_snapshots monthly"})
    if man_cnt > 10000:
        score -= 30; issues.append({"severity": "P0", "description": f"{man_cnt:,} manifests — metadata I/O bottleneck", "recommendation": "rewrite_manifests() to consolidate"})
    if w_amp > 10:
        score -= 30; issues.append({"severity": "P0", "description": f"Write amplification {w_amp}× — excessive rewrites", "recommendation": "Switch to merge-on-read; increase write batch"})
    elif w_amp > 5:
        score -= 15; issues.append({"severity": "P1", "description": f"Write amplification {w_amp}×", "recommendation": "Enable copy-on-write mode"})
    if skew > 10:
        score -= 30; issues.append({"severity": "P0", "description": f"Partition skew ratio {skew}× — straggler tasks expected", "recommendation": "Salt join key; use AQE skewJoin"})
    elif skew > 3:
        score -= 15; issues.append({"severity": "P1", "description": f"Partition skew {skew}×", "recommendation": "Review partition column cardinality"})
    if cv > 1.5:
        score -= 5; issues.append({"severity": "P2", "description": f"File size CV={cv} — uneven file sizes may cause OOM spikes", "recommendation": "Compact with uniform target size"})

    score = max(0, score)
    stats["health_score"] = score
    stats["health_grade"] = ("A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 40 else "F")
    stats["health_issues"] = issues
    return stats


@tool
def analyse_data_sizing_with_athena(
    tables_with_database_json: str,
    athena_output_s3: str,
    script_content: str = "",
    region: str = "us-west-2",
) -> str:
    """
    Athena-driven sizing analysis: queries Iceberg system tables ($files, $snapshots,
    $partitions, $manifests) for ground-truth telemetry, then applies 15-factor
    analysis (file distribution, snapshot bloat, write amplification, skew, growth,
    compression, cold partitions) per table.

    Args:
        tables_with_database_json: JSON list of {database, table} objects.
        athena_output_s3:          S3 URI for Athena query results (e.g. s3://bucket/athena/).
        script_content:            PySpark script content for context (optional).
        region:                    AWS region for Athena client.

    Returns:
        JSON with per-table iceberg_stats, aggregate sizing, health grades, and recommendations.
    """
    import math

    try:
        tables  = json.loads(tables_with_database_json)
        client  = boto3.client("athena", region_name=region)
        results = []

        for entry in tables:
            db    = entry.get("database", "default")
            tbl   = entry.get("table", entry.get("name", "unknown"))
            stats = _iceberg_stats_from_athena(client, db, tbl, athena_output_s3)

            # 15-factor per-table analysis
            size_gb   = stats.get("total_size_gb", 0)
            rec_cnt   = stats.get("total_records", 0)
            avg_mb    = stats.get("avg_file_size_mb", 128)
            file_cnt  = stats.get("file_cnt", 1)
            snap_cnt  = stats.get("snapshot_count", 0)
            tiny_cnt  = stats.get("tiny_file_cnt", 0)
            w_amp     = stats.get("write_amplification", 1)
            skew_r    = stats.get("skew_ratio", 1)
            grow_gpd  = stats.get("growth_gb_per_day", 0)

            # Compression ratio estimation from avg file size
            comp_ratio = 0.25 if avg_mb > 32 else 0.4  # larger files = better compression
            raw_gb     = round(size_gb / comp_ratio, 2) if comp_ratio > 0 else size_gb

            # Growth forecast (90 days, Euler model)
            projected_90d = round(size_gb * math.exp(grow_gpd / max(size_gb, 0.001) * 90), 2) if grow_gpd > 0 else size_gb

            # Recommended shuffle partitions: 1 partition per 128 MB
            shuffle_parts = max(200, min(2000, int((size_gb * 1024) / 128)))

            analysis = {
                "database": db,
                "table": tbl,
                "iceberg_stats": stats,
                "sizing_analysis": {
                    "total_size_gb": size_gb,
                    "raw_size_gb_estimated": raw_gb,
                    "compression_ratio": comp_ratio,
                    "total_records": rec_cnt,
                    "avg_file_size_mb": avg_mb,
                    "file_count": file_cnt,
                    "tiny_file_count": tiny_cnt,
                    "tiny_file_pct": round(100 * tiny_cnt / max(file_cnt, 1), 1),
                    "partition_count": stats.get("partition_count", 0),
                    "skew_ratio": skew_r,
                    "write_amplification": w_amp,
                    "snapshot_count": snap_cnt,
                    "growth_gb_per_day": grow_gpd,
                    "projected_size_90d_gb": projected_90d,
                    "recommended_shuffle_partitions": shuffle_parts,
                    "health_grade": stats.get("health_grade", "C"),
                    "health_score": stats.get("health_score", 50),
                },
                "recommendations": [i["recommendation"] for i in stats.get("health_issues", [])],
            }
            results.append(analysis)

        total_gb  = sum(r["sizing_analysis"]["total_size_gb"] for r in results)
        worst_tbl = max(results, key=lambda r: r["iceberg_stats"].get("health_score", 100) * -1 + 100, default={})

        return json.dumps({
            "source":              "athena_iceberg_system_tables",
            "tables_analyzed":     len(results),
            "total_size_gb":       round(total_gb, 3),
            "tables":              results,
            "worst_health_table":  worst_tbl.get("table", ""),
            "global_recommendations": [
                "Run REWRITE_DATA_FILES on tables with tiny_file_pct > 30%",
                "expire_snapshots on tables with snapshot_count > 30",
                "Enable AQE for all jobs: spark.sql.adaptive.enabled=true",
            ],
        })
    except Exception as exc:
        logger.error("Athena sizing analysis failed: %s", exc)
        return json.dumps({"error": str(exc), "source": "athena_iceberg_system_tables"})


# ---------------------------------------------------------------------------
# Agent factory
# ---------------------------------------------------------------------------
def create_sizing_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                        region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for data sizing analysis."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[
            analyse_data_sizing,
            analyse_data_sizing_with_athena,
            estimate_shuffle_size,
            analyse_partition_efficiency,
            analyse_file_size_distribution,
            forecast_table_growth,
            assess_iceberg_snapshot_health,
        ],
    )
