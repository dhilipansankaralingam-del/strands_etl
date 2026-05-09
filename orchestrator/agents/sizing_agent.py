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
            estimate_shuffle_size,
            analyse_partition_efficiency,
            analyse_file_size_distribution,
            forecast_table_growth,
            assess_iceberg_snapshot_health,
        ],
    )
