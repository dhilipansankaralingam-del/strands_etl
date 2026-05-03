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
# Agent factory
# ---------------------------------------------------------------------------
def create_sizing_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                        region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for data sizing analysis."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[analyse_data_sizing, estimate_shuffle_size, analyse_partition_efficiency],
    )
