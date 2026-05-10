"""
Size Analyzer Agent
===================

Analyzes source tables to estimate data volumes and all performance-impacting
characteristics.  Supports both rule-based and LLM-driven analysis.

The LLM mode receives the full Iceberg metadata telemetry plus the script code
so it can reason across dimensions that rule-based logic cannot — e.g. "these
three tables look like duplicates", "the join key matches the skewed partition
column", "growth rate will breach the cost threshold in 6 weeks".

Rule-based factors
------------------
  1.  File size distribution   – tiny/small/ideal/large buckets per table
  2.  Iceberg snapshot accuracy – uses $files (current snapshot, not S3 listing)
  3.  Snapshot bloat            – expire_snapshots cost and planning overhead
  4.  Manifest explosion        – $manifests count drives query-planning latency
  5.  Write amplification       – total added / current files → compaction debt
  6.  Partition skew            – $partitions max/avg ratio → straggler risk
  7.  File size uniformity (CV) – std/mean → uneven file sizes cause OOM spikes
  8.  Data growth forecasting   – GB/day from snapshot timestamps → future cost
  9.  Orphan file estimation    – S3 total vs $files live → unreferenced waste
  10. Storage cost attribution  – live / dead-snapshot / manifest cost breakdown
  11. Compression effectiveness – actual bytes vs estimated raw → format health
  12. Hot / cold partition age  – old unread partitions → Glacier archival
  13. Cross-table redundancy    – shared column names → duplicate dataset detection
  14. Column width tax          – 100+ columns → deserialization overhead
  15. Broadcast candidates      – small tables → avoid shuffle joins
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult

try:
    from .scientific_tools import (
        exponential_growth, zipf_skew_model,
        gaussian_file_distribution, bloom_filter_savings,
    )
    _HAS_SCIENTIFIC = True
except ImportError:
    _HAS_SCIENTIFIC = False

try:
    from .pipeline_tools import SIZE_AGENT_TOOLS as _SIZE_AGENT_TOOLS
except ImportError:
    _SIZE_AGENT_TOOLS = []

# ── Thresholds ────────────────────────────────────────────────────────────────
_TINY_FILE_MB      = 10
_SMALL_FILE_MB     = 128
_IDEAL_FILE_MB     = 256
_LARGE_FILE_MB     = 1024
_SNAPSHOT_WARN     = 30
_SNAPSHOT_CRITICAL = 100
_MANIFEST_WARN     = 1_000
_MANIFEST_CRITICAL = 10_000
_SKEW_WARN         = 3.0
_SKEW_CRITICAL     = 10.0
_WRITE_AMP_WARN    = 5.0
_FILE_CV_WARN      = 1.5
_BROADCAST_BYTES   = 100 * 1024 * 1024
_LARGE_TABLE_GB    = 100
_XLARGE_TABLE_GB   = 500
_COLD_PARTITION_DAYS = 700       # partitions not touched in 700 days → cold
_REDUNDANCY_THRESHOLD = 0.80     # 80% shared column names → likely duplicate
_S3_PRICE_PER_GB_MONTH  = 0.023   # standard S3 pricing
_GLACIER_DA_PER_GB_MONTH = 0.004  # S3 Glacier Deep Archive pricing
_COMPRESSION_POOR  = 0.60         # ratio > 0.6 = worse than CSV
_COMPRESSION_GOOD  = 0.30
_COMPRESSION_EXCEL = 0.10


def _build_cold_result(
    cold_gb: float,
    cold_count: Optional[int],
    total_count: Optional[int],
    oldest_days: int,
    live_gb: float,
    now: datetime,
    source: str,
) -> Dict:
    """Shared result builder for all cold-partition strategies."""
    s3_monthly   = round(cold_gb * _S3_PRICE_PER_GB_MONTH,  2)
    glac_monthly = round(cold_gb * _GLACIER_DA_PER_GB_MONTH, 2)
    saving       = round(s3_monthly - glac_monthly, 2)
    cold_pct     = round(cold_gb / max(live_gb, 0.001) * 100, 1)
    risk         = "high" if oldest_days > 1095 else "medium" if oldest_days > _COLD_PARTITION_DAYS else "none"
    result: Dict = {
        "source":               source,
        "cold_threshold_days":  _COLD_PARTITION_DAYS,
        "oldest_cold_days":     oldest_days,
        "cold_risk":            risk,
        "estimated_cold_gb":    cold_gb,
        "cold_pct_of_table":    cold_pct,
        "s3_monthly_usd":       s3_monthly,
        "glacier_monthly_usd":  glac_monthly,
        "potential_saving_usd": saving,
    }
    if cold_count is not None:
        result["cold_partition_count"] = cold_count
    if total_count is not None:
        result["total_partition_count"] = total_count
        result["cold_partition_pct"] = round(cold_count / max(total_count, 1) * 100, 1)
    if saving > 1:
        result["recommendation"] = (
            f"Archive ~{cold_gb:.1f} GB ({cold_pct:.0f}% of table) "
            f"untouched for >{_COLD_PARTITION_DAYS} days "
            f"to S3 Glacier Deep Archive. "
            f"Save ~${saving:.2f}/month (${saving * 12:.0f}/year)."
        )
    return result


class SizeAnalyzerAgent(CostOptimizerAgent):
    """Analyzes data sizes and all performance-impacting table characteristics."""

    AGENT_NAME     = "size_analyzer"
    AGENT_TOOLS    = _SIZE_AGENT_TOOLS   # compute_skew_model, compute_growth_forecast, compute_bloom_filter_value
    MAX_ITERATIONS = 3                   # up to 3 tool rounds: skew → growth → bloom

    BYTES_PER_ROW = {
        "narrow":    200,
        "medium":    500,
        "wide":     1000,
        "very_wide":2000,
    }
    COMPRESSION_RATIO = {
        "parquet": 0.25, "orc": 0.25, "iceberg": 0.25, "delta": 0.25,
        "avro":    0.50, "json": 0.70, "csv": 0.60,
    }

    # ── Public entry point ─────────────────────────────────────────────────────

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        tables_detail:  List[Dict] = []
        total_raw_bytes: int       = 0
        total_comp_bytes: int      = 0
        skew_risks:     List[Dict] = []

        for tbl in input_data.source_tables:
            ta = self._analyze_table(tbl, input_data.processing_mode)
            tables_detail.append(ta)
            total_raw_bytes  += ta["raw_size_bytes"]
            total_comp_bytes += ta["compressed_size_bytes"]
            if ta.get("skew_risk", "low") != "low":
                skew_risks.append({
                    "table":  tbl.get("table", tbl.get("name", "unknown")),
                    "risk":   ta["skew_risk"],
                    "reason": ta.get("skew_reason", ""),
                })

        delta_ratio  = (self._estimate_delta_ratio(input_data)
                        if input_data.processing_mode == "delta" else 1.0)
        join_factor  = self._estimate_join_amplification(input_data, context)
        effective_gb = (total_comp_bytes * delta_ratio * join_factor) / (1024 ** 3)
        total_raw_gb = total_raw_bytes  / (1024 ** 3)
        total_comp_gb= total_comp_bytes / (1024 ** 3)

        skew_score   = self._calculate_skew_score(skew_risks, input_data)
        part_eff     = self._analyze_partitions(input_data.source_tables)
        iceberg_hlth = self._aggregate_iceberg_health(tables_detail)
        cross_redund = self._detect_cross_table_redundancy(tables_detail)
        storage_cost = self._aggregate_storage_costs(tables_detail)

        analysis = {
            "total_raw_size_gb":           round(total_raw_gb, 2),
            "total_compressed_size_gb":    round(total_comp_gb, 2),
            "effective_size_gb":           round(effective_gb, 2),
            "processing_mode":             input_data.processing_mode,
            "delta_ratio":                 round(delta_ratio, 3),
            "join_amplification_factor":   round(join_factor, 2),
            "skew_risk_score":             skew_score,
            "skew_risk_factors":           skew_risks,
            "partition_efficiency_score":  part_eff["score"],
            "partition_recommendations":   part_eff["recommendations"],
            "tables_analyzed":             len(tables_detail),
            "tables_detail":               tables_detail,
            "iceberg_health":              iceberg_hlth,
            "cross_table_redundancy":      cross_redund,
            "storage_cost_summary":        storage_cost,
            "size_confidence":             self._calculate_confidence(input_data),
        }

        recs = self._generate_recommendations(analysis, tables_detail)
        analysis["scientific_analysis"] = self._run_scientific_analysis(tables_detail)
        return AnalysisResult(
            agent_name      = self.AGENT_NAME,
            success         = True,
            analysis        = analysis,
            recommendations = recs,
            metrics         = {
                "total_tables":      len(tables_detail),
                "effective_size_gb": analysis["effective_size_gb"],
                "skew_risk_score":   skew_score,
                "monthly_s3_cost":   storage_cost.get("total_monthly_usd", 0),
            },
        )

    # ── Scientific analysis ────────────────────────────────────────────────────

    def _run_scientific_analysis(self, tables_detail: List[Dict]) -> Dict:
        """
        Run mathematical models on already-gathered table metrics.
        No extra AWS calls — works with data collected by _analyze_table().
        """
        if not _HAS_SCIENTIFIC:
            return {}

        per_table: Dict = {}
        for ta in tables_detail:
            name = ta.get("table", "unknown")
            fs   = ta.get("file_stats", {})
            gf   = ta.get("growth_forecast") or {}
            res: Dict = {}

            total_gb   = ta.get("compressed_size_gb", 0.0) or fs.get("total_size_gb", 0.0)
            gb_per_day = gf.get("gb_per_day", 0.0)

            # 1. Euler's exponential growth N(t)=N₀·e^(rt) — more accurate than linear
            if total_gb > 0.1 and gb_per_day > 0:
                daily_rate = gb_per_day / total_gb
                res["exponential_growth"] = exponential_growth(
                    initial_gb=total_gb,
                    daily_rate=daily_rate,
                    days=90,
                )

            # 2. Zipf/power-law skew model — synthetic from skew_ratio + partition count
            skew_ratio  = fs.get("skew_ratio", 1.0)
            part_count  = max(fs.get("partition_count", 10), 4)
            if skew_ratio > 1.5:
                import math as _math
                avg  = 100.0
                # Power-law: size[k] ∝ k^(-α), α derived from skew_ratio
                alpha = _math.log(skew_ratio) / _math.log(part_count) if part_count > 1 else 1.0
                synth = sorted(
                    [avg * (k ** -alpha) for k in range(1, part_count + 1)],
                    reverse=True
                )
                res["zipf_skew"] = zipf_skew_model(partition_sizes=synth)
                res["zipf_skew"]["_note"] = (
                    "Synthetic from skew_ratio — query table$partitions for exact α"
                )

            # 3. Gaussian/bimodality analysis from file-size bucket counts
            fc    = fs.get("file_count", 0)
            tiny  = fs.get("tiny_count",  0)
            small = fs.get("small_count", 0)
            avg_mb = fs.get("avg_mb", 128.0)
            remaining = max(0, fc - tiny - small)
            if avg_mb > _LARGE_FILE_MB:
                ideal_c, large_c = 0, remaining
            elif avg_mb > _IDEAL_FILE_MB:
                ideal_c, large_c = remaining // 2, remaining - remaining // 2
            else:
                ideal_c, large_c = remaining, 0
            # Cap array sizes to avoid slowdowns on huge file counts
            synth_files = (
                [5.0]   * min(tiny,    500) +
                [64.0]  * min(small,   500) +
                [256.0] * min(ideal_c, 500) +
                [768.0] * min(large_c, 500)
            )
            if len(synth_files) >= 4:
                res["file_distribution"] = gaussian_file_distribution(
                    file_sizes_mb=synth_files
                )

            # 4. Bloom filter I/O savings — for every non-broadcast join table
            if not ta.get("is_broadcast_candidate") and total_gb > 1:
                distinct_keys = max(1, int(ta.get("record_count", 1_000_000) * 0.1))
                res["bloom_filter"] = bloom_filter_savings(
                    table_size_gb=total_gb,
                    join_selectivity=0.05,
                    num_distinct_keys=min(distinct_keys, 10_000_000),
                    bits_per_key=10,
                )

            if res:
                per_table[name] = res

        return {"per_table": per_table}

    # ── LLM prompt override ────────────────────────────────────────────────────

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        """
        Sends the full Iceberg metadata telemetry to the LLM so it can reason
        across dimensions that rule-based logic misses:
          - Join key ↔ partition skew correlation
          - Growth rate → cost inflection point timing
          - Schema similarity → cross-table duplicate detection
          - Compression anomalies → format or encoding issues
          - Cold partition archival opportunity windows
        """
        # Run rule-based first to give LLM the structured stats as input
        rule_result = self._analyze_rule_based(input_data, context)
        rule_analysis = rule_result.analysis

        # Build per-table telemetry blocks
        table_telemetry = []
        for tbl in input_data.source_tables:
            entry: Dict[str, Any] = {
                "table":          tbl.get("table", "unknown"),
                "database":       tbl.get("database", ""),
                "format":         tbl.get("format", "parquet"),
                "is_iceberg":     tbl.get("is_iceberg", False),
                "record_count":   tbl.get("record_count", 0),
                "size_gb":        tbl.get("size_gb", 0),
                "partition_col":  tbl.get("partition_column"),
                "sizing_source":  tbl.get("source", "unknown"),
            }
            if tbl.get("iceberg_stats"):
                s = tbl["iceberg_stats"]
                entry["iceberg_telemetry"] = {
                    "current_files":       s.get("file_cnt", 0),
                    "avg_file_mb":         s.get("avg_file_size_mb", 0),
                    "tiny_files":          s.get("tiny_file_cnt", 0),
                    "small_files":         s.get("small_file_cnt", 0),
                    "file_size_cv":        s.get("file_size_cv", 0),
                    "total_size_gb":       s.get("total_size_gb", 0),
                    "snapshot_count":      s.get("snapshot_count", 0),
                    "oldest_snapshot":     s.get("oldest_snapshot_ts", ""),
                    "newest_snapshot":     s.get("newest_snapshot_ts", ""),
                    "total_added_files":   s.get("total_added_files", 0),
                    "total_deleted_files": s.get("total_deleted_files", 0),
                    "total_added_records": s.get("total_added_records", 0),
                    "partition_count":     s.get("partition_count", 0),
                    "partition_skew":      s.get("skew_ratio", 1.0),
                    "manifest_count":      s.get("manifest_count", 0),
                    "write_amplification": round(
                        s.get("total_added_files", 0) / max(s.get("file_cnt", 1), 1), 1
                    ),
                }
                # Growth rate
                gr = self._compute_growth_rate(s)
                if gr:
                    entry["iceberg_telemetry"]["growth_gb_per_day"]    = gr["gb_per_day"]
                    entry["iceberg_telemetry"]["days_to_2x_size"]       = gr.get("days_to_2x")
                    entry["iceberg_telemetry"]["projected_size_90d_gb"] = gr.get("projected_90d_gb")
                # Compression
                cr = self._compute_compression_ratio(tbl, s)
                if cr is not None:
                    entry["iceberg_telemetry"]["compression_ratio"] = cr
                    entry["iceberg_telemetry"]["compression_health"] = (
                        "poor" if cr > _COMPRESSION_POOR else
                        "adequate" if cr > _COMPRESSION_GOOD else
                        "good" if cr > _COMPRESSION_EXCEL else "excellent"
                    )
            table_telemetry.append(entry)

        cross_redund = rule_analysis.get("cross_table_redundancy", {})
        storage_cost = rule_analysis.get("storage_cost_summary", {})

        # Run scientific algorithms on already-gathered table stats
        rule_tables = rule_result.analysis.get("tables_detail",
                      [{"table": t.get("table", ""), "file_stats": {},
                        "compressed_size_gb": t.get("size_gb", 0),
                        "growth_forecast": {}, "is_broadcast_candidate": False,
                        "record_count": t.get("record_count", 0)}
                       for t in input_data.source_tables])
        sci = self._run_scientific_analysis(rule_tables)
        sci_json = json.dumps(sci, indent=2, default=str) if sci.get("per_table") else "  (insufficient data)"

        tool_guidance = """
━━━━ TOOLS AVAILABLE (call selectively — only when evidence warrants) ━━━━━━━━
  compute_skew_model(partition_sizes, table_name)
      → CALL IF skew_ratio > 3 in any table's file_stats.
        Pass a synthetic power-law array if raw partition sizes are unavailable:
        [avg * skew_ratio^((n-k)/(n-1)) for k in 1..partition_count]
  compute_growth_forecast(table_name, current_size_gb, daily_growth_gb)
      → CALL IF growth_gb_per_day > 0 for any table (from iceberg_telemetry).
  compute_bloom_filter_value(table_name, table_size_gb, join_selectivity_pct)
      → CALL FOR every table > 1 GB that appears in a JOIN in the script AND
        is NOT a broadcast candidate (is_broadcast_candidate = false).
After calling any tools, respond with the JSON object specified below.
""" if self.AGENT_TOOLS else ""

        return f"""
You are a Senior Data Platform Engineer specializing in Apache Iceberg, AWS Glue,
and PySpark performance optimization.  Analyze the table telemetry below and the
PySpark script to produce a comprehensive sizing and health report.
{tool_guidance}

━━━━ SCRIPT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
File: {input_data.script_path}
Mode: {input_data.processing_mode}
```python
{input_data.script_content[:3000]}{'... [truncated]' if len(input_data.script_content) > 3000 else ''}
```

━━━━ TABLE TELEMETRY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```json
{json.dumps(table_telemetry, indent=2, default=str)}
```

━━━━ SCIENTIFIC ALGORITHM RESULTS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
(Pre-computed mathematical models — cite these in your findings)
  • exponential_growth  – Euler N(t)=N₀·e^(rt): growth curve, doubling time, 90d projection
  • zipf_skew           – Zipf α + recommended salt factor (biased from skew_ratio, not raw $partitions)
  • file_distribution   – Bimodality coefficient BC=(γ₁²+1)/κ: >0.555 = mixed tiny+huge anti-pattern
  • bloom_filter        – Bloom filter FPP + I/O savings if enabled on join column
```json
{sci_json}
```

━━━━ CROSS-TABLE REDUNDANCY SIGNALS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```json
{json.dumps(cross_redund, indent=2, default=str)}
```

━━━━ STORAGE COST BREAKDOWN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```json
{json.dumps(storage_cost, indent=2, default=str)}
```

━━━━ RULE-BASED PRE-ANALYSIS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
(Use this as a starting point — add LLM-detected patterns on top)
```json
{json.dumps({{
    "effective_size_gb":     rule_analysis.get("effective_size_gb"),
    "skew_risk_score":       rule_analysis.get("skew_risk_score"),
    "partition_efficiency":  rule_analysis.get("partition_efficiency_score"),
    "iceberg_health":        rule_analysis.get("iceberg_health", {{}}).get("health"),
    "rule_recommendations":  [r.get("title") for r in rule_result.recommendations[:8]],
}}, indent=2)}
```

━━━━ ANALYSIS GUIDELINES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Detect and score ALL of the following (add findings not caught by rule-based):

1. FILE SIZE HEALTH
   - tiny_files > 0 and avg_file_mb < 10  → P0: rewrite_data_files immediately
   - avg_file_mb < 128                     → P1: compact to 256 MB target
   - file_size_cv > 1.5                    → P1: uneven sizes cause OOM spikes
   - avg_file_mb > 1024                    → P2: oversized, tasks will spill

2. SNAPSHOT BLOAT
   - snapshot_count > 100  → P0: expire_snapshots (planning latency critical)
   - snapshot_count > 30   → P1: schedule weekly expire_snapshots
   - Detect: old snapshots holding large deleted data files on S3

3. MANIFEST EXPLOSION
   - manifest_count > 10000 → P0: rewrite_manifests now (queries will time out)
   - manifest_count > 1000  → P1: schedule rewrite_manifests weekly

4. WRITE AMPLIFICATION
   - write_amplification > 10× → P0: no compaction ever ran, backlog is severe
   - write_amplification > 5×  → P1: enable auto-compaction

5. PARTITION SKEW (cross-reference with script join keys)
   - partition_skew > 10× AND the skewed column is used as a join key in the
     script → P0: this join WILL produce stragglers
   - Suggest specific salting strategy based on the column name and data pattern

6. DATA GROWTH & COST TRAJECTORY
   - If days_to_2x_size < 90 → P1: current worker config will be inadequate
   - If projected_size_90d_gb > threshold → calculate future cost impact
   - Estimate: at this growth rate, annual storage cost will be $X

7. COMPRESSION EFFECTIVENESS
   - compression_ratio > 0.6 → likely JSON columns, binary blobs, or pre-compressed data
   - Identify which columns in the script likely cause poor compression
   - Suggest: columnar encoding hints, schema normalization, or format change

8. CROSS-TABLE REDUNDANCY
   - Tables with > {_REDUNDANCY_THRESHOLD:.0%} shared column names AND similar record counts
     are likely storing the same data in multiple locations
   - Estimate combined redundant storage cost
   - Suggest which table should be the canonical source

9. COLD PARTITION ARCHIVAL
   - If the table has date-based partitions and oldest data is > {_COLD_PARTITION_DAYS} days old
     with likely low read frequency → estimate Glacier archival savings
   - Only flag if the script does NOT filter to recent data only

10. ORPHAN FILES
    - Estimate orphan file count when total_added_files >> current_files AND
      large snapshot history exists
    - Calculate estimated orphan storage cost

11. SCRIPT-TELEMETRY CORRELATION (LLM-only, cannot be rule-based)
    - Does the script join on the same column that has high partition_skew?
      If yes → elevate to P0 and suggest specific salting code
    - Does the script do a full table scan (no partition filter) on a cold table?
      Flag the specific line and suggest partition pruning
    - Does the script read multiple tables with the same schema?
      Cross-reference with redundancy signals above
    - Is the script reading Iceberg tables without using time-travel or incremental
      snapshot reads when that would be beneficial?

Return a JSON object with this exact structure:
{{
  "effective_size_gb": <number>,
  "size_confidence": "high|medium|low",
  "tables_analysis": [
    {{
      "table": "<name>",
      "health_score": <0-100>,
      "key_findings": ["<finding1>", ...],
      "estimated_monthly_s3_cost_usd": <number>
    }}
  ],
  "iceberg_health_summary": {{
    "overall": "healthy|warning|critical",
    "top_issues": ["<issue>", ...],
    "estimated_wasted_storage_gb": <number>,
    "estimated_wasted_cost_usd_monthly": <number>
  }},
  "growth_forecast": {{
    "gb_per_day": <number>,
    "projected_90d_gb": <number>,
    "days_to_cost_breach": <number or null>
  }},
  "cross_table_redundancy": {{
    "duplicate_pairs": [["<table1>", "<table2>", <overlap_pct>]],
    "estimated_redundant_gb": <number>,
    "estimated_waste_usd_monthly": <number>
  }},
  "script_telemetry_findings": [
    {{
      "finding": "<description>",
      "severity": "critical|high|medium|low",
      "line_hint": "<code snippet if applicable>",
      "fix": "<specific fix>"
    }}
  ],
  "recommendations": [
    {{
      "priority": "P0|P1|P2|P3",
      "category": "<category>",
      "title": "<title>",
      "description": "<description>",
      "implementation": "<specific SQL or code>",
      "estimated_savings_percent": <number>,
      "source": "<which factor triggered this>"
    }}
  ]
}}
""".strip()

    # ── Per-table analysis ─────────────────────────────────────────────────────

    def _analyze_table(self, tbl: Dict, processing_mode: str) -> Dict:
        record_count = tbl.get("record_count", tbl.get("records", 0))
        column_count = tbl.get("column_count", tbl.get("columns", 30))
        fmt          = tbl.get("format", "parquet").lower()
        table_name   = tbl.get("table", tbl.get("name", "unknown"))
        is_iceberg   = tbl.get("is_iceberg", fmt == "iceberg")

        if   column_count < 20:  width = "narrow"
        elif column_count < 50:  width = "medium"
        elif column_count < 100: width = "wide"
        else:                    width = "very_wide"

        bpr        = self.BYTES_PER_ROW[width]
        raw_bytes  = record_count * bpr
        comp_ratio = self.COMPRESSION_RATIO.get(fmt, 0.30)
        comp_bytes = raw_bytes * comp_ratio

        if "size_gb" in tbl:
            comp_bytes = tbl["size_gb"] * (1024 ** 3)
            raw_bytes  = comp_bytes / comp_ratio

        comp_gb    = comp_bytes / (1024 ** 3)
        file_stats = self._extract_file_stats(tbl)
        skew_risk, skew_reason = self._assess_table_skew(tbl, file_stats)

        iceberg_issues: List[str] = []
        growth_info:    Dict      = {}
        comp_health:    Dict      = {}
        cold_info:      Dict      = {}
        storage_cost:   Dict      = {}
        orphan_est:     Dict      = {}

        if is_iceberg:
            iceberg_s    = tbl.get("iceberg_stats", {})
            iceberg_issues = self._iceberg_health_issues(tbl, file_stats)
            growth_info  = self._compute_growth_rate(iceberg_s)
            comp_health  = self._compression_health(tbl, iceberg_s)
            cold_info    = self._cold_partition_analysis(iceberg_s, tbl)
            orphan_est   = self._estimate_orphan_files(iceberg_s, file_stats)
            storage_cost = self._table_storage_cost(iceberg_s, file_stats, comp_gb)

        return {
            "table":                  table_name,
            "database":               tbl.get("database", ""),
            "format":                 fmt,
            "is_iceberg":             is_iceberg,
            "record_count":           record_count,
            "column_count":           column_count,
            "column_width":           width,
            "raw_size_bytes":         int(raw_bytes),
            "compressed_size_bytes":  int(comp_bytes),
            "compressed_size_gb":     round(comp_gb, 3),
            "skew_risk":              skew_risk,
            "skew_reason":            skew_reason,
            "is_broadcast_candidate": comp_bytes < _BROADCAST_BYTES,
            "iceberg_issues":         iceberg_issues,
            "growth_forecast":        growth_info,
            "compression_health":     comp_health,
            "cold_partition_info":    cold_info,
            "storage_cost_breakdown": storage_cost,
            "orphan_file_estimate":   orphan_est,
            "sizing_source":          tbl.get("source", "heuristic"),
            **({"file_stats": file_stats} if file_stats else {}),
        }

    # ── Enhancement 1 & 2: file stats extraction (from $files or S3) ──────────

    def _extract_file_stats(self, tbl: Dict) -> Dict:
        iceberg_s = tbl.get("iceberg_stats", {})
        sf_report = tbl.get("_small_file_report", {})
        if iceberg_s and "file_cnt" in iceberg_s:
            fc = iceberg_s.get("file_cnt", 0)
            return {
                "source":            "iceberg_$files",
                "file_count":        fc,
                "avg_mb":            iceberg_s.get("avg_file_size_mb", 0.0),
                "min_kb":            iceberg_s.get("min_file_size_kb", 0.0),
                "max_mb":            iceberg_s.get("max_file_size_mb", 0.0),
                "tiny_count":        iceberg_s.get("tiny_file_cnt", 0),
                "small_count":       iceberg_s.get("small_file_cnt", 0),
                "file_size_cv":      iceberg_s.get("file_size_cv", 0.0),
                "snapshot_count":    iceberg_s.get("snapshot_count", 0),
                "skew_ratio":        iceberg_s.get("skew_ratio", 1.0),
                "write_amp":         (iceberg_s.get("total_added_files", 0)
                                      / max(fc, 1)),
                "manifest_count":    iceberg_s.get("manifest_count", 0),
                "total_added_files": iceberg_s.get("total_added_files", 0),
                "total_deleted_files":iceberg_s.get("total_deleted_files", 0),
            }
        if sf_report and sf_report.get("file_count", 0) > 0:
            fc = sf_report["file_count"]
            avg_mb = sf_report.get("avg_file_size_mb", 0.0)
            return {
                "source":      "s3_listing",
                "file_count":  fc,
                "avg_mb":      avg_mb,
                "min_kb":      sf_report.get("min_file_size_mb", 0.0) * 1024,
                "max_mb":      sf_report.get("max_file_size_mb", 0.0),
                "tiny_count":  sf_report.get("tiny_file_count", 0),
                "small_count": sf_report.get("small_file_count", 0),
                "file_size_cv": 0.0, "snapshot_count": 0,
                "skew_ratio":  1.0, "write_amp": 1.0,
                "manifest_count": 0, "total_added_files": 0, "total_deleted_files": 0,
            }
        return {}

    # ── Enhancement 3: Snapshot bloat + manifest explosion ────────────────────

    def _iceberg_health_issues(self, tbl: Dict, fs: Dict) -> List[str]:
        issues: List[str] = []
        iceberg_s  = tbl.get("iceberg_stats", {})
        snap_count = fs.get("snapshot_count", 0)
        wa         = fs.get("write_amp", 1.0)
        skew       = fs.get("skew_ratio", 1.0)
        avg_mb     = fs.get("avg_mb", 0.0)
        fc         = fs.get("file_count", 0)
        cv         = fs.get("file_size_cv", 0.0)
        mc         = fs.get("manifest_count", 0)

        if snap_count > _SNAPSHOT_CRITICAL:
            issues.append(
                f"CRITICAL: {snap_count} snapshots — Iceberg planning severely degraded; "
                "run expire_snapshots immediately (retain_last=5)."
            )
        elif snap_count > _SNAPSHOT_WARN:
            issues.append(
                f"{snap_count} snapshots — schedule expire_snapshots weekly "
                "(retain_last=5) to keep planning fast."
            )

        # Enhancement 4: Manifest explosion
        if mc > _MANIFEST_CRITICAL:
            issues.append(
                f"CRITICAL: {mc:,} manifests — every query reads {mc:,} S3 metadata files "
                "before touching data. Run rewrite_manifests now."
            )
        elif mc > _MANIFEST_WARN:
            issues.append(
                f"{mc:,} manifests — query planning reads all of these on each scan. "
                "Schedule weekly rewrite_manifests."
            )

        if wa > _WRITE_AMP_WARN:
            issues.append(
                f"Write amplification {wa:.1f}× — {fs.get('total_added_files',0):,} files "
                f"added vs {fc:,} current. Enable auto-compaction."
            )

        if avg_mb > 0 and avg_mb < _TINY_FILE_MB and fc >= 10:
            issues.append(
                f"Tiny files: avg {avg_mb:.1f} MB ({fc:,} files). "
                "Every file is a Spark task → massive scheduling overhead."
            )
        elif avg_mb > 0 and avg_mb < _SMALL_FILE_MB and fc >= 10:
            issues.append(
                f"Small files: avg {avg_mb:.1f} MB ({fc:,} files). "
                f"Target {_IDEAL_FILE_MB} MB; run OPTIMIZE."
            )
        elif avg_mb > _LARGE_FILE_MB:
            issues.append(
                f"Oversized files: avg {avg_mb:.1f} MB — tasks will run long and may OOM."
            )

        if cv > _FILE_CV_WARN:
            issues.append(
                f"Uneven file sizes (CV={cv:.2f}) — mix of very large and tiny files. "
                "Run OPTIMIZE for uniform sizing."
            )

        if skew > _SKEW_CRITICAL:
            issues.append(
                f"Severe partition skew ({skew:.1f}×) — one partition has "
                f"{skew:.0f}× more records than average. Consider key salting."
            )
        elif skew > _SKEW_WARN:
            issues.append(
                f"Partition skew ({skew:.1f}×) — enable AQE skewJoin."
            )

        return issues

    # ── Enhancement 5: Data growth forecasting ────────────────────────────────

    def _compute_growth_rate(self, iceberg_s: Dict) -> Dict:
        oldest    = iceberg_s.get("oldest_snapshot_ts", "")
        newest    = iceberg_s.get("newest_snapshot_ts", "")
        total_gb  = iceberg_s.get("total_size_gb", 0.0)
        added_rec = iceberg_s.get("total_added_records", 0)
        if not oldest or not newest or total_gb == 0:
            return {}
        try:
            def _parse(ts: str):
                for fmt in ("%Y-%m-%d %H:%M:%S.%f %Z", "%Y-%m-%dT%H:%M:%S.%fZ",
                            "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
                    try:
                        return datetime.strptime(ts[:26], fmt[:len(ts)])
                    except Exception:
                        continue
                return None
            t0 = _parse(oldest)
            t1 = _parse(newest)
            if not t0 or not t1:
                return {}
            span_days = max((t1 - t0).days, 1)

            # Derive compressed bytes-per-row from $files stats (already fetched).
            # total_size_gb and total_records both come from the current-snapshot
            # $files query, so their ratio is the real on-disk bytes per live record.
            live_records = iceberg_s.get("total_records", 0)
            if live_records > 0:
                bytes_per_row_compressed = (total_gb * (1024 ** 3)) / live_records
            else:
                # Fallback: use avg_file_size_mb and file_cnt if record count missing
                avg_mb   = iceberg_s.get("avg_file_size_mb", 128.0)
                file_cnt = iceberg_s.get("file_cnt", 1)
                # Estimate ~1M rows per 128 MB parquet file as conservative floor
                est_rows = max(file_cnt * 1_000_000, 1)
                bytes_per_row_compressed = (avg_mb * 1024 * 1024 * file_cnt) / est_rows

            # added_records from $snapshots counts every record written (including
            # later-deleted ones), so growth is slightly over-estimated on CDC tables.
            added_gb   = added_rec * bytes_per_row_compressed / (1024 ** 3)
            gb_per_day = added_gb / span_days

            days_to_2x       = int(total_gb / gb_per_day) if gb_per_day > 0 else None
            projected_90d    = round(total_gb + gb_per_day * 90, 2)
            monthly_cost_now = round(total_gb    * _S3_PRICE_PER_GB_MONTH, 2)
            monthly_cost_90d = round(projected_90d * _S3_PRICE_PER_GB_MONTH, 2)
            return {
                "span_days":              span_days,
                "bytes_per_row_actual":   round(bytes_per_row_compressed, 2),
                "gb_per_day":             round(gb_per_day, 3),
                "days_to_2x":             days_to_2x,
                "projected_90d_gb":       projected_90d,
                "monthly_cost_now_usd":   monthly_cost_now,
                "monthly_cost_90d_usd":   monthly_cost_90d,
            }
        except Exception:
            return {}

    # ── Enhancement 6: Storage cost attribution ───────────────────────────────

    def _table_storage_cost(
        self, iceberg_s: Dict, fs: Dict, comp_gb: float
    ) -> Dict:
        live_gb      = iceberg_s.get("total_size_gb", comp_gb)
        # Dead snapshot files = S3 total - live (approximated by write amplification)
        wa           = fs.get("write_amp", 1.0)
        dead_gb      = max(0.0, live_gb * (wa - 1.0))
        # Manifests are tiny — estimate 1 KB per manifest
        mc           = fs.get("manifest_count", 0)
        manifest_gb  = mc * 1024 / (1024 ** 3)
        total_gb     = live_gb + dead_gb + manifest_gb

        return {
            "live_data_gb":         round(live_gb, 3),
            "dead_snapshot_gb":     round(dead_gb, 3),
            "manifest_metadata_gb": round(manifest_gb, 4),
            "total_gb":             round(total_gb, 3),
            "live_monthly_usd":     round(live_gb     * _S3_PRICE_PER_GB_MONTH, 2),
            "dead_monthly_usd":     round(dead_gb     * _S3_PRICE_PER_GB_MONTH, 2),
            "manifest_monthly_usd": round(manifest_gb * _S3_PRICE_PER_GB_MONTH, 4),
            "total_monthly_usd":    round(total_gb    * _S3_PRICE_PER_GB_MONTH, 2),
            "waste_pct":            round(dead_gb / max(total_gb, 0.001) * 100, 1),
        }

    # ── Enhancement 7: Orphan file estimation ─────────────────────────────────

    def _estimate_orphan_files(self, iceberg_s: Dict, fs: Dict) -> Dict:
        """
        Orphan files = S3 objects not referenced by any snapshot manifest.
        We can't count them without a full S3 scan, but high write amplification
        with many deleted files is a strong signal.
        """
        wa          = fs.get("write_amp", 1.0)
        del_files   = fs.get("total_deleted_files", 0)
        snap_count  = fs.get("snapshot_count", 0)
        live_gb     = iceberg_s.get("total_size_gb", 0.0)

        if wa < 2 or del_files == 0:
            return {"estimated_orphan_files": 0, "risk": "low"}

        # Rough estimate: if snapshots have expired without expire_snapshots
        # then deleted file references were removed from manifests but files remain
        orphan_risk = "high" if (snap_count > _SNAPSHOT_WARN and del_files > 1000) else "medium"
        # Conservative estimate: 10-30% of deleted files may be orphaned
        orphan_est  = int(del_files * 0.20)
        avg_mb      = fs.get("avg_mb", 128.0)
        orphan_gb   = orphan_est * avg_mb / 1024

        return {
            "estimated_orphan_files": orphan_est,
            "estimated_orphan_gb":    round(orphan_gb, 2),
            "estimated_monthly_cost": round(orphan_gb * _S3_PRICE_PER_GB_MONTH, 2),
            "risk":                   orphan_risk,
            "recommendation":         (
                "Run CALL system.remove_orphan_files(table => 'db.table', "
                "older_than => TIMESTAMP 'now - 3 days', dry_run => true) "
                "to confirm before deleting."
            ) if orphan_risk == "high" else None,
        }

    # ── Enhancement 8: Compression effectiveness ──────────────────────────────

    def _compression_ratio_val(self, tbl: Dict, iceberg_s: Dict) -> Optional[float]:
        actual_gb   = iceberg_s.get("total_size_gb", tbl.get("size_gb", 0.0))
        record_cnt  = tbl.get("record_count", iceberg_s.get("total_records", 0))
        col_count   = tbl.get("column_count", 30)
        if actual_gb == 0 or record_cnt == 0:
            return None
        if   col_count < 20:  bpr = 200
        elif col_count < 50:  bpr = 500
        elif col_count < 100: bpr = 1000
        else:                 bpr = 2000
        expected_raw_gb = record_cnt * bpr / (1024 ** 3)
        return round(actual_gb / max(expected_raw_gb, 0.001), 3)

    def _compress_ratio(self, tbl: Dict, iceberg_s: Dict) -> Optional[float]:
        return self._compression_ratio_val(tbl, iceberg_s)

    def _compression_health(self, tbl: Dict, iceberg_s: Dict) -> Dict:
        ratio = self._compression_ratio_val(tbl, iceberg_s)
        if ratio is None:
            return {}
        if ratio > _COMPRESSION_POOR:
            label = "poor"
            note  = ("Likely causes: JSON columns, binary BLOBs, pre-compressed data, "
                     "or UUID-heavy schema. Consider: columnar dictionary encoding, "
                     "schema normalization, or switch to Avro for binary-heavy data.")
        elif ratio > _COMPRESSION_GOOD:
            label = "adequate"
            note  = "Typical for mixed string/numeric schemas. Parquet dictionary encoding active."
        elif ratio > _COMPRESSION_EXCEL:
            label = "good"
            note  = "Strong compression. Schema is numeric/low-cardinality friendly."
        else:
            label = "excellent"
            note  = "Exceptional compression. Schema is highly repetitive/low-cardinality."
        return {"ratio": ratio, "label": label, "note": note}

    def _compute_compression_ratio(self, tbl: Dict, iceberg_s: Dict) -> Optional[float]:
        return self._compression_ratio_val(tbl, iceberg_s)

    # ── Enhancement 9: Hot / cold partition age ───────────────────────────────

    def _cold_partition_analysis(self, iceberg_s: Dict, tbl: Dict) -> Dict:
        """
        Determine cold (unaccessed) data volume using the most accurate source available.

        Strategy priority:
          1. Iceberg $partitions.last_updated_at  — per-partition timestamps (Iceberg 1.4+)
          2. Glue get_partitions CreationTime     — for Hive-style non-Iceberg tables
          3. Date-arithmetic heuristic            — fallback using oldest_snapshot_ts
        """
        now      = datetime.utcnow()
        live_gb  = iceberg_s.get("total_size_gb", tbl.get("size_gb", 0.0))
        is_iceberg = tbl.get("is_iceberg", False)

        # ── Strategy 1: Iceberg $partitions.last_updated_at ──────────────────
        cold_partitions = iceberg_s.get("cold_partitions")  # injected by _iceberg_table_stats
        if cold_partitions is not None:
            cold_gb      = round(cold_partitions.get("cold_size_gb", 0.0), 2)
            cold_count   = cold_partitions.get("cold_partition_count", 0)
            total_parts  = cold_partitions.get("total_partition_count", 1)
            source       = "iceberg_partitions_last_updated_at"
            oldest_days  = cold_partitions.get("oldest_cold_partition_days", 0)
            return _build_cold_result(
                cold_gb, cold_count, total_parts, oldest_days,
                live_gb, now, source
            )

        # ── Strategy 2: Glue get_partitions (Hive-style non-Iceberg tables) ─
        if not is_iceberg:
            database   = tbl.get("database", "")
            table_name = tbl.get("table", tbl.get("name", ""))
            glue_cold  = self._glue_cold_partitions(database, table_name, live_gb)
            if glue_cold:
                return glue_cold

        # ── Strategy 3: date-arithmetic heuristic ────────────────────────────
        oldest = iceberg_s.get("oldest_snapshot_ts", "")
        if not oldest:
            return {}
        try:
            def _parse(ts: str):
                for fmt in ("%Y-%m-%d %H:%M:%S.%f %Z", "%Y-%m-%dT%H:%M:%S.%fZ",
                            "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
                    try: return datetime.strptime(ts[:26], fmt[:len(ts)])
                    except: continue
                return None
            t0 = _parse(oldest)
            if not t0:
                return {}
            age_days = (now - t0).days
            if age_days < _COLD_PARTITION_DAYS:
                return {"oldest_data_age_days": age_days, "cold_risk": "none",
                        "source": "heuristic_snapshot_age"}
            cold_frac = max(0, (age_days - _COLD_PARTITION_DAYS)) / max(age_days, 1)
            cold_gb   = round(live_gb * cold_frac, 2)
            return _build_cold_result(
                cold_gb, None, None, age_days, live_gb, now,
                "heuristic_snapshot_age"
            )
        except Exception:
            return {}

    def _glue_cold_partitions(self, database: str, table_name: str, live_gb: float) -> Dict:
        """Use Glue get_partitions CreationTime for Hive-style table cold detection."""
        if not database or not table_name:
            return {}
        try:
            import boto3
            glue   = boto3.client("glue")
            paginator = glue.get_paginator("get_partitions")
            cutoff = datetime.utcnow()
            cold_count = 0
            total_count = 0
            oldest_days = 0
            for page in paginator.paginate(DatabaseName=database, TableName=table_name):
                for part in page.get("Partitions", []):
                    total_count += 1
                    created = part.get("CreationTime")
                    if created:
                        age = (cutoff - created.replace(tzinfo=None)).days
                        if age > _COLD_PARTITION_DAYS:
                            cold_count += 1
                            oldest_days = max(oldest_days, age)
            if total_count == 0:
                return {}
            cold_gb = round(live_gb * cold_count / total_count, 2)
            return _build_cold_result(
                cold_gb, cold_count, total_count, oldest_days,
                live_gb, cutoff, "glue_partition_creation_time"
            )
        except Exception:
            return {}

    # ── Enhancement 10: Cross-table redundancy ────────────────────────────────

    def _detect_cross_table_redundancy(self, tables: List[Dict]) -> Dict:
        duplicate_pairs: List[Dict] = []
        for i, ta in enumerate(tables):
            cols_a = self._col_set(ta)
            if not cols_a:
                continue
            for tb in tables[i + 1:]:
                cols_b = self._col_set(tb)
                if not cols_b:
                    continue
                shared   = cols_a & cols_b
                overlap  = len(shared) / max(len(cols_a | cols_b), 1)
                rec_sim  = self._record_count_similar(ta, tb)
                if overlap >= _REDUNDANCY_THRESHOLD and rec_sim:
                    gb_a = ta.get("compressed_size_gb", 0)
                    gb_b = tb.get("compressed_size_gb", 0)
                    dup_pair = {
                        "table_a":          ta["table"],
                        "table_b":          tb["table"],
                        "column_overlap":   round(overlap, 2),
                        "shared_columns":   sorted(shared)[:10],
                        "combined_gb":      round(gb_a + gb_b, 2),
                        "redundant_gb":     round(min(gb_a, gb_b), 2),
                        "monthly_waste_usd":round(min(gb_a, gb_b) * _S3_PRICE_PER_GB_MONTH, 2),
                        "recommendation":   (
                            f"Tables '{ta['table']}' and '{tb['table']}' share "
                            f"{overlap:.0%} column overlap with similar record counts. "
                            f"Consolidate to one canonical source to save ~"
                            f"${min(gb_a, gb_b) * _S3_PRICE_PER_GB_MONTH:.2f}/month."
                        ),
                    }
                    duplicate_pairs.append(dup_pair)

        total_waste_gb  = sum(p["redundant_gb"] for p in duplicate_pairs)
        total_waste_usd = sum(p["monthly_waste_usd"] for p in duplicate_pairs)
        return {
            "duplicate_pairs":          duplicate_pairs,
            "total_redundant_gb":       round(total_waste_gb, 2),
            "total_monthly_waste_usd":  round(total_waste_usd, 2),
        }

    def _col_set(self, table_analysis: Dict) -> set:
        """Extract column name set from various metadata shapes."""
        raw = table_analysis
        cols = raw.get("columns", raw.get("column_names", []))
        if isinstance(cols, list) and cols and isinstance(cols[0], dict):
            return {c.get("name", "") for c in cols if c.get("name")}
        if isinstance(cols, list) and cols and isinstance(cols[0], str):
            return set(cols)
        return set()

    def _record_count_similar(self, ta: Dict, tb: Dict) -> bool:
        rc_a = ta.get("record_count", 0)
        rc_b = tb.get("record_count", 0)
        if rc_a == 0 or rc_b == 0:
            return True  # can't rule out similarity
        ratio = max(rc_a, rc_b) / max(min(rc_a, rc_b), 1)
        return ratio < 5  # within 5× is "similar"

    # ── Aggregate storage cost across all tables ───────────────────────────────

    def _aggregate_storage_costs(self, tables: List[Dict]) -> Dict:
        total_live = total_dead = total_manifest = 0.0
        cold_saving = 0.0
        for t in tables:
            sc = t.get("storage_cost_breakdown", {})
            total_live     += sc.get("live_data_gb", t.get("compressed_size_gb", 0))
            total_dead     += sc.get("dead_snapshot_gb", 0)
            total_manifest += sc.get("manifest_metadata_gb", 0)
            cp = t.get("cold_partition_info", {})
            cold_saving    += cp.get("potential_saving_usd", 0)

        total_gb    = total_live + total_dead + total_manifest
        return {
            "live_data_gb":          round(total_live, 2),
            "dead_snapshot_gb":      round(total_dead, 2),
            "manifest_metadata_gb":  round(total_manifest, 3),
            "total_gb":              round(total_gb, 2),
            "live_monthly_usd":      round(total_live     * _S3_PRICE_PER_GB_MONTH, 2),
            "dead_monthly_usd":      round(total_dead     * _S3_PRICE_PER_GB_MONTH, 2),
            "total_monthly_usd":     round(total_gb       * _S3_PRICE_PER_GB_MONTH, 2),
            "cold_archival_saving_usd": round(cold_saving, 2),
            "waste_pct":             round(total_dead / max(total_gb, 0.001) * 100, 1),
        }

    # ── Skew assessment ────────────────────────────────────────────────────────

    def _assess_table_skew(self, tbl: Dict, fs: Dict) -> Tuple[str, str]:
        if tbl.get("has_skew", False):
            return "high", "Explicitly marked as skewed"
        skew_ratio = fs.get("skew_ratio", 1.0)
        if skew_ratio > _SKEW_CRITICAL:
            return "high",   f"Partition skew {skew_ratio:.1f}× from $partitions"
        if skew_ratio > _SKEW_WARN:
            return "medium", f"Partition skew {skew_ratio:.1f}× from $partitions"
        part_col = tbl.get("partition_column", "")
        if part_col.lower() in ("date", "dt", "process_date", "event_date"):
            return "medium", "Date partition may be uneven"
        if tbl.get("record_count", 0) < 100_000:
            return "low", "Small table"
        name = tbl.get("table", "").lower()
        if any(x in name for x in ("transaction", "event", "log", "click", "fact")):
            return "medium", "Transactional table may have temporal skew"
        return "low", ""

    # ── Delta / join amplification ─────────────────────────────────────────────

    def _estimate_delta_ratio(self, input_data: AnalysisInput) -> float:
        if "delta_ratio" in input_data.additional_context:
            return float(input_data.additional_context["delta_ratio"])
        schedule = input_data.additional_context.get("schedule", "daily")
        return {"hourly": 0.005, "daily": 0.03, "weekly": 0.15}.get(schedule, 0.05)

    def _estimate_join_amplification(self, input_data: AnalysisInput, context: Dict) -> float:
        join_count = context.get("join_count", 0)
        if join_count == 0:
            join_count = len(re.findall(r"\.join\(", input_data.script_content, re.IGNORECASE))
        if join_count == 0:   return 1.0
        if join_count <= 3:   return 1.0 + join_count * 0.10
        if join_count <= 10:  return 1.3 + (join_count - 3) * 0.05
        return                1.5 + (join_count - 10) * 0.02

    # ── Partition efficiency ───────────────────────────────────────────────────

    def _analyze_partitions(self, tables: List[Dict]) -> Dict:
        issues: List[str] = []
        partitioned = 0
        for t in tables:
            if t.get("partition_column"):
                partitioned += 1
            elif t.get("record_count", 0) > 1_000_000:
                issues.append(
                    f"Large table {t.get('table','?')} has no partition — full scans on every read."
                )
        total = max(len(tables), 1)
        return {
            "score":                  int(partitioned / total * 100),
            "recommendations":        issues[:3],
            "tables_with_partitions": partitioned,
            "total_tables":           total,
        }

    def _calculate_skew_score(self, skew_risks: List, input_data: AnalysisInput) -> int:
        if not skew_risks:
            return 10
        highs   = sum(1 for r in skew_risks if r["risk"] == "high")
        mediums = sum(1 for r in skew_risks if r["risk"] == "medium")
        return min(100, 10 + highs * 30 + mediums * 15)

    def _calculate_confidence(self, input_data: AnalysisInput) -> str:
        have = sum(
            1 for t in input_data.source_tables
            if t.get("size_gb") or t.get("record_count", 0) > 0 or t.get("iceberg_stats")
        )
        ratio = have / max(len(input_data.source_tables), 1)
        if ratio > 0.8: return "high"
        if ratio > 0.5: return "medium"
        return "low"

    def _aggregate_iceberg_health(self, tables: List[Dict]) -> Dict:
        iceberg = [t for t in tables if t.get("is_iceberg")]
        if not iceberg:
            return {"has_iceberg": False}
        all_issues: List[str] = []
        total_snaps = total_small = total_files = total_mani = 0
        for t in iceberg:
            all_issues.extend(t.get("iceberg_issues", []))
            fs          = t.get("file_stats", {})
            total_snaps += fs.get("snapshot_count", 0)
            total_small += fs.get("small_count", 0)
            total_files += fs.get("file_count", 0)
            total_mani  += fs.get("manifest_count", 0)
        return {
            "has_iceberg":         True,
            "iceberg_tables":      len(iceberg),
            "total_snapshots":     total_snaps,
            "total_files":         total_files,
            "total_small_files":   total_small,
            "total_manifests":     total_mani,
            "issues":              all_issues,
            "health": (
                "critical" if any("CRITICAL" in i for i in all_issues) else
                "warning"  if all_issues else "healthy"
            ),
        }

    # ── Recommendations ────────────────────────────────────────────────────────

    def _generate_recommendations(
        self, analysis: Dict, tables: List[Dict],
    ) -> List[Dict]:
        recs: List[Dict] = []
        eff_gb  = analysis["effective_size_gb"]
        comp_gb = analysis["total_compressed_size_gb"]

        for t in tables:
            tname = t["table"]
            fs    = t.get("file_stats", {})
            sc    = t.get("storage_cost_breakdown", {})
            gr    = t.get("growth_forecast", {})
            ch    = t.get("compression_health", {})
            cp    = t.get("cold_partition_info", {})
            oe    = t.get("orphan_file_estimate", {})
            snap  = fs.get("snapshot_count", 0)
            avg_mb= fs.get("avg_mb", 0.0)
            fc    = fs.get("file_count", 0)
            wa    = fs.get("write_amp", 1.0)
            skew  = fs.get("skew_ratio", 1.0)
            mc    = fs.get("manifest_count", 0)
            is_ice= t.get("is_iceberg", False)

            # ── Snapshot bloat ────────────────────────────────────────────────
            if is_ice and snap > _SNAPSHOT_CRITICAL:
                recs.append({
                    "priority": "P0", "category": "iceberg",
                    "title": f"[{tname}] CRITICAL: {snap} snapshots — expire now",
                    "description": (
                        f"{snap} snapshots is causing measurable planning latency. "
                        f"Estimated dead storage: {sc.get('dead_snapshot_gb',0):.1f} GB "
                        f"(${sc.get('dead_monthly_usd',0):.2f}/month wasted)."
                    ),
                    "implementation": (
                        f"CALL system.expire_snapshots("
                        f"table => '{tname}', "
                        f"older_than => TIMESTAMP 'now - 3 days', retain_last => 5);"
                    ),
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$snapshots",
                })
            elif is_ice and snap > _SNAPSHOT_WARN:
                recs.append({
                    "priority": "P1", "category": "iceberg",
                    "title": f"[{tname}] {snap} snapshots — schedule expire_snapshots",
                    "description": f"Retain ≤5. Dead storage: {sc.get('dead_snapshot_gb',0):.1f} GB.",
                    "implementation": (
                        f"CALL system.expire_snapshots("
                        f"table => '{tname}', "
                        f"older_than => TIMESTAMP 'now - 7 days', retain_last => 5);"
                    ),
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$snapshots",
                })

            # ── Manifest explosion ────────────────────────────────────────────
            if is_ice and mc > _MANIFEST_CRITICAL:
                recs.append({
                    "priority": "P0", "category": "iceberg",
                    "title": f"[{tname}] {mc:,} manifests — rewrite_manifests URGENT",
                    "description": (
                        f"Iceberg reads all {mc:,} manifest files at query-plan time. "
                        "Queries will have 10–60s planning overhead or time out."
                    ),
                    "implementation": (
                        f"CALL system.rewrite_manifests(table => '{tname}');"
                    ),
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$manifests",
                })
            elif is_ice and mc > _MANIFEST_WARN:
                recs.append({
                    "priority": "P1", "category": "iceberg",
                    "title": f"[{tname}] {mc:,} manifests — schedule weekly rewrite",
                    "implementation": (
                        f"CALL system.rewrite_manifests(table => '{tname}');"
                    ),
                    "description": "High manifest count slows query planning.",
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$manifests",
                })

            # ── Small / oversized files ───────────────────────────────────────
            if avg_mb > 0 and avg_mb < _SMALL_FILE_MB and fc >= 10:
                pri = "P0" if avg_mb < _TINY_FILE_MB else "P1"
                recs.append({
                    "priority": pri, "category": "iceberg" if is_ice else "small_files",
                    "title": f"[{tname}] Compact: avg {avg_mb:.1f} MB × {fc:,} files",
                    "description": (
                        f"{'$files (current snapshot)' if is_ice else 'S3 listing'} shows "
                        f"avg {avg_mb:.1f} MB. Target {_IDEAL_FILE_MB} MB."
                    ),
                    "implementation": (
                        f"CALL system.rewrite_data_files(table => '{tname}', "
                        f"options => map('target-file-size-bytes','{_IDEAL_FILE_MB*1024*1024}'));"
                        if is_ice else
                        f"df.repartition({max(1,int(t['compressed_size_gb']*4))}).write.parquet(path)"
                    ),
                    "estimated_savings_percent": 20,
                    "source": "iceberg_$files" if is_ice else "s3_listing",
                })

            # ── Write amplification → auto-compaction ─────────────────────────
            if is_ice and wa > _WRITE_AMP_WARN:
                recs.append({
                    "priority": "P1", "category": "iceberg",
                    "title": f"[{tname}] Write amplification {wa:.1f}× — enable auto-compaction",
                    "description": (
                        f"{int(wa * fc):,} files added historically vs {fc:,} current. "
                        f"Dead storage cost: ${sc.get('dead_monthly_usd',0):.2f}/month."
                    ),
                    "implementation": (
                        "ALTER TABLE " + tname +
                        " SET TBLPROPERTIES ('write.auto-optimize.enabled'='true',"
                        " 'write.auto-optimize.every-n-commits'='10');"
                    ),
                    "estimated_savings_percent": 15,
                    "source": "iceberg_$files",
                })

            # ── Partition skew ────────────────────────────────────────────────
            if skew > _SKEW_CRITICAL:
                recs.append({
                    "priority": "P0", "category": "iceberg",
                    "title": f"[{tname}] Severe partition skew {skew:.1f}×",
                    "description": "Straggler tasks will hold up every stage this table is in.",
                    "implementation": (
                        "1. Enable AQE: spark.sql.adaptive.skewJoin.enabled=true\n"
                        "2. Salt join keys: df.withColumn('salt', (rand()*100).cast('int'))\n"
                        "3. Consider finer partition granularity (month → day)"
                    ),
                    "estimated_savings_percent": 30,
                    "source": "iceberg_$partitions",
                })
            elif skew > _SKEW_WARN:
                recs.append({
                    "priority": "P1", "category": "iceberg",
                    "title": f"[{tname}] Partition skew {skew:.1f}×",
                    "description": "Enable AQE skewJoin to handle straggler partitions.",
                    "implementation": (
                        "spark.conf.set('spark.sql.adaptive.skewJoin.enabled','true')\n"
                        "spark.conf.set('spark.sql.adaptive.skewJoin.skewedPartitionFactor','3')"
                    ),
                    "estimated_savings_percent": 15,
                    "source": "iceberg_$partitions",
                })

            # ── Growth rate forecast ──────────────────────────────────────────
            if gr.get("days_to_2x") and gr["days_to_2x"] < 90:
                recs.append({
                    "priority": "P1", "category": "capacity",
                    "title": (
                        f"[{tname}] Table doubles in {gr['days_to_2x']} days "
                        f"(+{gr['gb_per_day']:.2f} GB/day)"
                    ),
                    "description": (
                        f"At current rate, size grows from "
                        f"{t.get('compressed_size_gb',0):.1f} GB → "
                        f"{gr.get('projected_90d_gb',0):.1f} GB in 90 days. "
                        f"Storage cost: ${gr.get('monthly_cost_now_usd',0):.2f} → "
                        f"${gr.get('monthly_cost_90d_usd',0):.2f}/month."
                    ),
                    "implementation": (
                        "Plan infrastructure migration before threshold. "
                        "Consider partition-level archival for old data. "
                        "Enable incremental processing to limit full-scan growth."
                    ),
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$snapshots",
                })

            # ── Compression issues ────────────────────────────────────────────
            if ch.get("label") == "poor":
                recs.append({
                    "priority": "P2", "category": "storage",
                    "title": f"[{tname}] Poor compression ratio ({ch['ratio']:.2f})",
                    "description": ch.get("note", ""),
                    "implementation": (
                        "1. Identify columns with low cardinality and enable dictionary encoding.\n"
                        "2. Normalize large string/JSON columns into separate tables.\n"
                        "3. For binary-heavy data, consider Avro format."
                    ),
                    "estimated_savings_percent": 25,
                    "source": "compression_analysis",
                })

            # ── Cold partition archival ───────────────────────────────────────
            if cp.get("potential_saving_usd", 0) > 5:
                recs.append({
                    "priority": "P2", "category": "storage",
                    "title": (
                        f"[{tname}] Archive {cp.get('estimated_cold_gb',0):.1f} GB "
                        f"of cold data to Glacier (save ${cp['potential_saving_usd']:.2f}/month)"
                    ),
                    "description": (
                        f"Data older than {_COLD_PARTITION_DAYS} days likely unread. "
                        f"S3 Standard: ${cp.get('s3_monthly_usd',0):.2f}/month → "
                        f"Glacier Deep Archive: ${cp.get('glacier_monthly_usd',0):.2f}/month."
                    ),
                    "implementation": (
                        "aws s3 cp s3://bucket/table/year=2023/ "
                        "s3://bucket-glacier/table/year=2023/ "
                        "--storage-class DEEP_ARCHIVE --recursive"
                    ),
                    "estimated_savings_percent": int(
                        (1 - 0.004 / _S3_PRICE_PER_GB_MONTH) * 100
                    ),
                    "source": "cold_partition_analysis",
                })

            # ── Orphan files ──────────────────────────────────────────────────
            if oe.get("risk") == "high" and oe.get("estimated_orphan_gb", 0) > 0.5:
                recs.append({
                    "priority": "P1", "category": "storage",
                    "title": (
                        f"[{tname}] Likely orphan files: ~{oe['estimated_orphan_files']:,} "
                        f"(~{oe['estimated_orphan_gb']:.1f} GB, "
                        f"${oe['estimated_monthly_cost']:.2f}/month)"
                    ),
                    "description": (
                        "Files physically present in S3 but not referenced by any "
                        "snapshot manifest — paying storage for unreachable data."
                    ),
                    "implementation": oe.get("recommendation", ""),
                    "estimated_savings_percent": 0,
                    "source": "orphan_estimation",
                })

        # ── Cross-table redundancy ─────────────────────────────────────────────
        cr = analysis.get("cross_table_redundancy", {})
        for pair in cr.get("duplicate_pairs", []):
            recs.append({
                "priority": "P2", "category": "redundancy",
                "title": (
                    f"Possible duplicate: {pair['table_a']} ↔ {pair['table_b']} "
                    f"({pair['column_overlap']:.0%} column overlap)"
                ),
                "description": pair.get("recommendation", ""),
                "implementation": (
                    f"Audit both tables. If confirmed duplicate, consolidate to one "
                    f"canonical source and update all downstream jobs."
                ),
                "estimated_savings_percent": 0,
                "source": "cross_table_redundancy",
            })

        # ── Platform scale-out ────────────────────────────────────────────────
        if eff_gb > _XLARGE_TABLE_GB:
            recs.append({
                "priority": "P1", "category": "architecture",
                "title": f"Consider EKS + Karpenter ({eff_gb:.0f} GB effective)",
                "description": (
                    "EKS with Karpenter Spot can cut compute cost 60-70% vs Glue."
                ),
                "implementation": "Migrate to EMR on EKS or self-managed Spark on EKS Spot fleet.",
                "estimated_savings_percent": 65,
                "source": "size_analysis",
            })
        elif eff_gb > _LARGE_TABLE_GB:
            recs.append({
                "priority": "P2", "category": "architecture",
                "title": f"Consider EMR Spot ({eff_gb:.0f} GB)",
                "description": "EMR Spot can reduce cost 40% for this data volume.",
                "implementation": "Evaluate EMR Serverless or EMR on Spot fleet.",
                "estimated_savings_percent": 40,
                "source": "size_analysis",
            })

        # ── Skew global ───────────────────────────────────────────────────────
        if analysis["skew_risk_score"] > 50:
            recs.append({
                "priority": "P1", "category": "code",
                "title": f"Address data skew (score {analysis['skew_risk_score']}/100)",
                "description": "High skew risk — straggler tasks will extend job duration.",
                "implementation": (
                    "1. AQE: spark.sql.adaptive.skewJoin.enabled=true\n"
                    "2. Salt high-cardinality join keys.\n"
                    "3. /*+ SKEW('table','col') */ query hint."
                ),
                "estimated_savings_percent": 20,
                "source": "skew_analysis",
            })

        # ── Missing partitions ────────────────────────────────────────────────
        if analysis["partition_efficiency_score"] < 50:
            recs.append({
                "priority": "P2", "category": "architecture",
                "title": "Add partitioning to large tables",
                "description": "Large tables without partition columns cause full table scans.",
                "implementation": "ALTER TABLE t ADD PARTITION FIELD date_col;",
                "estimated_savings_percent": 15,
                "source": "partition_analysis",
            })

        # ── Full→delta switch ─────────────────────────────────────────────────
        if analysis["processing_mode"] == "full" and comp_gb > 50:
            recs.append({
                "priority": "P0", "category": "architecture",
                "title": f"Switch to incremental processing ({comp_gb:.0f} GB full scan)",
                "description": "Full-table processing. Incremental can reduce volume by 90-97%.",
                "implementation": (
                    "For Iceberg: read only new snapshots using snapshot_id watermark.\n"
                    "SELECT * FROM t FOR SYSTEM_TIME AS OF @watermark_ts"
                ),
                "estimated_savings_percent": 90,
                "source": "size_analysis",
            })

        # ── Broadcast candidates ──────────────────────────────────────────────
        candidates = [t["table"] for t in tables if t.get("is_broadcast_candidate")]
        if candidates:
            recs.append({
                "priority": "P1", "category": "code",
                "title": f"Broadcast small tables: {', '.join(candidates[:5])}",
                "description": "Eliminates shuffle join for these small dimension tables.",
                "implementation": (
                    "from pyspark.sql.functions import broadcast\n"
                    "df.join(broadcast(small_df), 'key')"
                ),
                "estimated_savings_percent": 10,
                "source": "size_analysis",
            })

        # ── Wide table column pruning ─────────────────────────────────────────
        wide = [t["table"] for t in tables if t.get("column_width") == "very_wide"]
        if wide:
            recs.append({
                "priority": "P2", "category": "code",
                "title": f"Prune columns early: {', '.join(wide[:3])} (100+ cols)",
                "description": (
                    "Very wide tables have high deserialization cost per task "
                    "even when selecting few columns."
                ),
                "implementation": "df = df.select('col1', 'col2', ...)",
                "estimated_savings_percent": 10,
                "source": "size_analysis",
            })

        return recs
