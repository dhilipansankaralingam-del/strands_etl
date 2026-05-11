"""
Data Quality Agent
==================
Strands SDK agent that profiles Iceberg tables via Athena and scores them
on six scientific dimensions:

  1. PK Uniqueness    — duplicates & null violations  (pk_uniqueness_health)
  2. Completeness     — weighted null-rate score       (completeness_weighted_score)
  3. Benford's Law    — first-digit anomaly detection  (benford_law_test)
  4. Distribution Drift — JSD between snapshots       (jensen_shannon_drift)
  5. Outlier Density  — Z-score + IQR fence            (zscore_outlier_score)
  6. Overall DQ Grade — A–F composite

The agent runs in agentic mode: Claude calls `compute_pk_health`,
`compute_benford_score`, `compute_distribution_drift`, and
`compute_column_completeness` as needed based on the profiling data
provided in the prompt.

If `profile` data is missing (no --primary-keys passed), the agent falls
back to a lightweight rule-based analysis from `iceberg_stats` alone.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from .base import (
    AnalysisInput,
    AnalysisResult,
    CostOptimizerAgent,
)
from .pipeline_tools import DATA_QUALITY_AGENT_TOOLS

_log = logging.getLogger("strands_optimizer.dq")


class DataQualityAgent(CostOptimizerAgent):
    """Data Quality Agent — profiles Iceberg tables and grades DQ health."""

    AGENT_NAME     = "data_quality"
    MAX_ITERATIONS = 6
    AGENT_TOOLS    = DATA_QUALITY_AGENT_TOOLS

    # ─────────────────────────────────────────────────────────────────────────
    # Rule-based fallback (no LLM)
    # ─────────────────────────────────────────────────────────────────────────

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """
        Lightweight DQ check using only iceberg_stats (no Athena profiling).
        Flags small-file ratio and snapshot bloat as DQ-adjacent issues.
        """
        findings:        List[Dict] = []
        recommendations: List[Dict] = []

        for tbl in input_data.source_tables:
            s     = tbl.get("iceberg_stats", {})
            name  = f"{tbl.get('database','?')}.{tbl.get('table','?')}"

            fc      = s.get("file_cnt", 0)
            tiny    = s.get("tiny_file_cnt", 0)
            snaps   = s.get("snapshot_count", 0)
            skew    = s.get("skew_ratio", 1.0)
            profile = tbl.get("profile", {})

            issues: List[str] = []
            if fc and tiny / max(fc, 1) > 0.2:
                issues.append(f"{tiny}/{fc} tiny files (<10 MB) — Spark task overhead")
            if snaps > 100:
                issues.append(f"{snaps} snapshots — run expire_snapshots")
            if skew and skew > 10:
                issues.append(f"Partition skew {skew:.1f}× — uneven data distribution")

            # Profile-based checks (if --primary-keys was passed)
            pk_grade  = None
            comp_grade = None
            if profile:
                pk_dup = profile.get("pk_duplicate_count", 0)
                if pk_dup and pk_dup > 0:
                    issues.append(f"{pk_dup:,} PK duplicates detected")
                    pk_grade = "F"
                else:
                    pk_grade = "A"

                null_cols = [
                    c for c in profile.get("columns", [])
                    if c.get("null_pct", 0) > 10
                ]
                if null_cols:
                    names = ", ".join(c["column"] for c in null_cols[:3])
                    issues.append(f"High-null columns: {names}")
                    comp_grade = "C" if len(null_cols) <= 3 else "D"

            severity = "warning" if issues else "ok"
            findings.append({
                "table":     name,
                "issues":    issues,
                "severity":  severity,
                "pk_grade":  pk_grade,
                "completeness_grade": comp_grade,
            })

            if issues:
                recommendations.append({
                    "priority":    "P1",
                    "category":    "data_quality",
                    "title":       f"DQ issues in {name}",
                    "description": "; ".join(issues),
                    "quick_win":   any("snapshot" in i for i in issues),
                })

        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis={
                "findings":       findings,
                "tables_checked": len(input_data.source_tables),
                "issues_found":   sum(1 for f in findings if f["issues"]),
            },
            recommendations=recommendations,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # LLM prompt builder
    # ─────────────────────────────────────────────────────────────────────────

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        tables_info: List[Dict] = []

        for tbl in input_data.source_tables:
            s       = tbl.get("iceberg_stats", {})
            profile = tbl.get("profile", {})
            name    = f"{tbl.get('database', '?')}.{tbl.get('table', '?')}"

            entry: Dict[str, Any] = {
                "table":            name,
                "size_gb":          tbl.get("size_gb", 0),
                "record_count":     tbl.get("record_count", 0),
                # From $files
                "file_count":       s.get("file_cnt", 0),
                "tiny_file_count":  s.get("tiny_file_cnt", 0),
                "small_file_count": s.get("small_file_cnt", 0),
                "avg_file_mb":      s.get("avg_file_size_mb", 0),
                # From $snapshots
                "snapshot_count":   s.get("snapshot_count", 0),
                "oldest_snapshot":  s.get("oldest_snapshot_ts", ""),
                "newest_snapshot":  s.get("newest_snapshot_ts", ""),
                # From $partitions
                "skew_ratio":       s.get("skew_ratio", 1.0),
                "partition_count":  s.get("partition_count", 0),
            }

            if profile:
                entry["profile"] = {
                    "pk_columns":          profile.get("pk_columns", []),
                    "row_count":           profile.get("row_count", 0),
                    "pk_duplicate_count":  profile.get("pk_duplicate_count", 0),
                    "pk_null_count":       profile.get("pk_null_count", 0),
                    "pk_distinct_count":   profile.get("pk_distinct_count", 0),
                    "columns":             profile.get("columns", []),
                    "benford_digits":      profile.get("benford_digits", {}),
                }

            tables_info.append(entry)

        return f"""
You are a Data Quality engineer analysing Iceberg tables in AWS Athena.

## Tables
```json
{json.dumps(tables_info, indent=2, default=str)}
```

## Your mission
For EACH table, assess data quality across these dimensions and call the
appropriate tools to compute scientific scores:

1. **PK Uniqueness** (if profile.pk_columns is non-empty):
   - Call `compute_pk_health` with: total_rows=profile.row_count,
     distinct_pk=profile.pk_distinct_count, null_pk=profile.pk_null_count,
     pk_columns=profile.pk_columns

2. **Completeness** (if profile.columns is non-empty):
   - Call `compute_column_completeness` with the column null rates.
   - Assign role="pk" to pk_columns, role="required" to NOT NULL columns,
     role="optional" to others.

3. **Benford's Law** (if profile.benford_digits has entries):
   - For each numeric column with digit_counts, call `compute_benford_score`.
   - Flag SUSPICIOUS or ANOMALOUS results.

4. **Iceberg metadata signals** (always available):
   - Snapshot bloat (snapshot_count > 50 → expire_snapshots needed)
   - Partition skew (skew_ratio > 5 → salting or re-partitioning needed)
   - Tiny files (tiny_file_count / file_count > 20% → compaction needed)

After calling all relevant tools, return a JSON object:
```json
{{
  "dq_report": [
    {{
      "table": "<db.table>",
      "pk_grade": "<A-F or null>",
      "completeness_grade": "<A-F or null>",
      "benford_interpretation": "<NORMAL|SUSPICIOUS|ANOMALOUS or null>",
      "iceberg_signals": ["<signal 1>", "..."],
      "overall_grade": "<A-F>",
      "recommendations": [
        {{
          "priority": "P0|P1|P2",
          "action": "<concrete fix>",
          "estimated_impact": "<description>"
        }}
      ]
    }}
  ],
  "summary": {{
    "tables_assessed": <n>,
    "critical_issues": <n>,
    "overall_health": "<HEALTHY|DEGRADED|CRITICAL>"
  }}
}}
```
Respond with ONLY valid JSON inside ```json ... ``` fences.
"""

    # ─────────────────────────────────────────────────────────────────────────
    # Response parser
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_llm_response(self, response) -> AnalysisResult:
        import re
        text = str(response)

        # Try to extract JSON
        m = re.search(r"```json\s*([\s\S]*?)```", text)
        if m:
            raw = m.group(1).strip()
        else:
            m2 = re.search(r"\{[\s\S]*\}", text)
            raw = m2.group() if m2 else text

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return AnalysisResult(
                agent_name=self.AGENT_NAME,
                success=True,
                analysis={"raw_response": text},
                recommendations=[],
            )

        report   = data.get("dq_report", [])
        summary  = data.get("summary", {})
        recs: List[Dict] = []
        for tbl_report in report:
            for r in tbl_report.get("recommendations", []):
                recs.append({
                    "priority":    r.get("priority", "P2"),
                    "category":    "data_quality",
                    "title":       f"[{tbl_report['table']}] {r.get('action', '')}",
                    "description": r.get("estimated_impact", ""),
                    "quick_win":   r.get("priority") == "P1",
                })

        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis={
                "dq_report":       report,
                "summary":         summary,
                "tables_assessed": summary.get("tables_assessed", len(report)),
                "critical_issues": summary.get("critical_issues", 0),
                "overall_health":  summary.get("overall_health", "UNKNOWN"),
            },
            recommendations=recs,
        )
