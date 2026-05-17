"""
validation/csv_exporter.py
===========================
Writes all pipeline outputs to dated CSV flat files.

Four CSV files per run
-----------------------
1. audit_findings_<date>.csv       — per-record AI classification + action
2. profile_anomalies_<date>.csv    — per-table data profiling anomalies
3. nl_validations_<date>.csv       — natural language validation verdicts
4. token_usage_<date>.csv          — per-call token + cost breakdown
   + prints a grand total line at the end

All files land in the directory specified by output.csv_directory in the
config (default: output/).  They can be COPY'd / loaded directly into
any analytics table.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger("strands.csv_exporter")


# ---------------------------------------------------------------------------
# Column definitions  (order matters — this becomes the CSV header)
# ---------------------------------------------------------------------------

AUDIT_COLS = [
    "run_date", "table_name", "record_id", "rule_name", "column_name",
    "severity", "rule_type", "failure_rate_pct",
    "failed_value", "expected_constraint",
    "classification", "confidence", "confidence_label",
    "recommended_action", "final_action", "action_success",
    "root_causes", "suggested_next_steps", "validation_sql",
    "similar_failure_count", "explanation",
    "input_tokens", "output_tokens", "cost_usd",
    "failure_timestamp", "dispatched_at",
]

PROFILE_COLS = [
    "run_date", "table_name", "column_name",
    "anomaly_type", "severity",
    "description", "observed_value", "expected_range",
    "z_score", "detection_sql",
    "recommended_action", "ai_explanation",
    "data_health_score", "schema_drift_detected",
    "detected_at",
]

NL_COLS = [
    "run_date", "table", "nl_query", "sql_generated",
    "verdict", "confidence", "row_count", "severity",
    "explanation", "recommended_action", "sample_evidence",
    "input_tokens", "output_tokens", "cost_usd", "run_at",
]

TOKEN_COLS = [
    "run_date", "agent_type", "table_name",
    "input_tokens", "output_tokens", "total_tokens",
    "cost_usd", "timestamp",
]


# ---------------------------------------------------------------------------
# CSVExporter
# ---------------------------------------------------------------------------

class CSVExporter:

    def __init__(self, output_dir: str = "output", run_date: str = ""):
        self.run_date   = run_date or datetime.utcnow().strftime("%Y-%m-%d")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._paths = {
            "audit":   self.output_dir / f"audit_findings_{self.run_date}.csv",
            "profile": self.output_dir / f"profile_anomalies_{self.run_date}.csv",
            "nl":      self.output_dir / f"nl_validations_{self.run_date}.csv",
            "tokens":  self.output_dir / f"token_usage_{self.run_date}.csv",
        }
        self._handles: Dict[str, Any] = {}
        self._writers: Dict[str, csv.DictWriter] = {}

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        col_map = {
            "audit":   AUDIT_COLS,
            "profile": PROFILE_COLS,
            "nl":      NL_COLS,
            "tokens":  TOKEN_COLS,
        }
        for key, path in self._paths.items():
            fh = open(path, "w", newline="", encoding="utf-8")
            self._handles[key] = fh
            writer = csv.DictWriter(fh, fieldnames=col_map[key], extrasaction="ignore")
            writer.writeheader()
            self._writers[key] = writer
        return self

    def __exit__(self, *_):
        for fh in self._handles.values():
            fh.close()
        self._print_file_summary()

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------

    def write_audit_finding(
        self,
        record_dict: Dict[str, Any],
        analysis_dict: Dict[str, Any],
        dispatch_dict: Dict[str, Any],
        token_in: int = 0,
        token_out: int = 0,
    ) -> None:
        cost = round(token_in * 3e-6 + token_out * 15e-6, 6)
        total  = record_dict.get("total_row_count", 0)
        failed = record_dict.get("row_count_failed", 1)
        rate   = failed / total if total else 0.0

        row = {
            "run_date":             self.run_date,
            "table_name":           record_dict.get("table_name", ""),
            "record_id":            record_dict.get("record_id", ""),
            "rule_name":            record_dict.get("rule_name", ""),
            "column_name":          record_dict.get("column_name", ""),
            "severity":             record_dict.get("severity", ""),
            "rule_type":            record_dict.get("rule_type", ""),
            "failure_rate_pct":     round(rate * 100, 2),
            "failed_value":         str(record_dict.get("failed_value", ""))[:200],
            "expected_constraint":  str(record_dict.get("expected_constraint", ""))[:200],
            "classification":       analysis_dict.get("classification", ""),
            "confidence":           analysis_dict.get("confidence", ""),
            "confidence_label":     _confidence_label(float(analysis_dict.get("confidence", 0))),
            "recommended_action":   analysis_dict.get("recommended_action", ""),
            "final_action":         dispatch_dict.get("action_taken", ""),
            "action_success":       dispatch_dict.get("success", ""),
            "root_causes":          " | ".join(analysis_dict.get("root_causes", []))[:300],
            "suggested_next_steps": " | ".join(analysis_dict.get("suggested_next_steps", []))[:300],
            "validation_sql":       str(analysis_dict.get("validation_sql", ""))[:400],
            "similar_failure_count":len(analysis_dict.get("similar_historical_failures", [])),
            "explanation":          str(analysis_dict.get("explanation", ""))[:500],
            "input_tokens":         token_in,
            "output_tokens":        token_out,
            "cost_usd":             cost,
            "failure_timestamp":    record_dict.get("failure_timestamp", ""),
            "dispatched_at":        dispatch_dict.get("dispatched_at", ""),
        }
        self._writers["audit"].writerow(row)
        self._handles["audit"].flush()

    def write_profile_anomaly(
        self,
        table_name: str,
        anomaly_dict: Dict[str, Any],
        health_score: int,
        schema_drift: bool,
    ) -> None:
        row = {
            "run_date":              self.run_date,
            "table_name":            table_name,
            "column_name":           anomaly_dict.get("column_name", ""),
            "anomaly_type":          anomaly_dict.get("anomaly_type", ""),
            "severity":              anomaly_dict.get("severity", ""),
            "description":           str(anomaly_dict.get("description", ""))[:400],
            "observed_value":        str(anomaly_dict.get("observed_value", ""))[:200],
            "expected_range":        str(anomaly_dict.get("expected_range", ""))[:200],
            "z_score":               anomaly_dict.get("z_score", ""),
            "detection_sql":         str(anomaly_dict.get("detection_sql", ""))[:400],
            "recommended_action":    str(anomaly_dict.get("recommended_action", ""))[:300],
            "ai_explanation":        str(anomaly_dict.get("ai_explanation", ""))[:400],
            "data_health_score":     health_score,
            "schema_drift_detected": schema_drift,
            "detected_at":           anomaly_dict.get("detected_at", ""),
        }
        self._writers["profile"].writerow(row)
        self._handles["profile"].flush()

    def write_nl_finding(self, finding_dict: Dict[str, Any]) -> None:
        row = {"run_date": self.run_date, **finding_dict}
        self._writers["nl"].writerow(row)
        self._handles["nl"].flush()

    def write_token_usage(self, usage_dict: Dict[str, Any]) -> None:
        row = {"run_date": self.run_date, **usage_dict}
        self._writers["tokens"].writerow(row)
        self._handles["tokens"].flush()

    def write_all_token_usages(self, usages: List[Dict[str, Any]]) -> None:
        for u in usages:
            self.write_token_usage(u)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _print_file_summary(self) -> None:
        print()
        print("\033[96m╔" + "═" * 68 + "╗\033[0m")
        print("\033[96m║\033[0m\033[1m" + "  📄  CSV OUTPUT FILES".center(68) + "\033[0m\033[96m║\033[0m")
        print("\033[96m╠" + "═" * 68 + "╣\033[0m")
        for key, path in self._paths.items():
            size = path.stat().st_size if path.exists() else 0
            rows = self._count_lines(path) - 1  # subtract header
            print(f"\033[96m║\033[0m  {str(path):<44}  {rows:>4} rows  {size:>7,}B  \033[96m║\033[0m")
        print("\033[96m╚" + "═" * 68 + "╝\033[0m")
        print()

    @staticmethod
    def _count_lines(path: Path) -> int:
        try:
            with open(path, encoding="utf-8") as f:
                return sum(1 for _ in f)
        except Exception:
            return 0

    @property
    def paths(self) -> Dict[str, Path]:
        return self._paths


def _confidence_label(confidence: float) -> str:
    if confidence >= 0.85:
        return "HIGH"
    if confidence >= 0.60:
        return "MEDIUM"
    return "LOW"
