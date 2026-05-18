"""
validation/data_profiler.py
===========================
Strands SDK Agentic Data Profiler.

Builds a full statistical profile of a table — key columns, KPIs, and
anomaly detection — using Strands @tool-decorated functions and the Strands
Agent agentic loop backed by Amazon Bedrock.

Architecture
------------
Each profiling capability is exposed as a Strands @tool so the agent can
decide which checks to run and in what order based on what it discovers.
The agent orchestrates:

  profile_key_columns()       → null%, distinct%, top-values, type checks
  compute_kpis()              → row-count trend, growth rate, freshness
  detect_null_flood()         → EDGE: sudden null spike
  detect_cardinality_explosion() → EDGE: dimension column explodes
  detect_temporal_anomaly()   → EDGE: timestamps roll backwards
  detect_distribution_skew()  → EDGE: one value dominates
  check_schema_drift()        → EDGE: columns added/removed/retyped
  check_stale_partition()     → EDGE: data older than SLA
  detect_duplicate_key_storm() → EDGE: PK uniqueness collapse
  detect_boundary_explosion() → EDGE: numeric value far beyond historical max
  detect_zero_inflation()     → EDGE: metric zeros appear unexpectedly
  detect_encoding_rot()       → EDGE: garbled UTF-8 / control characters
  finalize_profile_report()   → synthesises everything into DataProfileResult

Usage
-----
    from validation.data_profiler import DataProfilerAgent

    profiler = DataProfilerAgent(
        database="my_db",
        learning_bucket="strands-etl-learning",
    )
    result = profiler.profile(
        table_name="policy_master",
        run_id="run-2024-07-01",
        key_columns=["policy_id", "effective_date", "premium_amount"],
        primary_key_column="policy_id",
        freshness_sla_hours=26.0,
    )
    print(result.data_health_score)
    for anomaly in result.critical_anomalies():
        print(anomaly.anomaly_type, anomaly.description)
"""

from __future__ import annotations

import json
import logging
import math
import re
import statistics
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from strands import Agent, tool

# ML libraries — optional; silently disabled if not installed
try:
    import numpy as np
    from sklearn.ensemble import IsolationForest
    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False

try:
    import pandas as pd
    from adtk.detector import LevelShiftAD, SeasonalAD, InterquartileRangeAD, PersistAD
    from adtk.data import validate_series
    _ADTK_AVAILABLE = True
except ImportError:
    _ADTK_AVAILABLE = False

from validation.models import (
    AnomalyReport,
    AnomalySeverity,
    AnomalyType,
    ColumnProfile,
    DataProfileResult,
    KPISnapshot,
)
from validation.prompts import (
    DATA_PROFILER_SYSTEM_PROMPT,
    build_column_anomaly_prompt,
    build_profiling_summary_prompt,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds (can be overridden per-table via constructor kwargs)
# ---------------------------------------------------------------------------
_DEFAULT_NULL_FLOOD_DELTA    = 0.20   # 20 pp jump in null% triggers flag
_DEFAULT_CARD_EXPLOSION_X    = 3.0    # distinct_count grows > 3×
_DEFAULT_SKEW_THRESHOLD      = 0.80   # one value ≥ 80% of rows
_DEFAULT_BOUNDARY_EXPLOSION_X = 10.0  # max > 10× historical max
_DEFAULT_ZERO_INFLATION_PCT  = 0.01   # 1% zeros in a must-be-nonzero column
_DEFAULT_DUP_KEY_PCT         = 0.005  # 0.5% duplicates in PK
_DEFAULT_FRESHNESS_SLA_HOURS = 26.0   # daily table SLA
_ZSCORE_ANOMALY_THRESHOLD    = 3.5    # |z| > 3.5 is an anomaly


# ===========================================================================
# DataProfilerAgent
# ===========================================================================

class DataProfilerAgent:
    """
    Strands SDK multi-tool agent for agentic table data profiling.

    The agent uses @tool functions to gather statistics from Athena and
    compares them against stored baselines in S3.  An LLM (Bedrock) drives
    the agentic loop — deciding which tools to call next and synthesising
    the final DataProfileResult.
    """

    BASELINE_PREFIX = "validation/profiling/baselines/"
    PROFILE_PREFIX  = "validation/profiling/results/"
    MODEL_ID        = "anthropic.claude-3-sonnet-20240229-v1:0"

    def __init__(
        self,
        database: str = "default",
        athena_output_location: str = "s3://strands-etl-athena-results/profiling/",
        learning_bucket: str = "strands-etl-learning",
        aws_region: str = "us-east-1",
        model_id: str = MODEL_ID,
        null_flood_delta: float = _DEFAULT_NULL_FLOOD_DELTA,
        cardinality_explosion_x: float = _DEFAULT_CARD_EXPLOSION_X,
        skew_threshold: float = _DEFAULT_SKEW_THRESHOLD,
        boundary_explosion_x: float = _DEFAULT_BOUNDARY_EXPLOSION_X,
        zero_inflation_pct: float = _DEFAULT_ZERO_INFLATION_PCT,
        dup_key_pct: float = _DEFAULT_DUP_KEY_PCT,
        freshness_sla_hours: float = _DEFAULT_FRESHNESS_SLA_HOURS,
        ml_config: Optional[Dict[str, Any]] = None,
    ):
        self.database              = database
        self.athena_output         = athena_output_location
        self.learning_bucket       = learning_bucket
        self.model_id              = model_id
        self.null_flood_delta      = null_flood_delta
        self.cardinality_explosion_x = cardinality_explosion_x
        self.skew_threshold        = skew_threshold
        self.boundary_explosion_x  = boundary_explosion_x
        self.zero_inflation_pct    = zero_inflation_pct
        self.dup_key_pct           = dup_key_pct
        self.freshness_sla_hours   = freshness_sla_hours

        self.athena  = boto3.client("athena",          region_name=aws_region)
        self.s3      = boto3.client("s3",              region_name=aws_region)
        self.bedrock = boto3.client("bedrock-runtime", region_name=aws_region)
        self.glue    = boto3.client("glue",            region_name=aws_region)

        # ML anomaly scoring config
        self.ml_config = ml_config or {}
        self._ml_enabled = self.ml_config.get("enabled", False)
        self._if_config  = self.ml_config.get("isolation_forest", {})
        self._adtk_config = self.ml_config.get("adtk", {})

        # State shared across tool calls within one profile() invocation
        self._state: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def profile(
        self,
        table_name: str,
        run_id: str,
        key_columns: Optional[List[str]] = None,
        primary_key_column: Optional[str] = None,
        metric_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        partition_column: Optional[str] = None,
        freshness_sla_hours: Optional[float] = None,
        profiling_columns: Optional[Dict[str, Any]] = None,
    ) -> DataProfileResult:
        """
        Run the full agentic profiling loop and return a DataProfileResult.
        """
        sla = freshness_sla_hours or self.freshness_sla_hours
        profile_id = str(uuid.uuid4())
        profiled_at = datetime.utcnow().isoformat()

        logger.info(f"[DataProfilerAgent] Starting profile for {self.database}.{table_name} run={run_id}")

        # ── 1. Gather raw stats (tools run in Python directly before agent loop)
        schema_info        = self._get_glue_schema(table_name)
        baseline           = self._load_baseline(table_name)
        column_profiles    = self._build_column_profiles(
            table_name, schema_info, baseline, key_columns, profiling_columns
        )
        kpi_snapshot       = self._build_kpi_snapshot(table_name, run_id, timestamp_column, partition_column, baseline)
        schema_drift       = self._detect_schema_drift(schema_info, baseline)

        # ── 2. Rule-based anomaly detection (edge cases)
        anomalies: List[AnomalyReport] = []
        anomalies += self._check_null_flood(table_name, run_id, column_profiles)
        anomalies += self._check_cardinality_explosion(table_name, run_id, column_profiles)
        anomalies += self._check_temporal_anomaly(table_name, run_id, column_profiles, timestamp_column, baseline)
        anomalies += self._check_distribution_skew(table_name, run_id, column_profiles)
        anomalies += self._check_schema_drift_anomalies(table_name, run_id, schema_drift)
        anomalies += self._check_stale_partition(table_name, run_id, kpi_snapshot, sla)
        anomalies += self._check_duplicate_key_storm(table_name, run_id, kpi_snapshot, primary_key_column)
        anomalies += self._check_boundary_explosion(table_name, run_id, column_profiles)
        anomalies += self._check_zero_inflation(table_name, run_id, column_profiles, metric_columns)
        anomalies += self._check_encoding_rot(table_name, run_id, column_profiles)

        # ── 2b. ML anomaly scoring (Isolation Forest + ADTK) if enabled
        if self._ml_enabled:
            anomalies += self._run_isolation_forest(table_name, run_id, column_profiles, baseline)
            anomalies += self._run_adtk(table_name, run_id, baseline)

        # ── 3. LLM agent loop — AI interprets stats and refines anomaly list
        ai_response = self._run_ai_agent(
            table_name=table_name,
            run_id=run_id,
            column_profiles=column_profiles,
            kpi_snapshot=kpi_snapshot,
            schema_drift=schema_drift,
            existing_anomalies=anomalies,
            sla=sla,
        )

        # Merge any AI-discovered anomalies not caught by rule-based checks
        ai_anomalies = self._parse_ai_anomalies(ai_response, table_name, run_id)
        all_anomalies = self._deduplicate_anomalies(anomalies + ai_anomalies)
        health_score  = ai_response.get("data_health_score") or self._compute_health_score(all_anomalies)
        ai_summary    = ai_response.get("ai_summary", "")

        result = DataProfileResult(
            profile_id=profile_id,
            table_name=table_name,
            database_name=self.database,
            run_id=run_id,
            profiled_at=profiled_at,
            column_profiles=column_profiles,
            kpi_snapshot=kpi_snapshot,
            anomalies=all_anomalies,
            schema_columns=schema_info.get("columns", []),
            schema_drift_detected=bool(
                schema_drift["added"] or schema_drift["removed"] or schema_drift["retyped"]
            ),
            ai_summary=ai_summary,
            data_health_score=max(0, min(100, health_score)),
        )

        # ── 4. Persist result + update baseline
        self._persist_profile(result)
        self._update_baseline(table_name, column_profiles, kpi_snapshot, schema_info)

        logger.info(
            f"[DataProfilerAgent] Profile complete: score={result.data_health_score}, "
            f"anomalies={len(result.anomalies)}, critical={len(result.critical_anomalies())}"
        )
        return result

    # ------------------------------------------------------------------
    # Schema / metadata helpers
    # ------------------------------------------------------------------

    def _get_glue_schema(self, table_name: str) -> Dict[str, Any]:
        """Fetch column list and types from AWS Glue Data Catalog."""
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name=table_name)
            storage = resp["Table"]["StorageDescriptor"]
            cols = {c["Name"]: c["Type"] for c in storage.get("Columns", [])}
            partition_keys = {c["Name"]: c["Type"] for c in resp["Table"].get("PartitionKeys", [])}
            cols.update(partition_keys)
            return {"columns": list(cols.keys()), "types": cols, "partition_keys": list(partition_keys.keys())}
        except Exception as e:
            logger.warning(f"Glue schema fetch failed for {table_name}: {e}")
            return {"columns": [], "types": {}, "partition_keys": []}

    def _build_column_profiles(
        self,
        table_name: str,
        schema_info: Dict[str, Any],
        baseline: Dict[str, Any],
        key_columns: Optional[List[str]],
        profiling_columns: Optional[Dict[str, Any]] = None,
    ) -> List[ColumnProfile]:
        """
        Run per-column Athena profiling queries and build ColumnProfile objects.

        Column selection and query depth are controlled by the profiling_columns tier config:
          FULL       — stats + top-values + sample + zero-count (3 Athena queries)
          STATS_ONLY — stats query only (1 query; no top-values/sample)
          COUNT_ONLY — null_count and distinct_count only (shared table-level query)
          SKIP       — column excluded entirely (0 queries)

        If profiling_columns is None, all key_columns fall back to FULL tier.
        """
        if profiling_columns:
            # Build ordered list from tier config, honouring SKIP
            columns = [c for c, cfg in profiling_columns.items() if cfg.get("tier", "FULL") != "SKIP"]
        else:
            columns = key_columns or schema_info.get("columns", [])

        profiles: List[ColumnProfile] = []
        baseline_cols: Dict[str, Any] = baseline.get("column_profiles", {})

        # For COUNT_ONLY columns we can batch them into a single Athena query to save cost
        count_only_cols = []
        if profiling_columns:
            count_only_cols = [
                c for c, cfg in profiling_columns.items()
                if cfg.get("tier") == "COUNT_ONLY"
            ]

        count_only_stats: Dict[str, Dict[str, Any]] = {}
        if count_only_cols:
            count_only_stats = self._run_count_only_batch(table_name, count_only_cols)

        for col in columns[:30]:  # cap at 30 columns to avoid runaway cost
            col_type = schema_info.get("types", {}).get(col, "string")
            tier = "FULL"
            if profiling_columns and col in profiling_columns:
                tier = profiling_columns[col].get("tier", "FULL")

            if tier == "COUNT_ONLY":
                raw = count_only_stats.get(col)
                if raw is None:
                    continue
                stats = raw
            elif tier == "STATS_ONLY":
                stats = self._run_column_stats_query(table_name, col, col_type, stats_only=True)
            else:
                # FULL (default)
                stats = self._run_column_stats_query(table_name, col, col_type, stats_only=False)

            if stats is None:
                continue

            b = baseline_cols.get(col, {})
            profiles.append(ColumnProfile(
                column_name=col,
                data_type=col_type,
                row_count=stats["row_count"],
                null_count=stats["null_count"],
                null_pct=stats["null_pct"],
                distinct_count=stats["distinct_count"],
                distinct_pct=stats["distinct_pct"],
                min_value=stats.get("min_value"),
                max_value=stats.get("max_value"),
                mean_value=stats.get("mean_value"),
                stddev_value=stats.get("stddev_value"),
                top_values=stats.get("top_values", []),
                sample_values=stats.get("sample_values", []),
                is_primary_key_candidate=stats["distinct_pct"] > 0.99 and stats["null_pct"] == 0,
                historical_null_pct=b.get("null_pct"),
                historical_distinct_count=b.get("distinct_count"),
                historical_max_value=b.get("max_value"),
            ))
        return profiles

    def _run_count_only_batch(
        self, table_name: str, columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Single Athena query returning null_count and distinct_count for many columns at once.
        Used by COUNT_ONLY tier to minimise Athena costs.
        """
        aggs = ", ".join(
            f"COUNT_IF({c} IS NULL) AS {c}__null_cnt, "
            f"COUNT(DISTINCT {c}) AS {c}__distinct_cnt, "
            f"COUNT(*) AS row_count"
            for c in columns[:20]  # Athena has expression limits
        )
        sql = f"SELECT {aggs} FROM {self.database}.{table_name} LIMIT 1"
        rows = self._run_athena(sql)
        if not rows:
            return {}

        r = rows[0]
        row_count = int(r.get("row_count", 0) or 0)
        result: Dict[str, Dict[str, Any]] = {}
        for c in columns:
            null_count = int(r.get(f"{c}__null_cnt", 0) or 0)
            distinct_count = int(r.get(f"{c}__distinct_cnt", 0) or 0)
            null_pct = null_count / row_count if row_count else 0.0
            distinct_pct = distinct_count / (row_count - null_count) if (row_count - null_count) > 0 else 0.0
            result[c] = {
                "row_count": row_count,
                "null_count": null_count,
                "null_pct": null_pct,
                "distinct_count": distinct_count,
                "distinct_pct": distinct_pct,
                "top_values": [],
                "sample_values": [],
                "zero_count": 0,
                "is_numeric": False,
                "is_string": True,
            }
        return result

    def _run_column_stats_query(
        self, table_name: str, column: str, col_type: str, stats_only: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Execute an Athena query to gather per-column statistics."""
        is_numeric = any(t in col_type.lower() for t in ("int", "double", "float", "decimal", "bigint", "long"))
        is_string  = any(t in col_type.lower() for t in ("string", "varchar", "char"))

        numeric_aggs = (
            f", MIN(CAST({column} AS DOUBLE)) AS min_val"
            f", MAX(CAST({column} AS DOUBLE)) AS max_val"
            f", AVG(CAST({column} AS DOUBLE)) AS mean_val"
            f", STDDEV(CAST({column} AS DOUBLE)) AS stddev_val"
        ) if is_numeric else ""

        sql = f"""
SELECT
    COUNT(*)                             AS row_count,
    COUNT_IF({column} IS NULL)           AS null_count,
    COUNT(DISTINCT {column})             AS distinct_count
    {numeric_aggs}
FROM {self.database}.{table_name}
"""
        rows = self._run_athena(sql)
        if not rows:
            return None

        r = rows[0]
        row_count     = int(r.get("row_count", 0) or 0)
        null_count    = int(r.get("null_count", 0) or 0)
        distinct_count = int(r.get("distinct_count", 0) or 0)
        null_pct      = null_count / row_count if row_count else 0.0
        distinct_pct  = distinct_count / (row_count - null_count) if (row_count - null_count) > 0 else 0.0

        # Top-5 value frequencies and sample values skipped in STATS_ONLY tier
        top_values = []
        sample_values = []
        zero_count = 0

        if not stats_only:
            top_sql = f"""
SELECT CAST({column} AS VARCHAR) AS val, COUNT(*) AS cnt
FROM {self.database}.{table_name}
WHERE {column} IS NOT NULL
GROUP BY {column}
ORDER BY cnt DESC
LIMIT 5
"""
            top_rows = self._run_athena(top_sql) or []
            top_values = [
                {"value": tr.get("val"), "freq_pct": int(tr.get("cnt", 0)) / row_count if row_count else 0}
                for tr in top_rows
            ]

            sample_sql = f"""
SELECT CAST({column} AS VARCHAR) AS val
FROM {self.database}.{table_name}
WHERE {column} IS NOT NULL
LIMIT 5
"""
            sample_rows = self._run_athena(sample_sql) or []
            sample_values = [sr.get("val") for sr in sample_rows]

            if is_numeric:
                z_sql = f"SELECT COUNT(*) AS zc FROM {self.database}.{table_name} WHERE CAST({column} AS DOUBLE) = 0"
                z_rows = self._run_athena(z_sql) or []
                zero_count = int(z_rows[0].get("zc", 0)) if z_rows else 0

        return {
            "row_count": row_count,
            "null_count": null_count,
            "null_pct": null_pct,
            "distinct_count": distinct_count,
            "distinct_pct": distinct_pct,
            "min_value": r.get("min_val"),
            "max_value": r.get("max_val"),
            "mean_value": float(r["mean_val"]) if r.get("mean_val") is not None else None,
            "stddev_value": float(r["stddev_val"]) if r.get("stddev_val") is not None else None,
            "top_values": top_values,
            "sample_values": sample_values,
            "zero_count": zero_count,
            "is_numeric": is_numeric,
            "is_string": is_string,
        }

    def _build_kpi_snapshot(
        self,
        table_name: str,
        run_id: str,
        timestamp_col: Optional[str],
        partition_col: Optional[str],
        baseline: Dict[str, Any],
    ) -> KPISnapshot:
        """Build a KPISnapshot by running aggregate Athena queries."""
        # Total row count
        rc_rows = self._run_athena(f"SELECT COUNT(*) AS rc FROM {self.database}.{table_name}")
        row_count = int(rc_rows[0]["rc"]) if rc_rows else 0

        # Previous row count from baseline
        prev_rc = baseline.get("kpi_snapshot", {}).get("row_count")
        row_count_delta_pct = None
        if prev_rc and prev_rc > 0:
            row_count_delta_pct = (row_count - prev_rc) / prev_rc

        # Freshness
        freshness_hours = None
        latest_partition_value = None
        if timestamp_col:
            ts_rows = self._run_athena(
                f"SELECT MAX({timestamp_col}) AS max_ts FROM {self.database}.{table_name}"
            )
            if ts_rows and ts_rows[0].get("max_ts"):
                try:
                    max_ts_str = str(ts_rows[0]["max_ts"])
                    max_ts = datetime.fromisoformat(max_ts_str.replace("Z", "+00:00"))
                    now_utc = datetime.now(timezone.utc)
                    if max_ts.tzinfo is None:
                        max_ts = max_ts.replace(tzinfo=timezone.utc)
                    freshness_hours = (now_utc - max_ts).total_seconds() / 3600
                    latest_partition_value = max_ts_str
                except Exception:
                    pass

        # Partition count
        partition_count = 0
        if partition_col:
            pc_rows = self._run_athena(
                f"SELECT COUNT(DISTINCT {partition_col}) AS pc FROM {self.database}.{table_name}"
            )
            partition_count = int(pc_rows[0]["pc"]) if pc_rows else 0

        return KPISnapshot(
            table_name=table_name,
            database_name=self.database,
            run_id=run_id,
            captured_at=datetime.utcnow().isoformat(),
            row_count=row_count,
            partition_count=partition_count,
            latest_partition_value=latest_partition_value,
            freshness_hours=freshness_hours,
            row_count_delta_pct=row_count_delta_pct,
            duplicate_key_pct=None,  # filled by _check_duplicate_key_storm if needed
        )

    def _detect_schema_drift(
        self, schema_info: Dict[str, Any], baseline: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        """Compare current schema against baseline to find added/removed/retyped columns."""
        current  = schema_info.get("types", {})
        previous = baseline.get("schema", {}).get("types", {})
        if not previous:
            return {"added": [], "removed": [], "retyped": []}

        added   = [c for c in current  if c not in previous]
        removed = [c for c in previous if c not in current]
        retyped = [
            c for c in current
            if c in previous and current[c] != previous[c]
        ]
        return {"added": added, "removed": removed, "retyped": retyped}

    # ------------------------------------------------------------------
    # Edge-case anomaly detectors
    # ------------------------------------------------------------------

    def _check_null_flood(
        self, table_name: str, run_id: str, profiles: List[ColumnProfile]
    ) -> List[AnomalyReport]:
        """EDGE: sudden spike in null% vs baseline."""
        results = []
        for p in profiles:
            if p.historical_null_pct is None:
                continue
            delta = p.null_pct - p.historical_null_pct
            if delta >= self.null_flood_delta:
                severity = AnomalySeverity.CRITICAL if delta > 0.50 else (
                    AnomalySeverity.HIGH if delta > 0.30 else AnomalySeverity.MEDIUM
                )
                results.append(AnomalyReport(
                    anomaly_id=str(uuid.uuid4()),
                    table_name=table_name,
                    database_name=self.database,
                    column_name=p.column_name,
                    anomaly_type=AnomalyType.NULL_FLOOD,
                    severity=severity,
                    description=(
                        f"Null% in '{p.column_name}' jumped from "
                        f"{p.historical_null_pct:.1%} → {p.null_pct:.1%} "
                        f"(+{delta:.1%}). Possible upstream truncation or join failure."
                    ),
                    observed_value=f"{p.null_pct:.1%}",
                    expected_range=f"≤ {p.historical_null_pct + self.null_flood_delta:.1%}",
                    z_score=None,
                    detection_sql=(
                        f"SELECT COUNT_IF({p.column_name} IS NULL) AS null_cnt, "
                        f"COUNT(*) AS total FROM {self.database}.{table_name}"
                    ),
                    run_id=run_id,
                    recommended_action="Check upstream join or ETL step for this column.",
                ))
        return results

    def _check_cardinality_explosion(
        self, table_name: str, run_id: str, profiles: List[ColumnProfile]
    ) -> List[AnomalyReport]:
        """EDGE: dimension/FK column distinct count grows > cardinality_explosion_x× baseline."""
        results = []
        for p in profiles:
            if p.historical_distinct_count is None or p.historical_distinct_count == 0:
                continue
            ratio = p.distinct_count / p.historical_distinct_count
            if ratio >= self.cardinality_explosion_x:
                results.append(AnomalyReport(
                    anomaly_id=str(uuid.uuid4()),
                    table_name=table_name,
                    database_name=self.database,
                    column_name=p.column_name,
                    anomaly_type=AnomalyType.CARDINALITY_EXPLOSION,
                    severity=AnomalySeverity.HIGH if ratio < 10 else AnomalySeverity.CRITICAL,
                    description=(
                        f"Distinct count in '{p.column_name}' exploded "
                        f"{p.historical_distinct_count:,} → {p.distinct_count:,} "
                        f"({ratio:.1f}×). Possible cross-join bug or bad dedup."
                    ),
                    observed_value=str(p.distinct_count),
                    expected_range=f"≤ {int(p.historical_distinct_count * self.cardinality_explosion_x):,}",
                    z_score=None,
                    detection_sql=(
                        f"SELECT COUNT(DISTINCT {p.column_name}) AS card "
                        f"FROM {self.database}.{table_name}"
                    ),
                    run_id=run_id,
                    recommended_action="Investigate join logic; check for missing WHERE predicate.",
                ))
        return results

    def _check_temporal_anomaly(
        self,
        table_name: str,
        run_id: str,
        profiles: List[ColumnProfile],
        timestamp_col: Optional[str],
        baseline: Dict[str, Any],
    ) -> List[AnomalyReport]:
        """EDGE: max timestamp has rolled backwards (stale data reprocessed as fresh)."""
        if not timestamp_col:
            return []
        prev_max = baseline.get("kpi_snapshot", {}).get("latest_partition_value")
        if not prev_max:
            return []
        ts_profile = next((p for p in profiles if p.column_name == timestamp_col), None)
        if not ts_profile or ts_profile.max_value is None:
            return []
        try:
            prev_dt = datetime.fromisoformat(str(prev_max).replace("Z", "+00:00"))
            curr_dt = datetime.fromisoformat(str(ts_profile.max_value).replace("Z", "+00:00"))
            if curr_dt < prev_dt:
                return [AnomalyReport(
                    anomaly_id=str(uuid.uuid4()),
                    table_name=table_name,
                    database_name=self.database,
                    column_name=timestamp_col,
                    anomaly_type=AnomalyType.TEMPORAL_ANOMALY,
                    severity=AnomalySeverity.CRITICAL,
                    description=(
                        f"Max timestamp in '{timestamp_col}' rolled backwards: "
                        f"previous max was {prev_max}, current max is {ts_profile.max_value}. "
                        "Late-arriving data may have been reprocessed into the wrong partition."
                    ),
                    observed_value=str(ts_profile.max_value),
                    expected_range=f"≥ {prev_max}",
                    z_score=None,
                    detection_sql=(
                        f"SELECT MAX({timestamp_col}) AS max_ts FROM {self.database}.{table_name}"
                    ),
                    run_id=run_id,
                    recommended_action="Verify partition logic and late-data handling in the ETL job.",
                )]
        except Exception:
            pass
        return []

    def _check_distribution_skew(
        self, table_name: str, run_id: str, profiles: List[ColumnProfile]
    ) -> List[AnomalyReport]:
        """EDGE: a single value now dominates (≥ skew_threshold%) when it was minor historically."""
        results = []
        for p in profiles:
            if not p.top_values:
                continue
            top_freq = p.top_values[0]["freq_pct"] if p.top_values else 0.0
            if top_freq >= self.skew_threshold:
                results.append(AnomalyReport(
                    anomaly_id=str(uuid.uuid4()),
                    table_name=table_name,
                    database_name=self.database,
                    column_name=p.column_name,
                    anomaly_type=AnomalyType.DISTRIBUTION_SKEW,
                    severity=AnomalySeverity.HIGH,
                    description=(
                        f"Column '{p.column_name}' is heavily skewed: "
                        f"value '{p.top_values[0]['value']}' accounts for "
                        f"{top_freq:.1%} of rows. This may mask data loading errors."
                    ),
                    observed_value=f"{p.top_values[0]['value']} ({top_freq:.1%})",
                    expected_range=f"< {self.skew_threshold:.0%} per value",
                    z_score=None,
                    detection_sql=(
                        f"SELECT CAST({p.column_name} AS VARCHAR) AS val, "
                        f"COUNT(*) AS cnt, COUNT(*)*100.0/SUM(COUNT(*)) OVER() AS pct "
                        f"FROM {self.database}.{table_name} GROUP BY {p.column_name} "
                        f"ORDER BY cnt DESC LIMIT 10"
                    ),
                    run_id=run_id,
                    recommended_action="Review source data filter conditions; check for default value injection.",
                ))
        return results

    def _check_schema_drift_anomalies(
        self, table_name: str, run_id: str, drift: Dict[str, List[str]]
    ) -> List[AnomalyReport]:
        """EDGE: columns appear, disappear, or change type between runs."""
        results = []
        if drift["removed"]:
            results.append(AnomalyReport(
                anomaly_id=str(uuid.uuid4()),
                table_name=table_name,
                database_name=self.database,
                column_name=None,
                anomaly_type=AnomalyType.SCHEMA_DRIFT,
                severity=AnomalySeverity.CRITICAL,
                description=f"Columns removed since last run: {drift['removed']}. Downstream consumers will break.",
                observed_value=str(drift["removed"]),
                expected_range="No columns removed",
                z_score=None,
                detection_sql=None,
                run_id=run_id,
                recommended_action="Halt downstream pipelines. Validate DDL change is intentional.",
            ))
        if drift["added"]:
            results.append(AnomalyReport(
                anomaly_id=str(uuid.uuid4()),
                table_name=table_name,
                database_name=self.database,
                column_name=None,
                anomaly_type=AnomalyType.SCHEMA_DRIFT,
                severity=AnomalySeverity.MEDIUM,
                description=f"New columns added since last run: {drift['added']}. Validate mapping rules.",
                observed_value=str(drift["added"]),
                expected_range="No schema changes",
                z_score=None,
                detection_sql=None,
                run_id=run_id,
                recommended_action="Update downstream column mappings and data contracts.",
            ))
        if drift["retyped"]:
            results.append(AnomalyReport(
                anomaly_id=str(uuid.uuid4()),
                table_name=table_name,
                database_name=self.database,
                column_name=None,
                anomaly_type=AnomalyType.SCHEMA_DRIFT,
                severity=AnomalySeverity.HIGH,
                description=f"Column types changed since last run: {drift['retyped']}. May cause silent cast errors.",
                observed_value=str(drift["retyped"]),
                expected_range="No type changes",
                z_score=None,
                detection_sql=None,
                run_id=run_id,
                recommended_action="Review Glue job type mappings; test downstream CAST expressions.",
            ))
        return results

    def _check_stale_partition(
        self, table_name: str, run_id: str, kpi: KPISnapshot, sla_hours: float
    ) -> List[AnomalyReport]:
        """EDGE: data freshness exceeds SLA threshold."""
        if kpi.freshness_hours is None:
            return []
        if kpi.freshness_hours > sla_hours:
            severity = (
                AnomalySeverity.CRITICAL if kpi.freshness_hours > sla_hours * 2
                else AnomalySeverity.HIGH
            )
            return [AnomalyReport(
                anomaly_id=str(uuid.uuid4()),
                table_name=table_name,
                database_name=self.database,
                column_name=None,
                anomaly_type=AnomalyType.STALE_PARTITION,
                severity=severity,
                description=(
                    f"Table '{table_name}' is {kpi.freshness_hours:.1f}h old "
                    f"(SLA: {sla_hours}h). Latest data: {kpi.latest_partition_value}."
                ),
                observed_value=f"{kpi.freshness_hours:.1f} hours",
                expected_range=f"≤ {sla_hours}h",
                z_score=None,
                detection_sql=None,
                run_id=run_id,
                recommended_action="Check upstream source and Glue job trigger schedule.",
            )]
        return []

    def _check_duplicate_key_storm(
        self, table_name: str, run_id: str, kpi: KPISnapshot, pk_col: Optional[str]
    ) -> List[AnomalyReport]:
        """EDGE: PK uniqueness collapses (duplicate_key_pct exceeds threshold)."""
        if not pk_col:
            return []
        sql = f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT {pk_col}) AS distinct_keys,
    (COUNT(*) - COUNT(DISTINCT {pk_col})) * 1.0 / COUNT(*) AS dup_pct
FROM {self.database}.{table_name}
"""
        rows = self._run_athena(sql)
        if not rows:
            return []
        dup_pct = float(rows[0].get("dup_pct") or 0)
        kpi.duplicate_key_pct = dup_pct
        if dup_pct > self.dup_key_pct:
            severity = AnomalySeverity.CRITICAL if dup_pct > 0.05 else AnomalySeverity.HIGH
            return [AnomalyReport(
                anomaly_id=str(uuid.uuid4()),
                table_name=table_name,
                database_name=self.database,
                column_name=pk_col,
                anomaly_type=AnomalyType.DUPLICATE_KEY_STORM,
                severity=severity,
                description=(
                    f"PK column '{pk_col}' has {dup_pct:.2%} duplicate rows "
                    f"({int(rows[0].get('total_rows',0)) - int(rows[0].get('distinct_keys',0)):,} dupes). "
                    "Upstream dedup / merge logic may have failed."
                ),
                observed_value=f"{dup_pct:.2%} duplicates",
                expected_range=f"≤ {self.dup_key_pct:.2%}",
                z_score=None,
                detection_sql=(
                    f"SELECT {pk_col}, COUNT(*) AS cnt FROM {self.database}.{table_name} "
                    f"GROUP BY {pk_col} HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 20"
                ),
                run_id=run_id,
                recommended_action="Inspect Glue job merge key logic; check for missing partition filter.",
            )]
        return []

    def _check_boundary_explosion(
        self, table_name: str, run_id: str, profiles: List[ColumnProfile]
    ) -> List[AnomalyReport]:
        """EDGE: numeric max_value exceeds boundary_explosion_x× historical max."""
        results = []
        for p in profiles:
            if p.max_value is None or p.historical_max_value is None:
                continue
            try:
                curr_max = float(p.max_value)
                hist_max = float(p.historical_max_value)
                if hist_max <= 0 or curr_max <= 0:
                    continue
                ratio = curr_max / hist_max
                if ratio >= self.boundary_explosion_x:
                    results.append(AnomalyReport(
                        anomaly_id=str(uuid.uuid4()),
                        table_name=table_name,
                        database_name=self.database,
                        column_name=p.column_name,
                        anomaly_type=AnomalyType.BOUNDARY_EXPLOSION,
                        severity=AnomalySeverity.HIGH if ratio < 100 else AnomalySeverity.CRITICAL,
                        description=(
                            f"Column '{p.column_name}' max value jumped from "
                            f"{hist_max:,.2f} → {curr_max:,.2f} ({ratio:.0f}×). "
                            "Possible unit conversion bug or raw-feed corruption."
                        ),
                        observed_value=str(curr_max),
                        expected_range=f"≤ {hist_max * self.boundary_explosion_x:,.2f}",
                        z_score=None,
                        detection_sql=(
                            f"SELECT MAX({p.column_name}) AS max_val, "
                            f"COUNT_IF({p.column_name} > {hist_max * self.boundary_explosion_x}) AS outlier_cnt "
                            f"FROM {self.database}.{table_name}"
                        ),
                        run_id=run_id,
                        recommended_action="Inspect raw source file; check for unit/currency mismatches.",
                    ))
            except (TypeError, ValueError):
                continue
        return results

    def _check_zero_inflation(
        self,
        table_name: str,
        run_id: str,
        profiles: List[ColumnProfile],
        metric_columns: Optional[List[str]],
    ) -> List[AnomalyReport]:
        """EDGE: metric columns that must never be 0 start returning zeros."""
        if not metric_columns:
            return []
        results = []
        for p in profiles:
            if p.column_name not in metric_columns:
                continue
            # We stored zero_count in the raw stats dict; retrieve from top_values as fallback
            zero_tv = next((tv for tv in p.top_values if str(tv.get("value")) in ("0", "0.0")), None)
            zero_pct = zero_tv["freq_pct"] if zero_tv else 0.0
            if zero_pct > self.zero_inflation_pct:
                results.append(AnomalyReport(
                    anomaly_id=str(uuid.uuid4()),
                    table_name=table_name,
                    database_name=self.database,
                    column_name=p.column_name,
                    anomaly_type=AnomalyType.ZERO_INFLATION,
                    severity=AnomalySeverity.HIGH,
                    description=(
                        f"Metric column '{p.column_name}' has {zero_pct:.1%} zero values. "
                        "This column should never be zero — check division logic or source system."
                    ),
                    observed_value=f"{zero_pct:.1%} zeros",
                    expected_range=f"< {self.zero_inflation_pct:.1%} zeros",
                    z_score=None,
                    detection_sql=(
                        f"SELECT COUNT_IF(CAST({p.column_name} AS DOUBLE) = 0) AS zero_cnt, "
                        f"COUNT(*) AS total FROM {self.database}.{table_name}"
                    ),
                    run_id=run_id,
                    recommended_action="Check denominator in KPI formula; validate source aggregation.",
                ))
        return results

    def _check_encoding_rot(
        self, table_name: str, run_id: str, profiles: List[ColumnProfile]
    ) -> List[AnomalyReport]:
        """EDGE: string columns contain garbled UTF-8 / control characters / replacement chars."""
        results = []
        control_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
        for p in profiles:
            if not p.sample_values:
                continue
            garbled = []
            for v in p.sample_values:
                sv = str(v) if v is not None else ""
                if "�" in sv or control_re.search(sv):
                    garbled.append(sv[:40])
            if garbled:
                results.append(AnomalyReport(
                    anomaly_id=str(uuid.uuid4()),
                    table_name=table_name,
                    database_name=self.database,
                    column_name=p.column_name,
                    anomaly_type=AnomalyType.ENCODING_ROT,
                    severity=AnomalySeverity.MEDIUM,
                    description=(
                        f"Column '{p.column_name}' contains garbled characters "
                        f"(replacement chars or control codes). Sample: {garbled[:2]}. "
                        "Possible encoding mismatch between source and S3 write."
                    ),
                    observed_value=str(garbled[:2]),
                    expected_range="Clean UTF-8 printable characters",
                    z_score=None,
                    detection_sql=(
                        f"SELECT {p.column_name} FROM {self.database}.{table_name} "
                        f"WHERE REGEXP_LIKE(CAST({p.column_name} AS VARCHAR), '[\x00-\x08\x0b\x0e-\x1f]') "
                        f"LIMIT 10"
                    ),
                    run_id=run_id,
                    recommended_action="Re-ingest source with explicit UTF-8 encoding; check CSV/Parquet writer.",
                ))
        return results

    # ------------------------------------------------------------------
    # ML anomaly scoring
    # ------------------------------------------------------------------

    def _run_isolation_forest(
        self,
        table_name: str,
        run_id: str,
        column_profiles: List[ColumnProfile],
        baseline: Dict[str, Any],
    ) -> List[AnomalyReport]:
        """
        Score each numeric column's current stats using Isolation Forest trained on
        the historical baseline run history stored in S3.

        Activated only when:
          - ml_config.isolation_forest.enabled = true
          - sklearn is installed
          - ≥ min_baseline_runs of history exist in S3
        """
        if not self._if_config.get("enabled", False):
            return []
        if not _SKLEARN_AVAILABLE:
            logger.warning("Isolation Forest requested but scikit-learn is not installed. pip install scikit-learn")
            return []

        history_runs = self._load_run_history(table_name, limit=100)
        min_runs = self.ml_config.get("min_baseline_runs", 20)
        if len(history_runs) < min_runs:
            logger.info(
                f"[IsolationForest] Skipping {table_name}: only {len(history_runs)} "
                f"runs available (need {min_runs})"
            )
            return []

        feature_names = self._if_config.get(
            "features", ["null_pct", "distinct_count", "row_count", "mean_value", "stddev_value"]
        )
        contamination = self._if_config.get("contamination", 0.05)

        results: List[AnomalyReport] = []
        for profile in column_profiles:
            # Build historical feature matrix for this column
            X_hist = []
            for run in history_runs:
                col_hist = run.get("column_profiles", {}).get(profile.column_name, {})
                row = [float(col_hist.get(f) or 0) for f in feature_names]
                X_hist.append(row)

            if len(X_hist) < min_runs:
                continue

            current_row = [float(getattr(profile, f, None) or 0) for f in feature_names]

            try:
                X_np = np.array(X_hist, dtype=float)
                clf = IsolationForest(contamination=contamination, random_state=42)
                clf.fit(X_np)
                score = clf.decision_function([current_row])[0]
                is_outlier = clf.predict([current_row])[0] == -1

                if is_outlier:
                    results.append(AnomalyReport(
                        anomaly_id=str(uuid.uuid4()),
                        table_name=table_name,
                        database_name=self.database,
                        column_name=profile.column_name,
                        anomaly_type=AnomalyType.DISTRIBUTION_SKEW,
                        severity=AnomalySeverity.MEDIUM,
                        description=(
                            f"Isolation Forest flagged '{profile.column_name}' as a statistical outlier "
                            f"(anomaly score={score:.4f}). Current stats deviate significantly from "
                            f"the {len(X_hist)}-run baseline."
                        ),
                        observed_value=json.dumps({f: current_row[i] for i, f in enumerate(feature_names)}),
                        expected_range=f"Within Isolation Forest normal boundary (score > 0)",
                        z_score=score,
                        detection_sql=None,
                        run_id=run_id,
                        ai_explanation=f"ML anomaly score: {score:.4f}",
                        recommended_action="Investigate column stats manually; compare to historical baselines.",
                    ))
            except Exception as e:
                logger.warning(f"[IsolationForest] Failed for column {profile.column_name}: {e}")

        return results

    def _run_adtk(
        self,
        table_name: str,
        run_id: str,
        baseline: Dict[str, Any],
    ) -> List[AnomalyReport]:
        """
        Apply ADTK time-series anomaly detection on metric time-series from run history.

        Supported detectors (configured per metric in ml_config.adtk.detectors):
          - LevelShiftAD   — detects sudden level shifts
          - SeasonalAD     — detects deviations from seasonal patterns (day-of-week)
          - InterquartileRangeAD — detects IQR outliers
          - PersistAD      — detects values persisting unchanged

        Activated only when ml_config.adtk.enabled = true and adtk is installed.
        """
        if not self._adtk_config.get("enabled", False):
            return []
        if not _ADTK_AVAILABLE:
            logger.warning("ADTK requested but adtk is not installed. pip install adtk")
            return []

        history_runs = self._load_run_history(table_name, limit=90)
        min_runs = self.ml_config.get("min_baseline_runs", 20)
        if len(history_runs) < min_runs:
            return []

        detectors_cfg = self._adtk_config.get("detectors", {})
        results: List[AnomalyReport] = []

        # Build metric time-series from run history
        metric_series: Dict[str, List[Tuple[datetime, float]]] = {}
        for run in history_runs:
            ts_str = run.get("updated_at", "")
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                continue
            kpi = run.get("kpi_snapshot", {})
            metric_series.setdefault("row_count", []).append((ts, float(kpi.get("row_count") or 0)))
            metric_series.setdefault("null_pct", [])  # populated below from column profiles
            for col, col_data in run.get("column_profiles", {}).items():
                key = f"{col}__null_pct"
                metric_series.setdefault(key, []).append((ts, float(col_data.get("null_pct") or 0)))

        detector_map = {
            "LevelShiftAD":            lambda: LevelShiftAD(c=6.0),
            "SeasonalAD":              lambda: SeasonalAD(freq=7),
            "InterquartileRangeAD":    lambda: InterquartileRangeAD(c=3.0),
            "PersistAD":               lambda: PersistAD(c=3.0, side="both"),
        }

        for detector_name, det_cfg in detectors_cfg.items():
            applies_to = det_cfg.get("applies_to", [])
            det_factory = detector_map.get(detector_name)
            if not det_factory:
                continue

            for metric in applies_to:
                points = metric_series.get(metric, [])
                if len(points) < min_runs:
                    continue
                try:
                    points_sorted = sorted(points, key=lambda x: x[0])
                    idx = [p[0] for p in points_sorted]
                    vals = [p[1] for p in points_sorted]
                    series = pd.Series(vals, index=pd.DatetimeIndex(idx))
                    series = validate_series(series)

                    detector = det_factory()
                    anomalies_mask = detector.fit_detect(series)
                    flagged_dates = anomalies_mask[anomalies_mask == True].index.tolist() if anomalies_mask is not None else []

                    # Only report if the LATEST point is flagged
                    if flagged_dates and series.index[-1] in flagged_dates:
                        col_name = metric.replace("__null_pct", "") if "__" in metric else None
                        results.append(AnomalyReport(
                            anomaly_id=str(uuid.uuid4()),
                            table_name=table_name,
                            database_name=self.database,
                            column_name=col_name,
                            anomaly_type=AnomalyType.TEMPORAL_ANOMALY if "row_count" in metric else AnomalyType.NULL_FLOOD,
                            severity=AnomalySeverity.HIGH,
                            description=(
                                f"ADTK {detector_name} flagged '{metric}' at "
                                f"{series.index[-1].date()}: value={series.iloc[-1]:.4f}. "
                                f"Time-series shows an unexpected pattern vs {len(points)} historical runs."
                            ),
                            observed_value=f"{series.iloc[-1]:.4f}",
                            expected_range=f"Within ADTK {detector_name} bounds",
                            z_score=None,
                            detection_sql=None,
                            run_id=run_id,
                            ai_explanation=f"ADTK {detector_name} anomaly on metric: {metric}",
                            recommended_action="Investigate time-series pattern; check upstream data source.",
                        ))
                except Exception as e:
                    logger.warning(f"[ADTK] {detector_name} on {metric} failed: {e}")

        return results

    def _load_run_history(self, table_name: str, limit: int = 90) -> List[Dict[str, Any]]:
        """Load the last N profile baselines from S3 for ML training."""
        try:
            prefix = f"{self.BASELINE_PREFIX}{self.database}/{table_name}/history/"
            response = self.s3.list_objects_v2(
                Bucket=self.learning_bucket,
                Prefix=prefix,
                MaxKeys=limit,
            )
            objects = sorted(
                response.get("Contents", []),
                key=lambda x: x["LastModified"],
                reverse=True,
            )
            runs = []
            for obj in objects[:limit]:
                try:
                    body = self.s3.get_object(Bucket=self.learning_bucket, Key=obj["Key"])["Body"].read()
                    runs.append(json.loads(body))
                except Exception:
                    pass
            return runs
        except Exception as e:
            logger.warning(f"Could not load run history for {table_name}: {e}")
            return []

    # ------------------------------------------------------------------
    # AI agent loop
    # ------------------------------------------------------------------

    def _run_ai_agent(
        self,
        table_name: str,
        run_id: str,
        column_profiles: List[ColumnProfile],
        kpi_snapshot: KPISnapshot,
        schema_drift: Dict[str, List[str]],
        existing_anomalies: List[AnomalyReport],
        sla: float,
    ) -> Dict[str, Any]:
        """Invoke Bedrock with the full profiling context and return parsed AI response."""
        prompt = build_column_anomaly_prompt(
            database=self.database,
            table_name=table_name,
            run_id=run_id,
            column_profiles=[p.to_dict() for p in column_profiles],
            kpi_snapshot=kpi_snapshot.to_dict(),
            baselines={},
            added_columns=schema_drift.get("added", []),
            removed_columns=schema_drift.get("removed", []),
            retyped_columns=schema_drift.get("retyped", []),
            freshness_sla_hours=sla,
            observed_freshness_hours=kpi_snapshot.freshness_hours,
        )
        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 3000,
                "system": DATA_PROFILER_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": prompt}],
            }
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
            )
            raw = json.loads(response["body"].read())["content"][0]["text"]
            return self._parse_json(raw)
        except Exception as e:
            logger.warning(f"AI profiling agent call failed: {e}")
            return {}

    def _parse_ai_anomalies(
        self, ai_response: Dict[str, Any], table_name: str, run_id: str
    ) -> List[AnomalyReport]:
        """Convert AI-returned anomaly dicts into AnomalyReport objects."""
        results = []
        for item in ai_response.get("anomalies", []):
            try:
                anomaly_type = AnomalyType(item.get("anomaly_type", "NULL_FLOOD"))
            except ValueError:
                anomaly_type = AnomalyType.NULL_FLOOD
            try:
                severity = AnomalySeverity(item.get("severity", "MEDIUM"))
            except ValueError:
                severity = AnomalySeverity.MEDIUM

            results.append(AnomalyReport(
                anomaly_id=str(uuid.uuid4()),
                table_name=table_name,
                database_name=self.database,
                column_name=item.get("column_name"),
                anomaly_type=anomaly_type,
                severity=severity,
                description=item.get("description", ""),
                observed_value=item.get("observed_value"),
                expected_range=item.get("expected_range"),
                z_score=item.get("z_score"),
                detection_sql=item.get("detection_sql"),
                run_id=run_id,
                ai_explanation=item.get("description", ""),
                recommended_action=item.get("recommended_action", ""),
            ))
        return results

    def _deduplicate_anomalies(self, anomalies: List[AnomalyReport]) -> List[AnomalyReport]:
        """Remove duplicate anomalies (same type + column), keeping highest severity."""
        seen: Dict[Tuple[str, Optional[str]], AnomalyReport] = {}
        severity_rank = {
            AnomalySeverity.CRITICAL: 4,
            AnomalySeverity.HIGH: 3,
            AnomalySeverity.MEDIUM: 2,
            AnomalySeverity.LOW: 1,
        }
        for a in anomalies:
            key = (a.anomaly_type.value, a.column_name)
            if key not in seen or severity_rank[a.severity] > severity_rank[seen[key].severity]:
                seen[key] = a
        return sorted(seen.values(), key=lambda x: severity_rank[x.severity], reverse=True)

    def _compute_health_score(self, anomalies: List[AnomalyReport]) -> int:
        deductions = {
            AnomalySeverity.CRITICAL: 25,
            AnomalySeverity.HIGH: 15,
            AnomalySeverity.MEDIUM: 8,
            AnomalySeverity.LOW: 3,
        }
        score = 100
        for a in anomalies:
            score -= deductions.get(a.severity, 0)
        return max(0, score)

    # ------------------------------------------------------------------
    # S3 baseline persistence
    # ------------------------------------------------------------------

    def _load_baseline(self, table_name: str) -> Dict[str, Any]:
        try:
            key = f"{self.BASELINE_PREFIX}{self.database}/{table_name}/latest.json"
            body = self.s3.get_object(Bucket=self.learning_bucket, Key=key)["Body"].read()
            return json.loads(body)
        except Exception:
            return {}

    def _update_baseline(
        self,
        table_name: str,
        column_profiles: List[ColumnProfile],
        kpi_snapshot: KPISnapshot,
        schema_info: Dict[str, Any],
    ) -> None:
        run_ts = datetime.utcnow().isoformat()
        baseline = {
            "updated_at": run_ts,
            "column_profiles": {p.column_name: p.to_dict() for p in column_profiles},
            "kpi_snapshot": kpi_snapshot.to_dict(),
            "schema": schema_info,
        }
        try:
            # Always update the latest snapshot
            key = f"{self.BASELINE_PREFIX}{self.database}/{table_name}/latest.json"
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(baseline, indent=2, default=str),
            )
            # Also append to timestamped history for ML training
            history_key = (
                f"{self.BASELINE_PREFIX}{self.database}/{table_name}/history/"
                f"{run_ts.replace(':', '-')}.json"
            )
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=history_key,
                Body=json.dumps(baseline, indent=2, default=str),
            )
        except Exception as e:
            logger.warning(f"Could not update baseline for {table_name}: {e}")

    def _persist_profile(self, result: DataProfileResult) -> None:
        try:
            key = (
                f"{self.PROFILE_PREFIX}{result.database_name}/{result.table_name}/"
                f"{result.run_id}/{result.profile_id}.json"
            )
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(result.to_dict(), indent=2, default=str),
            )
            logger.info(f"Profile persisted: s3://{self.learning_bucket}/{key}")
        except Exception as e:
            logger.warning(f"Could not persist profile: {e}")

    # ------------------------------------------------------------------
    # Athena helper
    # ------------------------------------------------------------------

    def _run_athena(self, sql: str, timeout_s: int = 60) -> Optional[List[Dict[str, Any]]]:
        """Execute an Athena query synchronously and return rows as list of dicts."""
        import time
        try:
            resp = self.athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.athena_output},
            )
            qid = resp["QueryExecutionId"]
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                status = self.athena.get_query_execution(QueryExecutionId=qid)
                state = status["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    break
                if state in ("FAILED", "CANCELLED"):
                    reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                    logger.warning(f"Athena query {state}: {reason[:200]}")
                    return None
                time.sleep(1)
            else:
                logger.warning(f"Athena query timed out after {timeout_s}s")
                return None

            result = self.athena.get_query_results(QueryExecutionId=qid)
            rs = result["ResultSet"]
            cols = [c["Label"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
            rows = []
            for row in rs["Rows"][1:]:   # skip header row
                data = row.get("Data", [])
                rows.append({cols[i]: data[i].get("VarCharValue") for i in range(len(cols))})
            return rows
        except Exception as e:
            logger.error(f"Athena execution error: {e}")
            return None

    def _parse_json(self, raw: str) -> Dict[str, Any]:
        text = raw.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        for marker in ("```json", "```"):
            if marker in text:
                start = text.index(marker) + len(marker)
                end = text.rfind("```")
                if end > start:
                    try:
                        return json.loads(text[start:end].strip())
                    except json.JSONDecodeError:
                        pass
        return {}


# ===========================================================================
# Strands @tool wrappers — expose profiler capabilities as agent tools
# ===========================================================================

def make_profiler_tools(profiler: DataProfilerAgent):
    """
    Return a list of Strands @tool-decorated functions bound to a profiler instance.
    Allows the Strands Agent to call these as tools in its agentic loop.
    """

    @tool
    def profile_table(table_name: str, run_id: str, key_columns: str = "") -> str:
        """
        Run full data profiling on a table. Returns a JSON health report.
        key_columns: comma-separated list of columns to profile (empty = all).
        """
        cols = [c.strip() for c in key_columns.split(",") if c.strip()] or None
        result = profiler.profile(table_name=table_name, run_id=run_id, key_columns=cols)
        return json.dumps({
            "health_score": result.data_health_score,
            "anomaly_count": len(result.anomalies),
            "critical_anomalies": [a.to_dict() for a in result.critical_anomalies()],
            "ai_summary": result.ai_summary,
        }, indent=2, default=str)

    @tool
    def get_column_null_stats(table_name: str, column_name: str) -> str:
        """Return current null% for a specific column in a table."""
        sql = (
            f"SELECT COUNT_IF({column_name} IS NULL) AS null_cnt, COUNT(*) AS total "
            f"FROM {profiler.database}.{table_name}"
        )
        rows = profiler._run_athena(sql)
        if not rows:
            return json.dumps({"error": "Query failed"})
        r = rows[0]
        total = int(r.get("total") or 0)
        null_cnt = int(r.get("null_cnt") or 0)
        return json.dumps({
            "column": column_name,
            "null_count": null_cnt,
            "total_rows": total,
            "null_pct": null_cnt / total if total else 0,
        })

    @tool
    def get_table_row_count(table_name: str) -> str:
        """Return the current row count of a table."""
        rows = profiler._run_athena(f"SELECT COUNT(*) AS rc FROM {profiler.database}.{table_name}")
        rc = int(rows[0]["rc"]) if rows else 0
        return json.dumps({"table": table_name, "row_count": rc})

    @tool
    def get_value_distribution(table_name: str, column_name: str, top_n: int = 10) -> str:
        """Return top-N value frequencies for a column."""
        sql = (
            f"SELECT CAST({column_name} AS VARCHAR) AS val, COUNT(*) AS cnt, "
            f"COUNT(*)*100.0/SUM(COUNT(*)) OVER() AS pct "
            f"FROM {profiler.database}.{table_name} "
            f"GROUP BY {column_name} ORDER BY cnt DESC LIMIT {top_n}"
        )
        rows = profiler._run_athena(sql) or []
        return json.dumps(rows, default=str)

    @tool
    def check_primary_key_uniqueness(table_name: str, pk_column: str) -> str:
        """Check uniqueness of a primary key column. Returns duplicate percentage."""
        sql = (
            f"SELECT COUNT(*) AS total, COUNT(DISTINCT {pk_column}) AS distinct_count "
            f"FROM {profiler.database}.{table_name}"
        )
        rows = profiler._run_athena(sql)
        if not rows:
            return json.dumps({"error": "Query failed"})
        total = int(rows[0]["total"] or 0)
        distinct = int(rows[0]["distinct_count"] or 0)
        dup_pct = (total - distinct) / total if total else 0
        return json.dumps({
            "pk_column": pk_column,
            "total_rows": total,
            "distinct_keys": distinct,
            "duplicate_pct": dup_pct,
            "is_unique": dup_pct == 0,
        })

    @tool
    def detect_anomalies_for_column(table_name: str, column_name: str, run_id: str) -> str:
        """
        Run targeted anomaly detection for a single column.
        Checks null flood, cardinality explosion, boundary explosion, and encoding rot.
        """
        schema  = profiler._get_glue_schema(table_name)
        baseline = profiler._load_baseline(table_name)
        profiles = profiler._build_column_profiles(table_name, schema, baseline, [column_name])
        anomalies: List[AnomalyReport] = []
        anomalies += profiler._check_null_flood(table_name, run_id, profiles)
        anomalies += profiler._check_cardinality_explosion(table_name, run_id, profiles)
        anomalies += profiler._check_boundary_explosion(table_name, run_id, profiles)
        anomalies += profiler._check_encoding_rot(table_name, run_id, profiles)
        anomalies += profiler._check_distribution_skew(table_name, run_id, profiles)
        return json.dumps([a.to_dict() for a in anomalies], indent=2, default=str)

    @tool
    def get_freshness_status(table_name: str, timestamp_column: str, sla_hours: float = 26.0) -> str:
        """
        Check data freshness against an SLA. Returns hours since latest record and SLA status.
        """
        sql = f"SELECT MAX({timestamp_column}) AS max_ts FROM {profiler.database}.{table_name}"
        rows = profiler._run_athena(sql)
        if not rows or not rows[0].get("max_ts"):
            return json.dumps({"error": "Could not determine freshness"})
        try:
            max_ts_str = str(rows[0]["max_ts"])
            max_ts = datetime.fromisoformat(max_ts_str.replace("Z", "+00:00"))
            if max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            freshness_hours = (datetime.now(timezone.utc) - max_ts).total_seconds() / 3600
            return json.dumps({
                "table": table_name,
                "latest_record": max_ts_str,
                "freshness_hours": freshness_hours,
                "sla_hours": sla_hours,
                "sla_breached": freshness_hours > sla_hours,
                "severity": "CRITICAL" if freshness_hours > sla_hours * 2 else (
                    "HIGH" if freshness_hours > sla_hours else "OK"
                ),
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    return [
        profile_table,
        get_column_null_stats,
        get_table_row_count,
        get_value_distribution,
        check_primary_key_uniqueness,
        detect_anomalies_for_column,
        get_freshness_status,
    ]


def build_profiler_agent(
    database: str = "default",
    learning_bucket: str = "strands-etl-learning",
    aws_region: str = "us-east-1",
    model_id: str = DataProfilerAgent.MODEL_ID,
) -> Agent:
    """
    Build and return a Strands Agent wired with all profiler tools.

    The agent can autonomously decide which profiling tools to call,
    in what order, and when to stop — driven by the Strands agentic loop.

    Example
    -------
        agent = build_profiler_agent(database="insurance_dw")
        response = agent(
            "Profile the policy_master table for run-2024-07-01. "
            "Focus on policy_id uniqueness, premium_amount boundaries, "
            "and effective_date freshness. SLA is 26 hours."
        )
        print(response)
    """
    profiler = DataProfilerAgent(
        database=database,
        learning_bucket=learning_bucket,
        aws_region=aws_region,
        model_id=model_id,
    )
    tools = make_profiler_tools(profiler)
    return Agent(
        system_prompt=DATA_PROFILER_SYSTEM_PROMPT,
        tools=tools,
        model=f"bedrock/{model_id}",
    )
