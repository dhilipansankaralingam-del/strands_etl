"""
Business tools — medallion-architecture awareness and deep false-positive detection.

Tools
-----
fetch_critical_table_metadata   Load criticality, layer, SLA, and FP hints from
                                the critical-table registry JSON.
fetch_business_calendar_events  Detect month-end, quarter-end, year-end, weekend,
                                DST transitions, and recovery days that inflate or
                                deflate volumes without being data errors.
fetch_column_pattern_history    For CSV/Lambda → staging (all-string) tables,
                                diff today's Glue schema vs the schema N days ago to
                                detect schema evolution vs missing-column failures.
check_statistical_baseline      Query audit_validation history for the same rule+table
                                and compute a 3-sigma test on the current actual value.

Design note
-----------
These tools provide context signals that the validation_agent uses to decide
TRUE_FAILURE vs FALSE_POSITIVE.  They never write to the audit table — that
is owned exclusively by write_ai_enrichment.
"""

from __future__ import annotations

import json
import math
import os
import re
import statistics
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3
from strands import tool

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
ATHENA_DATABASE: str = os.environ.get("ATHENA_DATABASE", "audit_db")
ATHENA_TABLE: str = os.environ.get("ATHENA_TABLE", "audit_validation")
ATHENA_OUTPUT: str = os.environ.get(
    "ATHENA_OUTPUT", "s3://strands-etl-audit/athena-results/"
)

_REGISTRY_PATH = Path(__file__).parent.parent / "config" / "critical_tables.json"

_glue_client: Optional[object] = None
_athena_client: Optional[object] = None


def _glue():
    global _glue_client
    if _glue_client is None:
        _glue_client = boto3.client("glue", region_name=AWS_REGION)
    return _glue_client


def _athena():
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client("athena", region_name=AWS_REGION)
    return _athena_client


# ---------------------------------------------------------------------------
# Shared Athena helper (mirrors athena_tools to avoid circular import)
# ---------------------------------------------------------------------------
def _run_athena(sql: str, database: str = ATHENA_DATABASE) -> list[dict]:
    client = _athena()
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(60):
        time.sleep(3)
        state = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
    if state != "SUCCEEDED":
        return []
    paginator = client.get_paginator("get_query_results")
    columns: list[str] = []
    rows: list[dict] = []
    for page in paginator.paginate(QueryExecutionId=qid):
        page_rows = page["ResultSet"]["Rows"]
        if not columns:
            columns = [c.get("VarCharValue", "") for c in page_rows[0]["Data"]]
            page_rows = page_rows[1:]
        for row in page_rows:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            rows.append(dict(zip(columns, values)))
    return rows


# ---------------------------------------------------------------------------
# Registry loader (read once, cache in memory)
# ---------------------------------------------------------------------------
_REGISTRY: Optional[dict] = None


def _registry() -> dict:
    global _REGISTRY
    if _REGISTRY is None:
        try:
            _REGISTRY = json.loads(_REGISTRY_PATH.read_text())
        except Exception:
            _REGISTRY = {"tables": {}}
    return _REGISTRY


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------

def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # 5=Sat, 6=Sun


def _business_days_from_month_end(d: date) -> int:
    """Positive = days before month end; negative = days after."""
    import calendar
    last_day = calendar.monthrange(d.year, d.month)[1]
    last = date(d.year, d.month, last_day)
    delta = 0
    cursor = d
    while cursor < last:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            delta += 1
    return delta if d <= last else -delta


def _quarter_end_month(d: date) -> bool:
    return d.month in (3, 6, 9, 12)


def _is_dst_transition(d: date, tz_name: str = "America/New_York") -> bool:
    try:
        import pytz  # type: ignore
        tz = pytz.timezone(tz_name)
        today_offset = tz.utcoffset(datetime(d.year, d.month, d.day, 12))
        yesterday = d - timedelta(days=1)
        yest_offset = tz.utcoffset(datetime(yesterday.year, yesterday.month, yesterday.day, 12))
        return today_offset != yest_offset
    except Exception:
        return False


def _parse_date(date_str: Optional[str]) -> date:
    if not date_str:
        return date.today()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str[:19], fmt).date()
        except ValueError:
            continue
    return date.today()


def _parse_numeric(s: str) -> Optional[float]:
    """Extract the first numeric value from a string like '12345', '15.3%', '3 rows failed'."""
    if not s:
        return None
    m = re.search(r"[-+]?\d[\d,]*\.?\d*", s.replace(",", ""))
    if m:
        try:
            return float(m.group().replace(",", ""))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Public tools
# ---------------------------------------------------------------------------


@tool
def fetch_critical_table_metadata(table_name: str) -> str:
    """
    Look up a table in the critical-table registry and return its medallion layer,
    source type, SLA, known false-positive patterns, required columns, and
    downstream dependencies.

    Use this as the FIRST tool call for any table — the metadata shapes which
    other tools and which FP patterns are relevant.

    Parameters
    ----------
    table_name : Exact table name (e.g. 'stg_member_enrollment').

    Returns
    -------
    JSON string with the full registry entry, or {"found": false} if unknown.
    The 'layer' field is one of: staging | datalake | base | master.
    The 'fp_patterns' list names the false-positive patterns most likely for
    this table (see validation_agent for the full FP catalog).
    """
    reg = _registry()
    tables = reg.get("tables", {})
    meta = tables.get(table_name) or tables.get(table_name.lower())
    if not meta:
        # Fuzzy: check if table_name is a suffix of any key
        for key, val in tables.items():
            if table_name in key or key in table_name:
                return json.dumps({"found": True, "partial_match": True, **val})
        return json.dumps({"found": False, "table_name": table_name})
    return json.dumps({"found": True, **meta})


@tool
def fetch_business_calendar_events(
    date_str: Optional[str] = None,
    country: str = "US",
) -> str:
    """
    Detect whether a date falls on a significant business calendar event
    that would explain anomalous data volumes without being a true data error.

    Events checked (beyond public holidays — use get_holiday_context for those):
      WEEKEND              Saturday or Sunday — expected lower volume.
      MONTH_END            Last 3 business days of the month — higher volume,
                           corrections, and catch-up loads are normal.
      MONTH_START          First 2 business days — source feeds often arrive late.
      QUARTER_END          Last 3 business days of Mar / Jun / Sep / Dec —
                           large batch corrections, inter-company postings.
      YEAR_END             Last 5 business days of December — major cutoffs,
                           data frozen in source systems.
      DAY_AFTER_HOLIDAY    Recovery / double-load day — volumes may be 2x normal.
      DST_TRANSITION       23-hour or 25-hour day (US spring forward / fall back) —
                           hourly row-count rules break at the boundary.
      FIRST_OF_MONTH       1st calendar day — some sources reset sequence numbers.

    Parameters
    ----------
    date_str : ISO-8601 date or datetime string.  Defaults to today.
    country  : ISO-3166-1 alpha-2 country code used for holiday lookup.

    Returns
    -------
    JSON string with:
        events          list[str]   — names of matched events (empty if none).
        is_critical_period bool     — True if any event was matched.
        fp_guidance     str         — Plain-English note for the agent.
    """
    target = _parse_date(date_str)
    events: list[str] = []

    # Weekend
    if _is_weekend(target):
        events.append("WEEKEND")

    # Business-days proximity to month end
    bdays_to_month_end = _business_days_from_month_end(target)
    if 0 <= bdays_to_month_end <= 3:
        if _quarter_end_month(target) and bdays_to_month_end <= 3:
            events.append("QUARTER_END")
        if target.month == 12 and bdays_to_month_end <= 5:
            events.append("YEAR_END")
        events.append("MONTH_END")

    # Month start (first 2 business days)
    day = target.day
    if day == 1:
        events.append("FIRST_OF_MONTH")
    if day <= 3 and not _is_weekend(target):
        events.append("MONTH_START")

    # DST transition
    if _is_dst_transition(target):
        events.append("DST_TRANSITION")

    # Day after a public holiday
    try:
        import holidays as _holidays_lib  # type: ignore
        cal = _holidays_lib.country_holidays(country, years=target.year)
        yesterday = target - timedelta(days=1)
        if yesterday in cal:
            events.append("DAY_AFTER_HOLIDAY")
    except Exception:
        pass

    guidance_parts: list[str] = []
    if "WEEKEND" in events:
        guidance_parts.append(
            "Weekend day — volume-based rules (RANGE, row_count) are expected to be "
            "lower. Check day-of-week pattern before classifying as TRUE_FAILURE."
        )
    if "MONTH_END" in events:
        guidance_parts.append(
            "Month-end window — catch-up loads, backdated corrections, and "
            "inter-company postings make volumes 2–5x higher than mid-month norm."
        )
    if "QUARTER_END" in events:
        guidance_parts.append(
            "Quarter-end — large batch adjustments and regulatory submissions "
            "produce spikes; referential-integrity rules may catch legitimate "
            "inter-period records."
        )
    if "YEAR_END" in events:
        guidance_parts.append(
            "Year-end cutoff — many source systems freeze data; near-zero row "
            "counts are expected and are FALSE_POSITIVE for most volume rules."
        )
    if "MONTH_START" in events:
        guidance_parts.append(
            "Month-start — source files often arrive 4–8 hours late on the first "
            "business day. A zero-row staging failure is likely STAGING_FILE_DELAY."
        )
    if "DAY_AFTER_HOLIDAY" in events:
        guidance_parts.append(
            "Day after a public holiday — recovery loads may double the normal "
            "row count, causing upper-bound RANGE rules to fire as FALSE_POSITIVE."
        )
    if "DST_TRANSITION" in events:
        guidance_parts.append(
            "DST transition — the day has 23 or 25 hours. Hourly count rules and "
            "timestamp-boundary checks will fire incorrectly; classify as FALSE_POSITIVE."
        )

    guidance = (
        "  |  ".join(guidance_parts)
        if guidance_parts
        else f"No special business calendar events detected for {target}."
    )

    return json.dumps(
        {
            "date": target.isoformat(),
            "events": events,
            "is_critical_period": bool(events),
            "fp_guidance": guidance,
        }
    )


@tool
def fetch_column_pattern_history(
    table_name: str,
    database_name: str = "staging",
    lookback_days: int = 7,
) -> str:
    """
    For staging tables loaded from CSV via Lambda, compare the current Glue
    Catalog schema against the schema that was in place *lookback_days* ago.

    This is the primary tool for detecting STAGING_SCHEMA_EVOLUTION vs
    STAGING_MISSING_REQUIRED_COLUMN failures.

    Flow modelled
    -------------
    Source CSV (with defined schema) → Lambda trigger → S3 landing → Glue Crawler
    → Glue Catalog table (all columns STRING) → Athena validation

    When a new optional column appears in today's CSV, the Crawler adds it to
    the Glue table → column-count or NOT_NULL validation fires → but this is
    expected schema evolution, not a data error (FALSE_POSITIVE).

    When a required column (e.g. member_id) disappears from the CSV, the
    column goes missing from the staging table → TRUE_FAILURE.

    Parameters
    ----------
    table_name    : Glue Catalog table name (e.g. 'stg_member_enrollment').
    database_name : Glue Catalog database (default: 'staging').
    lookback_days : How many days back to look for the historical schema.

    Returns
    -------
    JSON string with:
        current_columns   list[str]   — columns in today's schema.
        historical_columns list[str]  — columns from lookback_days ago.
        new_columns       list[str]   — appeared since historical snapshot.
        missing_columns   list[str]   — present historically but gone today.
        reordered_columns list[str]   — same columns but different ordinal position.
        schema_changed    bool
        fp_guidance       str         — classification hint.
    """
    try:
        # Current schema from Glue
        current_resp = _glue().get_table(DatabaseName=database_name, Name=table_name)
        current_cols: list[str] = [
            c["Name"]
            for c in current_resp["Table"]["StorageDescriptor"]["Columns"]
        ]
        current_positions = {c: i for i, c in enumerate(current_cols)}
    except Exception as exc:
        return json.dumps({"error": f"Could not fetch current schema from Glue: {exc}"})

    # Historical schema from Glue table versions
    historical_cols: list[str] = []
    historical_positions: dict[str, int] = {}
    try:
        versions_resp = _glue().get_table_versions(
            DatabaseName=database_name, TableName=table_name, MaxResults=50
        )
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(lookback_days))
        best_version = None
        for ver in versions_resp.get("TableVersions", []):
            update_time = ver["Table"].get("UpdateTime")
            if update_time and update_time <= cutoff:
                if best_version is None or update_time > best_version["Table"]["UpdateTime"]:
                    best_version = ver

        if best_version:
            historical_cols = [
                c["Name"]
                for c in best_version["Table"]["StorageDescriptor"]["Columns"]
            ]
            historical_positions = {c: i for i, c in enumerate(historical_cols)}
    except Exception as exc:
        return json.dumps(
            {
                "error": f"Could not fetch historical schema from Glue versions: {exc}",
                "current_columns": current_cols,
            }
        )

    if not historical_cols:
        return json.dumps(
            {
                "current_columns": current_cols,
                "historical_columns": [],
                "new_columns": [],
                "missing_columns": [],
                "reordered_columns": [],
                "schema_changed": False,
                "fp_guidance": (
                    "No historical schema version found for this table "
                    f"within the last {lookback_days} days.  "
                    "This may be a new table (INFRA_FIRST_RUN) — classify "
                    "schema failures as NEEDS_INVESTIGATION."
                ),
            }
        )

    current_set = set(current_cols)
    historical_set = set(historical_cols)
    new_cols = sorted(current_set - historical_set)
    missing_cols = sorted(historical_set - current_set)

    # Columns whose ordinal position changed
    common = current_set & historical_set
    reordered = sorted(
        c for c in common
        if current_positions.get(c) != historical_positions.get(c)
    )

    schema_changed = bool(new_cols or missing_cols or reordered)

    # Build FP guidance
    parts: list[str] = []
    if new_cols:
        parts.append(
            f"NEW columns detected: {new_cols}.  If these are optional / nullable, "
            "this is STAGING_SCHEMA_EVOLUTION → FALSE_POSITIVE.  Check the critical "
            "table registry to see if the source is known to add optional columns."
        )
    if missing_cols:
        parts.append(
            f"MISSING columns: {missing_cols}.  If any are marked 'required_columns' "
            "in the registry, this is STAGING_MISSING_REQUIRED_COLUMN → TRUE_FAILURE. "
            "Raise with the source team immediately."
        )
    if reordered and not new_cols and not missing_cols:
        parts.append(
            f"Column order changed for: {reordered}.  If the pipeline reads by "
            "column name (not position), this is FALSE_POSITIVE.  "
            "If it reads by position (e.g. raw CSV index), this is TRUE_FAILURE."
        )
    if not schema_changed:
        parts.append(
            "Schema is identical to the historical snapshot.  Schema is NOT the "
            "cause of this validation failure."
        )

    return json.dumps(
        {
            "current_columns": current_cols,
            "historical_columns": historical_cols,
            "new_columns": new_cols,
            "missing_columns": missing_cols,
            "reordered_columns": reordered,
            "schema_changed": schema_changed,
            "fp_guidance": "  |  ".join(parts) if parts else "No schema changes detected.",
        }
    )


@tool
def check_statistical_baseline(
    table_name: str,
    rule_name: str,
    actual_value: str,
    lookback_days: int = 30,
) -> str:
    """
    Compare the current failure's actual_value against the historical baseline
    for the same table + rule over the last *lookback_days* days.

    Uses mean and standard deviation of historical actual_values from
    audit_validation.  A current value within 3 standard deviations of the
    mean is unlikely to be a genuine data error.

    Interpretation guide
    --------------------
      |z| < 1.0   → well within normal range  → strong FP signal (IGNORE / MONITOR)
      |z| 1.0–2.0 → mild deviation            → weak FP signal   (MONITOR)
      |z| 2.0–3.0 → notable but within 3σ     → NEEDS_INVESTIGATION
      |z| > 3.0   → genuine outlier            → strong TF signal (ESCALATE / FIX)

    Parameters
    ----------
    table_name    : Table name to look up in audit_validation history.
    rule_name     : Validation rule name to look up.
    actual_value  : The current row's actual_value (string; numeric part is extracted).
    lookback_days : Days of history to use for baseline (default 30).

    Returns
    -------
    JSON string with:
        baseline_mean     float   — historical mean of actual_value.
        baseline_stddev   float   — historical standard deviation.
        z_score           float   — (current - mean) / stddev.
        sigma_band        str     — "<1σ" | "1σ–2σ" | "2σ–3σ" | ">3σ"
        sample_size       int     — number of historical data points used.
        fp_signal         bool    — True if |z| <= 3.0 (could be FP).
        interpretation    str     — Plain-English summary.
    """
    current_numeric = _parse_numeric(actual_value)
    if current_numeric is None:
        return json.dumps(
            {
                "error": (
                    f"Could not parse a numeric value from actual_value='{actual_value}'. "
                    "Statistical baseline check skipped."
                )
            }
        )

    safe_table = table_name.replace("'", "''")
    safe_rule = rule_name.replace("'", "''")
    sql = f"""
        SELECT actual_value
        FROM   {ATHENA_TABLE}
        WHERE  table_name = '{safe_table}'
          AND  rule_name  = '{safe_rule}'
          AND  ai_classification IS NOT NULL
          AND  failure_timestamp >= NOW() - INTERVAL '{int(lookback_days)}' DAY
        LIMIT  200
    """
    rows = _run_athena(sql)

    historical_values: list[float] = []
    for row in rows:
        v = _parse_numeric(row.get("actual_value", ""))
        if v is not None:
            historical_values.append(v)

    if len(historical_values) < 3:
        return json.dumps(
            {
                "error": "Insufficient history",
                "sample_size": len(historical_values),
                "interpretation": (
                    f"Only {len(historical_values)} historical data point(s) found "
                    f"for {table_name}/{rule_name} — cannot compute a reliable baseline. "
                    "Do not use statistical test; rely on other signals."
                ),
            }
        )

    mean = statistics.mean(historical_values)
    stddev = statistics.stdev(historical_values)

    if stddev == 0:
        sigma_band = "<1σ" if current_numeric == mean else ">3σ"
        z = 0.0 if current_numeric == mean else float("inf")
    else:
        z = abs((current_numeric - mean) / stddev)
        if z < 1.0:
            sigma_band = "<1σ"
        elif z < 2.0:
            sigma_band = "1σ–2σ"
        elif z < 3.0:
            sigma_band = "2σ–3σ"
        else:
            sigma_band = ">3σ"

    fp_signal = z <= 3.0

    if z < 1.0:
        interpretation = (
            f"Current value {current_numeric} is within 1σ of the 30-day mean "
            f"({mean:.2f} ± {stddev:.2f}).  This is a normal fluctuation — "
            "strong FALSE_POSITIVE signal."
        )
    elif z < 2.0:
        interpretation = (
            f"Current value {current_numeric} is within 2σ of the mean.  "
            "Mild deviation — lean FALSE_POSITIVE but worth monitoring."
        )
    elif z < 3.0:
        interpretation = (
            f"Current value {current_numeric} is within 3σ of the mean.  "
            "Notable but not extreme — classify as NEEDS_INVESTIGATION."
        )
    else:
        interpretation = (
            f"Current value {current_numeric} is {z:.1f}σ from the mean — "
            "a genuine statistical outlier.  Strong TRUE_FAILURE signal."
        )

    return json.dumps(
        {
            "baseline_mean": round(mean, 4),
            "baseline_stddev": round(stddev, 4),
            "current_value": current_numeric,
            "z_score": round(z, 3),
            "sigma_band": sigma_band,
            "sample_size": len(historical_values),
            "fp_signal": fp_signal,
            "interpretation": interpretation,
        }
    )
