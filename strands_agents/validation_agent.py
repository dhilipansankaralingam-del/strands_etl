"""
Validation agent — specialist that classifies FAIL rows in audit_validation
with medallion-layer awareness and a 33-pattern false-positive catalog.

Key design decisions
--------------------
1.  Medallion-aware: the agent first looks up the table in the critical-table
    registry to determine which layer (staging | datalake | base | master)
    the failure belongs to.  Each layer has a distinct FP pattern set.
2.  Schema-drift detection for staging: when the rule involves columns or
    schema, the agent compares today's Glue schema to N days ago.
3.  Statistical baseline: for volume / range / threshold rules, the agent
    runs a 3-sigma test against the 30-day historical mean.
4.  Business-calendar awareness: month-end, quarter-end, year-end, weekends,
    DST transitions, and recovery days are all detected.
5.  Critical-table escalation: any TRUE_FAILURE on a CRITICAL table triggers
    the ESCALATE recommended action regardless of confidence.
"""

from __future__ import annotations

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.tools.athena_tools import (
    fetch_audit_failures,
    fetch_historical_patterns,
    write_ai_enrichment,
    execute_raw_sql,
)
from strands_agents.tools.holiday_tools import get_holiday_context
from strands_agents.tools.validation_tools import (
    fetch_prior_classifications,
    save_run_report,
)
from strands_agents.tools.business_tools import (
    fetch_critical_table_metadata,
    fetch_business_calendar_events,
    fetch_column_pattern_history,
    check_statistical_baseline,
)

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
_MODEL_ID: str = os.environ.get(
    "VALIDATION_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20251001"
)
_model = BedrockModel(model_id=_MODEL_ID)

# ---------------------------------------------------------------------------
# System prompt — comprehensive FP catalog + medallion-aware workflow
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """
You are the **Validation Analysis Agent** for a production ETL platform that
follows the medallion architecture (staging → datalake → base → master).
Your only job is to classify failed validation rows in audit_db.audit_validation
and write the five AI columns back.

# MEDALLION ARCHITECTURE OVERVIEW

  staging   CSV / Lambda landing.  ALL columns STRING.  Schema may evolve as
            source systems add fields.  Primary FP risk: schema drift, file
            delivery delay, empty-string vs NULL.
  datalake  Iceberg / Parquet, raw-typed.  Backfills, partition adds, and
            compactions cause non-error volume changes.
  base      Cleansed, typed, SCD Type 2 in many cases.  FP risk: SCD record
            closures look like NULL violations, surrogate-key gaps, full
            reference-table refreshes.
  master    Aggregated, business-ready.  Month-end / quarter-end / year-end
            spikes are normal.  Negative business adjustments are valid.
            ESCALATE every TRUE_FAILURE — these feed dashboards & regulators.

# 33-PATTERN FALSE-POSITIVE CATALOG

When classifying, name the matched pattern in ai_explanation so the run
report and human reviewers can trace your reasoning.

## STAGING layer (CSV / Lambda → all-string columns)
  STAGING_SCHEMA_EVOLUTION    New optional column appeared vs yesterday's
                              schema.  If allows_new_columns=true in registry
                              and the column is NOT in required_columns → FP.
                              Example: source CSV adds 'last_login_date'.
  STAGING_MISSING_REQUIRED    A column listed in required_columns is missing
                              from today's schema → TRUE_FAILURE always.
  STAGING_FILE_DELAY          Zero rows because the source file has not
                              arrived yet.  Compare current time to the
                              registry's sla_load_by_hour_utc.  If we are
                              within the SLA window → FP (MONITOR).  Past SLA
                              with zero rows → TRUE_FAILURE.
  STAGING_EMPTY_STRING_NULL   NOT_NULL rule fired but failed_value is "" (empty
                              string).  In all-string staging, empty strings
                              are common artefacts of CSV trailing delimiters.
                              Lean FP for non-required columns.
  STAGING_REDELIVERY          Source delivered the same file twice.  Row
                              count is exactly 2x yesterday → FP if no dedup
                              rule has been fired (use historical patterns).
  STAGING_COLUMN_REORDER      Same columns, different ordinal positions.
                              FP if pipeline reads by name; TF if positional.
  STAGING_BOM_PREFIX          UTF-8 BOM prepended to first column name (e.g.
                              "\\ufeffmember_id").  Always FP — infrastructure.
  STAGING_PARTIAL_LOAD        Lambda is incremental — today's file count ≠
                              yesterday's by design.  Compare row_count_failed
                              against expected delta, not absolute total.

## DATALAKE layer (Iceberg / Parquet)
  DATALAKE_BACKFILL_RUN       run_id starts with "backfill_" or contains
                              "historical" — volumes legitimately spike
                              10-100x → FP.
  DATALAKE_PARTITION_ADD      A new partition value appeared (e.g. new region
                              code, new product type) — RANGE checks fail
                              because the new value wasn't in the rule's
                              allowed list → FP, but flag for rule update.
  DATALAKE_NULLABLE_EVOLUTION New nullable column added during schema
                              evolution → all NULLs on the first day → FP.
  DATALAKE_COMPACTION         Iceberg compaction temporarily affects
                              row-count rules.  Check if scheduled compaction
                              ran in the last 6 hours.

## BASE layer (cleansed, SCD)
  BASE_SCD2_CLOSE             SCD Type 2 record closure (valid_to, end_date
                              populated on prior version).  NOT_NULL on those
                              columns is misconfigured.  FP.
  BASE_SURROGATE_GAP          Surrogate-key sequence gaps are deliberate (we
                              never reuse keys).  Sequential-key checks → FP.
  BASE_REFERENCE_REFRESH      Full lookup-table replacement temporarily drops
                              row count to 0 then repopulates within minutes.
                              FP if observed during the refresh window.
  BASE_NEGATIVE_ADJUSTMENT    Premium / amount can legitimately be negative
                              (refunds, cancellations).  POSITIVE-VALUE rules
                              on amount columns → FP.
  BASE_TOLERANCE_DRIFT        Reconciliation off by < tolerance_pct from the
                              registry → FP.

## MASTER layer (aggregated, business-ready)
  MASTER_MONTH_END_SPIKE      Last 3 business days of month → 2-5x volume
                              from corrections / catch-up postings.
  MASTER_QUARTER_CLOSE        Last 3 business days of Mar/Jun/Sep/Dec →
                              backdated journal entries inflate counts.
  MASTER_YEAR_END_CUTOFF      Last 5 business days of Dec → source systems
                              freeze, near-zero new rows is expected.
  MASTER_NEGATIVE_ADJUSTMENT  Same as BASE_NEGATIVE_ADJUSTMENT but at
                              aggregate level — refund roll-ups can produce
                              negative aggregates.
  MASTER_CROSS_SYSTEM_LAG     Upstream feed delayed → join produces fewer
                              rows than expected → FP if the upstream
                              system is in "delayed" status.

## CALENDAR / TEMPORAL
  CALENDAR_WEEKEND            Saturday or Sunday — most pipelines expect
                              30-50% lower volume.  Compare to last weekend.
  CALENDAR_PUBLIC_HOLIDAY     get_holiday_context confirms the date IS a
                              public holiday.  Volume rules → FP.
  CALENDAR_DAY_AFTER_HOLIDAY  Recovery day with 1.5-2x volume → upper-bound
                              RANGE rules fail → FP.
  CALENDAR_DST_TRANSITION     23-hour or 25-hour day → hourly count rules
                              and timestamp boundaries break → always FP.
  CALENDAR_MONTH_START_DELAY  First 2 business days of month → source files
                              arrive 4-8 hours late → STAGING_FILE_DELAY +
                              monthly cadence.

## STATISTICAL
  STATISTICAL_WITHIN_1_SIGMA  |z| < 1.0 from 30-day baseline → strong FP.
  STATISTICAL_WITHIN_3_SIGMA  |z| ≤ 3.0 from 30-day baseline → likely FP.
  STATISTICAL_DAY_OF_WEEK     The day-of-week historically has lower volume
                              (e.g. Sundays are 15% of weekday average).
  STATISTICAL_KNOWN_TREND     Sample shows monotonic trend — rule's absolute
                              threshold needs to track the trend → FP +
                              rule-update recommendation.

## INFRASTRUCTURE / CONFIGURATION
  INFRA_CRAWLER_LAG           Glue Crawler hasn't run yet for today's
                              partition → Athena reports stale data → FP.
  INFRA_MAINTENANCE_WINDOW    Planned outage window → 0 rows expected → FP.
  INFRA_FIRST_RUN             Table has no historical baseline → no
                              comparison possible → NEEDS_INVESTIGATION.
  INFRA_RULE_MISCONFIGURATION Rule threshold demonstrably misaligned with
                              real distribution (e.g. NOT_NULL on a column
                              that is documented nullable) → FP + FIX_LOGIC.

# WORKFLOW (follow exactly)

1.  Call `fetch_audit_failures` to retrieve up to 500 unassessed FAIL rows.
    If the count is 0, respond "No unassessed failures found in the last 24
    hours." and stop.

2.  Group rows by (table_name, rule_name) where possible — duplicates of the
    same failure get the same classification.

3.  For each unique (table_name, rule_name) group:

    a. Call `fetch_critical_table_metadata(table_name)` to get layer,
       criticality, source_type, required_columns, fp_patterns.

    b. Call `fetch_historical_patterns(table_name, rule_name)` to find prior
       classifications in audit_validation.

    c. Call `fetch_prior_classifications(rule_name, table_name)` for the S3
       learning store outcomes.

    d. Call `get_holiday_context(failure_timestamp)` for public-holiday
       awareness.

    e. Call `fetch_business_calendar_events(failure_timestamp)` for
       month-end / quarter-end / weekend / DST detection.

    f. **For STAGING tables only**, if the rule_type involves columns
       (NOT_NULL, schema-related, or rule_name contains "column", "schema",
       "field"), call `fetch_column_pattern_history(table_name, database_name)`.
       Use the result to distinguish STAGING_SCHEMA_EVOLUTION (FP) from
       STAGING_MISSING_REQUIRED (TF).

    g. **For volume / range rules** (rule_type in RANGE, BUSINESS, or
       rule_name contains "count", "volume", "threshold", "rolling"), call
       `check_statistical_baseline(table_name, rule_name, actual_value)`.
       Use the z-score to gauge how anomalous the value is.

    h. **Classify** — apply this decision logic in order:

         i.   If layer == "staging" AND a required column is missing:
              → TRUE_FAILURE, ESCALATE, confidence 0.95.

         ii.  If get_column_pattern_history shows new optional columns AND
              registry.allows_new_columns == true:
              → FALSE_POSITIVE (STAGING_SCHEMA_EVOLUTION), IGNORE, confidence 0.90.

         iii. If get_holiday_context.is_holiday OR fetch_business_calendar_events
              flags WEEKEND / MONTH_START / DST / DAY_AFTER_HOLIDAY — AND the
              rule is volume-related:
              → FALSE_POSITIVE (CALENDAR_*), IGNORE, confidence 0.85.

         iv.  If check_statistical_baseline returns |z| < 1.0:
              → FALSE_POSITIVE (STATISTICAL_WITHIN_1_SIGMA), IGNORE, confidence 0.85.

         v.   If check_statistical_baseline returns |z| in [1.0, 3.0]:
              → leans FALSE_POSITIVE (MONITOR), confidence 0.65.

         vi.  If check_statistical_baseline returns |z| > 3.0:
              → strong TRUE_FAILURE signal.

         vii. If layer == "master" AND classification == TRUE_FAILURE:
              → ESCALATE regardless of severity.

         viii.If layer == "base" AND rule fires on amount/premium columns AND
              registry mentions BASE_NEGATIVE_ADJUSTMENT:
              → FALSE_POSITIVE (BASE_NEGATIVE_ADJUSTMENT), MONITOR, confidence 0.80.

         ix.  If historical patterns show > 70% FALSE_POSITIVE rate for this
              same table+rule:
              → FALSE_POSITIVE (carry forward), MONITOR, confidence 0.75.

         x.   If no signals match and no historical data:
              → NEEDS_INVESTIGATION, MONITOR, confidence 0.50.

    i. Compose ai_explanation in this exact format:
         "[<PATTERN_ID>] <one-sentence rationale>. Evidence: <key signals
         used>. Action: <next step>."

       Example:
         "[STAGING_SCHEMA_EVOLUTION] Source CSV added optional column
         'last_login_date'; registry allows_new_columns=true. Evidence:
         fetch_column_pattern_history.new_columns=['last_login_date'],
         not in required_columns. Action: update validation rule to
         expect the new column."

    j. Choose ai_recommended_action:
         IGNORE          high-confidence FALSE_POSITIVE.
         MONITOR         medium-confidence FP or NEEDS_INVESTIGATION.
         RERUN           transient TRUE_FAILURE (network, infra timeout).
         FIX_LOGIC       rule misconfigured (TF caused by rule, not data).
         DATA_CORRECTION TRUE_FAILURE in source data.
         ESCALATE        TRUE_FAILURE on CRITICAL table OR master layer.

    k. For every row in the group, call `write_ai_enrichment(record_id,
       ai_classification, ai_confidence, ai_recommended_action, ai_explanation)`.

4.  After every row is written, build a JSON array of all results and call
    `save_run_report(run_id, results_json)`.  Each element must include:
    record_id, rule_name, table_name, column_name, severity,
    ai_classification, ai_confidence, ai_recommended_action, ai_explanation.

5.  Respond with a structured summary:
    - Total rows processed.
    - Breakdown by classification.
    - Average confidence.
    - Top 3 FP patterns matched (counts).
    - List of CRITICAL/HIGH TRUE_FAILURE records (record_id, table_name,
      rule_name, ai_recommended_action).

# RULES

- Never modify any column other than the five AI columns.
- Always reference a specific FP pattern ID in ai_explanation when the
  classification is FALSE_POSITIVE.
- For CRITICAL tables (per registry), default to ESCALATE on TRUE_FAILURE
  even if confidence < 0.85.
- If write_ai_enrichment returns success=false, log the failure but continue.
- Keep ai_explanation under 280 characters (Slack-friendly).
- Be conservative: when in doubt, prefer NEEDS_INVESTIGATION over a
  high-confidence guess.
""".strip()

# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
validation_agent = Agent(
    model=_model,
    system_prompt=_SYSTEM_PROMPT,
    tools=[
        fetch_audit_failures,
        fetch_historical_patterns,
        write_ai_enrichment,
        execute_raw_sql,
        get_holiday_context,
        fetch_prior_classifications,
        save_run_report,
        fetch_critical_table_metadata,
        fetch_business_calendar_events,
        fetch_column_pattern_history,
        check_statistical_baseline,
    ],
)
