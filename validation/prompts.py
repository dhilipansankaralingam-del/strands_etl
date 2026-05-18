"""
Prompt library for the Strands Validation Analysis Agent.

Four prompt categories:
  1. SYSTEM_PROMPT      – primary persona injected into every Bedrock call
  2. DECISION_PROMPT    – classifies a single failed record
  3. LEARNING_PROMPT    – extracts patterns from resolved outcomes
  4. SQL_PROMPT         – converts natural-language queries to Athena SQL
"""

from typing import Dict, Any, List, Optional
import json
from datetime import datetime, date as date_type


# ---------------------------------------------------------------------------
# 1.  Primary system prompt – injected as the agent's standing persona
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are the **Strands Validation Intelligence Agent**, a production-grade AI assistant
embedded in a data-quality and ETL governance framework.

## Your Role
You analyse failed data-validation records to determine:
- Whether a failure is a TRUE_FAILURE, FALSE_POSITIVE, or NEEDS_INVESTIGATION
- The root cause of each failure
- The best corrective action to take
- Patterns across many failures that suggest systemic issues

## Confidence Scoring Guide
Your confidence score (0.0 – 1.0) must reflect actual certainty:

| Score range | Label  | When to use                                                         |
|-------------|--------|---------------------------------------------------------------------|
| 0.85 – 1.0  | HIGH   | Multiple corroborating signals (historical pattern + data profile + rule type all agree) |
| 0.60 – 0.84 | MEDIUM | Two signals agree but one is ambiguous or missing                   |
| 0.00 – 0.59 | LOW    | Conflicting signals, first occurrence, or insufficient data         |

**Factors that RAISE confidence:**
- Same rule+table combination has failed before with same root cause (historical match)
- Data profile confirms the anomaly (e.g. null flood corroborates a NOT_NULL failure)
- Failure rate > 10% (statistically significant)
- Configured business rule explicitly describes this scenario

**Factors that LOWER confidence:**
- First occurrence (no historical data)
- Low failure rate (< 0.1%) — could be sampling noise
- Rule type is BUSINESS or UNKNOWN (semantics are ambiguous)
- Historical outcomes for this rule are split (mix of TRUE_FAILURE and FALSE_POSITIVE)
- Data profile is healthy — anomaly may be in the rule, not the data

## How to Use Configured Rules
When `configured_rules` are provided, treat them as authoritative ground truth
for this table. A failure that matches a configured rule's condition is almost
certainly TRUE_FAILURE unless the data profile contradicts it.

## Your Reasoning Approach
1. **Read configured rules first** — do they explicitly describe this failure?
2. **Check historical outcomes** — same rule, same table. What happened before?
   - Mostly TRUE_FAILURE → lean TRUE_FAILURE, raise confidence
   - Mostly FALSE_POSITIVE → lean FALSE_POSITIVE, raise confidence
   - Mixed → lean NEEDS_INVESTIGATION, lower confidence
3. **Check the data profile trend** — is the table health score worsening?
   - Degrading trend → more likely TRUE_FAILURE
   - Stable/improving trend → consider FALSE_POSITIVE
4. **Check failure rate** — < 0.1% may be noise; > 10% is significant
5. **Synthesise** — combine all signals into a final classification + confidence

## Output Format
Always respond with a valid JSON object:
{
  "classification": "TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION",
  "confidence": <float 0.0-1.0>,
  "confidence_reasoning": "<which signals raised/lowered your confidence and why>",
  "explanation": "<clear 2-3 sentence explanation a non-engineer can understand>",
  "root_causes": ["<specific cause 1>", "<specific cause 2>"],
  "recommended_action": "IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR",
  "suggested_next_steps": ["<concrete step 1>", "<concrete step 2>"],
  "validation_sql": "<Athena SQL to verify this finding>",
  "historical_pattern_used": true | false,
  "configured_rule_matched": "<rule_name or null>"
}

If you cannot produce valid JSON, wrap your answer in <json>...</json> tags.
"""

# ---------------------------------------------------------------------------
# 2.  Decision prompt – classification of one failed ValidationRecord
# ---------------------------------------------------------------------------

def build_decision_prompt(
    record: Dict[str, Any],
    historical_outcomes: List[Dict[str, Any]],
    configured_rules: Optional[List[Dict[str, Any]]] = None,
    health_score_history: Optional[List[Dict[str, Any]]] = None,
    run_date: Optional[str] = None,
) -> str:
    """
    Build the full decision prompt with:
    - The failed record
    - Configured business rules for this table (authoritative ground truth)
    - Historical similar failures + their resolutions
    - Historical health score trend (improving / degrading / stable)
    - Computed failure statistics for confidence calibration
    """
    total  = record.get("total_row_count", 0)
    failed = record.get("row_count_failed", 1)
    failure_rate = failed / total if total > 0 else 0.0

    # --- Historical outcome analysis
    true_failures   = sum(1 for o in historical_outcomes if o.get("classification") == "TRUE_FAILURE")
    false_positives = sum(1 for o in historical_outcomes if o.get("classification") == "FALSE_POSITIVE")
    needs_inv       = sum(1 for o in historical_outcomes if o.get("classification") == "NEEDS_INVESTIGATION")
    correct_actions = sum(1 for o in historical_outcomes if o.get("was_correct") is True)
    historical_total = len(historical_outcomes)

    if historical_total > 0:
        historical_summary = (
            f"{historical_total} similar past failure(s): "
            f"{true_failures} TRUE_FAILURE, {false_positives} FALSE_POSITIVE, "
            f"{needs_inv} NEEDS_INVESTIGATION.  "
            f"AI was correct in {correct_actions}/{historical_total} resolved cases."
        )
        dominant = max(
            [("TRUE_FAILURE", true_failures),
             ("FALSE_POSITIVE", false_positives),
             ("NEEDS_INVESTIGATION", needs_inv)],
            key=lambda x: x[1]
        )
        dominant_pct = dominant[1] / historical_total
        historical_signal = (
            f"Historical dominant classification: **{dominant[0]}** "
            f"({dominant_pct:.0%} of past cases)."
        )
    else:
        historical_summary = "No historical data available for this rule+table combination."
        historical_signal  = "First occurrence — base confidence on rule type and failure rate alone."

    # --- Health score trend
    if health_score_history and len(health_score_history) >= 2:
        scores  = [h.get("data_health_score", 100) for h in health_score_history[-5:]]
        trend   = scores[-1] - scores[0]
        trend_s = f"DEGRADING ({trend:+.0f} pts)" if trend < -5 else (
                  f"IMPROVING ({trend:+.0f} pts)" if trend > 5 else "STABLE")
        health_context = (
            f"Last {len(scores)} profiling runs: scores = {scores}. "
            f"Trend: **{trend_s}**."
        )
    else:
        health_context = "No historical health score data available."

    # --- Day-of-week context (used for seasonal pattern matching)
    effective_date = run_date or record.get("failure_timestamp", "")[:10]
    try:
        dt = datetime.strptime(effective_date[:10], "%Y-%m-%d")
        day_of_week = dt.strftime("%A")          # e.g. "Saturday"
        day_of_month = str(dt.day)               # e.g. "31"
        day_of_week_num = dt.weekday()           # 0=Mon … 6=Sun
        is_weekend = day_of_week_num >= 5
    except Exception:
        day_of_week = "Unknown"
        day_of_month = "?"
        is_weekend = False

    date_context = (
        f"Failure date: **{effective_date}** ({day_of_week}, day-of-month={day_of_month}, "
        f"{'WEEKEND' if is_weekend else 'WEEKDAY'})"
    )

    # --- Parse known_seasonal_patterns from any configured rule
    seasonal_instructions: List[str] = []
    if configured_rules:
        for rule in configured_rules:
            for pattern in rule.get("known_seasonal_patterns", []):
                pattern_days     = pattern.get("days", [])
                pattern_dom      = [str(d) for d in pattern.get("days_of_month", [])]
                ai_instruction   = pattern.get("ai_instruction", "")
                pattern_name     = pattern.get("pattern", "")
                # Check if today matches the pattern
                day_match = day_of_week in pattern_days if pattern_days else False
                dom_match = day_of_month in pattern_dom if pattern_dom else False
                if day_match or dom_match:
                    seasonal_instructions.append(
                        f"  ⚠ Pattern **{pattern_name}** MATCHES today ({day_of_week}, day {day_of_month}): "
                        f"{ai_instruction}"
                    )
                else:
                    seasonal_instructions.append(
                        f"  ○ Pattern **{pattern_name}** does NOT match today "
                        f"(applies to days={pattern_days or pattern_dom}): {ai_instruction}"
                    )

    if seasonal_instructions:
        seasonal_section = (
            "\n## Seasonal Pattern Context\n"
            + date_context + "\n"
            + "\n".join(seasonal_instructions)
            + "\n→ If a MATCHING pattern's ai_instruction calls this a FALSE_POSITIVE, "
              "apply that instruction and lower confidence accordingly.\n"
        )
    else:
        seasonal_section = f"\n## Date Context\n{date_context}\n"

    # --- Configured rules section
    if configured_rules:
        rules_text = json.dumps(configured_rules, indent=2, default=str)
        rules_section = f"""
## Configured Business Rules for {record.get('database_name','')}.{record.get('table_name','')}
These rules were defined by the data engineering team and are authoritative:
```json
{rules_text}
```
→ If the failing record matches a configured rule's `condition`, classify as TRUE_FAILURE
  unless the data profile provides strong contradicting evidence.
{seasonal_section}"""
    else:
        rules_section = f"\n## Configured Business Rules\n(none configured for this table)\n{seasonal_section}"

    return f"""
## Failed Validation Record
```json
{json.dumps(record, indent=2, default=str)}
```

## Rule Statistics
- Rule type    : {record.get('rule_type', 'UNKNOWN')}
- Severity     : {record.get('severity', 'MEDIUM')}
- Failure rate : {failure_rate:.2%}  ({failed} of {total} rows)
- Column       : {record.get('column_name', 'n/a')}
- Constraint   : {record.get('expected_constraint', 'n/a')}
- Failed value : {record.get('failed_value', 'n/a')}
{rules_section}
## Historical Analysis (confidence calibration)
{historical_summary}
{historical_signal}

### Last {len(historical_outcomes)} Similar Failures (most recent first)
```json
{json.dumps(historical_outcomes[:10], indent=2, default=str)}
```

## Table Health Score Trend
{health_context}

## Confidence Calibration Checklist
Before deciding confidence, answer these:
- [ ] Does a configured rule explicitly describe this failure?  → +0.15 confidence if yes
- [ ] Do historical outcomes strongly agree on one classification? → +0.15 if ≥80% agree
- [ ] Is failure_rate > 5%? → +0.10 if yes (statistically meaningful)
- [ ] Is the table health trend DEGRADING? → +0.10 if yes (corroborates TRUE_FAILURE)
- [ ] Is this the first occurrence? → -0.20 if yes (insufficient evidence)
- [ ] Is rule_type BUSINESS/UNKNOWN? → -0.10 (ambiguous semantics)

## Your Task
1. Check if this failure matches any configured rule above.
2. Analyse the historical pattern — what did similar failures turn out to be?
3. Factor in the health score trend.
4. Apply the confidence checklist.
5. Output your classification + confidence with explicit reasoning.

Respond ONLY with the JSON schema in the system prompt.
""".strip()


# ---------------------------------------------------------------------------
# 3.  Learning prompt – extract patterns from a batch of resolved outcomes
# ---------------------------------------------------------------------------

LEARNING_PROMPT_TEMPLATE = """
## Resolved Validation Outcomes (batch)

The following {outcome_count} validation failures have been resolved by human operators
or automated pipelines. Your job is to extract reusable patterns and heuristics.

```json
{outcomes_json}
```

## Analysis Tasks
1. **False-positive patterns**: Which rule types / table+column combinations
   consistently produce FALSE_POSITIVE classifications?  Why?
2. **True-failure signatures**: What data characteristics reliably signal TRUE_FAILURE?
3. **Action effectiveness**: Which recommended actions led to `was_correct = true`?
4. **Emerging issues**: Are there clusters of failures that suggest a systemic pipeline
   or source-data problem?
5. **Confidence calibration**: When was the agent under- or over-confident?

## Output Format
Respond with a JSON object:
{{
  "false_positive_patterns": [
    {{"rule_type": "...", "table_name": "...", "pattern": "...", "frequency": <int>}}
  ],
  "true_failure_signatures": ["<signature 1>", ...],
  "effective_actions": {{"action": "observation"}},
  "systemic_issues": ["<issue description>"],
  "confidence_calibration_notes": "<text>",
  "learning_vector_summary": "<one-paragraph summary>"
}}
"""


def build_learning_prompt(outcomes: List[Dict[str, Any]]) -> str:
    return LEARNING_PROMPT_TEMPLATE.format(
        outcome_count=len(outcomes),
        outcomes_json=json.dumps(outcomes, indent=2, default=str),
    )


# ---------------------------------------------------------------------------
# 4.  SQL generation prompt – NL → Athena SQL
# ---------------------------------------------------------------------------

SQL_PROMPT_TEMPLATE = """
## Athena Schema Context

Database  : {database}
Tables and columns available:
```json
{schema_json}
```

## Conversation History (last 3 turns)
{history_text}

## User Query
"{nl_query}"

## Your Task
Convert the user's natural-language query into a valid **AWS Athena SQL** statement
(Presto/Trino dialect).

Rules:
- Use only tables and columns listed in the schema above.
- Always qualify table names with the database: `{database}.<table>`.
- For date filters use `date_parse(col, '%Y-%m-%d')` or `CAST(col AS DATE)`.
- Limit results to 1000 rows unless the user explicitly asks for more.
- If the query is ambiguous, choose the most likely interpretation and add a comment.
- If the query cannot be answered from the schema, return:
  {{"error": "Cannot answer: <reason>"}}

Respond ONLY with a JSON object:
{{
  "sql": "<complete Athena SQL statement>",
  "explanation": "<one-sentence description of what the query returns>",
  "tables_used": ["<table1>", ...],
  "assumed_intent": "<what you interpreted the user to want>"
}}
"""


def build_sql_prompt(
    nl_query: str,
    database: str,
    schema: Dict[str, Any],
    conversation_history: List[Dict[str, str]] | None = None,
) -> str:
    history_lines: List[str] = []
    if conversation_history:
        for turn in conversation_history[-3:]:
            role = turn.get("role", "user").upper()
            content = turn.get("content", "")
            history_lines.append(f"[{role}] {content[:300]}")
    history_text = "\n".join(history_lines) if history_lines else "(none)"

    return SQL_PROMPT_TEMPLATE.format(
        database=database,
        schema_json=json.dumps(schema, indent=2, default=str),
        history_text=history_text,
        nl_query=nl_query,
    )


# ---------------------------------------------------------------------------
# 5.  Batch-summary prompt – summarise multiple AnalysisResults
# ---------------------------------------------------------------------------

BATCH_SUMMARY_PROMPT_TEMPLATE = """
## Batch Validation Analysis Summary Request

You have analysed {result_count} failed validation records. Here are the results:

```json
{results_json}
```

Provide a concise executive summary covering:
1. Overall failure breakdown (TRUE_FAILURE / FALSE_POSITIVE / NEEDS_INVESTIGATION counts)
2. Top 3 most impactful issues requiring immediate attention
3. Recurring patterns across multiple records
4. Recommended prioritisation order for remediation
5. Estimated data-quality score (0-100) for the current pipeline run

Respond with JSON:
{{
  "true_failure_count": <int>,
  "false_positive_count": <int>,
  "needs_investigation_count": <int>,
  "data_quality_score": <int 0-100>,
  "top_issues": ["<issue 1>", "<issue 2>", "<issue 3>"],
  "recurring_patterns": ["<pattern>"],
  "remediation_priority": ["<record_id or rule_name>"],
  "executive_summary": "<2-3 sentence summary>"
}}
"""


def build_batch_summary_prompt(results: List[Dict[str, Any]]) -> str:
    return BATCH_SUMMARY_PROMPT_TEMPLATE.format(
        result_count=len(results),
        results_json=json.dumps(results, indent=2, default=str),
    )


# ---------------------------------------------------------------------------
# 6.  Data Profiling system prompt – used by DataProfilerAgent
# ---------------------------------------------------------------------------

DATA_PROFILER_SYSTEM_PROMPT = """
You are the **Strands Data Profiler Agent**, an expert in data quality, statistical analysis,
and anomaly detection embedded in an AWS-native ETL governance platform.

## Your Role
You receive raw column statistics, KPI snapshots, and schema metadata for data tables.
Your job is to:
1. Identify statistical anomalies using z-scores, IQR fences, and pattern heuristics.
2. Detect edge cases that simple validation rules miss (see below).
3. Generate a concise, prioritised health report with Athena SQL for verification.
4. Score overall table health (0-100).

## Edge Cases You Must Always Check
- **NULL_FLOOD**: null_pct jumped > 20 percentage points vs baseline.
- **CARDINALITY_EXPLOSION**: distinct_count grew > 3x vs baseline in a supposedly stable dimension.
- **TEMPORAL_ANOMALY**: max timestamp is older than previous run's max (clock rollback / late data reprocessed).
- **DISTRIBUTION_SKEW**: a single value accounts for > 80% of rows when historically it was < 20%.
- **SCHEMA_DRIFT**: columns present in baseline but absent now, or new columns not in baseline.
- **STALE_PARTITION**: freshness_hours > SLA threshold (default 26h for daily tables).
- **DUPLICATE_KEY_STORM**: duplicate_key_pct > 0.5% for a declared primary-key column.
- **BOUNDARY_EXPLOSION**: max_value > 10× historical_max_value (numeric columns).
- **ZERO_INFLATION**: for a metric that should never be zero, zero_pct > 1%.
- **ENCODING_ROT**: sample values contain non-printable ASCII or replacement characters (U+FFFD).

## Output Format
Always respond with a valid JSON object:
{
  "data_health_score": <int 0-100>,
  "anomalies": [
    {
      "column_name": "<col or null for table-level>",
      "anomaly_type": "<one of the 10 types above>",
      "severity": "LOW | MEDIUM | HIGH | CRITICAL",
      "description": "<one clear sentence>",
      "observed_value": "<value that triggered the detection>",
      "expected_range": "<what was expected>",
      "z_score": <float or null>,
      "detection_sql": "<Athena SQL to confirm>",
      "recommended_action": "<concrete next step>"
    }
  ],
  "kpi_insights": ["<insight 1>", "<insight 2>"],
  "ai_summary": "<2-3 sentence health narrative>"
}
"""

# ---------------------------------------------------------------------------
# 7.  Column profile anomaly prompt
# ---------------------------------------------------------------------------

COLUMN_ANOMALY_PROMPT_TEMPLATE = """
## Table: {database}.{table_name}
## Run ID: {run_id}

### Column Profiles (current run)
```json
{column_profiles_json}
```

### KPI Snapshot
```json
{kpi_snapshot_json}
```

### Schema Drift Summary
- Columns added since last run  : {added_columns}
- Columns removed since last run: {removed_columns}
- Columns with changed type     : {retyped_columns}

### Freshness SLA
- Expected max freshness (hours): {freshness_sla_hours}
- Observed freshness (hours)    : {observed_freshness_hours}

### Historical Baselines (previous run stats)
```json
{baselines_json}
```

## Your Task
1. Compare current stats against baselines and flag ALL anomalies.
2. Score overall table health 0-100 (deduct points per anomaly by severity:
   CRITICAL=-25, HIGH=-15, MEDIUM=-8, LOW=-3).
3. Produce Athena SQL (`{database}.{table_name}`) for every anomaly > MEDIUM.

Respond ONLY with the JSON schema defined in the system prompt.
"""


def build_column_anomaly_prompt(
    database: str,
    table_name: str,
    run_id: str,
    column_profiles: List[Dict[str, Any]],
    kpi_snapshot: Dict[str, Any],
    baselines: Dict[str, Any],
    added_columns: List[str],
    removed_columns: List[str],
    retyped_columns: List[str],
    freshness_sla_hours: float = 26.0,
    observed_freshness_hours: Optional[float] = None,
) -> str:
    return COLUMN_ANOMALY_PROMPT_TEMPLATE.format(
        database=database,
        table_name=table_name,
        run_id=run_id,
        column_profiles_json=json.dumps(column_profiles, indent=2, default=str),
        kpi_snapshot_json=json.dumps(kpi_snapshot, indent=2, default=str),
        added_columns=added_columns or "(none)",
        removed_columns=removed_columns or "(none)",
        retyped_columns=retyped_columns or "(none)",
        freshness_sla_hours=freshness_sla_hours,
        observed_freshness_hours=observed_freshness_hours if observed_freshness_hours is not None else "unknown",
        baselines_json=json.dumps(baselines, indent=2, default=str),
    )


# ---------------------------------------------------------------------------
# 8.  Audit action prompt – decide what to DO after classification
# ---------------------------------------------------------------------------

AUDIT_ACTION_PROMPT_TEMPLATE = """
## Validation Audit Failure — Action Decision

### Failure Record
```json
{record_json}
```

### AI Analysis Result
```json
{analysis_json}
```

### Data Profile Context (table where failure occurred)
```json
{profile_json}
```

### Available Actions
| Action           | When to use                                                         |
|------------------|---------------------------------------------------------------------|
| RERUN            | Transient infrastructure blip; profile looks healthy                |
| DATA_CORRECTION  | Genuine bad data; provide fix SQL                                   |
| FIX_LOGIC        | Validation rule itself is wrong; suggest rule change                |
| ESCALATE         | Critical failure; data unusable; needs human intervention           |
| MONITOR          | Borderline; watch next 3 runs before acting                         |
| IGNORE           | Confirmed false-positive with high confidence                       |

## Your Task
Given the failure details, AI classification, and data profile context:
1. Confirm or override the recommended action.
2. If DATA_CORRECTION: generate the exact Athena UPDATE / INSERT OVERWRITE SQL.
3. If FIX_LOGIC: describe the exact rule change needed.
4. If RERUN: specify which Glue job / pipeline step to re-trigger.
5. If ESCALATE: draft the PagerDuty / SNS alert message.

Respond with:
{{
  "final_action": "<ACTION>",
  "confidence": <float>,
  "rationale": "<why this action>",
  "action_payload": {{
    "fix_sql": "<SQL or null>",
    "rule_change": "<description or null>",
    "rerun_job": "<job name or null>",
    "alert_message": "<text or null>",
    "monitor_schedule": "<cron or null>"
  }},
  "data_profile_signals_used": ["<signal 1>", "<signal 2>"]
}}
"""


def build_audit_action_prompt(
    record: Dict[str, Any],
    analysis: Dict[str, Any],
    profile: Dict[str, Any],
) -> str:
    return AUDIT_ACTION_PROMPT_TEMPLATE.format(
        record_json=json.dumps(record, indent=2, default=str),
        analysis_json=json.dumps(analysis, indent=2, default=str),
        profile_json=json.dumps(profile, indent=2, default=str),
    )


# ---------------------------------------------------------------------------
# 9.  Profiling executive summary prompt
# ---------------------------------------------------------------------------

PROFILING_SUMMARY_PROMPT_TEMPLATE = """
## Multi-Table Data Profiling Summary

{table_count} tables were profiled in this pipeline run.

### Per-Table Results
```json
{profiles_json}
```

### Aggregate Statistics
- Total anomalies detected : {total_anomalies}
- Critical anomalies       : {critical_count}
- Tables with schema drift : {schema_drift_count}
- Average health score     : {avg_health_score:.1f}/100

## Your Task
Produce a C-level executive summary covering:
1. Overall data estate health
2. Tables needing immediate attention (critical/high anomalies)
3. Systemic patterns across tables (e.g. all tables showing STALE_PARTITION)
4. Recommended remediation order and estimated effort
5. Week-over-week trend (use health scores)

Respond with:
{{
  "overall_health_score": <int>,
  "immediate_action_tables": ["<table>"],
  "systemic_patterns": ["<pattern>"],
  "remediation_plan": [
    {{"table": "...", "action": "...", "priority": "P1|P2|P3", "effort": "LOW|MEDIUM|HIGH"}}
  ],
  "executive_summary": "<3-4 sentence narrative>",
  "week_over_week_trend": "IMPROVING | STABLE | DEGRADING"
}}
"""


def build_profiling_summary_prompt(profiles: List[Dict[str, Any]]) -> str:
    total_anomalies = sum(len(p.get("anomalies", [])) for p in profiles)
    critical_count = sum(
        sum(1 for a in p.get("anomalies", []) if a.get("severity") == "CRITICAL")
        for p in profiles
    )
    schema_drift_count = sum(1 for p in profiles if p.get("schema_drift_detected"))
    scores = [p.get("data_health_score", 100) for p in profiles]
    avg_health_score = sum(scores) / len(scores) if scores else 100.0

    return PROFILING_SUMMARY_PROMPT_TEMPLATE.format(
        table_count=len(profiles),
        profiles_json=json.dumps(profiles, indent=2, default=str),
        total_anomalies=total_anomalies,
        critical_count=critical_count,
        schema_drift_count=schema_drift_count,
        avg_health_score=avg_health_score,
    )
