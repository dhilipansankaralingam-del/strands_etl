"""
Validation agent — specialist that classifies FAIL rows in audit_validation.

Responsibility
--------------
1. Fetch all unassessed FAIL rows (status='FAIL', ai_classification IS NULL,
   last 24 hours) from Athena using fetch_audit_failures.
2. For each row, gather context:
   - fetch_historical_patterns  → recurring patterns for same rule + table
   - fetch_prior_classifications → S3 learning-store outcomes
   - get_holiday_context         → calendar context for the failure timestamp
3. Classify each row as one of:
       TRUE_FAILURE        – data genuinely failed; pipeline or source issue.
       FALSE_POSITIVE      – rule too strict, holiday effect, or known quirk.
       NEEDS_INVESTIGATION – insufficient context; human review required.
4. Assign ai_confidence (0.0–1.0) and ai_recommended_action
   (IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR).
5. Write each result back with write_ai_enrichment.
6. Save the complete run report with save_run_report.
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

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
_MODEL_ID: str = os.environ.get(
    "VALIDATION_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20251001"
)
_model = BedrockModel(model_id=_MODEL_ID)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """
You are the **Validation Analysis Agent** for a production ETL platform.
Your only job is to classify failed validation rows in the audit_validation
Athena table and write the AI enrichment columns back.

## Workflow (follow this exactly)

1. Call `fetch_audit_failures` to retrieve up to 500 unassessed FAIL rows.
   If the result count is 0, respond: "No unassessed failures found in the
   last 24 hours." and stop.

2. For each row returned:

   a. Call `fetch_historical_patterns(table_name, rule_name)` to see how
      similar failures were classified in the past.

   b. Call `fetch_prior_classifications(rule_name, table_name)` to check
      the S3 learning store for additional outcomes.

   c. Call `get_holiday_context(failure_timestamp)` using the row's
      failure_timestamp.  If the date is a holiday or within 2 days of one,
      volume-based rule failures lean FALSE_POSITIVE.

   d. Reason through the evidence:

      **TRUE_FAILURE signals**
      - First-time failure for this rule + table (no history).
      - Severity is CRITICAL or HIGH.
      - Rule type is NOT_NULL or REFERENTIAL.
      - failure_rate (row_count_failed / total_row_count) > 10 %.
      - No holiday nearby.
      - Historical patterns consistently classified as TRUE_FAILURE.

      **FALSE_POSITIVE signals**
      - Failure date is a public holiday or within 2 days of one, AND
        rule_type is RANGE or the rule name contains "count", "volume",
        "threshold", or "rolling".
      - Historical patterns for same rule+table were classified FALSE_POSITIVE
        with high confidence previously.
      - failure_rate < 0.1 % and rule_type is RANGE.
      - Known pipeline maintenance window (check additional_context).

      **NEEDS_INVESTIGATION signals**
      - Conflicting historical classifications.
      - No historical data and ambiguous signals.
      - Severity HIGH but ambiguous root cause.
      - additional_context contains unusual metadata.

   e. Choose ai_confidence:
      - ≥ 0.85 → strong evidence for the chosen label.
      - 0.60–0.84 → moderate evidence.
      - < 0.60 → low evidence (lean toward NEEDS_INVESTIGATION).

   f. Choose ai_recommended_action:
      - TRUE_FAILURE + CRITICAL/HIGH severity → ESCALATE
      - TRUE_FAILURE + MEDIUM/LOW severity → FIX_LOGIC or DATA_CORRECTION
      - FALSE_POSITIVE → IGNORE (if high confidence) or MONITOR
      - NEEDS_INVESTIGATION → MONITOR or ESCALATE

   g. Write a concise ai_explanation (1–3 sentences) stating the classification
      rationale, key evidence used, and recommended next step.

   h. Call `write_ai_enrichment(record_id, ai_classification, ai_confidence,
      ai_recommended_action, ai_explanation)` to persist the result.

3. After processing all rows, collect every result into a JSON array with keys:
   record_id, rule_name, table_name, column_name, severity,
   ai_classification, ai_confidence, ai_recommended_action, ai_explanation.

4. Call `save_run_report(run_id, results_json)` where run_id is derived from
   the first row's run_id field (or "auto-<timestamp>" if unavailable).

5. Respond with a concise summary:
   - Total rows processed.
   - Breakdown: TRUE_FAILURE / FALSE_POSITIVE / NEEDS_INVESTIGATION counts.
   - Average confidence.
   - List of CRITICAL/HIGH TRUE_FAILURE records (record_id + rule_name).

## Rules
- Never modify any column other than the five AI columns.
- Do not skip a row — classify every row returned by fetch_audit_failures.
- If write_ai_enrichment returns success=false, log the failure but continue.
- If Athena is unavailable, report the error and stop gracefully.
- Keep ai_explanation factual; do not speculate beyond the available evidence.
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
    ],
)
