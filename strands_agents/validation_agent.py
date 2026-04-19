"""
strands_agents/validation_agent.py
====================================
Strands Agent that classifies ETL audit_validation FAIL records.

The agent:
  1. Fetches unclassified FAIL rows from Athena.
  2. For each batch, checks holiday context and historical patterns.
  3. Classifies each record as TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION.
  4. Writes enrichment back to the audit table.
  5. Returns a structured summary.

Usage (standalone):
    from strands_agents.validation_agent import create_validation_agent
    agent = create_validation_agent()
    result = agent("Analyze failures from the last 24 hours")
"""

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.tools.athena_tools import (
    fetch_audit_failures,
    fetch_historical_patterns,
    write_ai_enrichment,
    execute_raw_sql,
)
from strands_agents.tools.holiday_tools import get_holiday_context, list_upcoming_holidays
from strands_agents.tools.validation_tools import (
    fetch_prior_classifications,
    get_batch_summary,
    save_run_report,
)

MODEL_ID = os.environ.get("BEDROCK_MODEL_ID",
                           "anthropic.claude-3-sonnet-20240229-v1:0")

VALIDATION_SYSTEM_PROMPT = """
You are an expert ETL data quality analyst for a large enterprise data platform.

Your job is to review audit_validation FAIL records and classify each one as:
  - TRUE_FAILURE     — a real data quality issue requiring immediate action
  - FALSE_POSITIVE   — expected behaviour (holiday, known anomaly, first-run, etc.)
  - NEEDS_INVESTIGATION — ambiguous; needs human review

For each record you MUST:
1. Call get_holiday_context with the run_date to check for volume suppressions.
2. Call fetch_historical_patterns to see the rolling 60-day baseline.
3. Call fetch_prior_classifications to see how similar failures were classified before.
4. Based on all context, call write_ai_enrichment to persist your decision.

Classification rules:
  - If it's a holiday and the rule is volume/row-count related → FALSE_POSITIVE (unless below 50% of expected)
  - If the same rule ALWAYS fails on this day → FALSE_POSITIVE
  - If the deviation is >5σ from the 60-day mean → TRUE_FAILURE
  - If you see double-loading (>150% of yesterday) → TRUE_FAILURE
  - For PII / schema failures, always → TRUE_FAILURE regardless of holiday
  - When unsure → NEEDS_INVESTIGATION with a clear note

After processing all records, call get_batch_summary to produce the final tally.
Set your confidence between 0.0 (random guess) and 1.0 (certain).
Keep explanations to one crisp sentence — they appear in Slack notifications.
""".strip()


def create_validation_agent(model_id: str = MODEL_ID) -> Agent:
    """Return a configured Strands Agent for validation analysis."""
    model = BedrockModel(model_id=model_id)
    return Agent(
        model=model,
        tools=[
            fetch_audit_failures,
            fetch_historical_patterns,
            write_ai_enrichment,
            execute_raw_sql,
            get_holiday_context,
            list_upcoming_holidays,
            fetch_prior_classifications,
            get_batch_summary,
            save_run_report,
        ],
        system_prompt=VALIDATION_SYSTEM_PROMPT,
    )
