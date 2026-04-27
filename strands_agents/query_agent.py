"""
Query agent — specialist that translates natural language into Athena SQL.

Responsibility
--------------
Accept a plain-English question about the audit_validation table (or any
table in audit_db) and:
1. Derive the correct SQL using its own LLM reasoning.
2. Optionally call execute_raw_sql to run the query and return results.
3. Present the SQL and results in a clear, readable format.

This agent is richer than the run_nl_query tool: it supports multi-turn
refinement, schema exploration, and result explanation.
"""

from __future__ import annotations

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.tools.athena_tools import (
    execute_raw_sql,
    run_nl_query,
    fetch_audit_failures,
    fetch_historical_patterns,
)

# ---------------------------------------------------------------------------
# Model — Haiku is fast and sufficient for NL-to-SQL
# ---------------------------------------------------------------------------
_MODEL_ID: str = os.environ.get(
    "QUERY_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001"
)
_model = BedrockModel(model_id=_MODEL_ID)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """
You are the **Query Agent** for the ETL audit platform.
You translate natural-language questions into correct Athena SQL and execute
them against the audit_db Glue Catalog.

## Primary table: audit_db.audit_validation

Columns:
    record_id              STRING        — primary key
    run_id                 STRING
    rule_name              STRING
    table_name             STRING
    column_name            STRING
    database_name          STRING
    pipeline_name          STRING
    severity               STRING        — LOW | MEDIUM | HIGH | CRITICAL
    rule_type              STRING        — NOT_NULL | RANGE | REGEX | REFERENTIAL | BUSINESS
    status                 STRING        — PASS | FAIL | WARN
    failed_value           STRING
    expected_constraint    STRING
    row_count_failed       BIGINT
    total_row_count        BIGINT
    failure_timestamp      TIMESTAMP
    ai_classification      STRING        — TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION
    ai_confidence          DOUBLE        — 0.0 – 1.0
    ai_recommended_action  STRING        — IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR
    ai_explanation         STRING
    ai_analysed_at         TIMESTAMP

## Reference table: audit_db.etl_orchestrator_audit

Columns include: run_id, event_timestamp, event_type, source_label, status,
validation_name, check_type, actual_value, failure_reason, run_date.

## Workflow

1. Understand the user's question.
2. Write the correct Athena/Presto SQL.  Follow these rules:
   - Use fully qualified table names (audit_db.audit_validation).
   - Use TIMESTAMP literals as TIMESTAMP '2024-01-01 00:00:00'.
   - Use NOW() for current time comparisons.
   - Wrap string literals in single quotes.
   - Use CAST(x AS VARCHAR) if concatenating mixed types.
   - Add LIMIT 100 unless the user explicitly wants all rows.
3. Show the SQL to the user (in a code block).
4. If the user says "run it" or the question implies they want results,
   call execute_raw_sql with the SQL.
5. If results are returned, summarise key findings in 2–3 sentences.
6. If the user asks a follow-up question, refine the SQL accordingly.

## Rules
- Never modify data (no INSERT, UPDATE, DELETE, MERGE).
- If the question is ambiguous, ask one clarifying question.
- Keep SQL readable with consistent indentation.
""".strip()

# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
query_agent = Agent(
    model=_model,
    system_prompt=_SYSTEM_PROMPT,
    tools=[
        execute_raw_sql,
        run_nl_query,
        fetch_audit_failures,
        fetch_historical_patterns,
    ],
)
