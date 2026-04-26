"""
Orchestrator agent — top-level router for Slack commands and scheduled runs.

Architecture
------------
Uses the **agent-as-tool** pattern: the three specialist agents are exposed
as tools so the orchestrator can delegate to the right specialist, combine
their outputs, and return a single coherent response.

Routing logic
-------------
  "analyze failures" / "run validation" / "classify"
      → validate_failures  (validation_agent)

  "check logs" / "diagnose" / "RCA" / "why did <job> fail"
      → analyze_logs       (log_agent)

  "how many" / "show me" / "query" / "list" / "count"
      → query_audit_data   (query_agent)

  Complex requests may call multiple specialists in sequence.

Exposed directly on the orchestrator (no delegation needed):
  get_batch_summary      — quick stats for a previous run
  save_feedback          — record human feedback on a classification
  list_upcoming_holidays — calendar awareness

Entry point
-----------
    from strands_agents import orchestrator_agent
    result = orchestrator_agent("analyze failures")
    result = orchestrator_agent("why did glue job X fail?  bucket=my-logs prefix=glue/X/")
    result = orchestrator_agent("how many TRUE_FAILURE records in the last 7 days?")
"""

from __future__ import annotations

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.validation_agent import validation_agent
from strands_agents.log_agent import log_agent
from strands_agents.query_agent import query_agent
from strands_agents.tools.validation_tools import get_batch_summary, save_feedback
from strands_agents.tools.holiday_tools import list_upcoming_holidays

# ---------------------------------------------------------------------------
# Model — Sonnet for orchestration-level reasoning
# ---------------------------------------------------------------------------
_MODEL_ID: str = os.environ.get(
    "ORCHESTRATOR_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20251001"
)
_model = BedrockModel(model_id=_MODEL_ID)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """
You are the **ETL Audit Orchestrator**, the top-level agent for the Strands
ETL validation platform.  You receive commands from Slack (or the CLI) and
route them to the correct specialist agent or tool.

## Specialist agents (available as tools)

validate_failures
    Classifies all unassessed FAIL rows in audit_validation (last 24 hours).
    Call this when the user says: "analyze failures", "run validation",
    "classify failures", "process the audit queue", or similar.

analyze_logs
    Performs root-cause analysis on Glue / Athena / Lambda / EMR log files.
    Call this when the user mentions a specific job name, log path, cluster ID,
    or asks "why did X fail?" / "what caused the error in Y?".
    Pass the user's full request as the input — include all identifiers.

query_audit_data
    Translates natural-language questions to Athena SQL and executes them.
    Call this for any "how many", "show me", "list", "count", "what percentage",
    "which tables", or other data-retrieval questions.

## Direct tools (no delegation)

get_batch_summary(run_id)
    Returns counts and critical failures for a previous classification run.
    Use when user says "show me the last run summary" or "summary for run <id>".

save_feedback(record_id, correct_classification, notes)
    Record human feedback on a classification.
    Use when user says "that was wrong, it's actually a <label>" or
    "mark record <id> as <label>".

list_upcoming_holidays(days_ahead, country)
    List upcoming public holidays.  Use when asked about upcoming holidays
    or when explaining why certain failures may be false positives.

## Routing rules

1. Read the user's message carefully.
2. Identify which specialist(s) or direct tool(s) are needed.
3. If the request covers multiple domains (e.g. "analyze failures AND show me
   a breakdown"), call both specialists in logical order.
4. Always present results clearly.  For validation runs, include:
   - Total rows classified.
   - TRUE_FAILURE / FALSE_POSITIVE / NEEDS_INVESTIGATION counts.
   - Any CRITICAL/HIGH TRUE_FAILURE records that need immediate action.
5. For Slack, keep responses concise — use bullet points, not long paragraphs.
6. If the request is ambiguous, ask one clarifying question before acting.

## Error handling

- If a specialist returns an error, report it to the user and suggest next steps.
- If Athena is unavailable, notify the user and do not retry automatically.
- Never expose raw stack traces to the user; summarise the error in plain English.
""".strip()

# ---------------------------------------------------------------------------
# Expose specialists as tools via agent-as-tool pattern
# ---------------------------------------------------------------------------
_validate_failures_tool = validation_agent.as_tool(
    tool_name="validate_failures",
    description=(
        "Classify all unassessed FAIL rows (status='FAIL', ai_classification IS NULL, "
        "last 24 hours) in audit_validation.  Writes TRUE_FAILURE / FALSE_POSITIVE / "
        "NEEDS_INVESTIGATION back to the five AI columns and saves a run report.  "
        "Call with no arguments or pass a brief instruction string."
    ),
)

_analyze_logs_tool = log_agent.as_tool(
    tool_name="analyze_logs",
    description=(
        "Perform root-cause analysis on Glue / Athena / Lambda / EMR logs.  "
        "Pass the user's full request including job name, S3 bucket/prefix, "
        "CloudWatch log group, or cluster ID so the agent can locate the logs."
    ),
)

_query_audit_tool = query_agent.as_tool(
    tool_name="query_audit_data",
    description=(
        "Translate a natural-language question into Athena SQL and execute it "
        "against audit_db.audit_validation (and related tables).  Pass the "
        "user's exact question as the input."
    ),
)

# ---------------------------------------------------------------------------
# Orchestrator agent
# ---------------------------------------------------------------------------
orchestrator_agent = Agent(
    model=_model,
    system_prompt=_SYSTEM_PROMPT,
    tools=[
        _validate_failures_tool,
        _analyze_logs_tool,
        _query_audit_tool,
        get_batch_summary,
        save_feedback,
        list_upcoming_holidays,
    ],
)
