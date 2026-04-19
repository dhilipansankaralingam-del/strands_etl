"""
strands_agents/orchestrator_agent.py
======================================
Top-level Strands orchestrator that routes user requests to the correct
specialist agent using the agent-as-tool pattern.

Specialist agents exposed as @tool functions so the orchestrator can
delegate naturally within a single conversation turn.

Supported intent routing:
  - "analyze" / "failures" / "validation"  → validation_agent
  - "logs" / "errors" / "glue" / "lambda"  → log_agent
  - "query" / "show me" / "how many"       → query_agent
  - "status" / "last run"                  → get_batch_summary (direct tool)

Usage (as the Slack processor entry point):
    from strands_agents.orchestrator_agent import create_orchestrator
    orch = create_orchestrator()
    result = orch(slack_text)
    print(str(result))
"""

import json
import os

from strands import Agent, tool
from strands.models import BedrockModel

from strands_agents.validation_agent import create_validation_agent
from strands_agents.log_agent import create_log_agent
from strands_agents.query_agent import create_query_agent
from strands_agents.tools.validation_tools import get_batch_summary, save_run_report
from strands_agents.tools.holiday_tools import get_holiday_context, list_upcoming_holidays

MODEL_ID = os.environ.get("BEDROCK_MODEL_ID",
                           "anthropic.claude-3-sonnet-20240229-v1:0")

# ── Specialist agents (lazy-initialised once per Lambda warm instance) ──────
_validation_agent: Agent | None = None
_log_agent:        Agent | None = None
_query_agent:      Agent | None = None


def _get_validation_agent() -> Agent:
    global _validation_agent
    if _validation_agent is None:
        _validation_agent = create_validation_agent()
    return _validation_agent


def _get_log_agent() -> Agent:
    global _log_agent
    if _log_agent is None:
        _log_agent = create_log_agent()
    return _log_agent


def _get_query_agent() -> Agent:
    global _query_agent
    if _query_agent is None:
        _query_agent = create_query_agent()
    return _query_agent


# ── Agent-as-tool wrappers ───────────────────────────────────────────────────

@tool
def analyze_validation_failures(request: str) -> str:
    """
    Delegate a validation-failure analysis request to the ValidationAgent.

    The ValidationAgent fetches FAIL records from Athena, applies holiday context
    and historical pattern analysis, classifies each record, and writes results back.

    Args:
        request: Natural-language description of the analysis scope, e.g.
                 "Analyze failures from the last 48 hours for pipeline member_info_etl"
                 or "Check CRITICAL severity failures from run_id abc-123".

    Returns:
        JSON string with classification summary and top failures.
    """
    result = _get_validation_agent()(request)
    return str(result)


@tool
def analyze_logs(request: str) -> str:
    """
    Delegate a log analysis request to the LogAgent.

    The LogAgent fetches content from S3 or CloudWatch, runs fast local
    diagnosis for known patterns, and calls Bedrock for novel errors.

    Args:
        request: Log path and time window, e.g.
                 "Analyze s3://strands-etl-audit/glue-logs/ last 12 hours" or
                 "Check /aws/glue/jobs/trigger_glue_job last 24 hours".

    Returns:
        JSON string with service, severity, root_causes, fix_steps, and summary.
    """
    result = _get_log_agent()(request)
    return str(result)


@tool
def query_data(question: str) -> str:
    """
    Delegate a natural-language data question to the QueryAgent.

    The QueryAgent introspects the Athena schema, generates SQL, executes it,
    and returns structured results.

    Args:
        question: Any plain-English question about the ETL data, e.g.
                  "Which clubs have the most failures this week?" or
                  "Show z-score anomalies for club CA-001 last 60 days".

    Returns:
        JSON string with sql, columns, rows, row_count, and a plain summary.
    """
    result = _get_query_agent()(question)
    return str(result)


# ── Orchestrator system prompt ───────────────────────────────────────────────

ORCHESTRATOR_SYSTEM_PROMPT = """
You are the ETL Operations Orchestrator for a large enterprise data platform.
You have three specialist agents available as tools:

  1. analyze_validation_failures — classify Athena audit_validation FAIL records
  2. analyze_logs                — diagnose Glue/Athena/Lambda/EMR/CloudWatch errors
  3. query_data                  — answer natural-language questions via Athena SQL

Additionally you can:
  - get_batch_summary(run_id)        — check the latest run status instantly
  - get_holiday_context(date_str)    — check if today is a holiday
  - list_upcoming_holidays(days)     — see what holidays are coming up
  - save_run_report(report_json)     — persist a run report to S3

Routing rules:
  - If the user mentions "analyze", "failures", "false positive", "validation" → analyze_validation_failures
  - If the user mentions "logs", "glue", "lambda", "error", "athena query failed" → analyze_logs
  - If the user asks a question about data (who, what, how many, show me) → query_data
  - If the user asks "status" or "last run" → get_batch_summary
  - If multiple topics appear, call multiple tools and synthesize the response.

Always return a response suitable for Slack Block Kit rendering:
  - Lead with a status emoji (✅ ⚠️ 🔴 ℹ️)
  - Bold the most important finding
  - Keep the summary under 300 words
  - If you call a specialist agent, include the key numbers from its response.
""".strip()


def create_orchestrator(model_id: str = MODEL_ID) -> Agent:
    """Return the top-level orchestrator Agent."""
    model = BedrockModel(model_id=model_id)
    return Agent(
        model=model,
        tools=[
            analyze_validation_failures,
            analyze_logs,
            query_data,
            get_batch_summary,
            get_holiday_context,
            list_upcoming_holidays,
            save_run_report,
        ],
        system_prompt=ORCHESTRATOR_SYSTEM_PROMPT,
    )
