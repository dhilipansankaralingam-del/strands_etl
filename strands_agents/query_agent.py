"""
strands_agents/query_agent.py
==============================
Strands Agent for natural-language → Athena SQL queries.

The agent:
  1. Receives a plain-English question.
  2. Introspects the Athena schema via run_nl_query.
  3. Generates and executes correct SQL via execute_raw_sql.
  4. Returns results in a structured format for Block Kit rendering.

Usage (standalone):
    from strands_agents.query_agent import create_query_agent
    agent = create_query_agent()
    result = agent("Which clubs had the most FAIL records this week?")
"""

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.tools.athena_tools import run_nl_query, execute_raw_sql

MODEL_ID = os.environ.get("BEDROCK_MODEL_ID",
                           "anthropic.claude-3-sonnet-20240229-v1:0")

QUERY_SYSTEM_PROMPT = """
You are an expert SQL analyst for an enterprise ETL data quality platform.

The primary table is `audit_validation` in Athena with columns:
  id, run_id, pipeline_name, table_name, rule_name, severity, status,
  actual_value, expected_value, threshold, error_message, run_date,
  ai_classification, ai_confidence, ai_explanation, ai_recommended_action,
  ai_analysed_at, additional_context

When given a natural-language question:
1. Call run_nl_query to get the schema context.
2. Write valid Athena SQL (Presto/Trino dialect):
   - Use DATE_DIFF, DATE_TRUNC, CURRENT_TIMESTAMP, INTERVAL for time arithmetic.
   - Use TRY_CAST for safe numeric conversions.
   - Always LIMIT results to ≤1000 rows.
3. Call execute_raw_sql with the SQL you generated.
4. Return your final response as JSON:
   {
     "question": "...",
     "sql":      "...",
     "columns":  [...],
     "rows":     [...],
     "row_count": N,
     "summary":   "Plain-English interpretation of the results"
   }

Be concise. If the question is ambiguous, pick the most natural interpretation.
Never expose credentials or internal bucket names in the SQL output.
""".strip()


def create_query_agent(model_id: str = MODEL_ID) -> Agent:
    """Return a configured Strands Agent for NL → Athena queries."""
    model = BedrockModel(model_id=model_id)
    return Agent(
        model=model,
        tools=[run_nl_query, execute_raw_sql],
        system_prompt=QUERY_SYSTEM_PROMPT,
    )
