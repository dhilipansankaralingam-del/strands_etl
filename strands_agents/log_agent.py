"""
strands_agents/log_agent.py
=============================
Strands Agent for log analysis across Glue, Athena, Lambda, EMR, CloudWatch.

The agent:
  1. Fetches log content from S3 or CloudWatch.
  2. Runs fast local diagnosis (no Bedrock) for known patterns.
  3. Calls Bedrock only for novel errors to produce root-cause analysis + fix steps.

Usage (standalone):
    from strands_agents.log_agent import create_log_agent
    agent = create_log_agent()
    result = agent("Analyze logs at s3://strands-etl-audit/glue-logs/ for the last 12 hours")
"""

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.tools.log_tools import (
    fetch_s3_logs,
    fetch_cloudwatch_logs,
    extract_error_blocks,
)

MODEL_ID = os.environ.get("BEDROCK_MODEL_ID",
                           "anthropic.claude-3-sonnet-20240229-v1:0")

LOG_SYSTEM_PROMPT = """
You are an AWS data platform operations expert specialising in Glue, Athena,
Lambda, EMR, and CloudWatch Logs.

When given a log source (S3 path or CloudWatch log group):
1. Call fetch_s3_logs OR fetch_cloudwatch_logs to retrieve the content.
2. Call extract_error_blocks on the returned content_sample to isolate errors.
3. Review the fast_diagnosis results — if a fix is already identified, include it.
4. For errors NOT covered by fast_diagnosis, reason through the root cause yourself.

Your response MUST be a JSON object with this exact structure:
{
  "service":       "glue|athena|lambda|emr|generic",
  "severity":      "CRITICAL|HIGH|MEDIUM|LOW|OK",
  "root_causes":   ["cause 1", "cause 2"],
  "fix_steps":     [
    {
      "step": 1,
      "action": "short description",
      "code_or_command": "optional shell/Python/SQL snippet"
    }
  ],
  "summary":       "One-paragraph plain-English summary for Slack",
  "confidence":    0.0-1.0
}

Guidelines:
  - Be specific: include exact IAM actions, config keys, or SQL fragments.
  - If the fast_diagnosis already explains the error, trust it and summarise.
  - CRITICAL = data pipeline is halted or producing wrong results NOW.
  - HIGH     = will cause problems within 24 h if unaddressed.
  - MEDIUM   = degraded performance, not data loss.
  - LOW / OK = informational.
""".strip()


def create_log_agent(model_id: str = MODEL_ID) -> Agent:
    """Return a configured Strands Agent for log analysis."""
    model = BedrockModel(model_id=model_id)
    return Agent(
        model=model,
        tools=[fetch_s3_logs, fetch_cloudwatch_logs, extract_error_blocks],
        system_prompt=LOG_SYSTEM_PROMPT,
    )
