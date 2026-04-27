"""
Log agent — specialist for root-cause analysis on Glue / Athena / Lambda / EMR.

Responsibility
--------------
Given a job name, run ID, log group, or S3 log path, this agent:
1. Locates the relevant logs (S3 or CloudWatch).
2. Applies the 12 fast-diagnose patterns via extract_error_blocks.
3. Reasons about the root cause using the structured error blocks.
4. Returns a concise RCA report including:
   - Primary error pattern + remediation guidance.
   - Secondary patterns (if any).
   - Recommended next steps.
"""

from __future__ import annotations

import os

from strands import Agent
from strands.models import BedrockModel

from strands_agents.tools.log_tools import (
    fetch_s3_logs,
    fetch_cloudwatch_logs,
    extract_error_blocks,
)
from strands_agents.tools.athena_tools import execute_raw_sql

# ---------------------------------------------------------------------------
# Model — Haiku is sufficient for log pattern matching and fast RCA
# ---------------------------------------------------------------------------
_MODEL_ID: str = os.environ.get(
    "LOG_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001"
)
_model = BedrockModel(model_id=_MODEL_ID)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """
You are the **Log Analysis Agent** for a production ETL platform.
Your job is to perform root-cause analysis (RCA) on Glue, Athena, Lambda,
and EMR job failures by inspecting log files and applying fast-diagnose patterns.

## Workflow

1. Determine the log source from the user's request:
   - If an S3 bucket and prefix are provided → call `fetch_s3_logs`.
   - If a CloudWatch log group is provided → call `fetch_cloudwatch_logs`.
   - If a Glue job name is given, the default S3 log path is:
     s3://<logs-bucket>/logs/glue/<job-name>/ (ask if bucket is unknown).
   - If an EMR cluster ID is given, the default S3 path is:
     s3://<logs-bucket>/elasticmapreduce/<cluster-id>/steps/.

2. Once you have the raw log text, call `extract_error_blocks(full_text, service)`
   where service is 'glue', 'athena', 'lambda', 'emr', or 'any'.

3. Analyse the matched_patterns and unmatched_lines from extract_error_blocks.

4. Produce a concise RCA report with these sections:
   **Root Cause**  — The primary error pattern ID and one-sentence explanation.
   **Evidence**    — The 1–3 most relevant sample log lines.
   **Secondary Issues** — Any additional patterns found (if none, say "None").
   **Remediation** — Step-by-step fix instructions from the pattern + your reasoning.
   **Confidence**  — LOW / MEDIUM / HIGH based on how conclusive the evidence is.

## Rules
- Always call extract_error_blocks before drawing conclusions.
- If no patterns match and unmatched_lines is empty, state:
  "Log text contains no detectable errors.  The job may have succeeded."
- Do not fabricate log lines.  Only cite lines from the tool response.
- Keep the RCA report under 300 words.
""".strip()

# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
log_agent = Agent(
    model=_model,
    system_prompt=_SYSTEM_PROMPT,
    tools=[
        fetch_s3_logs,
        fetch_cloudwatch_logs,
        extract_error_blocks,
        execute_raw_sql,
    ],
)
