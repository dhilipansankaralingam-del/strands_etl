"""
slack/slack_processor_strands.py
==================================
Strands-native async Slack processor.

Drop-in replacement for slack_processor.py.  Instead of calling
AgentAnalyzer / LogAnalyzerAgent / AthenaQueryEngine directly, this module
routes every command through the Strands orchestrator (or a specialist agent),
which manages tool calls, retries, and context automatically.

To switch to the Strands backend:
  In template.yaml, change Handler to:
      Handler: slack.slack_processor_strands.handler

Environment variables — same as slack_processor.py plus:
  BEDROCK_MODEL_ID  (default: anthropic.claude-3-sonnet-20240229-v1:0)
"""

import json
import logging
import os
import traceback
import urllib.request
from typing import Any, Dict

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SLACK_BOT_TOKEN    = os.environ.get("SLACK_BOT_TOKEN",    "")
LAST_REPORT_BUCKET = os.environ.get("LAST_REPORT_BUCKET", "strands-etl-audit")
LAST_REPORT_KEY    = os.environ.get("LAST_REPORT_KEY",    "agent_analyzer/last_report.json")
AWS_REGION         = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Lazy-init orchestrator (one instance per warm Lambda)
_orchestrator = None


def _get_orchestrator():
    global _orchestrator
    if _orchestrator is None:
        from strands_agents.orchestrator_agent import create_orchestrator
        _orchestrator = create_orchestrator()
    return _orchestrator


# ---------------------------------------------------------------------------
# Slack posting (same as slack_processor.py)
# ---------------------------------------------------------------------------

def _post_to_slack(response_url: str, body: Dict[str, Any]) -> None:
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        response_url, data=payload, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Slack post status: %d", resp.status)
    except Exception as exc:
        logger.error("Failed to post to Slack response_url: %s", exc)


def _chat_post(channel_id: str, body: Dict[str, Any]) -> None:
    if not SLACK_BOT_TOKEN:
        return
    payload = {"channel": channel_id, **body}
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                logger.error("chat.postMessage error: %s", result.get("error"))
    except Exception as exc:
        logger.error("chat.postMessage failed: %s", exc)


def _send(response_url: str, channel_id: str, body: Dict[str, Any]) -> None:
    if response_url:
        _post_to_slack(response_url, body)
    if channel_id:
        _chat_post(channel_id, body)


# ---------------------------------------------------------------------------
# Command → orchestrator prompt builders
# ---------------------------------------------------------------------------

def _build_prompt(command: str, args: Dict[str, str]) -> str:
    """Translate a Slack slash command + parsed args into a natural-language prompt."""
    if command == "/etl-analyze":
        hours    = args.get("hours", args.get("look_back", "24"))
        pipeline = args.get("pipeline", "")
        severity = args.get("severity", "")
        run_id   = args.get("run_id", "")
        parts    = [f"Analyze ETL validation failures from the last {hours} hours."]
        if pipeline:
            parts.append(f"Filter to pipeline: {pipeline}.")
        if severity:
            parts.append(f"Focus on {severity} severity only.")
        if run_id:
            parts.append(f"Scope to run_id: {run_id}.")
        parts.append("Apply holiday context and historical patterns. Write enrichment back to the audit table.")
        return " ".join(parts)

    if command == "/etl-logs":
        source = args.get("source", "")
        hours  = args.get("hours", "24")
        if source:
            return f"Analyze logs at {source} for the last {hours} hours. Identify root causes and fix steps."
        return f"Analyze default ETL CloudWatch log groups for the last {hours} hours."

    if command == "/etl-query":
        query = args.get("_query", "").strip()
        return query or "Show me the latest ETL failure summary."

    if command == "/etl-status":
        return "What is the status of the last ETL analyzer run? Call get_batch_summary."

    return args.get("_query", command)


# ---------------------------------------------------------------------------
# Response formatter — converts orchestrator text to Block Kit
# ---------------------------------------------------------------------------

def _to_block_kit(command: str, orchestrator_response: str,
                   is_error: bool = False) -> Dict[str, Any]:
    from slack.block_kit import processing_error

    if is_error:
        return processing_error(command, orchestrator_response[:400])

    # The orchestrator may return raw text or embedded JSON
    text = str(orchestrator_response).strip()

    return {
        "response_type": "in_channel",
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text[:2900]},
            }
        ],
    }


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event: Dict[str, Any], context: Any) -> None:
    """
    Async Lambda entry point — called with InvocationType=Event by slack_handler.py.
    Routes every Slack command through the Strands orchestrator.
    """
    command      = event.get("command", "")
    args         = event.get("args", {})
    response_url = event.get("response_url", "")
    channel_id   = event.get("channel_id", "")
    user_id      = event.get("user_id", "")

    logger.info("Strands processor: command=%s args=%s user=%s", command, args, user_id)

    prompt = _build_prompt(command, args)
    logger.info("Orchestrator prompt: %s", prompt)

    try:
        orch   = _get_orchestrator()
        result = orch(prompt)
        msg    = _to_block_kit(command, str(result))
    except Exception as exc:
        logger.error("Orchestrator failed: %s", traceback.format_exc())
        from slack.block_kit import processing_error
        msg = processing_error(command, str(exc))

    _send(response_url, channel_id, msg)
