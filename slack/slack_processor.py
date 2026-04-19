"""
slack/slack_processor.py
========================
AWS Lambda — *asynchronous* Slack command processor.

Invoked by slack_handler.py with InvocationType=Event so it runs in the
background (no 3-second Slack timeout to worry about).  When complete it
posts the formatted Block Kit result back to Slack via the response_url
(valid for 30 minutes) or chat.postMessage (persistent, channel-visible).

Supported commands
------------------
/etl-analyze  →  runs AgentAnalyzer, posts AI analysis Block Kit card
/etl-logs     →  runs LogAnalyzerAgent, posts log findings card
/etl-query    →  runs AthenaQueryEngine NL→SQL, posts query result card
/etl-status   →  reads last report from S3, posts summary card

Environment variables
---------------------
SLACK_BOT_TOKEN        – Bot OAuth token (xoxb-…) for chat.postMessage
AGENT_ANALYZER_CONFIG  – local or s3:// path to agent_analyzer_config.json
LOG_ANALYZER_CONFIG    – local or s3:// path to log_analyzer_config.json
HOLIDAY_CALENDAR_PATH  – local or s3:// path to holiday_calendar.json
LAST_REPORT_BUCKET     – S3 bucket where last report is stored
LAST_REPORT_KEY        – S3 key for last report JSON
AWS_REGION             – auto-set by Lambda
"""

import json
import logging
import os
import sys
import traceback
import urllib.request
from typing import Any, Dict, List, Optional

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment
SLACK_BOT_TOKEN        = os.environ.get("SLACK_BOT_TOKEN", "")
AGENT_ANALYZER_CONFIG  = os.environ.get("AGENT_ANALYZER_CONFIG",  "config/agent_analyzer_config.json")
LOG_ANALYZER_CONFIG    = os.environ.get("LOG_ANALYZER_CONFIG",    "config/log_analyzer_config.json")
HOLIDAY_CALENDAR_PATH  = os.environ.get("HOLIDAY_CALENDAR_PATH",  "config/holiday_calendar.json")
LAST_REPORT_BUCKET     = os.environ.get("LAST_REPORT_BUCKET",     "strands-etl-audit")
LAST_REPORT_KEY        = os.environ.get("LAST_REPORT_KEY",        "agent_analyzer/last_report.json")
AWS_REGION             = os.environ.get("AWS_DEFAULT_REGION",     "us-east-1")


# ---------------------------------------------------------------------------
# Slack posting
# ---------------------------------------------------------------------------

def _post_to_slack(response_url: str, body: Dict[str, Any]) -> None:
    """POST a Block Kit message back to Slack via response_url."""
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        response_url,
        data    = payload,
        method  = "POST",
        headers = {"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Slack post status: %d", resp.status)
    except Exception as exc:
        logger.error("Failed to post to Slack response_url: %s", exc)


def _chat_post(channel_id: str, body: Dict[str, Any]) -> None:
    """Post using chat.postMessage (persists after response_url expires)."""
    if not SLACK_BOT_TOKEN:
        logger.warning("SLACK_BOT_TOKEN not set — cannot use chat.postMessage")
        return
    payload = {"channel": channel_id, **body}
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data    = json.dumps(payload).encode("utf-8"),
        method  = "POST",
        headers = {
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
    """Try response_url first; fall back to chat.postMessage."""
    if response_url:
        _post_to_slack(response_url, body)
    if channel_id:
        _chat_post(channel_id, body)


# ---------------------------------------------------------------------------
# S3 last-report helpers
# ---------------------------------------------------------------------------

def _save_last_report(report: Dict[str, Any]) -> None:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket      = LAST_REPORT_BUCKET,
            Key         = LAST_REPORT_KEY,
            Body        = json.dumps(report, default=str).encode(),
            ContentType = "application/json",
        )
    except Exception as exc:
        logger.warning("Could not save last report: %s", exc)


def _load_last_report() -> Optional[Dict[str, Any]]:
    try:
        s3   = boto3.client("s3", region_name=AWS_REGION)
        body = s3.get_object(Bucket=LAST_REPORT_BUCKET, Key=LAST_REPORT_KEY)["Body"].read()
        return json.loads(body)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def _handle_analyze(args: Dict[str, str], response_url: str, channel_id: str) -> None:
    """Run AgentAnalyzer and post the result."""
    from slack.block_kit import analyze_result, processing_error

    pipeline    = args.get("pipeline", "")
    hours       = int(args.get("hours", args.get("look_back", 24)))
    severity    = args.get("severity", "")
    run_id_flt  = args.get("run_id", "")

    # Load and patch config
    config = _load_config(AGENT_ANALYZER_CONFIG)
    analysis = config.setdefault("analysis", {})
    analysis["look_back_hours"] = hours
    if pipeline:
        analysis.setdefault("pipeline_filters", []).append(pipeline)
    if severity:
        analysis.setdefault("severity_filters", []).append(severity)
    if run_id_flt:
        analysis["run_id_filter"] = run_id_flt
    config["email"] = {"enabled": False}  # email suppressed — using Slack instead
    if HOLIDAY_CALENDAR_PATH:
        config["holiday_calendar_path"] = HOLIDAY_CALENDAR_PATH

    try:
        # Import from the project root
        sys.path.insert(0, "/var/task")
        from pyscript.etl_agent_analyzer import AgentAnalyzer
        analyzer = AgentAnalyzer(config)
        report   = analyzer.run()
        _save_last_report(report)
        msg = analyze_result(report, report["run_id"])
    except Exception as exc:
        logger.error("AgentAnalyzer failed: %s", traceback.format_exc())
        msg = processing_error("/etl-analyze", str(exc))

    _send(response_url, channel_id, msg)


def _handle_logs(args: Dict[str, str], response_url: str, channel_id: str) -> None:
    """Run LogAnalyzerAgent and post the result."""
    from slack.block_kit import logs_result, processing_error

    source      = args.get("source", "")
    hours       = int(args.get("hours", 24))
    config      = _load_config(LOG_ANALYZER_CONFIG)

    log_paths   = [source] if source else config.get("log_sources", {}).get("paths", [])
    if not log_paths:
        _send(response_url, channel_id, processing_error(
            "/etl-logs", "No log source specified. Use `source=s3://... or /aws/glue/...`"
        ))
        return

    try:
        sys.path.insert(0, "/var/task")
        from pyscript.log_analyzer_agent import LogAnalyzerAgent
        agent  = LogAnalyzerAgent(region=AWS_REGION)
        report = agent.analyze_logs(log_paths, since_hours=hours, config=config)
        msg    = logs_result(report)
    except Exception as exc:
        logger.error("LogAnalyzerAgent failed: %s", traceback.format_exc())
        msg = processing_error("/etl-logs", str(exc))

    _send(response_url, channel_id, msg)


def _handle_query(args: Dict[str, str], response_url: str, channel_id: str) -> None:
    """Convert NL question → Athena SQL → results and post."""
    from slack.block_kit import query_result, processing_error

    nl_query = args.get("_query", "").strip()
    if not nl_query:
        _send(response_url, channel_id, processing_error(
            "/etl-query", "Please provide a natural-language question after the command."
        ))
        return

    config   = _load_config(AGENT_ANALYZER_CONFIG)
    ath_cfg  = config.get("athena", {})
    database = ath_cfg.get("database", "data_quality_db")
    s3_out   = ath_cfg.get("s3_output", "")
    workgrp  = ath_cfg.get("workgroup", "primary")
    ai_cfg   = config.get("ai", {})
    model_id = ai_cfg.get("model_id", "anthropic.claude-3-sonnet-20240229-v1:0")

    try:
        sys.path.insert(0, "/var/task")
        from validation.athena_query_engine import AthenaQueryEngine
        engine = AthenaQueryEngine(
            database  = database,
            s3_output = s3_out,
            workgroup = workgrp,
            region    = AWS_REGION,
            model_id  = model_id,
        )
        result = engine.nl_to_sql_and_execute(nl_query)

        cost_usd = 0.0
        if hasattr(engine, "cost_tracker") and engine.cost_tracker:
            cost_usd = engine.cost_tracker.cost_so_far()

        msg = query_result(
            nl_query  = nl_query,
            sql       = result.sql,
            rows      = result.rows,
            columns   = result.columns,
            row_count = result.row_count,
            cost_usd  = cost_usd,
        )
    except Exception as exc:
        logger.error("NL query failed: %s", traceback.format_exc())
        msg = processing_error("/etl-query", str(exc))

    _send(response_url, channel_id, msg)


def _handle_status(response_url: str, channel_id: str) -> None:
    """Load last report from S3 and post a summary card."""
    from slack.block_kit import status_message
    report = _load_last_report()
    _send(response_url, channel_id, status_message(report))


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config(path: str) -> Dict[str, Any]:
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    try:
        with open(path) as fh:
            return json.load(fh)
    except FileNotFoundError:
        logger.warning("Config not found at %s — using empty dict", path)
        return {}


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event: Dict[str, Any], context: Any) -> None:
    """
    Invoked asynchronously by slack_handler.py (InvocationType=Event).
    No return value — results are posted back to Slack via response_url.
    """
    command      = event.get("command", "")
    args         = event.get("args", {})
    response_url = event.get("response_url", "")
    channel_id   = event.get("channel_id", "")
    user_id      = event.get("user_id", "")

    logger.info("Processor started: command=%s args=%s user=%s", command, args, user_id)

    try:
        if command == "/etl-analyze":
            _handle_analyze(args, response_url, channel_id)

        elif command == "/etl-logs":
            _handle_logs(args, response_url, channel_id)

        elif command == "/etl-query":
            _handle_query(args, response_url, channel_id)

        elif command == "/etl-status":
            _handle_status(response_url, channel_id)

        else:
            from slack.block_kit import processing_error
            _send(response_url, channel_id,
                  processing_error(command, f"Unknown command: {command}"))

    except Exception as exc:
        logger.error("Unhandled processor error: %s", traceback.format_exc())
        from slack.block_kit import processing_error
        _send(response_url, channel_id, processing_error(command, str(exc)))
