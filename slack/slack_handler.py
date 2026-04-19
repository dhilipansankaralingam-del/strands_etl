"""
slack/slack_handler.py
======================
AWS Lambda — *synchronous* Slack request handler.

This function MUST respond within 3 seconds (Slack's hard timeout).
It:
  1. Verifies the Slack request signature (HMAC-SHA256).
  2. Parses the slash command + arguments.
  3. Asynchronously invokes the Processor Lambda (InvocationType=Event).
  4. Returns an immediate acknowledgement to Slack.

Environment variables (set via SAM / Lambda console):
  SLACK_SIGNING_SECRET   – from Slack App → Basic Information → Signing Secret
  PROCESSOR_LAMBDA_NAME  – ARN or name of etl-slack-processor Lambda
  AWS_REGION             – auto-set by Lambda runtime

Deployment:
  See slack/template.yaml for the SAM template that wires everything up.
"""

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
from typing import Any, Dict, Tuple

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SLACK_SIGNING_SECRET  = os.environ.get("SLACK_SIGNING_SECRET", "")
PROCESSOR_LAMBDA_NAME = os.environ.get("PROCESSOR_LAMBDA_NAME", "etl-slack-processor")

_lambda_client = None


def _get_lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------

def _verify_signature(headers: Dict[str, str], raw_body: str) -> bool:
    """Return True if the Slack X-Slack-Signature is valid."""
    if not SLACK_SIGNING_SECRET:
        logger.warning("SLACK_SIGNING_SECRET not set — skipping signature verification (dev mode)")
        return True

    ts  = headers.get("X-Slack-Request-Timestamp") or headers.get("x-slack-request-timestamp", "")
    sig = headers.get("X-Slack-Signature")          or headers.get("x-slack-signature", "")

    if not ts or not sig:
        logger.error("Missing Slack timestamp or signature header")
        return False

    # Reject requests older than 5 minutes (replay attack prevention)
    try:
        if abs(time.time() - float(ts)) > 300:
            logger.error("Slack timestamp is too old: %s", ts)
            return False
    except ValueError:
        return False

    base      = f"v0:{ts}:{raw_body}".encode("utf-8")
    expected  = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode("utf-8"), base, hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(expected, sig)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _parse_args(text: str) -> Dict[str, str]:
    """
    Parse slash command arguments into a dict.

    Supports:
      key=value pairs  →  {"key": "value"}
      bare words       →  {"_query": "word1 word2 …"}

    Examples
    --------
      "pipeline=member_info hours=48"  →  {"pipeline": "member_info", "hours": "48"}
      "which clubs failed last week?"  →  {"_query": "which clubs failed last week?"}
    """
    text = (text or "").strip()
    if not text:
        return {}

    result: Dict[str, str] = {}
    kv_parts   = []
    free_words = []

    for token in text.split():
        if "=" in token:
            k, _, v = token.partition("=")
            kv_parts.append((k.strip(), v.strip()))
        else:
            free_words.append(token)

    for k, v in kv_parts:
        result[k] = v

    if free_words:
        result["_query"] = " ".join(free_words)
        if kv_parts:
            # Append any trailing free words to _query
            existing = result.get("_query", "")
            result["_query"] = existing

    # If nothing was key=value, treat the whole text as a natural-language query
    if not kv_parts and free_words:
        result["_query"] = text

    return result


# ---------------------------------------------------------------------------
# Immediate Slack response helpers
# ---------------------------------------------------------------------------

def _slack_response(body: Dict[str, Any], status: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(body),
    }


def _ack(command: str, args: str, user_id: str) -> Dict[str, Any]:
    from slack.block_kit import ack_message
    return _slack_response(ack_message(command, args, user_id))


def _ack_help() -> Dict[str, Any]:
    from slack.block_kit import help_message
    return _slack_response(help_message())


def _ack_error(msg: str) -> Dict[str, Any]:
    from slack.block_kit import ack_error
    return _slack_response(ack_error(msg))


def _ack_holiday(date_str: str, calendar_path: str) -> Dict[str, Any]:
    """Handle /etl-holiday synchronously — no async needed (just a calendar lookup)."""
    from slack.block_kit import holiday_message
    try:
        with open(calendar_path) as fh:
            cal = json.load(fh)
        year = date_str[:4]
        entry = (
            cal.get(f"explicit_dates_{year}", {}).get(date_str)
            or cal.get(f"explicit_dates_{int(year)-1}", {}).get(date_str)
        )
        return _slack_response(holiday_message(date_str, entry))
    except Exception as exc:
        logger.warning("Holiday lookup failed: %s", exc)
        return _slack_response(holiday_message(date_str, None))


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda entry point.  Called by API Gateway on every Slack slash-command POST.
    """
    headers  = event.get("headers") or {}
    raw_body = event.get("body", "")
    is_b64   = event.get("isBase64Encoded", False)

    # API GW may base64-encode the body for binary-safe transit
    if is_b64:
        import base64
        raw_body = base64.b64decode(raw_body).decode("utf-8")

    # ── 1. Signature verification ────────────────────────────────────────
    if not _verify_signature(headers, raw_body):
        logger.error("Invalid Slack signature")
        return {"statusCode": 401, "body": "Unauthorized"}

    # ── 2. Parse form-encoded body ───────────────────────────────────────
    payload = dict(urllib.parse.parse_qsl(raw_body))

    command      = payload.get("command", "").strip()      # e.g. /etl-analyze
    text         = payload.get("text", "").strip()         # everything after the command
    user_id      = payload.get("user_id", "")
    channel_id   = payload.get("channel_id", "")
    response_url = payload.get("response_url", "")
    team_id      = payload.get("team_id", "")

    logger.info("Received command=%s text=%r user=%s channel=%s", command, text, user_id, channel_id)

    # ── 3. Handle /etl-help synchronously ───────────────────────────────
    if command == "/etl-help" or text.lower() in ("help", "--help", "-h"):
        return _ack_help()

    # ── 4. Handle /etl-holiday synchronously (no AI needed) ─────────────
    if command == "/etl-holiday":
        date_str = text.strip() or ""
        if not date_str:
            return _ack_error("Usage: `/etl-holiday YYYY-MM-DD`")
        return _ack_holiday(date_str, "config/holiday_calendar.json")

    # ── 5. Validate command is known ────────────────────────────────────
    known_commands = {"/etl-analyze", "/etl-logs", "/etl-query", "/etl-status"}
    if command not in known_commands:
        from slack.block_kit import ack_unknown_command
        return _slack_response(ack_unknown_command(command, text))

    # ── 6. Fire async Processor Lambda ──────────────────────────────────
    processor_payload = {
        "command":      command,
        "text":         text,
        "args":         _parse_args(text),
        "user_id":      user_id,
        "channel_id":   channel_id,
        "response_url": response_url,
        "team_id":      team_id,
    }

    try:
        _get_lambda().invoke(
            FunctionName   = PROCESSOR_LAMBDA_NAME,
            InvocationType = "Event",          # fire-and-forget
            Payload        = json.dumps(processor_payload).encode(),
        )
        logger.info("Processor Lambda invoked asynchronously for command=%s", command)
    except Exception as exc:
        logger.error("Failed to invoke processor Lambda: %s", exc)
        return _ack_error(f"Could not start agent: {exc}")

    # ── 7. Acknowledge Slack immediately (<3 s) ──────────────────────────
    return _ack(command, text, user_id)
