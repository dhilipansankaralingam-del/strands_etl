"""
strands_agents/tools/validation_tools.py
==========================================
Strands @tool functions for validation record lifecycle:
  - fetch existing classifications (learning)
  - write feedback / corrections
  - batch status reporting
"""

import json
import os
from typing import Any

import boto3
from strands import tool

from strands_agents.tools.athena_tools import _run_query

AWS_REGION        = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
LAST_REPORT_BUCKET = os.environ.get("LAST_REPORT_BUCKET", "strands-etl-audit")
LAST_REPORT_KEY    = os.environ.get("LAST_REPORT_KEY",   "agent_analyzer/last_report.json")


@tool
def fetch_prior_classifications(table_name: str, rule_name: str,
                                 limit: int = 20) -> str:
    """
    Retrieve recent AI classifications for the same table+rule combination
    so the agent can apply consistent reasoning (learning from history).

    Args:
        table_name: Table being validated.
        rule_name:  Validation rule name.
        limit:      Maximum historical rows to return (default 20).

    Returns:
        JSON list of prior classification decisions with confidence and explanation.
    """
    sql = f"""
        SELECT
            id, run_date, actual_value, expected_value,
            ai_classification, ai_confidence, ai_explanation,
            ai_recommended_action
        FROM audit_validation
        WHERE
            table_name = '{table_name}'
            AND rule_name = '{rule_name}'
            AND ai_classification IS NOT NULL
            AND ai_classification <> ''
        ORDER BY run_date DESC
        LIMIT {limit}
    """
    rows = _run_query(sql)
    return json.dumps({
        "table_name": table_name,
        "rule_name":  rule_name,
        "prior_decisions": rows,
        "count": len(rows),
    })


@tool
def save_feedback(record_id: str, corrected_classification: str,
                   corrected_by: str, feedback_note: str = "") -> str:
    """
    Persist a human correction to an AI classification (feedback loop).
    Updates the audit_validation row and logs the correction for model improvement.

    Args:
        record_id:                UUID of the audit_validation row.
        corrected_classification: The human-verified correct label.
        corrected_by:             Slack user ID or email of the reviewer.
        feedback_note:            Optional free-text explanation.

    Returns:
        JSON confirming the feedback was saved.
    """
    sql = f"""
        UPDATE audit_validation
        SET
            ai_classification     = '{corrected_classification}',
            ai_explanation        = 'HUMAN_CORRECTED: {feedback_note.replace("'","''")}',
            ai_recommended_action = 'HUMAN_VERIFIED',
            ai_analysed_at        = CURRENT_TIMESTAMP
        WHERE id = '{record_id}'
    """
    _run_query(sql)

    log = {
        "record_id":              record_id,
        "corrected_classification": corrected_classification,
        "corrected_by":           corrected_by,
        "feedback_note":          feedback_note,
        "action":                 "feedback_saved",
    }
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"agent_analyzer/feedback/{record_id}.json"
        s3.put_object(
            Bucket=LAST_REPORT_BUCKET, Key=key,
            Body=json.dumps(log).encode(), ContentType="application/json",
        )
    except Exception:
        pass

    return json.dumps(log)


@tool
def get_batch_summary(run_id: str = "") -> str:
    """
    Return aggregate counts for a given ETL run (or the latest run if run_id omitted).

    Args:
        run_id: ETL run UUID. If empty, uses the most recent run.

    Returns:
        JSON summary with true_failures, false_positives, needs_investigation counts.
    """
    where = f"run_id = '{run_id}'" if run_id else (
        "run_id = (SELECT run_id FROM audit_validation "
        "WHERE ai_classification IS NOT NULL ORDER BY ai_analysed_at DESC LIMIT 1)"
    )
    sql = f"""
        SELECT
            ai_classification,
            COUNT(*) AS cnt,
            AVG(ai_confidence) AS avg_confidence
        FROM audit_validation
        WHERE {where}
        GROUP BY ai_classification
    """
    rows = _run_query(sql)
    summary: dict[str, Any] = {"run_id": run_id, "breakdown": rows}
    for r in rows:
        cls = (r.get("ai_classification") or "").upper()
        if cls == "TRUE_FAILURE":
            summary["true_failures"] = int(r.get("cnt", 0))
        elif cls == "FALSE_POSITIVE":
            summary["false_positives"] = int(r.get("cnt", 0))
        elif cls == "NEEDS_INVESTIGATION":
            summary["needs_investigation"] = int(r.get("cnt", 0))
    return json.dumps(summary)


@tool
def save_run_report(report_json: str) -> str:
    """
    Persist the full agent run report to S3 as the 'last report' for /etl-status.

    Args:
        report_json: The complete report as a JSON string.

    Returns:
        JSON confirming the S3 key written.
    """
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=LAST_REPORT_BUCKET, Key=LAST_REPORT_KEY,
            Body=report_json.encode(), ContentType="application/json",
        )
        return json.dumps({"saved": True, "bucket": LAST_REPORT_BUCKET,
                           "key": LAST_REPORT_KEY})
    except Exception as exc:
        return json.dumps({"saved": False, "error": str(exc)})
