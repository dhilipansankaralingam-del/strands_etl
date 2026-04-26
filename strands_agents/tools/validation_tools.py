"""
Validation tools — learning-store access and run reporting.

These tools work with the S3-based learning store (prior classifications,
human feedback) and produce batch summaries / run reports.  They are
distinct from athena_tools, which owns live-table I/O.

Tools
-----
fetch_prior_classifications  Load S3 learning-store records for a rule + table.
save_feedback                Persist human feedback on a classification.
get_batch_summary            Aggregate AI-classification counts for a run.
save_run_report              Write a full JSON run report to S3.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Optional

import boto3
from strands import tool

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LEARNING_BUCKET: str = os.environ.get("LEARNING_BUCKET", "strands-etl-learning")
LEARNING_PREFIX: str = os.environ.get("LEARNING_PREFIX", "validation/classifications/")
FEEDBACK_PREFIX: str = os.environ.get("FEEDBACK_PREFIX", "validation/feedback/")
REPORTS_PREFIX: str = os.environ.get("REPORTS_PREFIX", "validation/reports/")
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")

_s3_client: Optional[object] = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_key(value: str) -> str:
    """Sanitise a string for use in an S3 key segment."""
    return value.replace("/", "_").replace(" ", "_").lower()


# ---------------------------------------------------------------------------
# Public tools
# ---------------------------------------------------------------------------


@tool
def fetch_prior_classifications(
    rule_name: str, table_name: str, limit: int = 10
) -> str:
    """
    Retrieve prior AI classifications stored in the S3 learning store for the
    given rule_name + table_name combination.

    The learning store accumulates records written by write_ai_enrichment and
    any human feedback.  Call this tool *before* classifying a new failure so
    the agent can calibrate its confidence against past outcomes.

    Parameters
    ----------
    rule_name  : Validation rule name to look up.
    table_name : Target table name to look up.
    limit      : Maximum number of prior records to return (default 10).

    Returns
    -------
    JSON string with keys 'count' and 'records' (list of classification dicts).
    """
    prefix = (
        f"{LEARNING_PREFIX}{_safe_key(table_name)}/{_safe_key(rule_name)}/"
    )
    try:
        resp = _s3().list_objects_v2(
            Bucket=LEARNING_BUCKET, Prefix=prefix, MaxKeys=int(limit)
        )
    except Exception as exc:
        return json.dumps({"count": 0, "records": [], "warning": str(exc)})

    objects = resp.get("Contents", [])
    records: list[dict] = []
    for obj in sorted(objects, key=lambda o: o["LastModified"], reverse=True)[
        : int(limit)
    ]:
        try:
            body = (
                _s3()
                .get_object(Bucket=LEARNING_BUCKET, Key=obj["Key"])["Body"]
                .read()
            )
            records.append(json.loads(body))
        except Exception:
            pass  # skip unreadable objects

    return json.dumps({"count": len(records), "records": records})


@tool
def save_feedback(
    record_id: str,
    correct_classification: str,
    notes: str = "",
    reviewed_by: str = "human",
) -> str:
    """
    Persist human feedback on an AI classification to the S3 learning store.

    Feedback is used to improve future classification accuracy.  Call this
    when a human analyst disagrees with or confirms the AI classification.

    Parameters
    ----------
    record_id              : The audit_validation record_id being reviewed.
    correct_classification : The correct label: TRUE_FAILURE | FALSE_POSITIVE |
                             NEEDS_INVESTIGATION.
    notes                  : Free-text explanation for the correction.
    reviewed_by            : Reviewer identifier (default: 'human').

    Returns
    -------
    JSON string with 'success' (bool), 's3_key', and 'timestamp'.
    """
    valid = {"TRUE_FAILURE", "FALSE_POSITIVE", "NEEDS_INVESTIGATION"}
    classification = correct_classification.upper().strip()
    if classification not in valid:
        return json.dumps(
            {
                "success": False,
                "error": (
                    f"Invalid classification '{classification}'. "
                    f"Must be one of: {', '.join(sorted(valid))}."
                ),
            }
        )

    ts = _utc_now()
    payload = {
        "record_id": record_id,
        "correct_classification": classification,
        "notes": notes,
        "reviewed_by": reviewed_by,
        "feedback_timestamp": ts,
        "feedback_id": str(uuid.uuid4()),
    }
    key = f"{FEEDBACK_PREFIX}{ts[:10]}/{record_id}_{uuid.uuid4().hex[:8]}.json"

    try:
        _s3().put_object(
            Bucket=LEARNING_BUCKET,
            Key=key,
            Body=json.dumps(payload, indent=2).encode(),
            ContentType="application/json",
        )
    except Exception as exc:
        return json.dumps({"success": False, "error": str(exc)})

    return json.dumps({"success": True, "s3_key": key, "timestamp": ts})


@tool
def get_batch_summary(run_id: str) -> str:
    """
    Aggregate AI-classification results for all records in a given run.

    Reads the run's report file from S3 (written by save_run_report) and
    returns counts broken down by classification type, average confidence,
    and a list of high-severity TRUE_FAILURE records for immediate attention.

    Parameters
    ----------
    run_id : The orchestrator run identifier.

    Returns
    -------
    JSON string with keys:
        run_id              str
        total               int
        by_classification   dict  {classification: count}
        by_action           dict  {recommended_action: count}
        avg_confidence      float
        critical_failures   list  – TRUE_FAILURE records with severity CRITICAL/HIGH.
        report_s3_key       str   – Location of the full report.
    """
    key = f"{REPORTS_PREFIX}{run_id}.json"
    try:
        body = (
            _s3().get_object(Bucket=LEARNING_BUCKET, Key=key)["Body"].read()
        )
        report: dict = json.loads(body)
    except Exception as exc:
        return json.dumps(
            {"error": f"Report not found for run_id '{run_id}': {exc}"}
        )

    results: list[dict] = report.get("results", [])
    by_cls: dict[str, int] = {}
    by_action: dict[str, int] = {}
    confidences: list[float] = []
    critical: list[dict] = []

    for r in results:
        cls = r.get("ai_classification", "UNKNOWN")
        action = r.get("ai_recommended_action", "UNKNOWN")
        conf = float(r.get("ai_confidence", 0.0))
        by_cls[cls] = by_cls.get(cls, 0) + 1
        by_action[action] = by_action.get(action, 0) + 1
        confidences.append(conf)
        if cls == "TRUE_FAILURE" and r.get("severity", "") in ("CRITICAL", "HIGH"):
            critical.append(
                {
                    "record_id": r.get("record_id"),
                    "rule_name": r.get("rule_name"),
                    "table_name": r.get("table_name"),
                    "severity": r.get("severity"),
                    "ai_recommended_action": action,
                }
            )

    avg_conf = round(sum(confidences) / len(confidences), 4) if confidences else 0.0

    return json.dumps(
        {
            "run_id": run_id,
            "total": len(results),
            "by_classification": by_cls,
            "by_action": by_action,
            "avg_confidence": avg_conf,
            "critical_failures": critical,
            "report_s3_key": f"s3://{LEARNING_BUCKET}/{key}",
        }
    )


@tool
def save_run_report(run_id: str, results_json: str) -> str:
    """
    Write a full classification run report to S3 as JSON.

    Call this once after all records in a batch have been classified and
    written back to audit_validation.  The report is the source of truth
    for get_batch_summary.

    Parameters
    ----------
    run_id       : The orchestrator run identifier.
    results_json : JSON array string — each element must be a dict with keys:
                   record_id, rule_name, table_name, column_name, severity,
                   ai_classification, ai_confidence, ai_recommended_action,
                   ai_explanation.

    Returns
    -------
    JSON string with 'success' (bool), 's3_key', and 'record_count'.
    """
    try:
        results = json.loads(results_json)
        if not isinstance(results, list):
            raise ValueError("results_json must be a JSON array")
    except (json.JSONDecodeError, ValueError) as exc:
        return json.dumps({"success": False, "error": str(exc)})

    report = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "record_count": len(results),
        "results": results,
    }
    key = f"{REPORTS_PREFIX}{run_id}.json"

    try:
        _s3().put_object(
            Bucket=LEARNING_BUCKET,
            Key=key,
            Body=json.dumps(report, indent=2).encode(),
            ContentType="application/json",
        )
    except Exception as exc:
        return json.dumps({"success": False, "error": str(exc)})

    return json.dumps(
        {
            "success": True,
            "s3_key": f"s3://{LEARNING_BUCKET}/{key}",
            "record_count": len(results),
        }
    )
