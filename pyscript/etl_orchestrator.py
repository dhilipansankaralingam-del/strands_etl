###################################################################################################
# SCRIPT        : etl_orchestrator.py
# PURPOSE       : End-to-end ETL orchestration:
#                   1) S3 stale folder detection
#                   2) Glue job triggering for stale sources
#                   3) Post-load validation queries via Athena (incl. cross-query comparisons)
#                   4) Summary mode: parallel Athena queries with chart generation
#                   5) Audit logging (pipe-delimited files on S3, MSCK REPAIR ready)
#                   6) Dashboard HTML email with embedded charts
#
# RUN MODES    : full_pipeline | validation_only | summary
#
# USAGE         : python etl_orchestrator.py --config s3://bucket/path/orchestrator_config.json
#                 python etl_orchestrator.py --config /local/path/orchestrator_config.json
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 01/2026       Dhilipan        Initial version
# 03/2026       Dhilipan        Cross-query comparisons, summary mode, chart generation
# 03/2026       Dhilipan        Athena retry, cost tracking, summary dashboard, streamlit
#
#=================================================================================================
###################################################################################################

import boto3
import json
import sys
import os
import time
import uuid
import argparse
import logging
import base64
import io
import math
from datetime import datetime, timezone, timedelta
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Optional: matplotlib for chart generation
try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_orchestrator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STATUS_STALE = "Stale"
STATUS_CHECK_SOURCE = "Check with Source"
STATUS_OK = "OK"
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_SKIPPED = "SKIPPED"
STATUS_ABORTED = "ABORTED"
GLUE_TERMINAL_STATES = ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR")
GLUE_POLL_INTERVAL_SECONDS = 30
ATHENA_RETRY_ATTEMPTS = 1  # retry once on failure
# Athena pricing: $5.00 per TB of data scanned (USD)
ATHENA_COST_PER_TB_SCANNED = 5.00
STATUS_WARN = "WARN"
STATUS_NOT_EXECUTED = "NOT_EXECUTED"

# Vibrant color palettes for charts
VIBRANT_COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#FF8C42", "#98D8C8", "#F7DC6F", "#BB8FCE",
    "#85C1E9", "#F1948A", "#82E0AA", "#F8C471", "#AED6F1",
]
CHART_BG = "#FAFBFC"
CHART_GRID = "#E8ECF0"

# ---------------------------------------------------------------------------
# AWS Clients (initialised lazily)
# ---------------------------------------------------------------------------
_s3 = None
_glue = None
_athena = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def glue_client():
    global _glue
    if _glue is None:
        _glue = boto3.client("glue")
    return _glue


def athena_client():
    global _athena
    if _athena is None:
        _athena = boto3.client("athena")
    return _athena


# ============================================================================
# 1. CONFIG LOADER
# ============================================================================
def load_config(config_path):
    """Load orchestrator config from a local path or S3 URI."""
    if config_path.startswith("s3://"):
        parts = config_path.replace("s3://", "").split("/", 1)
        bucket, key = parts[0], parts[1]
        resp = s3_client().get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    else:
        with open(config_path, "r") as f:
            return json.load(f)


# ============================================================================
# 2. S3 STALE FOLDER DETECTION
# ============================================================================
def list_subfolders(bucket, prefix):
    """Return list of (subfolder_prefix, latest_modified_time) under *prefix*."""
    client = s3_client()
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    common_prefixes = resp.get("CommonPrefixes", [])
    subfolders = []
    for cp in common_prefixes:
        sub_prefix = cp["Prefix"]
        latest = _get_latest_modified(bucket, sub_prefix)
        subfolders.append({"prefix": sub_prefix, "latest_modified": latest})
    return subfolders


def _get_latest_modified(bucket, prefix):
    """Return the most recent LastModified datetime for any object under prefix."""
    client = s3_client()
    paginator = client.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest:
                latest = obj["LastModified"]
    return latest


def get_prefix_last_modified(bucket, prefix):
    """Get last modified timestamp of the main prefix itself."""
    return _get_latest_modified(bucket, prefix)


def check_staleness(source, defaults):
    """Evaluate staleness for a single monitored source."""
    bucket = source["bucket"]
    prefix = source["prefix"]
    label = source.get("label", prefix)

    subfolder_count_threshold = source.get(
        "stale_subfolder_count_threshold",
        defaults.get("stale_subfolder_count_threshold", 3),
    )
    subfolder_hours_threshold = source.get(
        "stale_subfolder_hours_threshold",
        defaults.get("stale_subfolder_hours_threshold", 3),
    )
    no_subfolder_hours_threshold = source.get(
        "no_subfolder_hours_threshold",
        defaults.get("no_subfolder_hours_threshold", 5),
    )

    now = datetime.now(timezone.utc)
    subfolders = list_subfolders(bucket, prefix)
    subfolder_count = len(subfolders)

    result = {
        "bucket": bucket,
        "prefix": prefix,
        "label": label,
        "subfolder_count": subfolder_count,
        "stale_subfolders": [],
        "status": STATUS_OK,
        "detail": "",
        "checked_at": now.isoformat(),
    }

    if subfolder_count > 0:
        stale_sfs = []
        stale_details = []
        for sf in subfolders:
            if sf["latest_modified"] is None:
                stale_sfs.append(sf["prefix"])
                stale_details.append(f"{sf['prefix']} (no timestamp)")
                continue
            hours_since = (now - sf["latest_modified"]).total_seconds() / 3600
            if hours_since > subfolder_hours_threshold:
                stale_sfs.append(sf["prefix"])
                stale_details.append(f"{sf['prefix']} ({hours_since:.1f}h old)")

        if subfolder_count > subfolder_count_threshold:
            result["status"] = STATUS_STALE
            result["stale_subfolders"] = [sf["prefix"] for sf in subfolders]
            result["detail"] = (
                f"Subfolder count {subfolder_count} exceeds threshold "
                f"{subfolder_count_threshold} – processing all subfolders"
            )
            logger.warning("[%s] %s – %s", label, result["status"], result["detail"])
            return result

        if stale_sfs:
            result["status"] = STATUS_STALE
            result["stale_subfolders"] = stale_sfs
            result["detail"] = (
                f"{len(stale_sfs)} stale subfolder(s): "
                + "; ".join(stale_details)
            )
            logger.warning("[%s] %s – %s", label, result["status"], result["detail"])
            return result
    else:
        main_latest = get_prefix_last_modified(bucket, prefix)
        if main_latest is None:
            result["status"] = STATUS_CHECK_SOURCE
            result["detail"] = "No objects found under prefix"
            logger.warning("[%s] %s – %s", label, result["status"], result["detail"])
            return result

        hours_since = (now - main_latest).total_seconds() / 3600
        if hours_since > no_subfolder_hours_threshold:
            result["status"] = STATUS_CHECK_SOURCE
            result["detail"] = (
                f"No subfolders and last update was {hours_since:.1f}h ago "
                f"(threshold {no_subfolder_hours_threshold}h)"
            )
            logger.warning("[%s] %s – %s", label, result["status"], result["detail"])
            return result

    logger.info("[%s] %s", label, result["status"])
    return result


# ============================================================================
# 3. GLUE JOB TRIGGER
# ============================================================================
def trigger_glue_job(source, stale_subfolders, run_id):
    """Start an AWS Glue job for a stale source and wait for completion."""
    glue_cfg = source.get("glue_trigger", {})
    job_name = glue_cfg.get("job_name")
    if not job_name:
        logger.error("[%s] No glue_trigger.job_name configured – skipping", source["label"])
        return {
            "job_name": None, "job_run_id": None, "status": STATUS_SKIPPED,
            "detail": "No Glue job configured", "stale_subfolders_passed": [],
            "duration_seconds": 0,
        }

    stale_folders_csv = ",".join(stale_subfolders) if stale_subfolders else ""

    arguments = {
        "--json_file_name": glue_cfg.get("config_file_key", ""),
        "--bucket_name": glue_cfg.get("config_file_bucket", ""),
        "--orchestrator_run_id": run_id,
        "--stale_folders": stale_folders_csv,
        "--archive_s3_path": glue_cfg.get("archive_s3_path", ""),
        "--source_bucket": source.get("bucket", ""),
    }

    worker_type = glue_cfg.get("worker_type", "G.1X")
    num_workers = glue_cfg.get("workers", 10)
    timeout_min = glue_cfg.get("timeout_minutes", 120)

    logger.info(
        "[%s] Starting Glue job '%s' – workers=%s type=%s timeout=%smin",
        source["label"], job_name, num_workers, worker_type, timeout_min,
    )

    start_time = time.time()
    try:
        response = glue_client().start_job_run(
            JobName=job_name, Arguments=arguments,
            WorkerType=worker_type, NumberOfWorkers=num_workers, Timeout=timeout_min,
        )
        job_run_id = response["JobRunId"]
    except Exception as exc:
        logger.error("[%s] Failed to start Glue job: %s", source["label"], exc)
        return {
            "job_name": job_name, "job_run_id": None, "status": STATUS_FAIL,
            "detail": str(exc), "duration_seconds": 0,
        }

    logger.info("[%s] Glue job run started: %s", source["label"], job_run_id)

    state = "RUNNING"
    while state not in GLUE_TERMINAL_STATES:
        time.sleep(GLUE_POLL_INTERVAL_SECONDS)
        try:
            run_detail = glue_client().get_job_run(JobName=job_name, RunId=job_run_id)
            state = run_detail["JobRun"]["JobRunState"]
            logger.info("[%s] Glue job %s – state: %s", source["label"], job_run_id, state)
        except Exception as exc:
            logger.error("[%s] Error polling Glue job: %s", source["label"], exc)
            state = "ERROR"

    duration = time.time() - start_time
    status = STATUS_PASS if state == "SUCCEEDED" else STATUS_FAIL
    logger.info("[%s] Glue job %s finished – state=%s duration=%.0fs",
                source["label"], job_run_id, state, duration)

    return {
        "job_name": job_name, "job_run_id": job_run_id, "status": status,
        "glue_state": state, "detail": f"Glue state: {state}",
        "stale_subfolders_passed": stale_subfolders, "duration_seconds": round(duration),
    }


# ============================================================================
# 4. ATHENA QUERY HELPERS
# ============================================================================
def _run_athena_query_once(query, database, output_location, workgroup="primary"):
    """Execute a single Athena query attempt and return result with cost info."""
    client = athena_client()
    start_resp = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    query_id = start_resp["QueryExecutionId"]

    while True:
        time.sleep(5)
        status_resp = client.get_query_execution(QueryExecutionId=query_id)
        qe = status_resp["QueryExecution"]
        state = qe["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

    # Extract cost info (data scanned)
    stats = qe.get("Statistics", {})
    data_scanned_bytes = stats.get("DataScannedInBytes", 0)
    exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)
    cost_usd = (data_scanned_bytes / (1024 ** 4)) * ATHENA_COST_PER_TB_SCANNED

    failure_reason = ""
    if state != "SUCCEEDED":
        failure_reason = qe.get("Status", {}).get("StateChangeReason", "Unknown")

    result = {
        "query_id": query_id, "state": state, "rows": [],
        "data_scanned_bytes": data_scanned_bytes,
        "exec_time_ms": exec_time_ms,
        "cost_usd": round(cost_usd, 6),
        "failure_reason": failure_reason,
    }

    if state == "SUCCEEDED":
        try:
            data_resp = client.get_query_results(QueryExecutionId=query_id, MaxResults=500)
            rows = data_resp.get("ResultSet", {}).get("Rows", [])
            result["rows"] = rows
        except Exception as exc:
            logger.warning("Could not fetch results for query %s: %s", query_id, exc)

    return result


def run_athena_query(query, database, output_location, workgroup="primary"):
    """
    Execute an Athena query with retry logic.
    Retries once on failure. Returns result dict with cost info.
    """
    result = _run_athena_query_once(query, database, output_location, workgroup)

    if result["state"] != "SUCCEEDED" and ATHENA_RETRY_ATTEMPTS > 0:
        logger.warning(
            "Athena query %s failed (state=%s, reason=%s). Retrying once...",
            result["query_id"], result["state"], result["failure_reason"],
        )
        time.sleep(3)
        retry_result = _run_athena_query_once(query, database, output_location, workgroup)

        # Accumulate cost from both attempts
        retry_result["cost_usd"] = round(
            result.get("cost_usd", 0) + retry_result.get("cost_usd", 0), 6
        )
        retry_result["retry"] = True
        retry_result["original_failure"] = result["failure_reason"]

        if retry_result["state"] != "SUCCEEDED":
            logger.error(
                "Athena query RETRY also failed (id=%s, reason=%s). "
                "Recording as NOT_EXECUTED.",
                retry_result["query_id"], retry_result["failure_reason"],
            )

        return retry_result

    return result


def parse_athena_rows(rows):
    """
    Convert Athena result rows into (headers: list[str], data: list[dict]).
    First row is treated as header.
    """
    if not rows:
        return [], []

    header = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
    data = []
    for row in rows[1:]:
        values = [col.get("VarCharValue", "") for col in row.get("Data", [])]
        data.append(dict(zip(header, values)))
    return header, data


def extract_scalar(rows, column):
    """Extract a single numeric value from the first data row of Athena results."""
    header, data = parse_athena_rows(rows)
    if not data:
        return None
    val = data[0].get(column)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return val


def evaluate_condition(condition, rows):
    """Evaluate a simple condition string against Athena result rows."""
    if not rows or len(rows) < 2:
        if "row_count" in condition and "== 0" in condition:
            return True, 0
        return False, None

    header = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
    data = [col.get("VarCharValue", "") for col in rows[1].get("Data", [])]
    data_row_count = len(rows) - 1

    ctx = {"row_count": data_row_count}
    for h, v in zip(header, data):
        try:
            ctx[h] = float(v)
        except (ValueError, TypeError):
            ctx[h] = v

    try:
        passed = bool(eval(condition, {"__builtins__": {}}, ctx))  # noqa: S307
        for key in ctx:
            if key in condition and key != "row_count":
                return passed, ctx.get(key)
        return passed, ctx.get("row_count", None)
    except Exception as exc:
        logger.warning("Condition evaluation failed: %s – %s", condition, exc)
        return False, None


# ============================================================================
# 5. VALIDATIONS (single-query + cross-query comparisons)
# ============================================================================
def _execute_single_validation(qdef, output_location, workgroup):
    """Run one Athena validation query with retry and return a result dict."""
    query_str = qdef["query"]
    database = qdef.get("database", "default")
    condition = qdef.get("condition", "")

    logger.info("Running validation: %s", qdef["name"])

    try:
        athena_result = run_athena_query(query_str, database, output_location, workgroup)
    except Exception as exc:
        logger.error("Athena query failed for %s: %s", qdef["name"], exc)
        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "check_type": qdef.get("check_type", ""),
            "abort_on_failure": qdef.get("abort_on_failure", False),
            "status": STATUS_NOT_EXECUTED,
            "detail": f"Query execution error: {exc}",
            "actual_value": None, "query": query_str,
            "cost_usd": 0, "retried": False, "failure_reason": str(exc),
        }

    retried = athena_result.get("retry", False)
    cost_usd = athena_result.get("cost_usd", 0)

    if athena_result["state"] != "SUCCEEDED":
        failure_reason = athena_result.get("failure_reason", "Unknown")
        status = STATUS_NOT_EXECUTED
        detail = (f"Query not executed successfully after retry. "
                  f"Reason: {failure_reason}")
        actual = None
    else:
        passed, actual = evaluate_condition(condition, athena_result["rows"])
        status = STATUS_PASS if passed else STATUS_FAIL
        detail = f"Condition '{condition}' -> {'met' if passed else 'not met'} (actual={actual})"
        if retried:
            detail += " [succeeded on retry]"
        failure_reason = ""

    logger.info("Validation %s: %s – %s", qdef["name"], status, detail)

    return {
        "name": qdef["name"], "description": qdef.get("description", ""),
        "check_type": qdef.get("check_type", ""),
        "abort_on_failure": qdef.get("abort_on_failure", False),
        "status": status, "detail": detail,
        "actual_value": actual, "query": query_str,
        "cost_usd": cost_usd, "retried": retried,
        "failure_reason": failure_reason,
    }


def _execute_comparison_check(cdef, output_location, workgroup):
    """
    Run two Athena queries (query_a, query_b) and compare their results.

    Supported rules:
      match               – values must be exactly equal
      difference_within   – abs(a - b) <= tolerance
      percentage_within   – abs(a - b) / max(|a|, |b|) * 100 <= tolerance
      a_greater_than_b    – a > b
      a_less_than_b       – a < b
      a_equals_zero       – a == 0
      b_equals_zero       – b == 0
      a_not_zero          – a != 0
      b_not_zero          – b != 0
      custom              – user provides a Python expression with a_val, b_val
    """
    name = cdef["name"]
    qa = cdef["query_a"]
    qb = cdef["query_b"]
    rule = cdef.get("rule", "match")
    column = cdef.get("compare_column", "")
    tolerance = cdef.get("tolerance", 0)
    custom_expr = cdef.get("custom_expression", "")
    abort_on_failure = cdef.get("abort_on_failure", False)

    logger.info("Running comparison: %s (rule=%s)", name, rule)

    # Execute both queries (can be parallelised later)
    try:
        result_a = run_athena_query(qa["query"], qa.get("database", "default"),
                                    output_location, workgroup)
        result_b = run_athena_query(qb["query"], qb.get("database", "default"),
                                    output_location, workgroup)
    except Exception as exc:
        logger.error("Comparison query failed for %s: %s", name, exc)
        return {
            "name": name, "description": cdef.get("description", ""),
            "check_type": "comparison", "rule": rule,
            "abort_on_failure": abort_on_failure,
            "status": STATUS_FAIL, "detail": str(exc),
            "value_a": None, "value_b": None,
            "label_a": qa.get("label", "Query A"),
            "label_b": qb.get("label", "Query B"),
        }

    if result_a["state"] != "SUCCEEDED" or result_b["state"] != "SUCCEEDED":
        return {
            "name": name, "description": cdef.get("description", ""),
            "check_type": "comparison", "rule": rule,
            "abort_on_failure": abort_on_failure,
            "status": STATUS_FAIL,
            "detail": f"Query states: A={result_a['state']}, B={result_b['state']}",
            "value_a": None, "value_b": None,
            "label_a": qa.get("label", "Query A"),
            "label_b": qb.get("label", "Query B"),
        }

    # Extract scalar values
    a_val = extract_scalar(result_a["rows"], column) if column else None
    b_val = extract_scalar(result_b["rows"], column) if column else None

    # If no specific column, get the first numeric value from first row
    if a_val is None and not column:
        _, data_a = parse_athena_rows(result_a["rows"])
        if data_a:
            for v in data_a[0].values():
                try:
                    a_val = float(v)
                    break
                except (ValueError, TypeError):
                    pass
    if b_val is None and not column:
        _, data_b = parse_athena_rows(result_b["rows"])
        if data_b:
            for v in data_b[0].values():
                try:
                    b_val = float(v)
                    break
                except (ValueError, TypeError):
                    pass

    # Evaluate rule
    passed = False
    detail = ""

    try:
        if rule == "match":
            passed = a_val == b_val
            detail = f"A={a_val}, B={b_val} → {'matched' if passed else 'mismatch'}"

        elif rule == "difference_within":
            if a_val is not None and b_val is not None:
                diff = abs(float(a_val) - float(b_val))
                passed = diff <= tolerance
                detail = f"A={a_val}, B={b_val}, diff={diff}, tolerance={tolerance}"
            else:
                detail = f"Cannot compare: A={a_val}, B={b_val}"

        elif rule == "percentage_within":
            if a_val is not None and b_val is not None:
                max_val = max(abs(float(a_val)), abs(float(b_val)))
                pct_diff = (abs(float(a_val) - float(b_val)) / max_val * 100) if max_val else 0
                passed = pct_diff <= tolerance
                detail = f"A={a_val}, B={b_val}, pct_diff={pct_diff:.2f}%, tolerance={tolerance}%"
            else:
                detail = f"Cannot compare: A={a_val}, B={b_val}"

        elif rule == "a_greater_than_b":
            if a_val is not None and b_val is not None:
                passed = float(a_val) > float(b_val)
                detail = f"A={a_val} {'>' if passed else '<='} B={b_val}"
            else:
                detail = f"Cannot compare: A={a_val}, B={b_val}"

        elif rule == "a_less_than_b":
            if a_val is not None and b_val is not None:
                passed = float(a_val) < float(b_val)
                detail = f"A={a_val} {'<' if passed else '>='} B={b_val}"
            else:
                detail = f"Cannot compare: A={a_val}, B={b_val}"

        elif rule == "a_equals_zero":
            passed = a_val is not None and float(a_val) == 0
            detail = f"A={a_val} → {'is zero' if passed else 'not zero'}"

        elif rule == "b_equals_zero":
            passed = b_val is not None and float(b_val) == 0
            detail = f"B={b_val} → {'is zero' if passed else 'not zero'}"

        elif rule == "a_not_zero":
            passed = a_val is not None and float(a_val) != 0
            detail = f"A={a_val} → {'not zero' if passed else 'is zero'}"

        elif rule == "b_not_zero":
            passed = b_val is not None and float(b_val) != 0
            detail = f"B={b_val} → {'not zero' if passed else 'is zero'}"

        elif rule == "custom":
            ctx = {"a_val": a_val, "b_val": b_val, "abs": abs, "max": max, "min": min}
            passed = bool(eval(custom_expr, {"__builtins__": {}}, ctx))  # noqa: S307
            detail = f"A={a_val}, B={b_val}, expr='{custom_expr}' → {passed}"

        else:
            detail = f"Unknown rule: {rule}"

    except Exception as exc:
        detail = f"Rule evaluation error: {exc}"

    status = STATUS_PASS if passed else STATUS_FAIL
    logger.info("Comparison %s: %s – %s", name, status, detail)

    return {
        "name": name, "description": cdef.get("description", ""),
        "check_type": "comparison", "rule": rule,
        "abort_on_failure": abort_on_failure,
        "status": status, "detail": detail,
        "value_a": a_val, "value_b": b_val,
        "label_a": qa.get("label", "Query A"),
        "label_b": qb.get("label", "Query B"),
    }


def run_validations(config, stale_results):
    """
    Execute all validation queries + comparison checks from config.
    Returns (list of result dicts, aborted: bool).
    """
    val_cfg = config.get("validations", {})
    queries = val_cfg.get("queries", [])
    comparisons = val_cfg.get("comparison_checks", [])
    execution_mode = val_cfg.get("execution_mode", "sequential")
    max_parallel = val_cfg.get("max_parallel", 5)
    output_bucket = val_cfg.get("athena_output_bucket", "")
    output_prefix = val_cfg.get("athena_output_prefix", "query_results/")
    workgroup = val_cfg.get("athena_workgroup", "primary")
    output_location = f"s3://{output_bucket}/{output_prefix}"

    results = []
    comparison_results = []
    abort = False

    # ---- Single-query validations ----
    if execution_mode == "parallel":
        logger.info("Running %d validations in PARALLEL (max_parallel=%d)",
                     len(queries), max_parallel)
        futures = {}
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            for qdef in queries:
                future = executor.submit(
                    _execute_single_validation, qdef, output_location, workgroup)
                futures[future] = qdef
            for future in as_completed(futures):
                vr = future.result()
                results.append(vr)
                if vr["status"] == STATUS_FAIL and vr.get("abort_on_failure"):
                    abort = True
                    logger.error("ABORT: Validation '%s' failed", vr["name"])

        query_order = {q["name"]: i for i, q in enumerate(queries)}
        results.sort(key=lambda r: query_order.get(r["name"], 999))
    else:
        logger.info("Running %d validations SEQUENTIALLY", len(queries))
        for qdef in queries:
            if abort:
                results.append({
                    "name": qdef["name"], "description": qdef.get("description", ""),
                    "check_type": qdef.get("check_type", ""), "status": STATUS_ABORTED,
                    "detail": "Aborted due to prior failure",
                    "actual_value": None, "query": qdef["query"],
                })
                continue
            vr = _execute_single_validation(qdef, output_location, workgroup)
            results.append(vr)
            if vr["status"] == STATUS_FAIL and vr.get("abort_on_failure"):
                abort = True

    # Strip internal key
    for r in results:
        r.pop("abort_on_failure", None)

    # ---- Cross-query comparison checks ----
    if comparisons and not abort:
        logger.info("Running %d comparison checks", len(comparisons))
        if execution_mode == "parallel":
            futures = {}
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                for cdef in comparisons:
                    future = executor.submit(
                        _execute_comparison_check, cdef, output_location, workgroup)
                    futures[future] = cdef
                for future in as_completed(futures):
                    cr = future.result()
                    comparison_results.append(cr)
                    if cr["status"] == STATUS_FAIL and cr.get("abort_on_failure"):
                        abort = True
                        logger.error("ABORT: Comparison '%s' failed", cr["name"])
            comp_order = {c["name"]: i for i, c in enumerate(comparisons)}
            comparison_results.sort(key=lambda r: comp_order.get(r["name"], 999))
        else:
            for cdef in comparisons:
                if abort:
                    comparison_results.append({
                        "name": cdef["name"], "description": cdef.get("description", ""),
                        "check_type": "comparison", "rule": cdef.get("rule", ""),
                        "status": STATUS_ABORTED, "detail": "Aborted due to prior failure",
                        "value_a": None, "value_b": None,
                        "label_a": cdef.get("query_a", {}).get("label", "A"),
                        "label_b": cdef.get("query_b", {}).get("label", "B"),
                    })
                    continue
                cr = _execute_comparison_check(cdef, output_location, workgroup)
                comparison_results.append(cr)
                if cr["status"] == STATUS_FAIL and cr.get("abort_on_failure"):
                    abort = True

        for cr in comparison_results:
            cr.pop("abort_on_failure", None)
    elif comparisons and abort:
        for cdef in comparisons:
            comparison_results.append({
                "name": cdef["name"], "description": cdef.get("description", ""),
                "check_type": "comparison", "rule": cdef.get("rule", ""),
                "status": STATUS_ABORTED, "detail": "Aborted due to prior failure",
                "value_a": None, "value_b": None,
                "label_a": cdef.get("query_a", {}).get("label", "A"),
                "label_b": cdef.get("query_b", {}).get("label", "B"),
            })

    return results, comparison_results, abort


# ============================================================================
# 6. SUMMARY MODE — Parallel queries + chart data
# ============================================================================
def run_summary_queries(config):
    """
    Execute all summary queries in parallel.
    Returns list of {name, description, chart_config, headers, data, status, detail}.
    """
    sum_cfg = config.get("summary", {})
    queries = sum_cfg.get("queries", [])
    max_parallel = sum_cfg.get("max_parallel", 5)
    output_bucket = sum_cfg.get("athena_output_bucket",
                                config.get("validations", {}).get("athena_output_bucket", ""))
    output_prefix = sum_cfg.get("athena_output_prefix", "summary_results/")
    workgroup = sum_cfg.get("athena_workgroup", "primary")
    output_location = f"s3://{output_bucket}/{output_prefix}"

    if not queries:
        logger.info("No summary queries configured")
        return []

    logger.info("Running %d summary queries in PARALLEL (max_parallel=%d)",
                len(queries), max_parallel)

    def _run_one(qdef):
        try:
            result = run_athena_query(
                qdef["query"], qdef.get("database", "default"),
                output_location, workgroup)
        except Exception as exc:
            return {
                "name": qdef["name"], "description": qdef.get("description", ""),
                "chart": qdef.get("chart"), "headers": [], "data": [],
                "status": STATUS_FAIL, "detail": str(exc),
            }
        if result["state"] != "SUCCEEDED":
            return {
                "name": qdef["name"], "description": qdef.get("description", ""),
                "chart": qdef.get("chart"), "headers": [], "data": [],
                "status": STATUS_FAIL, "detail": f"State: {result['state']}",
            }
        headers, data = parse_athena_rows(result["rows"])
        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "chart": qdef.get("chart"), "headers": headers, "data": data,
            "status": STATUS_PASS, "detail": f"{len(data)} row(s) returned",
        }

    results = []
    futures = {}
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        for qdef in queries:
            futures[executor.submit(_run_one, qdef)] = qdef
        for future in as_completed(futures):
            results.append(future.result())

    # Re-order to match config order
    order = {q["name"]: i for i, q in enumerate(queries)}
    results.sort(key=lambda r: order.get(r["name"], 999))

    return results


# ============================================================================
# 7. CHART GENERATION (matplotlib → base64 PNG)
# ============================================================================
def generate_chart_base64(chart_cfg, headers, data):
    """
    Generate a chart as a base64-encoded PNG string.
    Returns base64 string or None if matplotlib is unavailable or data is empty.

    chart_cfg keys:
      type         : "bar" | "pie" | "trend"
      title        : chart title
      x_column     : column for X axis (bar, trend)
      y_column     : column for Y axis (bar, trend)
      label_column : column for pie labels
      value_column : column for pie values
    """
    if not HAS_MATPLOTLIB or not data:
        return None

    chart_type = chart_cfg.get("type", "bar")
    title = chart_cfg.get("title", "Chart")

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(CHART_BG)
    ax.set_facecolor(CHART_BG)

    colors = VIBRANT_COLORS[:len(data)]
    # Extend colours if we have more data points than palette entries
    while len(colors) < len(data):
        colors += VIBRANT_COLORS

    if chart_type == "bar":
        x_col = chart_cfg.get("x_column", headers[0] if headers else "")
        y_col = chart_cfg.get("y_column", headers[1] if len(headers) > 1 else "")
        labels = [str(row.get(x_col, "")) for row in data]
        values = []
        for row in data:
            try:
                values.append(float(row.get(y_col, 0)))
            except (ValueError, TypeError):
                values.append(0)

        bars = ax.barh(labels, values, color=colors[:len(labels)], edgecolor="white", linewidth=0.8)
        ax.set_xlabel(y_col, fontsize=11, fontweight="bold", color="#2c3e50")
        ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=15)
        ax.invert_yaxis()
        ax.grid(axis="x", color=CHART_GRID, linestyle="--", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Value labels on bars
        for bar, val in zip(bars, values):
            ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
                    f"{val:,.0f}", va="center", fontsize=10, fontweight="bold", color="#2c3e50")

    elif chart_type == "pie":
        label_col = chart_cfg.get("label_column", headers[0] if headers else "")
        value_col = chart_cfg.get("value_column", headers[1] if len(headers) > 1 else "")
        labels = [str(row.get(label_col, "")) for row in data]
        values = []
        for row in data:
            try:
                values.append(float(row.get(value_col, 0)))
            except (ValueError, TypeError):
                values.append(0)

        wedges, texts, autotexts = ax.pie(
            values, labels=labels, colors=colors[:len(labels)],
            autopct="%1.1f%%", startangle=140, pctdistance=0.75,
            wedgeprops={"edgecolor": "white", "linewidth": 2},
        )
        for t in autotexts:
            t.set_fontsize(10)
            t.set_fontweight("bold")
            t.set_color("white")
        for t in texts:
            t.set_fontsize(10)
            t.set_color("#2c3e50")
        ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=20)

    elif chart_type == "trend":
        x_col = chart_cfg.get("x_column", headers[0] if headers else "")
        y_col = chart_cfg.get("y_column", headers[1] if len(headers) > 1 else "")
        x_vals = [str(row.get(x_col, "")) for row in data]
        y_vals = []
        for row in data:
            try:
                y_vals.append(float(row.get(y_col, 0)))
            except (ValueError, TypeError):
                y_vals.append(0)

        ax.fill_between(range(len(x_vals)), y_vals, alpha=0.15, color="#45B7D1")
        ax.plot(range(len(x_vals)), y_vals, color="#45B7D1", linewidth=3,
                marker="o", markersize=8, markerfacecolor="#FF6B6B",
                markeredgecolor="white", markeredgewidth=2)

        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=9)
        ax.set_ylabel(y_col, fontsize=11, fontweight="bold", color="#2c3e50")
        ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=15)
        ax.grid(axis="y", color=CHART_GRID, linestyle="--", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Value labels on points
        for i, val in enumerate(y_vals):
            ax.annotate(f"{val:,.0f}", (i, val), textcoords="offset points",
                        xytext=(0, 12), ha="center", fontsize=9,
                        fontweight="bold", color="#2c3e50")
    else:
        logger.warning("Unknown chart type: %s", chart_type)
        plt.close(fig)
        return None

    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_html_bar_chart(chart_cfg, headers, data):
    """Fallback: generate a bar chart as pure HTML tables (no matplotlib needed)."""
    x_col = chart_cfg.get("x_column", headers[0] if headers else "")
    y_col = chart_cfg.get("y_column", headers[1] if len(headers) > 1 else "")
    title = chart_cfg.get("title", "Chart")

    values = []
    labels = []
    for row in data:
        labels.append(str(row.get(x_col, "")))
        try:
            values.append(float(row.get(y_col, 0)))
        except (ValueError, TypeError):
            values.append(0)

    max_val = max(values) if values else 1

    html = f'<h3 style="color:#2c3e50;margin-bottom:10px;">{title}</h3>'
    html += '<table style="width:100%;border-collapse:collapse;font-size:13px;">'
    for i, (lbl, val) in enumerate(zip(labels, values)):
        pct = (val / max_val * 100) if max_val else 0
        color = VIBRANT_COLORS[i % len(VIBRANT_COLORS)]
        html += f"""<tr>
            <td style="width:150px;padding:6px 8px;font-weight:bold;color:#2c3e50;">{lbl}</td>
            <td style="padding:6px 4px;">
                <div style="background:{color};width:{pct:.0f}%;height:28px;border-radius:4px;
                            display:inline-block;min-width:2px;"></div>
                <span style="margin-left:8px;font-weight:bold;color:#2c3e50;">{val:,.0f}</span>
            </td></tr>"""
    html += "</table>"
    return html


# ============================================================================
# 8. AUDIT LOGGING
# ============================================================================
def build_audit_records(run_id, run_date, stale_results, glue_results,
                        validation_results, comparison_results,
                        summary_results, overall_status):
    """Flatten all events into a list of audit record dicts."""
    records = []
    ts = datetime.now(timezone.utc).isoformat()

    for sr in stale_results:
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", "STALE_CHECK"), ("source_label", sr["label"]),
            ("source_bucket", sr["bucket"]), ("source_prefix", sr["prefix"]),
            ("subfolder_count", str(sr["subfolder_count"])),
            ("status", sr["status"]), ("detail", sr["detail"]),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", ""), ("check_type", ""),
            ("actual_value", ""), ("query", ""), ("overall_status", overall_status),
        ]))

    for label, gr in glue_results.items():
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", "GLUE_TRIGGER"), ("source_label", label),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", gr["status"]), ("detail", gr.get("detail", "")),
            ("glue_job_name", gr.get("job_name", "")),
            ("glue_job_run_id", gr.get("job_run_id", "")),
            ("glue_duration_sec", str(gr.get("duration_seconds", ""))),
            ("validation_name", ""), ("check_type", ""),
            ("actual_value", ""), ("query", ""), ("overall_status", overall_status),
        ]))

    for vr in validation_results:
        retry_info = " [retried]" if vr.get("retried") else ""
        fail_reason = vr.get("failure_reason", "")
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", "VALIDATION"), ("source_label", ""),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", vr["status"]),
            ("detail", vr["detail"] + retry_info),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", vr["name"]), ("check_type", vr.get("check_type", "")),
            ("actual_value", str(vr.get("actual_value", ""))),
            ("query", vr.get("query", "")), ("overall_status", overall_status),
            ("cost_usd", str(vr.get("cost_usd", 0))),
            ("failure_reason", fail_reason),
        ]))

    for cr in comparison_results:
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", "COMPARISON"), ("source_label", ""),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", cr["status"]), ("detail", cr["detail"]),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", cr["name"]), ("check_type", f"comparison:{cr.get('rule', '')}"),
            ("actual_value", f"A={cr.get('value_a')},B={cr.get('value_b')}"),
            ("query", ""), ("overall_status", overall_status),
        ]))

    for sr in summary_results:
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", "SUMMARY_QUERY"), ("source_label", ""),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", sr["status"]), ("detail", sr["detail"]),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", sr["name"]), ("check_type", "summary"),
            ("actual_value", ""), ("query", ""), ("overall_status", overall_status),
        ]))

    return records


def write_audit_to_s3(records, audit_cfg, run_date):
    """Write audit records as a pipe-delimited file to S3."""
    if not records:
        logger.info("No audit records to write.")
        return None

    bucket = audit_cfg["s3_bucket"]
    prefix = audit_cfg["s3_prefix"].rstrip("/")
    file_id = uuid.uuid4().hex[:8]
    key = f"{prefix}/run_date={run_date}/audit_{file_id}.csv"

    header = "|".join(records[0].keys())
    lines = [header]
    for rec in records:
        line = "|".join(str(v).replace("|", "\\|") for v in rec.values())
        lines.append(line)

    body = "\n".join(lines)
    s3_client().put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    s3_path = f"s3://{bucket}/{key}"
    logger.info("Audit log written to %s (%d records)", s3_path, len(records))
    return s3_path


def run_msck_repair(audit_cfg):
    """Run MSCK REPAIR TABLE on the audit table."""
    database = audit_cfg.get("database", "audit_db")
    table = audit_cfg.get("table", "etl_orchestrator_audit")
    query = f"MSCK REPAIR TABLE {database}.{table}"
    output_location = f"s3://{audit_cfg['s3_bucket']}/{audit_cfg['s3_prefix']}msck_results/"
    logger.info("Running MSCK REPAIR TABLE on %s.%s", database, table)
    try:
        run_athena_query(query, database, output_location)
        logger.info("MSCK REPAIR TABLE completed.")
    except Exception as exc:
        logger.warning("MSCK REPAIR TABLE failed (non-fatal): %s", exc)


# ============================================================================
# 9. DASHBOARD HTML EMAIL
# ============================================================================
def _status_color(status):
    return {
        STATUS_OK: "#28a745", STATUS_STALE: "#dc3545",
        STATUS_CHECK_SOURCE: "#fd7e14", STATUS_PASS: "#28a745",
        STATUS_FAIL: "#dc3545", STATUS_SKIPPED: "#6c757d",
        STATUS_ABORTED: "#6c757d", STATUS_WARN: "#fd7e14",
        STATUS_NOT_EXECUTED: "#6c757d",
    }.get(status, "#333333")


def generate_dashboard_html(
    run_id, run_date, run_mode, stale_results, glue_results,
    validation_results, comparison_results, summary_results,
    overall_status, audit_s3_path, run_duration_sec=0, total_cost_usd=0.0
):
    """Generate a full dashboard HTML email body with charts and KPI scorecards."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    overall_color = "#28a745" if overall_status == STATUS_PASS else "#dc3545"

    total_sources = len(stale_results)
    stale_count = sum(1 for r in stale_results if r["status"] == STATUS_STALE)
    check_source_count = sum(1 for r in stale_results if r["status"] == STATUS_CHECK_SOURCE)

    # KPI calculations
    all_checks = validation_results + comparison_results
    total_checks = len(all_checks)
    passed_count = sum(1 for c in all_checks if c["status"] == STATUS_PASS)
    failed_count = sum(1 for c in all_checks if c["status"] == STATUS_FAIL)
    warn_count = sum(1 for c in all_checks if c["status"] == STATUS_WARN)
    not_exec_count = sum(1 for c in all_checks if c["status"] == STATUS_NOT_EXECUTED)
    pass_rate = (passed_count / total_checks * 100) if total_checks > 0 else 0
    duration_str = f"{int(run_duration_sec // 60)}m {int(run_duration_sec % 60)}s"

    html = f"""
    <html><head><style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; color: #333;
               background-color: #f5f6fa; }}
        .container {{ max-width: 960px; margin: 0 auto; background: white;
                      border-radius: 12px; padding: 30px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 12px;
              font-size: 22px; }}
        h2 {{ color: #2c3e50; margin-top: 30px; font-size: 17px;
              border-left: 4px solid #3498db; padding-left: 12px; }}
        .summary-box {{ display: inline-block; padding: 12px 22px; margin: 4px 6px 4px 0;
                        border-radius: 8px; color: white; font-size: 14px;
                        font-weight: bold; text-align: center; min-width: 110px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 13px; }}
        th {{ background-color: #4285f4; color: #1a1a2e;
              padding: 10px 12px; text-align: left;
              font-size: 12px; font-weight: bold; letter-spacing: 0.3px; }}
        td {{ padding: 8px 12px; border-bottom: 1px solid #eee; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .badge {{ display: inline-block; padding: 4px 10px; border-radius: 4px;
                  color: white; font-weight: bold; font-size: 11px; }}
        .chart-container {{ margin: 20px 0; text-align: center; }}
        .chart-container img {{ max-width: 100%; border-radius: 8px;
                                box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .footer {{ margin-top: 30px; font-size: 11px; color: #888;
                   border-top: 1px solid #eee; padding-top: 15px; }}
        .mode-badge {{ display: inline-block; padding: 4px 12px; border-radius: 12px;
                       background: #3498db; color: white; font-size: 11px;
                       font-weight: bold; margin-left: 10px; }}
    </style></head>
    <body><div class="container">

    <h1>Strands ETL Orchestrator Dashboard
        <span class="mode-badge">{run_mode.upper().replace('_', ' ')}</span></h1>
    <p><b>Run ID:</b> {run_id} &nbsp;|&nbsp; <b>Date:</b> {run_date} &nbsp;|&nbsp;
       <b>Generated:</b> {now_str}</p>

    <div style="margin: 15px 0;">
        <span class="summary-box" style="background-color: {overall_color};">
            Overall: {overall_status}</span>"""

    if run_mode == "full_pipeline":
        html += f"""
        <span class="summary-box" style="background-color: #17a2b8;">Sources: {total_sources}</span>
        <span class="summary-box" style="background-color: {'#dc3545' if stale_count else '#28a745'};">
            Stale: {stale_count}</span>
        <span class="summary-box" style="background-color: {'#fd7e14' if check_source_count else '#28a745'};">
            Check Source: {check_source_count}</span>"""

    val_pass = sum(1 for v in validation_results if v["status"] == STATUS_PASS)
    val_fail = sum(1 for v in validation_results if v["status"] == STATUS_FAIL)
    comp_pass = sum(1 for c in comparison_results if c["status"] == STATUS_PASS)
    comp_fail = sum(1 for c in comparison_results if c["status"] == STATUS_FAIL)

    if validation_results or comparison_results:
        html += f"""
        <span class="summary-box" style="background-color: {'#28a745' if not val_fail else '#dc3545'};">
            Validations: {val_pass}/{val_pass + val_fail}</span>"""
    if comparison_results:
        html += f"""
        <span class="summary-box" style="background-color: {'#28a745' if not comp_fail else '#dc3545'};">
            Comparisons: {comp_pass}/{comp_pass + comp_fail}</span>"""

    html += "</div>"

    # ---- KPI Scorecard (shown when validations exist) ----
    if total_checks > 0:
        pass_color = "#28a745" if pass_rate >= 80 else ("#fd7e14" if pass_rate >= 60 else "#dc3545")
        html += f"""
    <div style="display:flex;flex-wrap:wrap;gap:12px;margin:20px 0;">
      <div style="flex:1;min-width:140px;background:#f0fff0;border-left:4px solid #28a745;
                  border-radius:8px;padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">PASSED</div>
        <div style="color:#28a745;font-size:28px;font-weight:bold;">{passed_count}</div>
        <div style="color:#888;font-size:11px;">{pass_rate:.1f}% of total</div>
      </div>
      <div style="flex:1;min-width:140px;background:#fff8e1;border-left:4px solid #fd7e14;
                  border-radius:8px;padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">WARNINGS</div>
        <div style="color:#fd7e14;font-size:28px;font-weight:bold;">{warn_count}</div>
        <div style="color:#888;font-size:11px;">{(warn_count/total_checks*100):.1f}% of total</div>
      </div>
      <div style="flex:1;min-width:140px;background:#fff0f0;border-left:4px solid #dc3545;
                  border-radius:8px;padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">FAILURES</div>
        <div style="color:#dc3545;font-size:28px;font-weight:bold;">{failed_count}</div>
        <div style="color:#888;font-size:11px;">{(failed_count/total_checks*100):.1f}% of total</div>
      </div>
      <div style="flex:1;min-width:140px;background:#f5f5f5;border-left:4px solid #6c757d;
                  border-radius:8px;padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">NOT EXECUTED</div>
        <div style="color:#6c757d;font-size:28px;font-weight:bold;">{not_exec_count}</div>
        <div style="color:#888;font-size:11px;">{(not_exec_count/total_checks*100):.1f}% of total</div>
      </div>
      <div style="flex:1;min-width:140px;background:#f0f4ff;border-left:4px solid #3498db;
                  border-radius:8px;padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">TOTAL CHECKS</div>
        <div style="color:#2c3e50;font-size:28px;font-weight:bold;">{total_checks}</div>
        <div style="color:#888;font-size:11px;">Duration: {duration_str}</div>
      </div>
    </div>

    <div style="display:flex;gap:15px;margin-bottom:20px;">
      <div style="flex:1;background:#fafbfc;border:1px solid #eee;border-radius:8px;
                  padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">PASS RATE</div>
        <div style="margin:8px 0;">
          <div style="background:#e9ecef;border-radius:10px;height:20px;overflow:hidden;">
            <div style="background:{pass_color};height:100%;width:{pass_rate:.0f}%;
                        border-radius:10px;transition:width 0.3s;"></div>
          </div>
        </div>
        <div style="color:{pass_color};font-size:22px;font-weight:bold;">{pass_rate:.1f}%</div>
      </div>
      <div style="flex:1;background:#fafbfc;border:1px solid #eee;border-radius:8px;
                  padding:15px;text-align:center;">
        <div style="color:#666;font-size:11px;font-weight:bold;">ATHENA QUERY COST</div>
        <div style="color:#8e44ad;font-size:22px;font-weight:bold;margin-top:12px;">
          ${total_cost_usd:.4f}</div>
        <div style="color:#888;font-size:11px;margin-top:4px;">
          $5.00 per TB scanned</div>
      </div>
    </div>
    """

    # ---- Section: Stale Detection (full_pipeline only) ----
    if run_mode == "full_pipeline" and stale_results:
        html += """<h2>1. S3 Stale File Detection</h2>
        <table><thead><tr><th>#</th><th>Source</th><th>Bucket / Prefix</th>
        <th>Subfolders</th><th>Status</th><th>Detail</th></tr></thead><tbody>"""
        for i, sr in enumerate(stale_results, 1):
            color = _status_color(sr["status"])
            html += f"""<tr><td>{i}</td><td><b>{sr['label']}</b></td>
                <td>{sr['bucket']}/{sr['prefix']}</td>
                <td style="text-align:center;">{sr['subfolder_count']}</td>
                <td><span class="badge" style="background-color:{color};">{sr['status']}</span></td>
                <td>{sr['detail'] or '-'}</td></tr>"""
        html += "</tbody></table>"

    # ---- Section: Glue Jobs ----
    if run_mode == "full_pipeline":
        html += "<h2>2. Glue Job Triggers</h2>"
        if glue_results:
            html += """<table><thead><tr><th>#</th><th>Source</th><th>Glue Job</th>
                <th>Run ID</th><th>Status</th><th>Duration</th><th>Detail</th>
                </tr></thead><tbody>"""
            for i, (label, gr) in enumerate(glue_results.items(), 1):
                color = _status_color(gr["status"])
                dur = f"{gr.get('duration_seconds', 0)}s" if gr.get("duration_seconds") else "-"
                html += f"""<tr><td>{i}</td><td><b>{label}</b></td>
                    <td>{gr.get('job_name', '-')}</td>
                    <td>{gr.get('job_run_id', '-') or '-'}</td>
                    <td><span class="badge" style="background-color:{color};">{gr['status']}</span></td>
                    <td>{dur}</td><td>{gr.get('detail', '-')}</td></tr>"""
            html += "</tbody></table>"
        else:
            html += "<p>No Glue jobs were triggered.</p>"

    # ---- Section: Validations ----
    section_num = 3 if run_mode == "full_pipeline" else 1
    if validation_results:
        html += f"<h2>{section_num}. Post-Load Validations</h2>"
        html += """<table><thead><tr><th>#</th><th>Validation</th><th>Check Type</th>
            <th>Status</th><th>Actual</th><th>Cost</th><th>Detail</th></tr></thead><tbody>"""
        for i, vr in enumerate(validation_results, 1):
            color = _status_color(vr["status"])
            retry_badge = ' <span style="color:#fd7e14;font-size:10px;">[retried]</span>' if vr.get("retried") else ""
            cost_str = f"${vr.get('cost_usd', 0):.4f}" if vr.get("cost_usd") else "-"
            row_style = ' style="background:#f5f5f5;"' if vr["status"] == STATUS_NOT_EXECUTED else ""
            html += f"""<tr{row_style}><td>{i}</td>
                <td><b>{vr['name']}</b>{retry_badge}<br>
                    <small>{vr.get('description', '')}</small></td>
                <td>{vr.get('check_type', '-')}</td>
                <td><span class="badge" style="background-color:{color};">{vr['status']}</span></td>
                <td>{vr.get('actual_value', '-')}</td>
                <td style="font-size:11px;">{cost_str}</td>
                <td>{vr.get('detail', '-')}</td></tr>"""
        html += "</tbody></table>"
        section_num += 1

    # ---- Section: Comparison Checks ----
    if comparison_results:
        html += f"<h2>{section_num}. Cross-Query Comparisons</h2>"
        html += """<table><thead><tr><th>#</th><th>Comparison</th><th>Rule</th>
            <th>Value A</th><th>Value B</th><th>Status</th><th>Detail</th>
            </tr></thead><tbody>"""
        for i, cr in enumerate(comparison_results, 1):
            color = _status_color(cr["status"])
            html += f"""<tr><td>{i}</td>
                <td><b>{cr['name']}</b><br><small>{cr.get('description', '')}</small></td>
                <td>{cr.get('rule', '-')}</td>
                <td><b>{cr.get('label_a', 'A')}:</b> {cr.get('value_a', '-')}</td>
                <td><b>{cr.get('label_b', 'B')}:</b> {cr.get('value_b', '-')}</td>
                <td><span class="badge" style="background-color:{color};">{cr['status']}</span></td>
                <td>{cr.get('detail', '-')}</td></tr>"""
        html += "</tbody></table>"
        section_num += 1

    # ---- Section: Summary Results + Charts ----
    if summary_results:
        html += f"<h2>{section_num}. Summary Dashboard</h2>"

        for sr in summary_results:
            html += f"""<div style="margin:20px 0; padding:15px; background:#fafbfc;
                         border-radius:8px; border:1px solid #eee;">
                <h3 style="color:#2c3e50;margin-top:0;">{sr['name']}</h3>
                <p style="color:#666;font-size:12px;">{sr.get('description', '')}</p>"""

            if sr["status"] == STATUS_FAIL:
                html += f'<p style="color:#dc3545;">Query failed: {sr["detail"]}</p>'
            else:
                # Data table
                if sr["data"]:
                    html += '<table style="margin-bottom:15px;"><thead><tr>'
                    for h in sr["headers"]:
                        html += f"<th>{h}</th>"
                    html += "</tr></thead><tbody>"
                    for row in sr["data"][:50]:  # limit to 50 rows in email
                        html += "<tr>"
                        for h in sr["headers"]:
                            html += f"<td>{row.get(h, '')}</td>"
                        html += "</tr>"
                    html += "</tbody></table>"

                # Chart
                if sr.get("chart") and sr["data"]:
                    chart_b64 = generate_chart_base64(sr["chart"], sr["headers"], sr["data"])
                    if chart_b64:
                        html += f"""<div class="chart-container">
                            <img src="data:image/png;base64,{chart_b64}"
                                 alt="{sr['chart'].get('title', 'Chart')}" />
                            </div>"""
                    else:
                        # Fallback to HTML bar chart
                        if sr["chart"].get("type") == "bar":
                            html += generate_html_bar_chart(
                                sr["chart"], sr["headers"], sr["data"])
                        else:
                            html += '<p style="color:#888;">(Chart requires matplotlib)</p>'

            html += "</div>"

    # ---- Footer ----
    html += f"""
    <div class="footer">
        <p><b>Audit Log:</b> {audit_s3_path or 'N/A'}</p>
        <p>Generated by Strands ETL Orchestrator v1.1 &nbsp;|&nbsp; BIG DATA Team</p>
    </div>
    </div></body></html>"""

    return html


def send_email(html_content, email_cfg, overall_status, run_mode):
    """Send the dashboard HTML email via SMTP."""
    sender = email_cfg["sender"]
    recipients = email_cfg["recipients"]
    mode_label = run_mode.upper().replace("_", " ")
    subject = (f"{email_cfg.get('subject_prefix', 'ETL Dashboard')} – "
               f"{mode_label} – {overall_status} – "
               f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}")

    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_content, "html"))

    smtp_host = email_cfg.get("smtp_host", "localhost")
    smtp_port = email_cfg.get("smtp_port", 25)

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info("Dashboard email sent to %s", ", ".join(recipients))
    except Exception as exc:
        logger.error("Failed to send email: %s", exc)


# ============================================================================
# 10. MAIN ORCHESTRATOR
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Strands ETL Orchestrator")
    parser.add_argument(
        "--config", required=True,
        help="Path to orchestrator_config.json (local or s3://...)",
    )
    parser.add_argument(
        "--summary-config", required=False, default=None,
        help="Optional: separate summary config JSON (local or s3://). "
             "If provided, summary queries and charts are generated from this config. "
             "If not provided and run_mode=summary, summary section from --config is used.",
    )
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:12]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_start_time = time.time()

    logger.info("=" * 70)
    logger.info("STRANDS ETL ORCHESTRATOR – Run ID: %s  Date: %s", run_id, run_date)
    logger.info("=" * 70)

    config = load_config(args.config)
    orch_cfg = config.get("orchestrator", {})
    mon_cfg = config.get("s3_monitoring", {})
    defaults = mon_cfg.get("defaults", {})
    sources = mon_cfg.get("sources", [])
    audit_cfg = config.get("audit", {})
    email_cfg = config.get("email", {})

    # Load separate summary config if provided
    summary_config = None
    if args.summary_config:
        logger.info("Loading separate summary config: %s", args.summary_config)
        summary_config = load_config(args.summary_config)

    run_mode = orch_cfg.get("run_mode", "full_pipeline")

    # If summary config is not defined anywhere, fall back to non-summary mode
    has_summary = bool(
        (summary_config and summary_config.get("summary", {}).get("queries"))
        or config.get("summary", {}).get("queries")
    )
    if run_mode == "summary" and not has_summary:
        has_validations = bool(
            config.get("validations", {}).get("queries")
            or config.get("validations", {}).get("comparison_checks")
        )
        if has_validations:
            logger.warning(
                "run_mode=summary but no summary queries defined. "
                "Falling back to validation_only mode."
            )
            run_mode = "validation_only"
        else:
            logger.warning(
                "run_mode=summary but no summary or validation queries defined. "
                "Falling back to full_pipeline mode."
            )
            run_mode = "full_pipeline"

    logger.info("Run mode: %s", run_mode)
    logger.info("Matplotlib available: %s", HAS_MATPLOTLIB)

    overall_status = STATUS_PASS
    stale_results = []
    stale_pairs = []
    stale_sources = []
    glue_results = {}
    validation_results = []
    comparison_results = []
    summary_results = []
    aborted = False

    # ================================================================
    # MODE: full_pipeline — stale check → glue → validations
    # ================================================================
    if run_mode == "full_pipeline":
        # Phase 1: Stale Detection
        logger.info("-" * 50)
        logger.info("PHASE 1: S3 Stale Detection")
        logger.info("-" * 50)
        for source in sources:
            stale_results.append(check_staleness(source, defaults))

        stale_pairs = [
            (s, r["stale_subfolders"])
            for s, r in zip(sources, stale_results)
            if r["status"] == STATUS_STALE
        ]
        stale_sources = [pair[0] for pair in stale_pairs]

        # Phase 2: Glue Jobs
        logger.info("-" * 50)
        logger.info("PHASE 2: Trigger Glue Jobs (%d stale sources)", len(stale_sources))
        logger.info("-" * 50)
        for source, stale_sfs in stale_pairs:
            label = source.get("label", source["prefix"])
            logger.info("[%s] Passing %d stale subfolder(s) to Glue: %s",
                        label, len(stale_sfs), stale_sfs)
            gr = trigger_glue_job(source, stale_sfs, run_id)
            glue_results[label] = gr
            if gr["status"] == STATUS_FAIL:
                overall_status = STATUS_FAIL

        # Phase 3: Validations + Comparisons
        logger.info("-" * 50)
        logger.info("PHASE 3: Post-Load Validations & Comparisons")
        logger.info("-" * 50)
        if stale_sources and overall_status != STATUS_FAIL:
            validation_results, comparison_results, aborted = run_validations(config, stale_results)
            if aborted:
                overall_status = STATUS_FAIL
        elif not stale_sources:
            logger.info("No stale sources – skipping validations")
        else:
            logger.warning("Skipping validations due to Glue failures")

    # ================================================================
    # MODE: validation_only — just validations + comparisons
    # ================================================================
    elif run_mode == "validation_only":
        logger.info("-" * 50)
        logger.info("VALIDATION-ONLY MODE")
        logger.info("-" * 50)
        validation_results, comparison_results, aborted = run_validations(config, stale_results)
        if aborted:
            overall_status = STATUS_FAIL

    # ================================================================
    # MODE: summary — parallel queries + charts
    # ================================================================
    elif run_mode == "summary":
        logger.info("-" * 50)
        logger.info("SUMMARY MODE: Parallel queries + chart generation")
        logger.info("-" * 50)
        # Use separate summary config if provided, else fall back to main config
        effective_summary_config = summary_config if summary_config else config
        summary_results = run_summary_queries(effective_summary_config)

        # Also run validations if configured
        val_cfg = config.get("validations", {})
        if val_cfg.get("queries") or val_cfg.get("comparison_checks"):
            logger.info("Also running validations configured alongside summary")
            validation_results, comparison_results, aborted = run_validations(config, stale_results)
            if aborted:
                overall_status = STATUS_FAIL

        # Check if any summary query failed
        for sr in summary_results:
            if sr["status"] == STATUS_FAIL:
                logger.warning("Summary query '%s' failed: %s", sr["name"], sr["detail"])

    else:
        logger.error("Unknown run_mode: %s", run_mode)
        sys.exit(1)

    # ================================================================
    # Audit Logging
    # ================================================================
    logger.info("-" * 50)
    logger.info("AUDIT LOGGING")
    logger.info("-" * 50)
    audit_records = build_audit_records(
        run_id, run_date, stale_results, glue_results,
        validation_results, comparison_results, summary_results, overall_status)
    audit_s3_path = None
    try:
        audit_s3_path = write_audit_to_s3(audit_records, audit_cfg, run_date)
        run_msck_repair(audit_cfg)
    except Exception as exc:
        logger.error("Audit write failed (non-fatal): %s", exc)

    # ================================================================
    # Dashboard Email
    # ================================================================
    logger.info("-" * 50)
    logger.info("DASHBOARD EMAIL")
    logger.info("-" * 50)
    # Calculate total Athena query cost
    total_cost_usd = sum(vr.get("cost_usd", 0) for vr in validation_results)
    total_cost_usd += sum(cr.get("cost_usd", 0) for cr in comparison_results)
    run_duration_sec = time.time() - run_start_time

    html = generate_dashboard_html(
        run_id, run_date, run_mode, stale_results, glue_results,
        validation_results, comparison_results, summary_results,
        overall_status, audit_s3_path,
        run_duration_sec=run_duration_sec,
        total_cost_usd=total_cost_usd)
    send_email(html, email_cfg, overall_status, run_mode)

    # ---- Final Summary ----
    logger.info("=" * 70)
    logger.info(
        "COMPLETE – Mode: %s | Overall: %s | Validations: %d pass/%d fail/%d not_exec | "
        "Comparisons: %d pass/%d fail | Summary queries: %d | "
        "Duration: %.0fs | Athena Cost: $%.4f",
        run_mode, overall_status,
        sum(1 for v in validation_results if v["status"] == STATUS_PASS),
        sum(1 for v in validation_results if v["status"] == STATUS_FAIL),
        sum(1 for v in validation_results if v["status"] == STATUS_NOT_EXECUTED),
        sum(1 for c in comparison_results if c["status"] == STATUS_PASS),
        sum(1 for c in comparison_results if c["status"] == STATUS_FAIL),
        len(summary_results), run_duration_sec, total_cost_usd,
    )
    logger.info("=" * 70)

    if overall_status != STATUS_PASS:
        sys.exit(1)


if __name__ == "__main__":
    main()
