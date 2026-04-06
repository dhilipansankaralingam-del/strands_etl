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
# 03/2026       Dhilipan        Summary mode: deviation alerts (20%), GIF support, enhanced HTML
# 03/2026       Dhilipan        Summary v2: Z-score analysis, trends charts, animated GIF output
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

# Optional: Pillow for animated GIF generation
try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

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
# 2b. END-OF-BATCH CHECKS: Glue Job Status + S3 Folder Freshness
# ============================================================================
GLUE_COST_PER_DPU_HOUR = 0.44


def _seconds_to_min_str(seconds):
    """Convert seconds to a human-readable minutes string."""
    if seconds is None:
        return "N/A"
    try:
        minutes = float(seconds) / 60.0
        return f"{minutes:.1f} min" if minutes >= 1 else f"{minutes:.2f} min"
    except (ValueError, TypeError):
        return "N/A"


def check_glue_jobs_today(job_configs):
    """
    Check each configured Glue job to see if it ran today.
    Similar to glue_job_status_report.py but lightweight for end-of-batch.

    job_configs: list of dicts with keys: name, domain, sla
    Returns: list of job status dicts
    """
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    results = []

    for jcfg in job_configs:
        job_name = jcfg["name"]
        domain = jcfg.get("domain", "")
        sla_deadline = jcfg.get("sla", "23:59")

        entry = {
            "job_name": job_name,
            "domain": domain,
            "sla_deadline": sla_deadline,
            "status": "NOT_RUN",
            "ran_today": False,
            "execution_time_min": "N/A",
            "last_run": "Never",
            "cost_today": 0.0,
            "sla_status": "N/A",
            "sla_detail": "",
            "error_message": "",
            "runs_today": 0,
            "worker_type": "N/A",
            "num_workers": "N/A",
        }

        try:
            response = glue_client().get_job_runs(
                JobName=job_name, MaxResults=20
            )
        except Exception as exc:
            entry["status"] = "ERROR"
            entry["error_message"] = str(exc)
            results.append(entry)
            continue

        if not response.get("JobRuns"):
            results.append(entry)
            continue

        # Check runs for today
        today_runs = []
        for run in response["JobRuns"]:
            started_on = run.get("StartedOn")
            if not started_on:
                continue
            if started_on.tzinfo is None:
                started_on = started_on.replace(tzinfo=timezone.utc)
            run_date = started_on.strftime("%Y-%m-%d")
            if run_date == today_str:
                today_runs.append(run)

        if not today_runs:
            # Job exists but didn't run today
            latest = response["JobRuns"][0]
            started_on = latest.get("StartedOn")
            if started_on:
                entry["last_run"] = started_on.strftime("%Y-%m-%d %H:%M UTC")
            results.append(entry)
            continue

        # Job ran today — use the latest today's run
        latest = today_runs[0]
        entry["ran_today"] = True
        entry["runs_today"] = len(today_runs)
        entry["status"] = latest.get("JobRunState", "UNKNOWN")
        entry["worker_type"] = latest.get("WorkerType", "Standard")
        entry["num_workers"] = latest.get("NumberOfWorkers",
                                          latest.get("AllocatedCapacity", "N/A"))

        exec_sec = latest.get("ExecutionTime") or 0
        entry["execution_time_min"] = _seconds_to_min_str(exec_sec)

        started_on = latest.get("StartedOn")
        if started_on:
            if started_on.tzinfo is None:
                started_on = started_on.replace(tzinfo=timezone.utc)
            entry["last_run"] = started_on.strftime("%Y-%m-%d %H:%M UTC")

        entry["error_message"] = latest.get("ErrorMessage", "")

        # Cost for today's runs
        total_cost = 0.0
        for run in today_runs:
            dpu_sec = run.get("DPUSeconds") or 0
            total_cost += (float(dpu_sec) / 3600.0) * GLUE_COST_PER_DPU_HOUR
        entry["cost_today"] = round(total_cost, 4)

        # SLA check
        completed_on = latest.get("CompletedOn")
        if completed_on:
            if completed_on.tzinfo is None:
                completed_on = completed_on.replace(tzinfo=timezone.utc)
            sla_hour, sla_min = [int(x) for x in sla_deadline.split(":")]
            sla_dt = datetime(
                started_on.year, started_on.month, started_on.day,
                sla_hour, sla_min, 0, tzinfo=timezone.utc
            )
            if completed_on <= sla_dt:
                entry["sla_status"] = "ON TIME"
                entry["sla_detail"] = (
                    f"Finished {completed_on.strftime('%H:%M')} UTC "
                    f"(deadline {sla_deadline} UTC)")
            else:
                entry["sla_status"] = "BREACHED"
                entry["sla_detail"] = (
                    f"Finished {completed_on.strftime('%H:%M')} UTC "
                    f"(deadline {sla_deadline} UTC)")
        elif entry["status"] == "RUNNING":
            entry["sla_status"] = "RUNNING"
            entry["sla_detail"] = "Job still running"

        results.append(entry)

    logger.info("Glue job check: %d jobs, %d ran today, %d not run",
                len(results),
                sum(1 for r in results if r["ran_today"]),
                sum(1 for r in results if not r["ran_today"]))
    return results


def check_s3_folders_summary(s3_folder_configs):
    """
    End-of-batch S3 folder check:
      - For each configured S3 path, count subfolders
      - Subfolder count must NOT exceed 3
      - Subfolders must have been created/modified in the last 3 hours

    s3_folder_configs: list of dicts with keys: bucket, prefix, label,
                       max_subfolders (default 3), max_age_hours (default 3)
    Returns: list of check result dicts
    """
    now = datetime.now(timezone.utc)
    results = []

    for folder_cfg in s3_folder_configs:
        bucket = folder_cfg["bucket"]
        prefix = folder_cfg["prefix"]
        label = folder_cfg.get("label", prefix)
        max_subfolders = folder_cfg.get("max_subfolders", 3)
        max_age_hours = folder_cfg.get("max_age_hours", 3)

        entry = {
            "bucket": bucket,
            "prefix": prefix,
            "label": label,
            "subfolder_count": 0,
            "max_subfolders": max_subfolders,
            "max_age_hours": max_age_hours,
            "status": STATUS_PASS,
            "detail": "",
            "subfolders": [],
            "stale_subfolders": [],
            "over_limit": False,
        }

        try:
            subfolders = list_subfolders(bucket, prefix)
            entry["subfolder_count"] = len(subfolders)

            if len(subfolders) > max_subfolders:
                entry["status"] = STATUS_FAIL
                entry["over_limit"] = True
                entry["detail"] = (
                    f"Subfolder count {len(subfolders)} exceeds limit "
                    f"of {max_subfolders}")

            # Check age of each subfolder
            stale_list = []
            for sf in subfolders:
                sf_name = sf["prefix"].rstrip("/").split("/")[-1]
                sf_entry = {
                    "name": sf_name,
                    "prefix": sf["prefix"],
                    "last_modified": None,
                    "age_hours": None,
                    "fresh": False,
                }
                if sf["latest_modified"]:
                    age_hours = (now - sf["latest_modified"]).total_seconds() / 3600
                    sf_entry["last_modified"] = sf["latest_modified"].strftime(
                        "%Y-%m-%d %H:%M UTC")
                    sf_entry["age_hours"] = round(age_hours, 1)
                    sf_entry["fresh"] = age_hours <= max_age_hours
                    if not sf_entry["fresh"]:
                        stale_list.append(sf_name)
                else:
                    stale_list.append(sf_name)

                entry["subfolders"].append(sf_entry)

            entry["stale_subfolders"] = stale_list
            if stale_list and entry["status"] != STATUS_FAIL:
                entry["status"] = STATUS_WARN
                entry["detail"] = (
                    f"{len(stale_list)} subfolder(s) older than "
                    f"{max_age_hours}h: {', '.join(stale_list)}")

            if not entry["detail"]:
                entry["detail"] = (
                    f"{len(subfolders)} subfolder(s), all within {max_age_hours}h")

        except Exception as exc:
            entry["status"] = STATUS_FAIL
            entry["detail"] = f"Error checking S3: {exc}"
            logger.error("[%s] S3 folder check error: %s", label, exc)

        results.append(entry)

    logger.info("S3 folder check: %d paths, %d PASS, %d FAIL/WARN",
                len(results),
                sum(1 for r in results if r["status"] == STATUS_PASS),
                sum(1 for r in results if r["status"] != STATUS_PASS))
    return results


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
    check_type = qdef.get("check_type", "")

    logger.info("Running validation: %s", qdef["name"])

    try:
        athena_result = run_athena_query(query_str, database, output_location, workgroup)
    except Exception as exc:
        logger.error("Athena query failed for %s: %s", qdef["name"], exc)
        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "check_type": check_type,
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
        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "check_type": check_type,
            "abort_on_failure": qdef.get("abort_on_failure", False),
            "status": status, "detail": detail,
            "actual_value": actual, "query": query_str,
            "cost_usd": cost_usd, "retried": retried,
            "failure_reason": failure_reason,
        }

    # ----- rolling_average check type -----
    # Expects query to return one row with columns:
    #   today_cnt, avg_Nday_cnt (or baseline_cnt), variance_pct,
    #   and optionally: window_start_utc, window_end_utc, status
    # Pass/fail is determined by comparing abs(variance_pct) against tolerance
    if check_type == "rolling_average":
        header, data = parse_athena_rows(athena_result["rows"])
        if not data:
            logger.warning("Rolling average query %s returned no rows", qdef["name"])
            return {
                "name": qdef["name"], "description": qdef.get("description", ""),
                "check_type": check_type,
                "abort_on_failure": qdef.get("abort_on_failure", False),
                "status": STATUS_WARN,
                "detail": "Rolling average query returned no rows",
                "actual_value": None, "query": query_str,
                "cost_usd": cost_usd, "retried": retried,
                "failure_reason": "No data returned",
            }

        row = data[0]  # first row has the result
        tolerance = qdef.get("tolerance", 25)

        # Map columns flexibly — support various naming conventions
        today_cnt = _to_float(row, "today_cnt")
        # Detect baseline column: avg_14day_cnt, avg_10day_cnt, baseline_cnt, etc.
        baseline_cnt = None
        baseline_col = None
        for col in header:
            if col.startswith("avg_") and col.endswith("_cnt"):
                baseline_cnt = _to_float(row, col)
                baseline_col = col
                break
            if col in ("baseline_cnt", "avg_cnt"):
                baseline_cnt = _to_float(row, col)
                baseline_col = col
                break
        if baseline_cnt is None:
            baseline_col = "baseline_cnt"
            baseline_cnt = _to_float(row, "baseline_cnt")

        variance_pct = _to_float(row, "variance_pct")
        window_start = row.get("window_start_utc", "")
        window_end = row.get("window_end_utc", "")
        query_status = row.get("status", row.get("Status", ""))

        # Calculate variance_pct if not returned by query
        if variance_pct is None and today_cnt is not None and baseline_cnt is not None:
            if baseline_cnt != 0:
                variance_pct = round(
                    abs(today_cnt - baseline_cnt) / baseline_cnt * 100, 2
                )
            else:
                variance_pct = 100.0 if today_cnt > 0 else 0.0

        # Determine pass/fail
        if variance_pct is not None:
            passed = abs(variance_pct) <= tolerance
        elif condition:
            # Fallback to generic condition evaluation
            passed, _ = evaluate_condition(condition, athena_result["rows"])
        else:
            passed = query_status.upper() in ("PASS", "OK", "") if query_status else True

        status = STATUS_PASS if passed else STATUS_FAIL
        if not passed and not qdef.get("abort_on_failure", False):
            # Non-critical rolling average breach → WARN instead of FAIL
            status = STATUS_WARN if qdef.get("warn_only", False) else STATUS_FAIL

        detail = (
            f"today={today_cnt}, {baseline_col}={baseline_cnt}, "
            f"variance={variance_pct}%, tolerance={tolerance}%, "
            f"window=[{window_start} → {window_end}]"
        )
        if retried:
            detail += " [succeeded on retry]"

        logger.info("Validation %s: %s – %s", qdef["name"], status, detail)

        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "check_type": check_type,
            "abort_on_failure": qdef.get("abort_on_failure", False),
            "status": status, "detail": detail,
            "actual_value": variance_pct,
            "query": query_str,
            "cost_usd": cost_usd, "retried": retried,
            "failure_reason": "",
            # Rolling average specific fields
            "today_count": today_cnt,
            "baseline_count": baseline_cnt,
            "variance_pct": variance_pct,
            "tolerance": tolerance,
            "window_start": window_start,
            "window_end": window_end,
        }

    # ----- source_reconciliation check type -----
    # Expects query to return rows from a reconciliation table with columns like:
    #   count_source, count_aws_datalake, (optionally: extract_date, table_name)
    # Checks if ABS(count_source - count_aws_datalake) >= deviation_threshold
    # Any deviating rows → FAIL.  No deviating rows → PASS.
    if check_type == "source_reconciliation":
        header, data = parse_athena_rows(athena_result["rows"])
        if not data:
            logger.info("Reconciliation %s: no data rows returned → PASS", qdef["name"])
            return {
                "name": qdef["name"], "description": qdef.get("description", ""),
                "check_type": check_type,
                "abort_on_failure": qdef.get("abort_on_failure", False),
                "status": STATUS_PASS,
                "detail": "No deviating rows found – reconciliation passed",
                "actual_value": 0, "query": query_str,
                "cost_usd": cost_usd, "retried": retried,
                "failure_reason": "",
                "recon_deviations": [],
            }

        deviation_threshold = qdef.get("deviation_threshold", 0)
        source_col = qdef.get("source_column", "count_source")
        target_col = qdef.get("target_column", "count_aws_datalake")

        deviations = []
        for row in data:
            src_val = _to_float(row, source_col)
            tgt_val = _to_float(row, target_col)
            if src_val is None or tgt_val is None:
                continue
            diff = abs(src_val - tgt_val)
            if diff >= deviation_threshold:
                # Collect identifying columns for the deviation
                table_name = (row.get("table_name") or row.get("source_table")
                              or row.get("entity") or "")
                extract_date = (row.get("extract_date") or row.get("run_date")
                                or row.get("load_date") or "")
                deviations.append({
                    "table_name": table_name,
                    "extract_date": extract_date,
                    "count_source": src_val,
                    "count_target": tgt_val,
                    "deviation": diff,
                })

        if deviations:
            status = STATUS_FAIL
            dev_summary = "; ".join(
                f"{d.get('table_name', 'row')}:src={d['count_source']:.0f},"
                f"tgt={d['count_target']:.0f},dev={d['deviation']:.0f}"
                for d in deviations[:5]  # limit to 5 in summary
            )
            if len(deviations) > 5:
                dev_summary += f" ...and {len(deviations) - 5} more"
            detail = (f"FAILED: {len(deviations)} row(s) with deviation >= "
                      f"{deviation_threshold}. [{dev_summary}]")
        else:
            status = STATUS_PASS
            detail = (f"All {len(data)} row(s) within tolerance "
                      f"(deviation < {deviation_threshold})")

        if retried:
            detail += " [succeeded on retry]"

        logger.info("Reconciliation %s: %s – %s", qdef["name"], status, detail)

        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "check_type": check_type,
            "abort_on_failure": qdef.get("abort_on_failure", False),
            "status": status, "detail": detail,
            "actual_value": len(deviations),
            "query": query_str,
            "cost_usd": cost_usd, "retried": retried,
            "failure_reason": detail if deviations else "",
            "recon_deviations": deviations,
        }

    # ----- Standard check types (row_count, no_rows, threshold, etc.) -----
    passed, actual = evaluate_condition(condition, athena_result["rows"])
    status = STATUS_PASS if passed else STATUS_FAIL
    detail = f"Condition '{condition}' -> {'met' if passed else 'not met'} (actual={actual})"
    if retried:
        detail += " [succeeded on retry]"
    failure_reason = ""

    logger.info("Validation %s: %s – %s", qdef["name"], status, detail)

    return {
        "name": qdef["name"], "description": qdef.get("description", ""),
        "check_type": check_type,
        "abort_on_failure": qdef.get("abort_on_failure", False),
        "status": status, "detail": detail,
        "actual_value": actual, "query": query_str,
        "cost_usd": cost_usd, "retried": retried,
        "failure_reason": failure_reason,
    }


def _to_float(row, col):
    """Safely extract a float value from a result row dict."""
    val = row.get(col)
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    """Safely convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _execute_data_diff(cdef, result_a, result_b, cost_usd):
    """
    Row-level data diff between two query results for delta-flow comparisons.

    Compares actual rows using one or more key columns and optionally checks
    non-key columns for value mismatches on matched keys.

    Config fields:
      key_columns        – str or list[str]: column(s) forming the composite key
      compare_columns    – list[str] (optional): non-key columns to diff on
                           matched rows. If omitted, only key presence is checked.
      case_sensitive     – bool (default True): key matching case sensitivity
      max_diff_rows      – int (default 100): cap on rows reported per category
      ignore_duplicates  – bool (default False): if True, de-dups keys before diff
    """
    name = cdef["name"]
    abort_on_failure = cdef.get("abort_on_failure", False)
    qa = cdef["query_a"]
    qb = cdef["query_b"]

    key_cols = cdef.get("key_columns", cdef.get("compare_column", ""))
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    compare_cols = cdef.get("compare_columns", [])
    case_sensitive = cdef.get("case_sensitive", True)
    max_diff_rows = cdef.get("max_diff_rows", 100)
    ignore_duplicates = cdef.get("ignore_duplicates", False)

    header_a, data_a = parse_athena_rows(result_a["rows"])
    header_b, data_b = parse_athena_rows(result_b["rows"])

    # Validate key columns exist in both results
    for kc in key_cols:
        if kc not in header_a:
            return {
                "name": name, "description": cdef.get("description", ""),
                "check_type": "comparison", "rule": "data_diff",
                "abort_on_failure": abort_on_failure,
                "status": STATUS_FAIL,
                "detail": f"Key column '{kc}' not found in Query A results (available: {header_a})",
                "value_a": len(data_a), "value_b": len(data_b),
                "label_a": qa.get("label", "Query A"),
                "label_b": qb.get("label", "Query B"),
                "cost_usd": cost_usd,
            }
        if kc not in header_b:
            return {
                "name": name, "description": cdef.get("description", ""),
                "check_type": "comparison", "rule": "data_diff",
                "abort_on_failure": abort_on_failure,
                "status": STATUS_FAIL,
                "detail": f"Key column '{kc}' not found in Query B results (available: {header_b})",
                "value_a": len(data_a), "value_b": len(data_b),
                "label_a": qa.get("label", "Query A"),
                "label_b": qb.get("label", "Query B"),
                "cost_usd": cost_usd,
            }

    def make_key(row):
        """Build composite key tuple from row dict."""
        parts = []
        for kc in key_cols:
            val = row.get(kc, "")
            if val is None:
                val = "<NULL>"
            elif not case_sensitive:
                val = str(val).lower()
            else:
                val = str(val)
            parts.append(val)
        return tuple(parts)

    # Build keyed dictionaries
    keys_a = {}   # key -> list of row dicts
    dup_a = 0
    for row in data_a:
        k = make_key(row)
        if k in keys_a:
            dup_a += 1
            if not ignore_duplicates:
                keys_a[k].append(row)
        else:
            keys_a[k] = [row]

    keys_b = {}
    dup_b = 0
    for row in data_b:
        k = make_key(row)
        if k in keys_b:
            dup_b += 1
            if not ignore_duplicates:
                keys_b[k].append(row)
        else:
            keys_b[k] = [row]

    set_a = set(keys_a.keys())
    set_b = set(keys_b.keys())

    only_in_a = sorted(set_a - set_b)
    only_in_b = sorted(set_b - set_a)
    common_keys = set_a & set_b

    # Check value mismatches on matched keys
    mismatched = []
    if compare_cols:
        for k in sorted(common_keys):
            row_a = keys_a[k][0]  # take first occurrence
            row_b = keys_b[k][0]
            diffs = {}
            for col in compare_cols:
                va = str(row_a.get(col, "")) if row_a.get(col) is not None else "<NULL>"
                vb = str(row_b.get(col, "")) if row_b.get(col) is not None else "<NULL>"
                if not case_sensitive:
                    va, vb = va.lower(), vb.lower()
                if va != vb:
                    diffs[col] = {"a": row_a.get(col, ""), "b": row_b.get(col, "")}
            if diffs:
                key_display = dict(zip(key_cols, k))
                mismatched.append({"key": key_display, "diffs": diffs})

    # Build result
    has_diff = len(only_in_a) > 0 or len(only_in_b) > 0 or len(mismatched) > 0
    passed = not has_diff

    # Summary detail string
    parts = []
    parts.append(f"A={len(data_a)} rows, B={len(data_b)} rows")
    parts.append(f"only_in_A={len(only_in_a)}, only_in_B={len(only_in_b)}")
    if compare_cols:
        parts.append(f"value_mismatches={len(mismatched)}")
    if dup_a or dup_b:
        parts.append(f"duplicates(A={dup_a}, B={dup_b})")
    detail = "; ".join(parts)
    detail += f" → {'no differences' if passed else 'DIFFERENCES FOUND'}"

    # Build diff detail for HTML rendering
    diff_detail = {
        "only_in_a": [dict(zip(key_cols, k)) for k in only_in_a[:max_diff_rows]],
        "only_in_b": [dict(zip(key_cols, k)) for k in only_in_b[:max_diff_rows]],
        "mismatched": mismatched[:max_diff_rows],
        "only_in_a_total": len(only_in_a),
        "only_in_b_total": len(only_in_b),
        "mismatched_total": len(mismatched),
        "key_columns": key_cols,
        "compare_columns": compare_cols,
        "duplicates_a": dup_a,
        "duplicates_b": dup_b,
    }

    status = STATUS_PASS if passed else STATUS_FAIL
    logger.info("Comparison %s (data_diff): %s – %s", name, status, detail)

    return {
        "name": name, "description": cdef.get("description", ""),
        "check_type": "comparison", "rule": "data_diff",
        "abort_on_failure": abort_on_failure,
        "status": status, "detail": detail,
        "value_a": len(data_a), "value_b": len(data_b),
        "label_a": qa.get("label", "Query A"),
        "label_b": qb.get("label", "Query B"),
        "cost_usd": cost_usd,
        "diff_detail": diff_detail,
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
      data_diff           – row-level diff using key columns (delta flow)
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
    cost_usd = 0.0
    try:
        result_a = run_athena_query(qa["query"], qa.get("database", "default"),
                                    output_location, workgroup)
        result_b = run_athena_query(qb["query"], qb.get("database", "default"),
                                    output_location, workgroup)
        cost_usd = result_a.get("cost_usd", 0) + result_b.get("cost_usd", 0)
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
            "cost_usd": cost_usd,
        }

    cost_usd = result_a.get("cost_usd", 0) + result_b.get("cost_usd", 0)

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
            "cost_usd": cost_usd,
        }

    # For data_diff rule, delegate to specialised handler
    if rule == "data_diff":
        return _execute_data_diff(cdef, result_a, result_b, cost_usd)

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
        "cost_usd": cost_usd,
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


def run_zscore_analysis(config, output_location, workgroup="primary"):
    """
    Execute z-score queries defined in config["summary"]["zscore_queries"].
    Each query should return rows with columns for the past 7 days:
      date, metric_name, today_count, avg_30d, stddev_30d
    Z-score is computed in Python: (today_count - avg_30d) / stddev_30d

    Returns list of dicts:
      {name, description, headers, data, status, detail}
    where each data row gets an extra 'z_score' and 'z_flag' field.
    """
    sum_cfg = config.get("summary", {})
    zscore_queries = sum_cfg.get("zscore_queries", [])
    max_parallel = sum_cfg.get("max_parallel", 5)
    output_bucket = sum_cfg.get("athena_output_bucket", "")
    output_prefix = sum_cfg.get("athena_output_prefix", "summary_results/")
    if not output_location:
        output_location = f"s3://{output_bucket}/{output_prefix}"

    if not zscore_queries:
        return []

    logger.info("Running %d z-score queries", len(zscore_queries))

    def _run_one(qdef):
        count_col = qdef.get("count_column", "today_count")
        avg_col = qdef.get("avg_column", "avg_30d")
        stddev_col = qdef.get("stddev_column", "stddev_30d")
        try:
            result = run_athena_query(
                qdef["query"], qdef.get("database", "default"),
                output_location, workgroup)
        except Exception as exc:
            return {
                "name": qdef["name"], "description": qdef.get("description", ""),
                "headers": [], "data": [],
                "status": STATUS_FAIL, "detail": str(exc),
            }
        if result["state"] != "SUCCEEDED":
            return {
                "name": qdef["name"], "description": qdef.get("description", ""),
                "headers": [], "data": [],
                "status": STATUS_FAIL, "detail": f"State: {result['state']}",
            }
        headers, data = parse_athena_rows(result["rows"])

        # Compute z-score for each row in Python
        for row in data:
            try:
                val = float(row.get(count_col, 0))
                avg = float(row.get(avg_col, 0))
                std = float(row.get(stddev_col, 0))
                z = round((val - avg) / std, 2) if std > 0 else 0.0
            except (ValueError, TypeError):
                z = 0.0
            row["z_score"] = str(z)
            abs_z = abs(z)
            if abs_z >= 3:
                row["z_flag"] = "CRITICAL"
            elif abs_z >= 2:
                row["z_flag"] = "WARNING"
            else:
                row["z_flag"] = "NORMAL"

        if "z_score" not in headers:
            headers.append("z_score")
        if "z_flag" not in headers:
            headers.append("z_flag")

        return {
            "name": qdef["name"], "description": qdef.get("description", ""),
            "headers": headers, "data": data,
            "status": STATUS_PASS, "detail": f"{len(data)} row(s) returned",
        }

    results = []
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(_run_one, qdef): qdef for qdef in zscore_queries}
        for future in as_completed(futures):
            results.append(future.result())

    # Re-order to match config order
    order = {q["name"]: i for i, q in enumerate(zscore_queries)}
    results.sort(key=lambda r: order.get(r["name"], 999))
    return results


def fetch_audit_summary_for_today(audit_cfg, run_date, output_location, workgroup="primary"):
    """
    Query the audit table for ALL runs today (not just current run) and return
    an overall summary: pass/fail/warn/not_exec counts by event_type (layer),
    total cost, distinct run_ids, etc.

    Returns dict:
      {
        "by_layer": [
          {"event_type": "VALIDATION", "total": 10, "pass": 8, "fail": 1,
           "warn": 0, "not_exec": 1, "cost_usd": 0.0042},
          ...
        ],
        "totals": {"total": N, "pass": N, "fail": N, "warn": N,
                   "not_exec": N, "cost_usd": F, "distinct_runs": N},
        "runs": [
          {"run_id": "abc", "overall_status": "PASS", "event_count": N, "cost_usd": F},
          ...
        ],
        "status": "PASS" | "FAIL"
      }
    Returns None if the query fails or audit config is missing.
    """
    database = audit_cfg.get("database", "audit_db")
    table = audit_cfg.get("table", "etl_orchestrator_audit")

    # Query 1: Summary by event_type (layer)
    sql_by_layer = f"""
        SELECT
            event_type,
            COUNT(*) AS total_checks,
            SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS pass_count,
            SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fail_count,
            SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warn_count,
            SUM(CASE WHEN status = 'NOT_EXECUTED' THEN 1 ELSE 0 END) AS not_exec_count,
            ROUND(SUM(CAST(COALESCE(NULLIF(cost_usd, ''), '0') AS DOUBLE)), 4) AS total_cost
        FROM {database}.{table}
        WHERE run_date = '{run_date}'
        GROUP BY event_type
        ORDER BY event_type
    """

    # Query 2: Summary by run_id
    sql_by_run = f"""
        SELECT
            run_id,
            MAX(overall_status) AS overall_status,
            COUNT(*) AS event_count,
            ROUND(SUM(CAST(COALESCE(NULLIF(cost_usd, ''), '0') AS DOUBLE)), 4) AS total_cost,
            MIN(event_timestamp) AS first_event,
            MAX(event_timestamp) AS last_event
        FROM {database}.{table}
        WHERE run_date = '{run_date}'
        GROUP BY run_id
        ORDER BY MIN(event_timestamp) DESC
    """

    # Query 3: Runs grouped by run_id + event_type (layer)
    sql_runs_by_layer = f"""
        SELECT
            run_id,
            event_type,
            MAX(overall_status) AS overall_status,
            COUNT(*) AS event_count,
            SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS pass_count,
            SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fail_count,
            ROUND(SUM(CAST(COALESCE(NULLIF(cost_usd, ''), '0') AS DOUBLE)), 4) AS total_cost,
            MIN(event_timestamp) AS first_event,
            MAX(event_timestamp) AS last_event
        FROM {database}.{table}
        WHERE run_date = '{run_date}'
        GROUP BY run_id, event_type
        ORDER BY run_id, event_type
    """

    try:
        result_layer = run_athena_query(sql_by_layer, database, output_location, workgroup)
        result_run = run_athena_query(sql_by_run, database, output_location, workgroup)
        result_runs_by_layer = run_athena_query(
            sql_runs_by_layer, database, output_location, workgroup)
    except Exception as exc:
        logger.warning("Audit summary query failed (non-fatal): %s", exc)
        return None

    if result_layer["state"] != "SUCCEEDED" or result_run["state"] != "SUCCEEDED":
        logger.warning("Audit summary query did not succeed")
        return None

    # Parse layer results
    layer_headers, layer_data = parse_athena_rows(result_layer["rows"])
    by_layer = []
    totals = {"total": 0, "pass": 0, "fail": 0, "warn": 0, "not_exec": 0,
              "cost_usd": 0.0, "distinct_runs": 0}
    for row in layer_data:
        entry = {
            "event_type": row.get("event_type", ""),
            "total": int(float(row.get("total_checks", 0))),
            "pass": int(float(row.get("pass_count", 0))),
            "fail": int(float(row.get("fail_count", 0))),
            "warn": int(float(row.get("warn_count", 0))),
            "not_exec": int(float(row.get("not_exec_count", 0))),
            "cost_usd": float(row.get("total_cost", 0)),
        }
        by_layer.append(entry)
        totals["total"] += entry["total"]
        totals["pass"] += entry["pass"]
        totals["fail"] += entry["fail"]
        totals["warn"] += entry["warn"]
        totals["not_exec"] += entry["not_exec"]
        totals["cost_usd"] += entry["cost_usd"]

    # Parse run results
    run_headers, run_data = parse_athena_rows(result_run["rows"])
    runs = []
    for row in run_data:
        runs.append({
            "run_id": row.get("run_id", ""),
            "overall_status": row.get("overall_status", ""),
            "event_count": int(float(row.get("event_count", 0))),
            "cost_usd": float(row.get("total_cost", 0)),
            "first_event": row.get("first_event", ""),
            "last_event": row.get("last_event", ""),
        })
    totals["distinct_runs"] = len(runs)

    # Parse runs-by-layer results
    runs_by_layer = []
    if result_runs_by_layer["state"] == "SUCCEEDED":
        rbl_headers, rbl_data = parse_athena_rows(result_runs_by_layer["rows"])
        for row in rbl_data:
            runs_by_layer.append({
                "run_id": row.get("run_id", ""),
                "event_type": row.get("event_type", ""),
                "overall_status": row.get("overall_status", ""),
                "event_count": int(float(row.get("event_count", 0))),
                "pass_count": int(float(row.get("pass_count", 0))),
                "fail_count": int(float(row.get("fail_count", 0))),
                "cost_usd": float(row.get("total_cost", 0)),
                "first_event": row.get("first_event", ""),
                "last_event": row.get("last_event", ""),
            })

    overall = "PASS" if totals["fail"] == 0 else "FAIL"
    return {
        "by_layer": by_layer,
        "totals": totals,
        "runs": runs,
        "runs_by_layer": runs_by_layer,
        "status": overall,
    }


# ============================================================================
# 7. CHART GENERATION (matplotlib → animated GIF via Pillow)
# ============================================================================
def _render_chart_frame(chart_cfg, headers, data, fraction=1.0):
    """Render a single matplotlib chart frame with data scaled by fraction (0..1).
    Returns a matplotlib Figure object."""
    chart_type = chart_cfg.get("type", "bar")
    title = chart_cfg.get("title", "Chart")

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(CHART_BG)
    ax.set_facecolor(CHART_BG)

    colors = VIBRANT_COLORS[:len(data)]
    while len(colors) < len(data):
        colors += VIBRANT_COLORS

    if chart_type == "bar":
        x_col = chart_cfg.get("x_column", headers[0] if headers else "")
        y_col = chart_cfg.get("y_column", headers[1] if len(headers) > 1 else "")
        labels = [str(row.get(x_col, "")) for row in data]
        full_values = []
        for row in data:
            try:
                full_values.append(float(row.get(y_col, 0)))
            except (ValueError, TypeError):
                full_values.append(0)
        values = [v * fraction for v in full_values]
        max_val = max(full_values) if full_values else 1

        bars = ax.barh(labels, values, color=colors[:len(labels)],
                       edgecolor="white", linewidth=0.8)
        ax.set_xlabel(y_col, fontsize=11, fontweight="bold", color="#2c3e50")
        ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=15)
        ax.set_xlim(0, max_val * 1.15)
        ax.invert_yaxis()
        ax.grid(axis="x", color=CHART_GRID, linestyle="--", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        if fraction >= 1.0:
            for bar, val in zip(bars, values):
                ax.text(bar.get_width() + max_val * 0.01,
                        bar.get_y() + bar.get_height() / 2,
                        f"{val:,.0f}", va="center", fontsize=10,
                        fontweight="bold", color="#2c3e50")

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

        # Guard: pie chart cannot render if all values are zero or empty
        if not values or sum(values) == 0:
            ax.text(0.5, 0.5, "No data to display", ha="center", va="center",
                    fontsize=14, color="#888", transform=ax.transAxes)
            ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=20)
            plt.tight_layout()
            return fig

        # For pie, animate by showing only first N slices
        n_show = max(1, int(len(values) * fraction))
        show_vals = values[:n_show] + [sum(values[n_show:])] if n_show < len(values) else values
        show_labels = labels[:n_show] + (["..."] if n_show < len(values) else [])
        show_colors = colors[:len(show_labels)]

        # Filter out zero-value slices to avoid autopct issues
        filtered = [(v, l, c) for v, l, c in zip(show_vals, show_labels, show_colors) if v > 0]
        if not filtered:
            ax.text(0.5, 0.5, "No data to display", ha="center", va="center",
                    fontsize=14, color="#888", transform=ax.transAxes)
            ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=20)
            plt.tight_layout()
            return fig
        show_vals, show_labels, show_colors = zip(*filtered)

        wedges, texts, autotexts = ax.pie(
            show_vals, labels=show_labels, colors=show_colors,
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

        # Animate by drawing progressively more points
        n_points = max(1, int(len(x_vals) * fraction))
        x_draw = list(range(n_points))
        y_draw = y_vals[:n_points]

        ax.fill_between(x_draw, y_draw, alpha=0.15, color="#45B7D1")
        ax.plot(x_draw, y_draw, color="#45B7D1", linewidth=3,
                marker="o", markersize=8, markerfacecolor="#FF6B6B",
                markeredgecolor="white", markeredgewidth=2)

        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=9)
        ax.set_ylabel(y_col, fontsize=11, fontweight="bold", color="#2c3e50")
        ax.set_title(title, fontsize=14, fontweight="bold", color="#2c3e50", pad=15)
        if y_vals:
            ax.set_ylim(0, max(y_vals) * 1.2)
        ax.grid(axis="y", color=CHART_GRID, linestyle="--", linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        if fraction >= 1.0:
            for i, val in enumerate(y_vals):
                ax.annotate(f"{val:,.0f}", (i, val), textcoords="offset points",
                            xytext=(0, 12), ha="center", fontsize=9,
                            fontweight="bold", color="#2c3e50")
    else:
        plt.close(fig)
        return None

    plt.tight_layout()
    return fig


def _fig_to_pil(fig, dpi=100):
    """Convert a matplotlib figure to a PIL Image (RGBA → RGB with white bg)."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    img = PILImage.open(buf).convert("RGBA")
    background = PILImage.new("RGBA", img.size, (255, 255, 255, 255))
    background.paste(img, mask=img)
    return background.convert("RGB")


def generate_chart_gif_base64(chart_cfg, headers, data, n_frames=10):
    """
    Generate an animated GIF of the chart (bars growing, trend drawing, pie expanding).
    Returns (base64_string, mime_type) or (None, None).
    Falls back to static PNG if Pillow is unavailable.
    """
    if not HAS_MATPLOTLIB or not data:
        return None, None

    # If Pillow not available, fall back to static PNG
    if not HAS_PIL:
        return generate_chart_base64_static(chart_cfg, headers, data), "image/png"

    frames = []
    for i in range(n_frames + 1):
        frac = i / n_frames
        fig = _render_chart_frame(chart_cfg, headers, data, fraction=frac)
        if fig is None:
            return None, None
        frames.append(_fig_to_pil(fig, dpi=100))

    # Hold the final frame longer
    durations = [80] * n_frames + [2000]

    gif_buf = io.BytesIO()
    frames[0].save(gif_buf, format="GIF", save_all=True,
                   append_images=frames[1:], duration=durations, loop=0)
    gif_buf.seek(0)
    return base64.b64encode(gif_buf.read()).decode("utf-8"), "image/gif"


def generate_chart_base64_static(chart_cfg, headers, data):
    """Generate a static chart as base64-encoded PNG (fallback when PIL unavailable)."""
    fig = _render_chart_frame(chart_cfg, headers, data, fraction=1.0)
    if fig is None:
        return None
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_chart_base64(chart_cfg, headers, data):
    """Generate chart — animated GIF if Pillow available, else static PNG.
    Returns base64 string or None. MIME type is stored in chart_cfg['_mime']."""
    b64, mime = generate_chart_gif_base64(chart_cfg, headers, data)
    if b64:
        chart_cfg["_mime"] = mime
        return b64
    # Final fallback
    result = generate_chart_base64_static(chart_cfg, headers, data)
    if result:
        chart_cfg["_mime"] = "image/png"
    return result


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
                        summary_results, overall_status, layer=None):
    """Flatten all events into a list of audit record dicts.
    If *layer* is provided (e.g. STAGING, BASE, MASTER), it is prepended to the
    check type as event_type (e.g. BASE_VALIDATION, MASTER_COMPARISON) so that
    summary reports can group runs by layer."""
    records = []
    ts = datetime.now(timezone.utc).isoformat()

    def _evt(check_type):
        """Return event_type: 'LAYER_CHECKTYPE' if layer set, else 'CHECKTYPE'."""
        return f"{layer}_{check_type}" if layer else check_type

    for sr in stale_results:
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", _evt("STALE_CHECK")), ("source_label", sr["label"]),
            ("source_bucket", sr["bucket"]), ("source_prefix", sr["prefix"]),
            ("subfolder_count", str(sr["subfolder_count"])),
            ("status", sr["status"]), ("detail", sr["detail"]),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", ""), ("check_type", ""),
            ("actual_value", ""), ("query", ""), ("overall_status", overall_status),
            ("cost_usd", ""), ("failure_reason", ""),
            ("today_count", ""), ("baseline_count", ""),
            ("variance_pct", ""), ("tolerance", ""),
            ("window_start", ""), ("window_end", ""),
        ]))

    for label, gr in glue_results.items():
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", _evt("GLUE_TRIGGER")), ("source_label", label),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", gr["status"]), ("detail", gr.get("detail", "")),
            ("glue_job_name", gr.get("job_name", "")),
            ("glue_job_run_id", gr.get("job_run_id", "")),
            ("glue_duration_sec", str(gr.get("duration_seconds", ""))),
            ("validation_name", ""), ("check_type", ""),
            ("actual_value", ""), ("query", ""), ("overall_status", overall_status),
            ("cost_usd", ""), ("failure_reason", ""),
            ("today_count", ""), ("baseline_count", ""),
            ("variance_pct", ""), ("tolerance", ""),
            ("window_start", ""), ("window_end", ""),
        ]))

    for vr in validation_results:
        retry_info = " [retried]" if vr.get("retried") else ""
        fail_reason = vr.get("failure_reason", "")
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", _evt("VALIDATION")), ("source_label", ""),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", vr["status"]),
            ("detail", vr["detail"] + retry_info),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", vr["name"]), ("check_type", vr.get("check_type", "")),
            ("actual_value", str(vr.get("actual_value", ""))),
            ("query", vr.get("query", "")), ("overall_status", overall_status),
            ("cost_usd", str(vr.get("cost_usd", 0))),
            ("failure_reason", fail_reason),
            ("today_count", str(vr.get("today_count", ""))),
            ("baseline_count", str(vr.get("baseline_count", ""))),
            ("variance_pct", str(vr.get("variance_pct", ""))),
            ("tolerance", str(vr.get("tolerance", ""))),
            ("window_start", vr.get("window_start", "")),
            ("window_end", vr.get("window_end", "")),
        ]))

    for cr in comparison_results:
        actual_value = f"A={cr.get('value_a')},B={cr.get('value_b')}"
        diff_detail = cr.get("diff_detail")
        if diff_detail:
            actual_value = (
                f"rows_A={cr.get('value_a')},rows_B={cr.get('value_b')},"
                f"only_in_A={diff_detail['only_in_a_total']},"
                f"only_in_B={diff_detail['only_in_b_total']},"
                f"mismatched={diff_detail['mismatched_total']}"
            )
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", _evt("COMPARISON")), ("source_label", ""),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", cr["status"]), ("detail", cr["detail"]),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", cr["name"]), ("check_type", f"comparison:{cr.get('rule', '')}"),
            ("actual_value", actual_value),
            ("query", ""), ("overall_status", overall_status),
            ("cost_usd", str(cr.get("cost_usd", 0))),
            ("failure_reason", ""),
            ("today_count", ""), ("baseline_count", ""),
            ("variance_pct", ""), ("tolerance", ""),
            ("window_start", ""), ("window_end", ""),
        ]))

    for sr in summary_results:
        records.append(OrderedDict([
            ("run_id", run_id), ("run_date", run_date), ("event_timestamp", ts),
            ("event_type", _evt("SUMMARY_QUERY")), ("source_label", ""),
            ("source_bucket", ""), ("source_prefix", ""), ("subfolder_count", ""),
            ("status", sr["status"]), ("detail", sr["detail"]),
            ("glue_job_name", ""), ("glue_job_run_id", ""), ("glue_duration_sec", ""),
            ("validation_name", sr["name"]), ("check_type", "summary"),
            ("actual_value", ""), ("query", ""), ("overall_status", overall_status),
            ("cost_usd", ""), ("failure_reason", ""),
            ("today_count", ""), ("baseline_count", ""),
            ("variance_pct", ""), ("tolerance", ""),
            ("window_start", ""), ("window_end", ""),
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


def _generate_summary_report(
    run_id, run_date, overall_status, overall_color, now_str,
    duration_str, total_cost_usd, audit_s3_path,
    audit_day_summary, validation_results, comparison_results,
    run_duration_sec, glue_job_statuses=None, s3_folder_results=None
):
    """Generate End of Batch Summary Report — audit-centric, per-layer,
    with Glue job status and S3 folder freshness checks.
    Section order: 1=Overview, 2=Glue Jobs, 3=S3 Folders, 4=Layer Breakdown,
    5=Runs (grouped by layer), 6=Validations, 7=Comparisons."""
    glue_job_statuses = glue_job_statuses or []
    s3_folder_results = s3_folder_results or []

    # ---- Compute totals from audit or fallback to validation results ----
    if audit_day_summary:
        tot = audit_day_summary["totals"]
    else:
        all_checks = validation_results + comparison_results
        tot = {
            "total": len(all_checks),
            "pass": sum(1 for c in all_checks if c["status"] == STATUS_PASS),
            "fail": sum(1 for c in all_checks if c["status"] == STATUS_FAIL),
            "warn": sum(1 for c in all_checks if c.get("status") == STATUS_WARN),
            "not_exec": sum(1 for c in all_checks if c.get("status") == STATUS_NOT_EXECUTED),
            "cost_usd": total_cost_usd,
            "distinct_runs": 1,
        }

    aud_pass_rate = (tot["pass"] / tot["total"] * 100) if tot["total"] > 0 else 0
    aud_fail_rate = (tot["fail"] / tot["total"] * 100) if tot["total"] > 0 else 0
    aud_warn_rate = (tot["warn"] / tot["total"] * 100) if tot["total"] > 0 else 0
    aud_ne_rate = (tot["not_exec"] / tot["total"] * 100) if tot["total"] > 0 else 0
    aud_pass_color = "#28a745" if aud_pass_rate >= 80 else (
        "#fd7e14" if aud_pass_rate >= 60 else "#dc3545")
    batch_status_icon = "&#x2705;" if overall_status == STATUS_PASS else "&#x274C;"
    # Bright badge color for header
    header_badge_bg = "#00e676" if overall_status == STATUS_PASS else "#ff1744"

    html = f"""
    <html><head><style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; color: #333;
               background-color: #f0f2f5; line-height: 1.6; }}
        .container {{ max-width: 960px; margin: 0 auto; background: white;
                      border-radius: 12px; overflow: hidden;
                      box-shadow: 0 4px 20px rgba(0,0,0,0.12); }}
        .header {{ background: linear-gradient(135deg, #0d47a1 0%, #1565c0 40%, #1976d2 70%, #42a5f5 100%);
                   padding: 35px 35px 28px 35px; color: white; position: relative; }}
        .header h1 {{ margin: 0 0 6px 0; font-size: 24px; letter-spacing: 0.5px;
                      color: #ffffff; text-shadow: 0 2px 4px rgba(0,0,0,0.3); }}
        .header p {{ margin: 0; font-size: 12px; opacity: 0.9; color: #e3f2fd; }}
        .header .batch-badge {{ display: inline-block; padding: 6px 18px; border-radius: 20px;
                                font-size: 13px; font-weight: bold; margin-left: 12px;
                                vertical-align: middle; letter-spacing: 0.5px;
                                box-shadow: 0 2px 8px rgba(0,0,0,0.25); }}
        .content {{ padding: 30px 35px; }}

        h2 {{ color: #1a237e; margin-top: 32px; margin-bottom: 16px; font-size: 17px;
              padding: 10px 16px; border-radius: 8px;
              background: linear-gradient(90deg, #e8eaf6 0%, #fff 100%);
              border-left: 5px solid #1565c0; }}

        table {{ border-collapse: collapse; width: 100%; margin-top: 12px; margin-bottom: 16px;
                 font-size: 12px; border-radius: 8px; overflow: hidden;
                 box-shadow: 0 1px 4px rgba(0,0,0,0.06); }}
        th {{ background: #212121; color: #ffffff;
              padding: 10px 12px; text-align: left;
              font-size: 11px; font-weight: bold; letter-spacing: 0.5px;
              text-transform: uppercase; white-space: nowrap; }}
        td {{ padding: 8px 12px; border-bottom: 1px solid #f0f0f0; white-space: nowrap; }}
        tr:nth-child(even) {{ background-color: #fafbfe; }}
        tr:hover {{ background-color: #f0f3ff; }}

        .badge {{ display: inline-block; padding: 4px 12px; border-radius: 12px;
                  color: white; font-weight: bold; font-size: 10px;
                  letter-spacing: 0.5px; text-transform: uppercase;
                  box-shadow: 0 2px 4px rgba(0,0,0,0.15); }}

        .pill {{ display: inline-block; padding: 6px 14px; border-radius: 20px;
                 color: white; font-size: 11px; font-weight: bold; }}

        .kpi-card {{ flex: 1; min-width: 130px; border-radius: 10px; padding: 18px 14px;
                     text-align: center; border-left: 4px solid; }}
        .kpi-card .kpi-label {{ font-size: 10px; font-weight: bold; color: #666;
                                text-transform: uppercase; letter-spacing: 0.5px; }}
        .kpi-card .kpi-value {{ font-size: 28px; font-weight: bold; margin: 6px 0 2px 0; }}
        .kpi-card .kpi-sub {{ font-size: 10px; color: #888; }}

        .health-bar {{ display: inline-block; height: 14px; border-radius: 7px;
                       vertical-align: middle; }}

        .layer-card {{ margin: 14px 0; border: 1px solid #e8eaf0; border-radius: 10px;
                       overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.04); }}
        .layer-card-header {{ display: flex; justify-content: space-between; align-items: center;
                              padding: 14px 18px;
                              background: linear-gradient(90deg, #fafbfe 0%, #fff 100%);
                              border-bottom: 1px solid #eee; }}
        .layer-card-body {{ padding: 14px 18px; }}

        .status-row {{ display: flex; gap: 18px; flex-wrap: wrap; margin-top: 8px; }}
        .status-item {{ display: flex; align-items: center; gap: 5px;
                        font-size: 12px; font-weight: bold; min-width: 100px; }}

        .layer-group-header {{ background: #e8eaf6; padding: 10px 16px; margin: 20px 0 4px 0;
                               border-radius: 8px; border-left: 4px solid #1565c0;
                               font-weight: bold; color: #1a237e; font-size: 13px; }}

        .red-row {{ background-color: #fff0f0 !important; }}
        .total-row {{ background-color: #e3f2fd !important; font-weight: bold; }}

        .footer {{ padding: 25px 35px; font-size: 11px; color: #888;
                   border-top: 2px solid #eee;
                   background: linear-gradient(90deg, #fafbfc 0%, #f5f6fa 100%); }}
    </style></head>
    <body>
    <div class="container">

    <!-- ======== HEADER ======== -->
    <div class="header">
        <h1 style="color:#ffffff;margin:0 0 6px 0;font-size:24px;">&#x1F4CB; End of Batch Summary Report
            <span style="display:inline-block;padding:6px 18px;border-radius:20px;
                         font-size:13px;font-weight:bold;margin-left:12px;
                         vertical-align:middle;letter-spacing:0.5px;
                         box-shadow:0 2px 8px rgba(0,0,0,0.25);
                         background:{header_badge_bg};color:#000;">
                {batch_status_icon} {overall_status}</span></h1>
        <p>&#x1F4C5; {run_date} &nbsp;&bull;&nbsp; &#x23F1; Duration: {duration_str}
           &nbsp;&bull;&nbsp; &#x1F550; Generated: {now_str}</p>
        <p style="margin-top:4px;">&#x1F194; Run ID: <code style="background:rgba(255,255,255,0.2);
           padding:2px 8px;border-radius:4px;font-size:11px;color:#fff;">{run_id}</code></p>
    </div>

    <div class="content">

    <!-- ======== 1. BATCH OVERVIEW ======== -->
    <h2>&#x1F4CA; 1. Batch Overview</h2>

    <div style="display:flex;flex-wrap:wrap;gap:12px;margin:15px 0;">
      <div class="kpi-card" style="background:#f0fff0;border-color:#28a745;">
        <div class="kpi-label">Total Checks</div>
        <div class="kpi-value" style="color:#2c3e50;">{tot['total']}</div>
        <div class="kpi-sub">across {tot['distinct_runs']} run(s)</div>
      </div>
      <div class="kpi-card" style="background:#f0fff0;border-color:#28a745;">
        <div class="kpi-label">Passed</div>
        <div class="kpi-value" style="color:#28a745;">{tot['pass']}</div>
        <div class="kpi-sub">{aud_pass_rate:.1f}%</div>
      </div>
      <div class="kpi-card" style="background:#fff0f0;border-color:#dc3545;">
        <div class="kpi-label">Failed</div>
        <div class="kpi-value" style="color:#dc3545;">{tot['fail']}</div>
        <div class="kpi-sub">{aud_fail_rate:.1f}%</div>
      </div>
      <div class="kpi-card" style="background:#fff8e1;border-color:#fd7e14;">
        <div class="kpi-label">Warnings</div>
        <div class="kpi-value" style="color:#fd7e14;">{tot['warn']}</div>
        <div class="kpi-sub">{aud_warn_rate:.1f}%</div>
      </div>
      <div class="kpi-card" style="background:#f5f5f5;border-color:#6c757d;">
        <div class="kpi-label">Not Executed</div>
        <div class="kpi-value" style="color:#6c757d;">{tot['not_exec']}</div>
        <div class="kpi-sub">{aud_ne_rate:.1f}%</div>
      </div>
    </div>

    <!-- Pass Rate Bar -->
    <div style="display:flex;gap:15px;margin:18px 0;">
      <div style="flex:2;background:#fafbfc;border:1px solid #eee;border-radius:8px;padding:15px;">
        <div style="color:#666;font-size:10px;font-weight:bold;margin-bottom:8px;
                    letter-spacing:0.5px;">OVERALL PASS RATE (TODAY)</div>
        <div style="background:#e9ecef;border-radius:10px;height:22px;overflow:hidden;">
          <div style="background:linear-gradient(90deg, {aud_pass_color}, {aud_pass_color}bb);
                      height:100%;width:{aud_pass_rate:.0f}%;border-radius:10px;
                      box-shadow:0 2px 4px {aud_pass_color}44;"></div>
        </div>
        <div style="text-align:center;margin-top:8px;">
          <span style="color:{aud_pass_color};font-size:24px;font-weight:bold;">{aud_pass_rate:.1f}%</span>
        </div>
      </div>
      <div style="flex:1;background:#fafbfc;border:1px solid #eee;border-radius:8px;
                  padding:15px;text-align:center;">
        <div style="color:#666;font-size:10px;font-weight:bold;letter-spacing:0.5px;">
            ATHENA COST (TODAY)</div>
        <div style="color:#8e44ad;font-size:24px;font-weight:bold;margin-top:14px;">
            ${tot['cost_usd']:.4f}</div>
        <div style="color:#888;font-size:10px;margin-top:4px;">
            {tot['distinct_runs']} run(s)</div>
      </div>
    </div>
"""

    section_num = 2

    # ======== 2. GLUE JOB STATUS ========
    if glue_job_statuses:
        total_jobs = len(glue_job_statuses)
        ran_today = sum(1 for g in glue_job_statuses if g["ran_today"])
        not_run = total_jobs - ran_today
        succeeded = sum(1 for g in glue_job_statuses if g["status"] == "SUCCEEDED")
        failed_jobs = sum(1 for g in glue_job_statuses if g["status"] in ("FAILED", "ERROR", "TIMEOUT"))
        sla_on_time = sum(1 for g in glue_job_statuses if g["sla_status"] == "ON TIME")
        sla_breached = sum(1 for g in glue_job_statuses if g["sla_status"] == "BREACHED")
        total_glue_cost = sum(g["cost_today"] for g in glue_job_statuses)

        html += f"""
    <h2>&#x2699; {section_num}. Glue Job Status</h2>
    <p style="color:#666;font-size:12px;">
        Checking {total_jobs} configured Glue job(s) for today's runs.</p>

    <!-- Glue summary pills -->
    <div style="display:flex;flex-wrap:wrap;gap:8px;margin:12px 0;">
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#17a2b8;
                   color:white;font-size:11px;font-weight:bold;">Jobs: {total_jobs}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#28a745;
                   color:white;font-size:11px;font-weight:bold;">Ran Today: {ran_today}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;
                   background:{'#dc3545' if not_run > 0 else '#28a745'};
                   color:white;font-size:11px;font-weight:bold;">Not Run: {not_run}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#28a745;
                   color:white;font-size:11px;font-weight:bold;">Succeeded: {succeeded}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;
                   background:{'#dc3545' if failed_jobs else '#6c757d'};
                   color:white;font-size:11px;font-weight:bold;">Failed: {failed_jobs}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#28a745;
                   color:white;font-size:11px;font-weight:bold;">SLA On Time: {sla_on_time}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;
                   background:{'#dc3545' if sla_breached else '#28a745'};
                   color:white;font-size:11px;font-weight:bold;">SLA Breached: {sla_breached}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#8e44ad;
                   color:white;font-size:11px;font-weight:bold;">Cost: ${total_glue_cost:.2f}</span>
    </div>

    <table><thead><tr>
        <th>#</th><th>Job Name</th><th>Domain</th><th>Status</th>
        <th>Ran Today</th><th>SLA</th><th>SLA Status</th>
        <th>Exec Time</th><th>Last Run</th><th>Worker</th>
        <th>Workers</th><th>Runs</th><th>Cost</th>
    </tr></thead><tbody>"""

        for i, gj in enumerate(glue_job_statuses, 1):
            st = gj["status"]
            st_color = {
                "SUCCEEDED": "#28a745", "FAILED": "#dc3545", "RUNNING": "#007bff",
                "ERROR": "#dc3545", "TIMEOUT": "#dc3545", "NOT_RUN": "#6c757d",
            }.get(st, "#6c757d")
            sla_color = {
                "ON TIME": "#28a745", "BREACHED": "#dc3545", "RUNNING": "#007bff",
            }.get(gj["sla_status"], "#6c757d")
            row_class = ' class="red-row"' if (
                st in ("FAILED", "ERROR", "TIMEOUT") or not gj["ran_today"]
            ) else ""
            ran_icon = "&#x2705;" if gj["ran_today"] else "&#x274C;"
            ran_text = "Yes" if gj["ran_today"] else "No"

            html += f"""<tr{row_class}>
                <td>{i}</td>
                <td><b>{gj['job_name']}</b></td>
                <td>{gj['domain']}</td>
                <td><span class="badge" style="background:{st_color};">{st}</span></td>
                <td style="text-align:center;">{ran_icon} {ran_text}</td>
                <td>{gj['sla_deadline']}</td>
                <td title="{gj['sla_detail']}">
                    <span class="badge" style="background:{sla_color};">
                        {gj['sla_status']}</span></td>
                <td>{gj['execution_time_min']}</td>
                <td style="font-size:11px;">{gj['last_run']}</td>
                <td>{gj['worker_type']}</td>
                <td>{gj['num_workers']}</td>
                <td style="text-align:center;">{gj['runs_today']}</td>
                <td style="text-align:right;">${gj['cost_today']:.4f}</td>
            </tr>"""

            if gj["error_message"] and st in ("FAILED", "ERROR", "TIMEOUT"):
                html += f"""<tr{row_class}>
                    <td colspan="13" style="color:#dc3545;font-size:11px;padding-left:30px;">
                        <b>Error:</b> {gj['error_message'][:300]}</td>
                </tr>"""

        html += "</tbody></table>"
        section_num += 1

    # ======== 3. S3 FOLDER FRESHNESS CHECK ========
    if s3_folder_results:
        s3_pass = sum(1 for s in s3_folder_results if s["status"] == STATUS_PASS)
        s3_warn = sum(1 for s in s3_folder_results if s["status"] == STATUS_WARN)
        s3_fail = sum(1 for s in s3_folder_results if s["status"] == STATUS_FAIL)

        html += f"""
    <h2>&#x1F4C2; {section_num}. S3 Folder Freshness Check</h2>
    <p style="color:#666;font-size:12px;">
        End-of-batch check: subfolders must be &le; max limit and created/modified within
        the configured time window.</p>

    <div style="display:flex;flex-wrap:wrap;gap:8px;margin:12px 0;">
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#17a2b8;
                   color:white;font-size:11px;font-weight:bold;">
          Folders: {len(s3_folder_results)}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;background:#28a745;
                   color:white;font-size:11px;font-weight:bold;">Pass: {s3_pass}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;
                   background:{'#fd7e14' if s3_warn else '#6c757d'};
                   color:white;font-size:11px;font-weight:bold;">Warn: {s3_warn}</span>
      <span style="display:inline-block;padding:6px 14px;border-radius:20px;
                   background:{'#dc3545' if s3_fail else '#6c757d'};
                   color:white;font-size:11px;font-weight:bold;">Fail: {s3_fail}</span>
    </div>
"""
        for sf in s3_folder_results:
            sf_color = {STATUS_PASS: "#28a745", STATUS_WARN: "#fd7e14",
                        STATUS_FAIL: "#dc3545"}.get(sf["status"], "#6c757d")
            sf_icon = {STATUS_PASS: "&#x2705;", STATUS_WARN: "&#x26A0;",
                       STATUS_FAIL: "&#x274C;"}.get(sf["status"], "&#x2139;")
            over_badge = ""
            if sf["over_limit"]:
                over_badge = (
                    f' <span class="badge" style="background:#dc3545;margin-left:6px;">'
                    f'OVER LIMIT ({sf["subfolder_count"]}/{sf["max_subfolders"]})</span>'
                )

            html += f"""
        <div class="layer-card" style="border-left:5px solid {sf_color};">
          <div class="layer-card-header">
            <div>
              <span style="font-size:16px;margin-right:6px;">{sf_icon}</span>
              <span style="font-weight:bold;color:#2c3e50;font-size:13px;">
                  {sf['label']}</span>
              <span class="badge" style="background:{sf_color};margin-left:8px;">
                  {sf['status']}</span>
              {over_badge}
            </div>
            <div style="font-size:11px;color:#888;">
              {sf['subfolder_count']} subfolder(s) &nbsp;|&nbsp;
              Max: {sf['max_subfolders']} &nbsp;|&nbsp;
              Freshness: {sf['max_age_hours']}h
            </div>
          </div>
          <div class="layer-card-body">
            <p style="color:#555;font-size:12px;margin:4px 0 10px 0;">
              &#x1F4DD; {sf['detail']}</p>"""

            # Subfolder detail table
            if sf["subfolders"]:
                html += """<table style="margin-top:8px;"><thead><tr>
                    <th>#</th><th>Subfolder</th><th>Last Modified</th>
                    <th>Age (Hours)</th><th>Fresh</th>
                </tr></thead><tbody>"""
                for si, sub in enumerate(sf["subfolders"], 1):
                    fresh_icon = "&#x2705;" if sub["fresh"] else "&#x274C;"
                    fresh_color = "#28a745" if sub["fresh"] else "#dc3545"
                    age_str = f'{sub["age_hours"]:.1f}h' if sub["age_hours"] is not None else "N/A"
                    html += f"""<tr>
                        <td>{si}</td>
                        <td><b>{sub['name']}</b></td>
                        <td style="font-size:11px;">{sub['last_modified'] or 'N/A'}</td>
                        <td style="text-align:center;">{age_str}</td>
                        <td style="text-align:center;color:{fresh_color};font-weight:bold;">
                            {fresh_icon}</td>
                    </tr>"""
                html += "</tbody></table>"

            html += """
          </div>
        </div>"""

        section_num += 1

    # ======== 4. LAYER-WISE BREAKDOWN ========
    if audit_day_summary and audit_day_summary.get("by_layer"):
        layers = audit_day_summary["by_layer"]
        html += f"""
    <h2>&#x1F4C2; {section_num}. Breakdown by Layer</h2>
    <p style="color:#666;font-size:12px;">
        Aggregated from <b>{tot['distinct_runs']}</b> run(s) in
        <code>audit_db.etl_orchestrator_audit</code> for {run_date}.</p>
"""

        # Layer cards
        for idx, layer in enumerate(layers):
            lpr = (layer["pass"] / layer["total"] * 100) if layer["total"] > 0 else 0
            lfr = (layer["fail"] / layer["total"] * 100) if layer["total"] > 0 else 0
            lpr_color = "#28a745" if lpr >= 80 else ("#fd7e14" if lpr >= 60 else "#dc3545")
            layer_status = "PASS" if layer["fail"] == 0 else "FAIL"
            layer_color = "#28a745" if layer_status == "PASS" else "#dc3545"
            layer_icon = "&#x2705;" if layer_status == "PASS" else "&#x274C;"

            health = min(int(lpr), 100)
            h_color = "#28a745" if health >= 80 else ("#fd7e14" if health >= 60 else "#dc3545")

            html += f"""
        <div class="layer-card" style="border-left:5px solid {layer_color};">
          <div class="layer-card-header">
            <div>
              <span style="font-size:16px;margin-right:6px;">{layer_icon}</span>
              <span style="font-weight:bold;color:#2c3e50;font-size:14px;">
                  {layer['event_type']}</span>
              <span class="badge" style="background:{layer_color};margin-left:8px;">
                  {layer_status}</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
              <span style="font-size:11px;color:#888;">{layer['total']} checks</span>
              <div class="health-bar" style="width:{health}px;background:{h_color};"></div>
              <span style="font-size:11px;color:{h_color};font-weight:bold;">{health}</span>
            </div>
          </div>
          <div class="layer-card-body">
            <div class="status-row">
              <div class="status-item">
                <span style="color:#28a745;">&#x2705;</span>
                <span style="color:#28a745;">Pass: {layer['pass']}</span>
              </div>
              <div class="status-item">
                <span style="color:#dc3545;">&#x274C;</span>
                <span style="color:#dc3545;">Fail: {layer['fail']}</span>
              </div>
              <div class="status-item">
                <span style="color:#fd7e14;">&#x26A0;</span>
                <span style="color:#fd7e14;">Warn: {layer['warn']}</span>
              </div>
              <div class="status-item">
                <span style="color:#6c757d;">&#x23F8;</span>
                <span style="color:#6c757d;">Not Exec: {layer['not_exec']}</span>
              </div>
              <div class="status-item">
                <span style="color:#8e44ad;">&#x1F4B0;</span>
                <span style="color:#8e44ad;">Cost: ${layer['cost_usd']:.4f}</span>
              </div>
            </div>
            <!-- Pass rate mini bar -->
            <div style="margin-top:10px;">
              <div style="display:flex;justify-content:space-between;font-size:10px;color:#666;
                          margin-bottom:4px;">
                <span>Pass Rate</span>
                <span style="color:{lpr_color};font-weight:bold;">{lpr:.1f}%</span>
              </div>
              <div style="background:#e9ecef;border-radius:6px;height:12px;overflow:hidden;">
                <div style="background:{lpr_color};height:100%;width:{lpr:.0f}%;
                            border-radius:6px;"></div>
              </div>
            </div>
          </div>
        </div>"""

        # Summary table
        html += """
        <table style="margin-top:20px;"><thead><tr>
            <th>#</th><th>Layer / Event Type</th><th>Total</th><th>Passed</th>
            <th>Failed</th><th>Warnings</th><th>Not Exec</th>
            <th>Pass Rate</th><th>Cost (USD)</th><th>Status</th>
        </tr></thead><tbody>"""

        for i, layer in enumerate(layers, 1):
            lpr = (layer["pass"] / layer["total"] * 100) if layer["total"] > 0 else 0
            lpr_color = "#28a745" if lpr >= 80 else ("#fd7e14" if lpr >= 60 else "#dc3545")
            l_status = "PASS" if layer["fail"] == 0 else "FAIL"
            l_color = "#28a745" if l_status == "PASS" else "#dc3545"
            fail_style = ' style="color:#dc3545;font-weight:bold;"' if layer["fail"] > 0 else ""
            row_class = ' class="red-row"' if layer["fail"] > 0 else ""

            html += f"""<tr{row_class}>
                <td>{i}</td>
                <td><b>{layer['event_type']}</b></td>
                <td style="text-align:center;">{layer['total']}</td>
                <td style="text-align:center;color:#28a745;font-weight:bold;">{layer['pass']}</td>
                <td style="text-align:center;"{fail_style}>{layer['fail']}</td>
                <td style="text-align:center;">{layer['warn']}</td>
                <td style="text-align:center;">{layer['not_exec']}</td>
                <td style="text-align:center;">
                    <span style="color:{lpr_color};font-weight:bold;">{lpr:.1f}%</span></td>
                <td style="text-align:right;">${layer['cost_usd']:.4f}</td>
                <td><span class="badge" style="background:{l_color};">{l_status}</span></td>
            </tr>"""

        # Totals row
        html += f"""<tr class="total-row">
            <td></td>
            <td><b>TOTAL</b></td>
            <td style="text-align:center;">{tot['total']}</td>
            <td style="text-align:center;color:#28a745;">{tot['pass']}</td>
            <td style="text-align:center;color:#dc3545;">{tot['fail']}</td>
            <td style="text-align:center;">{tot['warn']}</td>
            <td style="text-align:center;">{tot['not_exec']}</td>
            <td style="text-align:center;">
                <span style="color:{aud_pass_color};font-weight:bold;">{aud_pass_rate:.1f}%</span></td>
            <td style="text-align:right;">${tot['cost_usd']:.4f}</td>
            <td><span class="badge" style="background:{overall_color};">{overall_status}</span></td>
        </tr>"""
        html += "</tbody></table>"
        section_num += 1

    # ======== 5. TODAY'S BATCH RUNS (grouped by layer) ========
    runs_by_layer = (audit_day_summary or {}).get("runs_by_layer", [])
    plain_runs = (audit_day_summary or {}).get("runs", [])

    if runs_by_layer:
        html += f"""
    <h2>&#x1F504; {section_num}. Today's Batch Runs</h2>
    <p style="color:#666;font-size:12px;">
        Individual pipeline executions recorded for {run_date}, grouped by layer.</p>"""

        # Group runs_by_layer by layer prefix extracted from event_type
        # event_type format: "BASE_VALIDATION", "MASTER_COMPARISON", or legacy "VALIDATION"
        check_types = {"VALIDATION", "COMPARISON", "STALE_CHECK", "GLUE_TRIGGER", "SUMMARY_QUERY"}
        layer_order = ["STAGING", "DATALAKE", "BASE", "MASTER", "SUMMARY"]

        def _extract_layer(event_type):
            """Extract layer from event_type like BASE_VALIDATION -> BASE."""
            for ct in check_types:
                if event_type.endswith(f"_{ct}"):
                    return event_type[: -(len(ct) + 1)]
            # Legacy format: event_type IS the check type (no layer prefix)
            if event_type in check_types:
                return event_type
            return event_type

        grouped = OrderedDict()
        for rbl in runs_by_layer:
            et = rbl.get("event_type", "UNKNOWN")
            layer_key = _extract_layer(et)
            grouped.setdefault(layer_key, []).append(rbl)
        # Sort groups by canonical layer order
        sorted_groups = []
        for lo in layer_order:
            if lo in grouped:
                sorted_groups.append((lo, grouped.pop(lo)))
        for remaining_key, remaining_val in grouped.items():
            sorted_groups.append((remaining_key, remaining_val))

        row_num = 0
        for layer_name, layer_runs in sorted_groups:
            layer_pass = all(r.get("overall_status") == "PASS" for r in layer_runs)
            lg_color = "#28a745" if layer_pass else "#dc3545"
            lg_icon = "&#x2705;" if layer_pass else "&#x274C;"
            html += f"""
        <div class="layer-group-header">
            {lg_icon} Layer: {layer_name}
            <span style="float:right;font-size:11px;color:#666;">
                {len(layer_runs)} run(s)</span>
        </div>
        <table><thead><tr>
            <th>#</th><th>Run ID</th><th>Layer</th><th>Status</th><th>Events</th>
            <th>Pass</th><th>Fail</th>
            <th>Cost (USD)</th><th>First Event</th><th>Last Event</th><th>Health</th>
        </tr></thead><tbody>"""

            for r in layer_runs:
                row_num += 1
                r_status = r.get("overall_status", "")
                r_color = "#28a745" if r_status == "PASS" else (
                    "#dc3545" if r_status == "FAIL" else "#6c757d")
                row_class = ' class="red-row"' if r_status == "FAIL" else ""
                r_health = 100 if r_status == "PASS" else (30 if r_status == "FAIL" else 50)
                rh_color = "#28a745" if r_health >= 80 else (
                    "#fd7e14" if r_health >= 60 else "#dc3545")

                run_dur = ""
                try:
                    fe = datetime.fromisoformat(r["first_event"].replace("Z", "+00:00"))
                    le = datetime.fromisoformat(r["last_event"].replace("Z", "+00:00"))
                    rd_sec = (le - fe).total_seconds()
                    run_dur = f" ({int(rd_sec // 60)}m {int(rd_sec % 60)}s)"
                except Exception:
                    pass

                et = r.get("event_type", "")
                display_id = f'{r["run_id"][:12]}...[{et}]'

                html += f"""<tr{row_class}>
                    <td>{row_num}</td>
                    <td><code style="font-size:11px;">{display_id}</code></td>
                    <td><b>{r.get('event_type', '-')}</b></td>
                    <td><span class="badge" style="background:{r_color};">{r_status}</span></td>
                    <td style="text-align:center;">{r.get('event_count', 0)}</td>
                    <td style="text-align:center;color:#28a745;font-weight:bold;">
                        {r.get('pass_count', 0)}</td>
                    <td style="text-align:center;color:#dc3545;font-weight:bold;">
                        {r.get('fail_count', 0)}</td>
                    <td style="text-align:right;">${r.get('cost_usd', 0):.4f}</td>
                    <td style="font-size:11px;">{r.get('first_event', '-')}</td>
                    <td style="font-size:11px;">{r.get('last_event', '-')}{run_dur}</td>
                    <td>
                      <div class="health-bar" style="width:{r_health}px;background:{rh_color};"></div>
                      <span style="font-size:11px;color:{rh_color};font-weight:bold;">
                          {r_health}</span>
                    </td>
                </tr>"""

            html += "</tbody></table>"

        section_num += 1

    elif plain_runs:
        # Fallback: show ungrouped runs if runs_by_layer is not available
        html += f"""
    <h2>&#x1F504; {section_num}. Today's Batch Runs</h2>
    <p style="color:#666;font-size:12px;">
        Individual pipeline executions recorded for {run_date}.</p>

    <table><thead><tr>
        <th>#</th><th>Run ID</th><th>Status</th><th>Events</th>
        <th>Cost (USD)</th><th>First Event</th><th>Last Event</th><th>Health</th>
    </tr></thead><tbody>"""

        for i, r in enumerate(plain_runs, 1):
            r_status = r.get("overall_status", "")
            r_color = "#28a745" if r_status == "PASS" else (
                "#dc3545" if r_status == "FAIL" else "#6c757d")
            row_class = ' class="red-row"' if r_status == "FAIL" else ""
            r_health = 100 if r_status == "PASS" else (30 if r_status == "FAIL" else 50)
            rh_color = "#28a745" if r_health >= 80 else (
                "#fd7e14" if r_health >= 60 else "#dc3545")

            run_dur = ""
            try:
                fe = datetime.fromisoformat(r["first_event"].replace("Z", "+00:00"))
                le = datetime.fromisoformat(r["last_event"].replace("Z", "+00:00"))
                rd_sec = (le - fe).total_seconds()
                run_dur = f" ({int(rd_sec // 60)}m {int(rd_sec % 60)}s)"
            except Exception:
                pass

            html += f"""<tr{row_class}>
                <td>{i}</td>
                <td><code style="font-size:11px;">{r['run_id'][:12]}...</code></td>
                <td><span class="badge" style="background:{r_color};">{r_status}</span></td>
                <td style="text-align:center;">{r['event_count']}</td>
                <td style="text-align:right;">${r['cost_usd']:.4f}</td>
                <td style="font-size:11px;">{r['first_event']}</td>
                <td style="font-size:11px;">{r['last_event']}{run_dur}</td>
                <td>
                  <div class="health-bar" style="width:{r_health}px;background:{rh_color};"></div>
                  <span style="font-size:11px;color:{rh_color};font-weight:bold;">{r_health}</span>
                </td>
            </tr>"""

        html += "</tbody></table>"
        section_num += 1

    # ======== 6. VALIDATION DETAILS (if any ran alongside summary) ========
    if validation_results:
        html += f"""
    <h2>&#x1F50D; {section_num}. Validation Check Details</h2>
    <table><thead><tr>
        <th>#</th><th>Validation</th><th>Check Type</th>
        <th>Status</th><th>Actual</th><th>Cost</th><th>Detail</th>
    </tr></thead><tbody>"""

        for i, vr in enumerate(validation_results, 1):
            color = _status_color(vr["status"])
            cost_str = f"${vr.get('cost_usd', 0):.4f}" if vr.get("cost_usd") else "-"
            row_class = ' class="red-row"' if vr["status"] == STATUS_FAIL else ""
            retry_badge = (' <span style="color:#fd7e14;font-size:10px;">[retried]</span>'
                           if vr.get("retried") else "")
            html += f"""<tr{row_class}>
                <td>{i}</td>
                <td><b>{vr['name']}</b>{retry_badge}<br>
                    <small style="color:#888;">{vr.get('description', '')}</small></td>
                <td>{vr.get('check_type', '-')}</td>
                <td><span class="badge" style="background:{color};">{vr['status']}</span></td>
                <td>{vr.get('actual_value', '-')}</td>
                <td style="font-size:11px;">{cost_str}</td>
                <td>{vr.get('detail', '-')}</td>
            </tr>"""

        html += "</tbody></table>"
        section_num += 1

    # ======== 7. COMPARISON CHECKS (if any) ========
    if comparison_results:
        html += f"""
    <h2>&#x2696; {section_num}. Cross-Query Comparisons</h2>
    <table><thead><tr>
        <th>#</th><th>Comparison</th><th>Rule</th>
        <th>Value A</th><th>Value B</th><th>Status</th><th>Detail</th>
    </tr></thead><tbody>"""

        for i, cr in enumerate(comparison_results, 1):
            color = _status_color(cr["status"])
            row_class = ' class="red-row"' if cr["status"] == STATUS_FAIL else ""
            html += f"""<tr{row_class}>
                <td>{i}</td>
                <td><b>{cr['name']}</b><br>
                    <small style="color:#888;">{cr.get('description', '')}</small></td>
                <td>{cr.get('rule', '-')}</td>
                <td><b>{cr.get('label_a', 'A')}:</b> {cr.get('value_a', '-')}</td>
                <td><b>{cr.get('label_b', 'B')}:</b> {cr.get('value_b', '-')}</td>
                <td><span class="badge" style="background:{color};">{cr['status']}</span></td>
                <td>{cr.get('detail', '-')}</td>
            </tr>"""

        html += "</tbody></table>"
        section_num += 1

    # ======== FOOTER ========
    html += f"""
    </div><!-- end content -->

    <div class="footer">
        <p>&#x1F4CB; <b>End of Batch Summary</b> &nbsp;&bull;&nbsp;
           &#x1F194; {run_id}</p>
        <p>&#x1F4C1; <b>Audit Log:</b> {audit_s3_path or 'N/A'}</p>
        <p>&#x1F4B0; <b>Athena Cost:</b> ${total_cost_usd:.4f} &nbsp;&bull;&nbsp;
           &#x23F1; <b>Duration:</b> {duration_str} &nbsp;&bull;&nbsp;
           &#x1F4C5; <b>Date:</b> {run_date}</p>
        <p style="margin-top:10px;">Generated by <b>Strands ETL Orchestrator</b> &mdash;
           End of Batch Summary Report &nbsp;|&nbsp; BIG DATA Team</p>
    </div>

    </div><!-- end container -->
    </body></html>"""

    return html


def generate_dashboard_html(
    run_id, run_date, run_mode, stale_results, glue_results,
    validation_results, comparison_results, summary_results,
    overall_status, audit_s3_path, run_duration_sec=0, total_cost_usd=0.0,
    deviation_alerts=None, summary_config=None, audit_day_summary=None,
    zscore_results=None, glue_job_statuses=None, s3_folder_results=None
):
    """Generate a full dashboard HTML email body with charts, KPI scorecards,
    z-score analysis, optional GIF embeds, and today's audit summary."""
    deviation_alerts = deviation_alerts or []
    zscore_results = zscore_results or []
    glue_job_statuses = glue_job_statuses or []
    s3_folder_results = s3_folder_results or []
    summary_config = summary_config or {}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    overall_color = "#28a745" if overall_status == STATUS_PASS else "#dc3545"
    duration_str = f"{int(run_duration_sec // 60)}m {int(run_duration_sec % 60)}s"

    # ==================================================================
    # SUMMARY MODE: End of Batch Summary Report (audit-centric, no charts)
    # ==================================================================
    if run_mode == "summary":
        return _generate_summary_report(
            run_id, run_date, overall_status, overall_color, now_str,
            duration_str, total_cost_usd, audit_s3_path,
            audit_day_summary, validation_results, comparison_results,
            run_duration_sec, glue_job_statuses, s3_folder_results)

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
        .alert-banner {{ background: linear-gradient(135deg, #ff416c, #ff4b2b); color: white;
                         padding: 18px 24px; border-radius: 10px; margin: 20px 0;
                         display: flex; align-items: center; gap: 15px; }}
        .alert-banner .alert-title {{ font-size: 16px; font-weight: bold; margin-bottom: 4px; }}
        .alert-banner .alert-detail {{ font-size: 12px; opacity: 0.9; }}
        .gif-banner {{ text-align: center; margin: 15px 0; }}
        .gif-banner img {{ border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.12); }}
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

    if zscore_results:
        html += f"""
        <span class="summary-box" style="background-color: #8e44ad;">
            Z-Score Checks: {len(zscore_results)}</span>"""

    html += "</div>"

    # ---- GIF 1: Header decorative GIF (summary mode) ----
    gif_urls = summary_config.get("gif_urls", {})
    gif_header_url = gif_urls.get("header", "")
    if gif_header_url and run_mode == "summary":
        html += f"""
    <div class="gif-banner">
      <img src="{gif_header_url}" alt="Analytics Dashboard"
           style="width: 280px; height: auto;" title="ETL Summary Analytics" />
      <p style="color: #888; font-size: 11px; margin-top: 6px;">Real-time ETL Analytics Summary</p>
    </div>"""

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
        # Separate rolling_average checks from standard checks
        standard_checks = [v for v in validation_results if v.get("check_type") != "rolling_average"]
        rolling_checks = [v for v in validation_results if v.get("check_type") == "rolling_average"]

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

        # ---- Rolling Average Visual Cards ----
        if rolling_checks:
            html += f"""<h2 style="margin-top:25px;">{section_num}.1 Rolling Average Checks</h2>"""
            html += '<div style="display:flex;flex-wrap:wrap;gap:15px;margin:15px 0;">'
            for ra in rolling_checks:
                ra_color = _status_color(ra["status"])
                today_cnt = ra.get("today_count")
                baseline_cnt = ra.get("baseline_count")
                variance = ra.get("variance_pct")
                tolerance = ra.get("tolerance", 25)
                window_start = ra.get("window_start", "")
                window_end = ra.get("window_end", "")

                today_str = f"{today_cnt:,.0f}" if today_cnt is not None else "N/A"
                baseline_str = f"{baseline_cnt:,.0f}" if baseline_cnt is not None else "N/A"
                variance_str = f"{variance:.1f}%" if variance is not None else "N/A"

                # Variance gauge: fill proportional to variance/tolerance
                gauge_pct = min(abs(variance or 0) / max(tolerance, 1) * 100, 100)
                gauge_color = "#28a745" if gauge_pct <= 60 else ("#fd7e14" if gauge_pct <= 100 else "#dc3545")

                html += f"""
                <div style="flex:1;min-width:280px;max-width:450px;background:#fafbfc;
                            border:1px solid #eee;border-radius:10px;padding:18px;
                            border-top:4px solid {ra_color};">
                  <div style="display:flex;justify-content:space-between;align-items:center;
                              margin-bottom:12px;">
                    <div style="font-weight:bold;color:#2c3e50;font-size:14px;">
                        {ra['name'].replace('_', ' ').title()}</div>
                    <span class="badge" style="background-color:{ra_color};">{ra['status']}</span>
                  </div>
                  <p style="color:#888;font-size:11px;margin:0 0 12px 0;">
                    {ra.get('description', '')}</p>
                  <div style="display:flex;gap:12px;margin-bottom:12px;">
                    <div style="flex:1;text-align:center;background:white;border-radius:8px;
                                padding:10px;border:1px solid #eef;">
                      <div style="color:#666;font-size:10px;font-weight:bold;">TODAY</div>
                      <div style="color:#2c3e50;font-size:22px;font-weight:bold;">{today_str}</div>
                    </div>
                    <div style="flex:1;text-align:center;background:white;border-radius:8px;
                                padding:10px;border:1px solid #eef;">
                      <div style="color:#666;font-size:10px;font-weight:bold;">BASELINE AVG</div>
                      <div style="color:#3498db;font-size:22px;font-weight:bold;">{baseline_str}</div>
                    </div>
                  </div>
                  <div style="margin-bottom:8px;">
                    <div style="display:flex;justify-content:space-between;font-size:11px;
                                color:#666;margin-bottom:4px;">
                      <span>Variance: <b style="color:{ra_color};">{variance_str}</b></span>
                      <span>Tolerance: {tolerance}%</span>
                    </div>
                    <div style="background:#e9ecef;border-radius:6px;height:14px;overflow:hidden;">
                      <div style="background:{gauge_color};height:100%;width:{gauge_pct:.0f}%;
                                  border-radius:6px;transition:width 0.3s;"></div>
                    </div>
                  </div>
                  <div style="font-size:10px;color:#aaa;">
                    Window: {window_start} → {window_end}</div>
                </div>"""
            html += "</div>"

        # ---- Source Reconciliation Visual Cards ----
        recon_checks = [v for v in validation_results if v.get("check_type") == "source_reconciliation"]
        if recon_checks:
            html += f"""<h2 style="margin-top:25px;">{section_num}.2 Source Reconciliation</h2>"""
            for rc in recon_checks:
                rc_color = _status_color(rc["status"])
                devs = rc.get("recon_deviations", [])
                html += f"""
                <div style="margin:15px 0;padding:18px;background:#fafbfc;
                            border-radius:10px;border:1px solid #eee;
                            border-left:4px solid {rc_color};">
                  <div style="display:flex;justify-content:space-between;align-items:center;
                              margin-bottom:10px;">
                    <div style="font-weight:bold;color:#2c3e50;font-size:14px;">
                        {rc['name'].replace('_', ' ').title()}</div>
                    <span class="badge" style="background-color:{rc_color};">{rc['status']}</span>
                  </div>
                  <p style="color:#888;font-size:11px;margin:0 0 12px 0;">
                    {rc.get('description', '')}</p>"""

                if devs:
                    html += """<table style="margin-bottom:10px;">
                        <thead><tr><th>Table/Entity</th><th>Date</th>
                        <th>Source Count</th><th>Target Count</th>
                        <th>Deviation</th></tr></thead><tbody>"""
                    for d in devs[:20]:
                        html += f"""<tr>
                            <td><b>{d.get('table_name', '-')}</b></td>
                            <td>{d.get('extract_date', '-')}</td>
                            <td style="text-align:right;">{d['count_source']:,.0f}</td>
                            <td style="text-align:right;">{d['count_target']:,.0f}</td>
                            <td style="text-align:right;color:#dc3545;font-weight:bold;">
                                {d['deviation']:,.0f}</td></tr>"""
                    html += "</tbody></table>"
                    if len(devs) > 20:
                        html += f'<p style="color:#888;font-size:11px;">...and {len(devs) - 20} more rows</p>'
                else:
                    html += '<p style="color:#28a745;font-weight:bold;">All counts reconciled – no deviations found.</p>'

                html += "</div>"

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

            # Render data_diff detail tables when differences exist
            dd = cr.get("diff_detail")
            if dd and (dd["only_in_a"] or dd["only_in_b"] or dd["mismatched"]):
                key_cols = dd.get("key_columns", [])
                key_header = "".join(f"<th>{k}</th>" for k in key_cols)

                html += f'<tr><td colspan="7" style="padding:0;">'
                html += '<div style="padding:12px 16px;background:#fef9f0;border-top:2px solid #f0c040;">'

                if dd["only_in_a"]:
                    trunc_a = f" (showing {len(dd['only_in_a'])} of {dd['only_in_a_total']})" if dd["only_in_a_total"] > len(dd["only_in_a"]) else ""
                    html += f'<p style="color:#c0392b;font-weight:bold;margin:8px 0 4px;">Only in {cr.get("label_a", "A")} ({dd["only_in_a_total"]} records){trunc_a}</p>'
                    html += f'<table style="font-size:11px;"><thead><tr>{key_header}</tr></thead><tbody>'
                    for row in dd["only_in_a"]:
                        html += "<tr>" + "".join(f"<td>{row.get(k, '')}</td>" for k in key_cols) + "</tr>"
                    html += "</tbody></table>"

                if dd["only_in_b"]:
                    trunc_b = f" (showing {len(dd['only_in_b'])} of {dd['only_in_b_total']})" if dd["only_in_b_total"] > len(dd["only_in_b"]) else ""
                    html += f'<p style="color:#2980b9;font-weight:bold;margin:8px 0 4px;">Only in {cr.get("label_b", "B")} ({dd["only_in_b_total"]} records){trunc_b}</p>'
                    html += f'<table style="font-size:11px;"><thead><tr>{key_header}</tr></thead><tbody>'
                    for row in dd["only_in_b"]:
                        html += "<tr>" + "".join(f"<td>{row.get(k, '')}</td>" for k in key_cols) + "</tr>"
                    html += "</tbody></table>"

                if dd["mismatched"]:
                    cmp_cols = dd.get("compare_columns", [])
                    trunc_m = f" (showing {len(dd['mismatched'])} of {dd['mismatched_total']})" if dd["mismatched_total"] > len(dd["mismatched"]) else ""
                    html += f'<p style="color:#8e44ad;font-weight:bold;margin:8px 0 4px;">Value Mismatches ({dd["mismatched_total"]} records){trunc_m}</p>'
                    mismatch_hdr = key_header + "".join(f"<th>{c} (A)</th><th>{c} (B)</th>" for c in cmp_cols)
                    html += f'<table style="font-size:11px;"><thead><tr>{mismatch_hdr}</tr></thead><tbody>'
                    for m in dd["mismatched"]:
                        html += "<tr>"
                        for k in key_cols:
                            html += f"<td>{m['key'].get(k, '')}</td>"
                        for c in cmp_cols:
                            if c in m["diffs"]:
                                html += f'<td style="background:#fde8e8;">{m["diffs"][c]["a"]}</td>'
                                html += f'<td style="background:#e8f4fd;">{m["diffs"][c]["b"]}</td>'
                            else:
                                html += "<td>-</td><td>-</td>"
                        html += "</tr>"
                    html += "</tbody></table>"

                if dd["duplicates_a"] or dd["duplicates_b"]:
                    html += f'<p style="color:#e67e22;font-size:11px;margin:8px 0 0;">Duplicate keys: A={dd["duplicates_a"]}, B={dd["duplicates_b"]}</p>'

                html += '</div></td></tr>'

        html += "</tbody></table>"
        section_num += 1

    # ---- Section: Today's Audit Summary (all runs across all layers) ----
    if audit_day_summary and run_mode == "summary":
        ads = audit_day_summary
        tot = ads["totals"]
        html += f"<h2>{section_num}. Today's Overall Audit Summary</h2>"
        html += f"""<p style="color:#666;font-size:12px;">
            Aggregated from <b>{tot['distinct_runs']}</b> run(s) recorded in
            <code>audit_db.etl_orchestrator_audit</code> for {run_date}.</p>"""

        # KPI cards for audit totals
        aud_pass_rate = (tot["pass"] / tot["total"] * 100) if tot["total"] > 0 else 0
        aud_pass_color = "#28a745" if aud_pass_rate >= 80 else (
            "#fd7e14" if aud_pass_rate >= 60 else "#dc3545")
        html += f"""
        <div style="display:flex;flex-wrap:wrap;gap:12px;margin:15px 0;">
          <div style="flex:1;min-width:130px;background:#f0fff0;border-left:4px solid #28a745;
                      border-radius:8px;padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">TOTAL CHECKS</div>
            <div style="color:#2c3e50;font-size:26px;font-weight:bold;">{tot['total']}</div>
            <div style="color:#888;font-size:10px;">across all runs</div>
          </div>
          <div style="flex:1;min-width:130px;background:#f0fff0;border-left:4px solid #28a745;
                      border-radius:8px;padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">PASSED</div>
            <div style="color:#28a745;font-size:26px;font-weight:bold;">{tot['pass']}</div>
            <div style="color:#888;font-size:10px;">{aud_pass_rate:.1f}%</div>
          </div>
          <div style="flex:1;min-width:130px;background:#fff0f0;border-left:4px solid #dc3545;
                      border-radius:8px;padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">FAILED</div>
            <div style="color:#dc3545;font-size:26px;font-weight:bold;">{tot['fail']}</div>
            <div style="color:#888;font-size:10px;">{(tot['fail']/tot['total']*100) if tot['total'] else 0:.1f}%</div>
          </div>
          <div style="flex:1;min-width:130px;background:#fff8e1;border-left:4px solid #fd7e14;
                      border-radius:8px;padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">WARNINGS</div>
            <div style="color:#fd7e14;font-size:26px;font-weight:bold;">{tot['warn']}</div>
            <div style="color:#888;font-size:10px;">{(tot['warn']/tot['total']*100) if tot['total'] else 0:.1f}%</div>
          </div>
          <div style="flex:1;min-width:130px;background:#f5f5f5;border-left:4px solid #6c757d;
                      border-radius:8px;padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">NOT EXECUTED</div>
            <div style="color:#6c757d;font-size:26px;font-weight:bold;">{tot['not_exec']}</div>
            <div style="color:#888;font-size:10px;">{(tot['not_exec']/tot['total']*100) if tot['total'] else 0:.1f}%</div>
          </div>
        </div>

        <div style="display:flex;gap:15px;margin-bottom:20px;">
          <div style="flex:1;background:#fafbfc;border:1px solid #eee;border-radius:8px;
                      padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">OVERALL PASS RATE (TODAY)</div>
            <div style="margin:8px 0;">
              <div style="background:#e9ecef;border-radius:10px;height:20px;overflow:hidden;">
                <div style="background:{aud_pass_color};height:100%;width:{aud_pass_rate:.0f}%;
                            border-radius:10px;"></div>
              </div>
            </div>
            <div style="color:{aud_pass_color};font-size:22px;font-weight:bold;">{aud_pass_rate:.1f}%</div>
          </div>
          <div style="flex:1;background:#fafbfc;border:1px solid #eee;border-radius:8px;
                      padding:15px;text-align:center;">
            <div style="color:#666;font-size:10px;font-weight:bold;">TOTAL ATHENA COST (TODAY)</div>
            <div style="color:#8e44ad;font-size:22px;font-weight:bold;margin-top:12px;">
              ${tot['cost_usd']:.4f}</div>
            <div style="color:#888;font-size:10px;margin-top:4px;">
              {tot['distinct_runs']} run(s)</div>
          </div>
        </div>"""

        # By-layer breakdown table
        if ads["by_layer"]:
            html += """<h3 style="color:#2c3e50;font-size:14px;margin-top:20px;
                        border-left:3px solid #8e44ad;padding-left:10px;">
                        Breakdown by Layer / Event Type</h3>"""
            html += """<table><thead><tr>
                <th>Layer / Event Type</th><th>Total</th><th>Passed</th>
                <th>Failed</th><th>Warnings</th><th>Not Executed</th>
                <th>Pass Rate</th><th>Cost (USD)</th>
                </tr></thead><tbody>"""
            for layer in ads["by_layer"]:
                lpr = (layer["pass"] / layer["total"] * 100) if layer["total"] > 0 else 0
                pr_color = "#28a745" if lpr >= 80 else ("#fd7e14" if lpr >= 60 else "#dc3545")
                fail_style = ' style="color:#dc3545;font-weight:bold;"' if layer["fail"] > 0 else ""
                html += f"""<tr>
                    <td><b>{layer['event_type']}</b></td>
                    <td style="text-align:center;">{layer['total']}</td>
                    <td style="text-align:center;color:#28a745;font-weight:bold;">{layer['pass']}</td>
                    <td style="text-align:center;"{fail_style}>{layer['fail']}</td>
                    <td style="text-align:center;">{layer['warn']}</td>
                    <td style="text-align:center;">{layer['not_exec']}</td>
                    <td style="text-align:center;">
                      <span style="color:{pr_color};font-weight:bold;">{lpr:.1f}%</span>
                    </td>
                    <td style="text-align:right;">${layer['cost_usd']:.4f}</td>
                    </tr>"""
            # Totals row
            html += f"""<tr style="background:#e8f4fd;font-weight:bold;">
                <td>TOTAL</td>
                <td style="text-align:center;">{tot['total']}</td>
                <td style="text-align:center;color:#28a745;">{tot['pass']}</td>
                <td style="text-align:center;color:#dc3545;">{tot['fail']}</td>
                <td style="text-align:center;">{tot['warn']}</td>
                <td style="text-align:center;">{tot['not_exec']}</td>
                <td style="text-align:center;">
                  <span style="color:{aud_pass_color};">{aud_pass_rate:.1f}%</span>
                </td>
                <td style="text-align:right;">${tot['cost_usd']:.4f}</td>
                </tr>"""
            html += "</tbody></table>"

        # Runs breakdown table
        if ads["runs"]:
            html += """<h3 style="color:#2c3e50;font-size:14px;margin-top:20px;
                        border-left:3px solid #3498db;padding-left:10px;">
                        Today's Runs</h3>"""
            html += """<table><thead><tr>
                <th>#</th><th>Run ID</th><th>Status</th><th>Events</th>
                <th>Cost (USD)</th><th>First Event</th><th>Last Event</th>
                </tr></thead><tbody>"""
            for i, r in enumerate(ads["runs"], 1):
                r_color = "#28a745" if r["overall_status"] == "PASS" else "#dc3545"
                html += f"""<tr>
                    <td>{i}</td>
                    <td><code style="font-size:11px;">{r['run_id']}</code></td>
                    <td><span class="badge" style="background-color:{r_color};">
                        {r['overall_status']}</span></td>
                    <td style="text-align:center;">{r['event_count']}</td>
                    <td style="text-align:right;">${r['cost_usd']:.4f}</td>
                    <td style="font-size:11px;">{r['first_event']}</td>
                    <td style="font-size:11px;">{r['last_event']}</td>
                    </tr>"""
            html += "</tbody></table>"

        section_num += 1

    # ---- Section: Z-Score Analysis (past 7 days, no chart) ----
    if zscore_results:
        html += f"<h2>{section_num}. Z-Score Analysis (7-Day vs 30-Day Rolling Avg)</h2>"
        html += """<p style="color:#666;font-size:12px;">
            Statistical z-score computed for the past 7 days against the 30-day rolling average.
            <b>|z| &ge; 3</b> = CRITICAL (red), <b>|z| &ge; 2</b> = WARNING (orange),
            otherwise NORMAL (green).</p>"""

        for zr in zscore_results:
            html += f"""<div style="margin:20px 0;padding:15px;background:#fafbfc;
                         border-radius:8px;border:1px solid #eee;">
                <h3 style="color:#2c3e50;margin-top:0;">
                    {zr['name'].replace('_', ' ').title()}</h3>
                <p style="color:#666;font-size:12px;">{zr.get('description', '')}</p>"""

            if zr["status"] == STATUS_FAIL:
                html += f'<p style="color:#dc3545;">Query failed: {zr["detail"]}</p>'
            elif zr["data"]:
                html += '<table style="margin-bottom:15px;"><thead><tr>'
                for h in zr["headers"]:
                    html += f"<th>{h}</th>"
                html += "</tr></thead><tbody>"

                for row in zr["data"]:
                    z_flag = row.get("z_flag", "NORMAL")
                    if z_flag == "CRITICAL":
                        row_bg = "background-color:#fff0f0;"
                    elif z_flag == "WARNING":
                        row_bg = "background-color:#fff8e1;"
                    else:
                        row_bg = ""

                    html += f'<tr style="{row_bg}">'
                    for h in zr["headers"]:
                        cell_val = row.get(h, "")
                        if h == "z_score":
                            try:
                                zval = float(cell_val)
                                z_color = "#dc3545" if abs(zval) >= 3 else (
                                    "#fd7e14" if abs(zval) >= 2 else "#28a745")
                                sign = "+" if zval > 0 else ""
                                html += f'<td style="text-align:right;font-weight:bold;color:{z_color};">{sign}{zval:.2f}</td>'
                            except (ValueError, TypeError):
                                html += f"<td>{cell_val}</td>"
                        elif h == "z_flag":
                            flag_color = {"CRITICAL": "#dc3545", "WARNING": "#fd7e14",
                                          "NORMAL": "#28a745"}.get(cell_val, "#333")
                            html += f'<td><span class="badge" style="background-color:{flag_color};">{cell_val}</span></td>'
                        else:
                            html += f"<td>{cell_val}</td>"
                    html += "</tr>"
                html += "</tbody></table>"
            html += "</div>"
        section_num += 1

    # ---- Section: Trends Dashboard (charts — bar/pie/trend, no deviation %) ----
    if summary_results:
        html += f"<h2>{section_num}. Trends Dashboard</h2>"

        # GIF 2: Trend decorative GIF (before summary cards)
        gif_trend_url = gif_urls.get("trend", "")
        if gif_trend_url:
            html += f"""
        <div class="gif-banner">
          <img src="{gif_trend_url}" alt="Trend Analysis"
               style="width: 240px; height: auto;" title="Trend Analysis Animation" />
        </div>"""

        for sr in summary_results:
            html += f"""<div style="margin:20px 0;padding:15px;background:#fafbfc;
                         border-radius:8px;border:1px solid #eee;">
                <h3 style="color:#2c3e50;margin-top:0;">
                    {sr['name'].replace('_', ' ').title()}</h3>
                <p style="color:#666;font-size:12px;">{sr.get('description', '')}</p>"""

            if sr["status"] == STATUS_FAIL:
                html += f'<p style="color:#dc3545;">Query failed: {sr["detail"]}</p>'
            else:
                # Data table (clean, no deviation columns)
                if sr["data"]:
                    html += '<table style="margin-bottom:15px;"><thead><tr>'
                    for h in sr["headers"]:
                        html += f"<th>{h}</th>"
                    html += "</tr></thead><tbody>"

                    for row in sr["data"][:50]:
                        html += "<tr>"
                        for h in sr["headers"]:
                            cell_val = row.get(h, "")
                            html += f"<td>{cell_val}</td>"
                        html += "</tr>"
                    html += "</tbody></table>"

                # Chart (animated GIF if Pillow available)
                if sr.get("chart") and sr["data"]:
                    chart_cfg_copy = dict(sr["chart"])
                    try:
                        chart_b64 = generate_chart_base64(
                            chart_cfg_copy, sr["headers"], sr["data"])
                    except Exception as chart_exc:
                        logger.error("Chart generation error for '%s': %s",
                                     sr["name"], chart_exc)
                        chart_b64 = None
                    if chart_b64:
                        mime = chart_cfg_copy.get("_mime", "image/png")
                        html += f"""<div class="chart-container">
                            <img src="data:{mime};base64,{chart_b64}"
                                 alt="{sr['chart'].get('title', 'Chart')}" />
                            </div>"""
                    else:
                        # Fallback to HTML bar chart
                        if sr["chart"].get("type") == "bar":
                            html += generate_html_bar_chart(
                                sr["chart"], sr["headers"], sr["data"])
                        elif sr["chart"].get("type") == "pie":
                            # HTML fallback for pie chart
                            lbl_c = sr["chart"].get("label_column",
                                        sr["headers"][0] if sr["headers"] else "")
                            val_c = sr["chart"].get("value_column",
                                        sr["headers"][1] if len(sr["headers"]) > 1 else "")
                            pie_title = sr["chart"].get("title", "Pie Chart")
                            html += f'<h3 style="color:#2c3e50;">{pie_title}</h3>'
                            html += '<table><thead><tr>'
                            html += f'<th>{lbl_c}</th><th>{val_c}</th><th>%</th>'
                            html += '</tr></thead><tbody>'
                            pie_total = sum(
                                float(r.get(val_c, 0)) for r in sr["data"]
                                if _safe_float(r.get(val_c, 0)) is not None)
                            for row in sr["data"]:
                                lbl = row.get(lbl_c, "")
                                val = _safe_float(row.get(val_c, 0)) or 0
                                pct = (val / pie_total * 100) if pie_total else 0
                                html += f'<tr><td><b>{lbl}</b></td>'
                                html += f'<td style="text-align:right;">{val:,.0f}</td>'
                                html += f'<td style="text-align:right;">{pct:.1f}%</td></tr>'
                            html += '</tbody></table>'
                        else:
                            html += '<p style="color:#888;">(Chart requires matplotlib)</p>'

            html += "</div>"

    # ---- Footer ----
    total_queries = len(summary_results) + len(zscore_results) if summary_results else 0
    html += f"""
    <div class="footer">
        <p><b>Audit Log:</b> {audit_s3_path or 'N/A'}</p>
        <p><b>Athena Cost:</b> ${total_cost_usd:.4f} |
           <b>Queries:</b> {total_queries} |
           <b>Z-Score Checks:</b> {len(zscore_results)}</p>
        <p>Generated by Strands ETL Orchestrator v2.0 (Z-Score + Trends) &nbsp;|&nbsp;
           BIG DATA Team</p>
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
    # (unless glue_jobs or s3_folders are configured for end-of-batch checks)
    has_summary = bool(
        (summary_config and summary_config.get("summary", {}).get("queries"))
        or config.get("summary", {}).get("queries")
    )
    has_eob_checks = bool(
        config.get("summary", {}).get("glue_jobs")
        or config.get("summary", {}).get("s3_folders")
        or (summary_config and (
            summary_config.get("summary", {}).get("glue_jobs")
            or summary_config.get("summary", {}).get("s3_folders")
        ))
    )
    if run_mode == "summary" and not has_summary and not has_eob_checks:
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
    zscore_results = []
    glue_job_statuses = []
    s3_folder_results = []
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
        logger.info("SUMMARY MODE: End of Batch Summary")
        logger.info("-" * 50)
        # Use separate summary config if provided, else fall back to main config
        effective_summary_config = summary_config if summary_config else config
        effective_summary_section = effective_summary_config.get("summary", {})

        # ---- Phase 1: Glue Job Status Check ----
        glue_job_configs = effective_summary_section.get("glue_jobs", [])
        glue_job_statuses = []
        if glue_job_configs:
            logger.info("Checking %d Glue job(s) for today's runs...", len(glue_job_configs))
            glue_job_statuses = check_glue_jobs_today(glue_job_configs)
            # Mark overall FAIL if any job didn't run or failed
            for gj in glue_job_statuses:
                if gj["status"] in ("FAILED", "ERROR", "TIMEOUT"):
                    overall_status = STATUS_FAIL
                elif not gj["ran_today"]:
                    logger.warning("Glue job '%s' did not run today", gj["job_name"])

        # ---- Phase 2: S3 Folder Freshness Check ----
        s3_folder_configs = effective_summary_section.get("s3_folders", [])
        s3_folder_results = []
        if s3_folder_configs:
            logger.info("Checking %d S3 folder(s) for freshness...", len(s3_folder_configs))
            s3_folder_results = check_s3_folders_summary(s3_folder_configs)
            for sf in s3_folder_results:
                if sf["status"] == STATUS_FAIL:
                    overall_status = STATUS_FAIL

        # ---- Phase 3: Summary Queries (if configured) ----
        if effective_summary_section.get("queries"):
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

        # Run z-score analysis queries
        zscore_results = run_zscore_analysis(
            effective_summary_config,
            f"s3://{effective_summary_config.get('summary', {}).get('athena_output_bucket', '')}"
            f"/{effective_summary_config.get('summary', {}).get('athena_output_prefix', 'summary_results/')}",
            effective_summary_config.get("summary", {}).get("athena_workgroup", "primary"))
        if zscore_results:
            logger.info("Z-score analysis: %d queries executed", len(zscore_results))
            for zr in zscore_results:
                critical_count = sum(
                    1 for row in zr.get("data", []) if row.get("z_flag") == "CRITICAL")
                warn_count = sum(
                    1 for row in zr.get("data", []) if row.get("z_flag") == "WARNING")
                if critical_count or warn_count:
                    logger.warning(
                        "  Z-score '%s': %d CRITICAL, %d WARNING out of %d rows",
                        zr["name"], critical_count, warn_count, len(zr.get("data", [])))
                else:
                    logger.info("  Z-score '%s': all NORMAL (%d rows)",
                                zr["name"], len(zr.get("data", [])))

    else:
        logger.error("Unknown run_mode: %s", run_mode)
        sys.exit(1)

    # ================================================================
    # Audit Logging
    # ================================================================
    logger.info("-" * 50)
    logger.info("AUDIT LOGGING")
    logger.info("-" * 50)
    config_layer = orch_cfg.get("layer", None)
    audit_records = build_audit_records(
        run_id, run_date, stale_results, glue_results,
        validation_results, comparison_results, summary_results, overall_status,
        layer=config_layer)
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

    # Fetch today's audit summary (all runs, all layers) for the summary section
    audit_day_summary = None
    if run_mode == "summary" and audit_cfg.get("database") and audit_cfg.get("table"):
        try:
            val_cfg = config.get("validations", {})
            aud_output_bucket = val_cfg.get("athena_output_bucket", "")
            if not aud_output_bucket:
                _esc = summary_config if summary_config else config
                aud_output_bucket = _esc.get("summary", {}).get("athena_output_bucket", "")
            aud_output_location = f"s3://{aud_output_bucket}/audit_summary/"
            aud_workgroup = val_cfg.get("athena_workgroup", "primary")
            logger.info("Fetching today's audit summary from %s.%s",
                        audit_cfg["database"], audit_cfg["table"])
            audit_day_summary = fetch_audit_summary_for_today(
                audit_cfg, run_date, aud_output_location, aud_workgroup)
            if audit_day_summary:
                logger.info(
                    "Audit summary: %d runs, %d total checks, %d pass, %d fail, cost $%.4f",
                    audit_day_summary["totals"]["distinct_runs"],
                    audit_day_summary["totals"]["total"],
                    audit_day_summary["totals"]["pass"],
                    audit_day_summary["totals"]["fail"],
                    audit_day_summary["totals"]["cost_usd"],
                )
        except Exception as exc:
            logger.warning("Audit summary fetch failed (non-fatal): %s", exc)

    _effective_summary_cfg = {}
    if run_mode == "summary":
        _esc = summary_config if summary_config else config
        _effective_summary_cfg = _esc.get("summary", {})

    # Collect end-of-batch data (only populated in summary mode)
    _glue_job_statuses = glue_job_statuses if run_mode == "summary" else []
    _s3_folder_results = s3_folder_results if run_mode == "summary" else []

    html = generate_dashboard_html(
        run_id, run_date, run_mode, stale_results, glue_results,
        validation_results, comparison_results, summary_results,
        overall_status, audit_s3_path,
        run_duration_sec=run_duration_sec,
        total_cost_usd=total_cost_usd,
        summary_config=_effective_summary_cfg,
        audit_day_summary=audit_day_summary,
        zscore_results=zscore_results,
        glue_job_statuses=_glue_job_statuses,
        s3_folder_results=_s3_folder_results)
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
