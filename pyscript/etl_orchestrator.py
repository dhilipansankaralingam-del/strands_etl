###################################################################################################
# SCRIPT        : etl_orchestrator.py
# PURPOSE       : End-to-end ETL orchestration:
#                   1) S3 stale folder detection
#                   2) Glue job triggering for stale sources
#                   3) Post-load validation queries via Athena
#                   4) Audit logging (pipe-delimited files on S3, MSCK REPAIR ready)
#                   5) Dashboard HTML email on completion
#
# USAGE         : python etl_orchestrator.py --config s3://bucket/path/orchestrator_config.json
#                 python etl_orchestrator.py --config /local/path/orchestrator_config.json
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 01/2026       Dhilipan        Initial version
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
from datetime import datetime, timezone, timedelta
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
        # find latest object timestamp inside this subfolder
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
    """Get last modified timestamp of the main prefix itself (latest object directly under it)."""
    return _get_latest_modified(bucket, prefix)


def check_staleness(source, defaults):
    """
    Evaluate staleness for a single monitored source.

    Rules:
      - Has subfolders AND count > threshold         -> Stale
      - Has subfolders AND any subfolder updated > N hours ago -> Stale
      - No subfolders AND main prefix updated > M hours ago    -> Check with Source
      - Otherwise -> OK

    Returns dict with status, details, subfolder_count, etc.
    """
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
        "stale_subfolders": [],       # prefixes of individual stale subfolders
        "status": STATUS_OK,
        "detail": "",
        "checked_at": now.isoformat(),
    }

    if subfolder_count > 0:
        # Identify which individual subfolders are stale (>N hours old)
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

        # Rule 1: subfolder count exceeds threshold – all subfolders are stale
        if subfolder_count > subfolder_count_threshold:
            result["status"] = STATUS_STALE
            result["stale_subfolders"] = [sf["prefix"] for sf in subfolders]
            result["detail"] = (
                f"Subfolder count {subfolder_count} exceeds threshold "
                f"{subfolder_count_threshold} – processing all subfolders"
            )
            logger.warning("[%s] %s – %s", label, result["status"], result["detail"])
            return result

        # Rule 2: some subfolders have stale data
        if stale_sfs:
            result["status"] = STATUS_STALE
            result["stale_subfolders"] = stale_sfs
            result["detail"] = (
                f"{len(stale_sfs)} stale subfolder(s): "
                + "; ".join(stale_details)
            )
            logger.warning(
                "[%s] %s – %s", label, result["status"], result["detail"]
            )
            return result
    else:
        # Rule 3: no subfolders – check main prefix freshness
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
    """
    Start an AWS Glue job for a stale source and wait for completion.

    *stale_subfolders* is the list of S3 prefixes (inside the source prefix)
    that were detected as stale.  Only these folders will be processed and
    subsequently archived by trigger_glue.py.

    Returns dict with job run details and final status.
    """
    glue_cfg = source.get("glue_trigger", {})
    job_name = glue_cfg.get("job_name")
    if not job_name:
        logger.error("[%s] No glue_trigger.job_name configured – skipping", source["label"])
        return {
            "job_name": None,
            "job_run_id": None,
            "status": STATUS_SKIPPED,
            "detail": "No Glue job configured",
            "stale_subfolders_passed": [],
            "duration_seconds": 0,
        }

    # Comma-separated list of stale subfolder prefixes for the Glue job
    stale_folders_csv = ",".join(stale_subfolders) if stale_subfolders else ""

    arguments = {
        "--json_file_name": glue_cfg.get("config_file_key", ""),
        "--bucket_name": glue_cfg.get("config_file_bucket", ""),
        "--orchestrator_run_id": run_id,
        "--stale_folders": stale_folders_csv,
        "--archive_s3_path": glue_cfg.get("archive_s3_path", ""),
    }

    worker_type = glue_cfg.get("worker_type", "G.1X")
    num_workers = glue_cfg.get("workers", 10)
    timeout_min = glue_cfg.get("timeout_minutes", 120)

    logger.info(
        "[%s] Starting Glue job '%s' – workers=%s type=%s timeout=%smin",
        source["label"],
        job_name,
        num_workers,
        worker_type,
        timeout_min,
    )

    start_time = time.time()
    try:
        response = glue_client().start_job_run(
            JobName=job_name,
            Arguments=arguments,
            WorkerType=worker_type,
            NumberOfWorkers=num_workers,
            Timeout=timeout_min,
        )
        job_run_id = response["JobRunId"]
    except Exception as exc:
        logger.error("[%s] Failed to start Glue job: %s", source["label"], exc)
        return {
            "job_name": job_name,
            "job_run_id": None,
            "status": STATUS_FAIL,
            "detail": str(exc),
            "duration_seconds": 0,
        }

    logger.info("[%s] Glue job run started: %s", source["label"], job_run_id)

    # Poll until terminal state
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

    logger.info(
        "[%s] Glue job %s finished – state=%s duration=%.0fs",
        source["label"],
        job_run_id,
        state,
        duration,
    )

    return {
        "job_name": job_name,
        "job_run_id": job_run_id,
        "status": status,
        "glue_state": state,
        "detail": f"Glue state: {state}",
        "stale_subfolders_passed": stale_subfolders,
        "duration_seconds": round(duration),
    }


# ============================================================================
# 4. VALIDATION QUERIES (Athena)
# ============================================================================
def run_athena_query(query, database, output_location, workgroup="primary"):
    """Execute a single Athena query and return the query execution result."""
    client = athena_client()
    start_resp = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    query_id = start_resp["QueryExecutionId"]

    # Wait for query to finish
    while True:
        time.sleep(5)
        status_resp = client.get_query_execution(QueryExecutionId=query_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

    result = {
        "query_id": query_id,
        "state": state,
        "rows": [],
    }

    if state == "SUCCEEDED":
        try:
            data_resp = client.get_query_results(QueryExecutionId=query_id, MaxResults=100)
            rows = data_resp.get("ResultSet", {}).get("Rows", [])
            result["rows"] = rows
        except Exception as exc:
            logger.warning("Could not fetch results for query %s: %s", query_id, exc)

    return result


def evaluate_condition(condition, rows):
    """
    Evaluate a simple condition string against Athena result rows.

    Supports patterns like:
      - 'cnt > 0'       (check first data row, first column)
      - 'row_count == 0' (compare actual row count minus header)
      - 'null_cnt == 0'
      - 'hours_since < 24'

    Returns (passed: bool, actual_value).
    """
    if not rows or len(rows) < 2:
        # Only header row or empty – treat as no data
        if "row_count" in condition and "== 0" in condition:
            return True, 0
        return False, None

    # first row is header, second row is data
    header = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
    data = [col.get("VarCharValue", "") for col in rows[1].get("Data", [])]
    data_row_count = len(rows) - 1  # minus header

    # Build a context dict for evaluation
    ctx = {"row_count": data_row_count}
    for h, v in zip(header, data):
        try:
            ctx[h] = float(v)
        except (ValueError, TypeError):
            ctx[h] = v

    try:
        passed = bool(eval(condition, {"__builtins__": {}}, ctx))  # noqa: S307
        # determine which variable is referenced to return its value
        for key in ctx:
            if key in condition and key != "row_count":
                return passed, ctx.get(key)
        return passed, ctx.get("row_count", None)
    except Exception as exc:
        logger.warning("Condition evaluation failed: %s – %s", condition, exc)
        return False, None


def _execute_single_validation(qdef, output_location, workgroup):
    """
    Run one Athena validation query and return a result dict.
    Designed to be called from both sequential and parallel paths.
    """
    query_str = qdef["query"]
    database = qdef.get("database", "default")
    condition = qdef.get("condition", "")

    logger.info("Running validation: %s", qdef["name"])

    try:
        athena_result = run_athena_query(
            query_str, database, output_location, workgroup
        )
    except Exception as exc:
        logger.error("Athena query failed for %s: %s", qdef["name"], exc)
        return {
            "name": qdef["name"],
            "description": qdef.get("description", ""),
            "check_type": qdef.get("check_type", ""),
            "abort_on_failure": qdef.get("abort_on_failure", False),
            "status": STATUS_FAIL,
            "detail": str(exc),
            "actual_value": None,
            "query": query_str,
        }

    if athena_result["state"] != "SUCCEEDED":
        status = STATUS_FAIL
        detail = f"Athena query state: {athena_result['state']}"
        actual = None
    else:
        passed, actual = evaluate_condition(condition, athena_result["rows"])
        status = STATUS_PASS if passed else STATUS_FAIL
        detail = f"Condition '{condition}' -> {'met' if passed else 'not met'} (actual={actual})"

    logger.info("Validation %s: %s – %s", qdef["name"], status, detail)

    return {
        "name": qdef["name"],
        "description": qdef.get("description", ""),
        "check_type": qdef.get("check_type", ""),
        "abort_on_failure": qdef.get("abort_on_failure", False),
        "status": status,
        "detail": detail,
        "actual_value": actual,
        "query": query_str,
    }


def run_validations(config, stale_results):
    """
    Execute all validation queries from config.

    Supports two execution modes (set in config.validations.execution_mode):
      - "sequential": run one at a time; abort immediately on critical failure.
      - "parallel":   submit all queries concurrently via ThreadPoolExecutor,
                      then evaluate abort_on_failure after all finish.

    Returns (list of validation result dicts, aborted: bool).
    """
    val_cfg = config.get("validations", {})
    queries = val_cfg.get("queries", [])
    execution_mode = val_cfg.get("execution_mode", "sequential")
    max_parallel = val_cfg.get("max_parallel", 5)
    output_bucket = val_cfg.get("athena_output_bucket", "")
    output_prefix = val_cfg.get("athena_output_prefix", "query_results/")
    workgroup = val_cfg.get("athena_workgroup", "primary")
    output_location = f"s3://{output_bucket}/{output_prefix}"

    results = []
    abort = False

    if execution_mode == "parallel":
        # -- Parallel execution ------------------------------------------------
        logger.info(
            "Running %d validations in PARALLEL (max_parallel=%d)",
            len(queries), max_parallel,
        )
        futures = {}
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            for qdef in queries:
                future = executor.submit(
                    _execute_single_validation, qdef, output_location, workgroup,
                )
                futures[future] = qdef

            for future in as_completed(futures):
                vr = future.result()
                results.append(vr)
                if vr["status"] == STATUS_FAIL and vr.get("abort_on_failure"):
                    abort = True
                    logger.error(
                        "ABORT: Validation '%s' failed and abort_on_failure=true",
                        vr["name"],
                    )

        # Sort results back to config order for deterministic reporting
        query_order = {q["name"]: i for i, q in enumerate(queries)}
        results.sort(key=lambda r: query_order.get(r["name"], 999))

    else:
        # -- Sequential execution ----------------------------------------------
        logger.info("Running %d validations SEQUENTIALLY", len(queries))
        for qdef in queries:
            if abort:
                results.append({
                    "name": qdef["name"],
                    "description": qdef.get("description", ""),
                    "check_type": qdef.get("check_type", ""),
                    "status": STATUS_ABORTED,
                    "detail": "Aborted due to prior validation failure",
                    "actual_value": None,
                    "query": qdef["query"],
                })
                continue

            vr = _execute_single_validation(qdef, output_location, workgroup)
            results.append(vr)

            if vr["status"] == STATUS_FAIL and vr.get("abort_on_failure"):
                abort = True
                logger.error(
                    "ABORT: Validation '%s' failed and abort_on_failure=true",
                    vr["name"],
                )

    # Strip internal key before returning
    for r in results:
        r.pop("abort_on_failure", None)

    return results, abort


# ============================================================================
# 5. AUDIT LOGGING
# ============================================================================
def build_audit_records(run_id, run_date, stale_results, glue_results, validation_results, overall_status):
    """Flatten all events into a list of audit record dicts."""
    records = []
    ts = datetime.now(timezone.utc).isoformat()

    # Stale check events
    for sr in stale_results:
        records.append(OrderedDict([
            ("run_id", run_id),
            ("run_date", run_date),
            ("event_timestamp", ts),
            ("event_type", "STALE_CHECK"),
            ("source_label", sr["label"]),
            ("source_bucket", sr["bucket"]),
            ("source_prefix", sr["prefix"]),
            ("subfolder_count", str(sr["subfolder_count"])),
            ("status", sr["status"]),
            ("detail", sr["detail"]),
            ("glue_job_name", ""),
            ("glue_job_run_id", ""),
            ("glue_duration_sec", ""),
            ("validation_name", ""),
            ("check_type", ""),
            ("actual_value", ""),
            ("query", ""),
            ("overall_status", overall_status),
        ]))

    # Glue job events
    for label, gr in glue_results.items():
        records.append(OrderedDict([
            ("run_id", run_id),
            ("run_date", run_date),
            ("event_timestamp", ts),
            ("event_type", "GLUE_TRIGGER"),
            ("source_label", label),
            ("source_bucket", ""),
            ("source_prefix", ""),
            ("subfolder_count", ""),
            ("status", gr["status"]),
            ("detail", gr.get("detail", "")),
            ("glue_job_name", gr.get("job_name", "")),
            ("glue_job_run_id", gr.get("job_run_id", "")),
            ("glue_duration_sec", str(gr.get("duration_seconds", ""))),
            ("validation_name", ""),
            ("check_type", ""),
            ("actual_value", ""),
            ("query", ""),
            ("overall_status", overall_status),
        ]))

    # Validation events
    for vr in validation_results:
        records.append(OrderedDict([
            ("run_id", run_id),
            ("run_date", run_date),
            ("event_timestamp", ts),
            ("event_type", "VALIDATION"),
            ("source_label", ""),
            ("source_bucket", ""),
            ("source_prefix", ""),
            ("subfolder_count", ""),
            ("status", vr["status"]),
            ("detail", vr["detail"]),
            ("glue_job_name", ""),
            ("glue_job_run_id", ""),
            ("glue_duration_sec", ""),
            ("validation_name", vr["name"]),
            ("check_type", vr.get("check_type", "")),
            ("actual_value", str(vr.get("actual_value", ""))),
            ("query", vr.get("query", "")),
            ("overall_status", overall_status),
        ]))

    return records


def write_audit_to_s3(records, audit_cfg, run_date):
    """
    Write audit records as a pipe-delimited file to S3, partitioned by run_date.
    The file is compatible with MSCK REPAIR TABLE for Hive/Athena.
    """
    if not records:
        logger.info("No audit records to write.")
        return None

    bucket = audit_cfg["s3_bucket"]
    prefix = audit_cfg["s3_prefix"].rstrip("/")
    file_id = uuid.uuid4().hex[:8]
    key = f"{prefix}/run_date={run_date}/audit_{file_id}.csv"

    # Header
    header = "|".join(records[0].keys())
    lines = [header]
    for rec in records:
        # Escape pipes in values
        line = "|".join(str(v).replace("|", "\\|") for v in rec.values())
        lines.append(line)

    body = "\n".join(lines)

    s3_client().put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    s3_path = f"s3://{bucket}/{key}"
    logger.info("Audit log written to %s (%d records)", s3_path, len(records))
    return s3_path


def run_msck_repair(audit_cfg):
    """Run MSCK REPAIR TABLE on the audit table so new partitions are discovered."""
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
# 6. DASHBOARD HTML EMAIL
# ============================================================================
def _status_color(status):
    """Return an HTML colour for a given status."""
    return {
        STATUS_OK: "#28a745",
        STATUS_STALE: "#dc3545",
        STATUS_CHECK_SOURCE: "#fd7e14",
        STATUS_PASS: "#28a745",
        STATUS_FAIL: "#dc3545",
        STATUS_SKIPPED: "#6c757d",
        STATUS_ABORTED: "#6c757d",
    }.get(status, "#333333")


def generate_dashboard_html(
    run_id, run_date, stale_results, glue_results, validation_results, overall_status, audit_s3_path
):
    """Generate a full dashboard HTML email body."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    overall_color = "#28a745" if overall_status == STATUS_PASS else "#dc3545"

    # -- Summary counts --
    total_sources = len(stale_results)
    stale_count = sum(1 for r in stale_results if r["status"] == STATUS_STALE)
    check_source_count = sum(1 for r in stale_results if r["status"] == STATUS_CHECK_SOURCE)
    ok_count = sum(1 for r in stale_results if r["status"] == STATUS_OK)
    glue_pass = sum(1 for g in glue_results.values() if g["status"] == STATUS_PASS)
    glue_fail = sum(1 for g in glue_results.values() if g["status"] == STATUS_FAIL)
    val_pass = sum(1 for v in validation_results if v["status"] == STATUS_PASS)
    val_fail = sum(1 for v in validation_results if v["status"] == STATUS_FAIL)
    val_abort = sum(1 for v in validation_results if v["status"] == STATUS_ABORTED)

    # -- Build HTML --
    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; color: #333; }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #2c3e50; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        .summary-box {{
            display: inline-block; padding: 15px 25px; margin: 5px 10px 5px 0;
            border-radius: 8px; color: white; font-size: 16px; font-weight: bold;
            text-align: center; min-width: 120px;
        }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 14px; }}
        th {{ background-color: #2c3e50; color: white; padding: 10px 12px; text-align: left; }}
        td {{ padding: 8px 12px; border-bottom: 1px solid #ddd; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .badge {{
            display: inline-block; padding: 4px 10px; border-radius: 4px;
            color: white; font-weight: bold; font-size: 12px;
        }}
        .footer {{ margin-top: 30px; font-size: 12px; color: #888; }}
    </style>
    </head>
    <body>

    <h1>Strands ETL Orchestrator Dashboard</h1>
    <p><b>Run ID:</b> {run_id} &nbsp;|&nbsp; <b>Date:</b> {run_date} &nbsp;|&nbsp;
       <b>Generated:</b> {now_str}</p>

    <div style="margin: 15px 0;">
        <span class="summary-box" style="background-color: {overall_color};">
            Overall: {overall_status}
        </span>
        <span class="summary-box" style="background-color: #17a2b8;">
            Sources: {total_sources}
        </span>
        <span class="summary-box" style="background-color: {'#dc3545' if stale_count else '#28a745'};">
            Stale: {stale_count}
        </span>
        <span class="summary-box" style="background-color: {'#fd7e14' if check_source_count else '#28a745'};">
            Check Source: {check_source_count}
        </span>
    </div>

    <!-- ======== SECTION 1: STALE DETECTION ======== -->
    <h2>1. S3 Stale File Detection</h2>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Source Label</th>
                <th>Bucket / Prefix</th>
                <th>Subfolders</th>
                <th>Status</th>
                <th>Detail</th>
            </tr>
        </thead>
        <tbody>
    """

    for i, sr in enumerate(stale_results, 1):
        color = _status_color(sr["status"])
        html += f"""
            <tr>
                <td>{i}</td>
                <td><b>{sr['label']}</b></td>
                <td>{sr['bucket']}/{sr['prefix']}</td>
                <td style="text-align:center;">{sr['subfolder_count']}</td>
                <td><span class="badge" style="background-color:{color};">{sr['status']}</span></td>
                <td>{sr['detail'] or '-'}</td>
            </tr>"""

    html += """
        </tbody>
    </table>

    <!-- ======== SECTION 2: GLUE JOB TRIGGERS ======== -->
    <h2>2. Glue Job Triggers</h2>
    """

    if glue_results:
        html += """
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Source Label</th>
                <th>Glue Job</th>
                <th>Run ID</th>
                <th>Status</th>
                <th>Duration</th>
                <th>Detail</th>
            </tr>
        </thead>
        <tbody>"""

        for i, (label, gr) in enumerate(glue_results.items(), 1):
            color = _status_color(gr["status"])
            dur = f"{gr.get('duration_seconds', 0)}s" if gr.get("duration_seconds") else "-"
            html += f"""
            <tr>
                <td>{i}</td>
                <td><b>{label}</b></td>
                <td>{gr.get('job_name', '-')}</td>
                <td>{gr.get('job_run_id', '-') or '-'}</td>
                <td><span class="badge" style="background-color:{color};">{gr['status']}</span></td>
                <td>{dur}</td>
                <td>{gr.get('detail', '-')}</td>
            </tr>"""

        html += """
        </tbody>
    </table>"""
    else:
        html += "<p>No Glue jobs were triggered (all sources OK).</p>"

    # -- Section 3: Validations --
    html += """
    <!-- ======== SECTION 3: VALIDATION QUERIES ======== -->
    <h2>3. Post-Load Validations</h2>
    """

    if validation_results:
        html += """
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Validation Name</th>
                <th>Check Type</th>
                <th>Status</th>
                <th>Actual Value</th>
                <th>Detail</th>
            </tr>
        </thead>
        <tbody>"""

        for i, vr in enumerate(validation_results, 1):
            color = _status_color(vr["status"])
            html += f"""
            <tr>
                <td>{i}</td>
                <td><b>{vr['name']}</b><br><small>{vr.get('description', '')}</small></td>
                <td>{vr.get('check_type', '-')}</td>
                <td><span class="badge" style="background-color:{color};">{vr['status']}</span></td>
                <td>{vr.get('actual_value', '-')}</td>
                <td>{vr.get('detail', '-')}</td>
            </tr>"""

        html += """
        </tbody>
    </table>"""
    else:
        html += "<p>No validation queries configured.</p>"

    # -- Footer --
    html += f"""
    <hr style="margin-top:30px;">
    <div class="footer">
        <p><b>Audit Log:</b> {audit_s3_path or 'N/A'}</p>
        <p>Generated by Strands ETL Orchestrator v1.0 &nbsp;|&nbsp; BIG DATA Team</p>
    </div>

    </body>
    </html>
    """
    return html


def send_email(html_content, email_cfg, overall_status):
    """Send the dashboard HTML email via SMTP."""
    sender = email_cfg["sender"]
    recipients = email_cfg["recipients"]
    subject = f"{email_cfg.get('subject_prefix', 'ETL Dashboard')} – {overall_status} – {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

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
# 7. MAIN ORCHESTRATOR
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Strands ETL Orchestrator")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to orchestrator_config.json (local or s3://...)",
    )
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:12]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("=" * 70)
    logger.info("STRANDS ETL ORCHESTRATOR – Run ID: %s  Date: %s", run_id, run_date)
    logger.info("=" * 70)

    # ---- Load config ----
    config = load_config(args.config)
    orch_cfg = config.get("orchestrator", {})
    mon_cfg = config.get("s3_monitoring", {})
    defaults = mon_cfg.get("defaults", {})
    sources = mon_cfg.get("sources", [])
    audit_cfg = config.get("audit", {})
    email_cfg = config.get("email", {})

    # run_mode: "full_pipeline" (default) or "validation_only"
    run_mode = orch_cfg.get("run_mode", "full_pipeline")
    logger.info("Run mode: %s", run_mode)

    overall_status = STATUS_PASS
    stale_results = []
    stale_pairs = []
    stale_sources = []
    glue_results = {}

    if run_mode == "full_pipeline":
        # ============================================================
        # PHASE 1: S3 Stale Detection
        # ============================================================
        logger.info("-" * 50)
        logger.info("PHASE 1: S3 Stale Detection")
        logger.info("-" * 50)

        for source in sources:
            result = check_staleness(source, defaults)
            stale_results.append(result)

        # Pair each source config with its stale subfolder prefixes
        stale_pairs = [
            (s, r["stale_subfolders"])
            for s, r in zip(sources, stale_results)
            if r["status"] == STATUS_STALE
        ]
        stale_sources = [pair[0] for pair in stale_pairs]
        check_source_items = [r for r in stale_results if r["status"] == STATUS_CHECK_SOURCE]

        if check_source_items:
            logger.warning(
                "%d source(s) flagged as 'Check with Source' – email will include alert",
                len(check_source_items),
            )

        # ============================================================
        # PHASE 2: Trigger Glue Jobs for Stale Sources
        # ============================================================
        logger.info("-" * 50)
        logger.info("PHASE 2: Trigger Glue Jobs (%d stale sources)", len(stale_sources))
        logger.info("-" * 50)

        for source, stale_sfs in stale_pairs:
            label = source.get("label", source["prefix"])
            logger.info(
                "[%s] Passing %d stale subfolder(s) to Glue: %s",
                label, len(stale_sfs), stale_sfs,
            )
            gr = trigger_glue_job(source, stale_sfs, run_id)
            glue_results[label] = gr
            if gr["status"] == STATUS_FAIL:
                overall_status = STATUS_FAIL
                logger.error("[%s] Glue job FAILED – marking overall as FAIL", label)
    else:
        logger.info("PHASE 1 & 2 SKIPPED (run_mode=%s)", run_mode)

    # ================================================================
    # PHASE 3: Validation Queries
    # ================================================================
    logger.info("-" * 50)
    logger.info("PHASE 3: Post-Load Validation Queries")
    logger.info("-" * 50)

    validation_results = []
    aborted = False

    if run_mode == "validation_only":
        # In validation_only mode, always run all validations
        logger.info("validation_only mode – running all configured validations")
        validation_results, aborted = run_validations(config, stale_results)
        if aborted:
            overall_status = STATUS_FAIL
            logger.error("Validation phase ABORTED due to critical failure")
    elif stale_sources and overall_status != STATUS_FAIL:
        validation_results, aborted = run_validations(config, stale_results)
        if aborted:
            overall_status = STATUS_FAIL
            logger.error("Validation phase ABORTED due to critical failure")
    elif not stale_sources:
        logger.info("No stale sources detected – skipping validations")
    else:
        logger.warning("Skipping validations because Glue phase had failures")

    # ================================================================
    # PHASE 4: Audit Logging
    # ================================================================
    logger.info("-" * 50)
    logger.info("PHASE 4: Audit Logging")
    logger.info("-" * 50)

    audit_records = build_audit_records(
        run_id, run_date, stale_results, glue_results, validation_results, overall_status
    )
    audit_s3_path = None
    try:
        audit_s3_path = write_audit_to_s3(audit_records, audit_cfg, run_date)
        run_msck_repair(audit_cfg)
    except Exception as exc:
        logger.error("Audit write failed (non-fatal): %s", exc)

    # ================================================================
    # PHASE 5: Dashboard Email
    # ================================================================
    logger.info("-" * 50)
    logger.info("PHASE 5: Dashboard Email")
    logger.info("-" * 50)

    html = generate_dashboard_html(
        run_id, run_date, stale_results, glue_results, validation_results, overall_status, audit_s3_path
    )
    send_email(html, email_cfg, overall_status)

    # ---- Final Summary ----
    logger.info("=" * 70)
    logger.info(
        "ORCHESTRATOR COMPLETE – Overall: %s | Stale: %d | Check Source: %d | "
        "Glue Pass: %d Fail: %d | Validations Pass: %d Fail: %d Aborted: %d",
        overall_status,
        sum(1 for r in stale_results if r["status"] == STATUS_STALE),
        sum(1 for r in stale_results if r["status"] == STATUS_CHECK_SOURCE),
        sum(1 for g in glue_results.values() if g["status"] == STATUS_PASS),
        sum(1 for g in glue_results.values() if g["status"] == STATUS_FAIL),
        sum(1 for v in validation_results if v["status"] == STATUS_PASS),
        sum(1 for v in validation_results if v["status"] == STATUS_FAIL),
        sum(1 for v in validation_results if v["status"] == STATUS_ABORTED),
    )
    logger.info("=" * 70)

    # Exit non-zero if overall failed (useful for schedulers)
    if overall_status != STATUS_PASS:
        sys.exit(1)


if __name__ == "__main__":
    main()
