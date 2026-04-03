#!/usr/bin/env python3
"""
Strands ETL Pipeline — Enterprise-grade SQL pipeline executor using AWS Athena.

Loads single or multiple Iceberg tables from various sources (tables, files)
with SQL transformations, step dependencies, retry, rollback, audit logging,
cost tracking, and HTML email reporting.

Usage:
    python etl_pipeline.py --config pipeline_config.json
    python etl_pipeline.py --config s3://bucket/config.json --dry-run
    python etl_pipeline.py --config pipeline_config.json --resume-from step_name
    python etl_pipeline.py --config pipeline_config.json --run-date 2026-04-01
"""

import boto3
import json
import sys
import os
import time
import uuid
import argparse
import logging
import hashlib
import re
import smtplib
from datetime import datetime, timezone, timedelta
from collections import OrderedDict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_pipeline")

# ============================================================================
# CONSTANTS
# ============================================================================
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_SKIPPED = "SKIPPED"
STATUS_ABORTED = "ABORTED"
STATUS_WARN = "WARN"
STATUS_ROLLBACK = "ROLLBACK"
STATUS_NOT_EXECUTED = "NOT_EXECUTED"

ATHENA_COST_PER_TB_SCANNED = 5.00  # USD
DEFAULT_RETRY_ATTEMPTS = 2  # retry twice (3 total attempts)
DEFAULT_RETRY_BACKOFF_SEC = 10
DEFAULT_POLL_INTERVAL_SEC = 5
DEFAULT_TIMEOUT_MINUTES = 60

STEP_TYPES = {"sql", "ctas", "insert", "merge", "drop", "validation",
              "optimize", "snapshot"}

# ============================================================================
# AWS CLIENT LAZY INIT
# ============================================================================
_s3 = None
_athena = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def athena_client():
    global _athena
    if _athena is None:
        _athena = boto3.client("athena")
    return _athena


# ============================================================================
# 1. CONFIG LOADING & VARIABLE SUBSTITUTION
# ============================================================================
def load_config(config_path):
    """Load pipeline config from a local path or S3 URI."""
    if config_path.startswith("s3://"):
        parts = config_path.replace("s3://", "").split("/", 1)
        bucket, key = parts[0], parts[1]
        resp = s3_client().get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    else:
        with open(config_path, "r") as f:
            return json.load(f)


def resolve_variables(text, variables):
    """Replace {var_name} placeholders in text with variable values."""
    if not text or not variables:
        return text
    result = text
    for key, val in variables.items():
        result = result.replace(f"{{{key}}}", str(val))
    return result


def build_variables(pipeline_cfg, run_id, run_date):
    """Build the variable dict from config + system variables."""
    today = datetime.strptime(run_date, "%Y-%m-%d")
    variables = {}

    # System variables
    variables["run_id"] = run_id
    variables["run_date"] = run_date
    variables["CURRENT_DATE"] = run_date

    # Date arithmetic: {CURRENT_DATE-N}
    for n in range(1, 31):
        d = today - timedelta(days=n)
        variables[f"CURRENT_DATE-{n}"] = d.strftime("%Y-%m-%d")
        variables[f"CURRENT_DATE+{n}"] = (today + timedelta(days=n)).strftime("%Y-%m-%d")

    # Config variables (override system)
    for key, val in pipeline_cfg.get("variables", {}).items():
        resolved = resolve_variables(str(val), variables)
        variables[key] = resolved

    return variables


# ============================================================================
# 2. ATHENA QUERY EXECUTION
# ============================================================================
def _run_athena_query_once(query, database, output_location, workgroup="primary",
                           timeout_minutes=DEFAULT_TIMEOUT_MINUTES):
    """Execute a single Athena query and poll until complete."""
    client = athena_client()
    start_time = time.time()
    timeout_sec = timeout_minutes * 60

    try:
        start_resp = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )
    except Exception as exc:
        return {
            "query_id": "",
            "state": "FAILED",
            "rows": [],
            "data_scanned_bytes": 0,
            "exec_time_ms": 0,
            "cost_usd": 0.0,
            "failure_reason": f"start_query_execution failed: {exc}",
            "duration_sec": round(time.time() - start_time, 2),
        }

    query_id = start_resp["QueryExecutionId"]
    logger.info("  Athena query started: %s", query_id)

    # Poll for completion
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_sec:
            # Try to cancel
            try:
                client.stop_query_execution(QueryExecutionId=query_id)
            except Exception:
                pass
            return {
                "query_id": query_id,
                "state": "TIMEOUT",
                "rows": [],
                "data_scanned_bytes": 0,
                "exec_time_ms": int(elapsed * 1000),
                "cost_usd": 0.0,
                "failure_reason": f"Query timed out after {timeout_minutes} minutes",
                "duration_sec": round(elapsed, 2),
            }

        time.sleep(DEFAULT_POLL_INTERVAL_SEC)
        status_resp = client.get_query_execution(QueryExecutionId=query_id)
        qe = status_resp["QueryExecution"]
        state = qe["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

    elapsed = time.time() - start_time
    stats = qe.get("Statistics", {})
    data_scanned_bytes = stats.get("DataScannedInBytes", 0)
    exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)
    cost_usd = round((data_scanned_bytes / (1024 ** 4)) * ATHENA_COST_PER_TB_SCANNED, 6)

    failure_reason = ""
    if state != "SUCCEEDED":
        failure_reason = qe.get("Status", {}).get("StateChangeReason", "Unknown error")

    result = {
        "query_id": query_id,
        "state": state,
        "rows": [],
        "data_scanned_bytes": data_scanned_bytes,
        "exec_time_ms": exec_time_ms,
        "cost_usd": cost_usd,
        "failure_reason": failure_reason,
        "duration_sec": round(elapsed, 2),
    }

    if state == "SUCCEEDED":
        try:
            data_resp = client.get_query_results(
                QueryExecutionId=query_id, MaxResults=1000)
            result["rows"] = data_resp.get("ResultSet", {}).get("Rows", [])
        except Exception as exc:
            logger.warning("  Failed to fetch query results: %s", exc)

    return result


def run_athena_query(query, database, output_location, workgroup="primary",
                     timeout_minutes=DEFAULT_TIMEOUT_MINUTES,
                     max_retries=DEFAULT_RETRY_ATTEMPTS):
    """Execute an Athena query with retry logic (default: 2 retries = 3 total attempts)."""
    total_cost = 0.0
    last_result = None

    for attempt in range(1, max_retries + 2):  # +2 because range is exclusive
        if attempt > 1:
            backoff = DEFAULT_RETRY_BACKOFF_SEC * attempt
            logger.warning("  Retry attempt %d/%d (backoff %ds)...",
                           attempt, max_retries + 1, backoff)
            time.sleep(backoff)

        result = _run_athena_query_once(
            query, database, output_location, workgroup, timeout_minutes)
        total_cost += result.get("cost_usd", 0)
        result["attempt"] = attempt
        last_result = result

        if result["state"] == "SUCCEEDED":
            result["cost_usd"] = round(total_cost, 6)
            result["total_attempts"] = attempt
            return result

        logger.warning("  Query %s failed on attempt %d: %s – %s",
                       result.get("query_id", "?"), attempt,
                       result["state"], result["failure_reason"])

    # All attempts exhausted
    last_result["cost_usd"] = round(total_cost, 6)
    last_result["total_attempts"] = max_retries + 1
    return last_result


def parse_athena_rows(rows):
    """Convert Athena result rows into (headers, data_dicts)."""
    if not rows:
        return [], []
    header = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
    data = []
    for row in rows[1:]:
        values = [col.get("VarCharValue", "") for col in row.get("Data", [])]
        data.append(dict(zip(header, values)))
    return header, data


def extract_scalar(rows, column):
    """Extract a single numeric value from the first data row."""
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
    """Evaluate a condition string against Athena result rows.
    Examples: 'cnt > 0', 'row_count == 0', 'dup_cnt == 0'."""
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
        passed = bool(eval(condition, {"__builtins__": {}}, ctx))
        actual = None
        for key in ctx:
            if key in condition and key != "row_count":
                actual = ctx.get(key)
                break
        if actual is None:
            actual = ctx.get("row_count")
        return passed, actual
    except Exception as exc:
        logger.warning("  Condition evaluation failed: %s – %s", condition, exc)
        return False, None


def sql_hash(sql_text):
    """Return MD5 hash of a SQL statement for tracking."""
    return hashlib.md5(sql_text.strip().encode("utf-8")).hexdigest()[:12]


# ============================================================================
# 3. ICEBERG SNAPSHOT OPERATIONS
# ============================================================================
def capture_iceberg_snapshot(table_fqn, database, output_location, workgroup):
    """Capture current Iceberg snapshot ID for a table.
    Returns snapshot_id (str) or None on failure."""
    # table_fqn format: "db.table_name"
    parts = table_fqn.split(".")
    if len(parts) == 2:
        db, tbl = parts
    else:
        db = database
        tbl = table_fqn

    query = (
        f'SELECT snapshot_id FROM "{db}"."{tbl}$snapshots" '
        f"ORDER BY committed_at DESC LIMIT 1"
    )
    logger.info("  Capturing snapshot for %s.%s ...", db, tbl)
    result = run_athena_query(query, db, output_location, workgroup,
                              timeout_minutes=5, max_retries=1)
    if result["state"] == "SUCCEEDED" and result["rows"]:
        header, data = parse_athena_rows(result["rows"])
        if data:
            snap_id = data[0].get("snapshot_id", "")
            logger.info("  Snapshot captured: %s", snap_id)
            return snap_id
    logger.warning("  Could not capture snapshot for %s.%s", db, tbl)
    return None


def rollback_iceberg_to_snapshot(table_fqn, snapshot_id, database,
                                 output_location, workgroup):
    """Rollback an Iceberg table to a previous snapshot.
    Uses ALTER TABLE ... SET TBLPROPERTIES for Athena compatibility."""
    if not snapshot_id:
        logger.warning("  No snapshot_id to rollback to for %s", table_fqn)
        return False

    # Athena/Iceberg rollback via procedure call (Athena v3+)
    parts = table_fqn.split(".")
    if len(parts) == 2:
        db, tbl = parts
    else:
        db = database
        tbl = table_fqn

    # Try the system procedure first (Athena v3)
    rollback_sql = (
        f"ALTER TABLE {db}.{tbl} SET TBLPROPERTIES "
        f"('rollback_to_snapshot' = '{snapshot_id}')"
    )
    logger.info("  Rolling back %s.%s to snapshot %s ...", db, tbl, snapshot_id)
    result = run_athena_query(rollback_sql, db, output_location, workgroup,
                              timeout_minutes=10, max_retries=1)
    if result["state"] == "SUCCEEDED":
        logger.info("  Rollback succeeded for %s.%s", db, tbl)
        return True
    else:
        logger.error("  Rollback FAILED for %s.%s: %s",
                     db, tbl, result["failure_reason"])
        return False


# ============================================================================
# 4. STEP DEPENDENCY RESOLUTION
# ============================================================================
def resolve_execution_order(steps):
    """Topological sort of steps based on depends_on.
    Returns ordered list of step dicts. Raises on circular dependency."""
    name_map = {s["name"]: s for s in steps}
    visited = set()
    order = []
    visiting = set()

    def visit(name):
        if name in visiting:
            raise ValueError(f"Circular dependency detected involving step: {name}")
        if name in visited:
            return
        visiting.add(name)
        step = name_map.get(name)
        if not step:
            raise ValueError(f"Step '{name}' referenced in depends_on but not defined")
        for dep in step.get("depends_on", []):
            visit(dep)
        visiting.discard(name)
        visited.add(name)
        order.append(step)

    for s in steps:
        visit(s["name"])

    return order


# ============================================================================
# 5. VALIDATION RUNNER
# ============================================================================
def run_validations(validations, variables, default_database, output_location,
                    workgroup, phase="PRE"):
    """Run a list of validation checks (pre or post).
    Returns list of result dicts and overall pass/fail."""
    results = []
    all_pass = True

    for idx, v in enumerate(validations, 1):
        name = v["name"]
        desc = v.get("description", "")
        db = v.get("database", default_database)
        raw_sql = resolve_variables(v["sql"], variables)
        condition = v.get("condition", "")
        abort = v.get("abort_on_failure", False)

        logger.info("  [%s %d/%d] %s: %s", phase, idx, len(validations), name, desc)
        logger.info("    SQL: %.120s...", raw_sql.replace("\n", " "))

        start = time.time()
        qr = run_athena_query(raw_sql, db, output_location, workgroup,
                              timeout_minutes=v.get("timeout_minutes", 10),
                              max_retries=1)
        dur = round(time.time() - start, 2)

        if qr["state"] != "SUCCEEDED":
            status = STATUS_FAIL
            detail = f"Query failed: {qr['failure_reason']}"
            actual = None
        elif condition:
            passed, actual = evaluate_condition(condition, qr["rows"])
            if passed:
                status = STATUS_PASS
                detail = f"Condition '{condition}' met (actual={actual})"
            else:
                status = STATUS_FAIL
                detail = f"Condition '{condition}' NOT met (actual={actual})"
        else:
            status = STATUS_PASS
            actual = None
            detail = "Query succeeded (no condition to check)"

        if status == STATUS_FAIL:
            all_pass = False

        result = {
            "name": name,
            "description": desc,
            "phase": phase,
            "status": status,
            "detail": detail,
            "actual_value": actual,
            "condition": condition,
            "cost_usd": qr["cost_usd"],
            "duration_sec": dur,
            "query_id": qr.get("query_id", ""),
            "data_scanned_bytes": qr.get("data_scanned_bytes", 0),
            "abort_on_failure": abort,
            "sql_hash": sql_hash(raw_sql),
        }
        results.append(result)

        log_fn = logger.info if status == STATUS_PASS else logger.error
        log_fn("    -> %s: %s (%.1fs, $%.4f)", status, detail, dur, qr["cost_usd"])

        if status == STATUS_FAIL and abort:
            logger.error("    ABORT: %s validation '%s' failed with abort_on_failure=true",
                         phase, name)
            break

    return results, all_pass


# ============================================================================
# 6. PIPELINE STEP EXECUTOR
# ============================================================================
def execute_step(step, variables, default_database, output_location, workgroup,
                 pipeline_retry_attempts):
    """Execute a single pipeline step with retry logic.
    Returns a step result dict."""
    name = step["name"]
    step_type = step.get("type", "sql")
    desc = step.get("description", "")
    db = step.get("database", default_database)
    raw_sql = resolve_variables(step.get("sql", ""), variables)
    rollback_sql_raw = step.get("rollback_sql", "")
    if rollback_sql_raw:
        rollback_sql_raw = resolve_variables(rollback_sql_raw, variables)
    target_table = resolve_variables(step.get("target_table", ""), variables)
    capture_snap = step.get("capture_snapshot", False)
    abort = step.get("abort_on_failure", True)
    timeout = step.get("timeout_minutes", DEFAULT_TIMEOUT_MINUTES)
    max_retries = step.get("retry_attempts", pipeline_retry_attempts)

    logger.info("=" * 60)
    logger.info("STEP: %s [%s]", name, step_type.upper())
    logger.info("  Description: %s", desc)
    logger.info("  Database: %s | Target: %s", db, target_table or "N/A")
    logger.info("  SQL hash: %s | Timeout: %dm | Max retries: %d",
                sql_hash(raw_sql), timeout, max_retries)

    step_start = time.time()
    snapshot_before = None
    snapshot_after = None
    rollback_executed = False
    rollback_status = "N/A"

    # Capture snapshot before write operations
    if capture_snap and target_table and step_type in ("insert", "merge"):
        snapshot_before = capture_iceberg_snapshot(
            target_table, db, output_location, workgroup)

    # Special handling for validation steps
    if step_type == "validation":
        condition = step.get("condition", "")
        qr = run_athena_query(raw_sql, db, output_location, workgroup,
                              timeout_minutes=timeout, max_retries=max_retries)
        dur = round(time.time() - step_start, 2)

        if qr["state"] != "SUCCEEDED":
            status = STATUS_FAIL
            detail = f"Validation query failed: {qr['failure_reason']}"
            actual = None
        elif condition:
            passed, actual = evaluate_condition(condition, qr["rows"])
            if passed:
                status = STATUS_PASS
                detail = f"Condition '{condition}' met (actual={actual})"
            else:
                status = STATUS_FAIL
                detail = f"Condition '{condition}' NOT met (actual={actual})"
        else:
            status = STATUS_PASS
            actual = None
            detail = "Validation query succeeded"

        return {
            "name": name,
            "type": step_type,
            "description": desc,
            "database": db,
            "status": status,
            "detail": detail,
            "actual_value": actual,
            "cost_usd": qr["cost_usd"],
            "duration_sec": dur,
            "query_id": qr.get("query_id", ""),
            "data_scanned_bytes": qr.get("data_scanned_bytes", 0),
            "exec_time_ms": qr.get("exec_time_ms", 0),
            "attempts": qr.get("total_attempts", 1),
            "max_attempts": max_retries + 1,
            "target_table": target_table,
            "snapshot_before": snapshot_before,
            "snapshot_after": snapshot_after,
            "rollback_executed": rollback_executed,
            "rollback_status": rollback_status,
            "rollback_sql": rollback_sql_raw,
            "abort_on_failure": abort,
            "sql_hash": sql_hash(raw_sql),
        }

    # Execute the SQL with retry
    qr = run_athena_query(raw_sql, db, output_location, workgroup,
                          timeout_minutes=timeout, max_retries=max_retries)
    dur = round(time.time() - step_start, 2)

    if qr["state"] == "SUCCEEDED":
        status = STATUS_PASS
        detail = f"Step completed successfully in {dur:.1f}s"
        rows_affected = ""
        # Try to get rows affected from DML results
        if step_type in ("insert", "merge", "ctas") and qr["rows"]:
            header, data = parse_athena_rows(qr["rows"])
            if data:
                rows_affected = str(data[0].get(header[0], "")) if header else ""
                if rows_affected:
                    detail += f" ({rows_affected} rows)"

        # Capture snapshot after write
        if capture_snap and target_table and step_type in ("insert", "merge"):
            snapshot_after = capture_iceberg_snapshot(
                target_table, db, output_location, workgroup)

        logger.info("  -> %s: %s ($%.4f)", status, detail, qr["cost_usd"])
    else:
        status = STATUS_FAIL
        detail = f"Failed after {qr.get('total_attempts', 1)} attempt(s): {qr['failure_reason']}"
        rows_affected = ""
        logger.error("  -> %s: %s", status, detail)

        # Execute rollback if configured
        if rollback_sql_raw:
            logger.warning("  Executing rollback SQL for step '%s'...", name)
            rollback_executed = True
            rb_result = run_athena_query(
                rollback_sql_raw, db, output_location, workgroup,
                timeout_minutes=10, max_retries=1)
            rollback_status = STATUS_PASS if rb_result["state"] == "SUCCEEDED" else STATUS_FAIL
            logger.info("  Rollback %s for step '%s'", rollback_status, name)
        elif snapshot_before and target_table:
            logger.warning("  Attempting Iceberg rollback for %s ...", target_table)
            rollback_executed = True
            rb_ok = rollback_iceberg_to_snapshot(
                target_table, snapshot_before, db, output_location, workgroup)
            rollback_status = STATUS_PASS if rb_ok else STATUS_FAIL

    return {
        "name": name,
        "type": step_type,
        "description": desc,
        "database": db,
        "status": status,
        "detail": detail,
        "actual_value": rows_affected,
        "cost_usd": qr["cost_usd"],
        "duration_sec": dur,
        "query_id": qr.get("query_id", ""),
        "data_scanned_bytes": qr.get("data_scanned_bytes", 0),
        "exec_time_ms": qr.get("exec_time_ms", 0),
        "attempts": qr.get("total_attempts", 1),
        "max_attempts": max_retries + 1,
        "target_table": target_table,
        "snapshot_before": snapshot_before,
        "snapshot_after": snapshot_after,
        "rollback_executed": rollback_executed,
        "rollback_status": rollback_status,
        "rollback_sql": rollback_sql_raw,
        "abort_on_failure": abort,
        "sql_hash": sql_hash(raw_sql),
    }


def cascade_rollback(completed_steps, variables, default_database,
                     output_location, workgroup):
    """Rollback all completed steps in reverse order."""
    rollback_results = []
    for step_result in reversed(completed_steps):
        name = step_result["name"]
        rollback_sql_raw = step_result.get("rollback_sql", "")
        target_table = step_result.get("target_table", "")
        snapshot_before = step_result.get("snapshot_before")
        db = step_result.get("database", default_database)

        if rollback_sql_raw:
            logger.warning("  CASCADE ROLLBACK: Executing rollback for step '%s'...", name)
            rb_result = run_athena_query(
                rollback_sql_raw, db, output_location, workgroup,
                timeout_minutes=10, max_retries=1)
            rb_status = STATUS_PASS if rb_result["state"] == "SUCCEEDED" else STATUS_FAIL
            rollback_results.append({"step": name, "status": rb_status,
                                     "method": "rollback_sql"})
            logger.info("    -> Rollback %s for '%s'", rb_status, name)
        elif snapshot_before and target_table:
            logger.warning("  CASCADE ROLLBACK: Iceberg rollback for '%s' -> snapshot %s",
                           name, snapshot_before)
            rb_ok = rollback_iceberg_to_snapshot(
                target_table, snapshot_before, db, output_location, workgroup)
            rb_status = STATUS_PASS if rb_ok else STATUS_FAIL
            rollback_results.append({"step": name, "status": rb_status,
                                     "method": "iceberg_snapshot"})
        else:
            logger.info("  CASCADE ROLLBACK: No rollback action for step '%s'", name)
            rollback_results.append({"step": name, "status": STATUS_SKIPPED,
                                     "method": "none"})

    return rollback_results


# ============================================================================
# 7. AUDIT LOGGING
# ============================================================================
def build_pipeline_audit_records(run_id, run_date, pipeline_name, layer,
                                 step_results, validation_results,
                                 overall_status):
    """Build audit records for all pipeline steps and validations."""
    records = []
    ts = datetime.now(timezone.utc).isoformat()

    for seq, sr in enumerate(step_results, 1):
        records.append(OrderedDict([
            ("run_id", run_id),
            ("run_date", run_date),
            ("event_timestamp", ts),
            ("pipeline_name", pipeline_name),
            ("layer", layer),
            ("step_name", sr["name"]),
            ("step_type", sr["type"]),
            ("step_sequence", str(seq)),
            ("description", sr.get("description", "")),
            ("database_name", sr.get("database", "")),
            ("sql_hash", sr.get("sql_hash", "")),
            ("status", sr["status"]),
            ("detail", sr.get("detail", "")),
            ("attempt_number", str(sr.get("attempts", 1))),
            ("max_attempts", str(sr.get("max_attempts", 3))),
            ("query_execution_id", sr.get("query_id", "")),
            ("data_scanned_bytes", str(sr.get("data_scanned_bytes", 0))),
            ("exec_time_ms", str(sr.get("exec_time_ms", 0))),
            ("cost_usd", str(sr.get("cost_usd", 0))),
            ("target_table", sr.get("target_table", "")),
            ("snapshot_id_before", sr.get("snapshot_before", "") or ""),
            ("snapshot_id_after", sr.get("snapshot_after", "") or ""),
            ("rollback_executed", str(sr.get("rollback_executed", False)).lower()),
            ("rollback_status", sr.get("rollback_status", "N/A")),
            ("overall_status", overall_status),
            ("failure_reason", sr.get("detail", "") if sr["status"] == STATUS_FAIL else ""),
            ("rows_affected", str(sr.get("actual_value", ""))),
            ("duration_sec", str(sr.get("duration_sec", 0))),
        ]))

    for vr in validation_results:
        records.append(OrderedDict([
            ("run_id", run_id),
            ("run_date", run_date),
            ("event_timestamp", ts),
            ("pipeline_name", pipeline_name),
            ("layer", layer),
            ("step_name", vr["name"]),
            ("step_type", "validation"),
            ("step_sequence", ""),
            ("description", vr.get("description", "")),
            ("database_name", ""),
            ("sql_hash", vr.get("sql_hash", "")),
            ("status", vr["status"]),
            ("detail", vr.get("detail", "")),
            ("attempt_number", "1"),
            ("max_attempts", "1"),
            ("query_execution_id", vr.get("query_id", "")),
            ("data_scanned_bytes", str(vr.get("data_scanned_bytes", 0))),
            ("exec_time_ms", ""),
            ("cost_usd", str(vr.get("cost_usd", 0))),
            ("target_table", ""),
            ("snapshot_id_before", ""),
            ("snapshot_id_after", ""),
            ("rollback_executed", "false"),
            ("rollback_status", "N/A"),
            ("overall_status", overall_status),
            ("failure_reason", vr.get("detail", "") if vr["status"] == STATUS_FAIL else ""),
            ("rows_affected", str(vr.get("actual_value", ""))),
            ("duration_sec", str(vr.get("duration_sec", 0))),
        ]))

    return records


def write_audit_to_s3(records, audit_cfg, run_date):
    """Write audit records as pipe-delimited file to S3."""
    if not records:
        logger.info("No audit records to write.")
        return None

    bucket = audit_cfg["s3_bucket"]
    prefix = audit_cfg["s3_prefix"].rstrip("/")
    file_id = uuid.uuid4().hex[:8]
    key = f"{prefix}/run_date={run_date}/pipeline_{file_id}.csv"

    header = "|".join(records[0].keys())
    lines = [header]
    for rec in records:
        line = "|".join(str(v).replace("|", "\\|").replace("\n", " ") for v in rec.values())
        lines.append(line)

    body = "\n".join(lines)
    s3_client().put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    s3_path = f"s3://{bucket}/{key}"
    logger.info("Audit log written to %s (%d records)", s3_path, len(records))
    return s3_path


def run_msck_repair(audit_cfg):
    """Run MSCK REPAIR TABLE on the pipeline audit table."""
    database = audit_cfg.get("database", "audit_db")
    table = audit_cfg.get("table", "etl_pipeline_audit")
    output_location = f"s3://{audit_cfg['s3_bucket']}/{audit_cfg['s3_prefix']}msck_results/"
    query = f"MSCK REPAIR TABLE {database}.{table}"
    logger.info("Running MSCK REPAIR TABLE on %s.%s", database, table)
    try:
        run_athena_query(query, database, output_location, max_retries=0)
        logger.info("MSCK REPAIR TABLE completed.")
    except Exception as exc:
        logger.warning("MSCK REPAIR TABLE failed (non-fatal): %s", exc)


# ============================================================================
# 8. HTML EMAIL REPORT
# ============================================================================
def _status_color(status):
    return {
        STATUS_PASS: "#28a745", STATUS_FAIL: "#dc3545",
        STATUS_SKIPPED: "#6c757d", STATUS_ABORTED: "#dc3545",
        STATUS_WARN: "#fd7e14", STATUS_ROLLBACK: "#8e44ad",
        STATUS_NOT_EXECUTED: "#6c757d",
    }.get(status, "#6c757d")


def generate_pipeline_report(run_id, run_date, pipeline_name, layer,
                             overall_status, duration_str, total_cost,
                             step_results, pre_results, post_results,
                             rollback_results, audit_s3_path):
    """Generate styled HTML email report for pipeline execution."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    overall_color = "#28a745" if overall_status == STATUS_PASS else "#dc3545"
    badge_bg = "#00e676" if overall_status == STATUS_PASS else "#ff1744"
    status_icon = "&#x2705;" if overall_status == STATUS_PASS else "&#x274C;"

    total_steps = len(step_results)
    passed = sum(1 for s in step_results if s["status"] == STATUS_PASS)
    failed = sum(1 for s in step_results if s["status"] == STATUS_FAIL)
    skipped = sum(1 for s in step_results if s["status"] in (STATUS_SKIPPED, STATUS_NOT_EXECUTED))
    aborted = sum(1 for s in step_results if s["status"] == STATUS_ABORTED)
    rolled_back = sum(1 for s in step_results if s.get("rollback_executed"))

    html = f"""
    <html><head><style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; color: #333;
               background-color: #f0f2f5; line-height: 1.6; }}
        .container {{ max-width: 960px; margin: 0 auto; background: white;
                      border-radius: 12px; overflow: hidden;
                      box-shadow: 0 4px 20px rgba(0,0,0,0.12); }}
        .header {{ background: linear-gradient(135deg, #0d47a1 0%, #1565c0 40%, #1976d2 70%, #42a5f5 100%);
                   padding: 35px 35px 28px 35px; color: white; position: relative; }}
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
        .kpi-card {{ flex: 1; min-width: 110px; border-radius: 10px; padding: 18px 14px;
                     text-align: center; border-left: 4px solid; }}
        .kpi-card .kpi-label {{ font-size: 10px; font-weight: bold; color: #666;
                                text-transform: uppercase; letter-spacing: 0.5px; }}
        .kpi-card .kpi-value {{ font-size: 28px; font-weight: bold; margin: 6px 0 2px 0; }}
        .red-row {{ background-color: #fff0f0 !important; }}
        .total-row {{ background-color: #e3f2fd !important; font-weight: bold; }}
        .footer {{ padding: 25px 35px; font-size: 11px; color: #888;
                   border-top: 2px solid #eee;
                   background: linear-gradient(90deg, #fafbfc 0%, #f5f6fa 100%); }}
    </style></head>
    <body>
    <div class="container">

    <!-- HEADER -->
    <div class="header">
        <h1 style="color:#ffffff;margin:0 0 6px 0;font-size:24px;">
            &#x1F680; ETL Pipeline Report
            <span style="display:inline-block;padding:6px 18px;border-radius:20px;
                         font-size:13px;font-weight:bold;margin-left:12px;
                         vertical-align:middle;letter-spacing:0.5px;
                         box-shadow:0 2px 8px rgba(0,0,0,0.25);
                         background:{badge_bg};color:#000;">
                {status_icon} {overall_status}</span></h1>
        <p style="margin:4px 0 0 0;font-size:13px;opacity:0.95;color:#e3f2fd;">
            &#x1F4E6; <b>{pipeline_name}</b> &nbsp;&bull;&nbsp;
            &#x1F3F7; Layer: <b>{layer}</b></p>
        <p style="margin:4px 0 0 0;font-size:12px;opacity:0.9;color:#e3f2fd;">
            &#x1F4C5; {run_date} &nbsp;&bull;&nbsp;
            &#x23F1; Duration: {duration_str} &nbsp;&bull;&nbsp;
            &#x1F550; Generated: {now_str}</p>
        <p style="margin:4px 0 0 0;font-size:12px;opacity:0.85;color:#e3f2fd;">
            &#x1F194; Run ID: <code style="background:rgba(255,255,255,0.2);
               padding:2px 8px;border-radius:4px;font-size:11px;color:#fff;">{run_id}</code></p>
    </div>

    <div class="content">

    <!-- 1. PIPELINE OVERVIEW -->
    <h2>&#x1F4CA; 1. Pipeline Overview</h2>

    <div style="display:flex;flex-wrap:wrap;gap:12px;margin:15px 0;">
      <div class="kpi-card" style="background:#f0f4ff;border-color:#1565c0;">
        <div class="kpi-label">Total Steps</div>
        <div class="kpi-value" style="color:#1565c0;">{total_steps}</div>
      </div>
      <div class="kpi-card" style="background:#f0fff0;border-color:#28a745;">
        <div class="kpi-label">Passed</div>
        <div class="kpi-value" style="color:#28a745;">{passed}</div>
      </div>
      <div class="kpi-card" style="background:#fff0f0;border-color:#dc3545;">
        <div class="kpi-label">Failed</div>
        <div class="kpi-value" style="color:#dc3545;">{failed}</div>
      </div>
      <div class="kpi-card" style="background:#f5f5f5;border-color:#6c757d;">
        <div class="kpi-label">Skipped</div>
        <div class="kpi-value" style="color:#6c757d;">{skipped}</div>
      </div>
      <div class="kpi-card" style="background:#f3e5f5;border-color:#8e44ad;">
        <div class="kpi-label">Rolled Back</div>
        <div class="kpi-value" style="color:#8e44ad;">{rolled_back}</div>
      </div>
      <div class="kpi-card" style="background:#fff8e1;border-color:#fd7e14;">
        <div class="kpi-label">Total Cost</div>
        <div class="kpi-value" style="color:#fd7e14;">${total_cost:.4f}</div>
      </div>
    </div>
"""

    section = 2

    # 2. PRE-VALIDATIONS
    if pre_results:
        pre_pass = sum(1 for r in pre_results if r["status"] == STATUS_PASS)
        pre_fail = sum(1 for r in pre_results if r["status"] == STATUS_FAIL)
        html += f"""
    <h2>&#x1F50D; {section}. Pre-Validations</h2>
    <p style="color:#666;font-size:12px;">
        {pre_pass} passed, {pre_fail} failed out of {len(pre_results)} checks.</p>
    <table><thead><tr>
        <th>#</th><th>Check</th><th>Status</th><th>Condition</th>
        <th>Actual</th><th>Cost</th><th>Duration</th><th>Detail</th>
    </tr></thead><tbody>"""
        for i, vr in enumerate(pre_results, 1):
            color = _status_color(vr["status"])
            row_cls = ' class="red-row"' if vr["status"] == STATUS_FAIL else ""
            html += f"""<tr{row_cls}>
                <td>{i}</td>
                <td><b>{vr['name']}</b><br>
                    <small style="color:#888;">{vr.get('description', '')}</small></td>
                <td><span class="badge" style="background:{color};">{vr['status']}</span></td>
                <td><code style="font-size:10px;">{vr.get('condition', '-')}</code></td>
                <td>{vr.get('actual_value', '-')}</td>
                <td>${vr.get('cost_usd', 0):.4f}</td>
                <td>{vr.get('duration_sec', 0):.1f}s</td>
                <td style="font-size:11px;">{vr.get('detail', '')}</td>
            </tr>"""
        html += "</tbody></table>"
        section += 1

    # 3. STEP EXECUTION TIMELINE
    if step_results:
        html += f"""
    <h2>&#x1F4DD; {section}. Step Execution Timeline</h2>
    <p style="color:#666;font-size:12px;">
        Sequential execution of {total_steps} pipeline step(s).</p>
    <table><thead><tr>
        <th>#</th><th>Step</th><th>Type</th><th>Status</th>
        <th>Attempts</th><th>Duration</th><th>Cost</th>
        <th>Target</th><th>Snapshot</th><th>Rollback</th><th>Detail</th>
    </tr></thead><tbody>"""

        for i, sr in enumerate(step_results, 1):
            color = _status_color(sr["status"])
            row_cls = ' class="red-row"' if sr["status"] in (STATUS_FAIL, STATUS_ABORTED) else ""
            snap_info = ""
            if sr.get("snapshot_before"):
                snap_info = f'Before: {sr["snapshot_before"][:8]}...'
            if sr.get("snapshot_after"):
                snap_info += f' After: {sr["snapshot_after"][:8]}...'
            snap_info = snap_info or "-"
            rb_info = "-"
            if sr.get("rollback_executed"):
                rb_color = _status_color(sr["rollback_status"])
                rb_info = f'<span class="badge" style="background:{rb_color};">{sr["rollback_status"]}</span>'

            html += f"""<tr{row_cls}>
                <td>{i}</td>
                <td><b>{sr['name']}</b><br>
                    <small style="color:#888;">{sr.get('description', '')[:60]}</small></td>
                <td style="text-transform:uppercase;font-size:11px;font-weight:bold;">{sr['type']}</td>
                <td><span class="badge" style="background:{color};">{sr['status']}</span></td>
                <td style="text-align:center;">{sr.get('attempts', 1)}/{sr.get('max_attempts', 3)}</td>
                <td>{sr.get('duration_sec', 0):.1f}s</td>
                <td>${sr.get('cost_usd', 0):.4f}</td>
                <td style="font-size:11px;">{sr.get('target_table', '-') or '-'}</td>
                <td style="font-size:10px;">{snap_info}</td>
                <td>{rb_info}</td>
                <td style="font-size:11px;max-width:200px;overflow:hidden;
                    text-overflow:ellipsis;">{sr.get('detail', '')[:100]}</td>
            </tr>"""

        # Totals row
        total_dur = sum(s.get("duration_sec", 0) for s in step_results)
        total_step_cost = sum(s.get("cost_usd", 0) for s in step_results)
        html += f"""<tr class="total-row">
            <td></td><td><b>TOTAL</b></td><td></td>
            <td><span class="badge" style="background:{overall_color};">{overall_status}</span></td>
            <td></td>
            <td>{total_dur:.1f}s</td>
            <td>${total_step_cost:.4f}</td>
            <td colspan="4"></td>
        </tr>"""
        html += "</tbody></table>"
        section += 1

    # 4. POST-VALIDATIONS
    if post_results:
        post_pass = sum(1 for r in post_results if r["status"] == STATUS_PASS)
        post_fail = sum(1 for r in post_results if r["status"] == STATUS_FAIL)
        html += f"""
    <h2>&#x2705; {section}. Post-Validations</h2>
    <p style="color:#666;font-size:12px;">
        {post_pass} passed, {post_fail} failed out of {len(post_results)} checks.</p>
    <table><thead><tr>
        <th>#</th><th>Check</th><th>Status</th><th>Condition</th>
        <th>Actual</th><th>Cost</th><th>Duration</th><th>Detail</th>
    </tr></thead><tbody>"""
        for i, vr in enumerate(post_results, 1):
            color = _status_color(vr["status"])
            row_cls = ' class="red-row"' if vr["status"] == STATUS_FAIL else ""
            html += f"""<tr{row_cls}>
                <td>{i}</td>
                <td><b>{vr['name']}</b><br>
                    <small style="color:#888;">{vr.get('description', '')}</small></td>
                <td><span class="badge" style="background:{color};">{vr['status']}</span></td>
                <td><code style="font-size:10px;">{vr.get('condition', '-')}</code></td>
                <td>{vr.get('actual_value', '-')}</td>
                <td>${vr.get('cost_usd', 0):.4f}</td>
                <td>{vr.get('duration_sec', 0):.1f}s</td>
                <td style="font-size:11px;">{vr.get('detail', '')}</td>
            </tr>"""
        html += "</tbody></table>"
        section += 1

    # 5. ROLLBACK SUMMARY (if any)
    if rollback_results:
        html += f"""
    <h2>&#x21A9; {section}. Rollback Summary</h2>
    <p style="color:#666;font-size:12px;">
        Cascading rollback was triggered. {len(rollback_results)} step(s) processed.</p>
    <table><thead><tr>
        <th>#</th><th>Step</th><th>Method</th><th>Status</th>
    </tr></thead><tbody>"""
        for i, rb in enumerate(rollback_results, 1):
            color = _status_color(rb["status"])
            row_cls = ' class="red-row"' if rb["status"] == STATUS_FAIL else ""
            html += f"""<tr{row_cls}>
                <td>{i}</td>
                <td><b>{rb['step']}</b></td>
                <td>{rb.get('method', '-')}</td>
                <td><span class="badge" style="background:{color};">{rb['status']}</span></td>
            </tr>"""
        html += "</tbody></table>"
        section += 1

    # FOOTER
    html += f"""
    </div><!-- end content -->
    <div class="footer">
        <p>&#x1F680; <b>{pipeline_name}</b> &nbsp;&bull;&nbsp;
           &#x1F3F7; Layer: {layer} &nbsp;&bull;&nbsp;
           &#x1F194; {run_id}</p>
        <p>&#x1F4C1; <b>Audit Log:</b> {audit_s3_path or 'N/A'}</p>
        <p>&#x1F4B0; <b>Total Cost:</b> ${total_cost:.4f} &nbsp;&bull;&nbsp;
           &#x23F1; <b>Duration:</b> {duration_str} &nbsp;&bull;&nbsp;
           &#x1F4C5; <b>Date:</b> {run_date}</p>
        <p style="margin-top:10px;">Generated by <b>Strands ETL Pipeline</b> &mdash;
           Enterprise SQL Pipeline Executor &nbsp;|&nbsp; BIG DATA Team</p>
    </div>

    </div><!-- end container -->
    </body></html>"""

    return html


def send_email(html_content, email_cfg, pipeline_name, overall_status, run_date):
    """Send the pipeline report email via SMTP."""
    sender = email_cfg["sender"]
    recipients = email_cfg["recipients"]
    subject = (f"{email_cfg.get('subject_prefix', 'ETL Pipeline')} – "
               f"{pipeline_name} – {overall_status} – {run_date}")

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
        logger.info("Pipeline report email sent to %s", ", ".join(recipients))
    except Exception as exc:
        logger.error("Failed to send email: %s", exc)


# ============================================================================
# 9. MAIN PIPELINE EXECUTOR
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Strands ETL Pipeline — Enterprise SQL Pipeline Executor")
    parser.add_argument(
        "--config", required=True,
        help="Path to pipeline config JSON (local or s3://)")
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print steps without executing")
    parser.add_argument(
        "--resume-from", default=None,
        help="Resume pipeline from a specific step name (skip prior steps)")
    parser.add_argument(
        "--run-date", default=None,
        help="Override run date (default: today UTC, format: YYYY-MM-DD)")
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:12]
    run_date = args.run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_start_time = time.time()

    logger.info("=" * 70)
    logger.info("STRANDS ETL PIPELINE – Run ID: %s  Date: %s", run_id, run_date)
    logger.info("=" * 70)

    # Load config
    config = load_config(args.config)
    pipeline_cfg = config.get("pipeline", {})
    audit_cfg = config.get("audit", {})
    email_cfg = config.get("email", {})

    pipeline_name = pipeline_cfg.get("name", "unnamed_pipeline")
    layer = pipeline_cfg.get("layer", "UNKNOWN")
    default_database = pipeline_cfg.get("database", "default")
    output_location = pipeline_cfg.get("athena_output", "s3://bucket/athena-results/")
    workgroup = pipeline_cfg.get("workgroup", "primary")
    retry_attempts = pipeline_cfg.get("retry_attempts", DEFAULT_RETRY_ATTEMPTS)

    logger.info("Pipeline: %s | Layer: %s | Database: %s",
                pipeline_name, layer, default_database)
    logger.info("Output: %s | Workgroup: %s | Retries: %d",
                output_location, workgroup, retry_attempts)

    # Build variables
    variables = build_variables(pipeline_cfg, run_id, run_date)
    logger.info("Variables: %s", json.dumps(
        {k: v for k, v in variables.items()
         if not k.startswith("CURRENT_DATE") or k == "CURRENT_DATE"},
        indent=2))

    # Resolve step execution order
    steps = pipeline_cfg.get("steps", [])
    try:
        ordered_steps = resolve_execution_order(steps)
    except ValueError as exc:
        logger.error("FATAL: %s", exc)
        sys.exit(1)

    logger.info("Execution order: %s",
                " -> ".join(s["name"] for s in ordered_steps))

    # DRY RUN MODE
    if args.dry_run:
        logger.info("=" * 70)
        logger.info("DRY RUN MODE — No queries will be executed")
        logger.info("=" * 70)
        for i, step in enumerate(ordered_steps, 1):
            raw_sql = resolve_variables(step.get("sql", ""), variables)
            logger.info("")
            logger.info("Step %d: %s [%s]", i, step["name"],
                        step.get("type", "sql").upper())
            logger.info("  Description: %s", step.get("description", ""))
            logger.info("  Database: %s", step.get("database", default_database))
            logger.info("  Target: %s",
                        resolve_variables(step.get("target_table", ""), variables) or "N/A")
            logger.info("  Depends on: %s", step.get("depends_on", []))
            logger.info("  Capture snapshot: %s", step.get("capture_snapshot", False))
            logger.info("  Abort on failure: %s", step.get("abort_on_failure", True))
            logger.info("  Rollback SQL: %s",
                        "Yes" if step.get("rollback_sql") else "No")
            logger.info("  SQL:\n    %s", raw_sql[:500].replace("\n", "\n    "))
        logger.info("")
        logger.info("Pre-validations: %d",
                    len(pipeline_cfg.get("pre_validations", [])))
        logger.info("Post-validations: %d",
                    len(pipeline_cfg.get("post_validations", [])))
        logger.info("DRY RUN complete — exiting.")
        sys.exit(0)

    # ================================================================
    # EXECUTION
    # ================================================================
    overall_status = STATUS_PASS
    step_results = []
    pre_results = []
    post_results = []
    rollback_results = []
    total_cost = 0.0
    pipeline_aborted = False
    resume_from = args.resume_from

    # ---- PRE-VALIDATIONS ----
    pre_validations = pipeline_cfg.get("pre_validations", [])
    if pre_validations:
        logger.info("")
        logger.info("-" * 50)
        logger.info("PRE-VALIDATIONS (%d checks)", len(pre_validations))
        logger.info("-" * 50)

        pre_results, pre_ok = run_validations(
            pre_validations, variables, default_database,
            output_location, workgroup, phase="PRE")
        total_cost += sum(r.get("cost_usd", 0) for r in pre_results)

        if not pre_ok:
            # Check if any abort_on_failure
            abort_checks = [r for r in pre_results
                            if r["status"] == STATUS_FAIL and r.get("abort_on_failure")]
            if abort_checks:
                logger.error("PRE-VALIDATION FAILED with abort — pipeline will not run.")
                overall_status = STATUS_ABORTED
                pipeline_aborted = True

    # ---- STEP EXECUTION ----
    if not pipeline_aborted:
        logger.info("")
        logger.info("-" * 50)
        logger.info("PIPELINE STEPS (%d steps)", len(ordered_steps))
        logger.info("-" * 50)

        skip_until_found = resume_from is not None
        completed_for_rollback = []

        for step_idx, step in enumerate(ordered_steps, 1):
            # Resume logic
            if skip_until_found:
                if step["name"] == resume_from:
                    skip_until_found = False
                    logger.info("Resuming from step '%s'", resume_from)
                else:
                    logger.info("SKIP (resume): Step %d/%d: %s",
                                step_idx, len(ordered_steps), step["name"])
                    step_results.append({
                        "name": step["name"],
                        "type": step.get("type", "sql"),
                        "description": step.get("description", ""),
                        "database": step.get("database", default_database),
                        "status": STATUS_SKIPPED,
                        "detail": f"Skipped (resuming from '{resume_from}')",
                        "cost_usd": 0, "duration_sec": 0,
                        "attempts": 0, "max_attempts": retry_attempts + 1,
                        "target_table": step.get("target_table", ""),
                        "snapshot_before": None, "snapshot_after": None,
                        "rollback_executed": False, "rollback_status": "N/A",
                        "rollback_sql": step.get("rollback_sql", ""),
                        "abort_on_failure": step.get("abort_on_failure", True),
                        "sql_hash": sql_hash(step.get("sql", "")),
                    })
                    continue

            if pipeline_aborted:
                # Mark remaining steps as NOT_EXECUTED
                step_results.append({
                    "name": step["name"],
                    "type": step.get("type", "sql"),
                    "description": step.get("description", ""),
                    "database": step.get("database", default_database),
                    "status": STATUS_NOT_EXECUTED,
                    "detail": "Pipeline aborted — step not executed",
                    "cost_usd": 0, "duration_sec": 0,
                    "attempts": 0, "max_attempts": retry_attempts + 1,
                    "target_table": step.get("target_table", ""),
                    "snapshot_before": None, "snapshot_after": None,
                    "rollback_executed": False, "rollback_status": "N/A",
                    "rollback_sql": step.get("rollback_sql", ""),
                    "abort_on_failure": step.get("abort_on_failure", True),
                    "sql_hash": sql_hash(step.get("sql", "")),
                })
                continue

            # Check dependencies are met
            deps = step.get("depends_on", [])
            deps_met = True
            for dep_name in deps:
                dep_result = next((r for r in step_results if r["name"] == dep_name), None)
                if not dep_result or dep_result["status"] not in (STATUS_PASS, STATUS_SKIPPED):
                    deps_met = False
                    break

            if not deps_met:
                logger.warning("  Step '%s' skipped: dependency '%s' not met",
                               step["name"], dep_name)
                step_results.append({
                    "name": step["name"],
                    "type": step.get("type", "sql"),
                    "description": step.get("description", ""),
                    "database": step.get("database", default_database),
                    "status": STATUS_SKIPPED,
                    "detail": f"Skipped: dependency '{dep_name}' not met",
                    "cost_usd": 0, "duration_sec": 0,
                    "attempts": 0, "max_attempts": retry_attempts + 1,
                    "target_table": step.get("target_table", ""),
                    "snapshot_before": None, "snapshot_after": None,
                    "rollback_executed": False, "rollback_status": "N/A",
                    "rollback_sql": step.get("rollback_sql", ""),
                    "abort_on_failure": step.get("abort_on_failure", True),
                    "sql_hash": sql_hash(step.get("sql", "")),
                })
                continue

            # Execute step
            logger.info("")
            logger.info("[%d/%d] Executing step: %s",
                        step_idx, len(ordered_steps), step["name"])

            result = execute_step(
                step, variables, default_database,
                output_location, workgroup, retry_attempts)
            step_results.append(result)
            total_cost += result.get("cost_usd", 0)

            if result["status"] == STATUS_PASS:
                completed_for_rollback.append(result)
            elif result["status"] == STATUS_FAIL:
                overall_status = STATUS_FAIL
                if result.get("abort_on_failure", True):
                    logger.error("ABORT: Step '%s' failed with abort_on_failure=true",
                                 step["name"])
                    pipeline_aborted = True

                    # Cascade rollback
                    if completed_for_rollback:
                        logger.warning("")
                        logger.warning("=" * 50)
                        logger.warning("CASCADING ROLLBACK (%d completed steps)",
                                       len(completed_for_rollback))
                        logger.warning("=" * 50)
                        rollback_results = cascade_rollback(
                            completed_for_rollback, variables, default_database,
                            output_location, workgroup)

    # ---- POST-VALIDATIONS ----
    post_validations = pipeline_cfg.get("post_validations", [])
    if post_validations and not pipeline_aborted:
        logger.info("")
        logger.info("-" * 50)
        logger.info("POST-VALIDATIONS (%d checks)", len(post_validations))
        logger.info("-" * 50)

        post_results, post_ok = run_validations(
            post_validations, variables, default_database,
            output_location, workgroup, phase="POST")
        total_cost += sum(r.get("cost_usd", 0) for r in post_results)

        if not post_ok:
            overall_status = STATUS_FAIL

    # Also check step failures
    if any(s["status"] == STATUS_FAIL for s in step_results):
        overall_status = STATUS_FAIL

    # ---- TIMING ----
    run_duration_sec = round(time.time() - run_start_time, 2)
    mins = int(run_duration_sec // 60)
    secs = int(run_duration_sec % 60)
    duration_str = f"{mins}m {secs}s"

    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE: %s | Status: %s | Duration: %s | Cost: $%.4f",
                pipeline_name, overall_status, duration_str, total_cost)
    logger.info("=" * 70)

    # ---- AUDIT LOGGING ----
    audit_s3_path = None
    all_validation_results = pre_results + post_results
    if audit_cfg.get("s3_bucket"):
        logger.info("")
        logger.info("-" * 50)
        logger.info("AUDIT LOGGING")
        logger.info("-" * 50)

        audit_records = build_pipeline_audit_records(
            run_id, run_date, pipeline_name, layer,
            step_results, all_validation_results, overall_status)

        try:
            audit_s3_path = write_audit_to_s3(audit_records, audit_cfg, run_date)
        except Exception as exc:
            logger.error("Failed to write audit log: %s", exc)

        try:
            run_msck_repair(audit_cfg)
        except Exception as exc:
            logger.warning("MSCK REPAIR failed (non-fatal): %s", exc)

    # ---- EMAIL REPORT ----
    if email_cfg.get("sender") and email_cfg.get("recipients"):
        logger.info("")
        logger.info("-" * 50)
        logger.info("EMAIL REPORT")
        logger.info("-" * 50)

        html = generate_pipeline_report(
            run_id, run_date, pipeline_name, layer,
            overall_status, duration_str, total_cost,
            step_results, pre_results, post_results,
            rollback_results, audit_s3_path)

        send_email(html, email_cfg, pipeline_name, overall_status, run_date)

    # Exit with appropriate code
    if overall_status != STATUS_PASS:
        logger.error("Pipeline finished with status: %s", overall_status)
        sys.exit(1)
    else:
        logger.info("Pipeline finished successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
