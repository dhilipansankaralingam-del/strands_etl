"""
Athena tools — all direct I/O with the audit_validation Iceberg table.

Tools
-----
fetch_audit_failures       SELECT unassessed FAIL rows (last 24 h, severity DESC).
write_ai_enrichment        UPDATE one row with the five AI columns.
execute_raw_sql            Run arbitrary SQL and return results as JSON.
run_nl_query               Translate a natural-language question to SQL, then execute.
fetch_historical_patterns  Pull prior classified rows for the same rule + table.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

import boto3
from strands import tool

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
ATHENA_DATABASE: str = os.environ.get("ATHENA_DATABASE", "audit_db")
ATHENA_TABLE: str = os.environ.get("ATHENA_TABLE", "audit_validation")
ATHENA_OUTPUT: str = os.environ.get(
    "ATHENA_OUTPUT", "s3://strands-etl-audit/athena-results/"
)
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
BEDROCK_NL_MODEL: str = os.environ.get(
    "BEDROCK_NL_MODEL", "us.anthropic.claude-haiku-4-5-20251001"
)

_POLL_ATTEMPTS = 60
_POLL_INTERVAL = 3  # seconds

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------
_athena_client: Optional[object] = None
_bedrock_client: Optional[object] = None


def _athena() -> object:
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client("athena", region_name=AWS_REGION)
    return _athena_client


def _bedrock() -> object:
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    return _bedrock_client


def _submit_and_poll(sql: str, database: str = ATHENA_DATABASE) -> dict:
    """Submit SQL to Athena and block until the query reaches a terminal state."""
    client = _athena()
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    qid = resp["QueryExecutionId"]

    for _ in range(_POLL_ATTEMPTS):
        time.sleep(_POLL_INTERVAL)
        execution = client.get_query_execution(QueryExecutionId=qid)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution["QueryExecution"]

    # Timed out — return last known state
    return client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]


def _paginate_results(qid: str) -> list[dict]:
    """Return all result rows as a list of column-name → value dicts."""
    client = _athena()
    paginator = client.get_paginator("get_query_results")
    columns: list[str] = []
    rows: list[dict] = []

    for page in paginator.paginate(QueryExecutionId=qid):
        page_rows = page["ResultSet"]["Rows"]
        if not columns:
            columns = [c.get("VarCharValue", "") for c in page_rows[0]["Data"]]
            page_rows = page_rows[1:]  # skip header
        for row in page_rows:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            rows.append(dict(zip(columns, values)))

    return rows


def _execute_sql(sql: str, database: str = ATHENA_DATABASE) -> str:
    """Shared execution path used by multiple tools."""
    execution = _submit_and_poll(sql, database)
    state = execution["Status"]["State"]
    qid = execution["QueryExecutionId"]

    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown error")
        return json.dumps({"error": f"Query {state}: {reason}", "sql": sql})

    rows = _paginate_results(qid)
    scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
    return json.dumps({"count": len(rows), "rows": rows, "data_scanned_bytes": scanned})


# ---------------------------------------------------------------------------
# Public tools
# ---------------------------------------------------------------------------


@tool
def fetch_audit_failures(limit: int = 500) -> str:
    """
    Fetch unassessed FAIL rows from audit_validation that need AI classification.

    Selects rows where status = 'FAIL', ai_classification IS NULL, and
    failure_timestamp is within the last 24 hours.  Results are ordered by
    severity DESC so the most critical failures are processed first.

    Parameters
    ----------
    limit : int
        Maximum number of rows to return (default 500).

    Returns
    -------
    JSON string with keys 'count' and 'rows' (list of row dicts).
    """
    sql = f"""
        SELECT *
        FROM   {ATHENA_TABLE}
        WHERE  status = 'FAIL'
          AND  ai_classification IS NULL
          AND  failure_timestamp >= NOW() - INTERVAL '24' HOUR
        ORDER  BY severity DESC
        LIMIT  {int(limit)}
    """
    execution = _submit_and_poll(sql)
    state = execution["Status"]["State"]
    qid = execution["QueryExecutionId"]

    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown error")
        return json.dumps({"error": f"Query {state}: {reason}"})

    rows = _paginate_results(qid)
    return json.dumps({"count": len(rows), "rows": rows})


@tool
def write_ai_enrichment(
    record_id: str,
    ai_classification: str,
    ai_confidence: float,
    ai_recommended_action: str,
    ai_explanation: str,
) -> str:
    """
    Write the five AI columns back to audit_validation for one record.

    Executes an Iceberg UPDATE statement so the row is enriched in-place.
    The audit_validation table must be an Iceberg table to support DML.

    Parameters
    ----------
    record_id             : Primary key of the row to update.
    ai_classification     : TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION
    ai_confidence         : Float 0.0–1.0 representing classification certainty.
    ai_recommended_action : IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR
    ai_explanation        : Human-readable reasoning (single-quoted, apostrophes escaped).

    Returns
    -------
    JSON string with keys 'success' (bool) and 'record_id'.
    """
    safe_explanation = ai_explanation.replace("'", "''")
    safe_classification = ai_classification.replace("'", "''")
    safe_action = ai_recommended_action.replace("'", "''")
    safe_record_id = record_id.replace("'", "''")
    confidence = max(0.0, min(1.0, float(ai_confidence)))

    sql = f"""
        UPDATE {ATHENA_TABLE}
        SET    ai_classification      = '{safe_classification}',
               ai_confidence          = {confidence},
               ai_recommended_action  = '{safe_action}',
               ai_explanation         = '{safe_explanation}',
               ai_analysed_at         = CURRENT_TIMESTAMP
        WHERE  record_id = '{safe_record_id}'
    """
    execution = _submit_and_poll(sql)
    state = execution["Status"]["State"]

    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown error")
        return json.dumps(
            {"success": False, "record_id": record_id, "error": f"{state}: {reason}"}
        )

    return json.dumps({"success": True, "record_id": record_id})


@tool
def execute_raw_sql(sql: str, database: str = ATHENA_DATABASE) -> str:
    """
    Execute any SQL against Athena and return all result rows as JSON.

    Use this for ad-hoc investigation queries, verification checks, or
    any situation where a specific pre-built tool does not cover the need.

    Parameters
    ----------
    sql      : Valid Athena/Presto SQL string.
    database : Glue Catalog database to use (default: audit_db).

    Returns
    -------
    JSON string with keys 'count', 'rows', and 'data_scanned_bytes'.
    """
    return _execute_sql(sql, database)


@tool
def run_nl_query(question: str, database: str = ATHENA_DATABASE) -> str:
    """
    Translate a natural-language question into SQL and execute it on Athena.

    This is a lightweight helper for quick, one-shot NL queries.  For complex
    multi-turn query sessions use the query_agent instead.

    The audit_validation schema exposed to the translator:
        record_id, run_id, rule_name, table_name, column_name, database_name,
        pipeline_name, severity (LOW|MEDIUM|HIGH|CRITICAL),
        rule_type (NOT_NULL|RANGE|REGEX|REFERENTIAL|BUSINESS),
        status (PASS|FAIL|WARN), failed_value, expected_constraint,
        row_count_failed, total_row_count, failure_timestamp,
        ai_classification (TRUE_FAILURE|FALSE_POSITIVE|NEEDS_INVESTIGATION),
        ai_confidence, ai_recommended_action, ai_explanation, ai_analysed_at.

    Parameters
    ----------
    question : Natural language question about validation failures.
    database : Glue Catalog database to query.

    Returns
    -------
    JSON string with query results (same format as execute_raw_sql).
    """
    schema_hint = (
        f"Athena table {ATHENA_DATABASE}.{ATHENA_TABLE} has these columns: "
        "record_id STRING, run_id STRING, rule_name STRING, table_name STRING, "
        "column_name STRING, database_name STRING, pipeline_name STRING, "
        "severity STRING (LOW|MEDIUM|HIGH|CRITICAL), "
        "rule_type STRING (NOT_NULL|RANGE|REGEX|REFERENTIAL|BUSINESS), "
        "status STRING (PASS|FAIL|WARN), failed_value STRING, "
        "expected_constraint STRING, row_count_failed BIGINT, "
        "total_row_count BIGINT, failure_timestamp TIMESTAMP, "
        "ai_classification STRING (TRUE_FAILURE|FALSE_POSITIVE|NEEDS_INVESTIGATION), "
        "ai_confidence DOUBLE, ai_recommended_action STRING, "
        "ai_explanation STRING, ai_analysed_at TIMESTAMP."
    )
    prompt = (
        f"Schema: {schema_hint}\n\n"
        f"Write an Athena SQL query that answers this question: {question}\n"
        "Return ONLY the SQL statement with no explanation, no markdown fences."
    )
    response = _bedrock().invoke_model(
        modelId=BEDROCK_NL_MODEL,
        body=json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 512,
                "messages": [{"role": "user", "content": prompt}],
            }
        ),
    )
    sql = json.loads(response["body"].read())["content"][0]["text"].strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return _execute_sql(sql, database)


@tool
def fetch_historical_patterns(
    table_name: str, rule_name: str, limit: int = 20
) -> str:
    """
    Retrieve previously classified rows from audit_validation that share the
    same table_name or rule_name.  Use this to spot recurring patterns before
    deciding how to classify a new failure.

    Parameters
    ----------
    table_name : Table name to match (exact).
    rule_name  : Validation rule name to match (exact).
    limit      : Maximum rows to return (default 20).

    Returns
    -------
    JSON string with keys 'count' and 'patterns' (list of row dicts with
    ai_classification, ai_confidence, ai_recommended_action, ai_explanation).
    """
    safe_table = table_name.replace("'", "''")
    safe_rule = rule_name.replace("'", "''")

    sql = f"""
        SELECT record_id, rule_name, table_name, column_name, severity,
               rule_type, status, failure_timestamp,
               ai_classification, ai_confidence,
               ai_recommended_action, ai_explanation, ai_analysed_at
        FROM   {ATHENA_TABLE}
        WHERE  ai_classification IS NOT NULL
          AND  (table_name = '{safe_table}' OR rule_name = '{safe_rule}')
        ORDER  BY ai_analysed_at DESC
        LIMIT  {int(limit)}
    """
    execution = _submit_and_poll(sql)
    state = execution["Status"]["State"]

    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown error")
        return json.dumps({"error": f"Query {state}: {reason}"})

    rows = _paginate_results(execution["QueryExecutionId"])
    return json.dumps({"count": len(rows), "patterns": rows})
