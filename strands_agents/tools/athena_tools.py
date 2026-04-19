"""
strands_agents/tools/athena_tools.py
=====================================
Strands @tool functions for Athena and audit table operations.

All functions are pure AWS API calls — no AI reasoning inside.
The Agent/orchestrator decides WHEN to call them.
"""

import json
import os
import time
from typing import Any

import boto3
from strands import tool

AWS_REGION  = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB   = os.environ.get("ATHENA_DATABASE",    "data_quality_db")
ATHENA_OUT  = os.environ.get("ATHENA_S3_OUTPUT",   "s3://strands-etl-athena-results/")
ATHENA_WG   = os.environ.get("ATHENA_WORKGROUP",   "primary")


def _run_query(sql: str, database: str = ATHENA_DB) -> list[dict]:
    """Execute an Athena query and return rows as list of dicts."""
    client = boto3.client("athena", region_name=AWS_REGION)

    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
        WorkGroup=ATHENA_WG,
    )
    exec_id = resp["QueryExecutionId"]

    for _ in range(120):
        status = client.get_query_execution(QueryExecutionId=exec_id)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)
    else:
        raise TimeoutError("Athena query timed out after 240 s")

    result = client.get_query_results(QueryExecutionId=exec_id)
    col_names = [c["Label"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in result["ResultSet"]["Rows"][1:]:  # skip header
        values = [d.get("VarCharValue", "") for d in row["Data"]]
        rows.append(dict(zip(col_names, values)))
    return rows


@tool
def fetch_audit_failures(look_back_hours: int = 24, pipeline: str = "",
                         severity: str = "", run_id: str = "",
                         max_records: int = 500) -> str:
    """
    Fetch FAIL records from the audit_validation table that have not yet been
    AI-classified (ai_classification IS NULL).

    Args:
        look_back_hours: How many hours back to look (default 24).
        pipeline:        Filter to a specific pipeline name (empty = all).
        severity:        Filter to CRITICAL or HIGH (empty = all).
        run_id:          Filter to a specific ETL run UUID (empty = all).
        max_records:     Maximum rows to return (default 500).

    Returns:
        JSON string with list of validation record dicts.
    """
    where = ["status = 'FAIL'",
             "(ai_classification IS NULL OR ai_classification = '')",
             f"run_date >= CURRENT_TIMESTAMP - INTERVAL '{look_back_hours}' HOUR"]
    if pipeline:
        where.append(f"pipeline_name = '{pipeline}'")
    if severity:
        where.append(f"severity = '{severity}'")
    if run_id:
        where.append(f"run_id = '{run_id}'")

    sql = f"""
        SELECT
            id, run_id, pipeline_name, table_name, rule_name, severity,
            status, actual_value, expected_value, threshold,
            error_message, run_date, additional_context
        FROM audit_validation
        WHERE {' AND '.join(where)}
        ORDER BY severity DESC, run_date DESC
        LIMIT {max_records}
    """
    rows = _run_query(sql)
    return json.dumps({"records": rows, "count": len(rows)})


@tool
def fetch_historical_patterns(table_name: str, rule_name: str,
                               look_back_days: int = 60) -> str:
    """
    Fetch historical PASS/FAIL pattern for a given table + rule combination
    to provide the agent with Z-score context.

    Args:
        table_name:     The table being validated.
        rule_name:      The rule to look up history for.
        look_back_days: Rolling window for the baseline (default 60).

    Returns:
        JSON with pass_rate, avg_actual_value, stddev_actual_value, recent_rows.
    """
    sql = f"""
        SELECT
            DATE(run_date)                                  AS day,
            COUNT(*)                                        AS total,
            SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failures,
            AVG(TRY_CAST(actual_value AS DOUBLE))           AS avg_actual,
            STDDEV(TRY_CAST(actual_value AS DOUBLE))        AS stddev_actual
        FROM audit_validation
        WHERE
            table_name = '{table_name}'
            AND rule_name = '{rule_name}'
            AND run_date >= CURRENT_TIMESTAMP - INTERVAL '{look_back_days}' DAY
        GROUP BY DATE(run_date)
        ORDER BY day DESC
    """
    rows = _run_query(sql)
    return json.dumps({"table_name": table_name, "rule_name": rule_name,
                       "history": rows, "days": look_back_days})


@tool
def write_ai_enrichment(record_id: str, classification: str, confidence: float,
                         explanation: str, recommended_action: str,
                         run_id: str = "") -> str:
    """
    Update a single audit_validation row with the AI classification result.

    Args:
        record_id:           The UUID primary key of the audit_validation row.
        classification:      TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION.
        confidence:          Float 0.0–1.0 (e.g. 0.92).
        explanation:         One-sentence AI reasoning.
        recommended_action:  Terse action string (e.g. 'ALERT_AND_FIX').
        run_id:              ETL run_id for traceability (optional).

    Returns:
        JSON confirming the update.
    """
    sql = f"""
        UPDATE audit_validation
        SET
            ai_classification    = '{classification}',
            ai_confidence        = {confidence},
            ai_explanation       = '{explanation.replace("'", "''")}',
            ai_recommended_action = '{recommended_action}',
            ai_analysed_at       = CURRENT_TIMESTAMP
        WHERE id = '{record_id}'
    """
    _run_query(sql)
    return json.dumps({"updated": record_id, "classification": classification,
                       "confidence": confidence})


@tool
def run_nl_query(natural_language_question: str, database: str = ATHENA_DB) -> str:
    """
    Execute a natural-language question against the Athena data warehouse.
    This tool is used by the query agent to translate NL → SQL and return results.

    The agent is expected to generate the SQL itself based on schema context and
    then call this tool with the generated SQL embedded in the question, OR the
    orchestrator calls the query_agent which handles the NL→SQL step.

    Args:
        natural_language_question: The plain-English question to answer.
        database:                  Athena database to query.

    Returns:
        JSON with sql, columns, rows (up to 100), row_count.
    """
    # Schema context injected so the agent can write correct SQL
    schema_sql = "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '" + database + "' LIMIT 200"
    try:
        schema_rows = _run_query(schema_sql, database="information_schema")
    except Exception:
        schema_rows = []

    return json.dumps({
        "question":   natural_language_question,
        "schema":     schema_rows[:50],
        "instruction": (
            "Use the schema above to write Athena SQL answering the question. "
            "Return your answer as: {\"sql\": \"...\", \"explanation\": \"...\"}. "
            "Then call execute_raw_sql with the sql field."
        ),
    })


@tool
def execute_raw_sql(sql: str, database: str = ATHENA_DB) -> str:
    """
    Execute a raw SQL string against Athena and return results.

    Args:
        sql:      Valid Athena (Presto/Trino) SQL string.
        database: Target database (default: data_quality_db).

    Returns:
        JSON with columns, rows (up to 100), row_count.
    """
    rows = _run_query(sql, database=database)
    columns = list(rows[0].keys()) if rows else []
    return json.dumps({
        "sql":       sql,
        "columns":   columns,
        "rows":      rows[:100],
        "row_count": len(rows),
    })
