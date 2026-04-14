"""
table_validator.py
==================
Standalone table-validation script that evaluates a set of configured rules
against Athena tables and writes every result row to the `audit_validation`
table in Glue / Athena.

How it works
------------
1.  Load a JSON validation-config (from S3 or local path).
2.  For every rule in the config, run the corresponding Athena check SQL.
3.  Record PASS / FAIL / ERROR for each rule in `audit_validation` (S3 / Glue).
4.  Return a summary exit-code (0 = all passed, 1 = at least one failure, 2 = errors).

Supported rule types
--------------------
  NOT_NULL          – column must not contain NULL values
  RANGE             – numeric column must be within [min, max]
  REGEX             – string column must match a pattern
  UNIQUENESS        – column (or column set) must have no duplicates
  REFERENTIAL       – foreign-key column must exist in a reference table
  ROW_COUNT         – table row count must satisfy a threshold condition
  BUSINESS          – arbitrary SQL expression that must evaluate to 0 failures

`audit_validation` table schema
--------------------------------
  record_id          STRING      (UUID, primary key)
  run_id             STRING      (batch execution identifier)
  pipeline_name      STRING
  database_name      STRING
  table_name         STRING
  column_name        STRING
  rule_name          STRING
  rule_type          STRING
  severity           STRING      LOW | MEDIUM | HIGH | CRITICAL
  expected_constraint STRING
  failed_value       STRING      (stringified sample of a failing value, if any)
  row_count_failed   BIGINT
  total_row_count    BIGINT
  status             STRING      PASS | FAIL | ERROR
  error_message      STRING
  failure_timestamp  TIMESTAMP
  -- columns added by Strands framework after AI analysis --
  ai_classification  STRING      (populated later: TRUE_FAILURE | FALSE_POSITIVE | ...)
  ai_confidence      DOUBLE
  ai_recommended_action STRING
  ai_explanation     STRING
  ai_analysed_at     TIMESTAMP

Usage
-----
    # Run locally with a local config file
    python pyscript/table_validator.py --config config/etl_config.json

    # Run with an S3 config and write to a specific S3 output path
    python pyscript/table_validator.py \
        --config s3://my-bucket/config/validation_rules.json \
        --s3-output s3://my-bucket/audit_validation/ \
        --database validation_db \
        --workgroup primary

    # AWS Glue entry point (called automatically by Glue when deployed as a job)
    python pyscript/table_validator.py --glue-mode
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("table_validator")


# ---------------------------------------------------------------------------
# SQL templates for each rule type
# ---------------------------------------------------------------------------

RULE_SQL: Dict[str, str] = {
    # Returns number of NULLs in the target column
    "NOT_NULL": """
        SELECT COUNT(*) AS failed_count,
               COUNT(*) AS total_count
        FROM   {database}.{table}
        WHERE  {column} IS NULL
    """,

    # Returns rows outside [min_value, max_value]
    "RANGE": """
        SELECT COUNT(*) AS failed_count,
               (SELECT COUNT(*) FROM {database}.{table}) AS total_count
        FROM   {database}.{table}
        WHERE  {column} NOT BETWEEN {min_value} AND {max_value}
    """,

    # Returns rows that do NOT match the regex pattern
    "REGEX": """
        SELECT COUNT(*) AS failed_count,
               (SELECT COUNT(*) FROM {database}.{table}) AS total_count
        FROM   {database}.{table}
        WHERE  NOT regexp_like(CAST({column} AS VARCHAR), '{pattern}')
    """,

    # Returns rows with duplicate values (COUNT > 1 means duplicates exist)
    "UNIQUENESS": """
        SELECT SUM(dup_count - 1) AS failed_count,
               (SELECT COUNT(*) FROM {database}.{table}) AS total_count
        FROM (
            SELECT {column}, COUNT(*) AS dup_count
            FROM   {database}.{table}
            GROUP BY {column}
            HAVING COUNT(*) > 1
        ) t
    """,

    # Checks referential integrity: rows in source that have no match in ref table
    "REFERENTIAL": """
        SELECT COUNT(*) AS failed_count,
               (SELECT COUNT(*) FROM {database}.{table}) AS total_count
        FROM   {database}.{table} src
        WHERE  src.{column} IS NOT NULL
          AND  src.{column} NOT IN (
                   SELECT DISTINCT {ref_column}
                   FROM   {ref_database}.{ref_table}
               )
    """,

    # Compares current row count against threshold
    "ROW_COUNT": """
        SELECT CASE
                 WHEN COUNT(*) {operator} {threshold} THEN 0
                 ELSE COUNT(*)
               END AS failed_count,
               COUNT(*) AS total_count
        FROM {database}.{table}
    """,

    # Arbitrary SQL: caller provides full WHERE clause; failed_count = rows matching it
    "BUSINESS": """
        SELECT COUNT(*) AS failed_count,
               (SELECT COUNT(*) FROM {database}.{table}) AS total_count
        FROM   {database}.{table}
        WHERE  {condition}
    """,
}

# Sample-value SQL (best-effort: grab one failing value for the audit log)
SAMPLE_SQL: Dict[str, str] = {
    "NOT_NULL":    "SELECT CAST({column} AS VARCHAR) FROM {database}.{table} WHERE {column} IS NULL LIMIT 1",
    "RANGE":       "SELECT CAST({column} AS VARCHAR) FROM {database}.{table} WHERE {column} NOT BETWEEN {min_value} AND {max_value} LIMIT 1",
    "REGEX":       "SELECT CAST({column} AS VARCHAR) FROM {database}.{table} WHERE NOT regexp_like(CAST({column} AS VARCHAR), '{pattern}') LIMIT 1",
    "UNIQUENESS":  "SELECT CAST({column} AS VARCHAR) FROM {database}.{table} GROUP BY {column} HAVING COUNT(*) > 1 LIMIT 1",
    "REFERENTIAL": "SELECT CAST(src.{column} AS VARCHAR) FROM {database}.{table} src WHERE src.{column} IS NOT NULL AND src.{column} NOT IN (SELECT DISTINCT {ref_column} FROM {ref_database}.{ref_table}) LIMIT 1",
    "ROW_COUNT":   None,   # no sample for count checks
    "BUSINESS":    "SELECT CAST({column} AS VARCHAR) FROM {database}.{table} WHERE {condition} LIMIT 1",
}


# ---------------------------------------------------------------------------
# Athena helper
# ---------------------------------------------------------------------------

class AthenaRunner:
    """Thin Athena wrapper: submit SQL → poll → return rows."""

    POLL_INTERVAL = 5   # seconds
    MAX_POLLS     = 60  # 5-minute timeout

    def __init__(self, s3_output: str, workgroup: str = "primary", region: str = "us-east-1"):
        self.s3_output  = s3_output
        self.workgroup  = workgroup
        self.athena     = boto3.client("athena", region_name=region)

    def run(self, sql: str, database: str) -> Tuple[List[str], List[Dict[str, str]]]:
        """
        Execute *sql* in *database*.
        Returns (column_names, rows_as_dicts).
        Raises RuntimeError on failure.
        """
        exec_id = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": self.s3_output},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]

        for _ in range(self.MAX_POLLS):
            resp   = self.athena.get_query_execution(QueryExecutionId=exec_id)
            state  = resp["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
                raise RuntimeError(f"Athena query {state}: {reason}\nSQL: {sql[:300]}")
            time.sleep(self.POLL_INTERVAL)
        else:
            raise RuntimeError(f"Athena query timed out after {self.MAX_POLLS * self.POLL_INTERVAL}s")

        result = self.athena.get_query_results(QueryExecutionId=exec_id)
        cols   = [c["Label"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows   = [
            {cols[i]: d.get("VarCharValue", "") for i, d in enumerate(row.get("Data", []))}
            for row in result["ResultSet"]["Rows"][1:]   # skip header
        ]
        return cols, rows


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(path: str, s3_client) -> Dict[str, Any]:
    """Load validation config from S3 path (s3://…) or local file."""
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    with open(path) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Audit record builder
# ---------------------------------------------------------------------------

def _build_audit_record(
    run_id:      str,
    pipeline:    str,
    rule:        Dict[str, Any],
    status:      str,
    failed_count: int,
    total_count:  int,
    failed_value: str  = "",
    error_msg:    str  = "",
) -> Dict[str, Any]:
    """Assemble one row destined for audit_validation."""
    return {
        "record_id":          str(uuid.uuid4()),
        "run_id":             run_id,
        "pipeline_name":      pipeline,
        "database_name":      rule.get("database", ""),
        "table_name":         rule.get("table", ""),
        "column_name":        rule.get("column", ""),
        "rule_name":          rule["rule_name"],
        "rule_type":          rule["rule_type"],
        "severity":           rule.get("severity", "MEDIUM"),
        "expected_constraint": rule.get("constraint_description", ""),
        "failed_value":       failed_value,
        "row_count_failed":   failed_count,
        "total_row_count":    total_count,
        "status":             status,
        "error_message":      error_msg,
        "failure_timestamp":  datetime.utcnow().isoformat(),
        # AI columns — populated later by the Strands integration layer
        "ai_classification":     "",
        "ai_confidence":         None,
        "ai_recommended_action": "",
        "ai_explanation":        "",
        "ai_analysed_at":        "",
    }


# ---------------------------------------------------------------------------
# Rule evaluator
# ---------------------------------------------------------------------------

class RuleEvaluator:
    """
    Evaluates one validation rule and returns an audit record dict.

    The logic is intentionally rule-type-aware so the SQL templates
    are filled correctly before being sent to Athena.
    """

    def __init__(self, athena: AthenaRunner):
        self.athena = athena

    def evaluate(self, rule: Dict[str, Any], run_id: str, pipeline: str) -> Dict[str, Any]:
        rule_type = rule["rule_type"].upper()
        database  = rule.get("database", "default")
        table     = rule["table"]
        column    = rule.get("column", "*")

        try:
            sql = self._build_sql(rule_type, rule)
        except KeyError as exc:
            return _build_audit_record(
                run_id, pipeline, rule,
                status="ERROR", failed_count=0, total_count=0,
                error_msg=f"Config missing field: {exc}",
            )

        try:
            _, rows = self.athena.run(sql, database)
        except RuntimeError as exc:
            return _build_audit_record(
                run_id, pipeline, rule,
                status="ERROR", failed_count=0, total_count=0,
                error_msg=str(exc)[:500],
            )

        if not rows:
            return _build_audit_record(
                run_id, pipeline, rule,
                status="ERROR", failed_count=0, total_count=0,
                error_msg="Athena returned no rows.",
            )

        failed_count = int(rows[0].get("failed_count") or 0)
        total_count  = int(rows[0].get("total_count")  or 0)
        status       = "PASS" if failed_count == 0 else "FAIL"

        # Best-effort: grab one failing sample value
        failed_value = ""
        if status == "FAIL":
            failed_value = self._sample_failing_value(rule_type, rule, database)

        return _build_audit_record(
            run_id, pipeline, rule,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            failed_value=failed_value,
        )

    # ------------------------------------------------------------------

    def _build_sql(self, rule_type: str, rule: Dict[str, Any]) -> str:
        template = RULE_SQL.get(rule_type)
        if not template:
            raise KeyError(f"Unknown rule_type: {rule_type}")

        params: Dict[str, Any] = {
            "database":     rule.get("database", "default"),
            "table":        rule["table"],
            "column":       rule.get("column", "*"),
            "min_value":    rule.get("min_value", 0),
            "max_value":    rule.get("max_value", 0),
            "pattern":      rule.get("pattern", ".*"),
            "ref_database": rule.get("ref_database", rule.get("database", "default")),
            "ref_table":    rule.get("ref_table", ""),
            "ref_column":   rule.get("ref_column", rule.get("column", "")),
            "operator":     rule.get("operator", ">="),
            "threshold":    rule.get("threshold", 0),
            "condition":    rule.get("condition", "1=0"),
        }
        return template.format(**params).strip()

    def _sample_failing_value(
        self, rule_type: str, rule: Dict[str, Any], database: str
    ) -> str:
        sample_tmpl = SAMPLE_SQL.get(rule_type)
        if not sample_tmpl:
            return ""
        try:
            sql = sample_tmpl.format(
                database=database,
                table=rule["table"],
                column=rule.get("column", "*"),
                min_value=rule.get("min_value", 0),
                max_value=rule.get("max_value", 0),
                pattern=rule.get("pattern", ".*"),
                ref_database=rule.get("ref_database", database),
                ref_table=rule.get("ref_table", ""),
                ref_column=rule.get("ref_column", rule.get("column", "")),
                condition=rule.get("condition", "1=0"),
            )
            _, rows = self.athena.run(sql, database)
            if rows:
                return list(rows[0].values())[0] if rows[0] else ""
        except Exception as exc:
            logger.debug(f"Could not fetch sample value: {exc}")
        return ""


# ---------------------------------------------------------------------------
# Audit writer — persists results to S3 so Athena / Glue can read them
# ---------------------------------------------------------------------------

class AuditWriter:
    """
    Writes audit records to S3 as newline-delimited JSON (one file per run).
    The Glue Catalog table `audit_validation` is partitioned by date and run_id
    so Athena can query the latest results efficiently.

    File path pattern:
        s3://<bucket>/audit_validation/
            dt=<YYYY-MM-DD>/
                run_id=<run_id>/
                    results.json
    """

    def __init__(self, s3_output: str, region: str = "us-east-1"):
        # Expect s3://bucket/prefix/
        parts          = s3_output[5:].split("/", 1)
        self.bucket    = parts[0]
        self.prefix    = parts[1].rstrip("/") if len(parts) > 1 else ""
        self.s3        = boto3.client("s3", region_name=region)

    def write(self, records: List[Dict[str, Any]], run_id: str) -> str:
        """Write all records for *run_id* to S3. Returns the S3 URI."""
        today   = datetime.utcnow().strftime("%Y-%m-%d")
        key     = f"{self.prefix}/dt={today}/run_id={run_id}/results.json"
        payload = "\n".join(json.dumps(r, default=str) for r in records)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )
        uri = f"s3://{self.bucket}/{key}"
        logger.info(f"Audit records written to {uri}")
        return uri


# ---------------------------------------------------------------------------
# Main validator
# ---------------------------------------------------------------------------

class TableValidator:
    """
    Orchestrates the full validation pipeline for one execution run.

    Typical flow
    ------------
    1.  Load the validation config.
    2.  For each rule → call RuleEvaluator.evaluate().
    3.  Collect all audit records (PASS + FAIL + ERROR).
    4.  Write to S3 via AuditWriter (the Glue table picks them up automatically).
    5.  Return a summary dict and a list of FAILED records.
    """

    def __init__(
        self,
        config_path:    str,
        s3_output:      str,
        pipeline_name:  str = "default_pipeline",
        database:       str = "default",
        workgroup:      str = "primary",
        region:         str = "us-east-1",
    ):
        self.config_path   = config_path
        self.s3_output     = s3_output
        self.pipeline_name = pipeline_name
        self.database      = database
        self.workgroup     = workgroup
        self.region        = region

        self.s3        = boto3.client("s3", region_name=region)
        self.run_id    = str(uuid.uuid4())
        self.athena    = AthenaRunner(s3_output=s3_output, workgroup=workgroup, region=region)
        self.evaluator = RuleEvaluator(self.athena)
        self.writer    = AuditWriter(s3_output=s3_output, region=region)

    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """
        Execute the full validation run.

        Returns a summary dict::

            {
              "run_id":         "<uuid>",
              "pipeline_name":  "...",
              "total_rules":    25,
              "passed":         20,
              "failed":         4,
              "errors":         1,
              "s3_audit_path":  "s3://...",
              "failed_records": [...],   # list of audit rows where status == FAIL
              "timestamp":      "..."
            }
        """
        logger.info(f"Starting validation run {self.run_id} for pipeline '{self.pipeline_name}'")

        # 1. Load config
        config = load_config(self.config_path, self.s3)
        rules  = config.get("validation_rules", [])
        logger.info(f"Loaded {len(rules)} validation rules")

        # 2. Evaluate every rule
        audit_records: List[Dict[str, Any]] = []
        for rule in rules:
            rule.setdefault("database", self.database)
            logger.info(
                f"Evaluating [{rule['rule_type']}] {rule['rule_name']} "
                f"on {rule.get('database')}.{rule.get('table')}.{rule.get('column', '*')}"
            )
            record = self.evaluator.evaluate(rule, self.run_id, self.pipeline_name)
            audit_records.append(record)
            logger.info(f"  → {record['status']}  "
                        f"(failed={record['row_count_failed']}, total={record['total_row_count']})")

        # 3. Write to S3 / audit_validation
        s3_path = self.writer.write(audit_records, self.run_id)

        # 4. Build summary
        passed  = sum(1 for r in audit_records if r["status"] == "PASS")
        failed  = sum(1 for r in audit_records if r["status"] == "FAIL")
        errors  = sum(1 for r in audit_records if r["status"] == "ERROR")

        summary = {
            "run_id":         self.run_id,
            "pipeline_name":  self.pipeline_name,
            "total_rules":    len(audit_records),
            "passed":         passed,
            "failed":         failed,
            "errors":         errors,
            "s3_audit_path":  s3_path,
            "failed_records": [r for r in audit_records if r["status"] == "FAIL"],
            "timestamp":      datetime.utcnow().isoformat(),
        }

        logger.info(
            f"Validation run {self.run_id} complete. "
            f"PASS={passed}  FAIL={failed}  ERROR={errors}"
        )
        return summary


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Table Validator — validates tables and writes to audit_validation")
    p.add_argument("--config",      required=True,  help="Path to validation-rules config (local or s3://…)")
    p.add_argument("--s3-output",   required=True,  help="S3 path for Athena output + audit_validation writes")
    p.add_argument("--pipeline",    default="default_pipeline", help="Pipeline name tag on audit records")
    p.add_argument("--database",    default="default", help="Default Athena database")
    p.add_argument("--workgroup",   default="primary", help="Athena workgroup")
    p.add_argument("--region",      default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    p.add_argument("--glue-mode",   action="store_true", help="Read args from Glue job parameters")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    if args.glue_mode:
        # When running as a Glue job, override args from Glue job parameters
        try:
            from awsglue.utils import getResolvedOptions
            glue_args = getResolvedOptions(
                sys.argv,
                ["config", "s3_output", "pipeline", "database", "workgroup"],
            )
            args.config    = glue_args["config"]
            args.s3_output = glue_args["s3_output"]
            args.pipeline  = glue_args.get("pipeline", args.pipeline)
            args.database  = glue_args.get("database", args.database)
            args.workgroup = glue_args.get("workgroup", args.workgroup)
        except Exception as exc:
            logger.warning(f"Could not read Glue args: {exc}. Using CLI args.")

    validator = TableValidator(
        config_path=args.config,
        s3_output=args.s3_output,
        pipeline_name=args.pipeline,
        database=args.database,
        workgroup=args.workgroup,
        region=args.region,
    )

    summary = validator.run()

    print(json.dumps(summary, indent=2, default=str))

    # Exit code: 0 = all passed, 1 = failures, 2 = errors
    if summary["errors"] > 0:
        return 2
    if summary["failed"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
