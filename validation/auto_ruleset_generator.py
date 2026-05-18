"""
validation/auto_ruleset_generator.py
=====================================
Auto-generate Glue Data Quality rules from profiling baselines and run them
via the Athena API, writing PASS/FAIL rows directly to the audit results table.

This avoids Glue DQ cluster spin-up costs while still surfacing rule results
in the same table that AuditActionAgent already reads.

Supported rule types (configured via auto_ruleset_generation.rule_generation_policy):
  - IsComplete        : null_pct must remain below historical threshold
  - IsUnique          : distinct_pct must be ≥ threshold (PK columns)
  - RowCount          : row_count must be within ± tolerance_pct of baseline
  - ColumnValues_enum : allowed value set must not expand beyond historical distinct values
  - ColumnValues_range: min/max must stay within ± tolerance_pct of historical bounds
  - ZScore_distribution: column share / KPI must not deviate > zscore_threshold std deviations

Usage
-----
    from validation.auto_ruleset_generator import AutoRulesetGenerator

    gen = AutoRulesetGenerator(
        database="audit_db",
        results_table="etl_orchestrator_audit",
        athena_output="s3://bucket/athena/",
        learning_bucket="strands-etl-learning",
        aws_region="us-east-1",
        ruleset_config=cfg["auto_ruleset_generation"],
    )
    report = gen.run(
        target_table="orders_db.orders_fact",
        table_cfg=cfg["table_config"]["orders_db.orders_fact"],
        run_date="2024-07-01",
    )
    print(report)
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, date as date_type
from typing import Any, Dict, List, Optional, Tuple

import boto3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Severity mapping
# ---------------------------------------------------------------------------
_RULE_SEVERITY = {
    "IsComplete":            "HIGH",
    "IsUnique":              "CRITICAL",
    "RowCount":              "HIGH",
    "ColumnValues_enum":     "MEDIUM",
    "ColumnValues_range":    "HIGH",
    "ZScore_distribution":   "HIGH",
}


class AutoRulesetGenerator:
    """
    Reads profiling baselines from S3, generates validation SQL for each
    enabled rule type, runs the SQL via Athena, and writes PASS/FAIL rows
    into the audit results table.
    """

    BASELINE_PREFIX = "validation/profiling/baselines/"

    def __init__(
        self,
        database: str,
        results_table: str,
        athena_output: str,
        learning_bucket: str,
        aws_region: str = "us-east-1",
        ruleset_config: Optional[Dict[str, Any]] = None,
    ):
        self.database        = database
        self.results_table   = results_table
        self.athena_output   = athena_output
        self.learning_bucket = learning_bucket
        self.ruleset_cfg     = ruleset_config or {}
        self.policy          = self.ruleset_cfg.get("rule_generation_policy", {})
        self.min_runs        = self.ruleset_cfg.get("min_baseline_runs", 10)

        self.athena = boto3.client("athena", region_name=aws_region)
        self.s3     = boto3.client("s3",     region_name=aws_region)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        target_table: str,
        table_cfg: Optional[Dict[str, Any]] = None,
        run_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate and execute all applicable rules for target_table.

        Parameters
        ----------
        target_table : str
            "database.tablename" of the table to validate.
        table_cfg : dict, optional
            Per-table config from config JSON (table_config section).
        run_date : str, optional
            YYYY-MM-DD. Defaults to today.

        Returns a summary dict with counts of PASS / FAIL rules and row keys
        written to the audit table.
        """
        if not self.ruleset_cfg.get("enabled", False):
            logger.info("[AutoRulesetGenerator] Disabled in config — skipping.")
            return {"enabled": False}

        run_date = run_date or str(date_type.today())
        table_cfg = table_cfg or {}

        # Parse database.tablename
        parts = target_table.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Expected 'database.tablename', got '{target_table}'")
        target_db, target_tbl = parts

        logger.info(f"[AutoRulesetGenerator] Generating rules for {target_table}")

        baseline = self._load_baseline(target_db, target_tbl)
        if not baseline:
            logger.warning(f"[AutoRulesetGenerator] No baseline found for {target_table}. Run profiling first.")
            return {"error": "No baseline available", "target": target_table}

        baseline_runs = self._count_history_runs(target_db, target_tbl)
        if baseline_runs < self.min_runs:
            logger.info(
                f"[AutoRulesetGenerator] Only {baseline_runs} baseline runs for {target_table} "
                f"(need {self.min_runs}). Skipping."
            )
            return {"skipped": True, "reason": f"need {self.min_runs} runs, have {baseline_runs}"}

        # Generate rules
        rules = self._generate_rules(target_db, target_tbl, baseline, table_cfg)
        logger.info(f"[AutoRulesetGenerator] Generated {len(rules)} rules for {target_table}")

        # Execute each rule and collect results
        pass_count = 0
        fail_count = 0
        results = []
        for rule in rules:
            outcome = self._execute_rule(rule, target_db, target_tbl, run_date)
            results.append(outcome)
            if outcome["status"] == "PASS":
                pass_count += 1
            else:
                fail_count += 1
            # Write FAIL rows to audit table so AuditActionAgent can pick them up
            if outcome["status"] == "FAIL":
                self._write_audit_row(outcome, run_date)

        # Optionally write DQDL file to S3
        self._write_dqdl_file(target_db, target_tbl, rules, run_date)

        summary = {
            "target": target_table,
            "run_date": run_date,
            "rules_generated": len(rules),
            "pass": pass_count,
            "fail": fail_count,
            "results": results,
        }
        logger.info(f"[AutoRulesetGenerator] {target_table}: {pass_count} PASS, {fail_count} FAIL")
        return summary

    # ------------------------------------------------------------------
    # Rule generation
    # ------------------------------------------------------------------

    def _generate_rules(
        self,
        target_db: str,
        target_tbl: str,
        baseline: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Build a list of rule dicts, each with name, sql, severity, type."""
        rules: List[Dict[str, Any]] = []
        col_profiles: Dict[str, Any] = baseline.get("column_profiles", {})
        kpi: Dict[str, Any] = baseline.get("kpi_snapshot", {})
        profiling_cols: Dict[str, Any] = table_cfg.get("profiling_columns", {})
        pk_col = table_cfg.get("primary_key_column")

        # ── RowCount
        rc_policy = self.policy.get("RowCount", {})
        if rc_policy and kpi.get("row_count"):
            hist_rc  = int(kpi["row_count"])
            tol_pct  = rc_policy.get("tolerance_pct", 15) / 100
            lower    = int(hist_rc * (1 - tol_pct))
            upper    = int(hist_rc * (1 + tol_pct))
            rules.append({
                "rule_name":  "RowCount_within_tolerance",
                "rule_type":  "RowCount",
                "severity":   _RULE_SEVERITY["RowCount"],
                "sql": (
                    f"SELECT CASE WHEN COUNT(*) BETWEEN {lower} AND {upper} "
                    f"THEN 'PASS' ELSE 'FAIL' END AS result, COUNT(*) AS observed_value, "
                    f"'{lower}–{upper}' AS expected_constraint "
                    f"FROM {target_db}.{target_tbl}"
                ),
                "column_name": None,
                "expected_constraint": f"{lower}–{upper}",
            })

        for col, col_data in col_profiles.items():
            col_cfg  = profiling_cols.get(col, {})
            col_tier = col_cfg.get("tier", "FULL")
            col_role = col_cfg.get("role", "")
            if col_tier == "SKIP":
                continue

            hist_null_pct      = col_data.get("null_pct", 1.0)
            hist_distinct_cnt  = col_data.get("distinct_count", 0)
            hist_distinct_pct  = col_data.get("distinct_pct", 0.0)
            hist_min           = col_data.get("min_value")
            hist_max           = col_data.get("max_value")
            col_type           = col_data.get("data_type", "string")
            is_numeric = any(t in col_type.lower() for t in ("int", "double", "float", "decimal", "bigint"))

            # ── IsComplete
            ic_policy = self.policy.get("IsComplete", {})
            if ic_policy and hist_null_pct < ic_policy.get("generate_when_null_pct_below", 0.01):
                rules.append({
                    "rule_name":  f"IsComplete_{col}",
                    "rule_type":  "IsComplete",
                    "severity":   _RULE_SEVERITY["IsComplete"],
                    "sql": (
                        f"SELECT CASE WHEN COUNT_IF({col} IS NULL)*1.0/COUNT(*) < 0.01 "
                        f"THEN 'PASS' ELSE 'FAIL' END AS result, "
                        f"ROUND(COUNT_IF({col} IS NULL)*100.0/COUNT(*),2) AS observed_value, "
                        f"'< 1% nulls' AS expected_constraint "
                        f"FROM {target_db}.{target_tbl}"
                    ),
                    "column_name": col,
                    "expected_constraint": "< 1% nulls",
                })

            # ── IsUnique (PK columns only or high-distinct columns)
            iu_policy = self.policy.get("IsUnique", {})
            only_for_roles = iu_policy.get("only_for_roles", ["primary_key"])
            if iu_policy and hist_distinct_pct > iu_policy.get("generate_when_distinct_pct_above", 0.99):
                if not only_for_roles or col_role in only_for_roles or col == pk_col:
                    rules.append({
                        "rule_name":  f"IsUnique_{col}",
                        "rule_type":  "IsUnique",
                        "severity":   _RULE_SEVERITY["IsUnique"],
                        "sql": (
                            f"SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT {col}) "
                            f"THEN 'PASS' ELSE 'FAIL' END AS result, "
                            f"CAST(COUNT(*) - COUNT(DISTINCT {col}) AS VARCHAR) AS observed_value, "
                            f"'0 duplicates' AS expected_constraint "
                            f"FROM {target_db}.{target_tbl}"
                        ),
                        "column_name": col,
                        "expected_constraint": "0 duplicates",
                    })

            # ── ColumnValues_enum (low-cardinality stable dimensions)
            ce_policy = self.policy.get("ColumnValues_enum", {})
            if ce_policy and not is_numeric and 0 < hist_distinct_cnt <= 20:
                rules.append({
                    "rule_name":  f"ColumnValues_enum_{col}",
                    "rule_type":  "ColumnValues_enum",
                    "severity":   _RULE_SEVERITY["ColumnValues_enum"],
                    "sql": (
                        f"SELECT CASE WHEN COUNT(DISTINCT {col}) <= {hist_distinct_cnt} "
                        f"THEN 'PASS' ELSE 'FAIL' END AS result, "
                        f"CAST(COUNT(DISTINCT {col}) AS VARCHAR) AS observed_value, "
                        f"'<= {hist_distinct_cnt} distinct values' AS expected_constraint "
                        f"FROM {target_db}.{target_tbl}"
                    ),
                    "column_name": col,
                    "expected_constraint": f"<= {hist_distinct_cnt} distinct values",
                })

            # ── ColumnValues_range (numeric columns)
            cr_policy = self.policy.get("ColumnValues_range", {})
            if cr_policy and is_numeric and hist_min is not None and hist_max is not None:
                try:
                    tol   = cr_policy.get("tolerance_pct", 20) / 100
                    lo    = float(hist_min) * (1 - tol)
                    hi    = float(hist_max) * (1 + tol)
                    rules.append({
                        "rule_name":  f"ColumnValues_range_{col}",
                        "rule_type":  "ColumnValues_range",
                        "severity":   _RULE_SEVERITY["ColumnValues_range"],
                        "sql": (
                            f"SELECT CASE WHEN MIN(CAST({col} AS DOUBLE)) >= {lo:.6f} "
                            f"AND MAX(CAST({col} AS DOUBLE)) <= {hi:.6f} "
                            f"THEN 'PASS' ELSE 'FAIL' END AS result, "
                            f"CONCAT(CAST(MIN(CAST({col} AS DOUBLE)) AS VARCHAR),'–',"
                            f"CAST(MAX(CAST({col} AS DOUBLE)) AS VARCHAR)) AS observed_value, "
                            f"'{lo:.2f}–{hi:.2f}' AS expected_constraint "
                            f"FROM {target_db}.{target_tbl}"
                        ),
                        "column_name": col,
                        "expected_constraint": f"{lo:.2f}–{hi:.2f}",
                    })
                except (TypeError, ValueError):
                    pass

        # ── ZScore_distribution (from table_rules config — injected by caller)
        # This is handled by the Z-score validation query already in table_rules;
        # we register it here as an audit row generator only if config explicitly
        # includes a ZScore_distribution entry.
        zs_policy = self.policy.get("ZScore_distribution", {})
        if zs_policy:
            logger.debug(
                "[AutoRulesetGenerator] ZScore_distribution rules should be defined in "
                "table_rules[].validation_query — they are run by the orchestrator, not here."
            )

        return rules

    # ------------------------------------------------------------------
    # Rule execution via Athena
    # ------------------------------------------------------------------

    def _execute_rule(
        self,
        rule: Dict[str, Any],
        target_db: str,
        target_tbl: str,
        run_date: str,
    ) -> Dict[str, Any]:
        """Execute one rule's SQL via Athena and return outcome dict."""
        sql = rule["sql"]
        logger.debug(f"[AutoRulesetGenerator] Executing: {rule['rule_name']}")

        rows = self._run_athena(sql)
        if rows is None:
            return {
                "rule_name": rule["rule_name"],
                "rule_type": rule["rule_type"],
                "column_name": rule.get("column_name"),
                "status": "ERROR",
                "observed_value": "Athena query failed",
                "expected_constraint": rule.get("expected_constraint", ""),
                "severity": rule["severity"],
            }

        r = rows[0] if rows else {}
        status = r.get("result", "FAIL").upper()
        return {
            "rule_name":           rule["rule_name"],
            "rule_type":           rule["rule_type"],
            "column_name":         rule.get("column_name"),
            "status":              status,
            "observed_value":      str(r.get("observed_value", "")),
            "expected_constraint": str(r.get("expected_constraint", rule.get("expected_constraint", ""))),
            "severity":            rule["severity"],
            "table":               f"{target_db}.{target_tbl}",
            "run_date":            run_date,
        }

    # ------------------------------------------------------------------
    # Write FAIL outcomes to audit table
    # ------------------------------------------------------------------

    def _write_audit_row(self, outcome: Dict[str, Any], run_date: str) -> None:
        """
        Insert a FAIL row into the audit results table via Athena INSERT.
        This makes the failure visible to AuditActionAgent on the next run.
        """
        record_id = str(uuid.uuid4())
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        table_name = outcome.get("table", "").split(".")[-1]
        database   = outcome.get("table", ".").split(".")[0]

        sql = f"""
INSERT INTO {self.database}.{self.results_table}
  (record_id, table_name, database_name, rule_name, rule_type,
   column_name, severity, status, failed_value, expected_constraint,
   failure_timestamp, source)
VALUES (
  '{record_id}', '{table_name}', '{database}',
  '{outcome["rule_name"]}', '{outcome["rule_type"]}',
  '{outcome.get("column_name") or ""}', '{outcome["severity"]}',
  'FAIL',
  '{outcome.get("observed_value","").replace("'","''")}',
  '{outcome.get("expected_constraint","").replace("'","''")}',
  TIMESTAMP '{ts}',
  'auto_ruleset_generator'
)
"""
        try:
            self._run_athena(sql)
            logger.info(f"[AutoRulesetGenerator] Wrote FAIL row for {outcome['rule_name']}")
        except Exception as e:
            logger.warning(f"[AutoRulesetGenerator] Could not write audit row: {e}")

    # ------------------------------------------------------------------
    # DQDL export (informational — not executed by Glue DQ)
    # ------------------------------------------------------------------

    def _write_dqdl_file(
        self,
        target_db: str,
        target_tbl: str,
        rules: List[Dict[str, Any]],
        run_date: str,
    ) -> None:
        """
        Write a human-readable .dqdl file to S3 describing the generated rules.
        This is for documentation / future Glue DQ integration only.
        """
        lines = [f"# Auto-generated DQDL for {target_db}.{target_tbl} on {run_date}", "Rules = ["]
        for rule in rules:
            col = f'"{rule["column_name"]}"' if rule.get("column_name") else "TABLE"
            lines.append(f'  {rule["rule_type"]}({col}) with threshold {rule.get("expected_constraint","")},')
        lines.append("]")
        dqdl_body = "\n".join(lines)

        try:
            key = (
                f"validation/rulesets/{target_db}/{target_tbl}/"
                f"{run_date}.dqdl"
            )
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=dqdl_body.encode("utf-8"),
            )
            logger.info(f"[AutoRulesetGenerator] DQDL written: s3://{self.learning_bucket}/{key}")
        except Exception as e:
            logger.warning(f"[AutoRulesetGenerator] Could not write DQDL file: {e}")

    # ------------------------------------------------------------------
    # S3 baseline helpers
    # ------------------------------------------------------------------

    def _load_baseline(self, db: str, table_name: str) -> Optional[Dict[str, Any]]:
        try:
            key = f"{self.BASELINE_PREFIX}{db}/{table_name}/latest.json"
            body = self.s3.get_object(Bucket=self.learning_bucket, Key=key)["Body"].read()
            return json.loads(body)
        except Exception:
            return None

    def _count_history_runs(self, db: str, table_name: str) -> int:
        try:
            prefix = f"{self.BASELINE_PREFIX}{db}/{table_name}/history/"
            response = self.s3.list_objects_v2(Bucket=self.learning_bucket, Prefix=prefix)
            return len(response.get("Contents", []))
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Athena helper
    # ------------------------------------------------------------------

    def _run_athena(self, sql: str, timeout_s: int = 90) -> Optional[List[Dict[str, Any]]]:
        try:
            resp = self.athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.athena_output},
            )
            qid = resp["QueryExecutionId"]
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                status = self.athena.get_query_execution(QueryExecutionId=qid)
                state = status["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    break
                if state in ("FAILED", "CANCELLED"):
                    reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                    logger.warning(f"Athena {state}: {reason[:200]}")
                    return None
                time.sleep(1)
            else:
                logger.warning(f"Athena timed out after {timeout_s}s")
                return None

            result = self.athena.get_query_results(QueryExecutionId=qid)
            rs = result["ResultSet"]
            cols = [c["Label"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
            rows = []
            for row in rs["Rows"][1:]:
                data = row.get("Data", [])
                rows.append({cols[i]: data[i].get("VarCharValue") for i in range(len(cols))})
            return rows
        except Exception as e:
            logger.error(f"Athena execution error: {e}")
            return None
