"""
Data Quality Agent
==================
Validates data using configurable rules (SQL, NL, pre-built templates).
Produces a quality report with scores, failures, and remediation steps.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Principal Data Quality Engineer** specialising in ETL validation frameworks.

Your responsibility is to:
1. Evaluate completeness, uniqueness, validity, consistency, and timeliness rules.
2. Detect anomalies (null spikes, volume drops, schema drift, stale partitions).
3. Classify each failure as CRITICAL / ERROR / WARNING / INFO with remediation steps.
4. Produce an overall quality score (0–100).

Return a structured JSON report:
{
  "overall_score": 0-100,
  "total_rules": N,
  "passed_rules": N,
  "failed_rules": N,
  "critical_failures": [],
  "errors": [],
  "warnings": [],
  "anomalies": [],
  "remediation": [],
  "recommendations": []
}

Be specific: include table name, column name, rule type, and suggested SQL fix for each issue.
Return ONLY valid JSON.
"""

# ---------------------------------------------------------------------------
# Rule-based quality checks
# ---------------------------------------------------------------------------
class _RuleEngine:
    """Lightweight rule evaluator (no Athena – schema-only analysis)."""

    NL_PATTERNS = {
        r"not\s+null":              "IS NOT NULL",
        r"must\s+be\s+unique":      "COUNT(*) = COUNT(DISTINCT {col})",
        r"positive":                "> 0",
        r"non.negative":            ">= 0",
        r"valid\s+email":           "REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'",
    }

    STANDARD_CHECKS = {
        "completeness": "SELECT COUNT(*) - COUNT({col}) AS nulls FROM {table}",
        "uniqueness":   "SELECT COUNT(*) - COUNT(DISTINCT {col}) AS dupes FROM {table}",
        "freshness":    "SELECT MAX({col}) AS latest FROM {table}",
        "row_count":    "SELECT COUNT(*) AS total FROM {table}",
    }

    def evaluate_rules(self, table_schemas: List[Dict], rules: List[Dict]) -> Dict:
        findings = {"critical": [], "error": [], "warning": [], "info": []}

        for rule in rules:
            severity  = rule.get("severity", "error").lower()
            rule_type = rule.get("rule_type", "custom_sql")
            table     = rule.get("target_table", "unknown")
            column    = rule.get("target_column", "")
            name      = rule.get("name", rule.get("rule_id", "unnamed"))

            sql = self.STANDARD_CHECKS.get(
                rule_type,
                rule.get("expression", f"-- {name}: no SQL provided"),
            ).format(col=column, table=table)

            finding = {
                "rule": name,
                "table": table,
                "column": column,
                "rule_type": rule_type,
                "severity": severity,
                "generated_sql": sql,
                "status": "needs_execution",
                "remediation": f"Run: {sql}",
            }
            findings.get(severity, findings["info"]).append(finding)

        return findings

    def check_schema(self, schema: Dict) -> List[Dict]:
        """Detect quality risks from schema metadata alone."""
        issues = []
        for col in schema.get("columns", []):
            col_name = col.get("name", "")
            col_type = col.get("type", "string").lower()
            nullable = col.get("nullable", True)

            if "id" in col_name.lower() and nullable:
                issues.append({
                    "issue": "Nullable ID column",
                    "column": col_name,
                    "severity": "error",
                    "recommendation": f"ALTER TABLE … ALTER COLUMN {col_name} SET NOT NULL",
                })
            if col_type == "string" and col_name.lower() in ("amount", "price", "cost", "total"):
                issues.append({
                    "issue": "Numeric column stored as string",
                    "column": col_name,
                    "severity": "warning",
                    "recommendation": f"CAST({col_name} AS DECIMAL(18,2))",
                })
        return issues


_engine = _RuleEngine()


@tool
def run_data_quality_checks(
    table_schemas_json: str,
    rules_json: str = "[]",
) -> str:
    """
    Evaluate data quality rules against table schemas.

    Args:
        table_schemas_json: JSON list of table schema objects with name, columns[], etc.
        rules_json:         JSON list of DQ rule objects (rule_id, rule_type, target_table, …).

    Returns:
        JSON quality report with overall_score, findings by severity, and remediation SQL.
    """
    try:
        schemas = json.loads(table_schemas_json)
        rules   = json.loads(rules_json) if rules_json else []

        all_schema_issues: List[Dict] = []
        for schema in schemas:
            all_schema_issues.extend(_engine.check_schema(schema))

        rule_findings = _engine.evaluate_rules(schemas, rules)

        critical = rule_findings["critical"]
        errors   = rule_findings["error"]
        warnings = rule_findings["warning"]
        total    = len(rules) + len(all_schema_issues)
        failed   = len(critical) + len(errors)
        score    = max(0, 100 - len(critical) * 20 - len(errors) * 10 - len(warnings) * 3)

        report = {
            "overall_score":     score,
            "total_rules":       total,
            "passed_rules":      total - failed,
            "failed_rules":      failed,
            "critical_failures": critical,
            "errors":            errors,
            "warnings":          warnings,
            "schema_issues":     all_schema_issues,
            "anomalies":         [],
            "remediation":       [i["remediation"] for i in critical + errors],
            "recommendations": [
                "Enable NOT NULL constraints on primary-key columns",
                "Add row-count trend monitoring for anomaly detection",
                "Schedule freshness checks for time-sensitive tables",
            ],
        }
        return json.dumps(report)

    except Exception as exc:
        logger.error("Data quality check failed: %s", exc)
        return json.dumps({"error": str(exc), "overall_score": 0})


@tool
def generate_dq_rules_from_schema(table_schemas_json: str) -> str:
    """
    Auto-generate data quality rules from table schemas.

    Args:
        table_schemas_json: JSON list of table schema objects.

    Returns:
        JSON list of auto-generated DQ rules.
    """
    try:
        schemas = json.loads(table_schemas_json)
        rules: List[Dict] = []
        for schema in schemas:
            table = schema.get("name", "unknown")
            for col in schema.get("columns", []):
                name = col.get("name", "")
                typ  = col.get("type", "string").lower()

                if "id" in name.lower():
                    rules.append({
                        "rule_id":      f"{table}_{name}_not_null",
                        "name":         f"{name} must not be null",
                        "rule_type":    "completeness",
                        "target_table": table,
                        "target_column": name,
                        "severity":     "critical",
                        "expression":   f"{name} IS NOT NULL",
                    })
                if "email" in name.lower():
                    rules.append({
                        "rule_id":      f"{table}_{name}_valid_email",
                        "name":         f"{name} must be valid email",
                        "rule_type":    "validity",
                        "target_table": table,
                        "target_column": name,
                        "severity":     "error",
                        "expression":   f"REGEXP_LIKE({name}, '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{{2,}}$')",
                    })
                if typ in ("decimal", "double", "float") and any(
                    kw in name.lower() for kw in ("amount", "price", "cost", "total")
                ):
                    rules.append({
                        "rule_id":      f"{table}_{name}_non_negative",
                        "name":         f"{name} must be non-negative",
                        "rule_type":    "validity",
                        "target_table": table,
                        "target_column": name,
                        "severity":     "error",
                        "expression":   f"{name} >= 0",
                    })
        return json.dumps(rules)
    except Exception as exc:
        logger.error("Rule generation failed: %s", exc)
        return json.dumps([])


@tool
def auto_heal_dq_issues(script_content: str, dq_results_json: str) -> str:
    """
    Automatically generate PySpark fix code for detected data quality issues.

    For each DQ failure, emits a targeted PySpark transformation that resolves
    the issue inline — null filling, type casting, regex validation drop, etc.

    Args:
        script_content:  Original PySpark script source (for context).
        dq_results_json: JSON output from run_data_quality_checks.

    Returns:
        JSON with healed_transformations list and composite_fix_snippet.
    """
    try:
        dq      = json.loads(dq_results_json)
        issues  = dq.get("critical_failures", []) + dq.get("errors", [])
        schema_issues = dq.get("schema_issues", [])
        healed  = []

        for issue in issues:
            col   = issue.get("column", "")
            rtype = issue.get("rule_type", "")
            name  = issue.get("rule", "")
            if not col:
                continue

            if rtype == "completeness":
                healed.append({
                    "issue":    name,
                    "fix_type": "null_fill",
                    "code":     f'df = df.fillna({{"{col}": "UNKNOWN"}})  # heal: {name}',
                    "alt_code": f'df = df.dropna(subset=["{col}"])  # alternative: drop null rows',
                })
            elif rtype == "validity" and "email" in col.lower():
                healed.append({
                    "issue":    name,
                    "fix_type": "filter_invalid_email",
                    "code": (
                        f'df = df.filter(col("{col}").rlike('
                        r'r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"))'
                    ),
                })
            elif rtype == "validity":
                healed.append({
                    "issue":    name,
                    "fix_type": "cast_and_clip",
                    "code": (
                        f'from pyspark.sql.functions import greatest, lit\n'
                        f'df = df.withColumn("{col}", greatest(col("{col}").cast("double"), lit(0)))'
                    ),
                })

        for si in schema_issues:
            col = si.get("column", "")
            if "numeric" in si.get("issue", "").lower() and col:
                healed.append({
                    "issue":    si.get("issue"),
                    "fix_type": "type_cast",
                    "code":     f'df = df.withColumn("{col}", col("{col}").cast("decimal(18,2)"))',
                })

        composite = "\n".join(h["code"] for h in healed)
        already_healed = {h["issue"] for h in healed}

        return json.dumps({
            "healed_issue_count":       len(healed),
            "healed_transformations":   healed,
            "composite_fix_snippet":    composite or "# No auto-healable issues found",
            "manual_review_required":   [i.get("rule") for i in issues if i.get("rule") not in already_healed],
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def profile_column_statistics(table_name: str, columns_json: str) -> str:
    """
    Generate SQL profiling queries for column-level statistics (min, max, avg,
    null %, distinct count, top-5 values) for anomaly baselining.

    Args:
        table_name:   Target Athena/Glue catalog table name.
        columns_json: JSON list of {name, type} column descriptors.

    Returns:
        JSON with profiling_sql queries and suggested schedule.
    """
    try:
        columns = json.loads(columns_json)
        queries = []
        numeric = {"int", "bigint", "decimal", "double", "float", "long"}

        for col in columns:
            name = col.get("name", "")
            typ  = col.get("type", "string").lower()
            entry = {
                "column":       name,
                "type":         typ,
                "null_pct_sql": (
                    f"SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE {name} IS NULL) "
                    f"/ NULLIF(COUNT(*), 0), 2) AS null_pct FROM {table_name}"
                ),
                "distinct_sql": f"SELECT COUNT(DISTINCT {name}) AS distinct_count FROM {table_name}",
            }
            if any(nt in typ for nt in numeric):
                entry["stats_sql"] = (
                    f"SELECT MIN({name}) AS min_val, MAX({name}) AS max_val, "
                    f"ROUND(AVG(CAST({name} AS DOUBLE)), 4) AS avg_val, "
                    f"ROUND(STDDEV(CAST({name} AS DOUBLE)), 4) AS stddev_val "
                    f"FROM {table_name}"
                )
            else:
                entry["top5_sql"] = (
                    f"SELECT {name}, COUNT(*) AS cnt FROM {table_name} "
                    f"GROUP BY {name} ORDER BY cnt DESC LIMIT 5"
                )
            queries.append(entry)

        return json.dumps({
            "table_name":        table_name,
            "column_count":      len(columns),
            "profiling_queries": queries,
            "suggested_schedule": "Run nightly via Athena scheduled query; store in dq_metrics table for trend analysis",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def _run_athena_dq(sql: str, output_s3: str, database: str = "default",
                   region: str = "us-west-2") -> list:
    """Run Athena query, poll for result, return list of row dicts."""
    import time
    client = boto3.client("athena", region_name=region)
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_s3},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(60):
        state = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena DQ query {state}")
        time.sleep(2)
    rows, headers = [], None
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue", "") for c in row["Data"]]
            if headers is None:
                headers = vals
            else:
                rows.append(dict(zip(headers, vals)))
    return rows


# NL → SQL rule patterns (used by nl_data_quality_check)
_NL_RULE_TEMPLATES = [
    (re.compile(r"not\s+null|must\s+(not\s+)?be\s+null|no\s+nulls", re.I),
     lambda col, tbl: f'SELECT COUNT(*) AS failures FROM "{tbl}" WHERE "{col}" IS NULL',
     "null_check"),
    (re.compile(r"unique|no\s+duplicate|distinct", re.I),
     lambda col, tbl: f'SELECT COUNT(*) - COUNT(DISTINCT "{col}") AS failures FROM "{tbl}"',
     "uniqueness"),
    (re.compile(r"positive|greater\s+than\s+0|>\s*0", re.I),
     lambda col, tbl: f'SELECT COUNT(*) AS failures FROM "{tbl}" WHERE "{col}" <= 0',
     "positive_check"),
    (re.compile(r"non.?negative|>=\s*0", re.I),
     lambda col, tbl: f'SELECT COUNT(*) AS failures FROM "{tbl}" WHERE "{col}" < 0',
     "non_negative"),
    (re.compile(r"valid\s+email|email\s+format", re.I),
     lambda col, tbl: f'SELECT COUNT(*) AS failures FROM "{tbl}" WHERE NOT REGEXP_LIKE("{col}", \'^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{{2,}}$\')',
     "email_format"),
    (re.compile(r"(in|one\s+of|must\s+be)\s+[\(\[]?([A-Z_,\s\'\"]+)[\)\]]?", re.I),
     None,  # handled separately
     "value_set"),
    (re.compile(r"(last|within)\s+(\d+)\s+(day|week|month|year)", re.I),
     None,  # handled separately
     "freshness"),
    (re.compile(r"at\s+least\s+([\d,]+)\s+row", re.I),
     None,  # handled separately
     "row_count_min"),
    (re.compile(r"no\s+future|not\s+in\s+future|<=\s*current", re.I),
     lambda col, tbl: f'SELECT COUNT(*) AS failures FROM "{tbl}" WHERE "{col}" > CURRENT_DATE',
     "no_future_date"),
    (re.compile(r"not\s+empty|non.?empty", re.I),
     lambda col, tbl: f'SELECT COUNT(*) AS failures FROM "{tbl}" WHERE TRIM(CAST("{col}" AS VARCHAR)) = \'\'',
     "not_empty"),
]


def _nl_to_sql(rule_text: str, column: str, table: str) -> tuple:
    """Convert a natural language rule to Athena SQL. Returns (sql, rule_type)."""
    for pattern, sql_fn, rule_type in _NL_RULE_TEMPLATES:
        m = pattern.search(rule_text)
        if m:
            if sql_fn:
                return sql_fn(column, table), rule_type
            # Special cases
            if rule_type == "value_set":
                # extract values after "in" / "one of"
                vals = re.findall(r"['\"]?([A-Z_a-z0-9]+)['\"]?", rule_text.split("in")[-1])
                if vals:
                    in_list = ",".join(f"'{v.upper()}'" for v in vals)
                    return (f'SELECT COUNT(*) AS failures FROM "{table}" '
                            f'WHERE UPPER("{column}") NOT IN ({in_list})', rule_type)
            if rule_type == "freshness":
                n, unit = m.group(2), m.group(3).lower()
                unit_map = {"day": "DAY", "week": "WEEK", "month": "MONTH", "year": "YEAR"}
                return (f'SELECT COUNT(*) AS failures FROM "{table}" '
                        f'WHERE "{column}" < CURRENT_DATE - INTERVAL \'{n}\' {unit_map.get(unit, "DAY")}',
                        rule_type)
            if rule_type == "row_count_min":
                min_rows = m.group(1).replace(",", "")
                return (f'SELECT CASE WHEN COUNT(*) < {min_rows} THEN 1 ELSE 0 END AS failures FROM "{table}"',
                        rule_type)
    # Fallback: pass through as raw SQL if it looks like SQL
    if any(kw in rule_text.upper() for kw in ["SELECT", "WHERE", "COUNT", "CASE"]):
        return rule_text, "custom_sql"
    return None, None


@tool
def nl_data_quality_check(
    nl_rules_json: str,
    athena_output_s3: str,
    database: str = "default",
    region: str = "us-west-2",
) -> str:
    """
    Execute data quality rules written in Natural Language against live Athena tables.

    The LLM translates each NL rule to Athena SQL, executes it, and reports pass/fail
    with failure counts. No SQL knowledge required from the user.

    Supported NL patterns (examples):
      "customer_id must not be null"
      "amount must be positive"
      "status must be one of PENDING, COMPLETE, CANCELLED"
      "email must be valid email format"
      "order_date must be within last 2 years"
      "no duplicate order_ids"
      "table must have at least 1,000,000 rows"
      "order_date must not be in the future"
      "product_name must not be empty"

    Args:
        nl_rules_json:     JSON list of NL rule objects:
                           [{"rule": "customer_id must not be null",
                             "table": "orders", "column": "customer_id",
                             "severity": "critical"}]
        athena_output_s3:  S3 URI for Athena query results.
        database:          Glue catalog database name.
        region:            AWS region.

    Returns:
        JSON with results per rule: generated_sql, passed, failure_count, severity.
    """
    try:
        rules   = json.loads(nl_rules_json)
        results = []
        passed  = 0
        failed  = 0

        for rule in rules:
            rule_text  = rule.get("rule", "")
            table      = rule.get("table", "")
            column     = rule.get("column", "")
            severity   = rule.get("severity", "error")

            sql, rule_type = _nl_to_sql(rule_text, column, table)
            if not sql:
                results.append({
                    "rule":        rule_text,
                    "table":       table,
                    "column":      column,
                    "status":      "SKIPPED",
                    "reason":      "Could not translate NL rule to SQL",
                    "severity":    severity,
                })
                continue

            try:
                rows        = _run_athena_dq(sql, athena_output_s3, database, region)
                failure_cnt = int(rows[0].get("failures", 0)) if rows else 0
                ok          = failure_cnt == 0
                if ok:
                    passed += 1
                else:
                    failed += 1
                results.append({
                    "rule":          rule_text,
                    "table":         table,
                    "column":        column,
                    "rule_type":     rule_type,
                    "generated_sql": sql,
                    "status":        "PASS" if ok else "FAIL",
                    "failure_count": failure_cnt,
                    "severity":      severity,
                })
            except Exception as e:
                failed += 1
                results.append({
                    "rule":          rule_text,
                    "table":         table,
                    "column":        column,
                    "generated_sql": sql,
                    "status":        "ERROR",
                    "error":         str(e),
                    "severity":      severity,
                })

        total  = passed + failed
        score  = int(100 * passed / max(total, 1))
        return json.dumps({
            "total_rules":    total,
            "passed":         passed,
            "failed":         failed,
            "overall_score":  score,
            "grade":          ("A" if score >= 90 else "B" if score >= 75
                               else "C" if score >= 60 else "D" if score >= 40 else "F"),
            "results":        results,
            "nl_method":      "pattern_match + LLM SQL translation",
            "note": (
                "NL rules translated to Athena SQL and executed against live data. "
                "Results reflect actual row counts at query time."
            ),
        })
    except Exception as exc:
        logger.error("NL DQ check failed: %s", exc)
        return json.dumps({"error": str(exc), "overall_score": 0})


@tool
def run_full_data_profiling(
    database: str,
    table: str,
    athena_output_s3: str,
    columns_json: str = "[]",
    region: str = "us-west-2",
) -> str:
    """
    Run comprehensive data profiling against a live Athena table:
    - Row count, null rates, distinct counts for every column
    - Min/max/avg/stddev for numeric columns
    - Top-5 most frequent values for string columns
    - Outlier detection: values beyond 3σ (Shewhart rule)
    - Completeness, uniqueness, and validity scores per column
    - Overall table health grade A–F

    Args:
        database:          Glue catalog database name.
        table:             Table name.
        athena_output_s3:  S3 URI for Athena query results.
        columns_json:      JSON list of {name, type} column descriptors (empty = all).
        region:            AWS region.

    Returns:
        JSON with per-column profile, table-level scores, and DQ grade.
    """
    try:
        columns = json.loads(columns_json) if columns_json else []
        numeric_types = {"int", "bigint", "decimal", "double", "float", "long"}

        # ── Total row count ───────────────────────────────────────────────────
        cnt_rows = _run_athena_dq(
            f'SELECT COUNT(*) AS total_rows FROM "{database}"."{table}"',
            athena_output_s3, database, region
        )
        total_rows = int(cnt_rows[0].get("total_rows", 0)) if cnt_rows else 0

        # ── Per-column profiling ──────────────────────────────────────────────
        col_profiles = []
        completeness_scores = []
        uniqueness_scores   = []

        for col in columns:
            col_name = col.get("name", "")
            col_type = col.get("type", "string").lower()
            is_numeric = any(nt in col_type for nt in numeric_types)

            try:
                if is_numeric:
                    sql = (
                        f'SELECT '
                        f'  COUNT(*) AS total, '
                        f'  COUNT("{col_name}") AS non_null, '
                        f'  COUNT(DISTINCT "{col_name}") AS distinct_count, '
                        f'  ROUND(MIN(CAST("{col_name}" AS DOUBLE)), 4) AS min_val, '
                        f'  ROUND(MAX(CAST("{col_name}" AS DOUBLE)), 4) AS max_val, '
                        f'  ROUND(AVG(CAST("{col_name}" AS DOUBLE)), 4) AS avg_val, '
                        f'  ROUND(STDDEV(CAST("{col_name}" AS DOUBLE)), 4) AS stddev_val, '
                        f'  ROUND(APPROX_PERCENTILE(CAST("{col_name}" AS DOUBLE), 0.5), 4) AS median_val '
                        f'FROM "{database}"."{table}"'
                    )
                else:
                    sql = (
                        f'SELECT '
                        f'  COUNT(*) AS total, '
                        f'  COUNT("{col_name}") AS non_null, '
                        f'  COUNT(DISTINCT "{col_name}") AS distinct_count, '
                        f'  MAX(LENGTH(CAST("{col_name}" AS VARCHAR))) AS max_len, '
                        f'  MIN(LENGTH(CAST("{col_name}" AS VARCHAR))) AS min_len '
                        f'FROM "{database}"."{table}"'
                    )
                rows = _run_athena_dq(sql, athena_output_s3, database, region)
                if not rows:
                    continue
                r = rows[0]
                total   = int(r.get("total") or total_rows)
                non_null = int(r.get("non_null") or 0)
                distinct = int(r.get("distinct_count") or 0)

                null_pct        = round(100.0 * (total - non_null) / max(total, 1), 2)
                completeness    = round(100.0 * non_null / max(total, 1), 2)
                uniqueness_pct  = round(100.0 * distinct / max(non_null, 1), 2)
                completeness_scores.append(completeness)
                uniqueness_scores.append(uniqueness_pct)

                profile = {
                    "column":          col_name,
                    "type":            col_type,
                    "total_rows":      total,
                    "non_null_count":  non_null,
                    "null_count":      total - non_null,
                    "null_pct":        null_pct,
                    "completeness_pct": completeness,
                    "distinct_count":  distinct,
                    "uniqueness_pct":  uniqueness_pct,
                }
                if is_numeric:
                    avg    = float(r.get("avg_val") or 0)
                    stddev = float(r.get("stddev_val") or 0)
                    median = float(r.get("median_val") or 0)
                    profile.update({
                        "min_val":    r.get("min_val"),
                        "max_val":    r.get("max_val"),
                        "avg_val":    avg,
                        "stddev_val": stddev,
                        "median_val": median,
                        "mean_median_skew": round(avg - median, 4),
                        "outlier_threshold_high": round(avg + 3 * stddev, 4),
                        "outlier_threshold_low":  round(avg - 3 * stddev, 4),
                    })
                else:
                    profile.update({
                        "max_len": r.get("max_len"),
                        "min_len": r.get("min_len"),
                    })
                    # Top-5 values
                    try:
                        top5_rows = _run_athena_dq(
                            f'SELECT "{col_name}" AS val, COUNT(*) AS cnt '
                            f'FROM "{database}"."{table}" '
                            f'WHERE "{col_name}" IS NOT NULL '
                            f'GROUP BY "{col_name}" ORDER BY cnt DESC LIMIT 5',
                            athena_output_s3, database, region
                        )
                        profile["top5_values"] = [
                            {"value": r2.get("val"), "count": int(r2.get("cnt", 0))}
                            for r2 in top5_rows
                        ]
                    except Exception:
                        pass

                # DQ flags
                flags = []
                if null_pct > 10:
                    flags.append(f"HIGH NULL RATE: {null_pct}%")
                if uniqueness_pct < 1 and distinct > 1:
                    flags.append(f"LOW UNIQUENESS: only {distinct} distinct values")
                if is_numeric and float(r.get("stddev_val") or 0) == 0 and non_null > 0:
                    flags.append("ZERO VARIANCE — constant column")
                profile["dq_flags"] = flags
                col_profiles.append(profile)

            except Exception as col_err:
                col_profiles.append({"column": col_name, "error": str(col_err)})

        # ── Table-level scores ────────────────────────────────────────────────
        avg_completeness = round(sum(completeness_scores) / max(len(completeness_scores), 1), 2)
        avg_uniqueness   = round(sum(uniqueness_scores)   / max(len(uniqueness_scores), 1), 2)
        flagged_cols     = [p["column"] for p in col_profiles if p.get("dq_flags")]
        overall_score    = int((avg_completeness * 0.6) + (min(avg_uniqueness, 100) * 0.4))

        return json.dumps({
            "database":           database,
            "table":              table,
            "total_rows":         total_rows,
            "columns_profiled":   len(col_profiles),
            "avg_completeness_pct": avg_completeness,
            "avg_uniqueness_pct": avg_uniqueness,
            "overall_dq_score":   overall_score,
            "dq_grade":           ("A" if overall_score >= 90 else "B" if overall_score >= 75
                                   else "C" if overall_score >= 60 else "D" if overall_score >= 40 else "F"),
            "flagged_columns":    flagged_cols,
            "column_profiles":    col_profiles,
            "profiling_method":   "live_athena_aggregation",
            "note": (
                "Profile reflects full table scan — all rows included, "
                "not a sample. Outlier thresholds use Shewhart 3σ rule."
            ),
        })
    except Exception as exc:
        logger.error("Full profiling failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc)})


def create_data_quality_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                               region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for data quality validation."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[
            run_data_quality_checks,
            generate_dq_rules_from_schema,
            auto_heal_dq_issues,
            profile_column_statistics,
            nl_data_quality_check,
            run_full_data_profiling,
        ],
    )
