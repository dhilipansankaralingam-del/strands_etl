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
        ],
    )
