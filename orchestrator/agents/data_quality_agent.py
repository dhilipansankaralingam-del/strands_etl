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


def create_data_quality_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                               region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for data quality validation."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[run_data_quality_checks, generate_dq_rules_from_schema],
    )
