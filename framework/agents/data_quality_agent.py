#!/usr/bin/env python3
"""
Data Quality Agent
==================

Intelligent agent that performs data quality checks using:
1. Natural Language rules (e.g., "customer_id should not be null")
2. SQL-based rules (e.g., "SELECT COUNT(*) FROM table WHERE amount < 0")
3. Template rules (null checks, uniqueness, referential integrity)
4. Statistical anomaly detection
5. Schema validation

Supports custom rule definitions from configuration.
"""

import re
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class DQRuleType(Enum):
    """Types of data quality rules."""
    NULL_CHECK = "null_check"
    UNIQUE_CHECK = "unique_check"
    RANGE_CHECK = "range_check"
    PATTERN_CHECK = "pattern_check"
    REFERENTIAL_CHECK = "referential_check"
    STATISTICAL_CHECK = "statistical_check"
    CUSTOM_SQL = "custom_sql"
    NATURAL_LANGUAGE = "natural_language"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    FRESHNESS = "freshness"


class DQSeverity(Enum):
    """Severity levels for DQ issues."""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DQStatus(Enum):
    """Status of DQ check."""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class DQRule:
    """A data quality rule."""
    rule_id: str
    rule_type: DQRuleType
    description: str
    column: Optional[str] = None
    table: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: DQSeverity = DQSeverity.ERROR
    threshold: float = 1.0  # Pass threshold (e.g., 0.99 = 99% must pass)
    natural_language: str = ""  # Original NL rule text
    sql_expression: str = ""  # SQL for custom rules


@dataclass
class DQCheckResult:
    """Result of a single DQ check."""
    rule_id: str
    rule_description: str
    status: DQStatus
    records_checked: int = 0
    records_passed: int = 0
    records_failed: int = 0
    pass_rate: float = 0.0
    failed_sample: List[Dict] = field(default_factory=list)
    execution_time_ms: int = 0
    error_message: str = ""


@dataclass
class DQReport:
    """Complete DQ report for a dataset."""
    table_name: str
    timestamp: str
    overall_status: DQStatus
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    warning_rules: int = 0
    results: List[DQCheckResult] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class DataQualityAgent:
    """
    Agent that performs data quality checks using NL, SQL, and templates.
    """

    def __init__(self, config):
        self.config = config
        self.nl_patterns = self._init_nl_patterns()
        self.template_rules = self._init_template_rules()

    def _init_nl_patterns(self) -> List[Dict]:
        """Initialize natural language rule patterns."""
        return [
            # Null checks
            {
                "pattern": r"(\w+)\s+(?:should|must|cannot)\s+(?:not\s+be|never\s+be)\s+(?:null|empty|blank)",
                "rule_type": DQRuleType.NULL_CHECK,
                "extractor": lambda m: {"column": m.group(1)},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE {column} IS NULL OR TRIM({column}) = ''"
            },
            {
                "pattern": r"(\w+)\s+(?:is|are)\s+required",
                "rule_type": DQRuleType.NULL_CHECK,
                "extractor": lambda m: {"column": m.group(1)},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE {column} IS NULL"
            },

            # Uniqueness checks
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+unique",
                "rule_type": DQRuleType.UNIQUE_CHECK,
                "extractor": lambda m: {"column": m.group(1)},
                "sql_template": "SELECT COUNT(*) - COUNT(DISTINCT {column}) as failed FROM {table}"
            },
            {
                "pattern": r"(?:no|zero)\s+duplicate(?:s)?\s+(?:in|for|on)\s+(\w+)",
                "rule_type": DQRuleType.UNIQUE_CHECK,
                "extractor": lambda m: {"column": m.group(1)},
                "sql_template": "SELECT COUNT(*) - COUNT(DISTINCT {column}) as failed FROM {table}"
            },

            # Range checks
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+(?:between|in range)\s+(\d+(?:\.\d+)?)\s+(?:and|to)\s+(\d+(?:\.\d+)?)",
                "rule_type": DQRuleType.RANGE_CHECK,
                "extractor": lambda m: {"column": m.group(1), "min": float(m.group(2)), "max": float(m.group(3))},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE {column} < {min} OR {column} > {max}"
            },
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+(?:greater than|>|>=)\s+(\d+(?:\.\d+)?)",
                "rule_type": DQRuleType.RANGE_CHECK,
                "extractor": lambda m: {"column": m.group(1), "min": float(m.group(2))},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE {column} <= {min}"
            },
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+(?:less than|<|<=)\s+(\d+(?:\.\d+)?)",
                "rule_type": DQRuleType.RANGE_CHECK,
                "extractor": lambda m: {"column": m.group(1), "max": float(m.group(2))},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE {column} >= {max}"
            },
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+(?:positive|non-negative)",
                "rule_type": DQRuleType.RANGE_CHECK,
                "extractor": lambda m: {"column": m.group(1), "min": 0},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE {column} < {min}"
            },

            # Pattern checks
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+(?:match|follow)\s+(?:pattern|format)\s+['\"]?([^'\"]+)['\"]?",
                "rule_type": DQRuleType.PATTERN_CHECK,
                "extractor": lambda m: {"column": m.group(1), "regex_pattern": m.group(2)},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE NOT REGEXP_LIKE({column}, '{regex_pattern}')"
            },
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+(?:a\s+)?valid\s+email",
                "rule_type": DQRuleType.PATTERN_CHECK,
                "extractor": lambda m: {"column": m.group(1), "regex_pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE NOT REGEXP_LIKE({column}, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{{2,}}$')"
            },
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+be\s+(?:a\s+)?valid\s+(?:phone|telephone)",
                "rule_type": DQRuleType.PATTERN_CHECK,
                "extractor": lambda m: {"column": m.group(1), "regex_pattern": r"^\+?[\d\s\-\(\)]{10,}$"},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} WHERE NOT REGEXP_LIKE({column}, '^\\\\+?[\\\\d\\\\s\\\\-\\\\(\\\\)]{{10,}}$')"
            },

            # Completeness checks
            {
                "pattern": r"(\w+)\s+completeness\s+(?:should|must)\s+be\s+(?:at least|>=?)\s+(\d+(?:\.\d+)?)\s*%?",
                "rule_type": DQRuleType.COMPLETENESS,
                "extractor": lambda m: {"column": m.group(1), "threshold": float(m.group(2)) / 100 if float(m.group(2)) > 1 else float(m.group(2))},
                "sql_template": "SELECT 100.0 * SUM(CASE WHEN {column} IS NOT NULL AND TRIM({column}) != '' THEN 1 ELSE 0 END) / COUNT(*) as completeness FROM {table}"
            },

            # Freshness checks
            {
                "pattern": r"data\s+(?:should|must)\s+be\s+(?:no\s+)?(?:older|stale)\s+than\s+(\d+)\s+(hour|day|minute)s?",
                "rule_type": DQRuleType.FRESHNESS,
                "extractor": lambda m: {"max_age": int(m.group(1)), "unit": m.group(2)},
                "sql_template": "SELECT MAX({timestamp_column}) as latest FROM {table}"
            },

            # Referential integrity
            {
                "pattern": r"(\w+)\s+(?:should|must)\s+exist\s+in\s+(\w+)\.(\w+)",
                "rule_type": DQRuleType.REFERENTIAL_CHECK,
                "extractor": lambda m: {"column": m.group(1), "ref_table": m.group(2), "ref_column": m.group(3)},
                "sql_template": "SELECT COUNT(*) as failed FROM {table} t LEFT JOIN {ref_table} r ON t.{column} = r.{ref_column} WHERE r.{ref_column} IS NULL AND t.{column} IS NOT NULL"
            }
        ]

    def _init_template_rules(self) -> Dict[str, Dict]:
        """Initialize template rule definitions."""
        return {
            "null_check": {
                "description": "Check for null values in column {column}",
                "sql": "SELECT COUNT(*) as failed FROM {table} WHERE {column} IS NULL",
                "spark_code": "df.filter(col('{column}').isNull()).count()"
            },
            "unique_check": {
                "description": "Check for duplicate values in column {column}",
                "sql": "SELECT COUNT(*) - COUNT(DISTINCT {column}) as duplicates FROM {table}",
                "spark_code": "df.count() - df.select('{column}').distinct().count()"
            },
            "range_check": {
                "description": "Check if {column} is between {min} and {max}",
                "sql": "SELECT COUNT(*) as failed FROM {table} WHERE {column} < {min} OR {column} > {max}",
                "spark_code": "df.filter((col('{column}') < {min}) | (col('{column}') > {max})).count()"
            },
            "pattern_check": {
                "description": "Check if {column} matches pattern {pattern}",
                "sql": "SELECT COUNT(*) as failed FROM {table} WHERE NOT REGEXP_LIKE({column}, '{pattern}')",
                "spark_code": "df.filter(~col('{column}').rlike('{pattern}')).count()"
            },
            "completeness_check": {
                "description": "Check completeness percentage of column {column}",
                "sql": "SELECT 100.0 * COUNT({column}) / COUNT(*) as completeness FROM {table}",
                "spark_code": "100.0 * df.filter(col('{column}').isNotNull()).count() / df.count()"
            },
            "row_count_check": {
                "description": "Check if row count is within expected range",
                "sql": "SELECT COUNT(*) as row_count FROM {table}",
                "spark_code": "df.count()"
            },
            "statistical_check": {
                "description": "Check statistical properties of column {column}",
                "sql": "SELECT AVG({column}) as mean, STDDEV({column}) as stddev, MIN({column}) as min, MAX({column}) as max FROM {table}",
                "spark_code": "df.select(mean('{column}'), stddev('{column}'), min('{column}'), max('{column}')).collect()[0]"
            }
        }

    def parse_natural_language_rule(self, nl_rule: str, table_name: str) -> Optional[DQRule]:
        """
        Parse a natural language rule into a DQRule object.

        Args:
            nl_rule: Natural language rule text (e.g., "customer_id should not be null")
            table_name: Name of the table to apply the rule to

        Returns:
            DQRule object or None if rule cannot be parsed
        """
        nl_rule_lower = nl_rule.lower().strip()

        for pattern_info in self.nl_patterns:
            match = re.search(pattern_info["pattern"], nl_rule_lower, re.IGNORECASE)
            if match:
                params = pattern_info["extractor"](match)

                # Format SQL template
                sql = pattern_info["sql_template"].format(
                    table=table_name,
                    **params
                )

                return DQRule(
                    rule_id=f"NL_{hash(nl_rule) % 10000:04d}",
                    rule_type=pattern_info["rule_type"],
                    description=nl_rule,
                    column=params.get("column"),
                    table=table_name,
                    parameters=params,
                    natural_language=nl_rule,
                    sql_expression=sql
                )

        return None

    def parse_sql_rule(self, sql_rule: str, rule_id: str, description: str = "") -> DQRule:
        """
        Parse a SQL-based rule.

        Args:
            sql_rule: SQL query that returns failure count or pass/fail status
            rule_id: Unique identifier for the rule
            description: Human-readable description

        Returns:
            DQRule object
        """
        return DQRule(
            rule_id=rule_id,
            rule_type=DQRuleType.CUSTOM_SQL,
            description=description or f"Custom SQL rule: {rule_id}",
            sql_expression=sql_rule,
            severity=DQSeverity.ERROR
        )

    def create_rules_from_config(self, config_rules: Dict[str, Any], table_name: str) -> List[DQRule]:
        """
        Create DQRule objects from configuration.

        Args:
            config_rules: Configuration containing nl_rules, sql_rules, template_rules
            table_name: Name of the table

        Returns:
            List of DQRule objects
        """
        rules = []

        # Parse natural language rules
        for nl_rule in config_rules.get("natural_language_rules", []):
            parsed = self.parse_natural_language_rule(nl_rule, table_name)
            if parsed:
                rules.append(parsed)

        # Parse SQL rules
        for sql_config in config_rules.get("sql_rules", []):
            if isinstance(sql_config, dict):
                rule = self.parse_sql_rule(
                    sql_rule=sql_config.get("sql", ""),
                    rule_id=sql_config.get("id", f"SQL_{len(rules)}"),
                    description=sql_config.get("description", "")
                )
                rule.threshold = sql_config.get("threshold", 1.0)
                rules.append(rule)
            elif isinstance(sql_config, str):
                rules.append(self.parse_sql_rule(sql_config, f"SQL_{len(rules)}"))

        # Parse template rules
        for template_config in config_rules.get("template_rules", []):
            template_name = template_config.get("template")
            if template_name in self.template_rules:
                template = self.template_rules[template_name]
                params = template_config.get("parameters", {})

                sql = template["sql"].format(table=table_name, **params)
                description = template["description"].format(**params)

                # Map template name to DQRuleType
                type_mapping = {
                    "null_check": DQRuleType.NULL_CHECK,
                    "unique_check": DQRuleType.UNIQUE_CHECK,
                    "range_check": DQRuleType.RANGE_CHECK,
                    "pattern_check": DQRuleType.PATTERN_CHECK,
                    "completeness_check": DQRuleType.COMPLETENESS,
                    "row_count_check": DQRuleType.STATISTICAL_CHECK,
                    "statistical_check": DQRuleType.STATISTICAL_CHECK
                }
                rule_type = type_mapping.get(template_name, DQRuleType.CUSTOM_SQL)

                rules.append(DQRule(
                    rule_id=f"TPL_{template_name}_{len(rules)}",
                    rule_type=rule_type,
                    description=description,
                    column=params.get("column"),
                    table=table_name,
                    parameters=params,
                    sql_expression=sql,
                    threshold=template_config.get("threshold", 1.0)
                ))

        return rules

    def generate_spark_check_code(self, rule: DQRule) -> str:
        """
        Generate PySpark code for executing a DQ rule.

        Args:
            rule: DQRule to generate code for

        Returns:
            PySpark code string
        """
        if rule.rule_type == DQRuleType.NULL_CHECK:
            return f"""
# Rule: {rule.description}
null_count = df.filter(col("{rule.column}").isNull() | (trim(col("{rule.column}")) == "")).count()
total_count = df.count()
pass_rate = 1.0 - (null_count / total_count) if total_count > 0 else 1.0
result = {{"rule_id": "{rule.rule_id}", "passed": pass_rate >= {rule.threshold}, "null_count": null_count, "pass_rate": pass_rate}}
"""
        elif rule.rule_type == DQRuleType.UNIQUE_CHECK:
            return f"""
# Rule: {rule.description}
total_count = df.count()
distinct_count = df.select("{rule.column}").distinct().count()
duplicate_count = total_count - distinct_count
pass_rate = distinct_count / total_count if total_count > 0 else 1.0
result = {{"rule_id": "{rule.rule_id}", "passed": duplicate_count == 0, "duplicate_count": duplicate_count, "pass_rate": pass_rate}}
"""
        elif rule.rule_type == DQRuleType.RANGE_CHECK:
            min_val = rule.parameters.get("min", "float('-inf')")
            max_val = rule.parameters.get("max", "float('inf')")
            return f"""
# Rule: {rule.description}
out_of_range = df.filter((col("{rule.column}") < {min_val}) | (col("{rule.column}") > {max_val})).count()
total_count = df.count()
pass_rate = 1.0 - (out_of_range / total_count) if total_count > 0 else 1.0
result = {{"rule_id": "{rule.rule_id}", "passed": pass_rate >= {rule.threshold}, "out_of_range": out_of_range, "pass_rate": pass_rate}}
"""
        elif rule.rule_type == DQRuleType.PATTERN_CHECK:
            pattern = rule.parameters.get("regex_pattern", ".*")
            return f"""
# Rule: {rule.description}
invalid_count = df.filter(~col("{rule.column}").rlike(r"{pattern}")).count()
total_count = df.count()
pass_rate = 1.0 - (invalid_count / total_count) if total_count > 0 else 1.0
result = {{"rule_id": "{rule.rule_id}", "passed": pass_rate >= {rule.threshold}, "invalid_count": invalid_count, "pass_rate": pass_rate}}
"""
        elif rule.rule_type == DQRuleType.CUSTOM_SQL:
            return f"""
# Rule: {rule.description}
# Execute SQL: {rule.sql_expression}
result_df = spark.sql('''{rule.sql_expression}''')
failed_count = result_df.collect()[0][0]
result = {{"rule_id": "{rule.rule_id}", "passed": failed_count == 0, "failed_count": failed_count}}
"""
        else:
            return f"# Unsupported rule type: {rule.rule_type.value}"

    def run_dq_checks(
        self,
        spark_session: Any,
        table_name: str,
        rules: List[DQRule],
        sample_size: Optional[int] = None
    ) -> DQReport:
        """
        Run data quality checks on a table.

        Args:
            spark_session: Active SparkSession
            table_name: Name of the table to check
            rules: List of DQRule objects to execute
            sample_size: Optional sample size for large tables

        Returns:
            DQReport with all check results
        """
        report = DQReport(
            table_name=table_name,
            timestamp=datetime.utcnow().isoformat(),
            overall_status=DQStatus.PASSED,
            total_rules=len(rules)
        )

        for rule in rules:
            start_time = datetime.utcnow()

            try:
                if rule.sql_expression:
                    # Execute SQL rule
                    result_df = spark_session.sql(rule.sql_expression)
                    result = result_df.collect()[0]

                    # Interpret result based on rule type
                    if rule.rule_type == DQRuleType.COMPLETENESS:
                        completeness = float(result[0]) if result[0] else 0.0
                        threshold_pct = rule.threshold * 100
                        passed = completeness >= threshold_pct
                        check_result = DQCheckResult(
                            rule_id=rule.rule_id,
                            rule_description=rule.description,
                            status=DQStatus.PASSED if passed else DQStatus.FAILED,
                            pass_rate=completeness / 100,
                            records_checked=1,
                            records_passed=1 if passed else 0,
                            records_failed=0 if passed else 1
                        )
                    else:
                        # Assume result is failure count
                        failed_count = int(result[0]) if result[0] else 0
                        total_count = spark_session.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
                        pass_rate = 1.0 - (failed_count / total_count) if total_count > 0 else 1.0

                        check_result = DQCheckResult(
                            rule_id=rule.rule_id,
                            rule_description=rule.description,
                            status=DQStatus.PASSED if pass_rate >= rule.threshold else DQStatus.FAILED,
                            records_checked=total_count,
                            records_passed=total_count - failed_count,
                            records_failed=failed_count,
                            pass_rate=pass_rate
                        )
                else:
                    check_result = DQCheckResult(
                        rule_id=rule.rule_id,
                        rule_description=rule.description,
                        status=DQStatus.SKIPPED,
                        error_message="No SQL expression defined"
                    )

            except Exception as e:
                check_result = DQCheckResult(
                    rule_id=rule.rule_id,
                    rule_description=rule.description,
                    status=DQStatus.ERROR,
                    error_message=str(e)
                )

            end_time = datetime.utcnow()
            check_result.execution_time_ms = int((end_time - start_time).total_seconds() * 1000)
            report.results.append(check_result)

            # Update counters
            if check_result.status == DQStatus.PASSED:
                report.passed_rules += 1
            elif check_result.status == DQStatus.FAILED:
                report.failed_rules += 1
            elif check_result.status == DQStatus.WARNING:
                report.warning_rules += 1

        # Determine overall status
        if report.failed_rules > 0:
            report.overall_status = DQStatus.FAILED
        elif report.warning_rules > 0:
            report.overall_status = DQStatus.WARNING
        else:
            report.overall_status = DQStatus.PASSED

        # Generate summary and recommendations
        report.summary = self._generate_summary(report)
        report.recommendations = self._generate_recommendations(report)

        return report

    def _generate_summary(self, report: DQReport) -> Dict[str, Any]:
        """Generate summary statistics for the report."""
        return {
            "total_rules": report.total_rules,
            "passed": report.passed_rules,
            "failed": report.failed_rules,
            "warnings": report.warning_rules,
            "pass_rate": report.passed_rules / report.total_rules if report.total_rules > 0 else 1.0,
            "critical_failures": [
                r.rule_id for r in report.results
                if r.status == DQStatus.FAILED
            ],
            "by_type": self._group_by_type(report.results)
        }

    def _group_by_type(self, results: List[DQCheckResult]) -> Dict[str, Dict]:
        """Group results by rule type."""
        groups = {}
        for result in results:
            rule_type = result.rule_id.split("_")[0]
            if rule_type not in groups:
                groups[rule_type] = {"passed": 0, "failed": 0, "total": 0}
            groups[rule_type]["total"] += 1
            if result.status == DQStatus.PASSED:
                groups[rule_type]["passed"] += 1
            elif result.status == DQStatus.FAILED:
                groups[rule_type]["failed"] += 1
        return groups

    def _generate_recommendations(self, report: DQReport) -> List[str]:
        """Generate recommendations based on DQ results."""
        recommendations = []

        for result in report.results:
            if result.status == DQStatus.FAILED:
                if "null" in result.rule_id.lower() or "null" in result.rule_description.lower():
                    recommendations.append(
                        f"Consider adding COALESCE or default values for column with null issues "
                        f"({result.rule_id}: {result.records_failed} nulls found)"
                    )
                elif "unique" in result.rule_id.lower() or "duplicate" in result.rule_description.lower():
                    recommendations.append(
                        f"Review deduplication logic for column with duplicates "
                        f"({result.rule_id}: {result.records_failed} duplicates found)"
                    )
                elif "range" in result.rule_id.lower():
                    recommendations.append(
                        f"Add data validation in source system for out-of-range values "
                        f"({result.rule_id}: {result.records_failed} violations)"
                    )

        if report.failed_rules > report.total_rules * 0.3:
            recommendations.append(
                "High failure rate detected (>30%). Consider reviewing data pipeline and source data quality."
            )

        return recommendations

    def generate_report_html(self, report: DQReport) -> str:
        """Generate HTML report for data quality results."""
        status_color = {
            DQStatus.PASSED: "#28a745",
            DQStatus.FAILED: "#dc3545",
            DQStatus.WARNING: "#ffc107",
            DQStatus.SKIPPED: "#6c757d",
            DQStatus.ERROR: "#dc3545"
        }

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - {report.table_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #343a40; color: white; padding: 20px; border-radius: 5px; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .card {{ background: #f8f9fa; padding: 15px; border-radius: 5px; flex: 1; }}
        .card h3 {{ margin: 0 0 10px 0; }}
        .passed {{ color: #28a745; }}
        .failed {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #dee2e6; padding: 12px; text-align: left; }}
        th {{ background: #343a40; color: white; }}
        tr:nth-child(even) {{ background: #f8f9fa; }}
        .status-badge {{ padding: 4px 8px; border-radius: 4px; color: white; }}
        .recommendations {{ background: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report</h1>
        <p>Table: {report.table_name} | Generated: {report.timestamp}</p>
    </div>

    <div class="summary">
        <div class="card">
            <h3>Overall Status</h3>
            <span class="status-badge" style="background: {status_color[report.overall_status]}">
                {report.overall_status.value.upper()}
            </span>
        </div>
        <div class="card">
            <h3>Rules Passed</h3>
            <span class="passed">{report.passed_rules}</span> / {report.total_rules}
        </div>
        <div class="card">
            <h3>Rules Failed</h3>
            <span class="failed">{report.failed_rules}</span>
        </div>
        <div class="card">
            <h3>Pass Rate</h3>
            {report.passed_rules / report.total_rules * 100:.1f}%
        </div>
    </div>

    <h2>Rule Results</h2>
    <table>
        <tr>
            <th>Rule ID</th>
            <th>Description</th>
            <th>Status</th>
            <th>Pass Rate</th>
            <th>Failed Records</th>
            <th>Execution Time</th>
        </tr>
"""

        for result in report.results:
            html += f"""
        <tr>
            <td>{result.rule_id}</td>
            <td>{result.rule_description}</td>
            <td><span class="status-badge" style="background: {status_color[result.status]}">{result.status.value}</span></td>
            <td>{result.pass_rate:.1%}</td>
            <td>{result.records_failed}</td>
            <td>{result.execution_time_ms}ms</td>
        </tr>
"""

        html += """
    </table>
"""

        if report.recommendations:
            html += """
    <div class="recommendations">
        <h2>Recommendations</h2>
        <ul>
"""
            for rec in report.recommendations:
                html += f"            <li>{rec}</li>\n"
            html += """
        </ul>
    </div>
"""

        html += """
</body>
</html>
"""
        return html
