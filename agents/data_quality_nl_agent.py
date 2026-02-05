#!/usr/bin/env python3
"""
Natural Language + SQL Data Quality Agent
==========================================

This agent allows defining data quality rules using:
1. Natural Language descriptions (converted to SQL)
2. Direct SQL expressions
3. Pre-built rule templates

The agent can:
- Parse natural language DQ rules and convert to SQL
- Execute SQL-based validations on PySpark DataFrames
- Generate DQ reports with pass/fail metrics
- Quarantine failed records
- Send notifications on DQ failures

Usage:
    from agents.data_quality_nl_agent import DataQualityNLAgent

    agent = DataQualityNLAgent(spark, config)

    # Natural language rule
    agent.add_rule_nl("customer_id should not be null")
    agent.add_rule_nl("transaction_amount must be between 0 and 100000")
    agent.add_rule_nl("email should be unique")
    agent.add_rule_nl("created_date should not be in the future")

    # SQL rule
    agent.add_rule_sql("valid_email", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'")

    # Execute and get results
    results = agent.validate(df)
"""

import re
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, expr, when, count, sum as spark_sum,
    current_timestamp, current_date
)


class RuleType(Enum):
    """Types of data quality rules."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    REFERENTIAL = "referential"
    CUSTOM_SQL = "custom_sql"
    FRESHNESS = "freshness"
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"


@dataclass
class DQRule:
    """Data quality rule definition."""
    name: str
    rule_type: RuleType
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    sql_expression: Optional[str] = None
    description: str = ""
    severity: str = "error"  # error, warning, info
    threshold: float = 1.0  # Pass if >= threshold
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DQResult:
    """Data quality validation result."""
    rule_name: str
    rule_type: str
    description: str
    total_records: int
    passed_records: int
    failed_records: int
    pass_rate: float
    passed: bool
    severity: str
    execution_time_ms: float
    failed_sample: Optional[List[Dict]] = None


class NaturalLanguageParser:
    """Parses natural language DQ rules into SQL expressions."""

    # Patterns for natural language rule parsing
    PATTERNS = {
        # Not null patterns
        r"(?P<column>\w+)\s+(should|must|cannot)\s+(not\s+)?be\s+(null|empty|blank)":
            lambda m: ("NOT_NULL", m.group("column"), f"{m.group('column')} IS NOT NULL"),

        r"(?P<column>\w+)\s+is\s+required":
            lambda m: ("NOT_NULL", m.group("column"), f"{m.group('column')} IS NOT NULL"),

        # Range patterns
        r"(?P<column>\w+)\s+(should|must)\s+be\s+between\s+(?P<min>[\d.]+)\s+and\s+(?P<max>[\d.]+)":
            lambda m: ("RANGE", m.group("column"),
                      f"{m.group('column')} BETWEEN {m.group('min')} AND {m.group('max')}"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+(greater|more)\s+than\s+(?P<value>[\d.]+)":
            lambda m: ("RANGE", m.group("column"), f"{m.group('column')} > {m.group('value')}"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+(less|fewer)\s+than\s+(?P<value>[\d.]+)":
            lambda m: ("RANGE", m.group("column"), f"{m.group('column')} < {m.group('value')}"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+positive":
            lambda m: ("RANGE", m.group("column"), f"{m.group('column')} > 0"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+non-negative":
            lambda m: ("RANGE", m.group("column"), f"{m.group('column')} >= 0"),

        # Unique patterns
        r"(?P<column>\w+)\s+(should|must)\s+be\s+unique":
            lambda m: ("UNIQUE", m.group("column"), None),

        r"no\s+duplicate(s)?\s+(in\s+)?(?P<column>\w+)":
            lambda m: ("UNIQUE", m.group("column"), None),

        # Date/time patterns
        r"(?P<column>\w+)\s+(should|must)\s+not\s+be\s+in\s+the\s+future":
            lambda m: ("FRESHNESS", m.group("column"), f"{m.group('column')} <= current_date()"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+within\s+(?P<days>\d+)\s+days":
            lambda m: ("FRESHNESS", m.group("column"),
                      f"datediff(current_date(), {m.group('column')}) <= {m.group('days')}"),

        # Pattern matching
        r"(?P<column>\w+)\s+(should|must)\s+match\s+pattern\s+['\"](?P<pattern>[^'\"]+)['\"]":
            lambda m: ("PATTERN", m.group("column"),
                      f"{m.group('column')} RLIKE '{m.group('pattern')}'"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+a\s+valid\s+email":
            lambda m: ("PATTERN", m.group("column"),
                      f"{m.group('column')} RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Z|a-z]{{2,}}$'"),

        r"(?P<column>\w+)\s+(should|must)\s+be\s+a\s+valid\s+phone":
            lambda m: ("PATTERN", m.group("column"),
                      f"REGEXP_REPLACE({m.group('column')}, '[^0-9]', '') RLIKE '^[0-9]{{10,15}}$'"),

        # Length patterns
        r"(?P<column>\w+)\s+length\s+(should|must)\s+be\s+(?P<length>\d+)":
            lambda m: ("PATTERN", m.group("column"),
                      f"LENGTH({m.group('column')}) = {m.group('length')}"),

        r"(?P<column>\w+)\s+length\s+(should|must)\s+be\s+between\s+(?P<min>\d+)\s+and\s+(?P<max>\d+)":
            lambda m: ("PATTERN", m.group("column"),
                      f"LENGTH({m.group('column')}) BETWEEN {m.group('min')} AND {m.group('max')}"),

        # Allowed values
        r"(?P<column>\w+)\s+(should|must)\s+be\s+(one\s+of|in)\s+\[(?P<values>[^\]]+)\]":
            lambda m: ("PATTERN", m.group("column"),
                      f"{m.group('column')} IN ({m.group('values')})"),

        # Completeness
        r"(?P<column>\w+)\s+completeness\s+(should|must)\s+be\s+(at\s+least\s+)?(?P<pct>[\d.]+)%":
            lambda m: ("COMPLETENESS", m.group("column"), None, float(m.group("pct")) / 100),
    }

    @classmethod
    def parse(cls, natural_language: str) -> Optional[Tuple[str, str, Optional[str], Dict]]:
        """
        Parse natural language rule into components.
        Returns: (rule_type, column, sql_expression, parameters)
        """
        nl_lower = natural_language.lower().strip()

        for pattern, handler in cls.PATTERNS.items():
            match = re.search(pattern, nl_lower, re.IGNORECASE)
            if match:
                result = handler(match)
                if len(result) == 3:
                    rule_type, column, sql_expr = result
                    return (rule_type, column, sql_expr, {})
                elif len(result) == 4:
                    rule_type, column, sql_expr, threshold = result
                    return (rule_type, column, sql_expr, {"threshold": threshold})

        return None

    @classmethod
    def get_supported_patterns(cls) -> List[str]:
        """Get list of supported natural language patterns."""
        return [
            "column_name should not be null",
            "column_name is required",
            "column_name must be between X and Y",
            "column_name should be greater than X",
            "column_name should be positive",
            "column_name should be unique",
            "no duplicates in column_name",
            "column_name should not be in the future",
            "column_name should be within N days",
            "column_name should match pattern 'regex'",
            "column_name should be a valid email",
            "column_name should be a valid phone",
            "column_name length should be N",
            "column_name should be one of [val1, val2, val3]",
            "column_name completeness should be at least N%"
        ]


class DataQualityNLAgent:
    """
    Data Quality Agent with Natural Language and SQL support.

    Supports three ways to define rules:
    1. Natural Language: agent.add_rule_nl("customer_id should not be null")
    2. SQL Expression: agent.add_rule_sql("valid_amount", "amount > 0 AND amount < 1000000")
    3. Pre-built Templates: agent.add_rule_template("not_null", column="customer_id")
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.config = config or {}
        self.rules: List[DQRule] = []
        self.results: List[DQResult] = []
        self.quarantine_path = self.config.get("quarantine_path", "/tmp/dq_quarantine")

    def add_rule_nl(self, natural_language: str, severity: str = "error",
                    threshold: float = 1.0) -> bool:
        """
        Add a data quality rule using natural language.

        Examples:
            agent.add_rule_nl("customer_id should not be null")
            agent.add_rule_nl("transaction_amount must be between 0 and 100000")
            agent.add_rule_nl("email should be unique")
        """
        parsed = NaturalLanguageParser.parse(natural_language)

        if parsed is None:
            print(f"WARNING: Could not parse rule: '{natural_language}'")
            print("Supported patterns:")
            for pattern in NaturalLanguageParser.get_supported_patterns():
                print(f"  - {pattern}")
            return False

        rule_type_str, column, sql_expr, params = parsed

        rule = DQRule(
            name=f"nl_{column}_{rule_type_str.lower()}",
            rule_type=RuleType[rule_type_str],
            column=column,
            sql_expression=sql_expr,
            description=natural_language,
            severity=severity,
            threshold=params.get("threshold", threshold)
        )

        self.rules.append(rule)
        print(f"Added rule: {rule.name}")
        print(f"  Type: {rule.rule_type.value}")
        print(f"  Column: {rule.column}")
        print(f"  SQL: {rule.sql_expression}")
        return True

    def add_rule_sql(self, name: str, sql_expression: str, description: str = "",
                     severity: str = "error", threshold: float = 1.0) -> None:
        """
        Add a data quality rule using SQL expression.

        Examples:
            agent.add_rule_sql(
                "valid_email",
                "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'"
            )
            agent.add_rule_sql(
                "amount_sanity",
                "transaction_amount > 0 AND transaction_amount < order_total"
            )
        """
        rule = DQRule(
            name=name,
            rule_type=RuleType.CUSTOM_SQL,
            sql_expression=sql_expression,
            description=description or f"Custom SQL: {sql_expression}",
            severity=severity,
            threshold=threshold
        )
        self.rules.append(rule)
        print(f"Added SQL rule: {name}")

    def add_rule_template(self, template: str, column: str = None, columns: List[str] = None,
                          severity: str = "error", threshold: float = 1.0, **kwargs) -> None:
        """
        Add a data quality rule using pre-built template.

        Templates:
            - not_null: Check column is not null
            - unique: Check column values are unique
            - range: Check column is within min/max range (requires min, max params)
            - pattern: Check column matches regex pattern (requires pattern param)
            - freshness: Check date column is within N days (requires days param)
            - referential: Check column values exist in reference (requires ref_df, ref_column)
        """
        template_configs = {
            "not_null": {
                "rule_type": RuleType.NOT_NULL,
                "sql": lambda c, **kw: f"{c} IS NOT NULL",
                "desc": lambda c, **kw: f"{c} must not be null"
            },
            "unique": {
                "rule_type": RuleType.UNIQUE,
                "sql": None,
                "desc": lambda c, **kw: f"{c} must be unique"
            },
            "range": {
                "rule_type": RuleType.RANGE,
                "sql": lambda c, **kw: f"{c} BETWEEN {kw['min']} AND {kw['max']}",
                "desc": lambda c, **kw: f"{c} must be between {kw['min']} and {kw['max']}"
            },
            "positive": {
                "rule_type": RuleType.RANGE,
                "sql": lambda c, **kw: f"{c} > 0",
                "desc": lambda c, **kw: f"{c} must be positive"
            },
            "non_negative": {
                "rule_type": RuleType.RANGE,
                "sql": lambda c, **kw: f"{c} >= 0",
                "desc": lambda c, **kw: f"{c} must be non-negative"
            },
            "pattern": {
                "rule_type": RuleType.PATTERN,
                "sql": lambda c, **kw: f"{c} RLIKE '{kw['pattern']}'",
                "desc": lambda c, **kw: f"{c} must match pattern {kw['pattern']}"
            },
            "email": {
                "rule_type": RuleType.PATTERN,
                "sql": lambda c, **kw: f"{c} RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Z|a-z]{{2,}}$'",
                "desc": lambda c, **kw: f"{c} must be a valid email"
            },
            "freshness": {
                "rule_type": RuleType.FRESHNESS,
                "sql": lambda c, **kw: f"datediff(current_date(), {c}) <= {kw.get('days', 30)}",
                "desc": lambda c, **kw: f"{c} must be within {kw.get('days', 30)} days"
            },
            "not_future": {
                "rule_type": RuleType.FRESHNESS,
                "sql": lambda c, **kw: f"{c} <= current_date()",
                "desc": lambda c, **kw: f"{c} must not be in the future"
            },
            "allowed_values": {
                "rule_type": RuleType.PATTERN,
                "sql": lambda c, **kw: f"{c} IN ({', '.join(repr(v) for v in kw['values'])})",
                "desc": lambda c, **kw: f"{c} must be one of {kw['values']}"
            }
        }

        if template not in template_configs:
            raise ValueError(f"Unknown template: {template}. Available: {list(template_configs.keys())}")

        config = template_configs[template]
        col_name = column or (columns[0] if columns else "unknown")

        rule = DQRule(
            name=f"template_{col_name}_{template}",
            rule_type=config["rule_type"],
            column=column,
            columns=columns,
            sql_expression=config["sql"](col_name, **kwargs) if config["sql"] else None,
            description=config["desc"](col_name, **kwargs),
            severity=severity,
            threshold=threshold,
            parameters=kwargs
        )
        self.rules.append(rule)
        print(f"Added template rule: {rule.name}")

    def validate(self, df: DataFrame, quarantine_failures: bool = True) -> List[DQResult]:
        """
        Validate DataFrame against all configured rules.

        Returns list of DQResult objects with validation details.
        """
        print("\n" + "=" * 60)
        print("Data Quality Validation")
        print("=" * 60)
        print(f"Total rules to validate: {len(self.rules)}")
        print(f"Total records: {df.count()}")
        print("=" * 60)

        self.results = []
        failed_dfs = []

        for rule in self.rules:
            print(f"\nValidating: {rule.name}")
            print(f"  Description: {rule.description}")

            start_time = datetime.now()
            result = self._validate_rule(df, rule)
            result.execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000

            self.results.append(result)

            status = "PASSED" if result.passed else "FAILED"
            print(f"  Result: {status} ({result.pass_rate*100:.2f}% pass rate)")
            print(f"  Records: {result.passed_records}/{result.total_records} passed")

            # Collect failed records for quarantine
            if not result.passed and quarantine_failures and result.failed_sample:
                failed_dfs.append((rule.name, result.failed_sample))

        # Write quarantine data
        if failed_dfs:
            self._write_quarantine(df, failed_dfs)

        # Print summary
        self._print_summary()

        return self.results

    def _validate_rule(self, df: DataFrame, rule: DQRule) -> DQResult:
        """Validate a single rule against the DataFrame."""
        total_records = df.count()

        if rule.rule_type == RuleType.UNIQUE:
            return self._validate_unique(df, rule, total_records)
        elif rule.rule_type == RuleType.COMPLETENESS:
            return self._validate_completeness(df, rule, total_records)
        else:
            return self._validate_sql(df, rule, total_records)

    def _validate_sql(self, df: DataFrame, rule: DQRule, total_records: int) -> DQResult:
        """Validate using SQL expression."""
        sql_expr = rule.sql_expression

        # Count passing records
        passed_count = df.filter(expr(sql_expr)).count()
        failed_count = total_records - passed_count
        pass_rate = passed_count / total_records if total_records > 0 else 1.0

        # Get sample of failed records
        failed_sample = None
        if failed_count > 0:
            failed_df = df.filter(~expr(sql_expr)).limit(10)
            failed_sample = [row.asDict() for row in failed_df.collect()]

        return DQResult(
            rule_name=rule.name,
            rule_type=rule.rule_type.value,
            description=rule.description,
            total_records=total_records,
            passed_records=passed_count,
            failed_records=failed_count,
            pass_rate=pass_rate,
            passed=pass_rate >= rule.threshold,
            severity=rule.severity,
            execution_time_ms=0,
            failed_sample=failed_sample
        )

    def _validate_unique(self, df: DataFrame, rule: DQRule, total_records: int) -> DQResult:
        """Validate uniqueness constraint."""
        column = rule.column

        # Count distinct values
        distinct_count = df.select(column).distinct().count()
        duplicate_count = total_records - distinct_count
        pass_rate = distinct_count / total_records if total_records > 0 else 1.0

        # Get sample of duplicates
        failed_sample = None
        if duplicate_count > 0:
            dup_values = (df.groupBy(column)
                         .count()
                         .filter(col("count") > 1)
                         .limit(10))
            failed_sample = [row.asDict() for row in dup_values.collect()]

        return DQResult(
            rule_name=rule.name,
            rule_type=rule.rule_type.value,
            description=rule.description,
            total_records=total_records,
            passed_records=distinct_count,
            failed_records=duplicate_count,
            pass_rate=pass_rate,
            passed=pass_rate >= rule.threshold,
            severity=rule.severity,
            execution_time_ms=0,
            failed_sample=failed_sample
        )

    def _validate_completeness(self, df: DataFrame, rule: DQRule, total_records: int) -> DQResult:
        """Validate completeness (non-null percentage)."""
        column = rule.column

        non_null_count = df.filter(col(column).isNotNull()).count()
        null_count = total_records - non_null_count
        completeness = non_null_count / total_records if total_records > 0 else 1.0

        return DQResult(
            rule_name=rule.name,
            rule_type=rule.rule_type.value,
            description=rule.description,
            total_records=total_records,
            passed_records=non_null_count,
            failed_records=null_count,
            pass_rate=completeness,
            passed=completeness >= rule.threshold,
            severity=rule.severity,
            execution_time_ms=0,
            failed_sample=None
        )

    def _write_quarantine(self, df: DataFrame, failed_dfs: List[Tuple[str, List[Dict]]]):
        """Write failed records to quarantine location."""
        print(f"\nWriting quarantine data to: {self.quarantine_path}")
        # Implementation would write to S3/local storage

    def _print_summary(self):
        """Print validation summary."""
        print("\n" + "=" * 60)
        print("Validation Summary")
        print("=" * 60)

        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        print(f"Total Rules: {len(self.results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")

        if failed > 0:
            print("\nFailed Rules:")
            for r in self.results:
                if not r.passed:
                    print(f"  - {r.rule_name}: {r.pass_rate*100:.2f}% ({r.severity})")

        print("=" * 60)

    def get_results_df(self) -> DataFrame:
        """Get validation results as a DataFrame."""
        if not self.results:
            return None

        results_data = [
            {
                "rule_name": r.rule_name,
                "rule_type": r.rule_type,
                "description": r.description,
                "total_records": r.total_records,
                "passed_records": r.passed_records,
                "failed_records": r.failed_records,
                "pass_rate": r.pass_rate,
                "passed": r.passed,
                "severity": r.severity,
                "execution_time_ms": r.execution_time_ms
            }
            for r in self.results
        ]

        return self.spark.createDataFrame(results_data)

    def to_json(self) -> str:
        """Export results as JSON."""
        return json.dumps([
            {
                "rule_name": r.rule_name,
                "rule_type": r.rule_type,
                "description": r.description,
                "total_records": r.total_records,
                "passed_records": r.passed_records,
                "failed_records": r.failed_records,
                "pass_rate": r.pass_rate,
                "passed": r.passed,
                "severity": r.severity
            }
            for r in self.results
        ], indent=2)


# Example usage and testing
def example_usage():
    """Demonstrate agent usage."""
    spark = SparkSession.builder.appName("DQAgentDemo").getOrCreate()

    # Create sample data
    data = [
        ("C001", "john@email.com", 100.0, "2025-01-15"),
        ("C002", "jane@email.com", 250.0, "2025-01-16"),
        ("C003", None, -50.0, "2025-01-17"),  # Invalid: null email, negative amount
        ("C001", "duplicate@test.com", 75.0, "2030-01-01"),  # Invalid: duplicate ID, future date
        ("C004", "invalid-email", 500.0, "2025-01-18"),  # Invalid: bad email format
    ]

    df = spark.createDataFrame(data, ["customer_id", "email", "amount", "created_date"])

    # Initialize agent
    agent = DataQualityNLAgent(spark)

    # Add rules using natural language
    agent.add_rule_nl("customer_id should not be null")
    agent.add_rule_nl("customer_id should be unique")
    agent.add_rule_nl("email should not be null")
    agent.add_rule_nl("amount must be between 0 and 10000")
    agent.add_rule_nl("created_date should not be in the future")

    # Add rule using SQL
    agent.add_rule_sql(
        "valid_email_format",
        "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'",
        description="Email must be in valid format"
    )

    # Add rule using template
    agent.add_rule_template("positive", column="amount")

    # Validate
    results = agent.validate(df)

    # Get results as DataFrame
    results_df = agent.get_results_df()
    results_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    example_usage()
