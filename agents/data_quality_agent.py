"""
Data Quality Agent - Validates data using SQL or natural language rules
Supports rule-based validation, anomaly detection, and data profiling
"""

import json
import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RuleType(Enum):
    """Types of data quality rules."""
    COMPLETENESS = "completeness"       # Not null checks
    UNIQUENESS = "uniqueness"           # Unique value checks
    VALIDITY = "validity"               # Value range/format checks
    CONSISTENCY = "consistency"         # Cross-field/table checks
    TIMELINESS = "timeliness"           # Data freshness checks
    ACCURACY = "accuracy"               # Data accuracy checks
    CUSTOM_SQL = "custom_sql"           # Custom SQL expression
    NATURAL_LANGUAGE = "natural_language"  # AI-interpreted rule


class Severity(Enum):
    """Rule violation severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class RuleAction(Enum):
    """Actions to take on rule violation."""
    LOG = "log"                 # Just log the violation
    ALERT = "alert"             # Send alert notification
    QUARANTINE = "quarantine"   # Move bad records to quarantine
    REJECT = "reject"           # Reject entire batch
    FIX = "fix"                 # Attempt to fix the data


@dataclass
class DataQualityRule:
    """A single data quality rule definition."""
    rule_id: str
    name: str
    description: str
    rule_type: RuleType
    target_table: str
    target_column: Optional[str] = None
    expression: Optional[str] = None  # SQL expression or natural language
    threshold: float = 1.0  # Minimum pass rate (0-1)
    severity: Severity = Severity.ERROR
    action: RuleAction = RuleAction.LOG
    enabled: bool = True
    tags: List[str] = field(default_factory=list)


@dataclass
class RuleResult:
    """Result of evaluating a single rule."""
    rule: DataQualityRule
    passed: bool
    total_records: int
    passing_records: int
    failing_records: int
    pass_rate: float
    sample_failures: List[Dict[str, Any]]
    error_message: Optional[str] = None
    execution_time_ms: int = 0


@dataclass
class DataQualityReport:
    """Complete data quality report."""
    table_name: str
    timestamp: datetime
    total_rules: int
    passed_rules: int
    failed_rules: int
    overall_score: float
    results: List[RuleResult]
    recommendations: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataQualityAgent:
    """
    Data Quality Agent that validates data using configurable rules.

    Supports:
    - SQL-based rules
    - Natural language rules (converted to SQL via AI)
    - Pre-built rule templates (null checks, uniqueness, ranges)
    - Custom validation logic
    - Quarantine handling for bad records
    """

    # Common validation patterns
    VALIDATION_PATTERNS = {
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'phone': r'^\+?1?\d{10,14}$',
        'date_iso': r'^\d{4}-\d{2}-\d{2}$',
        'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        'zip_code': r'^\d{5}(-\d{4})?$',
        'url': r'^https?://[^\s]+$',
    }

    # Natural language to SQL mappings
    NL_PATTERNS = {
        r'(?:must|should)\s+(?:be\s+)?not\s+(?:be\s+)?null': 'IS NOT NULL',
        r'(?:must|should)\s+(?:be\s+)?unique': 'COUNT(*) = COUNT(DISTINCT {column})',
        r'(?:must|should)\s+be\s+positive': '> 0',
        r'(?:must|should)\s+be\s+non-negative': '>= 0',
        r'(?:must|should)\s+be\s+between\s+(\d+)\s+and\s+(\d+)': 'BETWEEN {0} AND {1}',
        r'(?:must|should)\s+be\s+(?:greater|more)\s+than\s+(\d+)': '> {0}',
        r'(?:must|should)\s+be\s+(?:less|fewer)\s+than\s+(\d+)': '< {0}',
        r'(?:must|should)\s+match\s+pattern\s+["\'](.+)["\']': "REGEXP '{0}'",
        r'(?:must|should)\s+be\s+valid\s+email': "REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
        r'(?:must|should)\s+be\s+in\s+\[([^\]]+)\]': "IN ({0})",
    }

    def __init__(self, region: str = 'us-east-1'):
        """Initialize the data quality agent."""
        self.region = region
        self.glue_client = boto3.client('glue', region_name=region)
        self.athena_client = boto3.client('athena', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        self.bedrock_client = None

        try:
            self.bedrock_client = boto3.client('bedrock-runtime', region_name=region)
        except Exception:
            logger.warning("Bedrock client not available for NL rule parsing")

    def parse_rules_from_config(self, config: Dict[str, Any]) -> List[DataQualityRule]:
        """
        Parse data quality rules from configuration.

        Args:
            config: Configuration dict with data_quality section

        Returns:
            List of DataQualityRule objects
        """
        rules = []
        dq_config = config.get('data_quality', {})

        if not dq_config.get('enabled', True):
            return rules

        # Parse rule definitions
        for rule_def in dq_config.get('rules', []):
            try:
                rule_type = RuleType(rule_def.get('type', 'custom_sql'))
                severity = Severity(rule_def.get('severity', 'error'))
                action = RuleAction(rule_def.get('action', 'log'))

                rule = DataQualityRule(
                    rule_id=rule_def.get('id', f"rule_{len(rules)}"),
                    name=rule_def.get('name', 'Unnamed Rule'),
                    description=rule_def.get('description', ''),
                    rule_type=rule_type,
                    target_table=rule_def.get('table', ''),
                    target_column=rule_def.get('column'),
                    expression=rule_def.get('expression'),
                    threshold=float(rule_def.get('threshold', 1.0)),
                    severity=severity,
                    action=action,
                    enabled=rule_def.get('enabled', True),
                    tags=rule_def.get('tags', [])
                )
                rules.append(rule)

            except Exception as e:
                logger.warning(f"Failed to parse rule: {e}")

        # Add auto-generated rules from schema if configured
        if dq_config.get('auto_generate_rules', False):
            auto_rules = self._generate_schema_rules(dq_config)
            rules.extend(auto_rules)

        logger.info(f"Parsed {len(rules)} data quality rules")
        return rules

    def _generate_schema_rules(self, dq_config: Dict[str, Any]) -> List[DataQualityRule]:
        """Auto-generate rules based on table schema."""
        rules = []

        for table_config in dq_config.get('tables', []):
            database = table_config.get('database', '')
            table = table_config.get('table', '')

            if not database or not table:
                continue

            try:
                # Get table schema
                response = self.glue_client.get_table(
                    DatabaseName=database,
                    Name=table
                )
                columns = response['Table'].get('StorageDescriptor', {}).get('Columns', [])

                for col in columns:
                    col_name = col['Name']
                    col_type = col['Type'].lower()

                    # Not null rule for common required fields
                    if any(key in col_name.lower() for key in ['id', 'key', 'code']):
                        rules.append(DataQualityRule(
                            rule_id=f"auto_{table}_{col_name}_not_null",
                            name=f"{col_name} Not Null",
                            description=f"Auto-generated: {col_name} should not be null",
                            rule_type=RuleType.COMPLETENESS,
                            target_table=f"{database}.{table}",
                            target_column=col_name,
                            expression=f"{col_name} IS NOT NULL",
                            threshold=1.0,
                            severity=Severity.ERROR,
                            tags=['auto-generated']
                        ))

                    # Positive value rule for numeric amount/count columns
                    if col_type in ['int', 'bigint', 'double', 'float', 'decimal']:
                        if any(key in col_name.lower() for key in ['amount', 'price', 'count', 'quantity']):
                            rules.append(DataQualityRule(
                                rule_id=f"auto_{table}_{col_name}_positive",
                                name=f"{col_name} Positive",
                                description=f"Auto-generated: {col_name} should be positive",
                                rule_type=RuleType.VALIDITY,
                                target_table=f"{database}.{table}",
                                target_column=col_name,
                                expression=f"{col_name} >= 0",
                                threshold=0.99,
                                severity=Severity.WARNING,
                                tags=['auto-generated']
                            ))

            except ClientError as e:
                logger.warning(f"Could not generate rules for {database}.{table}: {e}")

        return rules

    def natural_language_to_sql(self, nl_rule: str, column: str, table: str) -> Optional[str]:
        """
        Convert a natural language rule to SQL expression.

        Args:
            nl_rule: Natural language rule description
            column: Target column name
            table: Target table name

        Returns:
            SQL expression or None if conversion fails
        """
        nl_lower = nl_rule.lower()

        # Try pattern matching first
        for pattern, sql_template in self.NL_PATTERNS.items():
            match = re.search(pattern, nl_lower)
            if match:
                sql = sql_template.format(*match.groups()) if match.groups() else sql_template
                sql = sql.replace('{column}', column)
                return f"{column} {sql}" if not sql.startswith('COUNT') else sql

        # Use AI for complex rules
        if self.bedrock_client:
            return self._ai_parse_rule(nl_rule, column, table)

        logger.warning(f"Could not parse natural language rule: {nl_rule}")
        return None

    def _ai_parse_rule(self, nl_rule: str, column: str, table: str) -> Optional[str]:
        """Use Bedrock AI to parse complex natural language rules."""
        try:
            prompt = f"""Convert this natural language data quality rule to a SQL WHERE clause condition.

Rule: "{nl_rule}"
Table: {table}
Column: {column}

Return ONLY the SQL condition (no SELECT, no WHERE keyword, just the condition).
Example: If rule is "must be between 0 and 100", return "{column} BETWEEN 0 AND 100"

SQL Condition:"""

            response = self.bedrock_client.invoke_model(
                modelId='anthropic.claude-3-haiku-20240307-v1:0',
                body=json.dumps({
                    'anthropic_version': 'bedrock-2023-05-31',
                    'max_tokens': 200,
                    'messages': [{'role': 'user', 'content': prompt}]
                })
            )

            result = json.loads(response['body'].read())
            sql_condition = result['content'][0]['text'].strip()

            # Basic validation
            if sql_condition and len(sql_condition) < 500:
                return sql_condition

        except Exception as e:
            logger.warning(f"AI rule parsing failed: {e}")

        return None

    def build_validation_query(self, rule: DataQualityRule) -> str:
        """
        Build SQL query to validate a rule.

        Args:
            rule: DataQualityRule to validate

        Returns:
            SQL query string
        """
        table = rule.target_table
        column = rule.target_column or '*'

        if rule.rule_type == RuleType.COMPLETENESS:
            # Null check
            return f"""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN {column} IS NOT NULL THEN 1 ELSE 0 END) as passing_records,
                SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as failing_records
            FROM {table}
            """

        elif rule.rule_type == RuleType.UNIQUENESS:
            # Uniqueness check
            return f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT {column}) as unique_values,
                COUNT(*) - COUNT(DISTINCT {column}) as duplicate_count
            FROM {table}
            """

        elif rule.rule_type == RuleType.VALIDITY:
            # Value validity check using expression
            expr = rule.expression or f"{column} IS NOT NULL"
            return f"""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN {expr} THEN 1 ELSE 0 END) as passing_records,
                SUM(CASE WHEN NOT ({expr}) THEN 1 ELSE 0 END) as failing_records
            FROM {table}
            """

        elif rule.rule_type == RuleType.TIMELINESS:
            # Data freshness check
            expr = rule.expression or f"{column} >= CURRENT_DATE - INTERVAL '7' DAY"
            return f"""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN {expr} THEN 1 ELSE 0 END) as passing_records,
                MAX({column}) as latest_value
            FROM {table}
            """

        elif rule.rule_type == RuleType.CUSTOM_SQL:
            # Custom SQL expression
            if rule.expression:
                # If expression is a full query, use it directly
                if 'SELECT' in rule.expression.upper():
                    return rule.expression
                # Otherwise wrap it
                return f"""
                SELECT
                    COUNT(*) as total_records,
                    SUM(CASE WHEN {rule.expression} THEN 1 ELSE 0 END) as passing_records,
                    SUM(CASE WHEN NOT ({rule.expression}) THEN 1 ELSE 0 END) as failing_records
                FROM {table}
                """

        elif rule.rule_type == RuleType.NATURAL_LANGUAGE:
            # Convert natural language to SQL
            sql_expr = self.natural_language_to_sql(
                rule.expression or '',
                column,
                table
            )
            if sql_expr:
                return f"""
                SELECT
                    COUNT(*) as total_records,
                    SUM(CASE WHEN {sql_expr} THEN 1 ELSE 0 END) as passing_records,
                    SUM(CASE WHEN NOT ({sql_expr}) THEN 1 ELSE 0 END) as failing_records
                FROM {table}
                """

        # Default fallback
        return f"SELECT COUNT(*) as total_records FROM {table}"

    def get_sample_failures(self, rule: DataQualityRule, limit: int = 5) -> str:
        """Build query to get sample failing records."""
        table = rule.target_table
        column = rule.target_column or '*'
        expr = rule.expression or f"{column} IS NULL"

        # Negate the expression to find failures
        if rule.rule_type == RuleType.COMPLETENESS:
            where_clause = f"{column} IS NULL"
        elif rule.rule_type == RuleType.VALIDITY:
            where_clause = f"NOT ({expr})"
        else:
            where_clause = f"NOT ({expr})" if expr else "1=1"

        return f"""
        SELECT *
        FROM {table}
        WHERE {where_clause}
        LIMIT {limit}
        """

    def execute_athena_query(self, query: str, database: str,
                              output_location: str) -> Dict[str, Any]:
        """
        Execute a query using Athena.

        Args:
            query: SQL query to execute
            database: Athena database
            output_location: S3 location for results

        Returns:
            Query results as dict
        """
        try:
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': output_location}
            )

            execution_id = response['QueryExecutionId']

            # Wait for completion
            import time
            max_wait = 300  # 5 minutes
            waited = 0

            while waited < max_wait:
                status_response = self.athena_client.get_query_execution(
                    QueryExecutionId=execution_id
                )
                status = status_response['QueryExecution']['Status']['State']

                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    return {'error': error}

                time.sleep(2)
                waited += 2

            # Get results
            results_response = self.athena_client.get_query_results(
                QueryExecutionId=execution_id
            )

            # Parse results
            columns = [col['Name'] for col in results_response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = results_response['ResultSet']['Rows'][1:]  # Skip header

            data = []
            for row in rows:
                values = [cell.get('VarCharValue', '') for cell in row['Data']]
                data.append(dict(zip(columns, values)))

            return {'data': data, 'columns': columns}

        except ClientError as e:
            return {'error': str(e)}

    def evaluate_rule(self, rule: DataQualityRule,
                       athena_database: str = 'default',
                       output_location: str = 's3://temp-bucket/athena-results/') -> RuleResult:
        """
        Evaluate a single data quality rule.

        Args:
            rule: Rule to evaluate
            athena_database: Athena database for queries
            output_location: S3 output location

        Returns:
            RuleResult with evaluation results
        """
        import time
        start_time = time.time()

        if not rule.enabled:
            return RuleResult(
                rule=rule,
                passed=True,
                total_records=0,
                passing_records=0,
                failing_records=0,
                pass_rate=1.0,
                sample_failures=[],
                error_message="Rule is disabled"
            )

        try:
            # Build and execute validation query
            query = self.build_validation_query(rule)
            logger.info(f"Evaluating rule '{rule.name}': {query[:100]}...")

            result = self.execute_athena_query(query, athena_database, output_location)

            if 'error' in result:
                return RuleResult(
                    rule=rule,
                    passed=False,
                    total_records=0,
                    passing_records=0,
                    failing_records=0,
                    pass_rate=0.0,
                    sample_failures=[],
                    error_message=result['error']
                )

            # Parse results
            data = result.get('data', [{}])[0]
            total_records = int(data.get('total_records', 0))
            passing_records = int(data.get('passing_records', total_records))
            failing_records = int(data.get('failing_records', 0))

            # Handle uniqueness check differently
            if rule.rule_type == RuleType.UNIQUENESS:
                unique_values = int(data.get('unique_values', 0))
                passing_records = unique_values
                failing_records = total_records - unique_values

            pass_rate = passing_records / total_records if total_records > 0 else 1.0
            passed = pass_rate >= rule.threshold

            # Get sample failures if rule failed
            sample_failures = []
            if not passed and failing_records > 0:
                sample_query = self.get_sample_failures(rule)
                sample_result = self.execute_athena_query(
                    sample_query, athena_database, output_location
                )
                if 'data' in sample_result:
                    sample_failures = sample_result['data']

            execution_time = int((time.time() - start_time) * 1000)

            return RuleResult(
                rule=rule,
                passed=passed,
                total_records=total_records,
                passing_records=passing_records,
                failing_records=failing_records,
                pass_rate=pass_rate,
                sample_failures=sample_failures,
                execution_time_ms=execution_time
            )

        except Exception as e:
            logger.error(f"Rule evaluation failed: {e}")
            return RuleResult(
                rule=rule,
                passed=False,
                total_records=0,
                passing_records=0,
                failing_records=0,
                pass_rate=0.0,
                sample_failures=[],
                error_message=str(e)
            )

    def run_quality_check(self, config: Dict[str, Any],
                           athena_database: str = 'default',
                           output_location: str = 's3://temp-bucket/athena-results/') -> DataQualityReport:
        """
        Run complete data quality check based on configuration.

        Args:
            config: Pipeline configuration with data_quality section
            athena_database: Athena database
            output_location: S3 output location

        Returns:
            DataQualityReport with all results
        """
        # Parse rules from config
        rules = self.parse_rules_from_config(config)

        if not rules:
            logger.info("No data quality rules defined")
            return DataQualityReport(
                table_name="N/A",
                timestamp=datetime.utcnow(),
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
                overall_score=1.0,
                results=[],
                recommendations=["Consider adding data quality rules to your configuration"]
            )

        # Evaluate all rules
        results = []
        for rule in rules:
            result = self.evaluate_rule(rule, athena_database, output_location)
            results.append(result)

            if not result.passed:
                logger.warning(f"Rule '{rule.name}' FAILED: {result.pass_rate:.1%} pass rate")

        # Calculate overall metrics
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = len(results) - passed_rules

        # Weighted score based on severity
        severity_weights = {
            Severity.INFO: 0.1,
            Severity.WARNING: 0.3,
            Severity.ERROR: 0.5,
            Severity.CRITICAL: 1.0
        }

        total_weight = sum(severity_weights[r.rule.severity] for r in results)
        passed_weight = sum(
            severity_weights[r.rule.severity]
            for r in results if r.passed
        )
        overall_score = passed_weight / total_weight if total_weight > 0 else 1.0

        # Generate recommendations
        recommendations = self._generate_recommendations(results)

        # Determine table name(s)
        table_names = list(set(r.rule.target_table for r in results))
        table_name = ', '.join(table_names) if table_names else 'Unknown'

        report = DataQualityReport(
            table_name=table_name,
            timestamp=datetime.utcnow(),
            total_rules=len(results),
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            overall_score=overall_score,
            results=results,
            recommendations=recommendations,
            metadata={
                'config_version': config.get('version', '1.0'),
                'execution_mode': 'batch'
            }
        )

        logger.info(f"Data quality check complete: {passed_rules}/{len(results)} rules passed, "
                   f"score: {overall_score:.1%}")

        return report

    def _generate_recommendations(self, results: List[RuleResult]) -> List[str]:
        """Generate recommendations based on rule results."""
        recommendations = []

        # Group failures by type
        failures_by_type = {}
        for result in results:
            if not result.passed:
                rule_type = result.rule.rule_type.value
                if rule_type not in failures_by_type:
                    failures_by_type[rule_type] = []
                failures_by_type[rule_type].append(result)

        # Generate type-specific recommendations
        if 'completeness' in failures_by_type:
            count = len(failures_by_type['completeness'])
            recommendations.append(
                f"{count} completeness rule(s) failed. Review data ingestion pipeline for missing values."
            )

        if 'uniqueness' in failures_by_type:
            count = len(failures_by_type['uniqueness'])
            recommendations.append(
                f"{count} uniqueness rule(s) failed. Check for duplicate records in source data."
            )

        if 'validity' in failures_by_type:
            count = len(failures_by_type['validity'])
            recommendations.append(
                f"{count} validity rule(s) failed. Implement input validation or data cleansing."
            )

        if 'timeliness' in failures_by_type:
            recommendations.append(
                "Data freshness issues detected. Review data pipeline scheduling and latency."
            )

        # Critical failures need immediate attention
        critical_failures = [r for r in results
                           if not r.passed and r.rule.severity == Severity.CRITICAL]
        if critical_failures:
            recommendations.insert(0,
                f"URGENT: {len(critical_failures)} critical rule(s) failed. Immediate attention required!"
            )

        # If all passed
        if not failures_by_type:
            recommendations.append("All data quality rules passed. Continue monitoring.")

        return recommendations

    def generate_report_html(self, report: DataQualityReport) -> str:
        """Generate HTML report for email or dashboard."""
        passed_color = '#28a745'
        failed_color = '#dc3545'
        warning_color = '#ffc107'

        score_color = passed_color if report.overall_score >= 0.9 else (
            warning_color if report.overall_score >= 0.7 else failed_color
        )

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #1a73e8; color: white; padding: 20px; border-radius: 8px; }}
        .score {{ font-size: 48px; font-weight: bold; color: {score_color}; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; flex: 1; }}
        .passed {{ color: {passed_color}; }}
        .failed {{ color: {failed_color}; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #1a73e8; color: white; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .recommendations {{ background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report</h1>
        <p>Table: {report.table_name}</p>
        <p>Generated: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
    </div>

    <div class="summary">
        <div class="card">
            <h3>Overall Score</h3>
            <div class="score">{report.overall_score:.0%}</div>
        </div>
        <div class="card">
            <h3>Rules Summary</h3>
            <p class="passed">Passed: {report.passed_rules}</p>
            <p class="failed">Failed: {report.failed_rules}</p>
            <p>Total: {report.total_rules}</p>
        </div>
    </div>

    <h2>Rule Results</h2>
    <table>
        <tr>
            <th>Rule</th>
            <th>Type</th>
            <th>Severity</th>
            <th>Status</th>
            <th>Pass Rate</th>
            <th>Records</th>
        </tr>
"""
        for result in report.results:
            status = "PASSED" if result.passed else "FAILED"
            status_class = "passed" if result.passed else "failed"
            html += f"""
        <tr>
            <td>{result.rule.name}</td>
            <td>{result.rule.rule_type.value}</td>
            <td>{result.rule.severity.value}</td>
            <td class="{status_class}">{status}</td>
            <td>{result.pass_rate:.1%}</td>
            <td>{result.total_records:,}</td>
        </tr>
"""

        html += """
    </table>
"""

        if report.recommendations:
            html += """
    <div class="recommendations">
        <h3>Recommendations</h3>
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

    def generate_pyspark_validation(self, rules: List[DataQualityRule]) -> str:
        """
        Generate PySpark code to run validations inline.

        Args:
            rules: List of rules to generate code for

        Returns:
            PySpark validation code string
        """
        code_lines = [
            "# Auto-generated Data Quality Validation Code",
            "from pyspark.sql import functions as F",
            "from pyspark.sql import DataFrame",
            "from typing import Dict, List, Tuple",
            "",
            "def validate_data_quality(df: DataFrame, table_name: str) -> Tuple[bool, List[Dict]]:",
            "    '''Run data quality validations on DataFrame.'''",
            "    results = []",
            "    all_passed = True",
            "    total_count = df.count()",
            ""
        ]

        for rule in rules:
            if not rule.enabled:
                continue

            rule_code = self._generate_rule_code(rule)
            code_lines.extend(rule_code)

        code_lines.extend([
            "",
            "    return all_passed, results",
            "",
            "# Usage:",
            "# passed, results = validate_data_quality(my_dataframe, 'my_table')",
            "# if not passed:",
            "#     print('Data quality validation failed!')",
            "#     for r in results:",
            "#         if not r['passed']:",
            "#             print(f\"  - {r['rule']}: {r['message']}\")"
        ])

        return "\n".join(code_lines)

    def _generate_rule_code(self, rule: DataQualityRule) -> List[str]:
        """Generate PySpark code for a single rule."""
        lines = []
        rule_name = rule.name.replace("'", "\\'")
        column = rule.target_column or ''

        if rule.rule_type == RuleType.COMPLETENESS:
            lines.extend([
                f"    # Rule: {rule_name}",
                f"    null_count = df.filter(F.col('{column}').isNull()).count()",
                f"    pass_rate = 1 - (null_count / total_count) if total_count > 0 else 1",
                f"    passed = pass_rate >= {rule.threshold}",
                f"    if not passed: all_passed = False",
                f"    results.append({{",
                f"        'rule': '{rule_name}',",
                f"        'passed': passed,",
                f"        'pass_rate': pass_rate,",
                f"        'failing_count': null_count,",
                f"        'message': f'{column} null check: {{pass_rate:.1%}} pass rate'",
                f"    }})",
                ""
            ])

        elif rule.rule_type == RuleType.UNIQUENESS:
            lines.extend([
                f"    # Rule: {rule_name}",
                f"    distinct_count = df.select('{column}').distinct().count()",
                f"    duplicate_count = total_count - distinct_count",
                f"    passed = duplicate_count == 0",
                f"    if not passed: all_passed = False",
                f"    results.append({{",
                f"        'rule': '{rule_name}',",
                f"        'passed': passed,",
                f"        'pass_rate': distinct_count / total_count if total_count > 0 else 1,",
                f"        'failing_count': duplicate_count,",
                f"        'message': f'{column} uniqueness: {{duplicate_count}} duplicates found'",
                f"    }})",
                ""
            ])

        elif rule.rule_type == RuleType.VALIDITY and rule.expression:
            expr = rule.expression.replace("'", "\\'")
            lines.extend([
                f"    # Rule: {rule_name}",
                f"    valid_count = df.filter(F.expr(\"{expr}\")).count()",
                f"    invalid_count = total_count - valid_count",
                f"    pass_rate = valid_count / total_count if total_count > 0 else 1",
                f"    passed = pass_rate >= {rule.threshold}",
                f"    if not passed: all_passed = False",
                f"    results.append({{",
                f"        'rule': '{rule_name}',",
                f"        'passed': passed,",
                f"        'pass_rate': pass_rate,",
                f"        'failing_count': invalid_count,",
                f"        'message': f'Validity check: {{pass_rate:.1%}} pass rate'",
                f"    }})",
                ""
            ])

        return lines


# Example usage
if __name__ == '__main__':
    # Sample configuration
    sample_config = {
        'data_quality': {
            'enabled': True,
            'auto_generate_rules': True,
            'tables': [
                {'database': 'ecommerce_db', 'table': 'orders'}
            ],
            'rules': [
                {
                    'id': 'order_amount_positive',
                    'name': 'Order Amount Must Be Positive',
                    'type': 'validity',
                    'table': 'ecommerce_db.orders',
                    'column': 'total_amount',
                    'expression': 'total_amount > 0',
                    'threshold': 0.99,
                    'severity': 'error'
                },
                {
                    'id': 'customer_id_not_null',
                    'name': 'Customer ID Required',
                    'type': 'completeness',
                    'table': 'ecommerce_db.orders',
                    'column': 'customer_id',
                    'threshold': 1.0,
                    'severity': 'critical'
                },
                {
                    'id': 'email_format',
                    'name': 'Valid Email Format',
                    'type': 'natural_language',
                    'table': 'ecommerce_db.customers',
                    'column': 'email',
                    'expression': 'email must be valid email format',
                    'threshold': 0.95,
                    'severity': 'warning'
                }
            ]
        }
    }

    agent = DataQualityAgent()
    rules = agent.parse_rules_from_config(sample_config)

    print(f"Parsed {len(rules)} rules:")
    for rule in rules:
        print(f"  - {rule.name} ({rule.rule_type.value})")

    # Generate PySpark validation code
    pyspark_code = agent.generate_pyspark_validation(rules)
    print("\nGenerated PySpark Validation Code:")
    print(pyspark_code)
