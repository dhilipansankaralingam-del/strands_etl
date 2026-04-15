#!/usr/bin/env python3
"""
Strands Data Quality Agent
==========================

Validates data quality rules against source and target tables.
Supports PRE-LOAD and POST-LOAD validation phases.

IMPORTANT: This agent executes ACTUAL SQL queries via Athena to validate rules.
It does NOT simulate or generate fake data.

Configuration Structure:
------------------------
"data_quality": {
    "enabled": "Y",
    "fail_on_critical": "Y",
    "athena_workgroup": "primary",
    "athena_output_location": "s3://my-bucket/athena-results/",
    "query_timeout_seconds": 300,

    "pre_load_rules": [
        {
            "table": "finance_accounting_master.autopay_products_master",
            "rules": [
                {"type": "not_null", "column": "product_id", "severity": "critical"},
                {"type": "positive", "column": "amount", "severity": "warning"}
            ]
        }
    ],

    "post_load_rules": [
        {
            "table": "bd_work.pps_jss_test",
            "rules": [
                {"type": "not_null", "column": "customer_id", "severity": "critical"},
                {"type": "row_count_check", "min_rows": 1000, "severity": "critical"},
                {"type": "completeness", "column": "policy_number", "threshold": 0.99}
            ]
        }
    ],

    "table_rules": [...],
    "natural_language_rules": [...],
    "sql_rules": [...]
}

Audit Output:
-------------
Each validation writes to unified audit with:
- records_scanned: Number of records checked (from actual query)
- outliers_found: Number of records failing the rule (from actual query)
- pass_fail_status: "PASS" or "FAIL"
- threshold: The threshold used
- actual_value: The actual measured value
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage

# Unified audit logger
try:
    from ..unified_audit import get_audit_logger, EventType
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    get_audit_logger = None
    EventType = None


@register_agent
class StrandsDataQualityAgent(StrandsAgent):
    """
    Validates data against quality rules on specific tables.

    Supports:
    - PRE-LOAD validation (source tables before ETL)
    - POST-LOAD validation (target tables after ETL)
    - Natural language rules (parsed into SQL)
    - SQL rules (direct execution)
    - Table-specific rules with record counts and outlier detection
    """

    AGENT_NAME = "data_quality_agent"
    AGENT_VERSION = "2.2.0"
    AGENT_DESCRIPTION = "Validates data quality with pre/post load phases, record counts, and outlier detection"

    DEPENDENCIES = ['compliance_agent']
    PARALLEL_SAFE = True

    # Rule type mappings for natural language parsing
    NL_PATTERNS = {
        'should not be null': 'not_null',
        'should be positive': 'positive',
        'should be unique': 'unique',
        'should be greater than': 'greater_than',
        'should be less than': 'less_than',
        'should match pattern': 'pattern',
        'should be between': 'between',
        'should have completeness': 'completeness'
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)
        self.audit = None
        if AUDIT_AVAILABLE and get_audit_logger:
            try:
                self.audit = get_audit_logger(config)
            except Exception:
                pass

        # Athena client for executing validation queries
        self._athena_client = None
        self._athena_workgroup = 'primary'
        self._athena_output_location = None
        self._query_timeout = 300  # seconds

    @property
    def athena_client(self):
        """Lazy-load Athena client."""
        if self._athena_client is None:
            try:
                import boto3
                self._athena_client = boto3.client('athena')
            except Exception as e:
                self.logger.warning(f"Could not create Athena client: {e}")
        return self._athena_client

    def _execute_athena_query(
        self,
        sql: str,
        context: AgentContext,
        timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query via Athena and return the results.

        Returns:
            {
                'success': bool,
                'rows': List[List[str]],  # result rows
                'columns': List[str],     # column names
                'row_count': int,
                'error': str or None
            }
        """
        if not self.athena_client:
            return {
                'success': False,
                'rows': [],
                'columns': [],
                'row_count': 0,
                'error': 'Athena client not available'
            }

        dq_config = context.config.get('data_quality', {})
        workgroup = dq_config.get('athena_workgroup', self._athena_workgroup)
        output_location = dq_config.get('athena_output_location', self._athena_output_location)
        timeout = timeout_seconds or dq_config.get('query_timeout_seconds', self._query_timeout)

        # Output location is required for Athena
        if not output_location:
            # Try to get from global config
            output_location = context.config.get('athena', {}).get('output_location')
            if not output_location:
                output_location = context.config.get('s3_bucket')
                if output_location:
                    output_location = f"s3://{output_location}/athena-dq-results/"

        if not output_location:
            return {
                'success': False,
                'rows': [],
                'columns': [],
                'row_count': 0,
                'error': 'athena_output_location not configured in data_quality config'
            }

        try:
            # Start query execution
            start_params = {
                'QueryString': sql,
                'WorkGroup': workgroup,
                'ResultConfiguration': {
                    'OutputLocation': output_location
                }
            }

            response = self.athena_client.start_query_execution(**start_params)
            query_execution_id = response['QueryExecutionId']

            self.logger.debug(f"Started Athena query {query_execution_id}: {sql[:100]}...")

            # Poll for completion
            start_time = time.time()
            while True:
                status_response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                state = status_response['QueryExecution']['Status']['State']

                if state == 'SUCCEEDED':
                    break
                elif state in ('FAILED', 'CANCELLED'):
                    error_msg = status_response['QueryExecution']['Status'].get(
                        'StateChangeReason', f'Query {state}'
                    )
                    return {
                        'success': False,
                        'rows': [],
                        'columns': [],
                        'row_count': 0,
                        'error': error_msg
                    }

                if time.time() - start_time > timeout:
                    # Cancel the query
                    try:
                        self.athena_client.stop_query_execution(
                            QueryExecutionId=query_execution_id
                        )
                    except Exception:
                        pass
                    return {
                        'success': False,
                        'rows': [],
                        'columns': [],
                        'row_count': 0,
                        'error': f'Query timed out after {timeout}s'
                    }

                time.sleep(1)  # Poll every second

            # Fetch results with pagination (Athena max is 1000 per request)
            # Limit total rows to avoid memory issues with huge result sets
            max_total_rows = 100000  # Configurable upper limit
            columns = []
            rows = []
            next_token = None
            is_first_page = True

            while True:
                # Build request params
                request_params = {
                    'QueryExecutionId': query_execution_id,
                    'MaxResults': 1000  # Athena maximum per request
                }
                if next_token:
                    request_params['NextToken'] = next_token

                results_response = self.athena_client.get_query_results(**request_params)

                result_set = results_response.get('ResultSet', {})
                rows_data = result_set.get('Rows', [])

                if rows_data:
                    if is_first_page:
                        # First row of first page is column headers
                        header_row = rows_data[0]
                        columns = [
                            col.get('VarCharValue', '')
                            for col in header_row.get('Data', [])
                        ]
                        # Skip header row for data
                        data_rows = rows_data[1:]
                        is_first_page = False
                    else:
                        # Subsequent pages don't have header row
                        data_rows = rows_data

                    for row in data_rows:
                        row_values = [
                            col.get('VarCharValue', '')
                            for col in row.get('Data', [])
                        ]
                        rows.append(row_values)

                        # Check if we've hit the limit
                        if len(rows) >= max_total_rows:
                            self.logger.warning(
                                f"Results truncated at {max_total_rows} rows"
                            )
                            break

                # Check for more pages
                next_token = results_response.get('NextToken')
                if not next_token or len(rows) >= max_total_rows:
                    break

            return {
                'success': True,
                'rows': rows,
                'columns': columns,
                'row_count': len(rows),
                'error': None
            }

        except Exception as e:
            self.logger.error(f"Athena query failed: {e}")
            return {
                'success': False,
                'rows': [],
                'columns': [],
                'row_count': 0,
                'error': str(e)
            }

    def execute(self, context: AgentContext) -> AgentResult:
        """Execute data quality validation with pre/post load phases."""
        dq_config = context.config.get('data_quality', {})

        if not dq_config.get('enabled') in ('Y', 'y', True, 'true', 'True'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'data_quality.enabled is not Y'}
            )

        # Get table configurations
        source_tables = context.config.get('source_tables', [])
        target_tables = context.config.get('target_tables', [])

        # Build table name map
        all_tables = {}
        for t in source_tables:
            full_name = f"{t.get('database')}.{t.get('table')}"
            all_tables[full_name] = {'config': t, 'type': 'source'}
        for t in target_tables:
            full_name = f"{t.get('database')}.{t.get('table')}"
            all_tables[full_name] = {'config': t, 'type': 'target'}

        # Determine execution phase
        execution_phase = context.get_shared('execution_phase', 'pre_load')

        all_results = []
        total_records_scanned = 0
        total_outliers = 0
        passed = 0
        failed = 0
        warnings = 0
        recommendations = []

        # =====================
        # PRE-LOAD VALIDATION
        # =====================
        if execution_phase == 'pre_load':
            # Process pre_load_rules
            pre_load_rules = dq_config.get('pre_load_rules', [])
            for table_config in pre_load_rules:
                table_name = table_config.get('table')
                rules = table_config.get('rules', [])

                for rule in rules:
                    result = self._validate_rule_with_metrics(
                        table_name, rule, all_tables, context, 'pre_load'
                    )
                    all_results.append(result)
                    total_records_scanned += result.get('records_scanned', 0)
                    total_outliers += result.get('outliers_found', 0)

                    if result['pass_fail_status'] == 'PASS':
                        passed += 1
                    elif result.get('severity') == 'critical':
                        failed += 1
                    else:
                        warnings += 1

            # Also process legacy table_rules for source tables
            table_rules = dq_config.get('table_rules', [])
            for table_config in table_rules:
                table_name = table_config.get('table')
                if table_name in all_tables and all_tables[table_name]['type'] == 'source':
                    for rule in table_config.get('rules', []):
                        result = self._validate_rule_with_metrics(
                            table_name, rule, all_tables, context, 'pre_load'
                        )
                        all_results.append(result)
                        total_records_scanned += result.get('records_scanned', 0)
                        total_outliers += result.get('outliers_found', 0)

                        if result['pass_fail_status'] == 'PASS':
                            passed += 1
                        elif result.get('severity') == 'critical':
                            failed += 1
                        else:
                            warnings += 1

            # Process natural language rules for source tables
            nl_rules = dq_config.get('natural_language_rules', [])
            for nl_rule in nl_rules:
                tables_to_check = []
                if isinstance(nl_rule, str):
                    # Legacy: apply to first source table
                    if source_tables:
                        t = source_tables[0]
                        tables_to_check = [f"{t.get('database')}.{t.get('table')}"]
                    parsed = self._parse_nl_rule(nl_rule)
                else:
                    tables_to_check = nl_rule.get('tables', [])
                    parsed = self._parse_nl_rule(nl_rule.get('rule', ''))
                    parsed['severity'] = nl_rule.get('severity', 'warning')

                for table_name in tables_to_check:
                    if table_name in all_tables and all_tables[table_name]['type'] == 'source':
                        result = self._validate_rule_with_metrics(
                            table_name, parsed, all_tables, context, 'pre_load'
                        )
                        result['original_rule'] = nl_rule if isinstance(nl_rule, str) else nl_rule.get('rule')
                        all_results.append(result)
                        total_records_scanned += result.get('records_scanned', 0)
                        total_outliers += result.get('outliers_found', 0)

                        if result['pass_fail_status'] == 'PASS':
                            passed += 1
                        elif result.get('severity') == 'critical':
                            failed += 1
                        else:
                            warnings += 1

        # =====================
        # POST-LOAD VALIDATION
        # =====================
        elif execution_phase == 'post_load':
            # Process post_load_rules
            post_load_rules = dq_config.get('post_load_rules', [])
            for table_config in post_load_rules:
                table_name = table_config.get('table')
                rules = table_config.get('rules', [])

                for rule in rules:
                    result = self._validate_rule_with_metrics(
                        table_name, rule, all_tables, context, 'post_load'
                    )
                    all_results.append(result)
                    total_records_scanned += result.get('records_scanned', 0)
                    total_outliers += result.get('outliers_found', 0)

                    if result['pass_fail_status'] == 'PASS':
                        passed += 1
                    elif result.get('severity') == 'critical':
                        failed += 1
                    else:
                        warnings += 1

            # Also process legacy table_rules for target tables
            table_rules = dq_config.get('table_rules', [])
            for table_config in table_rules:
                table_name = table_config.get('table')
                if table_name in all_tables and all_tables[table_name]['type'] == 'target':
                    for rule in table_config.get('rules', []):
                        result = self._validate_rule_with_metrics(
                            table_name, rule, all_tables, context, 'post_load'
                        )
                        all_results.append(result)
                        total_records_scanned += result.get('records_scanned', 0)
                        total_outliers += result.get('outliers_found', 0)

                        if result['pass_fail_status'] == 'PASS':
                            passed += 1
                        elif result.get('severity') == 'critical':
                            failed += 1
                        else:
                            warnings += 1

            # Process SQL rules (typically for post-load)
            sql_rules = dq_config.get('sql_rules', [])
            for sql_rule in sql_rules:
                result = self._validate_sql_rule_with_metrics(sql_rule, context)
                all_results.append(result)
                total_records_scanned += result.get('records_scanned', 0)
                total_outliers += result.get('outliers_found', 0)

                if result['pass_fail_status'] == 'PASS':
                    passed += 1
                elif result.get('severity') == 'critical':
                    failed += 1
                else:
                    warnings += 1

        # Generate recommendations
        if failed > 0:
            recommendations.append(f"CRITICAL: {failed} DQ rules failed in {execution_phase} phase")
        if warnings > 0:
            recommendations.append(f"WARNING: {warnings} DQ rules have warnings")
        if total_outliers > 0:
            outlier_pct = (total_outliers / max(total_records_scanned, 1)) * 100
            recommendations.append(f"Found {total_outliers} outliers ({outlier_pct:.2f}% of {total_records_scanned} records)")

        # Calculate score
        total_rules = len(all_results)
        score = (passed / max(total_rules, 1)) * 100

        # Store results
        self.storage.store_agent_data(
            self.AGENT_NAME,
            f'dq_rules_{execution_phase}',
            all_results,
            use_pipe_delimited=True
        )

        # Share with other agents
        context.set_shared(f'dq_rules_{execution_phase}', all_results)
        context.set_shared(f'dq_summary_{execution_phase}', {
            'passed': passed,
            'failed': failed,
            'warnings': warnings,
            'total': total_rules,
            'records_scanned': total_records_scanned,
            'outliers_found': total_outliers,
            'score': score
        })

        # Log to unified audit
        if self.audit:
            try:
                self.audit.log_data_quality(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    score=score,
                    rules_passed=passed,
                    rules_failed=failed + warnings,
                    agent_name=self.AGENT_NAME,
                    metadata={
                        'phase': execution_phase,
                        'records_scanned': total_records_scanned,
                        'outliers_found': total_outliers,
                        'rules_details': all_results
                    }
                )
            except Exception:
                pass

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'phase': execution_phase,
                'total_rules': total_rules,
                'passed': passed,
                'failed': failed,
                'warnings': warnings,
                'score': score,
                'records_scanned': total_records_scanned,
                'outliers_found': total_outliers,
                'rules': all_results
            },
            metrics={
                'total_rules': total_rules,
                'pass_rate': passed / max(total_rules, 1),
                'score': score,
                'records_scanned': total_records_scanned,
                'outliers_found': total_outliers
            },
            recommendations=recommendations
        )

    def _validate_rule_with_metrics(
        self,
        table_name: str,
        rule: Dict[str, Any],
        all_tables: Dict[str, Any],
        context: AgentContext,
        phase: str
    ) -> Dict[str, Any]:
        """
        Validate a rule by executing ACTUAL SQL via Athena.
        Returns metrics including real record counts and outliers.
        """
        rule_type = rule.get('type')
        column = rule.get('column')
        severity = rule.get('severity', 'warning')
        threshold = rule.get('threshold', 0)

        # Generate validation SQL
        sql = self._generate_validation_sql(table_name, rule)

        # Initialize result with defaults (will be updated by actual query)
        records_scanned = 0
        outliers_found = 0
        actual_value = 0
        pass_fail = 'FAIL'
        query_error = None

        # Execute actual query via Athena
        if rule_type == 'row_count_check':
            # Simple count query
            count_sql = f"SELECT COUNT(*) as record_count FROM {table_name}"
            query_result = self._execute_athena_query(count_sql, context)

            if query_result['success'] and query_result['rows']:
                try:
                    records_scanned = int(query_result['rows'][0][0])
                    actual_value = records_scanned
                    min_rows = rule.get('min_rows', 0)
                    pass_fail = 'PASS' if records_scanned >= min_rows else 'FAIL'
                    self.logger.info(
                        f"row_count_check on {table_name}: "
                        f"actual={records_scanned}, min_required={min_rows}, result={pass_fail}"
                    )
                except (ValueError, IndexError) as e:
                    query_error = f"Failed to parse row count: {e}"
            else:
                query_error = query_result.get('error', 'Query failed')

        elif rule_type == 'completeness':
            # Get total count and null count
            completeness_sql = f"""
                SELECT
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count
                FROM {table_name}
            """
            query_result = self._execute_athena_query(completeness_sql, context)

            if query_result['success'] and query_result['rows']:
                try:
                    row = query_result['rows'][0]
                    records_scanned = int(row[0])
                    outliers_found = int(row[1])  # null count
                    if records_scanned > 0:
                        actual_value = round(1 - (outliers_found / records_scanned), 4)
                    else:
                        actual_value = 0
                    pass_fail = 'PASS' if actual_value >= threshold else 'FAIL'
                    self.logger.info(
                        f"completeness on {table_name}.{column}: "
                        f"total={records_scanned}, nulls={outliers_found}, "
                        f"completeness={actual_value:.2%}, threshold={threshold}, result={pass_fail}"
                    )
                except (ValueError, IndexError) as e:
                    query_error = f"Failed to parse completeness result: {e}"
            else:
                query_error = query_result.get('error', 'Query failed')

        elif rule_type == 'not_null':
            # Count nulls
            null_sql = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {column} IS NULL"
            count_sql = f"SELECT COUNT(*) as total FROM {table_name}"

            # Execute both queries
            null_result = self._execute_athena_query(null_sql, context)
            count_result = self._execute_athena_query(count_sql, context)

            if null_result['success'] and count_result['success']:
                try:
                    outliers_found = int(null_result['rows'][0][0]) if null_result['rows'] else 0
                    records_scanned = int(count_result['rows'][0][0]) if count_result['rows'] else 0
                    actual_value = outliers_found
                    # For not_null, threshold is max allowed nulls (usually 0)
                    pass_fail = 'PASS' if outliers_found <= threshold else 'FAIL'
                    self.logger.info(
                        f"not_null on {table_name}.{column}: "
                        f"total={records_scanned}, nulls={outliers_found}, result={pass_fail}"
                    )
                except (ValueError, IndexError) as e:
                    query_error = f"Failed to parse not_null result: {e}"
            else:
                query_error = null_result.get('error') or count_result.get('error', 'Query failed')

        elif rule_type == 'positive':
            # Count non-positive values
            check_sql = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {column} IS NOT NULL AND {column} <= 0 THEN 1 ELSE 0 END) as outliers
                FROM {table_name}
            """
            query_result = self._execute_athena_query(check_sql, context)

            if query_result['success'] and query_result['rows']:
                try:
                    row = query_result['rows'][0]
                    records_scanned = int(row[0])
                    outliers_found = int(row[1])
                    actual_value = outliers_found
                    pass_fail = 'PASS' if outliers_found <= threshold else 'FAIL'
                    self.logger.info(
                        f"positive on {table_name}.{column}: "
                        f"total={records_scanned}, non_positive={outliers_found}, result={pass_fail}"
                    )
                except (ValueError, IndexError) as e:
                    query_error = f"Failed to parse positive result: {e}"
            else:
                query_error = query_result.get('error', 'Query failed')

        elif rule_type == 'unique':
            # Check for duplicates
            unique_sql = f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) - COUNT(DISTINCT {column}) as duplicates
                FROM {table_name}
                WHERE {column} IS NOT NULL
            """
            query_result = self._execute_athena_query(unique_sql, context)

            if query_result['success'] and query_result['rows']:
                try:
                    row = query_result['rows'][0]
                    records_scanned = int(row[0])
                    outliers_found = int(row[1])  # duplicates
                    actual_value = outliers_found
                    pass_fail = 'PASS' if outliers_found <= threshold else 'FAIL'
                    self.logger.info(
                        f"unique on {table_name}.{column}: "
                        f"total={records_scanned}, duplicates={outliers_found}, result={pass_fail}"
                    )
                except (ValueError, IndexError) as e:
                    query_error = f"Failed to parse unique result: {e}"
            else:
                query_error = query_result.get('error', 'Query failed')

        elif rule_type in ('greater_than', 'less_than'):
            value = rule.get('value', 0)
            op = '<=' if rule_type == 'greater_than' else '>='
            check_sql = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {column} IS NOT NULL AND {column} {op} {value} THEN 1 ELSE 0 END) as outliers
                FROM {table_name}
            """
            query_result = self._execute_athena_query(check_sql, context)

            if query_result['success'] and query_result['rows']:
                try:
                    row = query_result['rows'][0]
                    records_scanned = int(row[0])
                    outliers_found = int(row[1])
                    actual_value = outliers_found
                    pass_fail = 'PASS' if outliers_found <= threshold else 'FAIL'
                    self.logger.info(
                        f"{rule_type} on {table_name}.{column}: "
                        f"total={records_scanned}, outliers={outliers_found}, result={pass_fail}"
                    )
                except (ValueError, IndexError) as e:
                    query_error = f"Failed to parse {rule_type} result: {e}"
            else:
                query_error = query_result.get('error', 'Query failed')

        else:
            # For unknown rule types, try to execute the generated SQL directly
            query_result = self._execute_athena_query(sql, context)
            if query_result['success'] and query_result['rows']:
                try:
                    # Assume first column is the count/outliers
                    outliers_found = int(query_result['rows'][0][0])
                    actual_value = outliers_found
                    pass_fail = 'PASS' if outliers_found <= threshold else 'FAIL'
                except (ValueError, IndexError):
                    query_error = "Failed to parse result"
            else:
                query_error = query_result.get('error', 'Query failed')

        result = {
            'table': table_name,
            'column': column,
            'rule_type': rule_type,
            'severity': severity,
            'phase': phase,
            'validation_sql': sql,
            'records_scanned': records_scanned,
            'outliers_found': outliers_found,
            'threshold': threshold,
            'actual_value': actual_value,
            'pass_fail_status': pass_fail,
            'validated_at': datetime.utcnow().isoformat(),
            'table_exists': table_name in all_tables,
            'query_error': query_error
        }

        # If query failed, log as error but don't crash
        if query_error:
            self.logger.error(
                f"DQ rule validation failed for {rule_type} on {table_name}.{column}: {query_error}"
            )
            result['pass_fail_status'] = 'ERROR'

        # Log individual rule to audit
        if self.audit:
            try:
                self.audit.log_event(
                    event_type=EventType.DATA_QUALITY if EventType else 'data_quality_rule',
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    agent_name=self.AGENT_NAME,
                    status=pass_fail.lower(),
                    message=f"DQ Rule {rule_type} on {table_name}.{column}: {pass_fail}",
                    metadata={
                        'table': table_name,
                        'column': column,
                        'rule_type': rule_type,
                        'phase': phase,
                        'records_scanned': records_scanned,
                        'outliers_found': outliers_found,
                        'threshold': threshold,
                        'actual_value': actual_value,
                        'query_error': query_error
                    }
                )
            except Exception:
                pass

        return result

    def _validate_sql_rule_with_metrics(
        self,
        sql_rule: Dict[str, Any],
        context: AgentContext
    ) -> Dict[str, Any]:
        """Validate a SQL rule by executing it via Athena."""
        rule_id = sql_rule.get('id', 'UNKNOWN')
        sql = sql_rule.get('sql', '')
        threshold = sql_rule.get('threshold', 0)
        severity = sql_rule.get('severity', 'warning')

        # Extract table name from SQL for logging
        table_match = sql.upper().split('FROM')
        table_name = table_match[1].split()[0].lower() if len(table_match) > 1 else 'unknown'

        # Execute actual SQL via Athena
        records_scanned = 0
        outliers_found = 0
        pass_fail = 'FAIL'
        query_error = None

        query_result = self._execute_athena_query(sql, context)

        if query_result['success'] and query_result['rows']:
            try:
                # SQL rules should return a count of violations in first column
                # E.g., SELECT COUNT(*) FROM table WHERE some_condition
                outliers_found = int(query_result['rows'][0][0])

                # If query has second column, treat as total records for context
                if len(query_result['rows'][0]) > 1:
                    records_scanned = int(query_result['rows'][0][1])
                else:
                    # Run a count query to get total records
                    count_result = self._execute_athena_query(
                        f"SELECT COUNT(*) FROM {table_name}", context
                    )
                    if count_result['success'] and count_result['rows']:
                        records_scanned = int(count_result['rows'][0][0])

                pass_fail = 'PASS' if outliers_found <= threshold else 'FAIL'
                self.logger.info(
                    f"SQL rule {rule_id}: outliers={outliers_found}, "
                    f"threshold={threshold}, result={pass_fail}"
                )
            except (ValueError, IndexError) as e:
                query_error = f"Failed to parse SQL rule result: {e}"
        else:
            query_error = query_result.get('error', 'Query failed')

        result = {
            'rule_id': rule_id,
            'table': table_name,
            'rule_type': 'sql',
            'severity': severity,
            'phase': 'post_load',
            'validation_sql': sql,
            'threshold': threshold,
            'records_scanned': records_scanned,
            'outliers_found': outliers_found,
            'actual_value': outliers_found,
            'pass_fail_status': pass_fail if not query_error else 'ERROR',
            'description': sql_rule.get('description', ''),
            'validated_at': datetime.utcnow().isoformat(),
            'query_error': query_error
        }

        if query_error:
            self.logger.error(f"SQL rule {rule_id} failed: {query_error}")

        # Log to audit
        if self.audit:
            try:
                self.audit.log_event(
                    event_type=EventType.DATA_QUALITY if EventType else 'data_quality_rule',
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    agent_name=self.AGENT_NAME,
                    status=pass_fail.lower(),
                    message=f"SQL Rule {rule_id}: {pass_fail}",
                    metadata={
                        'rule_id': rule_id,
                        'table': table_name,
                        'records_scanned': records_scanned,
                        'outliers_found': outliers_found,
                        'threshold': threshold,
                        'query_error': query_error
                    }
                )
            except Exception:
                pass

        return result

    def _generate_validation_sql(self, table_name: str, rule: Dict[str, Any]) -> str:
        """Generate validation SQL for a rule."""
        rule_type = rule.get('type')
        column = rule.get('column')

        if rule_type == 'not_null':
            return f"SELECT COUNT(*) as outliers FROM {table_name} WHERE {column} IS NULL"
        elif rule_type == 'positive':
            return f"SELECT COUNT(*) as outliers FROM {table_name} WHERE {column} IS NOT NULL AND {column} <= 0"
        elif rule_type == 'unique':
            return f"SELECT COUNT(*) - COUNT(DISTINCT {column}) as outliers FROM {table_name} WHERE {column} IS NOT NULL"
        elif rule_type == 'completeness':
            return f"SELECT COUNT(*) as total, SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as outliers FROM {table_name}"
        elif rule_type == 'row_count_check':
            return f"SELECT COUNT(*) as record_count FROM {table_name}"
        elif rule_type == 'greater_than':
            value = rule.get('value', 0)
            return f"SELECT COUNT(*) as outliers FROM {table_name} WHERE {column} IS NOT NULL AND {column} <= {value}"
        elif rule_type == 'less_than':
            value = rule.get('value', 0)
            return f"SELECT COUNT(*) as outliers FROM {table_name} WHERE {column} IS NOT NULL AND {column} >= {value}"
        elif rule_type == 'pattern':
            pattern = rule.get('pattern', '.*')
            return f"SELECT COUNT(*) as outliers FROM {table_name} WHERE {column} IS NOT NULL AND NOT REGEXP_LIKE({column}, '{pattern}')"
        else:
            return f"-- Unknown rule type: {rule_type} for column {column}"

    def _parse_nl_rule(self, rule: str) -> Dict[str, Any]:
        """Parse natural language rule into structured format."""
        rule_lower = rule.lower()
        words = rule.split()
        column = words[0] if words else 'unknown'

        for pattern, rule_type in self.NL_PATTERNS.items():
            if pattern in rule_lower:
                result = {'type': rule_type, 'column': column}
                if rule_type in ('greater_than', 'less_than'):
                    try:
                        idx = words.index('than') + 1
                        result['value'] = float(words[idx])
                    except (ValueError, IndexError):
                        result['value'] = 0
                return result

        return {'type': 'custom', 'column': column, 'expression': rule}
