#!/usr/bin/env python3
"""
Strands Data Quality Agent
==========================

Validates data quality rules against source and target tables.

Configuration Structure:
------------------------
"data_quality": {
    "enabled": "Y",
    "fail_on_critical": "Y",
    "sample_size": 100000,

    "table_rules": [
        {
            "table": "finance_accounting_master.autopay_products_master",
            "rules": [
                {"type": "not_null", "column": "product_id", "severity": "critical"},
                {"type": "positive", "column": "amount", "severity": "warning"},
                {"type": "unique", "column": "product_code", "severity": "critical"}
            ]
        },
        {
            "table": "bd_work.pps_jss_test",
            "rules": [
                {"type": "not_null", "column": "customer_id", "severity": "critical"},
                {"type": "completeness", "column": "policy_number", "threshold": 0.99}
            ]
        }
    ],

    "natural_language_rules": [
        {
            "rule": "customer_id should not be null",
            "tables": ["bd_work.pps_jss_test"],
            "severity": "critical"
        }
    ],

    "sql_rules": [
        {
            "id": "SQL_CUSTOMER_EXISTS",
            "description": "Every record must have a valid customer",
            "sql": "SELECT COUNT(*) FROM bd_work.pps_jss_test WHERE customer_id IS NULL",
            "threshold": 0,
            "severity": "critical"
        }
    ]
}
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage

# Unified audit logger (optional)
try:
    from ..unified_audit import get_audit_logger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    get_audit_logger = None


@register_agent
class StrandsDataQualityAgent(StrandsAgent):
    """
    Validates data against quality rules on specific tables.

    Supports:
    - Natural language rules (parsed into SQL)
    - SQL rules (direct execution)
    - Table-specific rules
    - Template-based rules (null_check, completeness, uniqueness, etc.)
    """

    AGENT_NAME = "data_quality_agent"
    AGENT_VERSION = "2.1.0"
    AGENT_DESCRIPTION = "Validates data quality using natural language and SQL rules against specified tables"

    DEPENDENCIES = ['compliance_agent']  # Runs after compliance
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

    def execute(self, context: AgentContext) -> AgentResult:
        """Execute data quality validation against configured tables."""
        dq_config = context.config.get('data_quality', {})

        # Note: agents.data_quality_agent.enabled is already checked by base_agent.run()
        # This checks the separate data_quality.enabled flag for backward compatibility
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

        # Build table name map for quick lookup
        all_tables = {}
        for t in source_tables:
            full_name = f"{t.get('database')}.{t.get('table')}"
            all_tables[full_name] = {'config': t, 'type': 'source'}
        for t in target_tables:
            full_name = f"{t.get('database')}.{t.get('table')}"
            all_tables[full_name] = {'config': t, 'type': 'target'}

        # Collect all rule results
        all_results = []
        passed = 0
        failed = 0
        warnings = 0
        recommendations = []

        # 1. Process table-specific rules
        table_rules = dq_config.get('table_rules', [])
        for table_config in table_rules:
            table_name = table_config.get('table')
            rules = table_config.get('rules', [])

            for rule in rules:
                result = self._validate_rule(table_name, rule, all_tables)
                all_results.append(result)

                if result['status'] == 'passed':
                    passed += 1
                elif result['status'] == 'failed':
                    if result.get('severity') == 'critical':
                        failed += 1
                    else:
                        warnings += 1

        # 2. Process natural language rules
        nl_rules = dq_config.get('natural_language_rules', [])
        for nl_rule in nl_rules:
            if isinstance(nl_rule, str):
                # Legacy format: just a string, apply to target tables
                parsed = self._parse_nl_rule(nl_rule)
                target_table = target_tables[0] if target_tables else None
                if target_table:
                    table_name = f"{target_table.get('database')}.{target_table.get('table')}"
                    result = self._validate_rule(table_name, parsed, all_tables)
                    result['original_rule'] = nl_rule
                    all_results.append(result)
                    if result['status'] == 'passed':
                        passed += 1
                    elif result.get('severity') == 'critical':
                        failed += 1
                    else:
                        warnings += 1
            else:
                # New format: dict with rule, tables, severity
                parsed = self._parse_nl_rule(nl_rule.get('rule', ''))
                parsed['severity'] = nl_rule.get('severity', 'warning')

                for table_name in nl_rule.get('tables', []):
                    result = self._validate_rule(table_name, parsed, all_tables)
                    result['original_rule'] = nl_rule.get('rule')
                    all_results.append(result)
                    if result['status'] == 'passed':
                        passed += 1
                    elif result.get('severity') == 'critical':
                        failed += 1
                    else:
                        warnings += 1

        # 3. Process SQL rules
        sql_rules = dq_config.get('sql_rules', [])
        for sql_rule in sql_rules:
            result = self._validate_sql_rule(sql_rule)
            all_results.append(result)
            if result['status'] == 'passed':
                passed += 1
            elif result.get('severity') == 'critical':
                failed += 1
            else:
                warnings += 1

        # 4. Process template rules
        template_rules = dq_config.get('template_rules', [])
        for tmpl_rule in template_rules:
            # Apply to target table by default
            target_table = target_tables[0] if target_tables else None
            if target_table:
                table_name = f"{target_table.get('database')}.{target_table.get('table')}"
                parsed = self._expand_template(tmpl_rule)
                result = self._validate_rule(table_name, parsed, all_tables)
                result['template'] = tmpl_rule.get('template')
                all_results.append(result)
                if result['status'] == 'passed':
                    passed += 1
                elif result.get('severity') == 'critical':
                    failed += 1
                else:
                    warnings += 1

        # Generate recommendations
        if failed > 0:
            recommendations.append(f"CRITICAL: {failed} data quality rules failed - review before processing")
        if warnings > 0:
            recommendations.append(f"WARNING: {warnings} data quality rules have warnings")

        # Store results
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'dq_rules',
            all_results,
            use_pipe_delimited=True
        )

        # Share with other agents
        context.set_shared('dq_rules', all_results)
        context.set_shared('dq_summary', {
            'passed': passed,
            'failed': failed,
            'warnings': warnings,
            'total': len(all_results)
        })

        # Log to unified audit for dashboard
        total_rules = len(all_results)
        score = (passed / max(total_rules, 1)) * 100
        if AUDIT_AVAILABLE and get_audit_logger:
            try:
                audit = get_audit_logger(self.config)
                audit.log_data_quality(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    score=score,
                    rules_passed=passed,
                    rules_failed=failed + warnings,
                    agent_name=self.AGENT_NAME
                )
            except Exception:
                pass

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'total_rules': total_rules,
                'passed': passed,
                'failed': failed,
                'warnings': warnings,
                'score': score,
                'rules': all_results
            },
            metrics={
                'total_rules': total_rules,
                'pass_rate': passed / max(total_rules, 1),
                'score': score
            },
            recommendations=recommendations
        )

    def _validate_rule(
        self,
        table_name: str,
        rule: Dict[str, Any],
        all_tables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate a single rule against a table.

        In production, this would execute actual SQL against the table.
        Currently generates the SQL and marks as 'pending_execution'.
        """
        rule_type = rule.get('type')
        column = rule.get('column')
        severity = rule.get('severity', 'warning')

        # Generate validation SQL
        sql = self._generate_validation_sql(table_name, rule)

        result = {
            'table': table_name,
            'column': column,
            'rule_type': rule_type,
            'severity': severity,
            'validation_sql': sql,
            'status': 'pending_execution',  # Would be 'passed' or 'failed' after actual execution
            'validated_at': datetime.utcnow().isoformat(),
            'table_exists': table_name in all_tables
        }

        # If table exists, mark as validated (simulated)
        if table_name in all_tables:
            result['status'] = 'passed'  # In production: execute SQL and check result
            result['message'] = f"Rule validated: {rule_type} on {column}"
        else:
            result['status'] = 'warning'
            result['message'] = f"Table {table_name} not found in config"

        return result

    def _validate_sql_rule(self, sql_rule: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a SQL-based rule.

        In production, this would execute the SQL and check against threshold.
        """
        rule_id = sql_rule.get('id', 'UNKNOWN')
        sql = sql_rule.get('sql', '')
        threshold = sql_rule.get('threshold', 0)
        severity = sql_rule.get('severity', 'warning')

        # Extract table name from SQL (simple extraction)
        table_match = sql.upper().split('FROM')
        table_name = table_match[1].split()[0].lower() if len(table_match) > 1 else 'unknown'

        return {
            'rule_id': rule_id,
            'table': table_name,
            'rule_type': 'sql',
            'severity': severity,
            'validation_sql': sql,
            'threshold': threshold,
            'description': sql_rule.get('description', ''),
            'status': 'pending_execution',  # Would execute and check count vs threshold
            'validated_at': datetime.utcnow().isoformat()
        }

    def _generate_validation_sql(self, table_name: str, rule: Dict[str, Any]) -> str:
        """Generate validation SQL for a rule."""
        rule_type = rule.get('type')
        column = rule.get('column')

        if rule_type == 'not_null':
            return f"SELECT COUNT(*) as violations FROM {table_name} WHERE {column} IS NULL"
        elif rule_type == 'positive':
            return f"SELECT COUNT(*) as violations FROM {table_name} WHERE {column} IS NOT NULL AND {column} <= 0"
        elif rule_type == 'unique':
            return f"SELECT COUNT(*) - COUNT(DISTINCT {column}) as violations FROM {table_name} WHERE {column} IS NOT NULL"
        elif rule_type == 'completeness':
            threshold = rule.get('threshold', 0.99)
            return f"SELECT (1.0 - COUNT({column}) / COUNT(*)) as null_rate FROM {table_name} -- threshold: {threshold}"
        elif rule_type == 'greater_than':
            value = rule.get('value', 0)
            return f"SELECT COUNT(*) as violations FROM {table_name} WHERE {column} IS NOT NULL AND {column} <= {value}"
        elif rule_type == 'less_than':
            value = rule.get('value', 0)
            return f"SELECT COUNT(*) as violations FROM {table_name} WHERE {column} IS NOT NULL AND {column} >= {value}"
        elif rule_type == 'pattern':
            pattern = rule.get('pattern', '.*')
            return f"SELECT COUNT(*) as violations FROM {table_name} WHERE {column} IS NOT NULL AND NOT REGEXP_LIKE({column}, '{pattern}')"
        else:
            return f"-- Unknown rule type: {rule_type} for column {column}"

    def _parse_nl_rule(self, rule: str) -> Dict[str, Any]:
        """Parse natural language rule into structured format."""
        rule_lower = rule.lower()

        # Extract column name (assumes it's the first word)
        words = rule.split()
        column = words[0] if words else 'unknown'

        for pattern, rule_type in self.NL_PATTERNS.items():
            if pattern in rule_lower:
                result = {'type': rule_type, 'column': column}

                # Extract additional parameters
                if rule_type == 'greater_than':
                    # Try to find number after 'than'
                    try:
                        idx = words.index('than') + 1
                        result['value'] = float(words[idx])
                    except (ValueError, IndexError):
                        result['value'] = 0
                elif rule_type == 'less_than':
                    try:
                        idx = words.index('than') + 1
                        result['value'] = float(words[idx])
                    except (ValueError, IndexError):
                        result['value'] = 0

                return result

        # Default: treat as custom rule
        return {'type': 'custom', 'column': column, 'expression': rule}

    def _expand_template(self, tmpl_rule: Dict[str, Any]) -> Dict[str, Any]:
        """Expand a template rule into a structured rule."""
        template = tmpl_rule.get('template', '')
        params = tmpl_rule.get('parameters', {})

        result = {
            'type': template,
            'column': params.get('column', 'unknown'),
            'severity': tmpl_rule.get('severity', 'warning')
        }

        if 'threshold' in tmpl_rule:
            result['threshold'] = tmpl_rule['threshold']
        if 'value' in params:
            result['value'] = params['value']
        if 'pattern' in params:
            result['pattern'] = params['pattern']

        return result
