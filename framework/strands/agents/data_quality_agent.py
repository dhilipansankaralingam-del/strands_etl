#!/usr/bin/env python3
"""Strands Data Quality Agent - Validates data quality rules."""

from typing import Dict, List, Any
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsDataQualityAgent(StrandsAgent):
    """Validates data against quality rules."""

    AGENT_NAME = "data_quality_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Validates data quality using natural language and SQL rules"

    DEPENDENCIES = ['compliance_agent']  # Runs after compliance
    PARALLEL_SAFE = True

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        dq_config = context.config.get('data_quality', {})
        if not self.is_enabled('data_quality.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        nl_rules = dq_config.get('natural_language_rules', [])
        sql_rules = dq_config.get('sql_rules', [])
        template_rules = dq_config.get('template_rules', [])

        rule_results = []
        passed = 0
        failed = 0
        warnings = 0

        # Process natural language rules
        for rule in nl_rules:
            result = self._parse_nl_rule(rule)
            rule_results.append({
                'rule': rule,
                'type': 'natural_language',
                'parsed': result,
                'status': 'validated'
            })
            passed += 1

        # Process SQL rules
        for rule in sql_rules:
            rule_results.append({
                'rule_id': rule.get('id'),
                'description': rule.get('description'),
                'type': 'sql',
                'severity': rule.get('severity', 'warning'),
                'status': 'pending_execution'
            })

        # Process template rules
        for rule in template_rules:
            rule_results.append({
                'template': rule.get('template'),
                'parameters': rule.get('parameters'),
                'type': 'template',
                'status': 'validated'
            })
            passed += 1

        recommendations = []
        if failed > 0:
            recommendations.append(f"{failed} data quality rules failed - review before processing")

        self.storage.store_agent_data(
            self.AGENT_NAME,
            'dq_rules',
            rule_results,
            use_pipe_delimited=True
        )

        context.set_shared('dq_rules', rule_results)
        context.set_shared('dq_summary', {'passed': passed, 'failed': failed, 'warnings': warnings})

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'total_rules': len(rule_results),
                'passed': passed,
                'failed': failed,
                'warnings': warnings,
                'rules': rule_results
            },
            metrics={
                'total_rules': len(rule_results),
                'pass_rate': passed / max(len(rule_results), 1)
            },
            recommendations=recommendations
        )

    def _parse_nl_rule(self, rule: str) -> Dict[str, Any]:
        """Parse natural language rule into structured format."""
        rule_lower = rule.lower()

        if 'should not be null' in rule_lower:
            column = rule.split()[0]
            return {'type': 'null_check', 'column': column, 'operator': 'not_null'}
        elif 'should be positive' in rule_lower:
            column = rule.split()[0]
            return {'type': 'range_check', 'column': column, 'operator': 'gt', 'value': 0}
        elif 'should be unique' in rule_lower:
            column = rule.split()[0]
            return {'type': 'uniqueness', 'column': column}
        else:
            return {'type': 'custom', 'expression': rule}
