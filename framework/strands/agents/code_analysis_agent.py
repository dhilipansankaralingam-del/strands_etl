#!/usr/bin/env python3
"""Strands Code Analysis Agent - Analyzes PySpark code for optimizations."""

import re
import json
from typing import Dict, List, Any, Tuple
from pathlib import Path
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage

# LLM support
try:
    from ..llm import ResponseParser
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    ResponseParser = None


@register_agent
class StrandsCodeAnalysisAgent(StrandsAgent):
    """Analyzes PySpark code for anti-patterns and optimization opportunities."""

    AGENT_NAME = "code_analysis_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Analyzes PySpark code for anti-patterns and optimizations"

    DEPENDENCIES = []
    PARALLEL_SAFE = True

    ANTI_PATTERNS = {
        'collect_in_driver': (r'\.collect\(\)', 'Avoid collect() - can cause OOM on driver'),
        'toPandas': (r'\.toPandas\(\)', 'toPandas() loads all data to driver memory'),
        'udf_without_type': (r'@udf\s*\n', 'UDF without return type hint is slow'),
        'crossJoin': (r'\.crossJoin\(', 'Cross joins are expensive - ensure intentional'),
        'repartition_shuffle': (r'\.repartition\(\d+\)', 'repartition causes full shuffle'),
    }

    OPTIMIZATION_PATTERNS = {
        'broadcast_join': (r'broadcast\(', 'Good: Using broadcast for small tables'),
        'cache_persist': (r'\.(cache|persist)\(\)', 'Good: Caching intermediate results'),
        'coalesce': (r'\.coalesce\(', 'Good: Using coalesce to reduce partitions'),
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        code_config = context.config.get('code_analysis', {})
        if not self.is_enabled('code_analysis.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        script_path = context.config.get('script', {}).get('local_path', '')
        code_content = None

        if script_path:
            try:
                code_content = Path(script_path).read_text()
            except Exception:
                pass

        findings = []
        recommendations = []

        if code_content:
            # Check anti-patterns
            for name, (pattern, message) in self.ANTI_PATTERNS.items():
                matches = re.findall(pattern, code_content)
                if matches:
                    findings.append({
                        'type': 'anti_pattern',
                        'name': name,
                        'occurrences': len(matches),
                        'message': message,
                        'severity': 'warning'
                    })
                    recommendations.append(f"Anti-pattern: {message}")

            # Check optimizations
            for name, (pattern, message) in self.OPTIMIZATION_PATTERNS.items():
                matches = re.findall(pattern, code_content)
                if matches:
                    findings.append({
                        'type': 'optimization',
                        'name': name,
                        'occurrences': len(matches),
                        'message': message,
                        'severity': 'info'
                    })

            # Check for join patterns
            if code_config.get('check_join_optimizations') in ('Y', 'y', True):
                join_findings = self._analyze_joins(code_content)
                findings.extend(join_findings)

        self.storage.store_agent_data(
            self.AGENT_NAME,
            'code_findings',
            findings,
            use_pipe_delimited=True
        )

        context.set_shared('code_findings', findings)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'script_analyzed': script_path,
                'findings_count': len(findings),
                'findings': findings
            },
            metrics={
                'anti_patterns_found': len([f for f in findings if f['type'] == 'anti_pattern']),
                'optimizations_found': len([f for f in findings if f['type'] == 'optimization'])
            },
            recommendations=recommendations
        )

    def _analyze_joins(self, code: str) -> List[Dict]:
        """Analyze join patterns in code."""
        findings = []

        # Check for multiple joins without broadcast
        join_count = len(re.findall(r'\.join\(', code))
        broadcast_count = len(re.findall(r'broadcast\(', code))

        if join_count > 3 and broadcast_count == 0:
            findings.append({
                'type': 'recommendation',
                'name': 'missing_broadcast',
                'message': f'{join_count} joins found but no broadcast - consider broadcasting small tables',
                'severity': 'warning'
            })

        return findings
