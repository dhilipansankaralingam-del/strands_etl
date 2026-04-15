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
        """Execute code analysis - uses LLM if enabled, falls back to rules."""
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

        # Use LLM-enhanced analysis if enabled
        if self.is_llm_enabled(context) and code_content:
            return self._execute_with_llm(context, code_content, script_path)

        # Fall back to rule-based analysis
        return self._execute_rule_based(context, code_content, script_path)

    def _execute_rule_based(self, context: AgentContext, code_content: str, script_path: str) -> AgentResult:
        """Rule-based code analysis using regex patterns."""
        code_config = context.config.get('code_analysis', {})
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
        context.set_shared('code_analysis', {
            'join_count': len(re.findall(r'\.join\(', code_content)) if code_content else 0,
            'window_function_count': len(re.findall(r'Window\.', code_content)) if code_content else 0,
            'aggregation_count': len(re.findall(r'\.(groupBy|agg)\(', code_content)) if code_content else 0
        })

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'script_analyzed': script_path,
                'findings_count': len(findings),
                'findings': findings,
                'llm_used': False
            },
            metrics={
                'anti_patterns_found': len([f for f in findings if f['type'] == 'anti_pattern']),
                'optimizations_found': len([f for f in findings if f['type'] == 'optimization'])
            },
            recommendations=recommendations
        )

    def _execute_with_llm(self, context: AgentContext, code_content: str, script_path: str) -> AgentResult:
        """LLM-enhanced code analysis with semantic understanding."""
        self.logger.info("Using LLM-enhanced code analysis")

        # Build context for LLM
        table_info = context.config.get('source_tables', [])
        size_gb = context.get_shared('total_size_gb', 0)

        prompt = f"""Analyze this PySpark script for performance issues and optimization opportunities.

```python
{code_content[:8000]}  # Truncate for token limits
```

Context:
- Source tables: {json.dumps([t.get('table_name', t.get('name', 'unknown')) for t in table_info[:10]])}
- Estimated data size: {size_gb} GB

Provide your analysis as JSON with this structure:
{{
    "issues": [
        {{
            "type": "anti_pattern|performance|memory|shuffle",
            "severity": "critical|high|medium|low",
            "line": <line_number_or_null>,
            "description": "what the issue is",
            "recommendation": "how to fix it",
            "impact": "estimated performance impact"
        }}
    ],
    "summary": {{
        "total_issues": <count>,
        "critical_count": <count>,
        "estimated_improvement": "percentage improvement possible"
    }},
    "spark_configs": {{
        "recommended_config_key": "value"
    }}
}}"""

        try:
            response = self.llm.invoke(prompt, system=self.get_system_prompt())

            if response.error:
                self.logger.warning(f"LLM failed: {response.error}. Falling back to rules.")
                return self._execute_rule_based(context, code_content, script_path)

            # Parse LLM response
            if LLM_AVAILABLE and ResponseParser:
                issues, metadata = ResponseParser.parse_code_analysis(response.content)
                findings = [
                    {
                        'type': issue.issue_type,
                        'name': issue.issue_type,
                        'line': issue.line_number,
                        'message': issue.description,
                        'recommendation': issue.recommendation,
                        'severity': issue.severity,
                        'impact': issue.estimated_impact
                    }
                    for issue in issues
                ]
                recommendations = [f"[{i.severity.upper()}] {i.recommendation}" for i in issues[:10]]
                spark_configs = metadata.get('spark_configs', {})
            else:
                # Basic JSON parsing fallback
                try:
                    data = json.loads(response.content)
                    findings = data.get('issues', [])
                    recommendations = [f.get('recommendation', '') for f in findings[:10]]
                    spark_configs = data.get('spark_configs', {})
                except json.JSONDecodeError:
                    self.logger.warning("Could not parse LLM response as JSON")
                    return self._execute_rule_based(context, code_content, script_path)

            # Store findings
            self.storage.store_agent_data(
                self.AGENT_NAME,
                'code_findings',
                findings,
                use_pipe_delimited=True
            )

            context.set_shared('code_findings', findings)
            context.set_shared('code_analysis', {
                'join_count': len(re.findall(r'\.join\(', code_content)),
                'window_function_count': len(re.findall(r'Window\.', code_content)),
                'aggregation_count': len(re.findall(r'\.(groupBy|agg)\(', code_content)),
                'spark_configs': spark_configs
            })

            # Track LLM usage
            context.llm_stats[self.AGENT_NAME] = response.to_dict()

            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={
                    'script_analyzed': script_path,
                    'findings_count': len(findings),
                    'findings': findings,
                    'spark_configs': spark_configs,
                    'llm_used': True,
                    'llm_cost': response.cost_usd
                },
                metrics={
                    'anti_patterns_found': len([f for f in findings if f.get('severity') in ['critical', 'high']]),
                    'optimizations_found': len(findings),
                    'llm_tokens': response.input_tokens + response.output_tokens,
                    'llm_cost_usd': response.cost_usd
                },
                recommendations=recommendations
            )

        except Exception as e:
            self.logger.error(f"LLM analysis failed: {e}. Falling back to rules.")
            return self._execute_rule_based(context, code_content, script_path)

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
