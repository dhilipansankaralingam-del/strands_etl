#!/usr/bin/env python3
"""
LLM Response Parser
===================

Parses structured responses from LLM into typed data structures.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger("strands.llm.parser")


@dataclass
class CodeIssue:
    """A code issue found by analysis."""
    issue_type: str
    severity: str  # critical, high, medium, low
    line_number: Optional[int]
    description: str
    recommendation: str
    code_snippet: str = ""
    estimated_impact: str = ""


@dataclass
class ResourceRecommendation:
    """Resource allocation recommendation."""
    workers: int
    worker_type: str
    platform: str
    estimated_cost_per_run: float
    estimated_monthly_cost: float
    reasoning: str
    alternatives: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class HealingStrategy:
    """Error healing strategy."""
    error_type: str
    root_cause: str
    remediation: str
    config_changes: Dict[str, Any] = field(default_factory=dict)
    retryable: bool = True
    prevention: str = ""


@dataclass
class PrioritizedRecommendation:
    """A prioritized recommendation with ROI."""
    priority: str  # P0, P1, P2, P3
    category: str  # performance, cost, reliability, compliance
    recommendation: str
    estimated_savings_usd: float
    implementation_effort: str  # hours or days
    payback_period: str
    source_agent: str
    details: Dict[str, Any] = field(default_factory=dict)


class ResponseParser:
    """Parser for LLM responses into structured data."""

    @staticmethod
    def parse_code_analysis(response: str) -> Tuple[List[CodeIssue], Dict[str, Any]]:
        """
        Parse code analysis response into structured issues.

        Expected format:
        {
            "issues": [
                {
                    "type": "collect_on_large_data",
                    "severity": "critical",
                    "line": 45,
                    "description": "...",
                    "recommendation": "...",
                    "impact": "..."
                }
            ],
            "summary": {...},
            "spark_configs": {...}
        }
        """
        try:
            # Try to parse as JSON
            data = ResponseParser._extract_json(response)

            issues = []
            for item in data.get('issues', []):
                issues.append(CodeIssue(
                    issue_type=item.get('type', 'unknown'),
                    severity=item.get('severity', 'medium'),
                    line_number=item.get('line'),
                    description=item.get('description', ''),
                    recommendation=item.get('recommendation', ''),
                    code_snippet=item.get('snippet', ''),
                    estimated_impact=item.get('impact', '')
                ))

            metadata = {
                'summary': data.get('summary', {}),
                'spark_configs': data.get('spark_configs', {}),
                'total_issues': len(issues),
                'critical_count': sum(1 for i in issues if i.severity == 'critical'),
                'high_count': sum(1 for i in issues if i.severity == 'high')
            }

            return issues, metadata

        except Exception as e:
            logger.warning(f"Failed to parse code analysis response: {e}")
            # Try to extract issues from text format
            return ResponseParser._parse_code_analysis_text(response)

    @staticmethod
    def _parse_code_analysis_text(response: str) -> Tuple[List[CodeIssue], Dict[str, Any]]:
        """Fallback parser for text-based responses."""
        issues = []

        # Look for patterns like "Line 45: collect() on large dataset"
        line_pattern = r'[Ll]ine\s*(\d+)[:\s]*(.+?)(?=\n[Ll]ine|\n\n|$)'
        for match in re.finditer(line_pattern, response, re.DOTALL):
            line_num = int(match.group(1))
            desc = match.group(2).strip()

            # Determine severity from keywords
            severity = 'medium'
            if any(w in desc.lower() for w in ['critical', 'oom', 'memory', 'crash']):
                severity = 'critical'
            elif any(w in desc.lower() for w in ['slow', 'inefficient', 'performance']):
                severity = 'high'

            issues.append(CodeIssue(
                issue_type='detected_issue',
                severity=severity,
                line_number=line_num,
                description=desc,
                recommendation='See description'
            ))

        return issues, {'parsed_from_text': True}

    @staticmethod
    def parse_resource_recommendation(response: str) -> ResourceRecommendation:
        """
        Parse resource allocation response.

        Expected format:
        {
            "workers": 10,
            "worker_type": "G.2X",
            "platform": "glue",
            "cost_per_run": 5.50,
            "monthly_cost": 165.0,
            "reasoning": "...",
            "alternatives": [...]
        }
        """
        try:
            data = ResponseParser._extract_json(response)

            return ResourceRecommendation(
                workers=data.get('workers', 5),
                worker_type=data.get('worker_type', 'G.2X'),
                platform=data.get('platform', 'glue'),
                estimated_cost_per_run=data.get('cost_per_run', 0),
                estimated_monthly_cost=data.get('monthly_cost', 0),
                reasoning=data.get('reasoning', ''),
                alternatives=data.get('alternatives', [])
            )

        except Exception as e:
            logger.warning(f"Failed to parse resource recommendation: {e}")
            return ResourceRecommendation(
                workers=5,
                worker_type='G.2X',
                platform='glue',
                estimated_cost_per_run=0,
                estimated_monthly_cost=0,
                reasoning=f'Parse error: {e}'
            )

    @staticmethod
    def parse_healing_strategies(response: str) -> List[HealingStrategy]:
        """
        Parse healing strategy response.

        Expected format:
        {
            "strategies": [
                {
                    "error_type": "oom",
                    "root_cause": "...",
                    "remediation": "...",
                    "config_changes": {...},
                    "retryable": true,
                    "prevention": "..."
                }
            ]
        }
        """
        try:
            data = ResponseParser._extract_json(response)
            strategies = []

            for item in data.get('strategies', [data]):  # Support single or list
                strategies.append(HealingStrategy(
                    error_type=item.get('error_type', 'unknown'),
                    root_cause=item.get('root_cause', ''),
                    remediation=item.get('remediation', ''),
                    config_changes=item.get('config_changes', {}),
                    retryable=item.get('retryable', True),
                    prevention=item.get('prevention', '')
                ))

            return strategies

        except Exception as e:
            logger.warning(f"Failed to parse healing strategies: {e}")
            return []

    @staticmethod
    def parse_recommendations(response: str) -> List[PrioritizedRecommendation]:
        """
        Parse aggregated recommendations.

        Expected format:
        {
            "recommendations": [
                {
                    "priority": "P0",
                    "category": "performance",
                    "recommendation": "...",
                    "savings_usd": 1000,
                    "effort": "2 hours",
                    "payback": "1 week",
                    "source": "code_analysis_agent"
                }
            ],
            "total_savings": 5000,
            "executive_summary": "..."
        }
        """
        try:
            data = ResponseParser._extract_json(response)
            recommendations = []

            for item in data.get('recommendations', []):
                recommendations.append(PrioritizedRecommendation(
                    priority=item.get('priority', 'P2'),
                    category=item.get('category', 'general'),
                    recommendation=item.get('recommendation', ''),
                    estimated_savings_usd=item.get('savings_usd', 0),
                    implementation_effort=item.get('effort', 'unknown'),
                    payback_period=item.get('payback', 'unknown'),
                    source_agent=item.get('source', 'unknown'),
                    details=item.get('details', {})
                ))

            return recommendations

        except Exception as e:
            logger.warning(f"Failed to parse recommendations: {e}")
            return []

    @staticmethod
    def parse_sizing_result(response: str) -> Dict[str, Any]:
        """
        Parse sizing analysis response.

        Expected format:
        {
            "total_size_gb": 45.5,
            "effective_size_gb": 35.0,
            "confidence": "high",
            "table_sizes": [...],
            "factors": {...}
        }
        """
        try:
            return ResponseParser._extract_json(response)
        except Exception as e:
            logger.warning(f"Failed to parse sizing result: {e}")
            return {'error': str(e)}

    @staticmethod
    def parse_anomalies(response: str) -> List[Dict[str, Any]]:
        """
        Parse anomaly detection response.

        Expected format:
        {
            "anomalies": [
                {
                    "type": "size_spike",
                    "severity": "high",
                    "description": "...",
                    "metric": "size_gb",
                    "expected": 50,
                    "actual": 150
                }
            ]
        }
        """
        try:
            data = ResponseParser._extract_json(response)
            return data.get('anomalies', [])
        except Exception as e:
            logger.warning(f"Failed to parse anomalies: {e}")
            return []

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        """
        Extract JSON from text that may contain markdown or other content.
        """
        text = text.strip()

        # Try direct parse first
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try to find JSON in markdown code blocks
        json_patterns = [
            r'```json\s*([\s\S]*?)\s*```',
            r'```\s*([\s\S]*?)\s*```',
            r'\{[\s\S]*\}',
        ]

        for pattern in json_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    json_str = match.group(1) if '```' in pattern else match.group(0)
                    return json.loads(json_str)
                except (json.JSONDecodeError, IndexError):
                    continue

        raise ValueError("No valid JSON found in response")

    @staticmethod
    def extract_natural_language_summary(response: str) -> str:
        """
        Extract human-readable summary from response.
        Removes JSON and code blocks, returns plain text.
        """
        # Remove code blocks
        text = re.sub(r'```[\s\S]*?```', '', response)
        # Remove JSON objects
        text = re.sub(r'\{[\s\S]*?\}', '', text)
        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()
