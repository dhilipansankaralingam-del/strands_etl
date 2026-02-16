"""
Base Agent for Cost Optimization
================================

Provides common functionality for all cost optimization agents.
Supports both rule-based and LLM-powered analysis.
"""

import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime


@dataclass
class AnalysisInput:
    """Input for cost analysis."""
    script_path: str
    script_content: str
    source_tables: List[Dict[str, Any]]
    processing_mode: str  # 'delta' or 'full'
    current_config: Dict[str, Any]
    job_name: str = ""
    additional_context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            'script_path': self.script_path,
            'source_tables': self.source_tables,
            'processing_mode': self.processing_mode,
            'current_config': self.current_config,
            'job_name': self.job_name or self.script_path.split('/')[-1].replace('.py', '')
        }


@dataclass
class AnalysisResult:
    """Result from an agent's analysis."""
    agent_name: str
    success: bool
    analysis: Dict[str, Any]
    recommendations: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    execution_time_ms: float = 0

    def to_dict(self) -> Dict:
        return {
            'agent_name': self.agent_name,
            'success': self.success,
            'analysis': self.analysis,
            'recommendations': self.recommendations,
            'metrics': self.metrics,
            'errors': self.errors,
            'execution_time_ms': self.execution_time_ms
        }


class CostOptimizerAgent(ABC):
    """Base class for cost optimization agents."""

    AGENT_NAME = "base_agent"

    def __init__(self, use_llm: bool = False, model_id: str = None):
        """
        Initialize agent.

        Args:
            use_llm: If True, use LLM for analysis. If False, use rule-based.
            model_id: Bedrock model ID (e.g., 'us.anthropic.claude-sonnet-4-20250514-v1:0')
        """
        self.use_llm = use_llm
        self.model_id = model_id or "us.anthropic.claude-sonnet-4-20250514-v1:0"
        self._agent = None

    def _get_llm_agent(self):
        """Lazy load LLM agent."""
        if self._agent is None and self.use_llm:
            try:
                from strands import Agent
                from ..prompts.super_prompts import get_prompt

                self._agent = Agent(
                    model=self.model_id,
                    system_prompt=get_prompt(self.AGENT_NAME)
                )
            except ImportError:
                raise ImportError(
                    "strands-agents not installed. Install with: pip install strands-agents"
                )
        return self._agent

    def analyze(self, input_data: AnalysisInput, context: Dict[str, Any] = None) -> AnalysisResult:
        """
        Run analysis on input data.

        Args:
            input_data: Analysis input with script, tables, config
            context: Additional context from other agents

        Returns:
            AnalysisResult with findings and recommendations
        """
        start_time = datetime.now()

        try:
            if self.use_llm:
                result = self._analyze_with_llm(input_data, context or {})
            else:
                result = self._analyze_rule_based(input_data, context or {})

            result.execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000
            return result

        except Exception as e:
            return AnalysisResult(
                agent_name=self.AGENT_NAME,
                success=False,
                analysis={},
                errors=[str(e)],
                execution_time_ms=(datetime.now() - start_time).total_seconds() * 1000
            )

    @abstractmethod
    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """Implement rule-based analysis logic."""
        pass

    def _analyze_with_llm(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """Use LLM agent for analysis."""
        agent = self._get_llm_agent()

        # Build prompt with input data
        prompt = self._build_llm_prompt(input_data, context)

        # Call LLM
        response = agent(prompt)

        # Parse response
        return self._parse_llm_response(response)

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        """Build prompt for LLM analysis."""
        return f"""
Analyze the following PySpark job for cost optimization:

## Job Information
- Script: {input_data.script_path}
- Processing Mode: {input_data.processing_mode}
- Job Name: {input_data.job_name}

## Source Tables
```json
{json.dumps(input_data.source_tables, indent=2)}
```

## Current Configuration
```json
{json.dumps(input_data.current_config, indent=2)}
```

## PySpark Code
```python
{input_data.script_content}
```

## Context from Other Agents
```json
{json.dumps(context, indent=2)}
```

Provide your analysis as a JSON object.
"""

    def _parse_llm_response(self, response) -> AnalysisResult:
        """Parse LLM response into AnalysisResult."""
        # Extract JSON from response
        response_text = str(response)

        # Try to find JSON in response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            try:
                analysis = json.loads(json_match.group())
                return AnalysisResult(
                    agent_name=self.AGENT_NAME,
                    success=True,
                    analysis=analysis,
                    recommendations=analysis.get('recommendations', []),
                    metrics=analysis.get('metrics', {})
                )
            except json.JSONDecodeError:
                pass

        # Fallback: return raw response
        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis={'raw_response': response_text},
            recommendations=[]
        )


class CodePatternMatcher:
    """Utility class for matching code patterns."""

    # Anti-pattern regex patterns
    PATTERNS = {
        'collect': r'\.collect\(\)',
        'toPandas': r'\.toPandas\(\)',
        'cartesian_join': r'\.crossJoin\(',
        'udf_usage': r'@udf|udf\(',
        'select_star': r'\.select\(\s*["\']?\*["\']?\s*\)',
        'no_cache': r'\.count\(\).*\.count\(\)',  # Multiple actions without cache
        'repartition_1': r'\.repartition\(1\)',
        'coalesce_1': r'\.coalesce\(1\)',
        'shuffle_in_loop': r'for\s+.*:[\s\S]*?\.join\(',
        'hardcoded_paths': r's3://[a-zA-Z0-9\-_./]+',
    }

    # Optimization opportunities
    OPTIMIZATIONS = {
        'broadcast_hint': r'broadcast\(',
        'cache_persist': r'\.(cache|persist)\(',
        'repartition': r'\.repartition\(',
        'coalesce': r'\.coalesce\(',
        'partition_by': r'\.partitionBy\(',
        'bucket_by': r'\.bucketBy\(',
    }

    @classmethod
    def find_pattern(cls, code: str, pattern_name: str) -> List[Dict]:
        """Find occurrences of a pattern in code."""
        pattern = cls.PATTERNS.get(pattern_name) or cls.OPTIMIZATIONS.get(pattern_name)
        if not pattern:
            return []

        matches = []
        for i, line in enumerate(code.split('\n'), 1):
            if re.search(pattern, line):
                matches.append({
                    'line': i,
                    'content': line.strip(),
                    'pattern': pattern_name
                })
        return matches

    @classmethod
    def count_joins(cls, code: str) -> int:
        """Count number of join operations."""
        return len(re.findall(r'\.join\(', code, re.IGNORECASE))

    @classmethod
    def count_window_functions(cls, code: str) -> int:
        """Count window function usage."""
        return len(re.findall(r'Window\.|over\(', code, re.IGNORECASE))

    @classmethod
    def count_aggregations(cls, code: str) -> int:
        """Count aggregation operations."""
        patterns = [r'\.groupBy\(', r'\.agg\(', r'\.sum\(', r'\.count\(', r'\.avg\(', r'\.max\(', r'\.min\(']
        total = 0
        for pattern in patterns:
            total += len(re.findall(pattern, code, re.IGNORECASE))
        return total

    @classmethod
    def detect_skew_risk(cls, code: str) -> List[str]:
        """Detect potential data skew risks."""
        risks = []

        # Join on low-cardinality columns
        if re.search(r'\.join\([^)]*["\']?(status|type|category|flag)["\']?', code, re.IGNORECASE):
            risks.append("Join on low-cardinality column (status/type/category)")

        # Group by date without partition
        if re.search(r'\.groupBy\([^)]*date', code, re.IGNORECASE):
            risks.append("GroupBy on date column - check for date skew")

        # No salting for skewed keys
        if cls.count_joins(code) > 3 and 'salt' not in code.lower():
            risks.append("Multiple joins without salting - consider key salting for skew")

        return risks
