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


# ─────────────────────────────────────────────────────────────────────────────
# Session-level token usage tracker
# ─────────────────────────────────────────────────────────────────────────────

class _TokenTracker:
    """
    Accumulates Bedrock/LLM token usage across all agent calls in a session.

    Provides per-operation breakdown and session totals expressed as a
    percentage of a configurable daily budget.
    """
    # Conservative daily budgets (on-demand Bedrock, developer account).
    # Override via TOKEN_TRACKER.daily_input_budget / daily_output_budget.
    daily_input_budget:  int = 1_000_000   # 1 M input tokens / day
    daily_output_budget: int =   200_000   # 200 K output tokens / day

    # Claude Sonnet 3.7 on Bedrock on-demand pricing
    _INPUT_COST_PER_MTOK  = 3.00   # $3.00 / 1M input tokens
    _OUTPUT_COST_PER_MTOK = 15.00  # $15.00 / 1M output tokens

    def __init__(self) -> None:
        self.ops: List[Dict[str, Any]] = []

    # ── recording ─────────────────────────────────────────────────────────────

    def record(
        self,
        operation:    str,
        input_tokens: int,
        output_tokens: int,
        estimated:    bool = False,
    ) -> None:
        """Record one LLM call and print a one-line summary immediately."""
        self.ops.append({
            "operation":     operation,
            "input_tokens":  input_tokens,
            "output_tokens": output_tokens,
            "estimated":     estimated,
        })
        est_flag = " ~est" if estimated else ""
        pct_in   = min(100.0, self.total_input / self.daily_input_budget * 100)
        print(
            f"  [LLM] {operation:<28}  "
            f"in={input_tokens:>6,}  out={output_tokens:>5,}{est_flag}  "
            f"session={self.total_input:>7,} in  ({pct_in:.1f}% of daily budget)"
        )

    # ── aggregates ────────────────────────────────────────────────────────────

    @property
    def total_input(self) -> int:
        return sum(o["input_tokens"] for o in self.ops)

    @property
    def total_output(self) -> int:
        return sum(o["output_tokens"] for o in self.ops)

    @property
    def estimated_cost_usd(self) -> float:
        return (
            self.total_input  / 1_000_000 * self._INPUT_COST_PER_MTOK
            + self.total_output / 1_000_000 * self._OUTPUT_COST_PER_MTOK
        )

    # ── summary ───────────────────────────────────────────────────────────────

    def print_summary(self) -> None:
        """Print a full token usage table at the end of a session."""
        if not self.ops:
            return
        w = 65
        print(f"\n{'─' * w}")
        print("  LLM TOKEN USAGE SUMMARY")
        print(f"{'─' * w}")
        hdr = f"  {'Operation':<28}  {'Input':>8}  {'Output':>7}  {'Est?':>5}"
        print(hdr)
        print(f"  {'─' * 28}  {'─' * 8}  {'─' * 7}  {'─' * 5}")
        for op in self.ops:
            est = "yes" if op["estimated"] else "no"
            print(
                f"  {op['operation']:<28}  "
                f"{op['input_tokens']:>8,}  {op['output_tokens']:>7,}  {est:>5}"
            )
        print(f"  {'─' * 28}  {'─' * 8}  {'─' * 7}")
        print(f"  {'TOTAL':<28}  {self.total_input:>8,}  {self.total_output:>7,}")
        pct_in  = min(100.0, self.total_input  / self.daily_input_budget  * 100)
        pct_out = min(100.0, self.total_output / self.daily_output_budget * 100)
        print(f"\n  Estimated cost   : ${self.estimated_cost_usd:.4f} USD")
        print(f"  Daily budget used: {pct_in:.1f}% input ({self.total_input:,} / "
              f"{self.daily_input_budget:,})  |  "
              f"{pct_out:.1f}% output ({self.total_output:,} / {self.daily_output_budget:,})")
        if any(o["estimated"] for o in self.ops):
            print("  * 'Est?' = yes means token count estimated from text length "
                  "(strands not installed; exact counts come from Bedrock direct calls)")


TOKEN_TRACKER: _TokenTracker = _TokenTracker()


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

    AGENT_NAME     = "base_agent"
    DEFAULT_MODEL  = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
    DEFAULT_REGION = "us-west-2"

    def __init__(self, use_llm: bool = False, model_id: str = None, region: str = None):
        """
        Initialize agent.

        Args:
            use_llm:  If True, use LLM for analysis. If False, use rule-based.
            model_id: Bedrock model ID (default: Claude Sonnet 3.7, us-west-2).
            region:   AWS region for Bedrock calls (default: us-west-2).
        """
        self.use_llm  = use_llm
        self.model_id = model_id or self.DEFAULT_MODEL
        self.region   = region   or self.DEFAULT_REGION
        self._agent   = None

    def _get_llm_agent(self):
        """Lazy load LLM agent."""
        if self._agent is None and self.use_llm:
            try:
                from strands import Agent
                from strands.models import BedrockModel
                from ..prompts.super_prompts import get_prompt

                bedrock_model = BedrockModel(
                    model_id       = self.model_id,
                    region_name    = self.region,
                )
                self._agent = Agent(
                    model         = bedrock_model,
                    system_prompt = get_prompt(self.AGENT_NAME)
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
        """
        Call the LLM for analysis, tracking exact token usage.

        Strategy:
        1. Try the strands Agent (when strands-agents is installed).
        2. Fall back to a direct boto3 bedrock-runtime call, which always
           returns exact token counts in the response body.
        3. If both are unavailable, fall back to rule-based analysis.
        """
        prompt = self._build_llm_prompt(input_data, context)
        response_text  = ""
        input_tokens   = 0
        output_tokens  = 0
        estimated      = False  # True when counts are character-length estimates

        # ── Attempt 1: strands Agent ──────────────────────────────────────────
        try:
            agent    = self._get_llm_agent()   # raises ImportError if not installed
            response = agent(prompt)
            response_text = str(response)

            # Try to extract token counts from the strands AgentResult object
            input_tokens  = (
                getattr(response, "input_tokens",  None)
                or getattr(response, "inputTokens", None)
                or 0
            )
            output_tokens = (
                getattr(response, "output_tokens",  None)
                or getattr(response, "outputTokens", None)
                or 0
            )
            # If strands didn't expose counts, estimate from text length
            if not input_tokens:
                input_tokens  = max(1, len(prompt)        // 4)
                output_tokens = max(1, len(response_text) // 4)
                estimated = True

        except ImportError:
            # ── Attempt 2: direct boto3 bedrock-runtime call ──────────────────
            try:
                import boto3 as _boto3
                import json   as _json

                client = _boto3.client("bedrock-runtime", region_name=self.region)

                try:
                    from ..prompts.super_prompts import get_prompt as _get_prompt
                    system_prompt = _get_prompt(self.AGENT_NAME)
                except Exception:
                    system_prompt = (
                        "You are an expert AWS Glue / PySpark cost-optimization assistant. "
                        "Respond with a JSON object."
                    )

                body = _json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens":        8096,
                    "system":            system_prompt,
                    "messages":          [{"role": "user", "content": prompt}],
                })

                resp      = client.invoke_model(modelId=self.model_id, body=body)
                resp_body = _json.loads(resp["body"].read())

                usage         = resp_body.get("usage", {})
                input_tokens  = usage.get("input_tokens",  max(1, len(prompt) // 4))
                output_tokens = usage.get("output_tokens", 0)
                estimated     = "input_tokens" not in usage  # True only if missing

                content       = resp_body.get("content", [])
                response_text = content[0].get("text", "") if content else ""

            except Exception as exc:
                # Both strands and boto3 failed — fall back to rule-based
                import logging as _logging
                _logging.getLogger("strands_optimizer").warning(
                    "LLM unavailable (%s); falling back to rule-based analysis.", exc
                )
                return self._analyze_rule_based(input_data, context)

        # ── Record token usage in the session tracker ─────────────────────────
        TOKEN_TRACKER.record(self.AGENT_NAME, input_tokens, output_tokens, estimated=estimated)

        return self._parse_llm_response(response_text)

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
