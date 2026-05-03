"""
Base Agent for Cost Optimization
================================

Provides common functionality for all cost optimization agents.
Supports both rule-based and LLM-powered analysis.
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime

_log = logging.getLogger("strands_optimizer.llm")

# ─────────────────────────────────────────────────────────────────────────────
# LLM verbosity flag
#   False (default) → print one-line call header + first 300 chars of prompt/response
#   True            → print full prompt and full response text
# Set via set_llm_verbose(True) or by passing --show-prompts on the CLI.
# ─────────────────────────────────────────────────────────────────────────────

LLM_VERBOSE: bool = False


def set_llm_verbose(flag: bool) -> None:
    global LLM_VERBOSE
    LLM_VERBOSE = flag


# ─────────────────────────────────────────────────────────────────────────────
# Print helpers — bordered box style consistent with the rest of the CLI output
# ─────────────────────────────────────────────────────────────────────────────

_BOX_W = 68   # total width of the bordered LLM call box


def _box_top(label: str) -> None:
    inner = f" LLM CALL [{label}] "
    fill  = _BOX_W - len(inner) - 2
    print(f"\n  ┌{'─' * (len(inner)//2)}{inner}{'─' * (fill - len(inner)//2)}┐")


def _box_row(key: str, value: str) -> None:
    line = f"  {key}: {value}"
    pad  = max(0, _BOX_W - len(line) - 1)
    print(f"  │{line[2:]:<{_BOX_W-4}}│")


def _box_divider(label: str = "") -> None:
    if label:
        inner = f" {label} "
        fill  = _BOX_W - len(inner) - 4
        print(f"  ├{'─' * 2}{inner}{'─' * fill}┤")
    else:
        print(f"  ├{'─' * (_BOX_W - 4)}┤")


def _box_text(text: str, max_chars: Optional[int] = None) -> None:
    """Print text lines inside the box, optionally truncating."""
    if max_chars and len(text) > max_chars:
        text = text[:max_chars] + f"\n  ... [{len(text) - max_chars:,} more chars hidden — use --show-prompts to see all]"
    for raw_line in text.splitlines():
        # wrap long lines
        while len(raw_line) > _BOX_W - 6:
            print(f"  │ {raw_line[:_BOX_W-6]} │")
            raw_line = raw_line[_BOX_W-6:]
        print(f"  │ {raw_line:<{_BOX_W-4}} │")


def _box_bottom() -> None:
    print(f"  └{'─' * (_BOX_W - 4)}┘")


def _box_error(source: str, exc_type: str, message: str, hints: List[str]) -> None:
    """Print a red-flagged error block inside the LLM call box."""
    _box_divider("ERROR")
    _box_text(f"[FAILED] {source} → {exc_type}")
    _box_text(f"Message : {message}")
    for hint in hints:
        _box_text(f"  → {hint}")


# ─────────────────────────────────────────────────────────────────────────────
# AWS error → actionable hint mapper
# ─────────────────────────────────────────────────────────────────────────────

def _aws_error_hints(error_code: str, region: str = "", model_id: str = "") -> List[str]:
    """Return a list of actionable fix hints for a given botocore error code."""
    hints: List[str] = []
    ec = error_code.lower()

    if "credentials" in ec or ec == "nocredentialserror":
        hints += [
            "No AWS credentials found.",
            "Set env vars: AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY + AWS_SESSION_TOKEN",
            "Or configure a profile:  aws configure  (then use --region and AWS_PROFILE)",
            "Or attach an IAM role if running on EC2/ECS/Lambda.",
        ]
    elif "accessdenied" in ec or "notauthorized" in ec:
        hints += [
            f"IAM principal lacks permission to call bedrock:InvokeModel.",
            f"Add to your IAM policy:",
            f'  {{"Effect":"Allow","Action":"bedrock:InvokeModel",',
            f'   "Resource":"arn:aws:bedrock:{region or "*"}::foundation-model/*"}}',
            "Also ensure Bedrock model access is enabled in the AWS Console → Bedrock → Model access.",
        ]
    elif "resourcenotfound" in ec or "validationexception" in ec:
        hints += [
            f"Model ID not found or not available in region '{region}'.",
            f"Requested model : {model_id}",
            "Check:  aws bedrock list-foundation-models --region " + (region or "us-west-2"),
            "For cross-region inference use the 'us.' prefix, e.g.:",
            "  us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        ]
    elif "throttling" in ec:
        hints += [
            "Request rate limit hit (ThrottlingException).",
            "Bedrock on-demand default: ~1 req/sec for Claude Sonnet.",
            "Options: add retry logic, request a quota increase in AWS Service Quotas,",
            "or switch to Provisioned Throughput.",
        ]
    elif "serviceunavailable" in ec or "internalservererror" in ec:
        hints += [
            "Transient AWS service error — safe to retry.",
            "If persistent: check https://health.aws.amazon.com/ for Bedrock outages.",
        ]
    elif "endpointresolution" in ec or "connect" in ec:
        hints += [
            f"Cannot reach Bedrock endpoint in region '{region}'.",
            "Check: network connectivity, VPC endpoint config, firewall rules.",
            "Verify the region is correct:  --region " + (region or "us-west-2"),
        ]
    else:
        hints.append(f"Unexpected AWS error code: {error_code}")

    return hints


# ─────────────────────────────────────────────────────────────────────────────
# Session-level token usage tracker
# ─────────────────────────────────────────────────────────────────────────────

class _TokenTracker:
    """
    Accumulates Bedrock/LLM token usage across all agent calls in a session.

    Provides per-operation breakdown and session totals expressed as a
    percentage of a configurable daily budget.
    """
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
        operation:     str,
        input_tokens:  int,
        output_tokens: int,
        estimated:     bool = False,
        call_type:     str  = "unknown",
    ) -> None:
        """Record one LLM call and print a one-line summary immediately."""
        self.ops.append({
            "operation":     operation,
            "input_tokens":  input_tokens,
            "output_tokens": output_tokens,
            "estimated":     estimated,
            "call_type":     call_type,
        })
        est_flag = " ~est" if estimated else ""
        pct_in   = min(100.0, self.total_input / self.daily_input_budget * 100)
        print(
            f"  [LLM/{call_type}] {operation:<24}  "
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
        w = 72
        print(f"\n{'─' * w}")
        print("  LLM TOKEN USAGE SUMMARY")
        print(f"{'─' * w}")
        print(f"  {'Operation':<28}  {'Via':<14}  {'Input':>8}  {'Output':>7}  {'Est?':>5}")
        print(f"  {'─'*28}  {'─'*14}  {'─'*8}  {'─'*7}  {'─'*5}")
        for op in self.ops:
            est = "yes" if op["estimated"] else "no"
            print(
                f"  {op['operation']:<28}  {op.get('call_type','?'):<14}  "
                f"{op['input_tokens']:>8,}  {op['output_tokens']:>7,}  {est:>5}"
            )
        print(f"  {'─'*28}  {'─'*14}  {'─'*8}  {'─'*7}")
        print(f"  {'TOTAL':<28}  {'':14}  {self.total_input:>8,}  {self.total_output:>7,}")
        pct_in  = min(100.0, self.total_input  / self.daily_input_budget  * 100)
        pct_out = min(100.0, self.total_output / self.daily_output_budget * 100)
        print(f"\n  Estimated cost   : ${self.estimated_cost_usd:.4f} USD")
        print(
            f"  Daily budget used: {pct_in:.1f}% input  "
            f"({self.total_input:,} / {self.daily_input_budget:,} tokens)  |  "
            f"{pct_out:.1f}% output  "
            f"({self.total_output:,} / {self.daily_output_budget:,} tokens)"
        )
        if any(o["estimated"] for o in self.ops):
            print(
                "  * Est?=yes — token count approximated (~4 chars = 1 token).\n"
                "    Cause: strands metrics.accumulated_usage unavailable or boto3 usage field missing.\n"
                "    Fix  : ensure strands-agents is installed and bedrock:InvokeModel is permitted."
            )


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
        Call the LLM for analysis, logging every step and tracking token usage.

        Call order:
          1. strands Agent  (when strands-agents is installed)
          2. boto3 bedrock-runtime direct call  (when strands is absent)
          3. rule-based fallback  (when both LLM paths fail)

        Use --show-prompts / set_llm_verbose(True) to see full prompt + response text.
        """
        prompt        = self._build_llm_prompt(input_data, context)
        response_text = ""
        input_tokens  = 0
        output_tokens = 0
        estimated     = False
        call_type     = "unknown"

        # ── Print call header (always visible) ───────────────────────────────
        _box_top("?")   # placeholder — redrawn with real call_type below
        print(f"  │ {'Agent':<10}: {self.AGENT_NAME:<{_BOX_W-18}} │")
        print(f"  │ {'Model':<10}: {self.model_id:<{_BOX_W-18}} │")
        print(f"  │ {'Region':<10}: {self.region:<{_BOX_W-18}} │")
        print(f"  │ {'Prompt':<10}: {len(prompt):,} chars (~{len(prompt)//4:,} tokens est)  │")

        # ── Show prompt ───────────────────────────────────────────────────────
        _box_divider("PROMPT")
        _box_text(prompt, max_chars=None if LLM_VERBOSE else 400)

        # ── Attempt 1: strands Agent ──────────────────────────────────────────
        _box_divider("CALLING")
        try:
            print(f"  │ Trying strands-agents SDK ...{' ' * (_BOX_W - 33)}│")
            agent    = self._get_llm_agent()   # raises ImportError if not installed
            response = agent(prompt)
            call_type     = "strands"
            response_text = str(response)
            print(f"  │ strands call SUCCESS{' ' * (_BOX_W - 24)}│")
            _log.info("[LLM/strands] %s — call succeeded", self.AGENT_NAME)

            # Extract token counts from response.metrics.accumulated_usage
            try:
                usage         = response.metrics.accumulated_usage
                input_tokens  = usage.get("inputTokens",  0)
                output_tokens = usage.get("outputTokens", 0)
                if input_tokens == 0:
                    raise ValueError("inputTokens=0 in accumulated_usage")
            except (AttributeError, TypeError, ValueError) as metrics_exc:
                print(f"  │ [WARN] metrics.accumulated_usage unavailable: {metrics_exc}{' ' * max(0, _BOX_W - 52 - len(str(metrics_exc)))}│")
                _log.warning("[LLM/strands] token metrics unavailable: %s — estimating", metrics_exc)
                input_tokens  = max(1, len(prompt)        // 4)
                output_tokens = max(1, len(response_text) // 4)
                estimated     = True

        except ImportError:
            print(f"  │ strands-agents not installed — trying boto3 bedrock-runtime{' ' * (_BOX_W - 64)}│")
            _log.info("[LLM] strands not installed, falling back to boto3 bedrock-runtime")

            # ── Attempt 2: boto3 bedrock-runtime ─────────────────────────────
            try:
                import boto3 as _boto3
                import json  as _json

                # Specific exception classes for fine-grained error messages
                try:
                    from botocore.exceptions import (
                        NoCredentialsError       as _NoCreds,
                        ClientError              as _ClientError,
                        EndpointResolutionError  as _EndpointErr,
                    )
                except ImportError:
                    _NoCreds, _ClientError, _EndpointErr = Exception, Exception, Exception

                print(f"  │ Trying boto3 bedrock-runtime (region={self.region}){' ' * max(0, _BOX_W - 52 - len(self.region))}│")

                try:
                    from ..prompts.super_prompts import get_prompt as _get_prompt
                    system_prompt = _get_prompt(self.AGENT_NAME)
                except Exception:
                    system_prompt = (
                        "You are an expert AWS Glue / PySpark cost-optimization assistant. "
                        "Respond with a valid JSON object only."
                    )

                client = _boto3.client("bedrock-runtime", region_name=self.region)
                body   = _json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens":        8096,
                    "system":            system_prompt,
                    "messages":          [{"role": "user", "content": prompt}],
                })

                resp      = client.invoke_model(modelId=self.model_id, body=body)
                resp_body = _json.loads(resp["body"].read())
                call_type = "bedrock_direct"

                usage         = resp_body.get("usage", {})
                input_tokens  = usage.get("input_tokens",  max(1, len(prompt) // 4))
                output_tokens = usage.get("output_tokens", 0)
                estimated     = "input_tokens" not in usage

                content       = resp_body.get("content", [])
                response_text = content[0].get("text", "") if content else ""
                print(f"  │ boto3 bedrock-runtime call SUCCESS{' ' * (_BOX_W - 38)}│")
                _log.info("[LLM/bedrock_direct] %s — call succeeded", self.AGENT_NAME)

            except _NoCreds as exc:
                hints = _aws_error_hints("NoCredentialsError", self.region, self.model_id)
                _box_error("boto3/bedrock", "NoCredentialsError", str(exc), hints)
                _log.error("[LLM/bedrock_direct] NoCredentialsError: %s", exc)
                _box_divider("FALLBACK")
                print(f"  │ Falling back to rule-based analysis (no LLM){' ' * (_BOX_W - 49)}│")
                _box_bottom()
                return self._analyze_rule_based(input_data, context)

            except _ClientError as exc:
                code  = exc.response["Error"]["Code"]
                msg   = exc.response["Error"]["Message"]
                hints = _aws_error_hints(code, self.region, self.model_id)
                _box_error("boto3/bedrock", code, msg, hints)
                _log.error("[LLM/bedrock_direct] ClientError %s: %s", code, msg)
                _box_divider("FALLBACK")
                print(f"  │ Falling back to rule-based analysis (no LLM){' ' * (_BOX_W - 49)}│")
                _box_bottom()
                return self._analyze_rule_based(input_data, context)

            except _EndpointErr as exc:
                hints = _aws_error_hints("EndpointResolutionError", self.region, self.model_id)
                _box_error("boto3/bedrock", "EndpointResolutionError", str(exc), hints)
                _log.error("[LLM/bedrock_direct] EndpointResolutionError: %s", exc)
                _box_divider("FALLBACK")
                print(f"  │ Falling back to rule-based analysis (no LLM){' ' * (_BOX_W - 49)}│")
                _box_bottom()
                return self._analyze_rule_based(input_data, context)

            except Exception as exc:
                _box_error("boto3/bedrock", type(exc).__name__, str(exc),
                           [f"Unexpected error — check logs for details."])
                _log.error("[LLM/bedrock_direct] %s: %s", type(exc).__name__, exc, exc_info=True)
                _box_divider("FALLBACK")
                print(f"  │ Falling back to rule-based analysis (no LLM){' ' * (_BOX_W - 49)}│")
                _box_bottom()
                return self._analyze_rule_based(input_data, context)

        # ── Show response ─────────────────────────────────────────────────────
        _box_divider("RESPONSE")
        _box_text(response_text, max_chars=None if LLM_VERBOSE else 400)
        _box_bottom()

        # ── Record token usage ────────────────────────────────────────────────
        TOKEN_TRACKER.record(
            self.AGENT_NAME, input_tokens, output_tokens,
            estimated=estimated, call_type=call_type,
        )

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
