"""
validation/cost_tracker.py
==========================
Token-usage and cost tracking for every Amazon Bedrock invocation made by
the Strands framework.

Pricing is based on Bedrock on-demand rates (us-east-1, April 2026).
Update PRICING_TABLE when AWS changes rates or when you add new models.

Usage
-----
    tracker = BedrockCostTracker()
    tracker.record_call(
        model_id      = "anthropic.claude-3-sonnet-20240229-v1:0",
        input_tokens  = 1_200,
        output_tokens = 340,
        agent_type    = "decision",
        step_label    = "Step 3 – AI enrichment",
    )
    summary = tracker.get_summary()
    print(summary["total_cost_usd"])   # e.g. 0.00889
"""

import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Any


# ---------------------------------------------------------------------------
# Bedrock on-demand pricing  (USD per 1 000 tokens)
# ---------------------------------------------------------------------------
PRICING_TABLE: Dict[str, Dict[str, float]] = {
    # Claude 3 family
    "anthropic.claude-3-sonnet-20240229-v1:0": {"input": 0.003,   "output": 0.015},
    "anthropic.claude-3-haiku-20240307-v1:0":  {"input": 0.00025, "output": 0.00125},
    "anthropic.claude-3-opus-20240229-v1:0":   {"input": 0.015,   "output": 0.075},
    # Claude 3.5 family
    "anthropic.claude-3-5-sonnet-20240620-v1:0": {"input": 0.003, "output": 0.015},
    "anthropic.claude-3-5-sonnet-20241022-v2:0": {"input": 0.003, "output": 0.015},
    "anthropic.claude-3-5-haiku-20241022-v1:0":  {"input": 0.0008,"output": 0.004},
    # Claude 4 family (preview pricing)
    "anthropic.claude-opus-4-5-20250514-v1:0":   {"input": 0.015,  "output": 0.075},
    "anthropic.claude-sonnet-4-5-20250514-v1:0": {"input": 0.003,  "output": 0.015},
}

_DEFAULT_PRICING = {"input": 0.003, "output": 0.015}  # fallback = Sonnet pricing


# ---------------------------------------------------------------------------
# Data model for a single call record
# ---------------------------------------------------------------------------

@dataclass
class CallRecord:
    timestamp:     str
    model_id:      str
    agent_type:    str          # decision | learning | batch | log_analyzer | …
    step_label:    str          # human label, e.g. "Step 3 – AI enrichment"
    input_tokens:  int
    output_tokens: int
    input_cost_usd:  float
    output_cost_usd: float
    total_cost_usd:  float
    latency_ms:    float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Tracker class
# ---------------------------------------------------------------------------

class BedrockCostTracker:
    """
    Thread-safe (single-process) accumulator for Bedrock token usage and costs.

    One instance should be shared across all agents in a single analyzer run
    so the final report shows the total bill for that run.
    """

    def __init__(self, run_id: str = ""):
        self.run_id    = run_id
        self._calls: List[CallRecord] = []
        self._run_start = datetime.utcnow()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_call(
        self,
        model_id:      str,
        input_tokens:  int,
        output_tokens: int,
        agent_type:    str  = "unknown",
        step_label:    str  = "",
        latency_ms:    float = 0.0,
    ) -> CallRecord:
        """
        Record one Bedrock invoke_model call.

        Parameters
        ----------
        model_id       : full Bedrock model ID string
        input_tokens   : from response["usage"]["inputTokens"]
        output_tokens  : from response["usage"]["outputTokens"]
        agent_type     : logical agent name (decision / learning / batch / log_analyzer)
        step_label     : human-readable step description shown in reports
        latency_ms     : wall-clock time for the API call (optional)
        """
        pricing      = PRICING_TABLE.get(model_id, _DEFAULT_PRICING)
        input_cost   = (input_tokens  / 1_000) * pricing["input"]
        output_cost  = (output_tokens / 1_000) * pricing["output"]
        total_cost   = input_cost + output_cost

        record = CallRecord(
            timestamp      = datetime.utcnow().isoformat(),
            model_id       = model_id,
            agent_type     = agent_type,
            step_label     = step_label,
            input_tokens   = input_tokens,
            output_tokens  = output_tokens,
            input_cost_usd  = round(input_cost,  6),
            output_cost_usd = round(output_cost, 6),
            total_cost_usd  = round(total_cost,  6),
            latency_ms     = round(latency_ms, 1),
        )
        self._calls.append(record)
        return record

    def get_summary(self) -> Dict[str, Any]:
        """
        Return an aggregated cost/token summary for the whole run.

        Structure::

            {
              "run_id": "...",
              "total_calls": 42,
              "total_input_tokens":  120_000,
              "total_output_tokens":  18_000,
              "total_cost_usd":       0.3447,
              "by_agent_type": {
                "decision":  {"calls":30, "input_tokens":…, "cost_usd":…},
                "batch":     {"calls": 1, …},
                "learning":  {"calls": 1, …},
              },
              "by_step": [
                {"step_label": "Step 3 – AI enrichment", "calls":30, "cost_usd":…},
                …
              ],
              "calls": [ <CallRecord dicts> ],
              "run_start":  "2026-04-18T…",
              "run_elapsed_s": 42.3,
            }
        """
        if not self._calls:
            return self._empty_summary()

        total_in  = sum(c.input_tokens  for c in self._calls)
        total_out = sum(c.output_tokens for c in self._calls)
        total_cost = sum(c.total_cost_usd for c in self._calls)

        by_agent: Dict[str, Dict[str, Any]] = {}
        for c in self._calls:
            ag = by_agent.setdefault(c.agent_type, {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0})
            ag["calls"]         += 1
            ag["input_tokens"]  += c.input_tokens
            ag["output_tokens"] += c.output_tokens
            ag["cost_usd"]      = round(ag["cost_usd"] + c.total_cost_usd, 6)

        by_step: Dict[str, Dict[str, Any]] = {}
        for c in self._calls:
            lbl = c.step_label or c.agent_type
            st  = by_step.setdefault(lbl, {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0})
            st["calls"]         += 1
            st["input_tokens"]  += c.input_tokens
            st["output_tokens"] += c.output_tokens
            st["cost_usd"]      = round(st["cost_usd"] + c.total_cost_usd, 6)

        elapsed = (datetime.utcnow() - self._run_start).total_seconds()

        return {
            "run_id":               self.run_id,
            "total_calls":          len(self._calls),
            "total_input_tokens":   total_in,
            "total_output_tokens":  total_out,
            "total_tokens":         total_in + total_out,
            "total_cost_usd":       round(total_cost, 6),
            "by_agent_type":        by_agent,
            "by_step":              list(by_step.values()),
            "by_step_labeled":      by_step,
            "calls":                [c.to_dict() for c in self._calls],
            "run_start":            self._run_start.isoformat(),
            "run_elapsed_s":        round(elapsed, 2),
        }

    def reset(self) -> None:
        """Clear all recorded calls (useful between pipeline runs)."""
        self._calls.clear()
        self._run_start = datetime.utcnow()

    def cost_so_far(self) -> float:
        """Quick accessor — total USD spent so far in this tracker."""
        return round(sum(c.total_cost_usd for c in self._calls), 6)

    def tokens_so_far(self) -> Dict[str, int]:
        return {
            "input":  sum(c.input_tokens  for c in self._calls),
            "output": sum(c.output_tokens for c in self._calls),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _empty_summary(self) -> Dict[str, Any]:
        return {
            "run_id":              self.run_id,
            "total_calls":         0,
            "total_input_tokens":  0,
            "total_output_tokens": 0,
            "total_tokens":        0,
            "total_cost_usd":      0.0,
            "by_agent_type":       {},
            "by_step":             [],
            "by_step_labeled":     {},
            "calls":               [],
            "run_start":           self._run_start.isoformat(),
            "run_elapsed_s":       0.0,
        }

    # ------------------------------------------------------------------
    # Convenience: render a compact cost table for CLI / log output
    # ------------------------------------------------------------------

    def render_table(self) -> str:
        s = self.get_summary()
        lines = [
            "┌─────────────────────────────────────────────────────────────┐",
            f"│  Bedrock Cost Summary  run_id={self.run_id[:20]}",
            "├──────────────────────────┬──────────┬──────────┬────────────┤",
            "│ Step / Agent             │ In Tok   │ Out Tok  │ Cost USD   │",
            "├──────────────────────────┼──────────┼──────────┼────────────┤",
        ]
        for label, data in s["by_step_labeled"].items():
            lines.append(
                f"│ {label[:26]:<26}│ {data['input_tokens']:>8,} │ {data['output_tokens']:>8,} │ ${data['cost_usd']:>9.6f} │"
            )
        lines += [
            "├──────────────────────────┼──────────┼──────────┼────────────┤",
            f"│ {'TOTAL':<26}│ {s['total_input_tokens']:>8,} │ {s['total_output_tokens']:>8,} │ ${s['total_cost_usd']:>9.6f} │",
            "└──────────────────────────┴──────────┴──────────┴────────────┘",
        ]
        return "\n".join(lines)
