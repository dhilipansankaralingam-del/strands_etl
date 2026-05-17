"""
validation/prompt_logger.py
============================
Console logger for every Bedrock prompt and response.

Prints a clear banner before each LLM call so you can see exactly what
the AI is being asked and what it replied — critical for debugging and
building confidence in AI-driven decisions.

Also tracks cumulative token usage and computes estimated cost.

Claude 3 Sonnet pricing (us-east-1, as of 2024):
  Input  : $3.00 / 1M tokens
  Output : $15.00 / 1M tokens
"""

from __future__ import annotations

import json
import logging
import textwrap
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("strands.prompt")

# Bedrock Claude 3 Sonnet pricing
_INPUT_COST_PER_TOKEN  = 3.00  / 1_000_000
_OUTPUT_COST_PER_TOKEN = 15.00 / 1_000_000

_C = {
    "RESET":   "\033[0m",
    "BOLD":    "\033[1m",
    "CYAN":    "\033[96m",
    "YELLOW":  "\033[93m",
    "GREEN":   "\033[92m",
    "MAGENTA": "\033[95m",
    "RED":     "\033[91m",
    "GREY":    "\033[37m",
    "BLUE":    "\033[94m",
}


def _c(text: str, colour: str) -> str:
    return f"{_C.get(colour, '')}{text}{_C['RESET']}"


@dataclass
class TokenUsage:
    agent_type: str
    table_name: str
    input_tokens: int
    output_tokens: int
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def cost_usd(self) -> float:
        return (
            self.input_tokens  * _INPUT_COST_PER_TOKEN +
            self.output_tokens * _OUTPUT_COST_PER_TOKEN
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_type":    self.agent_type,
            "table_name":    self.table_name,
            "input_tokens":  self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens":  self.total_tokens,
            "cost_usd":      round(self.cost_usd, 6),
            "timestamp":     self.timestamp,
        }


class PromptLogger:
    """
    Singleton-style logger that wraps every Bedrock invocation.
    Call log_prompt() before invoking and log_response() after.
    Accumulates TokenUsage for a final cost summary.
    """

    def __init__(self, preview_chars: int = 800, log_responses: bool = True):
        self.preview_chars  = preview_chars
        self.log_responses  = log_responses
        self.usages: List[TokenUsage] = []
        self._call_index = 0

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def log_prompt(
        self,
        agent_type: str,
        system_prompt: str,
        user_prompt: str,
        table_name: str = "",
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._call_index += 1
        ts = datetime.utcnow().strftime("%H:%M:%S")
        width = 72

        print()
        print(_c("╔" + "═" * (width - 2) + "╗", "CYAN"))
        print(_c(f"║  🤖  BEDROCK PROMPT  [{agent_type.upper()}]  {ts}  call #{self._call_index:<3}{'':>{width - 52}}║", "CYAN"))
        print(_c("╚" + "═" * (width - 2) + "╝", "CYAN"))

        if table_name:
            print(_c(f"  Table : {table_name}", "GREY"))

        if extra_context:
            for k, v in extra_context.items():
                print(_c(f"  {k:<14}: {str(v)[:80]}", "GREY"))

        print(_c("\n  ── SYSTEM ──────────────────────────────────────────────────", "YELLOW"))
        for line in textwrap.wrap(system_prompt[:self.preview_chars], width=70):
            print(f"  {_c(line, 'YELLOW')}")
        if len(system_prompt) > self.preview_chars:
            print(_c(f"  ... [{len(system_prompt) - self.preview_chars} more chars]", "GREY"))

        print(_c("\n  ── USER PROMPT ─────────────────────────────────────────────", "BLUE"))
        for line in textwrap.wrap(user_prompt[:self.preview_chars], width=70):
            print(f"  {line}")
        if len(user_prompt) > self.preview_chars:
            print(_c(f"  ... [{len(user_prompt) - self.preview_chars} more chars]", "GREY"))

        print()

    def log_response(
        self,
        agent_type: str,
        table_name: str,
        raw_response: str,
        input_tokens: int,
        output_tokens: int,
    ) -> TokenUsage:
        usage = TokenUsage(
            agent_type=agent_type,
            table_name=table_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        self.usages.append(usage)

        width = 72
        if self.log_responses:
            print(_c("  ── AI RESPONSE ─────────────────────────────────────────────", "GREEN"))
            for line in textwrap.wrap(raw_response[:self.preview_chars], width=70):
                print(f"  {_c(line, 'GREEN')}")
            if len(raw_response) > self.preview_chars:
                print(_c(f"  ... [{len(raw_response) - self.preview_chars} more chars]", "GREY"))

        print(_c(
            f"\n  ── TOKENS  in={input_tokens:,}  out={output_tokens:,}  "
            f"total={usage.total_tokens:,}  cost=${usage.cost_usd:.5f}",
            "MAGENTA"
        ))
        print(_c("─" * width, "CYAN"))
        return usage

    def print_cost_summary(self) -> None:
        """Print cumulative token and cost summary at end of run."""
        if not self.usages:
            return

        total_in   = sum(u.input_tokens  for u in self.usages)
        total_out  = sum(u.output_tokens for u in self.usages)
        total_cost = sum(u.cost_usd      for u in self.usages)
        width = 72

        print()
        print(_c("╔" + "═" * (width - 2) + "╗", "MAGENTA"))
        print(_c(f"║{'  💰  TOKEN USAGE & COST SUMMARY':^{width-2}}║", "MAGENTA"))
        print(_c("╠" + "═" * (width - 2) + "╣", "MAGENTA"))
        print(_c(f"║  {'Agent Type':<22} {'Table':<24} {'In':>6} {'Out':>6} {'Cost':>10}  ║", "MAGENTA"))
        print(_c("║" + "─" * (width - 2) + "║", "MAGENTA"))

        by_type: Dict[str, TokenUsage] = {}
        for u in self.usages:
            key = f"{u.agent_type}|{u.table_name}"
            if key not in by_type:
                by_type[key] = TokenUsage(u.agent_type, u.table_name, 0, 0)
            by_type[key].input_tokens  += u.input_tokens
            by_type[key].output_tokens += u.output_tokens

        for u in by_type.values():
            table_display = (u.table_name[:22] + "…") if len(u.table_name) > 23 else u.table_name
            print(_c(
                f"║  {u.agent_type:<22} {table_display:<24} "
                f"{u.input_tokens:>6,} {u.output_tokens:>6,} "
                f"${u.cost_usd:>9.5f}  ║",
                "MAGENTA"
            ))

        print(_c("╠" + "═" * (width - 2) + "╣", "MAGENTA"))
        print(_c(
            f"║  {'TOTAL':<22} {' ':<24} "
            f"{total_in:>6,} {total_out:>6,} "
            f"${total_cost:>9.5f}  ║",
            "MAGENTA"
        ))
        print(_c("╚" + "═" * (width - 2) + "╝", "MAGENTA"))
        print()

    def to_dicts(self) -> List[Dict[str, Any]]:
        return [u.to_dict() for u in self.usages]


# Module-level singleton — shared across all agents in one run
_default_logger = PromptLogger()


def get_logger() -> PromptLogger:
    return _default_logger


def reset_logger(preview_chars: int = 800, log_responses: bool = True) -> PromptLogger:
    """Call this at the start of each run to get a fresh logger."""
    global _default_logger
    _default_logger = PromptLogger(preview_chars=preview_chars, log_responses=log_responses)
    return _default_logger
