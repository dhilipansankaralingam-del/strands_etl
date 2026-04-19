"""
tests/test_orchestrator.py
============================
End-to-end orchestrator tests.

These tests verify that:
  1. The orchestrator is correctly wired with all agent-as-tool functions.
  2. Prompt routing maps Slack commands to the right specialist agent.
  3. The Strands processor (slack_processor_strands) builds correct prompts.
  4. Errors from specialist agents are caught and posted as error Block Kit cards.
"""

import json
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Orchestrator tool wiring
# ---------------------------------------------------------------------------

class TestOrchestratorWiring:

    def test_orchestrator_has_all_agent_tools(self):
        import inspect
        import strands_agents.orchestrator_agent as oa
        source = inspect.getsource(oa.create_orchestrator)
        assert "analyze_validation_failures" in source
        assert "analyze_logs"                in source
        assert "query_data"                  in source
        assert "get_batch_summary"           in source
        assert "get_holiday_context"         in source

    def test_orchestrator_system_prompt_covers_routing(self):
        from strands_agents.orchestrator_agent import ORCHESTRATOR_SYSTEM_PROMPT
        assert "analyze_validation_failures" in ORCHESTRATOR_SYSTEM_PROMPT
        assert "analyze_logs"                in ORCHESTRATOR_SYSTEM_PROMPT
        assert "query_data"                  in ORCHESTRATOR_SYSTEM_PROMPT
        assert "get_batch_summary"           in ORCHESTRATOR_SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# Slack processor prompt builder
# ---------------------------------------------------------------------------

class TestSlackProcessorPrompts:

    def _build(self, command: str, args: dict) -> str:
        from slack.slack_processor_strands import _build_prompt
        return _build_prompt(command, args)

    def test_analyze_prompt_includes_hours(self):
        prompt = self._build("/etl-analyze", {"hours": "48", "pipeline": "member_info_etl"})
        assert "48 hours" in prompt
        assert "member_info_etl" in prompt

    def test_analyze_prompt_severity(self):
        prompt = self._build("/etl-analyze", {"severity": "CRITICAL", "hours": "12"})
        assert "CRITICAL" in prompt

    def test_logs_prompt_includes_source(self):
        prompt = self._build("/etl-logs", {"source": "s3://bucket/logs/", "hours": "6"})
        assert "s3://bucket/logs/" in prompt
        assert "6 hours" in prompt

    def test_query_prompt_passes_through(self):
        prompt = self._build("/etl-query", {"_query": "which clubs failed this week?"})
        assert "which clubs failed this week?" in prompt

    def test_status_prompt(self):
        prompt = self._build("/etl-status", {})
        assert "status" in prompt.lower() or "last" in prompt.lower()

    def test_analyze_with_run_id(self):
        prompt = self._build("/etl-analyze", {"run_id": "abc-xyz-789", "hours": "24"})
        assert "abc-xyz-789" in prompt


# ---------------------------------------------------------------------------
# End-to-end processor handler (mock orchestrator)
# ---------------------------------------------------------------------------

class TestSlackProcessorHandler:

    def _make_event(self, command: str, args: dict,
                    response_url: str = "https://hooks.slack.com/test") -> dict:
        return {
            "command":      command,
            "args":         args,
            "response_url": response_url,
            "channel_id":   "C12345",
            "user_id":      "U99999",
        }

    def test_handler_calls_orchestrator(self):
        mock_orch = MagicMock()
        mock_orch.return_value = "Analysis complete: 0 true failures found."

        with patch("slack.slack_processor_strands._get_orchestrator",
                   return_value=mock_orch), \
             patch("slack.slack_processor_strands._send") as mock_send:
            from slack.slack_processor_strands import handler
            handler(self._make_event("/etl-analyze", {"hours": "24"}), None)

        mock_orch.assert_called_once()
        mock_send.assert_called_once()
        body = mock_send.call_args.args[2]
        assert body["response_type"] == "in_channel"

    def test_handler_posts_error_on_exception(self):
        mock_orch = MagicMock()
        mock_orch.side_effect = RuntimeError("Bedrock unavailable")

        with patch("slack.slack_processor_strands._get_orchestrator",
                   return_value=mock_orch), \
             patch("slack.slack_processor_strands._send") as mock_send:
            from slack.slack_processor_strands import handler
            handler(self._make_event("/etl-query", {"_query": "show failures"}), None)

        body = mock_send.call_args.args[2]
        # Should be an ephemeral error card
        assert body["response_type"] == "ephemeral"
        block_texts = [
            b.get("text", {}).get("text", "")
            for b in body["blocks"]
        ]
        assert any("Error" in t or "error" in t for t in block_texts)

    def test_handler_uses_correct_prompt_for_logs(self):
        captured_prompts: list[str] = []

        def capturing_orch(prompt):
            captured_prompts.append(prompt)
            return "Log analysis done: no issues."

        mock_orch = MagicMock(side_effect=capturing_orch)

        with patch("slack.slack_processor_strands._get_orchestrator",
                   return_value=mock_orch), \
             patch("slack.slack_processor_strands._send"):
            from slack.slack_processor_strands import handler
            handler(self._make_event(
                "/etl-logs",
                {"source": "s3://strands-etl-audit/glue-logs/", "hours": "12"}
            ), None)

        assert len(captured_prompts) == 1
        assert "s3://strands-etl-audit/glue-logs/" in captured_prompts[0]
        assert "12 hours" in captured_prompts[0]
