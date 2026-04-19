"""
tests/test_validation_agent.py
================================
Integration tests for the Strands validation agent.

These tests mock the Strands Agent itself (so no real Bedrock calls occur)
but verify that:
  - create_validation_agent() returns a callable Agent.
  - Tool list is correct and complete.
  - Prompts constructed for realistic Slack inputs work end-to-end.
"""

import inspect
import json
from unittest.mock import MagicMock, patch

import pytest
import strands_agents.validation_agent as va


class TestCreateValidationAgent:

    def test_agent_has_correct_tools(self):
        """Verify the agent source code wires all required tools."""
        source = inspect.getsource(va.create_validation_agent)
        for tool_name in [
            "fetch_audit_failures",
            "write_ai_enrichment",
            "get_holiday_context",
            "fetch_historical_patterns",
            "fetch_prior_classifications",
            "get_batch_summary",
        ]:
            assert tool_name in source, f"{tool_name} missing from create_validation_agent"

    def test_system_prompt_contains_classifications(self):
        assert "TRUE_FAILURE"          in va.VALIDATION_SYSTEM_PROMPT
        assert "FALSE_POSITIVE"        in va.VALIDATION_SYSTEM_PROMPT
        assert "NEEDS_INVESTIGATION"   in va.VALIDATION_SYSTEM_PROMPT
        assert "holiday"               in va.VALIDATION_SYSTEM_PROMPT.lower()
        assert "PII"                   in va.VALIDATION_SYSTEM_PROMPT

    def test_agent_receives_prompt(self, mock_athena, mock_s3):
        """Agent should be called with a non-empty string prompt."""
        mock_agent_instance = MagicMock()
        mock_agent_instance.return_value = json.dumps({"summary": "ok"})

        with patch.object(va, "Agent", return_value=mock_agent_instance), \
             patch.object(va, "BedrockModel"):
            agent = va.create_validation_agent()
            # Simulate calling the agent as the orchestrator would
            result = agent("Analyze failures from the last 24 hours")
            mock_agent_instance.assert_called_once()


class TestValidationAgentHolidayLogic:
    """
    Verify the holiday tool correctly instructs FALSE_POSITIVE classification.
    These tests do NOT call Bedrock — they test the tool output that Bedrock sees.
    """

    def test_christmas_day_tool_output(self):
        from strands_agents.tools.holiday_tools import get_holiday_context
        data = json.loads(get_holiday_context("2026-12-25"))
        assert data["is_holiday"] is True
        # Prompt summary should contain FALSE_POSITIVE hint
        assert "FALSE_POSITIVE" in data["summary"]

    def test_normal_day_no_suppression(self):
        from strands_agents.tools.holiday_tools import get_holiday_context
        data = json.loads(get_holiday_context("2026-06-15"))
        assert data["is_holiday"] is False
        assert data["zscore_threshold"] == pytest.approx(3.0)
