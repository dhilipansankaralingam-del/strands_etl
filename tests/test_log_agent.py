"""
tests/test_log_agent.py
=========================
Unit + integration tests for the Strands log agent.

Verifies:
  - Service auto-detection (Glue vs Lambda vs Athena vs EMR).
  - Fast-diagnosis patterns fire before Bedrock is called.
  - Agent tool list is correct.
  - CloudWatch log fetching correctly handles missing log groups.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Service detection
# ---------------------------------------------------------------------------

class TestServiceDetection:

    def test_detects_glue_from_path(self):
        from strands_agents.tools.log_tools import _detect_service
        service = _detect_service("/aws/glue/jobs/trigger_glue_job", "some glue content")
        assert service == "glue"

    def test_detects_lambda_from_path(self):
        from strands_agents.tools.log_tools import _detect_service
        content = "START RequestId: abc END RequestId: abc REPORT RequestId: abc"
        service = _detect_service("/aws/lambda/etl-processor", content)
        assert service == "lambda"

    def test_detects_athena_from_content(self):
        from strands_agents.tools.log_tools import _detect_service
        content = "SYNTAX_ERROR: line 3:1 mismatched input 'FORM'"
        service = _detect_service("s3://logs/athena/", content)
        assert service == "athena"

    def test_generic_for_unknown(self):
        from strands_agents.tools.log_tools import _detect_service
        service = _detect_service("s3://bucket/random/path", "no recognizable content here")
        assert service == "generic"


# ---------------------------------------------------------------------------
# Fast diagnosis
# ---------------------------------------------------------------------------

class TestFastDiagnosis:

    def test_s3_access_denied(self):
        from strands_agents.tools.log_tools import _fast_diagnose
        content = "ERROR: S3 Access Denied for bucket strands-etl-audit"
        findings = _fast_diagnose(content)
        assert any("IAM" in f["recommended_fix"] or "s3:" in f["recommended_fix"]
                   for f in findings)

    def test_max_retry_exceeded(self):
        from strands_agents.tools.log_tools import _fast_diagnose
        content = "Job run attempt has failed: exceeded max retries"
        findings = _fast_diagnose(content)
        assert any("MaxRetries" in f["recommended_fix"] or "Glue" in f["recommended_fix"]
                   for f in findings)

    def test_oom_error(self):
        from strands_agents.tools.log_tools import _fast_diagnose
        content = "Task failed: java.lang.OutOfMemoryError: Java heap space"
        findings = _fast_diagnose(content)
        assert any("DPU" in f["recommended_fix"] or "partition" in f["recommended_fix"]
                   for f in findings)

    def test_no_false_positive_on_clean_log(self):
        from strands_agents.tools.log_tools import _fast_diagnose
        content = "INFO Starting job. INFO Step 1/3 done. INFO Completed successfully."
        findings = _fast_diagnose(content)
        assert findings == []


# ---------------------------------------------------------------------------
# fetch_s3_logs / fetch_cloudwatch_logs
# ---------------------------------------------------------------------------

class TestFetchLogs:

    def test_fetch_s3_bad_path_returns_error(self):
        from strands_agents.tools.log_tools import fetch_s3_logs
        result = json.loads(fetch_s3_logs("not-s3://bucket/key"))
        assert "error" in result

    def test_fetch_cloudwatch_missing_group(self, monkeypatch):
        """ResourceNotFoundException should return a clean error JSON."""
        import boto3
        logs_mock = MagicMock()
        logs_mock.describe_log_streams.side_effect = (
            logs_mock.exceptions.ResourceNotFoundException(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
                "DescribeLogStreams",
            )
        )
        # Set up exceptions attribute
        class FakeExceptions:
            ResourceNotFoundException = Exception

        logs_mock.exceptions = FakeExceptions()
        logs_mock.describe_log_streams.side_effect = Exception("ResourceNotFoundException")

        monkeypatch.setattr("boto3.client", lambda svc, **kw: logs_mock)
        from strands_agents.tools.log_tools import fetch_cloudwatch_logs
        result = json.loads(fetch_cloudwatch_logs("/aws/glue/nonexistent", 1))
        assert "error" in result


# ---------------------------------------------------------------------------
# Agent wiring
# ---------------------------------------------------------------------------

class TestLogAgentWiring:

    def test_agent_has_log_tools(self):
        import inspect
        import strands_agents.log_agent as la
        source = inspect.getsource(la.create_log_agent)
        assert "fetch_s3_logs"         in source
        assert "fetch_cloudwatch_logs" in source
        assert "extract_error_blocks"  in source

    def test_system_prompt_contains_services(self):
        from strands_agents.log_agent import LOG_SYSTEM_PROMPT
        for svc in ["glue", "athena", "lambda", "emr"]:
            assert svc in LOG_SYSTEM_PROMPT.lower()
        assert "JSON" in LOG_SYSTEM_PROMPT
        assert "fix_steps" in LOG_SYSTEM_PROMPT
