"""
tests/test_tools.py
====================
Unit tests for all Strands @tool functions.
All AWS calls are intercepted by the mock_athena / mock_s3 fixtures.
No real AWS credentials required.
"""

import json
import pytest


# ---------------------------------------------------------------------------
# holiday_tools
# ---------------------------------------------------------------------------

class TestHolidayTools:

    def test_christmas_is_holiday(self):
        from strands_agents.tools.holiday_tools import get_holiday_context
        raw    = get_holiday_context("2026-12-25")
        result = json.loads(raw)
        assert result["is_holiday"] is True
        assert result["volume_multiplier"] == pytest.approx(0.05)
        assert result["zscore_threshold"]  == pytest.approx(99.0)
        assert "christmas" in result["holiday_name"].lower()

    def test_normal_day_not_holiday(self):
        from strands_agents.tools.holiday_tools import get_holiday_context
        raw    = get_holiday_context("2026-07-15")
        result = json.loads(raw)
        assert result["is_holiday"] is False
        assert result["zscore_threshold"] == pytest.approx(3.0)

    def test_thanksgiving(self):
        from strands_agents.tools.holiday_tools import get_holiday_context
        raw    = get_holiday_context("2026-11-26")
        result = json.loads(raw)
        assert result["is_holiday"] is True
        assert result["volume_multiplier"] == pytest.approx(0.10)

    def test_list_upcoming_includes_christmas(self):
        from strands_agents.tools.holiday_tools import list_upcoming_holidays
        from datetime import date, timedelta
        # Mock today to be Dec 20 2026 — Christmas is 5 days away
        import strands_agents.tools.holiday_tools as ht
        import unittest.mock as mock
        fake_today = date(2026, 12, 20)
        with mock.patch("strands_agents.tools.holiday_tools.datetime") as dt_mock:
            dt_mock.utcnow.return_value.__class__ = type("dt", (), {})
            dt_mock.utcnow.return_value.date.return_value = fake_today
            dt_mock.strptime = __import__("datetime").datetime.strptime
            raw    = list_upcoming_holidays(10)
        result = json.loads(raw)
        # christmas should be in the list
        dates = [h["date"] for h in result["upcoming_holidays"]]
        assert "2026-12-25" in dates


# ---------------------------------------------------------------------------
# athena_tools
# ---------------------------------------------------------------------------

class TestAthenaTools:

    def test_fetch_audit_failures_returns_records(self, mock_athena):
        from strands_agents.tools.athena_tools import fetch_audit_failures
        raw    = fetch_audit_failures(24)
        result = json.loads(raw)
        assert result["count"] == 2
        assert result["records"][0]["id"] == "rec-001"

    def test_fetch_audit_failures_pipeline_filter(self, mock_athena):
        from strands_agents.tools.athena_tools import fetch_audit_failures
        # Doesn't blow up and returns the mock rows
        raw = fetch_audit_failures(24, pipeline="member_info_etl")
        assert json.loads(raw)["count"] >= 0

    def test_fetch_historical_patterns(self, mock_athena):
        from strands_agents.tools.athena_tools import fetch_historical_patterns
        raw    = fetch_historical_patterns("member_info_silver", "row_count_check")
        result = json.loads(raw)
        assert result["table_name"] == "member_info_silver"
        assert isinstance(result["history"], list)

    def test_execute_raw_sql(self, mock_athena):
        from strands_agents.tools.athena_tools import execute_raw_sql
        raw    = execute_raw_sql("SELECT 1")
        result = json.loads(raw)
        assert "rows" in result
        assert "columns" in result


# ---------------------------------------------------------------------------
# log_tools
# ---------------------------------------------------------------------------

class TestLogTools:

    def test_fetch_s3_logs_requires_s3_prefix(self):
        from strands_agents.tools.log_tools import fetch_s3_logs
        raw    = fetch_s3_logs("not-s3://something")
        result = json.loads(raw)
        assert "error" in result

    def test_fetch_s3_logs_with_mock(self, mock_s3):
        from strands_agents.tools.log_tools import fetch_s3_logs
        # Make the mock return realistic log content
        mock_s3.get_object.return_value = {
            "Body": type("B", (), {
                "read": lambda self: b"ERROR: GlueJobRun failed after max retries\n"
            })()
        }
        raw    = fetch_s3_logs("s3://test-bucket/glue-logs/", 24)
        result = json.loads(raw)
        assert "service" in result
        # Fast diagnosis should catch the retry error
        assert isinstance(result["fast_diagnosis"], list)

    def test_extract_error_blocks(self):
        from strands_agents.tools.log_tools import extract_error_blocks
        content = "INFO something\nERROR NullPointerException at line 42\nINFO done"
        raw     = extract_error_blocks(content, "lambda")
        result  = json.loads(raw)
        assert result["count"] > 0
        assert any("ERROR" in b or "NullPointerException" in b
                   for b in result["error_blocks"])

    def test_fast_diagnose_s3_access_denied(self, mock_s3):
        from strands_agents.tools.log_tools import fetch_s3_logs
        mock_s3.get_object.return_value = {
            "Body": type("B", (), {
                "read": lambda self: b"S3 Access Denied for bucket strands-etl-audit"
            })()
        }
        raw    = fetch_s3_logs("s3://test-bucket/logs/", 1)
        result = json.loads(raw)
        fixes  = [d["recommended_fix"] for d in result.get("fast_diagnosis", [])]
        assert any("IAM" in f or "s3:" in f for f in fixes)


# ---------------------------------------------------------------------------
# validation_tools
# ---------------------------------------------------------------------------

class TestValidationTools:

    def test_get_batch_summary(self, mock_athena):
        from strands_agents.tools.validation_tools import get_batch_summary
        raw    = get_batch_summary("run-abc-123")
        result = json.loads(raw)
        assert "true_failures" in result or "breakdown" in result

    def test_save_run_report(self, mock_s3):
        from strands_agents.tools.validation_tools import save_run_report
        raw    = save_run_report(json.dumps({"run_id": "run-001"}))
        result = json.loads(raw)
        assert result["saved"] is True

    def test_fetch_prior_classifications(self, mock_athena):
        from strands_agents.tools.validation_tools import fetch_prior_classifications
        raw    = fetch_prior_classifications("member_info_silver", "row_count_check")
        result = json.loads(raw)
        assert "prior_decisions" in result
