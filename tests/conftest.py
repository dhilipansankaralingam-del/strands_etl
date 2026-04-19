"""
tests/conftest.py
==================
Pytest fixtures shared across all test modules.

All AWS calls (Athena, S3, CloudWatch, Bedrock) are mocked with moto or
unittest.mock so tests run fully offline with no AWS credentials required.

Run with:
    pytest tests/ -v --tb=short
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Environment defaults for unit tests
# ---------------------------------------------------------------------------

os.environ.setdefault("AWS_DEFAULT_REGION",  "us-east-1")
os.environ.setdefault("ATHENA_DATABASE",     "data_quality_db")
os.environ.setdefault("ATHENA_S3_OUTPUT",    "s3://test-bucket/athena-results/")
os.environ.setdefault("ATHENA_WORKGROUP",    "primary")
os.environ.setdefault("LAST_REPORT_BUCKET",  "test-bucket")
os.environ.setdefault("LAST_REPORT_KEY",     "agent_analyzer/last_report.json")
os.environ.setdefault("BEDROCK_MODEL_ID",    "anthropic.claude-3-sonnet-20240229-v1:0")
os.environ.setdefault("HOLIDAY_CALENDAR_PATH", "config/holiday_calendar.json")

# Point to a fixture calendar so holiday tests don't need the real file
_FIXTURE_CALENDAR = {
    "explicit_dates_2026": {
        "2026-12-25": {
            "holiday":    "christmas_day",
            "multiplier": 0.05,
            "surge_next": 1.55,
            "zscore_context": "holiday_low_volume",
        },
        "2026-11-26": {
            "holiday":    "thanksgiving",
            "multiplier": 0.10,
            "surge_next": 1.40,
            "zscore_context": "holiday_low_volume",
        },
    },
    "zscore_thresholds": {
        "normal":             {"alert_zscore": 3.0},
        "holiday_low_volume": {"alert_zscore": 99.0},
        "post_holiday_surge": {"alert_zscore": 6.0},
    },
}


@pytest.fixture(autouse=True)
def patch_calendar(tmp_path, monkeypatch):
    """Write a temp holiday calendar and point the env var at it."""
    cal_file = tmp_path / "holiday_calendar.json"
    cal_file.write_text(json.dumps(_FIXTURE_CALENDAR))
    monkeypatch.setenv("HOLIDAY_CALENDAR_PATH", str(cal_file))
    # Patch the module-level constant AND the cache (module reads HOLIDAY_CALENDAR_PATH
    # at import time, so we must override both the env var and the captured variable)
    import strands_agents.tools.holiday_tools as ht
    monkeypatch.setattr(ht, "HOLIDAY_CALENDAR_PATH", str(cal_file))
    ht._calendar_cache = None
    yield
    ht._calendar_cache = None


# ---------------------------------------------------------------------------
# Sample Athena row fixtures
# ---------------------------------------------------------------------------

SAMPLE_FAIL_RECORDS = [
    {
        "id":           "rec-001",
        "run_id":       "run-abc-123",
        "pipeline_name": "member_info_etl",
        "table_name":   "member_info_silver",
        "rule_name":    "row_count_check",
        "severity":     "HIGH",
        "status":       "FAIL",
        "actual_value": "50",
        "expected_value": "1000",
        "threshold":    "0.9",
        "error_message": "Row count dropped by 95%",
        "run_date":     "2026-12-25 08:00:00",
        "additional_context": "",
    },
    {
        "id":           "rec-002",
        "run_id":       "run-abc-123",
        "pipeline_name": "member_info_etl",
        "table_name":   "member_info_silver",
        "rule_name":    "ssn_not_exposed",
        "severity":     "CRITICAL",
        "status":       "FAIL",
        "actual_value": "1",
        "expected_value": "0",
        "threshold":    "0",
        "error_message": "SSN column found in output",
        "run_date":     "2026-12-25 08:00:00",
        "additional_context": "",
    },
]

SAMPLE_HISTORY = [
    {"day": "2026-12-24", "total": "500", "failures": "2",
     "avg_actual": "950.0", "stddev_actual": "30.0"},
    {"day": "2026-12-23", "total": "480", "failures": "0",
     "avg_actual": "945.0", "stddev_actual": "28.0"},
]


@pytest.fixture
def mock_athena(monkeypatch):
    """Return mock Athena rows for fetch calls."""
    def _run_query_mock(sql: str, database: str = "data_quality_db"):
        sql_upper = sql.upper()
        if "AI_CLASSIFICATION IS NULL" in sql_upper or "STATUS = 'FAIL'" in sql_upper:
            return SAMPLE_FAIL_RECORDS
        if "GROUP BY DATE" in sql_upper or "STDDEV" in sql_upper:
            return SAMPLE_HISTORY
        if "AI_CLASSIFICATION IS NOT NULL" in sql_upper:
            return []
        if "GROUP BY AI_CLASSIFICATION" in sql_upper:
            return [{"ai_classification": "TRUE_FAILURE", "cnt": "1", "avg_confidence": "0.95"}]
        return []

    import strands_agents.tools.athena_tools as at
    import strands_agents.tools.validation_tools as vt
    monkeypatch.setattr(at, "_run_query", _run_query_mock)
    monkeypatch.setattr(vt, "_run_query", _run_query_mock)
    return _run_query_mock


@pytest.fixture
def mock_s3(monkeypatch):
    """Mock boto3 S3 client for log and report tests."""
    s3_mock = MagicMock()
    s3_mock.put_object.return_value = {}
    s3_mock.get_object.return_value = {
        "Body": MagicMock(read=lambda: b'{"run_id": "run-abc-123", "ai_analysis": {}}')
    }
    s3_mock.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "glue-logs/job.log",
                        "LastModified": __import__("datetime").datetime.now(
                            __import__("datetime").timezone.utc)}]}
    ]

    def _boto3_client(service, **kwargs):
        return s3_mock

    monkeypatch.setattr("boto3.client", _boto3_client)
    return s3_mock


@pytest.fixture
def mock_strands_agent():
    """
    Stub for a Strands Agent — returns a canned response string.
    Use this when testing orchestrator routing without real Bedrock calls.
    """
    agent = MagicMock()
    agent.return_value = json.dumps({
        "classification": "TRUE_FAILURE",
        "confidence": 0.95,
        "summary": "Test agent response",
    })
    return agent
