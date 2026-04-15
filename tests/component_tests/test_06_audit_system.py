#!/usr/bin/env python3
"""
Component Test 6: Audit System
Tests ETL audit record creation and management
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from audit.etl_audit import (
    ETLRunAudit,
    DataQualityAudit,
    PlatformRecommendationAudit,
    RunStatus
)


def test_audit_record_creation():
    """Test ETL audit record creation"""
    print("\n  Testing: Audit Record Creation")
    print("  " + "-" * 50)

    try:
        run_id = f"test-{int(time.time() * 1000)}"
        audit = ETLRunAudit(
            run_id=run_id,
            job_name="test_job",
            status=RunStatus.RUNNING.value,
            started_at=datetime.now().isoformat(),
            platform="glue",
            config_hash="abc123",
            source_type="s3",
            target_type="redshift"
        )

        print(f"    run_id: {audit.run_id}")
        print(f"    job_name: {audit.job_name}")
        print(f"    status: {audit.status}")
        print(f"    platform: {audit.platform}")
        print(f"    source_type: {audit.source_type}")
        print(f"    target_type: {audit.target_type}")

        assert audit.run_id == run_id
        assert audit.job_name == "test_job"
        assert audit.status == RunStatus.RUNNING.value

        print("    [PASS] Audit record created")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_audit_status_transitions():
    """Test audit status transitions"""
    print("\n  Testing: Audit Status Transitions")
    print("  " + "-" * 50)

    try:
        audit = ETLRunAudit(
            run_id=f"test-{int(time.time() * 1000)}",
            job_name="status_test_job",
            status=RunStatus.PENDING.value,
            started_at=None,
            platform="emr",
            config_hash="xyz",
            source_type="glue_catalog",
            target_type="s3"
        )

        # Test PENDING -> RUNNING
        print(f"    Initial status: {audit.status}")
        assert audit.status == RunStatus.PENDING.value

        audit.status = RunStatus.RUNNING.value
        audit.started_at = datetime.now().isoformat()
        print(f"    After start: {audit.status}")
        assert audit.status == RunStatus.RUNNING.value

        # Test RUNNING -> SUCCEEDED
        audit.status = RunStatus.SUCCEEDED.value
        audit.completed_at = datetime.now().isoformat()
        audit.rows_read = 10000
        audit.rows_written = 9900
        print(f"    After completion: {audit.status}")
        assert audit.status == RunStatus.SUCCEEDED.value

        # Test all statuses
        valid_statuses = [s.value for s in RunStatus]
        print(f"    Valid statuses: {valid_statuses}")

        print("    [PASS] Status transitions work correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_audit_metrics_update():
    """Test audit metrics update"""
    print("\n  Testing: Audit Metrics Update")
    print("  " + "-" * 50)

    try:
        audit = ETLRunAudit(
            run_id=f"test-{int(time.time() * 1000)}",
            job_name="metrics_test_job",
            status=RunStatus.RUNNING.value,
            started_at=datetime.now().isoformat(),
            platform="glue",
            config_hash="metrics123",
            source_type="jdbc",
            target_type="iceberg"
        )

        # Update metrics
        audit.rows_read = 1_000_000
        audit.rows_written = 999_500
        audit.bytes_read = 500_000_000
        audit.bytes_written = 480_000_000
        audit.dq_score = 0.995
        audit.estimated_cost_usd = 2.50
        audit.recommendations = [
            "Consider partitioning by date",
            "Enable Adaptive Query Execution"
        ]

        print(f"    rows_read: {audit.rows_read:,}")
        print(f"    rows_written: {audit.rows_written:,}")
        print(f"    bytes_read: {audit.bytes_read:,}")
        print(f"    bytes_written: {audit.bytes_written:,}")
        print(f"    dq_score: {audit.dq_score:.1%}")
        print(f"    estimated_cost_usd: ${audit.estimated_cost_usd:.2f}")
        print(f"    recommendations: {len(audit.recommendations)}")

        assert audit.rows_read == 1_000_000
        assert audit.dq_score == 0.995
        assert len(audit.recommendations) == 2

        print("    [PASS] Metrics updated correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_audit_to_dict():
    """Test audit serialization to dictionary"""
    print("\n  Testing: Audit to Dictionary")
    print("  " + "-" * 50)

    try:
        audit = ETLRunAudit(
            run_id="test-dict-123",
            job_name="dict_test_job",
            status=RunStatus.SUCCEEDED.value,
            started_at="2024-01-15T10:00:00",
            completed_at="2024-01-15T10:05:00",
            platform="emr",
            config_hash="dict123",
            source_type="delta",
            target_type="redshift",
            rows_read=50000,
            rows_written=49800,
            dq_score=0.99
        )

        audit_dict = audit.to_dict()

        assert isinstance(audit_dict, dict)
        assert audit_dict['run_id'] == "test-dict-123"
        assert audit_dict['job_name'] == "dict_test_job"
        assert audit_dict['status'] == RunStatus.SUCCEEDED.value
        assert audit_dict['rows_read'] == 50000

        print(f"    Dict keys: {len(audit_dict)}")
        print(f"    Sample keys: {list(audit_dict.keys())[:5]}...")

        # Test JSON serializable
        json_str = json.dumps(audit_dict)
        assert len(json_str) > 0
        print(f"    JSON serializable: Yes ({len(json_str)} chars)")

        print("    [PASS] Audit serialization works")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_dq_audit_record():
    """Test Data Quality audit record"""
    print("\n  Testing: Data Quality Audit Record")
    print("  " + "-" * 50)

    try:
        dq_audit = DataQualityAudit(
            dq_id=f"dq-{int(time.time() * 1000)}",
            run_id="test-run-123",
            job_name="dq_test_job",
            executed_at=datetime.now().isoformat(),
            overall_score=0.95,
            total_rules=10,
            passed_rules=9,
            failed_rules=1,
            rule_results=[
                {"rule": "not_null", "status": "passed"},
                {"rule": "range_check", "status": "failed", "error": "Out of range"}
            ]
        )

        print(f"    dq_id: {dq_audit.dq_id}")
        print(f"    overall_score: {dq_audit.overall_score:.1%}")
        print(f"    total_rules: {dq_audit.total_rules}")
        print(f"    passed_rules: {dq_audit.passed_rules}")
        print(f"    failed_rules: {dq_audit.failed_rules}")

        assert dq_audit.overall_score == 0.95
        assert dq_audit.total_rules == 10
        assert dq_audit.passed_rules == 9

        print("    [PASS] DQ audit record created")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_platform_recommendation_audit():
    """Test Platform Recommendation audit record"""
    print("\n  Testing: Platform Recommendation Audit")
    print("  " + "-" * 50)

    try:
        rec_audit = PlatformRecommendationAudit(
            recommendation_id=f"rec-{int(time.time() * 1000)}",
            run_id="test-run-456",
            job_name="rec_test_job",
            created_at=datetime.now().isoformat(),
            current_platform="glue",
            recommended_platform="emr",
            reason="Large dataset benefits from EMR cluster",
            estimated_cost_current=10.0,
            estimated_cost_recommended=6.0,
            estimated_savings_pct=40.0
        )

        print(f"    current_platform: {rec_audit.current_platform}")
        print(f"    recommended_platform: {rec_audit.recommended_platform}")
        print(f"    estimated_savings_pct: {rec_audit.estimated_savings_pct}%")

        assert rec_audit.current_platform == "glue"
        assert rec_audit.recommended_platform == "emr"
        assert rec_audit.estimated_savings_pct == 40.0

        print("    [PASS] Platform recommendation audit created")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_audit_with_config_files():
    """Test audit creation with actual config files"""
    print("\n[TESTING WITH CONFIG FILES]")

    results = {"passed": 0, "failed": 0}
    config_files = [
        'simple_s3_to_s3.json',
        'simple_glue_catalog.json',
        'complex_full_pipeline.json',
        'complex_eks_karpenter.json'
    ]

    for config_file in config_files:
        print(f"\n  Testing: {config_file}")
        print("  " + "-" * 50)

        try:
            config_path = PROJECT_ROOT / 'test_configs' / config_file
            with open(config_path, 'r') as f:
                config = json.load(f)

            job_name = config.get('job_name', 'unknown')
            source_type = config.get('source', {}).get('type', 'multi_source')
            target_type = config.get('target', {}).get('type', 'unknown')

            platform = config.get('platform', 'auto')
            if isinstance(platform, dict):
                platform = platform.get('primary', 'auto')

            audit = ETLRunAudit(
                run_id=f"{job_name}-{int(time.time() * 1000)}",
                job_name=job_name,
                status=RunStatus.RUNNING.value,
                started_at=datetime.now().isoformat(),
                platform=platform,
                config_hash="test",
                source_type=source_type,
                target_type=target_type
            )

            print(f"    job_name: {audit.job_name}")
            print(f"    platform: {audit.platform}")
            print(f"    source_type: {audit.source_type}")
            print(f"    target_type: {audit.target_type}")
            print("    [PASS]")
            results["passed"] += 1

        except Exception as e:
            print(f"    [FAIL] Exception: {e}")
            results["failed"] += 1

    return results


def run_tests():
    """Run all audit system tests"""
    print("=" * 60)
    print("COMPONENT TEST 6: Audit System")
    print("=" * 60)

    results = {"passed": 0, "failed": 0}

    # Unit tests
    print("\n[UNIT TESTS]")
    tests = [
        test_audit_record_creation,
        test_audit_status_transitions,
        test_audit_metrics_update,
        test_audit_to_dict,
        test_dq_audit_record,
        test_platform_recommendation_audit
    ]

    for test_func in tests:
        if test_func():
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Config file tests
    file_results = test_audit_with_config_files()
    results["passed"] += file_results["passed"]
    results["failed"] += file_results["failed"]

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = results["passed"] + results["failed"]
    print(f"  Total:  {total}")
    print(f"  Passed: {results['passed']}")
    print(f"  Failed: {results['failed']}")

    if results["failed"] == 0:
        print("\n  [SUCCESS] All audit system tests passed!")
        return True
    else:
        print(f"\n  [FAILURE] {results['failed']} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
