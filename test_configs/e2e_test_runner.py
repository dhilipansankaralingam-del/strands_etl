#!/usr/bin/env python3
"""
End-to-End Test Runner for ETL Framework
Run this script to test the entire ETL framework with various configurations
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import traceback

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


@dataclass
class TestResult:
    """Test execution result"""
    test_name: str
    config_file: str
    status: str  # PASSED, FAILED, SKIPPED
    duration_seconds: float
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class ETLTestRunner:
    """E2E Test Runner for ETL Framework"""

    def __init__(self, config_dir: str = None, mock_mode: bool = True):
        self.config_dir = config_dir or str(Path(__file__).parent)
        self.mock_mode = mock_mode  # Use mock mode for local testing
        self.results: List[TestResult] = []

    def load_config(self, config_file: str) -> Dict[str, Any]:
        """Load JSON configuration file"""
        config_path = os.path.join(self.config_dir, config_file)
        with open(config_path, 'r') as f:
            return json.load(f)

    def list_configs(self) -> List[str]:
        """List all JSON config files in directory"""
        return [f for f in os.listdir(self.config_dir) if f.endswith('.json')]

    # ============ Test: Configuration Parsing ============

    def test_config_parsing(self, config_file: str) -> TestResult:
        """Test that configuration can be parsed correctly"""
        start_time = time.time()
        test_name = f"config_parsing:{config_file}"

        try:
            config = self.load_config(config_file)

            # Validate required fields
            required_fields = ['job_name', 'source', 'target']
            missing = [f for f in required_fields if f not in config]

            if missing:
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="FAILED",
                    duration_seconds=time.time() - start_time,
                    error_message=f"Missing required fields: {missing}"
                )

            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="PASSED",
                duration_seconds=time.time() - start_time,
                details={"job_name": config.get('job_name')}
            )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )

    # ============ Test: Notification Configuration ============

    def test_notification_config(self, config_file: str) -> TestResult:
        """Test notification configuration parsing with Y/N flags"""
        start_time = time.time()
        test_name = f"notification_config:{config_file}"

        try:
            config = self.load_config(config_file)
            notifications = config.get('notifications', {})

            from integrations.notification_manager import NotificationConfig

            notif_config = NotificationConfig.from_json(config)

            details = {
                "notifications_enabled": notif_config.notifications_enabled,
                "slack_enabled": notif_config.slack_enabled,
                "teams_enabled": notif_config.teams_enabled,
                "email_enabled": notif_config.email_enabled,
                "notify_on_failure": notif_config.notify_on_failure,
                "notify_on_success": notif_config.notify_on_success
            }

            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="PASSED",
                duration_seconds=time.time() - start_time,
                details=details
            )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    # ============ Test: Notification Manager Creation ============

    def test_notification_manager(self, config_file: str) -> TestResult:
        """Test notification manager initialization"""
        start_time = time.time()
        test_name = f"notification_manager:{config_file}"

        try:
            config = self.load_config(config_file)

            from integrations.notification_manager import create_notification_manager

            manager = create_notification_manager(config)
            status = manager.get_status()

            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="PASSED",
                duration_seconds=time.time() - start_time,
                details=status
            )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    # ============ Test: Data Quality Config ============

    def test_dq_config(self, config_file: str) -> TestResult:
        """Test data quality configuration"""
        start_time = time.time()
        test_name = f"dq_config:{config_file}"

        try:
            config = self.load_config(config_file)
            dq_config = config.get('data_quality', {})

            if not dq_config:
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="SKIPPED",
                    duration_seconds=time.time() - start_time,
                    details={"reason": "No data quality config"}
                )

            rules = dq_config.get('rules', [])

            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="PASSED",
                duration_seconds=time.time() - start_time,
                details={
                    "enabled": dq_config.get('enabled'),
                    "rule_count": len(rules),
                    "fail_on_error": dq_config.get('fail_on_error')
                }
            )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )

    # ============ Test: Audit Manager ============

    def test_audit_manager(self, config_file: str) -> TestResult:
        """Test audit manager with mock data"""
        start_time = time.time()
        test_name = f"audit_manager:{config_file}"

        try:
            config = self.load_config(config_file)

            if self.mock_mode:
                # Mock audit test
                from audit.etl_audit import ETLRunAudit, RunStatus
                from datetime import datetime

                audit = ETLRunAudit(
                    run_id=f"test-{int(time.time())}",
                    job_name=config.get('job_name', 'test_job'),
                    status=RunStatus.RUNNING.value,
                    started_at=datetime.now().isoformat(),
                    platform=config.get('platform', 'glue') if isinstance(config.get('platform'), str) else config.get('platform', {}).get('primary', 'glue'),
                    config_hash="test_hash",
                    source_type=config.get('source', {}).get('type', 'unknown'),
                    target_type=config.get('target', {}).get('type', 'unknown')
                )

                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="PASSED",
                    duration_seconds=time.time() - start_time,
                    details={
                        "run_id": audit.run_id,
                        "job_name": audit.job_name,
                        "status": audit.status
                    }
                )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    # ============ Test: Teams Integration ============

    def test_teams_integration(self, config_file: str) -> TestResult:
        """Test Teams integration configuration"""
        start_time = time.time()
        test_name = f"teams_integration:{config_file}"

        try:
            config = self.load_config(config_file)
            teams_config = config.get('notifications', {}).get('teams', {})

            if teams_config.get('enabled', 'N').upper() != 'Y':
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="SKIPPED",
                    duration_seconds=time.time() - start_time,
                    details={"reason": "Teams not enabled"}
                )

            from integrations.teams_integration import TeamsIntegrationFactory

            teams = TeamsIntegrationFactory.from_json_flags(config.get('notifications', {}))

            if teams:
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="PASSED",
                    duration_seconds=time.time() - start_time,
                    details={
                        "channel": teams.config.channel_name,
                        "webhook_configured": bool(teams.config.webhook_url)
                    }
                )
            else:
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="FAILED",
                    duration_seconds=time.time() - start_time,
                    error_message="Teams integration returned None"
                )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    # ============ Test: API Gateway Handlers ============

    def test_api_handlers(self, config_file: str) -> TestResult:
        """Test API Gateway handler configuration"""
        start_time = time.time()
        test_name = f"api_handlers:{config_file}"

        try:
            from integrations.api_gateway import APIConfig, create_api_handler

            api_config = APIConfig(
                api_name="test-etl-api",
                stage="test"
            )

            api = create_api_handler(api_config)

            # Test health check endpoint
            mock_event = {
                'httpMethod': 'GET',
                'path': '/health',
                'headers': {},
                'queryStringParameters': {},
                'pathParameters': {},
                'body': None
            }

            response = api.handle_request(mock_event, None)

            if response['statusCode'] == 200:
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="PASSED",
                    duration_seconds=time.time() - start_time,
                    details={"health_check": "OK"}
                )
            else:
                return TestResult(
                    test_name=test_name,
                    config_file=config_file,
                    status="FAILED",
                    duration_seconds=time.time() - start_time,
                    error_message=f"Health check failed: {response}"
                )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    # ============ Test: Full E2E Flow (Mock) ============

    def test_e2e_flow_mock(self, config_file: str) -> TestResult:
        """Test full E2E flow with mock data"""
        start_time = time.time()
        test_name = f"e2e_flow_mock:{config_file}"

        try:
            config = self.load_config(config_file)
            job_name = config.get('job_name', 'test_job')

            # Step 1: Parse config
            from integrations.notification_manager import create_notification_manager

            # Step 2: Create notification manager
            notif_manager = create_notification_manager(config)

            # Step 3: Create audit record
            from audit.etl_audit import ETLRunAudit, RunStatus

            run_id = f"{job_name}-{int(time.time() * 1000)}"
            audit = ETLRunAudit(
                run_id=run_id,
                job_name=job_name,
                status=RunStatus.RUNNING.value,
                started_at=datetime.now().isoformat(),
                platform=config.get('platform', 'auto') if isinstance(config.get('platform'), str) else config.get('platform', {}).get('primary', 'auto'),
                config_hash="e2e_test",
                source_type=config.get('source', {}).get('type', 'unknown'),
                target_type=config.get('target', {}).get('type', 'unknown')
            )

            # Step 4: Simulate job execution
            time.sleep(0.1)  # Simulate processing

            # Step 5: Update audit with results
            audit.status = RunStatus.SUCCEEDED.value
            audit.completed_at = datetime.now().isoformat()
            audit.rows_read = 10000
            audit.rows_written = 9950
            audit.dq_score = 0.95
            audit.estimated_cost_usd = 0.05

            # Step 6: Check notification status
            notif_status = notif_manager.get_status()

            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="PASSED",
                duration_seconds=time.time() - start_time,
                details={
                    "run_id": run_id,
                    "job_name": job_name,
                    "audit_status": audit.status,
                    "rows_processed": audit.rows_read,
                    "dq_score": audit.dq_score,
                    "notification_channels": {
                        "slack": notif_status['channels']['slack']['enabled'],
                        "teams": notif_status['channels']['teams']['enabled'],
                        "email": notif_status['channels']['email']['enabled']
                    }
                }
            )

        except Exception as e:
            return TestResult(
                test_name=test_name,
                config_file=config_file,
                status="FAILED",
                duration_seconds=time.time() - start_time,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    # ============ Run All Tests ============

    def run_all_tests(self, config_files: List[str] = None) -> List[TestResult]:
        """Run all tests for specified config files"""

        if config_files is None:
            config_files = self.list_configs()

        print("\n" + "=" * 80)
        print("ETL Framework E2E Test Runner")
        print("=" * 80)
        print(f"Test configs directory: {self.config_dir}")
        print(f"Mock mode: {self.mock_mode}")
        print(f"Config files to test: {len(config_files)}")
        print("=" * 80 + "\n")

        all_results = []

        for config_file in config_files:
            if config_file == 'e2e_test_runner.py':
                continue

            print(f"\n{'=' * 60}")
            print(f"Testing: {config_file}")
            print('=' * 60)

            # Run all test types
            tests = [
                self.test_config_parsing,
                self.test_notification_config,
                self.test_notification_manager,
                self.test_dq_config,
                self.test_audit_manager,
                self.test_teams_integration,
                self.test_api_handlers,
                self.test_e2e_flow_mock
            ]

            for test_func in tests:
                result = test_func(config_file)
                all_results.append(result)
                self._print_result(result)

        self.results = all_results
        self._print_summary()

        return all_results

    def _print_result(self, result: TestResult):
        """Print individual test result"""
        status_icon = {
            "PASSED": "[PASS]",
            "FAILED": "[FAIL]",
            "SKIPPED": "[SKIP]"
        }

        icon = status_icon.get(result.status, "[ ?? ]")
        print(f"  {icon} {result.test_name} ({result.duration_seconds:.3f}s)")

        if result.status == "FAILED" and result.error_message:
            print(f"         Error: {result.error_message[:100]}...")

    def _print_summary(self):
        """Print test summary"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == "PASSED")
        failed = sum(1 for r in self.results if r.status == "FAILED")
        skipped = sum(1 for r in self.results if r.status == "SKIPPED")

        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"Total Tests:  {total}")
        print(f"Passed:       {passed} ({100 * passed / total:.1f}%)" if total > 0 else "Passed: 0")
        print(f"Failed:       {failed}")
        print(f"Skipped:      {skipped}")
        print("=" * 80)

        if failed > 0:
            print("\nFailed Tests:")
            for r in self.results:
                if r.status == "FAILED":
                    print(f"  - {r.test_name}")
                    print(f"    Config: {r.config_file}")
                    print(f"    Error: {r.error_message[:200] if r.error_message else 'Unknown'}")

        print("\n")

    def generate_report(self, output_file: str = None) -> str:
        """Generate JSON test report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "config_dir": self.config_dir,
            "mock_mode": self.mock_mode,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.status == "PASSED"),
                "failed": sum(1 for r in self.results if r.status == "FAILED"),
                "skipped": sum(1 for r in self.results if r.status == "SKIPPED")
            },
            "results": [
                {
                    "test_name": r.test_name,
                    "config_file": r.config_file,
                    "status": r.status,
                    "duration_seconds": r.duration_seconds,
                    "error_message": r.error_message,
                    "details": r.details
                }
                for r in self.results
            ]
        }

        report_json = json.dumps(report, indent=2)

        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_json)
            print(f"Report written to: {output_file}")

        return report_json


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='ETL Framework E2E Test Runner')
    parser.add_argument('--config-dir', type=str, default=None,
                        help='Directory containing test configs')
    parser.add_argument('--config', type=str, default=None,
                        help='Specific config file to test')
    parser.add_argument('--mock', action='store_true', default=True,
                        help='Use mock mode (default: True)')
    parser.add_argument('--no-mock', action='store_true',
                        help='Disable mock mode (requires AWS credentials)')
    parser.add_argument('--report', type=str, default=None,
                        help='Output file for JSON report')

    args = parser.parse_args()

    mock_mode = not args.no_mock

    runner = ETLTestRunner(
        config_dir=args.config_dir,
        mock_mode=mock_mode
    )

    config_files = [args.config] if args.config else None

    results = runner.run_all_tests(config_files)

    if args.report:
        runner.generate_report(args.report)

    # Exit with error code if any tests failed
    failed = sum(1 for r in results if r.status == "FAILED")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
