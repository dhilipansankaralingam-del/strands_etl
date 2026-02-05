#!/usr/bin/env python3
"""
ETL Framework - Complete Test Suite Runner
Runs all component tests and E2E integration tests

Usage:
    python tests/run_all_tests.py                    # Run all tests
    python tests/run_all_tests.py --component-only   # Only component tests
    python tests/run_all_tests.py --e2e-only        # Only E2E tests
    python tests/run_all_tests.py --report report.json  # Generate JSON report
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
COMPONENT_TESTS_DIR = PROJECT_ROOT / 'tests' / 'component_tests'
CONFIG_DIR = PROJECT_ROOT / 'test_configs'


class TestRunner:
    """Runs all tests and collects results"""

    def __init__(self):
        self.results = {
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'component_tests': [],
            'e2e_tests': [],
            'summary': {
                'total': 0,
                'passed': 0,
                'failed': 0
            }
        }

    def run_component_test(self, test_file: Path) -> Dict[str, Any]:
        """Run a single component test"""
        test_name = test_file.stem
        print(f"\n{'=' * 60}")
        print(f"RUNNING: {test_name}")
        print('=' * 60)

        start_time = time.time()

        try:
            result = subprocess.run(
                [sys.executable, str(test_file)],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=str(PROJECT_ROOT)
            )

            duration = time.time() - start_time
            passed = result.returncode == 0

            # Print output
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)

            return {
                'name': test_name,
                'file': str(test_file),
                'passed': passed,
                'duration': duration,
                'returncode': result.returncode,
                'stdout': result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout,
                'stderr': result.stderr[-1000:] if len(result.stderr) > 1000 else result.stderr
            }

        except subprocess.TimeoutExpired:
            return {
                'name': test_name,
                'file': str(test_file),
                'passed': False,
                'duration': 120,
                'error': 'Test timed out after 120 seconds'
            }
        except Exception as e:
            return {
                'name': test_name,
                'file': str(test_file),
                'passed': False,
                'duration': time.time() - start_time,
                'error': str(e)
            }

    def run_all_component_tests(self) -> List[Dict[str, Any]]:
        """Run all component tests in order"""
        print("\n" + "=" * 70)
        print("   COMPONENT TESTS")
        print("=" * 70)

        results = []

        # Get all test files, sorted by name
        test_files = sorted(COMPONENT_TESTS_DIR.glob('test_*.py'))

        if not test_files:
            print(f"No component tests found in {COMPONENT_TESTS_DIR}")
            return results

        print(f"\nFound {len(test_files)} component tests")

        for test_file in test_files:
            result = self.run_component_test(test_file)
            results.append(result)

        return results

    def run_e2e_test(self, config_file: str) -> Dict[str, Any]:
        """Run E2E test with a specific config"""
        print(f"\n  Testing E2E: {config_file}")
        print("  " + "-" * 50)

        start_time = time.time()

        try:
            # Add project root to Python path
            sys.path.insert(0, str(PROJECT_ROOT))

            from integrations.notification_manager import create_notification_manager
            from audit.etl_audit import ETLRunAudit, RunStatus

            # Load config
            config_path = CONFIG_DIR / config_file
            with open(config_path, 'r') as f:
                config = json.load(f)

            job_name = config.get('job_name', 'unknown')

            # Test 1: Create notification manager
            manager = create_notification_manager(config)
            status = manager.get_status()

            # Test 2: Create audit record
            run_id = f"{job_name}-e2e-{int(time.time() * 1000)}"
            audit = ETLRunAudit(
                run_id=run_id,
                job_name=job_name,
                status=RunStatus.RUNNING.value,
                started_at=datetime.now().isoformat(),
                platform=config.get('platform', 'auto') if isinstance(config.get('platform'), str) else config.get('platform', {}).get('primary', 'auto'),
                config_hash="e2e_test",
                source_type=config.get('source', {}).get('type', 'multi_source'),
                target_type=config.get('target', {}).get('type', 'unknown')
            )

            # Simulate completion
            audit.status = RunStatus.SUCCEEDED.value
            audit.completed_at = datetime.now().isoformat()
            audit.rows_read = 10000
            audit.rows_written = 9900
            audit.dq_score = 0.95

            duration = time.time() - start_time

            print(f"    Job: {job_name}")
            print(f"    Notifications: slack={status['channels']['slack']['enabled']}, "
                  f"teams={status['channels']['teams']['enabled']}, "
                  f"email={status['channels']['email']['enabled']}")
            print(f"    Audit: {audit.status}")
            print(f"    [PASS]")

            return {
                'config': config_file,
                'job_name': job_name,
                'passed': True,
                'duration': duration,
                'notifications': {
                    'slack': status['channels']['slack']['enabled'],
                    'teams': status['channels']['teams']['enabled'],
                    'email': status['channels']['email']['enabled']
                }
            }

        except Exception as e:
            duration = time.time() - start_time
            print(f"    [FAIL] {e}")
            import traceback
            traceback.print_exc()

            return {
                'config': config_file,
                'passed': False,
                'duration': duration,
                'error': str(e)
            }

    def run_all_e2e_tests(self) -> List[Dict[str, Any]]:
        """Run E2E tests with all config files"""
        print("\n" + "=" * 70)
        print("   END-TO-END TESTS")
        print("=" * 70)

        results = []

        # Get all JSON config files
        config_files = sorted([f.name for f in CONFIG_DIR.glob('*.json')])

        if not config_files:
            print(f"No config files found in {CONFIG_DIR}")
            return results

        print(f"\nFound {len(config_files)} config files")

        # Run Simple configs first
        simple_configs = [f for f in config_files if f.startswith('simple_')]
        complex_configs = [f for f in config_files if f.startswith('complex_')]

        print("\n[SIMPLE CONFIGURATIONS]")
        for config_file in simple_configs:
            result = self.run_e2e_test(config_file)
            results.append(result)

        print("\n[COMPLEX CONFIGURATIONS]")
        for config_file in complex_configs:
            result = self.run_e2e_test(config_file)
            results.append(result)

        return results

    def run_all(self, component_only: bool = False, e2e_only: bool = False):
        """Run all tests"""
        print("\n" + "=" * 70)
        print("   ETL FRAMEWORK - COMPLETE TEST SUITE")
        print("=" * 70)
        print(f"   Started: {self.results['start_time']}")
        print("=" * 70)

        # Run component tests
        if not e2e_only:
            self.results['component_tests'] = self.run_all_component_tests()

        # Run E2E tests
        if not component_only:
            self.results['e2e_tests'] = self.run_all_e2e_tests()

        # Calculate summary
        self.results['end_time'] = datetime.now().isoformat()

        all_tests = self.results['component_tests'] + self.results['e2e_tests']
        self.results['summary']['total'] = len(all_tests)
        self.results['summary']['passed'] = sum(1 for t in all_tests if t.get('passed', False))
        self.results['summary']['failed'] = self.results['summary']['total'] - self.results['summary']['passed']

        # Print summary
        self._print_summary()

        return self.results['summary']['failed'] == 0

    def _print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 70)
        print("   FINAL SUMMARY")
        print("=" * 70)

        # Component tests
        if self.results['component_tests']:
            print("\nComponent Tests:")
            for test in self.results['component_tests']:
                status = "[PASS]" if test['passed'] else "[FAIL]"
                print(f"  {status} {test['name']} ({test['duration']:.2f}s)")

        # E2E tests
        if self.results['e2e_tests']:
            print("\nE2E Tests:")
            for test in self.results['e2e_tests']:
                status = "[PASS]" if test['passed'] else "[FAIL]"
                print(f"  {status} {test['config']} ({test['duration']:.2f}s)")

        # Overall
        print("\n" + "-" * 70)
        total = self.results['summary']['total']
        passed = self.results['summary']['passed']
        failed = self.results['summary']['failed']

        print(f"  Total Tests:  {total}")
        print(f"  Passed:       {passed} ({100*passed/total:.1f}%)" if total > 0 else "  Passed: 0")
        print(f"  Failed:       {failed}")
        print("-" * 70)

        if failed == 0:
            print("\n  *** ALL TESTS PASSED ***")
        else:
            print(f"\n  *** {failed} TEST(S) FAILED ***")

        print()

    def generate_report(self, output_file: str):
        """Generate JSON report"""
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nReport written to: {output_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='ETL Framework Complete Test Suite')
    parser.add_argument('--component-only', action='store_true', help='Run only component tests')
    parser.add_argument('--e2e-only', action='store_true', help='Run only E2E tests')
    parser.add_argument('--report', type=str, help='Output JSON report file')

    args = parser.parse_args()

    runner = TestRunner()
    success = runner.run_all(
        component_only=args.component_only,
        e2e_only=args.e2e_only
    )

    if args.report:
        runner.generate_report(args.report)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
