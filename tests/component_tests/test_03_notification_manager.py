#!/usr/bin/env python3
"""
Component Test 3: Notification Manager
Tests the unified notification manager creation and status
"""

import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from integrations.notification_manager import (
    NotificationManager,
    NotificationConfig,
    create_notification_manager
)


def test_manager_creation(config_file: str, verbose: bool = True) -> bool:
    """Test notification manager creation"""
    print(f"\n  Testing: {config_file}")
    print("  " + "-" * 50)

    try:
        config_path = PROJECT_ROOT / 'test_configs' / config_file
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Create manager using factory
        manager = create_notification_manager(config)

        # Get status
        status = manager.get_status()

        if verbose:
            print(f"    Master Enabled: {status['notifications_enabled']}")
            print(f"    Channels:")
            print(f"      - Slack: enabled={status['channels']['slack']['enabled']}, "
                  f"configured={status['channels']['slack']['configured']}")
            print(f"      - Teams: enabled={status['channels']['teams']['enabled']}, "
                  f"configured={status['channels']['teams']['configured']}")
            print(f"      - Email: enabled={status['channels']['email']['enabled']}, "
                  f"configured={status['channels']['email']['configured']}")
            print(f"    Preferences:")
            print(f"      - on_success: {status['preferences']['on_success']}")
            print(f"      - on_failure: {status['preferences']['on_failure']}")

        print("    [PASS] Manager created successfully")
        return True

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_manager_status_structure():
    """Test that manager status has correct structure"""
    print("\n  Testing: Manager Status Structure")
    print("  " + "-" * 50)

    try:
        # Create with minimal config
        manager = create_notification_manager({})
        status = manager.get_status()

        required_keys = ['notifications_enabled', 'channels', 'preferences', 'thresholds']
        channel_keys = ['slack', 'teams', 'email']
        preference_keys = ['on_start', 'on_success', 'on_failure', 'on_warning',
                          'on_dq_failure', 'on_cost_alert', 'on_recommendations']
        threshold_keys = ['dq_score', 'cost_alert_usd', 'duration_alert_minutes']

        # Check top-level keys
        for key in required_keys:
            if key not in status:
                print(f"    [FAIL] Missing key: {key}")
                return False

        # Check channel keys
        for key in channel_keys:
            if key not in status['channels']:
                print(f"    [FAIL] Missing channel: {key}")
                return False

        # Check preference keys
        for key in preference_keys:
            if key not in status['preferences']:
                print(f"    [FAIL] Missing preference: {key}")
                return False

        # Check threshold keys
        for key in threshold_keys:
            if key not in status['thresholds']:
                print(f"    [FAIL] Missing threshold: {key}")
                return False

        print("    [PASS] Status structure is correct")
        return True

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_should_notify_logic():
    """Test notification preference logic"""
    print("\n  Testing: Should Notify Logic")
    print("  " + "-" * 50)

    try:
        # Config with specific preferences
        config = {
            "notifications": {
                "enabled": "Y",
                "preferences": {
                    "on_start": "N",
                    "on_success": "Y",
                    "on_failure": "Y",
                    "on_warning": "N"
                }
            }
        }

        manager = create_notification_manager(config)

        # Test _should_notify method indirectly through get_status
        status = manager.get_status()

        tests = [
            ("on_start", False),
            ("on_success", True),
            ("on_failure", True),
            ("on_warning", False)
        ]

        all_pass = True
        for pref_name, expected in tests:
            actual = status['preferences'][pref_name]
            status_str = "PASS" if actual == expected else "FAIL"
            print(f"    {pref_name}: {actual} (expected: {expected}) [{status_str}]")
            if actual != expected:
                all_pass = False

        if all_pass:
            print("    [PASS] Should notify logic correct")
        return all_pass

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def run_tests():
    """Run all notification manager tests"""
    print("=" * 60)
    print("COMPONENT TEST 3: Notification Manager")
    print("=" * 60)

    results = {"passed": 0, "failed": 0}

    # Test manager status structure
    if test_manager_status_structure():
        results["passed"] += 1
    else:
        results["failed"] += 1

    # Test should notify logic
    if test_should_notify_logic():
        results["passed"] += 1
    else:
        results["failed"] += 1

    # Test with simple configs
    print("\n[SIMPLE CONFIGURATIONS]")
    simple_configs = [
        'simple_s3_to_s3.json',
        'simple_glue_catalog.json',
        'simple_email_alerts.json',
        'simple_teams_only.json',
        'simple_all_notifications.json'
    ]

    for config_file in simple_configs:
        if test_manager_creation(config_file):
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Test with complex configs
    print("\n[COMPLEX CONFIGURATIONS]")
    complex_configs = [
        'complex_full_pipeline.json',
        'complex_streaming_pipeline.json',
        'complex_eks_karpenter.json'
    ]

    for config_file in complex_configs:
        if test_manager_creation(config_file):
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = results["passed"] + results["failed"]
    print(f"  Total:  {total}")
    print(f"  Passed: {results['passed']}")
    print(f"  Failed: {results['failed']}")

    if results["failed"] == 0:
        print("\n  [SUCCESS] All notification manager tests passed!")
        return True
    else:
        print(f"\n  [FAILURE] {results['failed']} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
