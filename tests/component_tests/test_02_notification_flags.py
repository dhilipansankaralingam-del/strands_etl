#!/usr/bin/env python3
"""
Component Test 2: Notification Y/N Flag Parsing
Tests notification configuration with Y/N flags from JSON
"""

import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from integrations.notification_manager import NotificationConfig

# Test data with various Y/N flag formats
TEST_CASES = [
    {
        "name": "All Y flags (uppercase)",
        "config": {
            "notifications": {
                "enabled": "Y",
                "slack": {"enabled": "Y", "webhook_url": "test"},
                "teams": {"enabled": "Y", "webhook_url": "test"},
                "email": {"enabled": "Y", "sender": "test@test.com", "recipients": ["a@b.com"]},
                "preferences": {
                    "on_start": "Y",
                    "on_success": "Y",
                    "on_failure": "Y"
                }
            }
        },
        "expected": {
            "notifications_enabled": True,
            "slack_enabled": True,
            "teams_enabled": True,
            "email_enabled": True
        }
    },
    {
        "name": "All N flags (uppercase)",
        "config": {
            "notifications": {
                "enabled": "N",
                "slack": {"enabled": "N"},
                "teams": {"enabled": "N"},
                "email": {"enabled": "N"},
                "preferences": {"on_start": "N", "on_success": "N", "on_failure": "N"}
            }
        },
        "expected": {
            "notifications_enabled": False,
            "slack_enabled": False,
            "teams_enabled": False,
            "email_enabled": False
        }
    },
    {
        "name": "Mixed Y/N flags (lowercase)",
        "config": {
            "notifications": {
                "enabled": "y",
                "slack": {"enabled": "y", "webhook_url": "test"},
                "teams": {"enabled": "n"},
                "email": {"enabled": "y", "sender": "test@test.com", "recipients": ["a@b.com"]},
                "preferences": {"on_start": "n", "on_success": "y", "on_failure": "y"}
            }
        },
        "expected": {
            "notifications_enabled": True,
            "slack_enabled": True,
            "teams_enabled": False,
            "email_enabled": True
        }
    },
    {
        "name": "YES/NO format",
        "config": {
            "notifications": {
                "enabled": "YES",
                "slack": {"enabled": "NO"},
                "teams": {"enabled": "YES", "webhook_url": "test"},
                "email": {"enabled": "NO"}
            }
        },
        "expected": {
            "notifications_enabled": True,
            "slack_enabled": False,
            "teams_enabled": True,
            "email_enabled": False
        }
    },
    {
        "name": "Boolean format",
        "config": {
            "notifications": {
                "enabled": True,
                "slack": {"enabled": True, "webhook_url": "test"},
                "teams": {"enabled": False},
                "email": {"enabled": True, "sender": "test@test.com", "recipients": ["a@b.com"]}
            }
        },
        "expected": {
            "notifications_enabled": True,
            "slack_enabled": True,
            "teams_enabled": False,
            "email_enabled": True
        }
    },
    {
        "name": "Numeric 1/0 format",
        "config": {
            "notifications": {
                "enabled": "1",
                "slack": {"enabled": "0"},
                "teams": {"enabled": "1", "webhook_url": "test"},
                "email": {"enabled": "0"}
            }
        },
        "expected": {
            "notifications_enabled": True,
            "slack_enabled": False,
            "teams_enabled": True,
            "email_enabled": False
        }
    },
    {
        "name": "Empty/Missing notifications",
        "config": {},
        "expected": {
            "notifications_enabled": True,  # Default
            "slack_enabled": False,
            "teams_enabled": False,
            "email_enabled": False
        }
    }
]


def test_notification_flags(test_case: dict, verbose: bool = True) -> bool:
    """Test notification flag parsing"""
    name = test_case["name"]
    config = test_case["config"]
    expected = test_case["expected"]

    print(f"\n  Testing: {name}")
    print("  " + "-" * 50)

    try:
        notif_config = NotificationConfig.from_json(config)

        # Check each expected value
        all_pass = True
        for key, expected_value in expected.items():
            actual_value = getattr(notif_config, key)
            status = "PASS" if actual_value == expected_value else "FAIL"

            if verbose:
                print(f"    {key}: {actual_value} (expected: {expected_value}) [{status}]")

            if actual_value != expected_value:
                all_pass = False

        if all_pass:
            print("    [PASS] All flags parsed correctly")
        else:
            print("    [FAIL] Some flags did not match expected values")

        return all_pass

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_with_config_files():
    """Test with actual config files"""
    print("\n[TESTING WITH CONFIG FILES]")

    config_tests = [
        ("simple_s3_to_s3.json", {"slack": False, "teams": False, "email": False}),
        ("simple_glue_catalog.json", {"slack": True, "teams": False, "email": False}),
        ("simple_teams_only.json", {"slack": False, "teams": True, "email": False}),
        ("simple_email_alerts.json", {"slack": False, "teams": False, "email": True}),
        ("simple_all_notifications.json", {"slack": True, "teams": True, "email": True}),
        ("complex_full_pipeline.json", {"slack": True, "teams": True, "email": True}),
    ]

    results = {"passed": 0, "failed": 0}

    for config_file, expected in config_tests:
        print(f"\n  Testing: {config_file}")
        print("  " + "-" * 50)

        try:
            config_path = PROJECT_ROOT / 'test_configs' / config_file
            with open(config_path, 'r') as f:
                config = json.load(f)

            notif_config = NotificationConfig.from_json(config)

            all_pass = True
            print(f"    Slack: {notif_config.slack_enabled} (expected: {expected['slack']})")
            print(f"    Teams: {notif_config.teams_enabled} (expected: {expected['teams']})")
            print(f"    Email: {notif_config.email_enabled} (expected: {expected['email']})")

            if notif_config.slack_enabled != expected['slack']:
                all_pass = False
            if notif_config.teams_enabled != expected['teams']:
                all_pass = False
            if notif_config.email_enabled != expected['email']:
                all_pass = False

            if all_pass:
                print("    [PASS]")
                results["passed"] += 1
            else:
                print("    [FAIL]")
                results["failed"] += 1

        except Exception as e:
            print(f"    [FAIL] Exception: {e}")
            results["failed"] += 1

    return results


def run_tests():
    """Run all notification flag tests"""
    print("=" * 60)
    print("COMPONENT TEST 2: Notification Y/N Flag Parsing")
    print("=" * 60)

    results = {"passed": 0, "failed": 0}

    # Test Y/N flag variations
    print("\n[Y/N FLAG VARIATIONS]")
    for test_case in TEST_CASES:
        if test_notification_flags(test_case):
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Test with actual config files
    file_results = test_with_config_files()
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
        print("\n  [SUCCESS] All notification flag tests passed!")
        return True
    else:
        print(f"\n  [FAILURE] {results['failed']} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
