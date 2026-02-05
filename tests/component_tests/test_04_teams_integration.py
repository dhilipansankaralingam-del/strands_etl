#!/usr/bin/env python3
"""
Component Test 4: Teams Integration
Tests Microsoft Teams integration configuration and message building
"""

import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from integrations.teams_integration import (
    TeamsIntegration,
    TeamsConfig,
    TeamsIntegrationFactory,
    TeamsMessage,
    AlertSeverity,
    TeamsMessageType
)


def test_teams_config_creation():
    """Test Teams config creation"""
    print("\n  Testing: Teams Config Creation")
    print("  " + "-" * 50)

    try:
        config = TeamsConfig(
            webhook_url="https://test.webhook.office.com/test",
            channel_name="test-channel",
            enabled=True,
            notify_on_start=True,
            notify_on_success=True,
            notify_on_failure=True
        )

        print(f"    webhook_url: {config.webhook_url[:30]}...")
        print(f"    channel_name: {config.channel_name}")
        print(f"    enabled: {config.enabled}")
        print(f"    notify_on_start: {config.notify_on_start}")
        print(f"    notify_on_success: {config.notify_on_success}")
        print(f"    notify_on_failure: {config.notify_on_failure}")

        print("    [PASS] Teams config created")
        return True

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_teams_message_creation():
    """Test Teams message creation"""
    print("\n  Testing: Teams Message Creation")
    print("  " + "-" * 50)

    try:
        message = TeamsMessage(
            title="Test ETL Job Completed",
            text="Job test_job completed successfully.",
            severity=AlertSeverity.SUCCESS,
            facts={
                "Run ID": "test-12345",
                "Duration": "120 seconds",
                "Rows Processed": "10,000"
            },
            actions=[
                {"title": "View Logs", "url": "https://example.com/logs"}
            ]
        )

        print(f"    Title: {message.title}")
        print(f"    Text: {message.text}")
        print(f"    Severity: {message.severity.value}")
        print(f"    Facts: {len(message.facts)} items")
        print(f"    Actions: {len(message.actions)} items")

        print("    [PASS] Teams message created")
        return True

    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_adaptive_card_building():
    """Test adaptive card payload building"""
    print("\n  Testing: Adaptive Card Building")
    print("  " + "-" * 50)

    try:
        config = TeamsConfig(
            webhook_url="https://test.webhook.office.com/test",
            channel_name="test-channel",
            enabled=True
        )
        teams = TeamsIntegration(config)

        message = TeamsMessage(
            title="ETL Job Failed",
            text="Job sales_pipeline failed with error.",
            severity=AlertSeverity.ERROR,
            facts={
                "Run ID": "run-xyz",
                "Error": "OutOfMemoryError",
                "Duration": "45 minutes"
            },
            mentions=["oncall@company.com"]
        )

        card = teams._build_adaptive_card(message)

        # Validate card structure
        assert "attachments" in card, "Missing attachments"
        assert len(card["attachments"]) > 0, "Empty attachments"
        assert card["attachments"][0]["contentType"] == "application/vnd.microsoft.card.adaptive"

        content = card["attachments"][0]["content"]
        assert content["type"] == "AdaptiveCard", "Wrong card type"
        assert "body" in content, "Missing body"

        print(f"    Card type: {card['type']}")
        print(f"    Attachment contentType: {card['attachments'][0]['contentType']}")
        print(f"    Body elements: {len(content['body'])}")

        print("    [PASS] Adaptive card built correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_simple_message_building():
    """Test simple message card building"""
    print("\n  Testing: Simple Message Card Building")
    print("  " + "-" * 50)

    try:
        config = TeamsConfig(
            webhook_url="https://test.webhook.office.com/test",
            enabled=True
        )
        teams = TeamsIntegration(config)

        message = TeamsMessage(
            title="Daily Summary",
            text="All jobs completed successfully",
            severity=AlertSeverity.SUCCESS,
            facts={"Total Jobs": "15", "Success Rate": "100%"}
        )

        card = teams._build_simple_message(message)

        assert "@type" in card, "Missing @type"
        assert card["@type"] == "MessageCard", "Wrong card type"
        assert "sections" in card, "Missing sections"
        assert "themeColor" in card, "Missing themeColor"

        print(f"    Card @type: {card['@type']}")
        print(f"    Theme color: {card['themeColor']}")
        print(f"    Sections: {len(card['sections'])}")

        print("    [PASS] Simple message card built correctly")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_factory_from_json():
    """Test Teams factory with JSON config"""
    print("\n  Testing: Factory from JSON Config")
    print("  " + "-" * 50)

    try:
        # Test with Teams enabled
        config_enabled = {
            "teams": {
                "enabled": "Y",
                "webhook_url": "https://test.webhook.office.com/test",
                "channel_name": "test-channel"
            }
        }

        teams = TeamsIntegrationFactory.from_json_flags(config_enabled)
        assert teams is not None, "Teams should be created when enabled=Y"
        print(f"    Enabled config: Teams created = {teams is not None}")

        # Test with Teams disabled
        config_disabled = {
            "teams": {
                "enabled": "N"
            }
        }

        teams_disabled = TeamsIntegrationFactory.from_json_flags(config_disabled)
        assert teams_disabled is None, "Teams should be None when enabled=N"
        print(f"    Disabled config: Teams created = {teams_disabled is not None}")

        print("    [PASS] Factory correctly handles Y/N flags")
        return True

    except AssertionError as e:
        print(f"    [FAIL] Assertion: {e}")
        return False
    except Exception as e:
        print(f"    [FAIL] Exception: {e}")
        return False


def test_with_config_files():
    """Test Teams integration with actual config files"""
    print("\n[TESTING WITH CONFIG FILES]")

    config_tests = [
        ("simple_teams_only.json", True),
        ("simple_s3_to_s3.json", False),  # Teams disabled
        ("complex_eks_karpenter.json", True),
        ("complex_full_pipeline.json", True),
    ]

    results = {"passed": 0, "failed": 0}

    for config_file, expected_teams in config_tests:
        print(f"\n  Testing: {config_file}")
        print("  " + "-" * 50)

        try:
            config_path = PROJECT_ROOT / 'test_configs' / config_file
            with open(config_path, 'r') as f:
                config = json.load(f)

            teams = TeamsIntegrationFactory.from_json_flags(config.get('notifications', {}))
            teams_created = teams is not None

            if teams_created == expected_teams:
                print(f"    Teams created: {teams_created} (expected: {expected_teams}) [PASS]")
                if teams:
                    print(f"    Channel: {teams.config.channel_name}")
                results["passed"] += 1
            else:
                print(f"    Teams created: {teams_created} (expected: {expected_teams}) [FAIL]")
                results["failed"] += 1

        except Exception as e:
            print(f"    [FAIL] Exception: {e}")
            results["failed"] += 1

    return results


def run_tests():
    """Run all Teams integration tests"""
    print("=" * 60)
    print("COMPONENT TEST 4: Teams Integration")
    print("=" * 60)

    results = {"passed": 0, "failed": 0}

    # Unit tests
    print("\n[UNIT TESTS]")

    if test_teams_config_creation():
        results["passed"] += 1
    else:
        results["failed"] += 1

    if test_teams_message_creation():
        results["passed"] += 1
    else:
        results["failed"] += 1

    if test_adaptive_card_building():
        results["passed"] += 1
    else:
        results["failed"] += 1

    if test_simple_message_building():
        results["passed"] += 1
    else:
        results["failed"] += 1

    if test_factory_from_json():
        results["passed"] += 1
    else:
        results["failed"] += 1

    # Config file tests
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
        print("\n  [SUCCESS] All Teams integration tests passed!")
        return True
    else:
        print(f"\n  [FAILURE] {results['failed']} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
