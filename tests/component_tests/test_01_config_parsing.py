#!/usr/bin/env python3
"""
Component Test 1: Configuration Parsing
Tests JSON configuration loading and validation
"""

import json
import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Test configurations
SIMPLE_CONFIGS = [
    'simple_s3_to_s3.json',
    'simple_glue_catalog.json',
    'simple_email_alerts.json',
    'simple_teams_only.json',
    'simple_all_notifications.json'
]

COMPLEX_CONFIGS = [
    'complex_full_pipeline.json',
    'complex_streaming_pipeline.json',
    'complex_eks_karpenter.json'
]


def load_config(config_file: str) -> dict:
    """Load a JSON configuration file"""
    config_path = PROJECT_ROOT / 'test_configs' / config_file
    with open(config_path, 'r') as f:
        return json.load(f)


def validate_config(config: dict) -> tuple:
    """Validate configuration structure"""
    errors = []
    warnings = []

    # Required fields
    required = ['job_name', 'source', 'target']
    for field in required:
        if field not in config:
            errors.append(f"Missing required field: {field}")

    # Validate source
    source = config.get('source', {})
    if 'type' not in source and 'sources' not in source:
        errors.append("Source must have 'type' or 'sources'")

    # Validate target
    target = config.get('target', {})
    if 'type' not in target:
        errors.append("Target must have 'type'")

    # Validate notifications (if present)
    notifications = config.get('notifications', {})
    if notifications:
        enabled = notifications.get('enabled', 'N')
        if str(enabled).upper() not in ('Y', 'N', 'YES', 'NO', 'TRUE', 'FALSE', '1', '0'):
            warnings.append(f"Notification enabled flag unusual value: {enabled}")

    return errors, warnings


def test_config_parsing(config_file: str, verbose: bool = True) -> bool:
    """Test single configuration file parsing"""
    print(f"\n  Testing: {config_file}")
    print("  " + "-" * 50)

    try:
        # Load config
        config = load_config(config_file)

        if verbose:
            print(f"    Job Name: {config.get('job_name', 'N/A')}")
            print(f"    Source Type: {config.get('source', {}).get('type', 'multi_source')}")
            print(f"    Target Type: {config.get('target', {}).get('type', 'N/A')}")

        # Validate
        errors, warnings = validate_config(config)

        if errors:
            for err in errors:
                print(f"    [ERROR] {err}")
            return False

        if warnings:
            for warn in warnings:
                print(f"    [WARN] {warn}")

        print("    [PASS] Configuration valid")
        return True

    except json.JSONDecodeError as e:
        print(f"    [FAIL] JSON parse error: {e}")
        return False
    except FileNotFoundError:
        print(f"    [FAIL] File not found")
        return False
    except Exception as e:
        print(f"    [FAIL] Unexpected error: {e}")
        return False


def run_tests():
    """Run all configuration parsing tests"""
    print("=" * 60)
    print("COMPONENT TEST 1: Configuration Parsing")
    print("=" * 60)

    results = {"passed": 0, "failed": 0}

    # Test Simple Configs
    print("\n[SIMPLE CONFIGURATIONS]")
    for config_file in SIMPLE_CONFIGS:
        if test_config_parsing(config_file):
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Test Complex Configs
    print("\n[COMPLEX CONFIGURATIONS]")
    for config_file in COMPLEX_CONFIGS:
        if test_config_parsing(config_file):
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
        print("\n  [SUCCESS] All configuration parsing tests passed!")
        return True
    else:
        print(f"\n  [FAILURE] {results['failed']} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
