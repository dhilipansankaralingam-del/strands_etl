#!/usr/bin/env python3
"""
Convert Glue PySpark script to EMR or EKS format.

Usage:
    python scripts/convert_to_eks.py -c config.json [--platform eks|emr]
    python scripts/convert_to_eks.py --script scripts/pyspark/my_job.py --platform eks
"""

import sys
import json
import argparse
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.strands.agents.code_conversion_agent import StrandsCodeConversionAgent


def convert_script(script_path: str, target_platform: str = 'eks', output_dir: str = 'converted') -> str:
    """Convert a single script to target platform."""

    script_file = Path(script_path)
    if not script_file.exists():
        print(f"Error: Script not found: {script_path}")
        return None

    original_code = script_file.read_text()

    # Create agent (minimal config)
    agent = StrandsCodeConversionAgent({})

    # Convert
    if target_platform == 'emr':
        converted_code, changes = agent._convert_glue_to_emr(original_code)
    elif target_platform == 'eks':
        converted_code, changes = agent._convert_glue_to_eks(original_code)
    else:
        print(f"Error: Unknown platform: {target_platform}")
        return None

    # Save converted script
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / f"{script_file.stem}_{target_platform}.py"
    output_file.write_text(converted_code)

    print(f"\n{'='*60}")
    print(f" Code Conversion: Glue → {target_platform.upper()}")
    print(f"{'='*60}")
    print(f" Input:  {script_path}")
    print(f" Output: {output_file}")
    print(f" Changes: {len(changes)}")
    print(f"{'='*60}")

    for i, change in enumerate(changes, 1):
        print(f"  {i}. {change.get('type', 'unknown')}: {change}")

    print(f"\n✓ Converted script saved to: {output_file}")
    return str(output_file)


def convert_from_config(config_path: str, target_platform: str = 'eks') -> str:
    """Convert script specified in config file."""

    with open(config_path) as f:
        config = json.load(f)

    script_path = config.get('script', {}).get('local_path')
    if not script_path:
        print("Error: No script.local_path in config")
        return None

    output_dir = config.get('platform', {}).get('code_conversion', {}).get(
        'output_converted_script_path', 'converted'
    )

    return convert_script(script_path, target_platform, output_dir)


def main():
    parser = argparse.ArgumentParser(description='Convert Glue PySpark to EMR/EKS format')
    parser.add_argument('-c', '--config', help='Config JSON file')
    parser.add_argument('-s', '--script', help='Direct path to PySpark script')
    parser.add_argument('-p', '--platform', default='eks', choices=['emr', 'eks'],
                        help='Target platform (default: eks)')
    parser.add_argument('-o', '--output', default='converted',
                        help='Output directory (default: converted)')

    args = parser.parse_args()

    if not args.config and not args.script:
        parser.print_help()
        print("\nError: Provide either --config or --script")
        sys.exit(1)

    if args.script:
        convert_script(args.script, args.platform, args.output)
    else:
        convert_from_config(args.config, args.platform)


if __name__ == '__main__':
    main()
