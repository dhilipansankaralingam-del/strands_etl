#!/usr/bin/env python3
"""
Strands SDK ETL Runner
======================

Run enterprise ETL jobs using Strands SDK multi-agent orchestration.

Usage:
    # Basic execution (dry-run by default)
    python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json

    # Execute actual jobs (requires AWS credentials)
    python scripts/run_strands_swarm.py -c config.json --execute

    # Use specific model
    python scripts/run_strands_swarm.py -c config.json --model us.anthropic.claude-sonnet-4-20250514-v1:0

    # Run single agent
    python scripts/run_strands_swarm.py -c config.json --agent sizing_agent --task "Analyze table sizes"

    # List available agents
    python scripts/run_strands_swarm.py --list-agents

Prerequisites:
    1. Install strands-agents: pip install strands-agents strands-agents-tools
    2. Configure AWS credentials with Bedrock access
    3. Set AWS_REGION environment variable (default: us-east-1)

Examples:
    # Run with enterprise sales config
    python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json

    # Quick sizing analysis only
    python scripts/run_strands_swarm.py -c config.json --quick-analysis

    # Customize agent prompt
    python scripts/run_strands_swarm.py -c config.json \\
        --customize sizing_agent \\
        --prompt "Focus only on Delta Lake tables"
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strands_sdk import ETLSwarm, ETLConfig
from strands_sdk.agents import AGENT_PROMPTS, create_agent
from strands_sdk.tools.aws_tools import set_dry_run_mode


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def print_banner(config: ETLConfig, dry_run: bool):
    """Print execution banner."""
    print("\n" + "=" * 70)
    print("  STRANDS SDK ETL FRAMEWORK")
    print("  Multi-Agent LLM-Powered Orchestration")
    print("=" * 70)
    print(f"  Job: {config.job_name}")
    print(f"  Tables: {len(config.source_tables)}")
    print(f"  Platform: {config.platform.get('primary', 'glue')}")
    print(f"  Mode: {'DRY-RUN (simulated)' if dry_run else 'EXECUTE (live)'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def list_agents():
    """List all available agents."""
    print("\n" + "=" * 70)
    print("  AVAILABLE AGENTS")
    print("=" * 70 + "\n")

    for name, prompt in AGENT_PROMPTS.items():
        # Get first line of prompt as description
        first_line = prompt.strip().split('\n')[0]
        print(f"  - {name}")
        print(f"    {first_line[:60]}...")
        print()


def run_single_agent(
    config_path: str,
    agent_name: str,
    task: str,
    model_id: str,
    dry_run: bool
):
    """Run a single agent with a task."""
    set_dry_run_mode(dry_run)

    config = ETLConfig.from_file(config_path)

    print(f"\nRunning agent: {agent_name}")
    print(f"Task: {task[:100]}...")
    print("-" * 50)

    agent = create_agent(agent_name, model_id=model_id)
    result = agent(task)

    print("\nAgent Response:")
    print("-" * 50)
    print(result)


def run_swarm(
    config_path: str,
    model_id: str,
    dry_run: bool,
    custom_prompts: dict = None
):
    """Run the full ETL swarm."""
    config = ETLConfig.from_file(config_path)
    print_banner(config, dry_run)

    print("Initializing Strands SDK Swarm...\n")

    swarm = ETLSwarm(
        config=config,
        model_id=model_id,
        dry_run=dry_run,
        custom_prompts=custom_prompts
    )

    print(f"Agents created: {', '.join(swarm.list_agents())}\n")
    print("Starting multi-agent orchestration...\n")
    print("-" * 70)

    result = swarm.run()

    print("-" * 70)
    print("\n" + "=" * 70)
    print("  EXECUTION SUMMARY")
    print("=" * 70)
    print(f"  Status: {result.get('status', 'unknown').upper()}")
    print(f"  Execution ID: {result.get('execution_id', 'N/A')}")
    print(f"  Duration: {result.get('duration_seconds', 0):.1f} seconds")
    print(f"  Agents: {result.get('agent_count', 0)}")
    print("=" * 70)

    if result.get('swarm_response'):
        print("\n  SWARM RESPONSE:")
        print("-" * 70)
        # Print first 2000 chars of response
        response = result['swarm_response'][:2000]
        print(response)
        if len(result['swarm_response']) > 2000:
            print("\n  ... [truncated]")

    return result


def main():
    parser = argparse.ArgumentParser(
        description='Strands SDK ETL Runner - Multi-Agent Orchestration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with enterprise sales config (dry-run)
  python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json

  # Execute actual jobs (requires AWS credentials)
  python scripts/run_strands_swarm.py -c config.json --execute

  # Run single agent
  python scripts/run_strands_swarm.py -c config.json --agent sizing_agent --task "Analyze sizes"

  # List agents
  python scripts/run_strands_swarm.py --list-agents
        """
    )

    parser.add_argument(
        '-c', '--config',
        help='Path to configuration JSON file'
    )

    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute actual AWS operations (default: dry-run)'
    )

    parser.add_argument(
        '--model',
        default='us.anthropic.claude-sonnet-4-20250514-v1:0',
        help='Bedrock model ID (default: Claude Sonnet)'
    )

    parser.add_argument(
        '--agent',
        help='Run a single agent instead of full swarm'
    )

    parser.add_argument(
        '--task',
        help='Task description for single agent mode'
    )

    parser.add_argument(
        '--list-agents',
        action='store_true',
        help='List all available agents'
    )

    parser.add_argument(
        '--customize',
        help='Agent name to customize'
    )

    parser.add_argument(
        '--prompt',
        help='Custom prompt for the agent'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # List agents
    if args.list_agents:
        list_agents()
        return

    # Require config for other operations
    if not args.config:
        parser.error("--config is required unless using --list-agents")

    # Validate config exists
    if not Path(args.config).exists():
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    dry_run = not args.execute

    # Single agent mode
    if args.agent:
        if not args.task:
            args.task = f"Analyze the ETL job configuration and provide recommendations."

        run_single_agent(
            config_path=args.config,
            agent_name=args.agent,
            task=args.task,
            model_id=args.model,
            dry_run=dry_run
        )
        return

    # Custom prompts
    custom_prompts = {}
    if args.customize and args.prompt:
        custom_prompts[args.customize] = args.prompt

    # Run full swarm
    result = run_swarm(
        config_path=args.config,
        model_id=args.model,
        dry_run=dry_run,
        custom_prompts=custom_prompts
    )

    # Exit code based on status
    sys.exit(0 if result.get('status') == 'completed' else 1)


if __name__ == '__main__':
    main()
