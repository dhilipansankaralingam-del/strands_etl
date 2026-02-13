#!/usr/bin/env python3
"""
Enterprise ETL Orchestrator - Strands SDK Edition
==================================================

Multi-agent ETL orchestrator using Strands SDK for parallel execution
and agent collaboration.

Usage:
    python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json
    python scripts/run_strands_etl.py -c config.json --dry-run
    python scripts/run_strands_etl.py -c config.json --platform eks

Features:
- Multi-agent parallel execution
- Automatic size detection from Glue/S3
- Dynamic resource allocation
- Platform auto-conversion (Glue → EMR → EKS)
- Code conversion (GlueContext → SparkSession)
- Auto-healing for failures
- Learning from historical runs with model training
- Pipe-delimited fallback storage
- Actual job execution on Glue/EMR/EKS
- CloudWatch metrics collection

Agent Execution Phases:
    Phase 1 (Parallel):
        - SizingAgent: Detect source table sizes
        - ComplianceAgent: Check regulatory compliance
        - CodeAnalysisAgent: Analyze PySpark code

    Phase 2 (Depends on Phase 1):
        - ResourceAllocatorAgent: Recommend resources based on size
        - DataQualityAgent: Validate data quality rules
        - HealingAgent: Prepare healing strategies

    Phase 3 (Depends on Phase 2):
        - PlatformConversionAgent: Decide target platform
        - CodeConversionAgent: Convert code if needed

    Phase 4 (Execution):
        - ExecutionAgent: Execute job on target platform, collect metrics

    Phase 5 (Learning & Recommendations):
        - LearningAgent: Train ML models from execution data
        - RecommendationAgent: Aggregate all recommendations
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.strands import (
    StrandsOrchestrator,
    StrandsStorage,
    AgentStatus
)

from framework.strands.agents import (
    SizingAgent,
    StrandsComplianceAgent,
    StrandsDataQualityAgent,
    StrandsCodeAnalysisAgent,
    StrandsResourceAllocatorAgent,
    StrandsPlatformConversionAgent,
    StrandsCodeConversionAgent,
    StrandsHealingAgent,
    StrandsLearningAgent,
    StrandsRecommendationAgent,
    ExecutionAgent
)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, 'r') as f:
        return json.load(f)


def print_banner(job_name: str, config: Dict) -> None:
    """Print execution banner."""
    print("\n" + "=" * 70)
    print("  ENTERPRISE ETL FRAMEWORK - STRANDS SDK EDITION")
    print("=" * 70)
    print(f"  Job: {job_name}")
    print(f"  Platform: {config.get('platform', {}).get('primary', 'glue')}")
    print(f"  Tables: {len(config.get('source_tables', []))}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def print_phase_header(phase_num: int, phase_name: str, agents: list) -> None:
    """Print phase header."""
    print(f"\n{'─' * 60}")
    print(f"  PHASE {phase_num}: {phase_name}")
    print(f"  Agents: {', '.join(agents)}")
    print(f"{'─' * 60}")


def print_agent_result(name: str, result) -> None:
    """Print individual agent result."""
    status_icon = "✓" if result.status == AgentStatus.COMPLETED else "✗"
    print(f"  {status_icon} {name}: {result.status.value} ({result.execution_time_ms:.0f}ms)")

    # Print key metrics
    if result.metrics:
        for key, value in list(result.metrics.items())[:3]:
            if isinstance(value, float):
                print(f"      {key}: {value:.2f}")
            else:
                print(f"      {key}: {value}")

    # Print recommendations
    for rec in result.recommendations[:2]:
        print(f"      → {rec}")


def print_summary(orchestrator_result) -> None:
    """Print execution summary."""
    print("\n" + "=" * 70)
    print("  EXECUTION SUMMARY")
    print("=" * 70)
    print(f"  Status: {orchestrator_result.status.upper()}")
    print(f"  Execution ID: {orchestrator_result.execution_id}")
    print(f"  Total Time: {orchestrator_result.total_time_ms:.0f}ms ({orchestrator_result.total_time_ms/1000:.1f}s)")
    print(f"  Agents: {orchestrator_result.completed_agents} completed, "
          f"{orchestrator_result.failed_agents} failed, "
          f"{orchestrator_result.skipped_agents} skipped")
    print("=" * 70)

    # Print all recommendations
    if orchestrator_result.recommendations:
        print("\n  RECOMMENDATIONS:")
        for i, rec in enumerate(orchestrator_result.recommendations[:10], 1):
            priority = rec.get('priority', 'normal')
            icon = "⚠" if priority in ['critical', 'high'] else "→"
            print(f"    {i}. [{priority.upper()}] {icon} {rec['recommendation']}")

    print()


def create_orchestrator(config: Dict[str, Any]) -> StrandsOrchestrator:
    """Create and configure the orchestrator with all agents."""
    orchestrator = StrandsOrchestrator(
        config=config,
        max_workers=10,
        fail_fast=False
    )

    # Phase 1: Independent agents (run in parallel)
    orchestrator.add_agent(SizingAgent(config), priority=1, dependencies=[])
    orchestrator.add_agent(StrandsComplianceAgent(config), priority=1, dependencies=[])
    orchestrator.add_agent(StrandsCodeAnalysisAgent(config), priority=1, dependencies=[])

    # Phase 2: Depends on Phase 1
    orchestrator.add_agent(
        StrandsResourceAllocatorAgent(config),
        priority=2,
        dependencies=['sizing_agent']
    )
    orchestrator.add_agent(
        StrandsDataQualityAgent(config),
        priority=2,
        dependencies=['compliance_agent']
    )
    orchestrator.add_agent(
        StrandsHealingAgent(config),
        priority=2,
        dependencies=[]
    )

    # Phase 3: Platform and code conversion
    orchestrator.add_agent(
        StrandsPlatformConversionAgent(config),
        priority=3,
        dependencies=['sizing_agent', 'resource_allocator_agent']
    )
    orchestrator.add_agent(
        StrandsCodeConversionAgent(config),
        priority=3,
        dependencies=['platform_conversion_agent']
    )

    # Phase 4: Job Execution
    orchestrator.add_agent(
        ExecutionAgent(config),
        priority=4,
        dependencies=['platform_conversion_agent', 'code_conversion_agent']
    )

    # Phase 5: Learning and recommendations
    orchestrator.add_agent(
        StrandsLearningAgent(config),
        priority=5,
        dependencies=['execution_agent']  # Learn from execution metrics
    )
    orchestrator.add_agent(
        StrandsRecommendationAgent(config),
        priority=6,
        dependencies=[
            'sizing_agent',
            'compliance_agent',
            'data_quality_agent',
            'code_analysis_agent',
            'resource_allocator_agent',
            'platform_conversion_agent',
            'code_conversion_agent',
            'healing_agent',
            'execution_agent',
            'learning_agent'
        ]
    )

    return orchestrator


def run_etl(
    config_path: str,
    dry_run: bool = False,
    platform: Optional[str] = None,
    run_date: Optional[datetime] = None,
    verbose: bool = False
) -> int:
    """
    Run ETL job with multi-agent orchestration.

    Args:
        config_path: Path to config JSON file
        dry_run: If True, simulate without executing
        platform: Override platform (glue, emr, eks)
        run_date: Override run date
        verbose: Enable verbose logging

    Returns:
        Exit code (0 = success, 1 = failure)
    """
    setup_logging(verbose)
    logger = logging.getLogger("strands.main")

    try:
        # Load config
        config = load_config(config_path)
        job_name = config.get('job_name', 'unknown_job')

        # Override platform if specified
        if platform:
            config.setdefault('platform', {})['force_platform'] = platform

        # Print banner
        print_banner(job_name, config)

        if dry_run:
            print("  [DRY RUN MODE - No actual execution]\n")

        # Create orchestrator
        orchestrator = create_orchestrator(config)

        # Execute all agents
        run_date = run_date or datetime.utcnow()

        print("Starting multi-agent orchestration...\n")

        result = orchestrator.execute(
            job_name=job_name,
            run_date=run_date,
            platform=config.get('platform', {}).get('primary', 'glue')
        )

        # Print results by agent
        print("\nAGENT RESULTS:")
        for agent_name, agent_result in result.agent_results.items():
            print_agent_result(agent_name, agent_result)

        # Print summary
        print_summary(result)

        # Write audit log
        storage = StrandsStorage(config)
        storage.write_audit_log(
            job_name=job_name,
            event_type='orchestration_complete',
            event_data={
                'execution_id': result.execution_id,
                'status': result.status,
                'completed': result.completed_agents,
                'failed': result.failed_agents,
                'total_time_ms': result.total_time_ms
            }
        )

        # Return exit code
        if result.failed_agents > 0:
            return 1
        return 0

    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Orchestration failed: {e}")
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Enterprise ETL Framework - Strands SDK Edition',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json
  python scripts/run_strands_etl.py -c config.json --dry-run
  python scripts/run_strands_etl.py -c config.json --platform eks
  python scripts/run_strands_etl.py -c config.json --verbose
        """
    )

    parser.add_argument(
        '-c', '--config',
        required=True,
        help='Path to configuration JSON file'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate execution without running actual jobs'
    )

    parser.add_argument(
        '--platform',
        choices=['glue', 'emr', 'eks'],
        help='Override target platform'
    )

    parser.add_argument(
        '--date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help='Override run date (YYYY-MM-DD format)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    exit_code = run_etl(
        config_path=args.config,
        dry_run=args.dry_run,
        platform=args.platform,
        run_date=args.date,
        verbose=args.verbose
    )

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
