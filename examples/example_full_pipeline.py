"""
Example: Full Strands Pipeline Orchestration

This example demonstrates the complete Strands multi-agent ETL orchestration:
- Independent agents working asynchronously
- ML-based platform selection
- Automatic quality assessment
- Pattern-based optimization recommendations
- Continuous learning from executions
"""

import asyncio
import json
from strands import StrandsCoordinator


async def run_single_pipeline():
    """Run a single ETL pipeline through Strands."""
    print("\n" + "=" * 70)
    print("Strands Multi-Agent ETL Pipeline Execution")
    print("=" * 70 + "\n")

    # Create coordinator
    coordinator = StrandsCoordinator()

    try:
        # Initialize all agents
        print("Initializing Strands agents...")
        await coordinator.initialize()
        print("✓ All agents initialized and ready\n")

        # Run pipeline
        print("Starting ETL pipeline orchestration...")
        result = await coordinator.orchestrate_pipeline(
            user_request="Process customer order summary with quality checks and compliance",
            config_path="/home/user/strands_etl/etl_config.json"
        )

        # Display results
        print("\n" + "=" * 70)
        print("Pipeline Execution Results")
        print("=" * 70)

        print(f"\nPipeline ID: {result['pipeline_id']}")
        print(f"Status: {result['status']}")
        print(f"Start Time: {result['start_time']}")
        print(f"End Time: {result.get('end_time', 'N/A')}")

        # Decision Agent Results
        decision = result.get('decision', {})
        if decision:
            print("\n--- Decision Agent ---")
            print(f"Selected Platform: {decision.get('selected_platform')}")
            print(f"Confidence Score: {decision.get('confidence_score', 0):.2f}")
            print(f"Reasoning: {decision.get('reasoning', 'N/A')}")
            print(f"Similar Workloads Analyzed: {decision.get('similar_workloads_count', 0)}")

        # Execution Results
        execution = result.get('execution_result', {})
        if execution:
            print("\n--- Execution Results ---")
            print(f"Platform Used: {execution.get('platform')}")
            print(f"Job Name: {execution.get('job_name', 'N/A')}")
            print(f"Status: {execution.get('status')}")
            if execution.get('error'):
                print(f"Error: {execution.get('error')}")

        # Quality Reports
        quality_reports = result.get('quality_reports', [])
        if quality_reports:
            print("\n--- Quality Assessment ---")
            for report in quality_reports[:1]:  # Show first report
                if 'assessment_type' in report:
                    quality_data = report.get('quality_report', {})
                    print(f"Overall Quality Score: {quality_data.get('overall_score', 0):.2f}")
                    print(f"AI Assessment: {quality_data.get('ai_assessment', 'N/A')[:200]}...")

        # Optimization Recommendations
        opt_reports = result.get('optimization_reports', [])
        if opt_reports:
            print("\n--- Optimization Recommendations ---")
            for report in opt_reports[:1]:  # Show first report
                if 'execution_id' in report:
                    recommendations = report.get('recommendations', [])
                    print(f"Recommendations Generated: {len(recommendations)}")

                    priority_actions = report.get('priority_actions', [])
                    if priority_actions:
                        print("\nTop Priority Actions:")
                        for i, action in enumerate(priority_actions[:3], 1):
                            print(f"{i}. [{action.get('priority', 'medium').upper()}] {action.get('title')}")
                            print(f"   {action.get('description')}")
                            print(f"   Action: {action.get('action')}\n")

                    improvement = report.get('estimated_improvement', {})
                    if improvement:
                        print(f"Estimated Improvement: {improvement.get('estimated_improvement_pct', 0)}%")
                        print(f"Confidence: {improvement.get('confidence', 'medium')}")

        # Learning Updates
        learning_updates = result.get('learning_updates', [])
        if learning_updates:
            print("\n--- Learning Insights ---")
            for update in learning_updates[:1]:  # Show first update
                print(f"Learning Vector ID: {update.get('vector_id', 'N/A')}")
                print(f"Execution Time: {update.get('execution_time', 0):.1f} seconds")
                print(f"Quality Score: {update.get('quality_score', 0):.2f}")
                print(f"Success: {update.get('success', False)}")
                insights = update.get('insights', '')
                if insights:
                    print(f"\nInsights:\n{insights[:300]}...")

        # Agent Metrics
        print("\n" + "=" * 70)
        print("Agent Performance Metrics")
        print("=" * 70)

        metrics = await coordinator.get_agent_metrics()
        for agent_name, agent_metrics in metrics.items():
            print(f"\n{agent_name}:")
            print(f"  State: {agent_metrics.get('state', 'unknown')}")
            print(f"  Messages Processed: {agent_metrics.get('messages_processed', 0)}")
            print(f"  Errors: {agent_metrics.get('errors', 0)}")
            print(f"  Avg Processing Time: {agent_metrics.get('avg_processing_time', 0):.2f}s")

        # Learning Summary
        print("\n" + "=" * 70)
        print("Learning Summary")
        print("=" * 70)

        learning = await coordinator.get_learning_summary()
        if learning.get('total_executions', 0) > 0:
            print(f"\nTotal Executions Learned From: {learning.get('total_executions')}")
            print(f"Successful Executions: {learning.get('successful_executions')}")
            print(f"Success Rate: {learning.get('success_rate', 0):.1%}")
            print(f"Average Execution Time: {learning.get('avg_execution_time_seconds', 0):.1f}s")

            platforms = learning.get('platforms_used', {})
            if platforms:
                print(f"\nPlatform Usage:")
                for platform, count in platforms.items():
                    print(f"  {platform}: {count} executions")
        else:
            print("\nNo learning data available yet")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        print("\n\nShutting down Strands coordinator...")
        await coordinator.shutdown()
        print("✓ Shutdown complete")


async def run_multiple_pipelines():
    """Run multiple pipelines to demonstrate learning."""
    print("\n" + "=" * 70)
    print("Running Multiple Pipelines to Demonstrate Learning")
    print("=" * 70 + "\n")

    coordinator = StrandsCoordinator()

    try:
        await coordinator.initialize()

        # Simulate 3 pipeline runs
        for i in range(3):
            print(f"\n{'='*70}")
            print(f"Pipeline Run {i+1}/3")
            print(f"{'='*70}\n")

            result = await coordinator.orchestrate_pipeline(
                user_request=f"Customer analytics pipeline run {i+1}",
                config_path="/home/user/strands_etl/etl_config.json"
            )

            print(f"Status: {result['status']}")
            print(f"Platform: {result.get('decision', {}).get('selected_platform')}")

            # Brief pause between runs
            if i < 2:
                await asyncio.sleep(5)

        # Show learning summary after multiple runs
        print("\n" + "=" * 70)
        print("Learning Summary After 3 Executions")
        print("=" * 70)

        learning = await coordinator.get_learning_summary()
        print(json.dumps(learning, indent=2))

    finally:
        await coordinator.shutdown()


async def main():
    """Main entry point."""
    import sys

    print("Strands Multi-Agent ETL Framework")
    print("Choose example:")
    print("1. Single pipeline execution (default)")
    print("2. Multiple pipelines to demonstrate learning")

    # For demo, just run single pipeline
    # In real usage, user would select

    await run_single_pipeline()

    # Uncomment to run multiple pipelines demo:
    # await run_multiple_pipelines()


if __name__ == '__main__':
    asyncio.run(main())
