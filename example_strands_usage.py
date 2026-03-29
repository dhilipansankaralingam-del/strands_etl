"""
Strands ETL Framework - Example Usage

This script demonstrates how to use the native Strands agentic framework
for ETL orchestration with multiple specialized agents working together.

Run this example:
    python example_strands_usage.py
"""
from strands import Agent
from strands.multiagent import Swarm
from strands_agents.orchestrator.swarm_orchestrator import ETLSwarm
import json


def example_1_single_agent():
    """
    Example 1: Using a single agent (Decision Agent)
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Single Agent - Platform Decision")
    print("=" * 80 + "\n")

    # Import the decision agent tools
    from strands_agents.tools.decision_tools import (
        analyze_workload_characteristics,
        calculate_platform_costs,
        recommend_platform
    )
    from strands_tools import memory

    # Create a Decision Agent
    decision_agent = Agent(
        name="decision_agent",
        system_prompt="You are an expert at analyzing ETL workloads and recommending optimal platforms.",
        tools=[
            analyze_workload_characteristics,
            calculate_platform_costs,
            recommend_platform,
            memory
        ]
    )

    # Use the agent
    response = decision_agent("""
    I need to process 150GB of customer transaction data with complex joins and aggregations.
    I have about 500 files to process. This is a batch job that runs daily.

    Please:
    1. Analyze the workload characteristics
    2. Calculate costs for Glue and EMR
    3. Recommend the best platform
    """)

    print("Agent Response:")
    print(response)


def example_2_multi_agent_swarm():
    """
    Example 2: Using the full multi-agent swarm
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Multi-Agent Swarm - Complete ETL Planning")
    print("=" * 80 + "\n")

    # Create the ETL Swarm
    swarm = ETLSwarm(max_handoffs=20)

    # Define a job request
    job_request = {
        'job_name': 'customer_order_summary',
        'data_volume_gb': 250,
        'file_count': 1000,
        'transformation_complexity': 'complex',
        'query_pattern': 'batch',
        'source_path': 's3://my-bucket/raw/orders/',
        'destination_path': 's3://my-bucket/processed/order_summary/',
        'contains_pii': True,  # Has customer emails and phone numbers
        'script_path': './pyscript/customer_order_summary_glue.py'
    }

    # Process the job with the swarm
    print(f"Processing job: {job_request['job_name']}")
    print(f"Data volume: {job_request['data_volume_gb']} GB")
    print(f"File count: {job_request['file_count']}")
    print("\nSwarm is coordinating agents to create execution plan...\n")

    result = swarm.process_etl_job(job_request)

    print("\nSwarm Result:")
    print(json.dumps(result, indent=2))


def example_3_post_execution_analysis():
    """
    Example 3: Analyzing a completed job execution
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Post-Execution Analysis")
    print("=" * 80 + "\n")

    swarm = ETLSwarm()

    # Historical execution data
    execution_data = {
        'execution_id': 'exec-2024-01-21-abc123',
        'job_name': 'customer_order_summary',
        'platform': 'glue',
        'dpu_count': 10,
        'duration_minutes': 45,
        'cost_usd': 8.50,
        'data_volume_gb': 250,
        'data_read_gb': 250,
        'data_written_gb': 30,
        'success': True,
        'metrics': {
            'cpu_utilization_avg': 45,
            'memory_utilization_avg': 65,
            'memory_mb_avg': 4096,
            'shuffle_read_gb': 100,
            'shuffle_write_gb': 50,
            'output_file_count': 2000
        },
        'quality_score': 0.92,
        'issues_detected': [
            {
                'type': 'multiple_counts',
                'severity': 'high',
                'message': 'Multiple .count() operations detected'
            },
            {
                'type': 'small_files',
                'severity': 'medium',
                'message': '2000 small output files'
            }
        ]
    }

    print(f"Analyzing execution: {execution_data['execution_id']}")
    print(f"Platform: {execution_data['platform']}")
    print(f"Cost: ${execution_data['cost_usd']}")
    print(f"Duration: {execution_data['duration_minutes']} minutes")
    print("\nSwarm analyzing for optimization opportunities...\n")

    result = swarm.analyze_existing_job(execution_data)

    print("\nAnalysis Result:")
    print(json.dumps(result, indent=2))


def example_4_individual_agents():
    """
    Example 4: Using individual agents directly
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Individual Agent Usage")
    print("=" * 80 + "\n")

    from strands_agents.agents.etl_agents import (
        create_quality_agent,
        create_cost_tracking_agent
    )

    # Example 4a: Quality Agent
    print("4a. Quality Agent - Analyzing Data Completeness\n")
    quality_agent = create_quality_agent()

    quality_response = quality_agent("""
    I have a customer dataset with 1,000,000 records.
    Here are the null counts:
    - customer_id: 0 nulls
    - email: 5,000 nulls
    - phone: 12,000 nulls
    - address: 50,000 nulls

    customer_id, email, and phone are required fields.

    Please analyze the data completeness.
    """)

    print("Quality Agent Response:")
    print(quality_response)

    # Example 4b: Cost Tracking Agent
    print("\n\n4b. Cost Tracking Agent - Identifying Optimizations\n")
    cost_agent = create_cost_tracking_agent()

    cost_response = cost_agent("""
    My last ETL job cost $15.50 with the following breakdown:
    - Platform (Glue): $12.00
    - Storage: $2.00
    - Network: $1.00
    - AI: $0.50

    The job processed 200GB of data in 60 minutes using 10 DPUs.
    CPU utilization was only 35% average.
    Memory utilization was 50% average.

    Please identify cost optimization opportunities.
    """)

    print("Cost Agent Response:")
    print(cost_response)


def example_5_agent_handoff():
    """
    Example 5: Demonstrating agent handoff in a Swarm
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Agent Handoff Pattern")
    print("=" * 80 + "\n")

    from strands_agents.agents.etl_agents import (
        create_decision_agent,
        create_learning_agent,
        create_cost_tracking_agent
    )

    # Create a mini-swarm with 3 agents
    agents = [
        create_decision_agent(),
        create_learning_agent(),
        create_cost_tracking_agent()
    ]

    mini_swarm = Swarm(agents=agents, max_handoffs=10)

    response = mini_swarm("""
    I need to process a 500GB dataset with 2000 files.
    The transformation is moderately complex (joins + aggregations).

    Please:
    1. Decision Agent: Recommend a platform
    2. Learning Agent: Check if we've run similar jobs before
    3. Cost Tracking Agent: Provide detailed cost estimates

    Work together to give me a comprehensive recommendation.
    """)

    print("Swarm Response (with handoffs):")
    print(response)


if __name__ == '__main__':
    print("\n")
    print("=" * 80)
    print("STRANDS ETL FRAMEWORK - EXAMPLE USAGE")
    print("=" * 80)
    print("\nThis demonstrates the native Strands agentic framework with")
    print("multiple specialized agents coordinating via Swarm pattern.\n")

    # Run examples
    try:
        example_1_single_agent()
    except Exception as e:
        print(f"Example 1 error: {e}")

    try:
        example_2_multi_agent_swarm()
    except Exception as e:
        print(f"Example 2 error: {e}")

    try:
        example_3_post_execution_analysis()
    except Exception as e:
        print(f"Example 3 error: {e}")

    try:
        example_4_individual_agents()
    except Exception as e:
        print(f"Example 4 error: {e}")

    try:
        example_5_agent_handoff()
    except Exception as e:
        print(f"Example 5 error: {e}")

    print("\n" + "=" * 80)
    print("EXAMPLES COMPLETED")
    print("=" * 80 + "\n")
