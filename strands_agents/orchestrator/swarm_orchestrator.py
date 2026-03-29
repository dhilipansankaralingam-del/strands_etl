"""
Strands ETL Swarm Orchestrator

This module coordinates multiple specialized agents using the Strands Swarm pattern.
The swarm enables autonomous multi-agent collaboration where agents can hand off
tasks to each other and work together to solve complex ETL problems.

Example Usage:
    from orchestrator.swarm_orchestrator import ETLSwarm

    # Create the swarm
    swarm = ETLSwarm()

    # Process an ETL job
    result = swarm.process_etl_job({
        'job_name': 'customer_order_summary',
        'data_volume_gb': 250,
        'file_count': 1000,
        'transformation_complexity': 'complex',
        'query_pattern': 'batch'
    })
"""
from strands.multiagent import Swarm
from agents.etl_agents import create_all_agents
from typing import Dict, Any
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLSwarm:
    """
    ETL Swarm Orchestrator - Coordinates specialized agents for ETL workflows.

    The swarm manages 6 specialized agents:
    - Decision Agent: Platform selection and workload analysis
    - Quality Agent: Data quality validation
    - Optimization Agent: Performance tuning
    - Learning Agent: Historical pattern analysis
    - Compliance Agent: Regulatory compliance (GDPR/HIPAA)
    - Cost Tracking Agent: Cost analysis and optimization
    """

    def __init__(self, max_handoffs: int = 20):
        """
        Initialize the ETL Swarm with all specialized agents.

        Args:
            max_handoffs: Maximum number of agent handoffs before stopping (default: 20)
        """
        logger.info("Initializing ETL Swarm with specialized agents...")

        # Create all agents
        self.agents_dict = create_all_agents()
        self.agents = list(self.agents_dict.values())

        # Create the Swarm
        self.swarm = Swarm(
            agents=self.agents,
            max_handoffs=max_handoffs,
            execution_timeout=900.0,  # 15 minutes total
            node_timeout=300.0  # 5 minutes per agent
        )

        logger.info(f"ETL Swarm initialized with {len(self.agents)} agents")

    def process_etl_job(self, job_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a complete ETL job using the agent swarm.

        This is the main entry point for ETL job processing. The swarm will:
        1. Analyze the workload (Decision Agent)
        2. Check data quality (Quality Agent)
        3. Ensure compliance (Compliance Agent)
        4. Learn from history (Learning Agent)
        5. Optimize performance (Optimization Agent)
        6. Track costs (Cost Tracking Agent)

        Agents will autonomously hand off to each other as needed.

        Args:
            job_request: Dictionary containing:
                - job_name: Name of the ETL job
                - data_volume_gb: Size of data in GB
                - file_count: Number of input files
                - transformation_complexity: 'simple', 'moderate', or 'complex'
                - query_pattern: 'batch', 'streaming', or 'interactive'
                - (optional) source_path: S3 path to source data
                - (optional) destination_path: S3 path for output
                - (optional) script_path: Path to PySpark script

        Returns:
            Complete execution plan with recommendations from all agents
        """
        logger.info(f"Processing ETL job: {job_request.get('job_name', 'unnamed')}")

        # Create a comprehensive prompt for the swarm
        prompt = self._create_job_prompt(job_request)

        # Execute the swarm
        logger.info("Starting swarm execution...")
        result = self.swarm(prompt)

        logger.info("Swarm execution completed")
        return self._parse_swarm_result(result, job_request)

    def analyze_existing_job(self, execution_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze an existing job execution to provide recommendations.

        Use this after a job has run to get optimization and cost saving recommendations.

        Args:
            execution_data: Historical execution data including:
                - execution_id: Unique ID
                - platform: Platform used (glue, emr, lambda)
                - duration_minutes: How long it took
                - cost_usd: Total cost
                - data_volume_gb: Data processed
                - metrics: Performance metrics (CPU, memory, etc)
                - success: Boolean indicating success/failure

        Returns:
            Analysis with optimization and cost saving recommendations
        """
        logger.info(f"Analyzing existing job: {execution_data.get('execution_id', 'unknown')}")

        prompt = f"""Analyze this completed ETL job execution and provide comprehensive recommendations:

Job Execution Data:
{json.dumps(execution_data, indent=2)}

Please coordinate with all specialized agents to:
1. Evaluate if the right platform was chosen (Decision Agent)
2. Assess data quality issues encountered (Quality Agent)
3. Identify performance optimization opportunities (Optimization Agent)
4. Learn patterns from this execution for future jobs (Learning Agent)
5. Verify compliance requirements were met (Compliance Agent)
6. Identify cost optimization opportunities (Cost Tracking Agent)

Provide specific, actionable recommendations for improving future executions."""

        result = self.swarm(prompt)
        return self._parse_swarm_result(result, execution_data)

    def validate_job_before_execution(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a job configuration before execution.

        This performs pre-flight checks to catch issues early.

        Args:
            job_config: Job configuration including data paths, schema, requirements

        Returns:
            Validation results with go/no-go recommendation
        """
        logger.info("Validating job configuration before execution...")

        prompt = f"""Validate this ETL job configuration before execution:

Job Configuration:
{json.dumps(job_config, indent=2)}

Quality Agent: Check if schema is valid and data quality rules are defined
Compliance Agent: Verify compliance requirements (PII handling, retention policies)
Decision Agent: Confirm platform choice is appropriate for workload
Cost Tracking Agent: Estimate costs and flag if unusually high

Provide a clear GO/NO-GO recommendation with reasoning."""

        result = self.swarm(prompt)
        return self._parse_swarm_result(result, job_config)

    def _create_job_prompt(self, job_request: Dict[str, Any]) -> str:
        """Create a comprehensive prompt for the swarm based on job request."""
        prompt = f"""I need help planning and executing this ETL job:

Job Details:
- Name: {job_request.get('job_name', 'unnamed')}
- Data Volume: {job_request.get('data_volume_gb', 0)} GB
- File Count: {job_request.get('file_count', 0)} files
- Transformation Complexity: {job_request.get('transformation_complexity', 'moderate')}
- Query Pattern: {job_request.get('query_pattern', 'batch')}

Please coordinate to provide a complete execution plan:

1. Decision Agent: Analyze the workload and recommend the best platform (Glue/EMR/Lambda). Consider costs and performance.

2. Learning Agent: Search for similar historical workloads and recommend based on past successes.

3. Quality Agent: Review the requirements and suggest quality checks to implement.

4. Compliance Agent: Check for any compliance requirements (PII, GDPR, HIPAA).

5. Optimization Agent: Suggest performance optimizations for this workload.

6. Cost Tracking Agent: Provide detailed cost estimates and identify potential savings.

Work together to create a comprehensive, production-ready execution plan."""

        # Add script analysis if provided
        if 'script_path' in job_request:
            prompt += f"\n\nScript Path: {job_request['script_path']}"
            prompt += "\nQuality Agent: Please analyze the PySpark script for anti-patterns."

        return prompt

    def _parse_swarm_result(self, result: Any, original_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the swarm result into a structured format.

        Args:
            result: Raw result from swarm execution
            original_request: Original job request

        Returns:
            Structured result dictionary
        """
        return {
            'job_name': original_request.get('job_name', 'unnamed'),
            'swarm_result': str(result),
            'original_request': original_request,
            'status': 'completed',
            'agents_involved': [agent.name for agent in self.agents]
        }


def main():
    """
    Example usage of the ETL Swarm.
    """
    # Create the swarm
    swarm = ETLSwarm()

    # Example 1: Process a new ETL job
    job_request = {
        'job_name': 'customer_order_summary',
        'data_volume_gb': 250,
        'file_count': 1000,
        'transformation_complexity': 'complex',
        'query_pattern': 'batch',
        'source_path': 's3://my-bucket/raw/orders/',
        'destination_path': 's3://my-bucket/processed/order_summary/'
    }

    print("=" * 80)
    print("EXAMPLE 1: Processing new ETL job")
    print("=" * 80)
    result = swarm.process_etl_job(job_request)
    print(json.dumps(result, indent=2))

    # Example 2: Analyze existing execution
    execution_data = {
        'execution_id': 'exec-2024-01-21-abc123',
        'job_name': 'customer_order_summary',
        'platform': 'glue',
        'duration_minutes': 45,
        'cost_usd': 8.50,
        'data_volume_gb': 250,
        'success': True,
        'metrics': {
            'cpu_utilization_avg': 45,
            'memory_utilization_avg': 65,
            'shuffle_read_gb': 100,
            'shuffle_write_gb': 50
        }
    }

    print("\n" + "=" * 80)
    print("EXAMPLE 2: Analyzing existing job execution")
    print("=" * 80)
    result = swarm.analyze_existing_job(execution_data)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
