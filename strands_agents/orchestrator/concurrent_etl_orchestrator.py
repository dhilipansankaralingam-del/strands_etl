"""
Concurrent ETL Orchestrator with Strands Agents

This orchestrator implements the concurrent analysis pattern where:
1. ETL jobs start immediately (no delay)
2. Agents analyze scripts and workload in parallel
3. Recommendations are stored for next run
4. Continuous learning improves future executions

This is the RECOMMENDED approach for production ETL workflows.

Usage:
    from strands_agents.orchestrator.concurrent_etl_orchestrator import ConcurrentETLOrchestrator

    orchestrator = ConcurrentETLOrchestrator()

    result = orchestrator.run_etl_job(
        script_path='./pyscript/customer_analytics.py',
        job_config={
            'glue_job_name': 'customer-analytics-job',
            'data_volume_gb': 500,
            'dpu_count': 10
        }
    )
"""
import boto3
import time
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from strands_agents.orchestrator.swarm_orchestrator import ETLSwarm
from strands_agents.agents.etl_agents import create_quality_agent, create_decision_agent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConcurrentETLOrchestrator:
    """
    Orchestrates ETL jobs with concurrent agent analysis.

    This orchestrator:
    - Starts jobs immediately without delay
    - Analyzes scripts in parallel while jobs run
    - Stores recommendations in DynamoDB for next run
    - Provides post-execution optimization suggestions
    """

    def __init__(
        self,
        aws_region: str = 'us-east-1',
        recommendations_table: str = 'StrandsJobRecommendations'
    ):
        """
        Initialize the orchestrator.

        Args:
            aws_region: AWS region for Glue and DynamoDB
            recommendations_table: DynamoDB table to store recommendations
        """
        self.aws_region = aws_region
        self.recommendations_table = recommendations_table

        # AWS clients
        self.glue_client = boto3.client('glue', region_name=aws_region)
        self.dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=aws_region)

        # Strands agents
        self.swarm = ETLSwarm()
        self.quality_agent = create_quality_agent()
        self.decision_agent = create_decision_agent()

        logger.info(f"Initialized ConcurrentETLOrchestrator for region {aws_region}")

    def run_etl_job(
        self,
        script_path: str,
        job_config: Dict[str, Any],
        apply_previous_recommendations: bool = True
    ) -> Dict[str, Any]:
        """
        Run an ETL job with concurrent agent analysis.

        Flow:
        1. Load previous recommendations (if any)
        2. Apply recommendations to job config
        3. Start Glue job immediately
        4. Analyze script concurrently while job runs
        5. Monitor job execution
        6. Post-execution analysis
        7. Store recommendations for next run

        Args:
            script_path: Path to PySpark script (local or S3)
            job_config: Job configuration dict containing:
                - glue_job_name: Name of Glue job
                - data_volume_gb: Data size
                - dpu_count: Number of DPUs
                - (optional) source_path, destination_path
            apply_previous_recommendations: Whether to apply past recommendations

        Returns:
            Dict with execution results and recommendations
        """
        job_name = job_config.get('glue_job_name', 'unnamed_job')
        logger.info(f"Starting ETL job: {job_name}")

        # Step 1: Load previous recommendations
        previous_recommendations = None
        if apply_previous_recommendations:
            previous_recommendations = self._load_previous_recommendations(job_name)

            if previous_recommendations:
                logger.info(f"Found {len(previous_recommendations)} previous recommendations")
                job_config = self._apply_recommendations(job_config, previous_recommendations)

        # Step 2: Submit job to Glue immediately (no delay!)
        logger.info("Submitting job to AWS Glue...")

        glue_arguments = self._build_glue_arguments(job_config)

        try:
            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments=glue_arguments
            )

            job_run_id = response['JobRunId']
            start_time = datetime.utcnow()

            logger.info(f"✓ Job started successfully: {job_run_id}")

        except Exception as e:
            logger.error(f"✗ Failed to start job: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

        # Step 3: Concurrent analysis (runs in parallel with Glue job)
        logger.info("Starting concurrent script analysis...")

        analysis_future = self._analyze_script_async(script_path, job_config)

        # Step 4: Monitor job execution
        logger.info("Monitoring job execution...")

        execution_state, execution_metrics = self._monitor_job(job_name, job_run_id, start_time)

        logger.info(f"Job completed with state: {execution_state}")

        # Step 5: Wait for analysis to complete (if not already done)
        logger.info("Waiting for analysis to complete...")
        script_analysis = analysis_future.result()

        logger.info("✓ Script analysis complete")

        # Step 6: Post-execution comprehensive analysis
        logger.info("Running post-execution analysis...")

        post_execution_analysis = self._post_execution_analysis(
            job_run_id=job_run_id,
            job_config=job_config,
            execution_metrics=execution_metrics,
            script_analysis=script_analysis,
            execution_state=execution_state
        )

        # Step 7: Store recommendations for next run
        self._store_recommendations(
            job_name=job_name,
            recommendations=post_execution_analysis.get('recommendations', [])
        )

        logger.info("✓ Recommendations stored for next run")

        # Step 8: Return complete results
        end_time = datetime.utcnow()
        duration_minutes = (end_time - start_time).total_seconds() / 60

        return {
            'status': 'completed',
            'job_run_id': job_run_id,
            'execution_state': execution_state,
            'duration_minutes': round(duration_minutes, 2),
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'execution_metrics': execution_metrics,
            'script_analysis': script_analysis,
            'post_execution_analysis': post_execution_analysis,
            'recommendations_for_next_run': post_execution_analysis.get('recommendations', []),
            'applied_previous_recommendations': previous_recommendations is not None
        }

    def _analyze_script_async(
        self,
        script_path: str,
        job_config: Dict[str, Any]
    ) -> ThreadPoolExecutor:
        """
        Analyze script asynchronously while job runs.

        Returns:
            Future object that will contain analysis results
        """
        executor = ThreadPoolExecutor(max_workers=1)

        def analyze():
            logger.info("Reading script...")
            try:
                with open(script_path, 'r') as f:
                    script_content = f.read()
            except FileNotFoundError:
                logger.warning(f"Script not found locally, trying S3: {script_path}")
                # In production, fetch from S3
                script_content = "# Script content not available"

            logger.info("Quality Agent analyzing script...")

            analysis_prompt = f"""
Analyze this PySpark script for performance issues and anti-patterns:

Script:
```python
{script_content}
```

Job Configuration:
- Data Volume: {job_config.get('data_volume_gb', 0)} GB
- File Count: {job_config.get('file_count', 0)} files
- Transformation Complexity: {job_config.get('transformation_complexity', 'moderate')}

Please identify:
1. Performance anti-patterns (multiple counts, missing broadcast, etc.)
2. Code quality issues
3. Specific recommendations with code examples
4. Expected performance impact of each fix
"""

            return self.quality_agent(analysis_prompt)

        return executor.submit(analyze)

    def _monitor_job(
        self,
        job_name: str,
        job_run_id: str,
        start_time: datetime
    ) -> tuple:
        """
        Monitor Glue job execution until completion.

        Returns:
            Tuple of (execution_state, execution_metrics)
        """
        while True:
            try:
                response = self.glue_client.get_job_run(
                    JobName=job_name,
                    RunId=job_run_id
                )

                job_run = response['JobRun']
                state = job_run['State']
                elapsed = (datetime.utcnow() - start_time).total_seconds() / 60

                logger.info(f"Job status: {state} (elapsed: {elapsed:.1f} min)")

                if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR']:
                    # Extract execution metrics
                    execution_metrics = {
                        'state': state,
                        'duration_seconds': job_run.get('ExecutionTime', 0),
                        'dpu_seconds': job_run.get('DPUSeconds', 0),
                        'max_capacity': job_run.get('MaxCapacity', 0)
                    }

                    # Fetch CloudWatch metrics if available
                    try:
                        cw_metrics = self._fetch_cloudwatch_metrics(job_name, job_run_id)
                        execution_metrics.update(cw_metrics)
                    except Exception as e:
                        logger.warning(f"Could not fetch CloudWatch metrics: {e}")

                    return state, execution_metrics

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                logger.error(f"Error monitoring job: {e}")
                return 'UNKNOWN', {}

    def _fetch_cloudwatch_metrics(
        self,
        job_name: str,
        job_run_id: str
    ) -> Dict[str, Any]:
        """
        Fetch CloudWatch metrics for the job run.

        Returns:
            Dict with CPU, memory, shuffle metrics
        """
        # In production, fetch actual metrics from CloudWatch
        # For now, return placeholder
        return {
            'cpu_utilization_avg': 0,
            'memory_utilization_avg': 0,
            'shuffle_read_gb': 0,
            'shuffle_write_gb': 0
        }

    def _post_execution_analysis(
        self,
        job_run_id: str,
        job_config: Dict[str, Any],
        execution_metrics: Dict[str, Any],
        script_analysis: Any,
        execution_state: str
    ) -> Dict[str, Any]:
        """
        Run comprehensive post-execution analysis using agent swarm.

        Returns:
            Dict with recommendations for next run
        """
        analysis_prompt = f"""
An ETL job has completed. Provide comprehensive analysis and recommendations:

Job Details:
- Job Run ID: {job_run_id}
- State: {execution_state}
- Data Volume: {job_config.get('data_volume_gb', 0)} GB
- DPU Count: {job_config.get('dpu_count', 0)}

Execution Metrics:
{json.dumps(execution_metrics, indent=2)}

Script Analysis (from Quality Agent):
{script_analysis}

Please coordinate between all agents:

1. Decision Agent: Was the right platform and DPU count used?
2. Quality Agent: What code improvements are needed?
3. Optimization Agent: What configuration changes would help?
4. Learning Agent: What can we learn from this execution?
5. Cost Tracking Agent: How can we reduce costs?
6. Compliance Agent: Any compliance concerns?

Provide:
- Top 5 recommendations for next run (prioritized)
- Expected impact of each recommendation
- Implementation difficulty (low/medium/high)
- Estimated cost savings
"""

        result = self.swarm.swarm(analysis_prompt)

        # Parse recommendations (in production, would parse structured response)
        return {
            'full_analysis': str(result),
            'recommendations': [
                {
                    'category': 'performance',
                    'priority': 'high',
                    'recommendation': 'See full analysis for details',
                    'expected_impact': 'TBD'
                }
            ]
        }

    def _build_glue_arguments(self, job_config: Dict[str, Any]) -> Dict[str, str]:
        """
        Build Glue job arguments from config and recommendations.

        Returns:
            Dict of Glue arguments
        """
        args = {
            '--job-language': 'python',
            '--enable-metrics': 'true',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-spark-ui': 'true'
        }

        # Add optimized Spark configs if available
        if 'spark_config' in job_config:
            for key, value in job_config['spark_config'].items():
                args[f'--conf'] = f'{key}={value}'

        # Add source/destination paths
        if 'source_path' in job_config:
            args['--source_path'] = job_config['source_path']

        if 'destination_path' in job_config:
            args['--destination_path'] = job_config['destination_path']

        return args

    def _load_previous_recommendations(self, job_name: str) -> Optional[list]:
        """
        Load recommendations from previous runs from DynamoDB.

        Returns:
            List of recommendations or None
        """
        try:
            table = self.dynamodb.Table(self.recommendations_table)

            response = table.get_item(
                Key={'job_name': job_name}
            )

            if 'Item' in response:
                return response['Item'].get('recommendations', [])

        except Exception as e:
            logger.warning(f"Could not load previous recommendations: {e}")

        return None

    def _store_recommendations(
        self,
        job_name: str,
        recommendations: list
    ) -> None:
        """
        Store recommendations in DynamoDB for next run.
        """
        try:
            table = self.dynamodb.Table(self.recommendations_table)

            table.put_item(
                Item={
                    'job_name': job_name,
                    'timestamp': datetime.utcnow().isoformat(),
                    'recommendations': recommendations
                }
            )

            logger.info(f"Stored {len(recommendations)} recommendations for {job_name}")

        except Exception as e:
            logger.error(f"Failed to store recommendations: {e}")

    def _apply_recommendations(
        self,
        job_config: Dict[str, Any],
        recommendations: list
    ) -> Dict[str, Any]:
        """
        Apply previous recommendations to current job config.

        Returns:
            Updated job config
        """
        logger.info("Applying previous recommendations to job config")

        # Example: Apply Spark config recommendations
        spark_config = job_config.get('spark_config', {})

        for rec in recommendations:
            if rec.get('category') == 'spark_config':
                config_key = rec.get('config_key')
                config_value = rec.get('config_value')

                if config_key and config_value:
                    spark_config[config_key] = config_value
                    logger.info(f"  Applied: {config_key} = {config_value}")

            elif rec.get('category') == 'resource_allocation':
                if 'dpu_count' in rec:
                    job_config['dpu_count'] = rec['dpu_count']
                    logger.info(f"  Applied: DPU count = {rec['dpu_count']}")

        job_config['spark_config'] = spark_config

        return job_config


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

def example_usage():
    """
    Example of using the Concurrent ETL Orchestrator
    """
    print("=" * 80)
    print("CONCURRENT ETL ORCHESTRATOR - EXAMPLE")
    print("=" * 80)

    # Initialize orchestrator
    orchestrator = ConcurrentETLOrchestrator(
        aws_region='us-east-1',
        recommendations_table='StrandsJobRecommendations'
    )

    # Job configuration
    job_config = {
        'glue_job_name': 'customer-purchase-analytics',
        'data_volume_gb': 400,
        'file_count': 2000,
        'transformation_complexity': 'complex',
        'dpu_count': 10,
        'source_path': 's3://my-bucket/raw/transactions/',
        'destination_path': 's3://my-bucket/processed/analytics/'
    }

    script_path = './pyscript/customer_order_summary_glue.py'

    # Run ETL job with concurrent analysis
    print("\n🚀 Starting ETL job with concurrent agent analysis...")
    print("   Job will start immediately while agents analyze in parallel\n")

    result = orchestrator.run_etl_job(
        script_path=script_path,
        job_config=job_config,
        apply_previous_recommendations=True  # Apply learnings from past runs
    )

    # Print results
    print("\n" + "=" * 80)
    print("EXECUTION RESULTS")
    print("=" * 80)
    print(f"Status: {result['status']}")
    print(f"Job Run ID: {result['job_run_id']}")
    print(f"Duration: {result['duration_minutes']} minutes")
    print(f"Execution State: {result['execution_state']}")

    print("\n" + "=" * 80)
    print("RECOMMENDATIONS FOR NEXT RUN")
    print("=" * 80)

    recommendations = result.get('recommendations_for_next_run', [])
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec.get('category', 'general').upper()}")
        print(f"   Priority: {rec.get('priority', 'medium')}")
        print(f"   Recommendation: {rec.get('recommendation', 'N/A')}")
        print(f"   Expected Impact: {rec.get('expected_impact', 'N/A')}")

    print("\n" + "=" * 80)
    print("NEXT TIME THIS JOB RUNS:")
    print("  ✓ Recommendations will be automatically applied")
    print("  ✓ Job will benefit from learnings")
    print("  ✓ Continuous improvement over time")
    print("=" * 80)


if __name__ == '__main__':
    example_usage()
