"""
Strands Coordinator - Manages the multi-agent ETL orchestration system.
Coordinates independent agents working asynchronously on ETL pipelines.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import uuid
import boto3

from strands_message_bus import (
    StrandsMessageBus, get_message_bus, start_message_bus, MessageType
)
from strands_decision_agent import StrandsDecisionAgent
from strands_quality_agent import StrandsQualityAgent
from strands_optimization_agent import StrandsOptimizationAgent
from strands_learning_agent import StrandsLearningAgent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrandsCoordinator:
    """
    Coordinates multiple independent Strands agents for ETL pipeline execution.
    Manages agent lifecycle and facilitates message-based communication.
    """

    def __init__(self):
        self.message_bus: Optional[StrandsMessageBus] = None
        self.agents = {}
        self.running = False

        # AWS clients for job execution
        self.glue = boto3.client('glue')
        self.emr = boto3.client('emr')
        self.lambda_client = boto3.client('lambda')
        self.s3 = boto3.client('s3')

        # Track pipeline executions
        self.active_pipelines = {}

    async def initialize(self):
        """Initialize the coordinator and all agents."""
        logger.info("Initializing Strands Coordinator...")

        # Start message bus
        self.message_bus = get_message_bus()
        asyncio.create_task(start_message_bus())

        # Create agents
        self.agents = {
            'decision': StrandsDecisionAgent(),
            'quality': StrandsQualityAgent(),
            'optimization': StrandsOptimizationAgent(),
            'learning': StrandsLearningAgent()
        }

        # Start all agents
        agent_tasks = []
        for name, agent in self.agents.items():
            logger.info(f"Starting agent: {name}")
            agent_tasks.append(asyncio.create_task(agent.start()))

        # Wait a moment for agents to initialize
        await asyncio.sleep(2)

        # Subscribe to key events
        self.message_bus.subscribe_to_type(
            MessageType.DECISION_MADE,
            self._handle_decision_made
        )

        self.running = True
        logger.info("Strands Coordinator initialized successfully")

    async def shutdown(self):
        """Shutdown the coordinator and all agents."""
        logger.info("Shutting down Strands Coordinator...")
        self.running = False

        # Stop all agents
        for name, agent in self.agents.items():
            logger.info(f"Stopping agent: {name}")
            await agent.stop()

        logger.info("Strands Coordinator shutdown complete")

    async def orchestrate_pipeline(self,
                                   user_request: str,
                                   config_path: str) -> Dict[str, Any]:
        """
        Orchestrate an ETL pipeline using the Strands multi-agent system.

        Args:
            user_request: Natural language description of the ETL task
            config_path: Path to configuration file (S3 or local)

        Returns:
            Pipeline execution context with results from all agents
        """
        # Load configuration
        config = await self._load_config(config_path)

        # Create pipeline context
        correlation_id = str(uuid.uuid4())
        pipeline_id = str(uuid.uuid4())

        context = {
            'pipeline_id': pipeline_id,
            'correlation_id': correlation_id,
            'user_request': user_request,
            'config': config,
            'start_time': datetime.utcnow().isoformat(),
            'status': 'initializing'
        }

        self.active_pipelines[correlation_id] = context

        logger.info(f"Starting pipeline {pipeline_id} with correlation {correlation_id}")

        try:
            # Step 1: Request platform decision from Decision Agent
            logger.info("Step 1: Requesting platform decision...")
            decision_msg = self.message_bus.create_message(
                sender='coordinator',
                message_type=MessageType.AGENT_REQUEST,
                payload={
                    'request_type': 'platform_decision',
                    'workload': config.get('workload', {}),
                    'config': config
                },
                target='decision_agent',
                correlation_id=correlation_id,
                priority=8
            )
            await self.message_bus.publish(decision_msg)

            # Wait for decision (with timeout)
            decision = await self._wait_for_decision(correlation_id, timeout=30)

            if not decision:
                raise Exception("Platform decision timeout")

            context['decision'] = decision
            selected_platform = decision.get('selected_platform', 'glue')

            logger.info(f"Platform selected: {selected_platform}")

            # Step 2: Execute on selected platform
            logger.info(f"Step 2: Executing on {selected_platform}...")
            execution_result = await self._execute_on_platform(
                selected_platform, config, correlation_id
            )

            context['execution_result'] = execution_result

            # Notify execution started
            await self._broadcast_execution_started(
                selected_platform, config, correlation_id
            )

            # Step 3: Monitor execution
            logger.info("Step 3: Monitoring execution...")
            final_result = await self._monitor_execution(execution_result)

            context['execution_result'] = final_result

            # Notify execution completed
            await self._broadcast_execution_completed(
                final_result, context, correlation_id
            )

            # Step 4: Wait for quality assessment
            logger.info("Step 4: Waiting for quality assessment...")
            await asyncio.sleep(2)  # Give agents time to process

            # Step 5: Collect results from all agents
            context['messages'] = self.message_bus.get_message_history(
                correlation_id=correlation_id
            )

            # Extract agent outputs
            context['quality_reports'] = self._extract_agent_messages(
                context['messages'], 'quality_agent'
            )
            context['optimization_reports'] = self._extract_agent_messages(
                context['messages'], 'optimization_agent'
            )
            context['learning_updates'] = self._extract_agent_messages(
                context['messages'], 'learning_agent'
            )

            # Set final status
            context['status'] = 'completed' if final_result.get('status') == 'completed' else 'failed'
            context['end_time'] = datetime.utcnow().isoformat()

            logger.info(f"Pipeline {pipeline_id} completed with status: {context['status']}")

            return context

        except Exception as e:
            logger.error(f"Pipeline {pipeline_id} failed: {e}")
            context['status'] = 'failed'
            context['error'] = str(e)
            context['end_time'] = datetime.utcnow().isoformat()
            return context

        finally:
            # Clean up
            if correlation_id in self.active_pipelines:
                del self.active_pipelines[correlation_id]

    async def _wait_for_decision(self, correlation_id: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Wait for platform decision from Decision Agent."""
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout:
            # Check message history for decision
            messages = self.message_bus.get_message_history(correlation_id=correlation_id)

            for msg in messages:
                if msg.message_type == MessageType.DECISION_MADE:
                    return msg.payload

            await asyncio.sleep(0.5)

        return None

    async def _handle_decision_made(self, message):
        """Handle decision made event (callback from message bus)."""
        logger.debug(f"Decision made: {message.payload.get('selected_platform')}")

    async def _execute_on_platform(self,
                                   platform: str,
                                   config: Dict[str, Any],
                                   correlation_id: str) -> Dict[str, Any]:
        """Execute ETL job on the selected platform."""
        if platform == 'glue':
            return await self._execute_glue_job(config, correlation_id)
        elif platform == 'emr':
            return await self._execute_emr_job(config, correlation_id)
        elif platform == 'lambda':
            return await self._execute_lambda_job(config, correlation_id)
        else:
            raise ValueError(f"Unknown platform: {platform}")

    async def _execute_glue_job(self, config: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
        """Execute ETL job on AWS Glue."""
        job_name = f"strands-etl-{uuid.uuid4().hex[:8]}"

        try:
            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()

            # Create job
            await loop.run_in_executor(
                None,
                lambda: self.glue.create_job(
                    Name=job_name,
                    Role='StrandsETLDemoRole',
                    Command={
                        'Name': 'glueetl',
                        'ScriptLocation': config['scripts']['pyspark'],
                        'PythonVersion': '3'
                    },
                    DefaultArguments={
                        '--config_path': config.get('config_s3_path', ''),
                        '--JOB_NAME': job_name,
                        '--correlation_id': correlation_id
                    },
                    MaxCapacity=config.get('resource_allocation', {}).get('workers', 10)
                )
            )

            # Start job run
            run_response = await loop.run_in_executor(
                None,
                lambda: self.glue.start_job_run(JobName=job_name)
            )

            run_id = run_response['JobRunId']

            logger.info(f"Glue job {job_name} started with run ID: {run_id}")

            return {
                'platform': 'glue',
                'job_name': job_name,
                'run_id': run_id,
                'status': 'running',
                'correlation_id': correlation_id
            }

        except Exception as e:
            logger.error(f"Glue job execution failed: {e}")
            return {'error': str(e), 'status': 'failed', 'platform': 'glue'}

    async def _execute_emr_job(self, config: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
        """Execute ETL job on AWS EMR."""
        # Similar to Glue but for EMR
        logger.warning("EMR execution not fully implemented in this demo")
        return {
            'platform': 'emr',
            'status': 'not_implemented',
            'correlation_id': correlation_id
        }

    async def _execute_lambda_job(self, config: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
        """Execute ETL job on AWS Lambda."""
        # Similar to Glue but for Lambda
        logger.warning("Lambda execution not fully implemented in this demo")
        return {
            'platform': 'lambda',
            'status': 'not_implemented',
            'correlation_id': correlation_id
        }

    async def _monitor_execution(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor job execution until completion."""
        platform = execution_result.get('platform')

        if platform == 'glue':
            return await self._monitor_glue_job(execution_result)
        elif platform == 'emr':
            return await self._monitor_emr_job(execution_result)
        else:
            return execution_result

    async def _monitor_glue_job(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor Glue job execution."""
        job_name = execution_result.get('job_name')
        run_id = execution_result.get('run_id')

        if not job_name or not run_id:
            return execution_result

        max_wait_time = 3600  # 1 hour
        poll_interval = 30  # 30 seconds
        start_time = asyncio.get_event_loop().time()

        loop = asyncio.get_event_loop()

        while asyncio.get_event_loop().time() - start_time < max_wait_time:
            try:
                response = await loop.run_in_executor(
                    None,
                    lambda: self.glue.get_job_run(JobName=job_name, RunId=run_id)
                )

                job_run = response['JobRun']
                status = job_run['JobRunState']

                logger.info(f"Job {job_name} status: {status}")

                if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    execution_result.update({
                        'final_status': status,
                        'completion_time': datetime.utcnow().isoformat(),
                        'job_run_details': job_run,
                        'status': 'completed' if status == 'SUCCEEDED' else 'failed'
                    })

                    if status != 'SUCCEEDED':
                        execution_result['error'] = job_run.get('ErrorMessage', f'Job {status}')

                    return execution_result

                await asyncio.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error monitoring Glue job: {e}")
                execution_result.update({
                    'status': 'failed',
                    'error': f'Monitoring failed: {str(e)}'
                })
                return execution_result

        # Timeout
        execution_result.update({
            'status': 'failed',
            'error': f'Monitoring timeout after {max_wait_time} seconds'
        })
        return execution_result

    async def _monitor_emr_job(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor EMR job execution."""
        # Placeholder
        return execution_result

    async def _broadcast_execution_started(self,
                                          platform: str,
                                          config: Dict[str, Any],
                                          correlation_id: str):
        """Broadcast that execution has started."""
        msg = self.message_bus.create_message(
            sender='coordinator',
            message_type=MessageType.EXECUTION_STARTED,
            payload={
                'platform': platform,
                'config': config
            },
            target=None,  # Broadcast
            correlation_id=correlation_id
        )
        await self.message_bus.publish(msg)

    async def _broadcast_execution_completed(self,
                                            execution_result: Dict[str, Any],
                                            context: Dict[str, Any],
                                            correlation_id: str):
        """Broadcast that execution has completed."""
        msg = self.message_bus.create_message(
            sender='coordinator',
            message_type=MessageType.EXECUTION_COMPLETED,
            payload={
                'execution_result': execution_result,
                'context': context
            },
            target=None,  # Broadcast
            correlation_id=correlation_id
        )
        await self.message_bus.publish(msg)

    def _extract_agent_messages(self,
                                messages: List,
                                agent_name: str) -> List[Dict[str, Any]]:
        """Extract messages from a specific agent."""
        return [
            msg.payload
            for msg in messages
            if msg.sender_agent == agent_name
        ]

    async def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from S3 or local path."""
        loop = asyncio.get_event_loop()

        if config_path.startswith('s3://'):
            bucket, key = config_path.replace('s3://', '').split('/', 1)
            response = await loop.run_in_executor(
                None,
                lambda: self.s3.get_object(Bucket=bucket, Key=key)
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        else:
            with open(config_path, 'r') as f:
                return json.load(f)

    async def get_agent_metrics(self) -> Dict[str, Any]:
        """Get metrics from all agents."""
        metrics = {}
        for name, agent in self.agents.items():
            metrics[name] = agent.get_metrics()
        return metrics

    async def get_learning_summary(self) -> Dict[str, Any]:
        """Get learning summary from Learning Agent."""
        learning_agent = self.agents.get('learning')
        if learning_agent:
            return await learning_agent.get_learning_summary()
        return {}


# Example usage
async def main():
    """Example of using Strands Coordinator."""
    coordinator = StrandsCoordinator()

    try:
        # Initialize
        await coordinator.initialize()

        # Run a pipeline
        result = await coordinator.orchestrate_pipeline(
            user_request="Process customer data with quality checks",
            config_path="/home/user/strands_etl/etl_config.json"
        )

        print("\n=== Pipeline Result ===")
        print(json.dumps({
            'pipeline_id': result['pipeline_id'],
            'status': result['status'],
            'platform': result.get('decision', {}).get('selected_platform'),
            'execution_time': result.get('execution_result', {}).get('execution_time_seconds')
        }, indent=2))

        # Get agent metrics
        metrics = await coordinator.get_agent_metrics()
        print("\n=== Agent Metrics ===")
        print(json.dumps(metrics, indent=2))

        # Get learning summary
        learning = await coordinator.get_learning_summary()
        print("\n=== Learning Summary ===")
        print(json.dumps(learning, indent=2))

    finally:
        await coordinator.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
