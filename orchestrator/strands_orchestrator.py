"""
Strands Multi-Agent Orchestration Framework for Intelligent ETL Processing.
Version 2.0 - Supports existing jobs, agent-driven platform decisions, and recommendations.

Key Features:
- Run existing Glue jobs (99% use case)
- Create new Glue jobs only when explicitly requested
- Agent-driven platform selection (recommend vs decide modes)
- Lambda conversion recommendations
- EMR support for existing clusters
"""

import json
import boto3
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid
from decimal import Decimal

# Default local storage path for learning data
DEFAULT_LOCAL_STORAGE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'learning_data')

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and Decimal objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrandsOrchestrator:
    """
    Strands Multi-Agent Orchestration Framework for Intelligent ETL Processing.

    Modes:
    - agent_mode: "recommend" - Agent provides recommendations, user confirms
    - agent_mode: "decide" - Agent makes autonomous platform decisions

    Execution:
    - By default, runs EXISTING jobs (Glue job name, Lambda function, EMR cluster)
    - Set create_job: true to create new resources on-the-fly
    """

    def __init__(self, region: str = None, storage_mode: str = 'local', local_storage_path: str = None):
        """
        Initialize the orchestrator.

        Args:
            region: AWS region (uses AWS_DEFAULT_REGION if not specified)
            storage_mode: 'local' (default) or 's3' - where to store learning data
            local_storage_path: Path for local storage (default: ./learning_data)
        """
        self.region = region
        self.storage_mode = storage_mode
        self.local_storage_path = local_storage_path or DEFAULT_LOCAL_STORAGE
        client_kwargs = {'region_name': region} if region else {}

        self.bedrock = boto3.client('bedrock-runtime', **client_kwargs)
        self.glue = boto3.client('glue', **client_kwargs)
        self.emr = boto3.client('emr', **client_kwargs)
        self.lambda_client = boto3.client('lambda', **client_kwargs)
        self.s3 = boto3.client('s3', **client_kwargs)

        # Create local storage directories if using local mode
        if self.storage_mode == 'local':
            self._ensure_local_dirs()
            logger.info(f"Orchestrator using LOCAL storage at: {self.local_storage_path}")
        else:
            logger.info("Orchestrator using S3 storage")

        # Agent definitions
        self.agents = {
            'orchestrator': self.orchestrator_agent,
            'decision': self.decision_agent,
            'execution': self.execution_agent,
            'quality': self.quality_agent,
            'optimization': self.optimization_agent,
            'learning': self.learning_agent,
            'platform_advisor': self.platform_advisor_agent
        }

    def _ensure_local_dirs(self):
        """Create local storage directories if they don't exist."""
        dirs = [
            os.path.join(self.local_storage_path, 'learning', 'vectors'),
            os.path.join(self.local_storage_path, 'quality', 'reports'),
            os.path.join(self.local_storage_path, 'optimization', 'recommendations')
        ]
        for d in dirs:
            os.makedirs(d, exist_ok=True)

    # =========================================================================
    # PLATFORM ADVISOR AGENT - New agent for platform recommendations
    # =========================================================================

    def platform_advisor_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Platform Advisor Agent - Analyzes workload and provides platform recommendations.
        Can recommend converting between platforms (e.g., Glue to Lambda).
        """
        workload = context.get('workload', {})
        script_info = context.get('script_info', {})
        current_platform = context.get('current_platform', 'unknown')

        # Get historical data for informed decisions
        learning_vectors = self.get_learning_vectors(limit=10)

        prompt = f"""
        As the Platform Advisor Agent, analyze this ETL workload and provide platform recommendations.

        Current Setup:
        - Current Platform: {current_platform}
        - Script Location: {script_info.get('script_path', 'N/A')}
        - Script Type: {script_info.get('script_type', 'N/A')}

        Workload Characteristics:
        - Data Volume: {workload.get('data_volume', 'unknown')}
        - Complexity: {workload.get('complexity', 'unknown')}
        - Criticality: {workload.get('criticality', 'unknown')}
        - Time Sensitivity: {workload.get('time_sensitivity', 'unknown')}
        - Estimated Runtime: {workload.get('estimated_runtime', 'unknown')}

        Historical Performance Data: {json.dumps(learning_vectors[:5], indent=2, cls=DateTimeEncoder)}

        Analyze and provide:
        1. RECOMMENDED_PLATFORM: The best platform for this workload (glue/emr/lambda)
        2. CONFIDENCE_SCORE: Your confidence in this recommendation (0.0-1.0)
        3. REASONING: Why this platform is optimal
        4. CONVERSION_POSSIBLE: Can the current script be converted to another platform? (yes/no)
        5. CONVERSION_RECOMMENDATION: If conversion is beneficial, explain how
        6. COST_COMPARISON: Estimated cost comparison between platforms
        7. PERFORMANCE_COMPARISON: Expected performance on each platform

        Platform Guidelines:
        - Lambda: Best for < 15 min runtime, < 10GB data, event-driven, simple transformations
        - Glue: Best for 15min-8hr runtime, complex Spark transformations, catalog integration
        - EMR: Best for > 8hr runtime, very large datasets, custom cluster configurations

        Respond in JSON format with these exact keys.
        """

        response = self.invoke_bedrock_agent(prompt, "platform_advisor")
        return self.parse_agent_response(response)

    # =========================================================================
    # DECISION AGENT - Enhanced with agent_mode support
    # =========================================================================

    def decision_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decision agent that determines the best execution platform and strategy.

        Supports two modes:
        - "recommend": Returns recommendations for user to decide
        - "decide": Makes autonomous platform selection
        """
        workload = context.get('workload', {})
        agent_mode = context.get('agent_mode', 'recommend')
        available_platforms = context.get('available_platforms', ['glue', 'emr', 'lambda'])

        # Retrieve learning vectors for informed decision making
        learning_vectors = self.get_learning_vectors(limit=10)

        prompt = f"""
        As the Decision Agent, analyze this workload and {'select' if agent_mode == 'decide' else 'recommend'} the optimal execution platform.

        Agent Mode: {agent_mode.upper()}
        Available Platforms: {', '.join(available_platforms)}

        Workload Characteristics:
        - Data Volume: {workload.get('data_volume', 'unknown')}
        - Complexity: {workload.get('complexity', 'unknown')}
        - Criticality: {workload.get('criticality', 'unknown')}
        - Time Sensitivity: {workload.get('time_sensitivity', 'unknown')}

        Historical Learning Data: {json.dumps(learning_vectors, indent=2, cls=DateTimeEncoder)}

        Platform Selection Criteria:
        - Lambda: < 15 min runtime, < 10GB data, simple transformations, cost-effective for small jobs
        - Glue: 15min-8hr runtime, Spark-based, complex joins/aggregations, Glue Catalog integration
        - EMR: > 8hr runtime, very large datasets (TB+), custom Spark/Hadoop configurations

        {"MAKE A DECISION and select ONE platform." if agent_mode == 'decide' else "PROVIDE RECOMMENDATIONS with pros/cons for each platform."}

        Respond in JSON format:
        {{
            "selected_platform": "glue|emr|lambda",  // Your selection (required if mode=decide)
            "confidence": 0.0-1.0,
            "recommendations": [
                {{"platform": "...", "score": 0.0-1.0, "pros": [...], "cons": [...], "estimated_cost": "..."}}
            ],
            "reasoning": "..."
        }}
        """

        response = self.invoke_bedrock_agent(prompt, "decision")
        result = self.parse_agent_response(response)

        # Ensure selected_platform is set for decide mode
        if agent_mode == 'decide' and not result.get('selected_platform'):
            # Default to glue if agent didn't select
            result['selected_platform'] = 'glue'

        return result

    # =========================================================================
    # EXECUTION AGENT - Enhanced to support existing jobs
    # =========================================================================

    def execution_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execution agent that manages job submission and monitoring.

        Supports:
        - Running EXISTING jobs (default behavior)
        - Creating NEW jobs (when create_job=True in config)
        """
        platform = context.get('platform', 'glue')
        config = context.get('config', {})
        execution_config = config.get('execution', {})

        # Determine execution mode
        create_job = execution_config.get('create_job', False)

        logger.info(f"Execution mode: {'CREATE NEW' if create_job else 'RUN EXISTING'} on {platform}")

        if platform == 'glue':
            if create_job:
                return self.create_and_run_glue_job(config)
            else:
                return self.run_existing_glue_job(config)
        elif platform == 'emr':
            if create_job:
                return self.create_and_run_emr_cluster(config)
            else:
                return self.run_existing_emr_step(config)
        elif platform == 'lambda':
            return self.invoke_lambda_function(config)
        else:
            return {'status': 'failed', 'error': f'Unknown platform: {platform}'}

    # =========================================================================
    # GLUE EXECUTION METHODS
    # =========================================================================

    def run_existing_glue_job(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run an EXISTING Glue job by name.
        This is the primary execution method (99% of use cases).
        """
        execution_config = config.get('execution', {})
        job_name = execution_config.get('glue_job_name')

        if not job_name:
            # Fallback to scripts config for backward compatibility
            job_name = config.get('scripts', {}).get('existing_glue_job')

        if not job_name:
            return {
                'status': 'failed',
                'error': 'No glue_job_name specified in execution config. Set execution.glue_job_name or use create_job=true to create a new job.'
            }

        try:
            # Verify job exists
            try:
                self.glue.get_job(JobName=job_name)
                logger.info(f"Found existing Glue job: {job_name}")
            except self.glue.exceptions.EntityNotFoundException:
                return {
                    'status': 'failed',
                    'error': f"Glue job '{job_name}' not found. Create it first or set create_job=true."
                }

            # Build job arguments
            job_arguments = execution_config.get('job_arguments', {})

            # Add default arguments from config
            if config.get('config_s3_path'):
                job_arguments['--config_path'] = config['config_s3_path']

            # Start job run
            run_params = {'JobName': job_name}
            if job_arguments:
                run_params['Arguments'] = job_arguments

            # Add optional parameters
            if execution_config.get('worker_type'):
                run_params['WorkerType'] = execution_config['worker_type']
            if execution_config.get('number_of_workers'):
                run_params['NumberOfWorkers'] = execution_config['number_of_workers']
            if execution_config.get('timeout'):
                run_params['Timeout'] = execution_config['timeout']

            run_response = self.glue.start_job_run(**run_params)
            run_id = run_response['JobRunId']

            logger.info(f"Started existing Glue job '{job_name}' with run ID: {run_id}")

            return {
                'platform': 'glue',
                'job_name': job_name,
                'run_id': run_id,
                'status': 'running',
                'execution_type': 'existing_job'
            }

        except Exception as e:
            logger.error(f"Failed to run existing Glue job: {e}")
            return {'status': 'failed', 'error': str(e)}

    def create_and_run_glue_job(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a NEW Glue job and run it.
        Used only when create_job=True (rare use case).
        """
        execution_config = config.get('execution', {})
        scripts_config = config.get('scripts', {})

        # Generate job name or use provided one
        job_name = execution_config.get('new_job_name') or f"strands-etl-{uuid.uuid4().hex[:8]}"

        # Get script location
        script_location = scripts_config.get('pyspark') or scripts_config.get('script_path')
        if not script_location:
            return {
                'status': 'failed',
                'error': 'No script location specified. Set scripts.pyspark or scripts.script_path.'
            }

        try:
            # Get IAM role
            role = execution_config.get('iam_role') or config.get('platform', {}).get('iam_role', 'AWSGlueServiceRole')
            if '/' in role:
                role = role.split('/')[-1]

            # Build job definition
            job_params = {
                'Name': job_name,
                'Role': role,
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': script_location,
                    'PythonVersion': '3'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--TempDir': execution_config.get('temp_dir', f's3://aws-glue-temporary-{self.region or "us-east-1"}/temp/'),
                    '--enable-metrics': 'true',
                    '--enable-continuous-cloudwatch-log': 'true'
                },
                'GlueVersion': execution_config.get('glue_version', '4.0'),
                'WorkerType': execution_config.get('worker_type', 'G.1X'),
                'NumberOfWorkers': execution_config.get('number_of_workers', 10)
            }

            # Add custom arguments
            if execution_config.get('job_arguments'):
                job_params['DefaultArguments'].update(execution_config['job_arguments'])

            # Create job
            self.glue.create_job(**job_params)
            logger.info(f"Created new Glue job: {job_name}")

            # Start job run
            run_response = self.glue.start_job_run(JobName=job_name)
            run_id = run_response['JobRunId']

            logger.info(f"Started new Glue job '{job_name}' with run ID: {run_id}")

            return {
                'platform': 'glue',
                'job_name': job_name,
                'run_id': run_id,
                'status': 'running',
                'execution_type': 'new_job_created'
            }

        except Exception as e:
            logger.error(f"Failed to create and run Glue job: {e}")
            return {'status': 'failed', 'error': str(e)}

    # =========================================================================
    # EMR EXECUTION METHODS
    # =========================================================================

    def run_existing_emr_step(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add a step to an EXISTING EMR cluster.
        """
        execution_config = config.get('execution', {})
        cluster_id = execution_config.get('emr_cluster_id')

        if not cluster_id:
            return {
                'status': 'failed',
                'error': 'No emr_cluster_id specified. Set execution.emr_cluster_id or use create_job=true.'
            }

        scripts_config = config.get('scripts', {})
        script_location = scripts_config.get('pyspark') or scripts_config.get('script_path')

        if not script_location:
            return {
                'status': 'failed',
                'error': 'No script location specified for EMR step.'
            }

        try:
            # Verify cluster exists and is running
            cluster_response = self.emr.describe_cluster(ClusterId=cluster_id)
            cluster_state = cluster_response['Cluster']['Status']['State']

            if cluster_state not in ['RUNNING', 'WAITING']:
                return {
                    'status': 'failed',
                    'error': f"EMR cluster '{cluster_id}' is not running (state: {cluster_state})"
                }

            # Build step
            step_name = execution_config.get('step_name', f'strands-etl-step-{uuid.uuid4().hex[:8]}')
            spark_args = ['spark-submit']

            # Add Spark configuration
            if execution_config.get('spark_conf'):
                for key, value in execution_config['spark_conf'].items():
                    spark_args.extend(['--conf', f'{key}={value}'])

            spark_args.append(script_location)

            # Add script arguments
            if execution_config.get('script_arguments'):
                spark_args.extend(execution_config['script_arguments'])

            step = {
                'Name': step_name,
                'ActionOnFailure': execution_config.get('action_on_failure', 'CONTINUE'),
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': spark_args
                }
            }

            # Add step to cluster
            response = self.emr.add_job_flow_steps(
                JobFlowId=cluster_id,
                Steps=[step]
            )
            step_id = response['StepIds'][0]

            logger.info(f"Added step '{step_name}' to EMR cluster '{cluster_id}' with step ID: {step_id}")

            return {
                'platform': 'emr',
                'cluster_id': cluster_id,
                'step_id': step_id,
                'step_name': step_name,
                'status': 'running',
                'execution_type': 'existing_cluster'
            }

        except Exception as e:
            logger.error(f"Failed to add EMR step: {e}")
            return {'status': 'failed', 'error': str(e)}

    def create_and_run_emr_cluster(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a NEW EMR cluster and run the job.
        """
        execution_config = config.get('execution', {})
        scripts_config = config.get('scripts', {})

        cluster_name = execution_config.get('cluster_name', f"strands-etl-cluster-{uuid.uuid4().hex[:8]}")
        script_location = scripts_config.get('pyspark') or scripts_config.get('script_path')

        if not script_location:
            return {
                'status': 'failed',
                'error': 'No script location specified for EMR cluster.'
            }

        try:
            cluster_params = {
                'Name': cluster_name,
                'ReleaseLabel': execution_config.get('release_label', 'emr-6.15.0'),
                'Instances': {
                    'MasterInstanceType': execution_config.get('master_instance_type', 'm5.xlarge'),
                    'SlaveInstanceType': execution_config.get('worker_instance_type', 'm5.xlarge'),
                    'InstanceCount': execution_config.get('instance_count', 3),
                    'KeepJobFlowAliveWhenNoSteps': False,
                    'TerminationProtected': False
                },
                'Steps': [
                    {
                        'Name': 'ETL Step',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit', '--master', 'yarn', script_location]
                        }
                    }
                ],
                'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
                'ServiceRole': execution_config.get('service_role', 'EMR_DefaultRole'),
                'JobFlowRole': execution_config.get('job_flow_role', 'EMR_EC2_DefaultRole'),
                'VisibleToAllUsers': True
            }

            # Add logging if specified
            if execution_config.get('log_uri'):
                cluster_params['LogUri'] = execution_config['log_uri']

            response = self.emr.run_job_flow(**cluster_params)
            cluster_id = response['JobFlowId']

            logger.info(f"Created EMR cluster '{cluster_name}' with ID: {cluster_id}")

            return {
                'platform': 'emr',
                'cluster_id': cluster_id,
                'cluster_name': cluster_name,
                'status': 'running',
                'execution_type': 'new_cluster_created'
            }

        except Exception as e:
            logger.error(f"Failed to create EMR cluster: {e}")
            return {'status': 'failed', 'error': str(e)}

    # =========================================================================
    # LAMBDA EXECUTION METHODS
    # =========================================================================

    def invoke_lambda_function(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invoke an EXISTING Lambda function.
        Lambda always invokes existing functions (no create option).
        """
        execution_config = config.get('execution', {})
        function_name = execution_config.get('lambda_function_name') or config.get('lambda_function_name')

        if not function_name:
            return {
                'status': 'failed',
                'error': 'No lambda_function_name specified in execution config.'
            }

        try:
            # Build payload
            payload = execution_config.get('payload', {})
            payload.update({
                'config_path': config.get('config_s3_path', ''),
                'job_name': f"strands-lambda-etl-{uuid.uuid4().hex[:8]}",
                'timestamp': datetime.utcnow().isoformat()
            })

            # Determine invocation type
            invocation_type = execution_config.get('invocation_type', 'RequestResponse')

            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=json.dumps(payload)
            )

            if invocation_type == 'RequestResponse':
                result = json.loads(response['Payload'].read())
                status = 'completed' if response.get('StatusCode') == 200 else 'failed'
            else:
                result = {'async': True, 'request_id': response.get('ResponseMetadata', {}).get('RequestId')}
                status = 'submitted'

            logger.info(f"Lambda function '{function_name}' invoked successfully")

            return {
                'platform': 'lambda',
                'function_name': function_name,
                'status': status,
                'result': result,
                'execution_type': 'existing_function'
            }

        except Exception as e:
            logger.error(f"Lambda invocation failed: {e}")
            return {'status': 'failed', 'error': str(e)}

    # =========================================================================
    # MONITORING METHODS
    # =========================================================================

    def monitor_job_execution(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor job execution until completion."""
        platform = execution_result.get('platform', 'glue')

        if platform == 'glue':
            return self.monitor_glue_job(execution_result)
        elif platform == 'emr':
            if execution_result.get('step_id'):
                return self.monitor_emr_step(execution_result)
            else:
                return self.monitor_emr_cluster(execution_result)
        elif platform == 'lambda':
            # Lambda RequestResponse is synchronous, already completed
            return execution_result
        else:
            logger.warning(f"Unknown platform {platform}, returning as-is")
            return execution_result

    def monitor_glue_job(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor Glue job execution until completion."""
        job_name = execution_result.get('job_name')
        run_id = execution_result.get('run_id')

        if not job_name or not run_id:
            logger.error("Missing job_name or run_id in execution result")
            return execution_result

        logger.info(f"Monitoring Glue job {job_name} with run ID {run_id}")

        import time
        max_wait_time = 3600
        poll_interval = 30
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                response = self.glue.get_job_run(JobName=job_name, RunId=run_id)
                job_run = response['JobRun']
                status = job_run['JobRunState']

                logger.info(f"Job {job_name} status: {status}")

                if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    # Extract key metrics from job_run
                    job_run_details = {
                        'JobRunState': job_run.get('JobRunState'),
                        'ExecutionTime': job_run.get('ExecutionTime'),
                        'DPUSeconds': job_run.get('DPUSeconds'),
                        'NumberOfWorkers': job_run.get('NumberOfWorkers'),
                        'WorkerType': job_run.get('WorkerType'),
                        'AllocatedCapacity': job_run.get('AllocatedCapacity'),
                        'MaxCapacity': job_run.get('MaxCapacity'),
                        'StartedOn': job_run.get('StartedOn').isoformat() if job_run.get('StartedOn') else None,
                        'CompletedOn': job_run.get('CompletedOn').isoformat() if job_run.get('CompletedOn') else None,
                        'ErrorMessage': job_run.get('ErrorMessage')
                    }

                    logger.info(f"Job metrics - ExecutionTime: {job_run_details.get('ExecutionTime')}s, "
                               f"DPUSeconds: {job_run_details.get('DPUSeconds')}, "
                               f"Workers: {job_run_details.get('NumberOfWorkers')}")

                    execution_result.update({
                        'final_status': status,
                        'completion_time': datetime.utcnow().isoformat(),
                        'job_run_details': job_run_details
                    })

                    if status == 'SUCCEEDED':
                        execution_result['status'] = 'completed'
                    else:
                        execution_result['status'] = 'failed'
                        execution_result['error'] = job_run.get('ErrorMessage', f'Job {status}')

                    return execution_result

                time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error monitoring Glue job: {e}")
                execution_result.update({'status': 'failed', 'error': f'Monitoring failed: {str(e)}'})
                return execution_result

        execution_result.update({'status': 'failed', 'error': f'Monitoring timeout after {max_wait_time} seconds'})
        return execution_result

    def monitor_emr_step(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor EMR step execution on existing cluster."""
        cluster_id = execution_result.get('cluster_id')
        step_id = execution_result.get('step_id')

        if not cluster_id or not step_id:
            logger.error("Missing cluster_id or step_id")
            return execution_result

        logger.info(f"Monitoring EMR step {step_id} on cluster {cluster_id}")

        import time
        max_wait_time = 7200
        poll_interval = 60
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                response = self.emr.describe_step(ClusterId=cluster_id, StepId=step_id)
                step = response['Step']
                status = step['Status']['State']

                logger.info(f"EMR step {step_id} status: {status}")

                if status in ['COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED']:
                    execution_result.update({
                        'final_status': status,
                        'completion_time': datetime.utcnow().isoformat(),
                        'step_details': step
                    })

                    if status == 'COMPLETED':
                        execution_result['status'] = 'completed'
                    else:
                        execution_result['status'] = 'failed'
                        failure_details = step.get('Status', {}).get('FailureDetails', {})
                        execution_result['error'] = failure_details.get('Message', f'Step {status}')

                    return execution_result

                time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error monitoring EMR step: {e}")
                execution_result.update({'status': 'failed', 'error': f'Monitoring failed: {str(e)}'})
                return execution_result

        execution_result.update({'status': 'failed', 'error': f'Monitoring timeout after {max_wait_time} seconds'})
        return execution_result

    def monitor_emr_cluster(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Monitor EMR cluster execution until completion."""
        cluster_id = execution_result.get('cluster_id')

        if not cluster_id:
            logger.error("Missing cluster_id in execution result")
            return execution_result

        logger.info(f"Monitoring EMR cluster {cluster_id}")

        import time
        max_wait_time = 7200
        poll_interval = 60
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                cluster_response = self.emr.describe_cluster(ClusterId=cluster_id)
                cluster = cluster_response['Cluster']
                cluster_status = cluster['Status']['State']

                logger.info(f"EMR cluster {cluster_id} status: {cluster_status}")

                if cluster_status in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
                    steps_response = self.emr.list_steps(ClusterId=cluster_id)
                    steps = steps_response.get('Steps', [])

                    step_status = 'UNKNOWN'
                    step_details = {}
                    if steps:
                        latest_step = steps[0]
                        step_status = latest_step['Status']['State']
                        step_details = latest_step

                    execution_result.update({
                        'final_status': cluster_status,
                        'step_status': step_status,
                        'completion_time': datetime.utcnow().isoformat(),
                        'cluster_details': cluster,
                        'step_details': step_details
                    })

                    if cluster_status == 'TERMINATED' and step_status == 'COMPLETED':
                        execution_result['status'] = 'completed'
                    else:
                        execution_result['status'] = 'failed'
                        error_msg = cluster.get('Status', {}).get('StateChangeReason', {}).get('Message', f'Cluster {cluster_status}')
                        execution_result['error'] = error_msg

                    return execution_result

                time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error monitoring EMR cluster: {e}")
                execution_result.update({'status': 'failed', 'error': f'Monitoring failed: {str(e)}'})
                return execution_result

        execution_result.update({'status': 'failed', 'error': f'Monitoring timeout after {max_wait_time} seconds'})
        return execution_result

    # =========================================================================
    # OTHER AGENTS (Quality, Optimization, Learning, Orchestrator)
    # =========================================================================

    def orchestrator_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Main orchestrator agent that coordinates the ETL pipeline."""
        user_request = context.get('user_request', '')
        config = context.get('config', {})

        prompt = f"""
        As the ETL Orchestrator Agent, analyze this user request and determine the required ETL operations:

        User Request: {user_request}

        Current Configuration: {json.dumps(config, indent=2, cls=DateTimeEncoder)}

        Determine:
        1. What data sources need to be processed
        2. What transformations are required
        3. What quality checks should be performed
        4. What compliance requirements exist

        Provide a structured plan for the ETL pipeline execution.
        """

        response = self.invoke_bedrock_agent(prompt, "orchestrator")
        return self.parse_agent_response(response)

    def quality_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Quality agent that monitors data quality and compliance."""
        execution_result = context.get('execution_result', {})
        historical_reports = self.get_quality_reports(limit=5)

        prompt = f"""
        As the Quality Agent, analyze the ETL execution results:

        Execution Results: {json.dumps(execution_result, indent=2, cls=DateTimeEncoder)}
        Historical Reports: {json.dumps(historical_reports, indent=2, cls=DateTimeEncoder)}

        Provide quality assessment with overall_score (0.0-1.0).
        """

        response = self.invoke_bedrock_agent(prompt, "quality")
        quality_report = self.parse_agent_response(response)

        if not quality_report.get('overall_score'):
            quality_report['overall_score'] = 0.95

        return quality_report

    def optimization_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Optimization agent that provides recommendations for improvement."""
        current_metrics = context.get('current_metrics', {})
        learning_vectors = self.get_learning_vectors(limit=5)

        prompt = f"""
        As the Optimization Agent, analyze pipeline performance:

        Current Metrics: {json.dumps(current_metrics, indent=2, cls=DateTimeEncoder)}
        Learning Vectors: {json.dumps(learning_vectors, indent=2, cls=DateTimeEncoder)}

        Provide optimization recommendations with efficiency_score and cost_efficiency (0.0-1.0).
        """

        response = self.invoke_bedrock_agent(prompt, "optimization")
        optimization_report = self.parse_agent_response(response)

        if not optimization_report.get('efficiency_score'):
            optimization_report['efficiency_score'] = 0.85
        if not optimization_report.get('cost_efficiency'):
            optimization_report['cost_efficiency'] = 0.80

        return optimization_report

    def learning_agent(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Learning agent that captures execution patterns."""
        execution_result = context.get('execution_result', {})
        quality_report = context.get('quality_report', {})
        optimization = context.get('optimization', {})

        learning_vector = {
            'vector_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'pipeline_id': context.get('pipeline_id'),
            'workload': context.get('config', {}).get('workload', {}),
            'execution': {
                'platform': execution_result.get('platform'),
                'execution_type': execution_result.get('execution_type'),
                'job_name': execution_result.get('job_name'),
                'status': execution_result.get('status'),
                'final_status': execution_result.get('final_status'),
                'error': execution_result.get('error')
            },
            'metrics': {
                'execution_time_seconds': self._calculate_execution_time(context),
                'estimated_cost_usd': self._estimate_cost_from_result(execution_result)
            },
            'quality': {
                'overall_score': quality_report.get('overall_score', 0.95)
            },
            'optimization': {
                'efficiency_score': optimization.get('efficiency_score', 0.8),
                'cost_efficiency': optimization.get('cost_efficiency', 0.85)
            },
            'agent_decisions': {
                'mode': context.get('agent_mode'),
                'selected_platform': context.get('selected_platform')
            }
        }

        # Store learning vector (local or S3)
        self._store_learning_vector(learning_vector)

        return {'learning_vector': learning_vector}

    def _calculate_execution_time(self, context: Dict[str, Any]) -> Optional[float]:
        """Calculate execution time from context or job run details."""
        execution_result = context.get('execution_result', {})
        job_run = execution_result.get('job_run_details', {})

        # Try to get ExecutionTime from Glue job run (in seconds)
        if job_run.get('ExecutionTime'):
            return float(job_run['ExecutionTime'])

        # Try to calculate from Glue job StartedOn/CompletedOn
        if job_run.get('StartedOn') and job_run.get('CompletedOn'):
            try:
                started = job_run['StartedOn']
                completed = job_run['CompletedOn']
                # Handle both datetime objects and strings
                if isinstance(started, str):
                    started = datetime.fromisoformat(started.replace('Z', '+00:00'))
                if isinstance(completed, str):
                    completed = datetime.fromisoformat(completed.replace('Z', '+00:00'))
                return (completed - started).total_seconds()
            except:
                pass

        # Fallback to context start/end times
        try:
            start_time = context.get('start_time')
            end_time = context.get('end_time')
            if start_time and end_time:
                start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                return (end - start).total_seconds()
        except:
            pass

        return None

    def _estimate_cost_from_result(self, execution_result: Dict[str, Any]) -> Optional[float]:
        """Estimate cost from execution result."""
        job_run = execution_result.get('job_run_details', {})
        platform = execution_result.get('platform', 'glue')

        # Try DPUSeconds first (Glue)
        dpu_seconds = job_run.get('DPUSeconds')
        if dpu_seconds:
            # Glue pricing: ~$0.44 per DPU-hour
            dpu_hours = dpu_seconds / 3600
            return round(dpu_hours * 0.44, 4)

        # Estimate from ExecutionTime and worker config (Glue)
        exec_time = job_run.get('ExecutionTime')
        num_workers = job_run.get('NumberOfWorkers') or job_run.get('AllocatedCapacity')
        if exec_time and num_workers:
            # Estimate: ExecutionTime * NumberOfWorkers * rate
            dpu_hours = (exec_time * num_workers) / 3600
            return round(dpu_hours * 0.44, 4)

        # For Lambda, estimate from duration
        if platform == 'lambda':
            duration_ms = execution_result.get('result', {}).get('duration_ms', 0)
            memory_mb = execution_result.get('result', {}).get('memory_mb', 128)
            if duration_ms:
                # Lambda pricing: ~$0.0000166667 per GB-second
                gb_seconds = (duration_ms / 1000) * (memory_mb / 1024)
                return round(gb_seconds * 0.0000166667, 6)

        return None

    def _store_learning_vector(self, learning_vector: Dict[str, Any]) -> None:
        """Store learning vector to local storage or S3."""
        vector_id = learning_vector['vector_id']

        if self.storage_mode == 'local':
            try:
                file_path = os.path.join(
                    self.local_storage_path,
                    'learning', 'vectors',
                    f"{vector_id}.json"
                )
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'w') as f:
                    json.dump(learning_vector, f, indent=2, cls=DateTimeEncoder)
                logger.info(f"Learning vector stored locally: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to store learning vector locally: {e}")
        else:
            try:
                learning_bucket = 'strands-etl-learning'
                s3_key = f"learning/vectors/{vector_id}.json"
                self.s3.put_object(
                    Bucket=learning_bucket,
                    Key=s3_key,
                    Body=json.dumps(learning_vector, indent=2, cls=DateTimeEncoder)
                )
                logger.info(f"Learning vector stored: s3://{learning_bucket}/{s3_key}")
            except Exception as e:
                logger.warning(f"Failed to store learning vector to S3: {e}")

    # =========================================================================
    # MAIN ORCHESTRATION METHOD
    # =========================================================================

    def orchestrate_pipeline(
        self,
        user_request: str,
        config_path: str,
        agent_mode: str = 'recommend',
        execute: bool = True
    ) -> Dict[str, Any]:
        """
        Main orchestration method for the ETL pipeline.

        Args:
            user_request: Description of what the ETL should do
            config_path: Path to configuration file (local or S3)
            agent_mode: 'recommend' (default) or 'decide'
            execute: Whether to actually execute the job (False for dry-run)

        Returns:
            Pipeline execution context with all results
        """
        config = self.load_config(config_path)

        context = {
            'user_request': user_request,
            'config': config,
            'pipeline_id': str(uuid.uuid4()),
            'start_time': datetime.utcnow().isoformat(),
            'agent_mode': agent_mode
        }

        try:
            # Step 1: Analyze request
            logger.info("Step 1: Analyzing user request")
            orchestration_plan = self.orchestrator_agent(context)
            context['orchestration_plan'] = orchestration_plan

            # Step 2: Platform decision/recommendation
            logger.info(f"Step 2: Platform {'decision' if agent_mode == 'decide' else 'recommendation'}")
            decision_context = {
                'workload': config.get('workload', {}),
                'agent_mode': agent_mode,
                'available_platforms': config.get('execution', {}).get('available_platforms', ['glue', 'emr', 'lambda'])
            }
            decision = self.decision_agent(decision_context)
            context['platform_decision'] = decision

            # Determine platform
            if agent_mode == 'decide':
                selected_platform = decision.get('selected_platform', 'glue')
                logger.info(f"Agent selected platform: {selected_platform}")
            else:
                selected_platform = config.get('execution', {}).get('platform', 'glue')
                logger.info(f"Using configured platform: {selected_platform}")
                logger.info(f"Agent recommendations: {json.dumps(decision.get('recommendations', []), indent=2)}")

            context['selected_platform'] = selected_platform

            if not execute:
                logger.info("Dry-run mode - skipping execution")
                context['status'] = 'dry_run'
                context['end_time'] = datetime.utcnow().isoformat()
                return context

            # Step 3: Execute
            logger.info(f"Step 3: Executing on {selected_platform}")
            execution_result = self.execution_agent({
                'platform': selected_platform,
                'config': config
            })
            context['execution_result'] = execution_result

            if execution_result.get('status') == 'failed':
                context['status'] = 'failed'
                context['error'] = execution_result.get('error')
                context['end_time'] = datetime.utcnow().isoformat()
                return context

            # Step 4: Monitor
            logger.info("Step 4: Monitoring execution")
            execution_result = self.monitor_job_execution(execution_result)
            context['execution_result'] = execution_result

            if execution_result.get('status') == 'failed':
                context['status'] = 'failed'
                context['error'] = execution_result.get('error')
                context['end_time'] = datetime.utcnow().isoformat()
                return context

            # Step 5: Quality assessment
            logger.info("Step 5: Quality assessment")
            quality_report = self.quality_agent({'execution_result': execution_result})
            context['quality_report'] = quality_report

            # Step 6: Optimization
            logger.info("Step 6: Optimization recommendations")
            optimization = self.optimization_agent(context)
            context['optimization'] = optimization

            # Step 7: Learning
            logger.info("Step 7: Capturing learning insights")
            learning = self.learning_agent(context)
            context['learning'] = learning

            context['status'] = 'completed'
            context['end_time'] = datetime.utcnow().isoformat()

            logger.info("Pipeline completed successfully")
            return context

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            context['status'] = 'failed'
            context['error'] = str(e)
            context['end_time'] = datetime.utcnow().isoformat()
            return context

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def get_platform_recommendation(self, config_path: str) -> Dict[str, Any]:
        """
        Get platform recommendation without executing.
        Useful for planning and cost estimation.
        """
        config = self.load_config(config_path)

        context = {
            'workload': config.get('workload', {}),
            'script_info': {
                'script_path': config.get('scripts', {}).get('pyspark'),
                'script_type': 'pyspark'
            },
            'current_platform': config.get('execution', {}).get('platform', 'unknown')
        }

        recommendation = self.platform_advisor_agent(context)
        return recommendation

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from S3 or local path."""
        if config_path.startswith('s3://'):
            bucket, key = config_path.replace('s3://', '').split('/', 1)
            response = self.s3.get_object(Bucket=bucket, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        else:
            with open(config_path, 'r') as f:
                return json.load(f)

    def invoke_bedrock_agent(self, prompt: str, agent_type: str) -> str:
        """Invoke Bedrock AI for agent reasoning."""
        try:
            system_prompts = {
                'orchestrator': "You are an expert ETL orchestrator with deep knowledge of data pipelines.",
                'decision': "You are a platform selection expert specializing in AWS analytics services. Always respond in valid JSON.",
                'platform_advisor': "You are an AWS platform specialist. Analyze workloads and recommend optimal platforms. Always respond in valid JSON.",
                'quality': "You are a data quality expert. Always respond in valid JSON.",
                'optimization': "You are a performance optimization expert. Always respond in valid JSON.",
                'learning': "You are a machine learning specialist analyzing ETL patterns."
            }

            full_prompt = f"{system_prompts.get(agent_type, '')}\n\n{prompt}"

            response = self.bedrock.invoke_model(
                modelId='anthropic.claude-3-sonnet-20240229-v1:0',
                body=json.dumps({
                    'anthropic_version': 'bedrock-2023-05-31',
                    'max_tokens': 1500,
                    'messages': [{'role': 'user', 'content': full_prompt}]
                })
            )

            result = json.loads(response['body'].read())
            return result['content'][0]['text']

        except Exception as e:
            logger.error(f"Bedrock invocation failed: {e}")
            return f'{{"error": "{str(e)}"}}'

    def parse_agent_response(self, response: str) -> Dict[str, Any]:
        """Parse AI agent response into structured format."""
        try:
            # Try to extract JSON from response
            if '```json' in response:
                json_str = response.split('```json')[1].split('```')[0].strip()
                return json.loads(json_str)
            elif '{' in response:
                # Find JSON object in response
                start = response.find('{')
                end = response.rfind('}') + 1
                if start != -1 and end > start:
                    return json.loads(response[start:end])
            return json.loads(response)
        except json.JSONDecodeError:
            return {'response': response, 'parsed': False, 'timestamp': datetime.utcnow().isoformat()}

    def get_learning_vectors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent learning vectors from local storage or S3."""
        vectors = []

        if self.storage_mode == 'local':
            try:
                vectors_dir = os.path.join(self.local_storage_path, 'learning', 'vectors')
                if os.path.exists(vectors_dir):
                    files = sorted(
                        [f for f in os.listdir(vectors_dir) if f.endswith('.json')],
                        key=lambda x: os.path.getmtime(os.path.join(vectors_dir, x)),
                        reverse=True
                    )[:limit]
                    for filename in files:
                        try:
                            with open(os.path.join(vectors_dir, filename), 'r') as f:
                                vectors.append(json.load(f))
                        except:
                            pass
            except:
                pass
        else:
            try:
                response = self.s3.list_objects_v2(
                    Bucket='strands-etl-learning',
                    Prefix='learning/vectors/',
                    MaxKeys=limit
                )
                for obj in response.get('Contents', [])[:limit]:
                    try:
                        data = self.s3.get_object(Bucket='strands-etl-learning', Key=obj['Key'])
                        vectors.append(json.loads(data['Body'].read().decode('utf-8')))
                    except:
                        pass
            except:
                pass

        return vectors

    def get_quality_reports(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent quality reports from local storage or S3."""
        reports = []

        if self.storage_mode == 'local':
            try:
                reports_dir = os.path.join(self.local_storage_path, 'quality', 'reports')
                if os.path.exists(reports_dir):
                    files = sorted(
                        [f for f in os.listdir(reports_dir) if f.endswith('.json')],
                        key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)),
                        reverse=True
                    )[:limit]
                    for filename in files:
                        try:
                            with open(os.path.join(reports_dir, filename), 'r') as f:
                                reports.append(json.load(f))
                        except:
                            pass
            except:
                pass
        else:
            try:
                response = self.s3.list_objects_v2(
                    Bucket='strands-etl-learning',
                    Prefix='quality/reports/',
                    MaxKeys=limit
                )
                for obj in response.get('Contents', [])[:limit]:
                    try:
                        data = self.s3.get_object(Bucket='strands-etl-learning', Key=obj['Key'])
                        reports.append(json.loads(data['Body'].read().decode('utf-8')))
                    except:
                        pass
            except:
                pass

        return reports

    def get_optimization_recommendations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent optimization recommendations from local storage or S3."""
        recs = []

        if self.storage_mode == 'local':
            try:
                recs_dir = os.path.join(self.local_storage_path, 'optimization', 'recommendations')
                if os.path.exists(recs_dir):
                    files = sorted(
                        [f for f in os.listdir(recs_dir) if f.endswith('.json')],
                        key=lambda x: os.path.getmtime(os.path.join(recs_dir, x)),
                        reverse=True
                    )[:limit]
                    for filename in files:
                        try:
                            with open(os.path.join(recs_dir, filename), 'r') as f:
                                recs.append(json.load(f))
                        except:
                            pass
            except:
                pass
        else:
            try:
                response = self.s3.list_objects_v2(
                    Bucket='strands-etl-learning',
                    Prefix='optimization/recommendations/',
                    MaxKeys=limit
                )
                for obj in response.get('Contents', [])[:limit]:
                    try:
                        data = self.s3.get_object(Bucket='strands-etl-learning', Key=obj['Key'])
                        recs.append(json.loads(data['Body'].read().decode('utf-8')))
                    except:
                        pass
            except:
                pass

        return recs


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Example usage of Strands Orchestrator."""
    import argparse

    parser = argparse.ArgumentParser(description='Strands ETL Orchestrator')
    parser.add_argument('--config', '-c', required=True, help='Path to config file')
    parser.add_argument('--request', '-r', default='Execute ETL job', help='User request')
    parser.add_argument('--mode', '-m', choices=['recommend', 'decide'], default='recommend', help='Agent mode')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without execution')
    parser.add_argument('--recommend-only', action='store_true', help='Get platform recommendation only')
    parser.add_argument('--storage', '-s', choices=['local', 's3'], default='local',
                        help='Storage mode for learning data: local (default) or s3')
    parser.add_argument('--local-path', help='Local storage path (default: ./learning_data)')

    args = parser.parse_args()

    orchestrator = StrandsOrchestrator(
        storage_mode=args.storage,
        local_storage_path=args.local_path
    )

    if args.recommend_only:
        print("Getting platform recommendation...")
        recommendation = orchestrator.get_platform_recommendation(args.config)
        print(json.dumps(recommendation, indent=2, cls=DateTimeEncoder))
    else:
        print(f"Running pipeline in {args.mode} mode...")
        result = orchestrator.orchestrate_pipeline(
            user_request=args.request,
            config_path=args.config,
            agent_mode=args.mode,
            execute=not args.dry_run
        )
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))


if __name__ == '__main__':
    main()
