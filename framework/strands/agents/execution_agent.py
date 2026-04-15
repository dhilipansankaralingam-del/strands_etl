#!/usr/bin/env python3
"""
Strands Execution Agent
=======================

Executes the actual ETL job on the target platform (Glue, EMR, EKS).
Captures job metrics from CloudWatch and tracks execution history.
"""

import time
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field

from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@dataclass
class JobExecution:
    """Job execution details."""
    job_name: str
    run_id: str
    platform: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    workers: int = 0
    worker_type: str = "G.2X"
    records_read: int = 0
    records_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    shuffle_bytes: int = 0
    cost_usd: float = 0.0
    error_message: str = ""
    # Extended metrics for learning
    skewness_ratio: float = 0.0          # shuffle_bytes / bytes_read - high = data skew
    etl_throughput_mbps: float = 0.0     # bytes_read / duration in MB/s
    avg_executor_memory_pct: float = 0.0 # avg JVM heap usage across executors
    max_executor_memory_pct: float = 0.0 # max JVM heap usage (OOM risk indicator)
    driver_memory_pct: float = 0.0       # driver JVM heap usage
    glue_stage: str = ""                 # last reported stage at time of failure/completion
    completed_stages: int = 0            # number of Spark stages completed
    failed_stages: int = 0               # number of Spark stages that failed

    def to_dict(self) -> Dict:
        return {
            'job_name': self.job_name,
            'run_id': self.run_id,
            'platform': self.platform,
            'status': self.status,
            'started_at': self.started_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_seconds': self.duration_seconds,
            'workers': self.workers,
            'worker_type': self.worker_type,
            'records_read': self.records_read,
            'records_written': self.records_written,
            'bytes_read': self.bytes_read,
            'bytes_written': self.bytes_written,
            'shuffle_bytes': self.shuffle_bytes,
            'cost_usd': self.cost_usd,
            'error_message': self.error_message,
            'skewness_ratio': self.skewness_ratio,
            'etl_throughput_mbps': self.etl_throughput_mbps,
            'avg_executor_memory_pct': self.avg_executor_memory_pct,
            'max_executor_memory_pct': self.max_executor_memory_pct,
            'driver_memory_pct': self.driver_memory_pct,
            'glue_stage': self.glue_stage,
            'completed_stages': self.completed_stages,
            'failed_stages': self.failed_stages
        }


@register_agent
class ExecutionAgent(StrandsAgent):
    """
    Executes ETL jobs on AWS Glue, EMR, or EKS.

    Responsibilities:
    - Start job execution on target platform
    - Monitor job progress
    - Collect metrics from CloudWatch
    - Calculate execution costs
    - Store execution history for learning
    """

    AGENT_NAME = "execution_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Executes ETL jobs and collects metrics"

    DEPENDENCIES = ['platform_conversion_agent']
    PARALLEL_SAFE = False  # Only one execution at a time

    # Cost per DPU-hour by worker type
    GLUE_COSTS = {
        'G.1X': 0.44,
        'G.2X': 0.88,
        'G.4X': 1.76,
        'G.8X': 3.52
    }

    # EMR costs (approximate per instance-hour)
    EMR_COSTS = {
        'm5.xlarge': 0.192,
        'm5.2xlarge': 0.384,
        'm5.4xlarge': 0.768,
        'm5.8xlarge': 1.536
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)
        self._glue_client = None
        self._cloudwatch_client = None
        self._emr_client = None

    @property
    def glue_client(self):
        if self._glue_client is None:
            try:
                import boto3
                self._glue_client = boto3.client('glue')
            except Exception as e:
                self.logger.warning(f"Could not create Glue client: {e}")
        return self._glue_client

    @property
    def cloudwatch_client(self):
        if self._cloudwatch_client is None:
            try:
                import boto3
                self._cloudwatch_client = boto3.client('cloudwatch')
            except Exception as e:
                self.logger.warning(f"Could not create CloudWatch client: {e}")
        return self._cloudwatch_client

    def execute(self, context: AgentContext) -> AgentResult:
        """Execute the job and collect metrics."""
        # Get platform and configuration
        target_platform = context.get_shared('target_platform', 'glue')
        converted_config = context.get_shared('converted_config')
        recommended_workers = context.get_shared('recommended_workers', 10)
        recommended_type = context.get_shared('recommended_worker_type', 'G.2X')

        # Check if dry run
        dry_run = context.config.get('dry_run', False)

        execution = JobExecution(
            job_name=context.job_name,
            run_id=context.execution_id,
            platform=target_platform,
            status='STARTING',
            started_at=datetime.now(timezone.utc),
            workers=recommended_workers,
            worker_type=recommended_type
        )

        try:
            if dry_run:
                # Simulate execution
                execution = self._simulate_execution(execution, context)
            else:
                # Real execution
                if target_platform == 'glue':
                    execution = self._execute_glue_job(execution, context)
                elif target_platform == 'emr':
                    execution = self._execute_emr_job(execution, context, converted_config)
                elif target_platform == 'eks':
                    execution = self._execute_eks_job(execution, context, converted_config)
                else:
                    execution.status = 'FAILED'
                    execution.error_message = f"Unknown platform: {target_platform}"

        except Exception as e:
            execution.status = 'FAILED'
            execution.error_message = str(e)
            execution.completed_at = datetime.now(timezone.utc)
            self.logger.error(f"Job execution failed: {e}")

        # Calculate final metrics
        if execution.completed_at:
            execution.duration_seconds = (
                execution.completed_at - execution.started_at
            ).total_seconds()

        execution.cost_usd = self._calculate_cost(execution)

        # Store execution data
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'executions',
            [execution.to_dict()],
            use_pipe_delimited=True
        )

        # Share full execution data with learning agent
        context.set_shared('job_execution', execution.to_dict())
        context.set_shared('job_metrics', {
            'duration_seconds': execution.duration_seconds,
            'records_read': execution.records_read,
            'records_written': execution.records_written,
            'bytes_read': execution.bytes_read,
            'bytes_written': execution.bytes_written,
            'shuffle_bytes': execution.shuffle_bytes,
            'cost_usd': execution.cost_usd,
            # Extended metrics for learning
            'skewness_ratio': execution.skewness_ratio,
            'etl_throughput_mbps': execution.etl_throughput_mbps,
            'avg_executor_memory_pct': execution.avg_executor_memory_pct,
            'max_executor_memory_pct': execution.max_executor_memory_pct,
            'driver_memory_pct': execution.driver_memory_pct,
            'completed_stages': execution.completed_stages,
            'failed_stages': execution.failed_stages,
            'job_status': execution.status,  # pass actual Glue status for learning
        })

        recommendations = []
        if execution.status == 'SUCCEEDED':
            recommendations.append(
                f"Job completed in {execution.duration_seconds:.0f}s, "
                f"cost: ${execution.cost_usd:.2f}, "
                f"throughput: {execution.etl_throughput_mbps:.1f} MB/s"
            )
        elif execution.status in self.GLUE_FAILURE_STATES:
            recommendations.append(
                f"Job ended with status={execution.status}: {execution.error_message}"
            )

        # Skewness warning (shuffle >> input bytes = partitioned skew)
        if execution.skewness_ratio > 0.5:
            recommendations.append(
                f"High data skewness detected (shuffle/read ratio={execution.skewness_ratio:.2f}). "
                "Consider repartitioning or salting join keys."
            )

        # Memory pressure warnings
        if execution.max_executor_memory_pct > 85:
            recommendations.append(
                f"Executor memory pressure: max heap={execution.max_executor_memory_pct:.0f}%. "
                "Consider larger worker type or more workers to avoid OOM."
            )
        if execution.driver_memory_pct > 80:
            recommendations.append(
                f"Driver memory pressure: heap={execution.driver_memory_pct:.0f}%. "
                "Avoid driver-side collect() calls on large datasets."
            )

        if execution.failed_stages > 0:
            recommendations.append(
                f"{execution.failed_stages} Spark stage(s) failed during execution."
            )

        # Map Glue job states to AgentStatus:
        # SUCCEEDED → COMPLETED; anything else (FAILED, STOPPED, TIMEOUT) → FAILED
        agent_status = (
            AgentStatus.COMPLETED
            if execution.status == 'SUCCEEDED'
            else AgentStatus.FAILED
        )

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=agent_status,
            output=execution.to_dict(),
            metrics={
                'duration_seconds': execution.duration_seconds,
                'records_processed': execution.records_read,
                'cost_usd': execution.cost_usd,
                'status': execution.status,
                'skewness_ratio': execution.skewness_ratio,
                'etl_throughput_mbps': execution.etl_throughput_mbps,
                'avg_executor_memory_pct': execution.avg_executor_memory_pct,
                'max_executor_memory_pct': execution.max_executor_memory_pct
            },
            recommendations=recommendations,
            errors=[execution.error_message] if execution.error_message else []
        )

    def _simulate_execution(self, execution: JobExecution, context: AgentContext) -> JobExecution:
        """Simulate job execution for dry run / demo mode."""
        self.logger.info(f"[DRY RUN] Simulating {execution.platform} job execution...")

        # Simulate based on estimated data size
        total_size_gb = context.get_shared('total_size_gb', 100)

        # Simulate processing time (1 minute per 10GB with workers)
        workers = execution.workers
        estimated_time = (total_size_gb / 10) / (workers / 10) * 60  # seconds

        # Simulate progress
        time.sleep(0.5)  # Brief pause to simulate work

        execution.completed_at = datetime.now(timezone.utc)
        execution.status = 'SUCCEEDED'
        execution.duration_seconds = estimated_time

        # Simulate metrics based on data size
        execution.records_read = int(total_size_gb * 2_000_000)  # ~2M records per GB
        execution.records_written = int(execution.records_read * 0.95)  # 5% filtered
        execution.bytes_read = int(total_size_gb * 1024 * 1024 * 1024)
        execution.bytes_written = int(execution.bytes_read * 0.8)  # Compression
        execution.shuffle_bytes = int(execution.bytes_read * 0.3)  # 30% shuffle

        return execution

    # Terminal states that end the polling loop
    GLUE_TERMINAL_STATES = {'SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR'}
    # States that are considered failures for learning/reporting purposes
    GLUE_FAILURE_STATES = {'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR'}

    def _execute_glue_job(self, execution: JobExecution, context: AgentContext) -> JobExecution:
        """Execute job on AWS Glue.

        Polls every 30 seconds until terminal state, then collects full
        CloudWatch metrics (records, bytes, shuffle, executor loads, skewness)
        for ALL terminal states — not only SUCCEEDED — so that learning agent
        can learn from failures and timeouts too.
        """
        if not self.glue_client:
            return self._simulate_execution(execution, context)

        job_name = context.job_name
        glue_config = context.config.get('glue_config', {})

        try:
            # Start job run
            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments=context.config.get('job_arguments', {}),
                NumberOfWorkers=execution.workers,
                WorkerType=execution.worker_type,
                Timeout=glue_config.get('timeout_minutes', 480)
            )

            run_id = response['JobRunId']
            execution.run_id = run_id
            execution.status = 'RUNNING'
            poll_count = 0

            self.logger.info(f"Started Glue job {job_name}, run_id: {run_id}")

            # Poll every 30 seconds until terminal state
            while True:
                time.sleep(30)
                poll_count += 1

                status_response = self.glue_client.get_job_run(
                    JobName=job_name,
                    RunId=run_id
                )

                job_run = status_response['JobRun']
                state = job_run['JobRunState']

                # Log progress every 5 polls (~2.5 min)
                if poll_count % 5 == 0:
                    self.logger.info(
                        f"Glue job {job_name} [{run_id}] state={state} "
                        f"after {poll_count * 30}s"
                    )

                if state in self.GLUE_TERMINAL_STATES:
                    execution.status = state
                    execution.completed_at = job_run.get(
                        'CompletedOn', datetime.now(timezone.utc)
                    )

                    # Capture error for any failure state
                    if state in self.GLUE_FAILURE_STATES:
                        execution.error_message = job_run.get(
                            'ErrorMessage',
                            f'Job ended with state: {state}'
                        )
                        self.logger.warning(
                            f"Glue job {job_name} ended with state={state}: "
                            f"{execution.error_message}"
                        )
                    break

            # Always collect CloudWatch metrics regardless of terminal state —
            # even failed/stopped runs have partial metrics useful for learning
            execution = self._collect_glue_metrics(execution, job_name, run_id)

            # Compute derived learning metrics
            execution = self._compute_derived_metrics(execution)

        except Exception as e:
            self.logger.error(f"Glue execution error: {e}")
            execution.status = 'FAILED'
            execution.error_message = str(e)
            execution.completed_at = datetime.now(timezone.utc)

        return execution

    def _compute_derived_metrics(self, execution: JobExecution) -> JobExecution:
        """Compute derived metrics for learning: skewness, throughput."""
        # Skewness ratio: high shuffle relative to bytes read signals data skew
        if execution.bytes_read > 0:
            execution.skewness_ratio = round(
                execution.shuffle_bytes / execution.bytes_read, 4
            )

        # ETL throughput in MB/s
        if execution.duration_seconds > 0 and execution.bytes_read > 0:
            execution.etl_throughput_mbps = round(
                (execution.bytes_read / (1024 * 1024)) / execution.duration_seconds, 2
            )

        return execution

    def _execute_emr_job(
        self,
        execution: JobExecution,
        context: AgentContext,
        emr_config: Dict
    ) -> JobExecution:
        """Execute job on EMR cluster."""
        self.logger.info("[EMR] Would execute on EMR cluster...")
        # For now, simulate EMR execution
        return self._simulate_execution(execution, context)

    def _execute_eks_job(
        self,
        execution: JobExecution,
        context: AgentContext,
        eks_config: Dict
    ) -> JobExecution:
        """Execute job on EKS with Spark Operator."""
        self.logger.info("[EKS] Would execute on EKS with Karpenter...")
        # For now, simulate EKS execution
        return self._simulate_execution(execution, context)

    def _collect_glue_metrics(
        self,
        execution: JobExecution,
        job_name: str,
        run_id: str
    ) -> JobExecution:
        """Collect metrics from CloudWatch for Glue job.

        Collects for ALL terminal states (not only SUCCEEDED) so learning agent
        receives data from failed/stopped runs too.

        Metrics collected:
        - ETL movement: recordsRead/Written, bytesRead/Written
        - Skewness indicator: shuffleBytesWritten
        - Executor loads: ALL.jvm.heap.usage (avg + max across executors)
        - Driver load: driver.jvm.heap.usage
        - Stage tracking: completedStages, failedStages
        """
        if not self.cloudwatch_client:
            return execution

        try:
            end_time = execution.completed_at or datetime.now(timezone.utc)
            start_time = execution.started_at
            # Add buffer for metrics propagation delay
            metrics_end = end_time + timedelta(minutes=5)

            dimensions = [
                {'Name': 'JobName', 'Value': job_name},
                {'Name': 'JobRunId', 'Value': run_id}
            ]

            # -- ETL movement metrics (Sum) --
            sum_metrics = [
                ('glue.driver.aggregate.recordsRead',        'records_read'),
                ('glue.driver.aggregate.recordsWritten',     'records_written'),
                ('glue.driver.aggregate.bytesRead',          'bytes_read'),
                ('glue.driver.aggregate.bytesWritten',       'bytes_written'),
                ('glue.driver.aggregate.shuffleBytesWritten','shuffle_bytes'),
                ('glue.driver.aggregate.numCompletedStages', 'completed_stages'),
                ('glue.driver.aggregate.numFailedStages',    'failed_stages'),
            ]

            for metric_name, attr_name in sum_metrics:
                try:
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace='Glue',
                        MetricName=metric_name,
                        Dimensions=dimensions,
                        StartTime=start_time,
                        EndTime=metrics_end,
                        Period=int((metrics_end - start_time).total_seconds()) + 60,
                        Statistics=['Sum']
                    )
                    if response.get('Datapoints'):
                        value = int(sum(dp['Sum'] for dp in response['Datapoints']))
                        setattr(execution, attr_name, value)
                except Exception as e:
                    self.logger.warning(f"Could not fetch metric {metric_name}: {e}")

            # -- Executor heap usage (Average + Maximum across all executors) --
            # glue.ALL.jvm.heap.usage covers all executor JVMs
            avg_heap_metrics = [
                ('glue.ALL.jvm.heap.usage',    'avg_executor_memory_pct', 'Average'),
                ('glue.ALL.jvm.heap.usage',    'max_executor_memory_pct', 'Maximum'),
                ('glue.driver.jvm.heap.usage', 'driver_memory_pct',       'Average'),
            ]

            for metric_name, attr_name, stat in avg_heap_metrics:
                try:
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace='Glue',
                        MetricName=metric_name,
                        Dimensions=dimensions,
                        StartTime=start_time,
                        EndTime=metrics_end,
                        Period=60,  # 1-min resolution for heap trends
                        Statistics=[stat]
                    )
                    if response.get('Datapoints'):
                        values = [dp[stat] for dp in response['Datapoints']]
                        # For avg_executor: take mean of all 1-min averages
                        # For max_executor: take the peak observed
                        if stat == 'Maximum':
                            result = round(max(values) * 100, 2)
                        else:
                            result = round(
                                sum(values) / len(values) * 100, 2
                            )
                        setattr(execution, attr_name, result)
                except Exception as e:
                    self.logger.warning(f"Could not fetch metric {metric_name}/{stat}: {e}")

        except Exception as e:
            self.logger.warning(f"CloudWatch metrics collection failed: {e}")

        return execution

    def _calculate_cost(self, execution: JobExecution) -> float:
        """Calculate execution cost based on platform and resources."""
        hours = execution.duration_seconds / 3600

        if execution.platform == 'glue':
            # Glue cost = workers * DPU-hour rate * hours
            dpu_rate = self.GLUE_COSTS.get(execution.worker_type, 0.88)
            return execution.workers * dpu_rate * hours

        elif execution.platform == 'emr':
            # EMR cost = instances * instance-hour rate * hours
            instance_rate = self.EMR_COSTS.get('m5.2xlarge', 0.384)
            return execution.workers * instance_rate * hours

        elif execution.platform == 'eks':
            # EKS cost estimation (compute + overhead)
            compute_cost = execution.workers * 0.40 * hours  # ~$0.40/executor-hour
            overhead = compute_cost * 0.1  # 10% EKS overhead
            return compute_cost + overhead

        return 0.0
