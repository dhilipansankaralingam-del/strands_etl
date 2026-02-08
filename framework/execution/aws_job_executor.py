#!/usr/bin/env python3
"""
AWS Job Executor
================

Actual execution of Glue and EMR jobs with:
1. Pre-flight validation (job exists, configs valid)
2. Real job triggering and monitoring
3. Metrics collection for agent training
4. Platform fallback handling
5. Error notification before execution starts
"""

import os
import sys
import json
import time
import boto3
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum


class Platform(Enum):
    GLUE = "glue"
    EMR = "emr"
    EKS = "eks"
    LOCAL = "local"


class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    STOPPED = "STOPPED"


@dataclass
class ValidationResult:
    """Result of pre-flight validation."""
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    job_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionMetrics:
    """Metrics collected from job execution."""
    execution_id: str
    job_name: str
    platform: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    # Resource metrics
    dpu_hours: float = 0.0
    executor_count: int = 0
    driver_memory_gb: float = 0.0
    executor_memory_gb: float = 0.0

    # Data metrics
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_bytes: int = 0
    records_read: int = 0
    records_written: int = 0

    # Cost metrics
    estimated_cost: float = 0.0

    # Error info
    error_message: str = ""
    error_category: str = ""


class AWSJobExecutor:
    """
    Executes actual AWS Glue and EMR jobs.
    """

    def __init__(self, config: Dict[str, Any], region: str = None):
        self.config = config
        self.region = region or os.getenv('AWS_REGION', 'us-east-1')

        # Initialize AWS clients
        self.glue_client = None
        self.emr_client = None
        self.cloudwatch_client = None
        self.s3_client = None

        try:
            self.glue_client = boto3.client('glue', region_name=self.region)
            self.emr_client = boto3.client('emr', region_name=self.region)
            self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.region)
            self.s3_client = boto3.client('s3', region_name=self.region)
        except Exception as e:
            print(f"[WARN] AWS client initialization failed: {e}")

    def validate_before_execution(self) -> ValidationResult:
        """
        Validate all prerequisites before execution.
        Returns errors immediately if validation fails.
        """
        result = ValidationResult(valid=True)

        platform = self.config.get('platform', {}).get('primary', 'glue')
        job_name = self.config.get('job_name', '')

        print("\n" + "=" * 60)
        print("PRE-FLIGHT VALIDATION")
        print("=" * 60)

        # 1. Validate job name
        if not job_name:
            result.errors.append("Job name is required in config")
            result.valid = False

        # 2. Validate platform-specific requirements
        if platform == 'glue':
            glue_validation = self._validate_glue_job(job_name)
            if not glue_validation['valid']:
                result.errors.extend(glue_validation['errors'])
                result.valid = False
            result.warnings.extend(glue_validation.get('warnings', []))
            result.job_details = glue_validation.get('job_details', {})

        elif platform == 'emr':
            emr_validation = self._validate_emr_config()
            if not emr_validation['valid']:
                result.errors.extend(emr_validation['errors'])
                result.valid = False
            result.warnings.extend(emr_validation.get('warnings', []))

        # 3. Validate script exists
        script_validation = self._validate_script()
        if not script_validation['valid']:
            result.errors.extend(script_validation['errors'])
            result.valid = False

        # 4. Validate S3 paths
        s3_validation = self._validate_s3_paths()
        if not s3_validation['valid']:
            result.errors.extend(s3_validation['errors'])
            result.valid = False
        result.warnings.extend(s3_validation.get('warnings', []))

        # 5. Validate IAM permissions
        iam_validation = self._validate_iam_permissions()
        result.warnings.extend(iam_validation.get('warnings', []))

        # Print validation results
        if result.valid:
            print("\n✓ All validations passed")
        else:
            print("\n✗ Validation FAILED:")
            for error in result.errors:
                print(f"  ERROR: {error}")

        if result.warnings:
            print("\n⚠ Warnings:")
            for warning in result.warnings:
                print(f"  WARN: {warning}")

        return result

    def _validate_glue_job(self, job_name: str) -> Dict:
        """Validate Glue job exists and is configured correctly."""
        result = {'valid': True, 'errors': [], 'warnings': [], 'job_details': {}}

        if not self.glue_client:
            result['errors'].append("Glue client not available - check AWS credentials")
            result['valid'] = False
            return result

        print(f"\nChecking Glue job: {job_name}")

        try:
            response = self.glue_client.get_job(JobName=job_name)
            job = response['Job']
            result['job_details'] = {
                'name': job['Name'],
                'role': job.get('Role', ''),
                'script_location': job.get('Command', {}).get('ScriptLocation', ''),
                'glue_version': job.get('GlueVersion', ''),
                'worker_type': job.get('WorkerType', ''),
                'number_of_workers': job.get('NumberOfWorkers', 0),
                'timeout': job.get('Timeout', 0),
                'max_retries': job.get('MaxRetries', 0)
            }

            print(f"  ✓ Job found: {job_name}")
            print(f"    - Glue Version: {result['job_details']['glue_version']}")
            print(f"    - Worker Type: {result['job_details']['worker_type']}")
            print(f"    - Workers: {result['job_details']['number_of_workers']}")
            print(f"    - Timeout: {result['job_details']['timeout']} min")

            # Check script exists in S3
            script_location = result['job_details']['script_location']
            if script_location.startswith('s3://'):
                if not self._s3_object_exists(script_location):
                    result['errors'].append(f"Script not found at: {script_location}")
                    result['valid'] = False
                else:
                    print(f"  ✓ Script exists: {script_location}")

        except self.glue_client.exceptions.EntityNotFoundException:
            result['errors'].append(f"Glue job '{job_name}' does not exist")
            result['valid'] = False
            print(f"  ✗ Job NOT FOUND: {job_name}")

            # Suggest creating the job
            print(f"\n  To create the job, run:")
            print(f"  aws glue create-job --name {job_name} --role <role-arn> \\")
            print(f"    --command '{{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://bucket/script.py\"}}' \\")
            print(f"    --glue-version 4.0 --number-of-workers 5 --worker-type G.1X")

        except Exception as e:
            result['errors'].append(f"Error checking Glue job: {str(e)}")
            result['valid'] = False

        return result

    def _validate_emr_config(self) -> Dict:
        """Validate EMR configuration."""
        result = {'valid': True, 'errors': [], 'warnings': []}

        emr_config = self.config.get('emr_config', {})

        if not emr_config:
            result['errors'].append("EMR configuration missing in config")
            result['valid'] = False
            return result

        required_fields = ['release_label', 'instance_type']
        for field in required_fields:
            if field not in emr_config:
                result['errors'].append(f"EMR config missing required field: {field}")
                result['valid'] = False

        # Check cluster if specified
        cluster_id = emr_config.get('cluster_id')
        if cluster_id and self.emr_client:
            try:
                response = self.emr_client.describe_cluster(ClusterId=cluster_id)
                state = response['Cluster']['Status']['State']
                if state not in ['RUNNING', 'WAITING']:
                    result['errors'].append(f"EMR cluster {cluster_id} is in state: {state}")
                    result['valid'] = False
                else:
                    print(f"  ✓ EMR cluster {cluster_id} is {state}")
            except Exception as e:
                result['warnings'].append(f"Could not verify EMR cluster: {str(e)}")

        return result

    def _validate_script(self) -> Dict:
        """Validate script exists."""
        result = {'valid': True, 'errors': []}

        script_config = self.config.get('script', {})
        local_path = script_config.get('local_path', '')
        s3_path = script_config.get('path', '')

        print(f"\nChecking script:")

        # Check local path
        if local_path:
            if os.path.exists(local_path):
                print(f"  ✓ Local script exists: {local_path}")
            else:
                result['errors'].append(f"Local script not found: {local_path}")
                result['valid'] = False
                print(f"  ✗ Local script NOT FOUND: {local_path}")

        # Check S3 path
        if s3_path and s3_path.startswith('s3://'):
            if self._s3_object_exists(s3_path):
                print(f"  ✓ S3 script exists: {s3_path}")
            else:
                result['errors'].append(f"S3 script not found: {s3_path}")
                result['valid'] = False
                print(f"  ✗ S3 script NOT FOUND: {s3_path}")

        return result

    def _validate_s3_paths(self) -> Dict:
        """Validate S3 paths for source and target tables."""
        result = {'valid': True, 'errors': [], 'warnings': []}

        print(f"\nChecking S3 paths:")

        # Check source tables
        for table in self.config.get('source_tables', []):
            location = table.get('location', '')
            if location.startswith('s3://'):
                bucket, prefix = self._parse_s3_path(location)
                if bucket:
                    try:
                        response = self.s3_client.list_objects_v2(
                            Bucket=bucket,
                            Prefix=prefix,
                            MaxKeys=1
                        )
                        if response.get('KeyCount', 0) > 0:
                            print(f"  ✓ Source data exists: {location}")
                        else:
                            result['warnings'].append(f"Source path empty: {location}")
                            print(f"  ⚠ Source path empty: {location}")
                    except Exception as e:
                        result['warnings'].append(f"Cannot verify source: {location}")

        # Check target paths are writable
        for table in self.config.get('target_tables', []):
            location = table.get('location', '')
            if location.startswith('s3://'):
                bucket, prefix = self._parse_s3_path(location)
                if bucket:
                    try:
                        self.s3_client.head_bucket(Bucket=bucket)
                        print(f"  ✓ Target bucket accessible: s3://{bucket}/")
                    except Exception as e:
                        result['errors'].append(f"Target bucket not accessible: s3://{bucket}/")
                        result['valid'] = False

        return result

    def _validate_iam_permissions(self) -> Dict:
        """Check IAM permissions (best effort)."""
        result = {'warnings': []}

        # This is a best-effort check - actual permissions depend on the role
        glue_config = self.config.get('glue_config', {})
        role = glue_config.get('role', '')

        if not role:
            result['warnings'].append("No IAM role specified in config")

        return result

    def _s3_object_exists(self, s3_path: str) -> bool:
        """Check if S3 object exists."""
        if not self.s3_client:
            return False

        bucket, key = self._parse_s3_path(s3_path)
        if not bucket or not key:
            return False

        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except:
            return False

    def _parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and key."""
        if not s3_path.startswith('s3://'):
            return '', ''
        path = s3_path[5:]  # Remove 's3://'
        parts = path.split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        return bucket, key

    def execute_glue_job(self, job_name: str, arguments: Dict[str, str] = None) -> ExecutionMetrics:
        """Execute a Glue job and monitor until completion."""
        metrics = ExecutionMetrics(
            execution_id=f"{job_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            job_name=job_name,
            platform='glue',
            status='PENDING',
            start_time=datetime.utcnow()
        )

        if not self.glue_client:
            metrics.status = 'FAILED'
            metrics.error_message = "Glue client not available"
            return metrics

        print(f"\n{'=' * 60}")
        print(f"EXECUTING GLUE JOB: {job_name}")
        print(f"{'=' * 60}")

        try:
            # Start the job
            run_args = arguments or {}
            run_args.update(self.config.get('job_arguments', {}))

            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments=run_args
            )

            run_id = response['JobRunId']
            metrics.execution_id = run_id
            metrics.status = 'RUNNING'

            print(f"  Job Run ID: {run_id}")
            print(f"  Start Time: {metrics.start_time.isoformat()}")
            print(f"\n  Monitoring job progress...")

            # Monitor job until completion
            while True:
                time.sleep(30)  # Check every 30 seconds

                run_response = self.glue_client.get_job_run(
                    JobName=job_name,
                    RunId=run_id
                )

                run = run_response['JobRun']
                state = run['JobRunState']

                # Print progress
                elapsed = (datetime.utcnow() - metrics.start_time).total_seconds()
                print(f"  [{elapsed:.0f}s] Status: {state}")

                if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    metrics.status = state
                    metrics.end_time = datetime.utcnow()
                    metrics.duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()

                    # Collect metrics from job run
                    if 'ExecutionTime' in run:
                        metrics.duration_seconds = run['ExecutionTime']
                    if 'DPUSeconds' in run:
                        metrics.dpu_hours = run['DPUSeconds'] / 3600
                    if 'NumberOfWorkers' in run:
                        metrics.executor_count = run['NumberOfWorkers']

                    # Estimate cost (Glue pricing: $0.44 per DPU-hour)
                    # If DPUSeconds not available, estimate from workers and duration
                    if metrics.dpu_hours == 0 and metrics.executor_count > 0:
                        # Estimate: each worker = 1 DPU for standard type
                        metrics.dpu_hours = (metrics.duration_seconds / 3600) * metrics.executor_count
                    metrics.estimated_cost = metrics.dpu_hours * 0.44

                    # Get records from CloudWatch metrics
                    metrics.records_read = self._get_records_from_cloudwatch(
                        job_name, run_id, metrics.start_time, metrics.end_time
                    )
                    metrics.records_written = metrics.records_read  # Assume same for now

                    # If no CloudWatch data, estimate from input size
                    if metrics.records_read == 0:
                        metrics.records_read = self._estimate_records_from_config()
                        metrics.records_written = metrics.records_read

                    if state == 'FAILED':
                        metrics.error_message = run.get('ErrorMessage', 'Unknown error')
                        metrics.error_category = self._categorize_error(metrics.error_message)

                    break

            # Print final status
            print(f"\n  {'=' * 50}")
            if metrics.status == 'SUCCEEDED':
                print(f"  ✓ JOB SUCCEEDED")
            else:
                print(f"  ✗ JOB {metrics.status}")
                if metrics.error_message:
                    print(f"  Error: {metrics.error_message[:200]}")

            print(f"  Duration: {metrics.duration_seconds:.1f} seconds")
            print(f"  DPU Hours: {metrics.dpu_hours:.2f}")
            print(f"  Estimated Cost: ${metrics.estimated_cost:.2f}")
            print(f"  Records Processed: {metrics.records_read:,}")
            print(f"  {'=' * 50}")

        except Exception as e:
            metrics.status = 'FAILED'
            metrics.error_message = str(e)
            metrics.end_time = datetime.utcnow()
            metrics.duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
            print(f"  ✗ Execution failed: {e}")

        return metrics

    def execute_emr_step(self, cluster_id: str = None) -> ExecutionMetrics:
        """Execute a step on EMR cluster."""
        metrics = ExecutionMetrics(
            execution_id=f"emr_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            job_name=self.config.get('job_name', 'unknown'),
            platform='emr',
            status='PENDING',
            start_time=datetime.utcnow()
        )

        if not self.emr_client:
            metrics.status = 'FAILED'
            metrics.error_message = "EMR client not available"
            return metrics

        emr_config = self.config.get('emr_config', {})
        cluster_id = cluster_id or emr_config.get('cluster_id')

        if not cluster_id:
            metrics.status = 'FAILED'
            metrics.error_message = "No EMR cluster ID specified"
            return metrics

        print(f"\n{'=' * 60}")
        print(f"EXECUTING EMR STEP: {self.config.get('job_name')}")
        print(f"Cluster: {cluster_id}")
        print(f"{'=' * 60}")

        try:
            script_path = self.config.get('script', {}).get('path', '')
            job_args = self.config.get('job_arguments', {})

            # Build step
            step = {
                'Name': self.config.get('job_name', 'ETL-Step'),
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--master', 'yarn',
                        script_path
                    ] + [f"--{k}={v}" for k, v in job_args.items()]
                }
            }

            # Add step to cluster
            response = self.emr_client.add_job_flow_steps(
                JobFlowId=cluster_id,
                Steps=[step]
            )

            step_id = response['StepIds'][0]
            metrics.execution_id = step_id
            metrics.status = 'RUNNING'

            print(f"  Step ID: {step_id}")
            print(f"  Monitoring step progress...")

            # Monitor step
            while True:
                time.sleep(30)

                step_response = self.emr_client.describe_step(
                    ClusterId=cluster_id,
                    StepId=step_id
                )

                state = step_response['Step']['Status']['State']
                elapsed = (datetime.utcnow() - metrics.start_time).total_seconds()
                print(f"  [{elapsed:.0f}s] Status: {state}")

                if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    metrics.status = 'SUCCEEDED' if state == 'COMPLETED' else state
                    metrics.end_time = datetime.utcnow()
                    metrics.duration_seconds = elapsed

                    if state == 'FAILED':
                        failure = step_response['Step']['Status'].get('FailureDetails', {})
                        metrics.error_message = failure.get('Message', 'Unknown error')

                    break

            # Get cluster metrics for cost estimation
            try:
                cluster_response = self.emr_client.describe_cluster(ClusterId=cluster_id)
                instance_hours = cluster_response['Cluster'].get('NormalizedInstanceHours', 0)
                # Rough cost estimate: $0.10 per normalized instance hour
                metrics.estimated_cost = (metrics.duration_seconds / 3600) * instance_hours * 0.10
            except:
                pass

            print(f"\n  {'=' * 50}")
            if metrics.status == 'SUCCEEDED':
                print(f"  ✓ STEP COMPLETED")
            else:
                print(f"  ✗ STEP {metrics.status}")
            print(f"  Duration: {metrics.duration_seconds:.1f} seconds")
            print(f"  Estimated Cost: ${metrics.estimated_cost:.2f}")
            print(f"  {'=' * 50}")

        except Exception as e:
            metrics.status = 'FAILED'
            metrics.error_message = str(e)
            metrics.end_time = datetime.utcnow()
            print(f"  ✗ Execution failed: {e}")

        return metrics

    def execute_with_fallback(self) -> ExecutionMetrics:
        """Execute with platform fallback chain."""
        platform_config = self.config.get('platform', {})
        primary = platform_config.get('primary', 'glue')
        fallback_chain = platform_config.get('fallback_chain', [])
        auto_fallback = platform_config.get('auto_fallback_on_error', 'Y').upper() == 'Y'

        platforms_to_try = [primary] + fallback_chain

        for platform in platforms_to_try:
            print(f"\n[PLATFORM] Attempting execution on: {platform.upper()}")

            if platform == 'glue':
                metrics = self.execute_glue_job(self.config.get('job_name'))
            elif platform == 'emr':
                metrics = self.execute_emr_step()
            else:
                print(f"  Platform {platform} not yet implemented")
                continue

            if metrics.status == 'SUCCEEDED':
                return metrics

            if not auto_fallback:
                print(f"  Auto-fallback disabled. Stopping.")
                return metrics

            if platform != platforms_to_try[-1]:
                print(f"  Falling back to next platform...")

        return metrics

    def _categorize_error(self, error_message: str) -> str:
        """Categorize error for auto-healing."""
        error_lower = error_message.lower()

        if 'outofmemory' in error_lower or 'heap' in error_lower:
            return 'MEMORY'
        elif 'shuffle' in error_lower or 'fetch failed' in error_lower:
            return 'SHUFFLE'
        elif 'timeout' in error_lower:
            return 'TIMEOUT'
        elif 'connection' in error_lower or 'network' in error_lower:
            return 'CONNECTION'
        elif 'permission' in error_lower or 'access denied' in error_lower:
            return 'PERMISSION'
        elif 'not found' in error_lower:
            return 'NOT_FOUND'
        else:
            return 'UNKNOWN'

    def _get_records_from_cloudwatch(self, job_name: str, run_id: str,
                                      start_time: datetime, end_time: datetime) -> int:
        """Get records processed from CloudWatch metrics."""
        if not self.cloudwatch_client or not end_time:
            return 0

        try:
            # Glue publishes metrics to CloudWatch under 'Glue' namespace
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='Glue',
                MetricName='glue.driver.aggregate.recordsRead',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'JobRunId', 'Value': run_id}
                ],
                StartTime=start_time,
                EndTime=end_time + timedelta(minutes=5),  # Add buffer
                Period=3600,
                Statistics=['Sum']
            )

            if response.get('Datapoints'):
                return int(sum(dp['Sum'] for dp in response['Datapoints']))

            # Try alternative metric name
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='Glue',
                MetricName='glue.ALL.s3.filesystem.read_bytes',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'JobRunId', 'Value': run_id}
                ],
                StartTime=start_time,
                EndTime=end_time + timedelta(minutes=5),
                Period=3600,
                Statistics=['Sum']
            )

            if response.get('Datapoints'):
                # Estimate records from bytes (assume ~500 bytes per record average)
                total_bytes = sum(dp['Sum'] for dp in response['Datapoints'])
                return int(total_bytes / 500)

        except Exception as e:
            print(f"  [WARN] CloudWatch metrics not available: {e}")

        return 0

    def _estimate_records_from_config(self) -> int:
        """Estimate records from source table config."""
        total_records = 0

        for table in self.config.get('source_tables', []):
            # Check for explicit row count
            rows = table.get('estimated_rows', 0)
            if rows:
                total_records += rows
                continue

            # Estimate from size (assume ~500 bytes per record)
            size_gb = table.get('estimated_size_gb', 0)
            if size_gb:
                total_records += int(size_gb * 1024 * 1024 * 1024 / 500)

        # If still no estimate, use a reasonable default
        if total_records == 0:
            total_records = 100000  # Default assumption

        return total_records

    def get_execution_metrics_from_cloudwatch(self, job_name: str, run_id: str,
                                              start_time: datetime = None,
                                              end_time: datetime = None) -> Dict:
        """
        Get detailed metrics from CloudWatch after job completion.

        Metrics collected:
        - bytes_read: Total bytes read from sources
        - bytes_written: Total bytes written to targets
        - shuffle_bytes_read: Shuffle data read
        - shuffle_bytes_written: Shuffle data written
        - records_read: Total records read
        - records_written: Total records written
        - heap_memory_used: JVM heap memory used (bytes)
        - non_heap_memory_used: Non-heap memory used (bytes)
        - s3_bytes_read: Bytes read from S3
        - s3_bytes_written: Bytes written to S3
        """
        metrics = {}

        if not self.cloudwatch_client:
            return metrics

        try:
            # Use provided times or default to last 2 hours
            if not end_time:
                end_time = datetime.utcnow()
            if not start_time:
                start_time = end_time - timedelta(hours=2)

            # All available Glue metrics
            metric_names = [
                ('glue.driver.aggregate.bytesRead', 'bytes_read'),
                ('glue.driver.aggregate.bytesWritten', 'bytes_written'),
                ('glue.driver.aggregate.recordsRead', 'records_read'),
                ('glue.driver.aggregate.recordsWritten', 'records_written'),
                ('glue.driver.aggregate.shuffleBytesWritten', 'shuffle_bytes_written'),
                ('glue.driver.aggregate.shuffleLocalBytesRead', 'shuffle_bytes_read'),
                ('glue.driver.aggregate.elapsedTime', 'elapsed_time_ms'),
                ('glue.driver.jvm.heap.used', 'heap_memory_bytes'),
                ('glue.driver.jvm.non-heap.used', 'non_heap_memory_bytes'),
                ('glue.driver.s3.filesystem.read_bytes', 's3_bytes_read'),
                ('glue.driver.s3.filesystem.write_bytes', 's3_bytes_written'),
                ('glue.driver.aggregate.numCompletedTasks', 'tasks_completed'),
                ('glue.driver.aggregate.numFailedTasks', 'tasks_failed'),
            ]

            # Build metric queries
            metric_queries = []
            for i, (metric_name, metric_id) in enumerate(metric_names):
                metric_queries.append({
                    'Id': f'm{i}',
                    'Label': metric_id,
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'Glue',
                            'MetricName': metric_name,
                            'Dimensions': [
                                {'Name': 'JobName', 'Value': job_name},
                                {'Name': 'JobRunId', 'Value': run_id}
                            ]
                        },
                        'Period': 60,  # 1 minute granularity
                        'Stat': 'Sum'
                    }
                })

            # Fetch metrics in batches (CloudWatch allows max 500 queries)
            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time + timedelta(minutes=5)  # Buffer
            )

            for result in response['MetricDataResults']:
                label = result.get('Label', result['Id'])
                if result['Values']:
                    # Sum all datapoints
                    metrics[label] = sum(result['Values'])

            # Convert bytes to human readable for logging
            if metrics:
                print(f"\n  CloudWatch Metrics Retrieved:")
                if 'bytes_read' in metrics:
                    print(f"    Input Data: {metrics['bytes_read'] / (1024**3):.2f} GB")
                if 'bytes_written' in metrics:
                    print(f"    Output Data: {metrics['bytes_written'] / (1024**3):.2f} GB")
                if 'shuffle_bytes_written' in metrics:
                    print(f"    Shuffle Data: {metrics['shuffle_bytes_written'] / (1024**3):.2f} GB")
                if 'records_read' in metrics:
                    print(f"    Records Read: {int(metrics['records_read']):,}")
                if 'records_written' in metrics:
                    print(f"    Records Written: {int(metrics['records_written']):,}")
                if 'heap_memory_bytes' in metrics:
                    print(f"    Heap Memory: {metrics['heap_memory_bytes'] / (1024**3):.2f} GB")

        except Exception as e:
            print(f"  [WARN] Could not get CloudWatch metrics: {e}")

        return metrics

    def collect_complete_metrics(self, job_name: str, run_id: str) -> Dict:
        """
        Collect complete metrics from both Glue API and CloudWatch.
        Call this after job completion for accurate training data.
        """
        complete_metrics = {}

        if not self.glue_client:
            return complete_metrics

        try:
            # Get job run details from Glue API
            response = self.glue_client.get_job_run(
                JobName=job_name,
                RunId=run_id
            )
            run = response['JobRun']

            # Basic metrics from Glue API
            complete_metrics['execution_id'] = run_id
            complete_metrics['job_name'] = job_name
            complete_metrics['status'] = run.get('JobRunState', 'UNKNOWN')
            complete_metrics['start_time'] = run.get('StartedOn')
            complete_metrics['end_time'] = run.get('CompletedOn')

            # Duration
            if run.get('ExecutionTime'):
                complete_metrics['duration_seconds'] = run['ExecutionTime']
            elif run.get('StartedOn') and run.get('CompletedOn'):
                complete_metrics['duration_seconds'] = (
                    run['CompletedOn'] - run['StartedOn']
                ).total_seconds()

            # DPU and Cost
            complete_metrics['dpu_seconds'] = run.get('DPUSeconds', 0)
            complete_metrics['dpu_hours'] = complete_metrics['dpu_seconds'] / 3600
            complete_metrics['workers'] = run.get('NumberOfWorkers', 0)
            complete_metrics['worker_type'] = run.get('WorkerType', 'G.1X')
            complete_metrics['max_capacity'] = run.get('MaxCapacity', 0)

            # Cost calculation
            dpu_hour_cost = 0.44  # Standard
            if 'flex' in str(run.get('ExecutionClass', '')).lower():
                dpu_hour_cost = 0.29  # Flex pricing
            complete_metrics['estimated_cost'] = complete_metrics['dpu_hours'] * dpu_hour_cost

            # Memory from worker type
            memory_map = {'G.1X': 16, 'G.2X': 32, 'G.4X': 64, 'G.8X': 128, 'Standard': 16}
            complete_metrics['memory_gb'] = memory_map.get(
                complete_metrics['worker_type'], 16
            ) * complete_metrics['workers']

            # Get CloudWatch metrics
            cw_metrics = self.get_execution_metrics_from_cloudwatch(
                job_name, run_id,
                complete_metrics.get('start_time'),
                complete_metrics.get('end_time')
            )

            # Merge CloudWatch metrics
            complete_metrics['records_read'] = int(cw_metrics.get('records_read', 0))
            complete_metrics['records_written'] = int(cw_metrics.get('records_written', 0))
            complete_metrics['bytes_read'] = int(cw_metrics.get('bytes_read', 0))
            complete_metrics['bytes_written'] = int(cw_metrics.get('bytes_written', 0))
            complete_metrics['shuffle_bytes'] = int(
                cw_metrics.get('shuffle_bytes_written', 0) +
                cw_metrics.get('shuffle_bytes_read', 0)
            )
            complete_metrics['s3_bytes_read'] = int(cw_metrics.get('s3_bytes_read', 0))
            complete_metrics['s3_bytes_written'] = int(cw_metrics.get('s3_bytes_written', 0))
            complete_metrics['heap_memory_used'] = int(cw_metrics.get('heap_memory_bytes', 0))
            complete_metrics['tasks_completed'] = int(cw_metrics.get('tasks_completed', 0))
            complete_metrics['tasks_failed'] = int(cw_metrics.get('tasks_failed', 0))

            # Print summary
            print(f"\n  {'=' * 50}")
            print(f"  COMPLETE METRICS COLLECTED")
            print(f"  {'=' * 50}")
            print(f"  Duration: {complete_metrics['duration_seconds']:.0f}s ({complete_metrics['duration_seconds']/60:.1f} min)")
            print(f"  DPU Hours: {complete_metrics['dpu_hours']:.2f}")
            print(f"  Cost: ${complete_metrics['estimated_cost']:.2f}")
            print(f"  Workers: {complete_metrics['workers']} x {complete_metrics['worker_type']}")
            print(f"  Memory: {complete_metrics['memory_gb']} GB total")
            if complete_metrics['records_read']:
                print(f"  Records: {complete_metrics['records_read']:,} read, {complete_metrics['records_written']:,} written")
            if complete_metrics['bytes_read']:
                print(f"  Data: {complete_metrics['bytes_read']/(1024**3):.2f} GB in, {complete_metrics['bytes_written']/(1024**3):.2f} GB out")
            if complete_metrics['shuffle_bytes']:
                print(f"  Shuffle: {complete_metrics['shuffle_bytes']/(1024**3):.2f} GB")
            print(f"  {'=' * 50}")

        except Exception as e:
            print(f"  [ERROR] Failed to collect complete metrics: {e}")

        return complete_metrics


def validate_and_execute(config_path: str, dry_run: bool = False) -> Optional[ExecutionMetrics]:
    """
    Main entry point for validated execution.
    """
    # Load config
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Create executor
    executor = AWSJobExecutor(config)

    # Validate first
    validation = executor.validate_before_execution()

    if not validation.valid:
        print("\n" + "=" * 60)
        print("EXECUTION BLOCKED - Fix the following errors:")
        print("=" * 60)
        for error in validation.errors:
            print(f"  ✗ {error}")
        print("\nExecution will not proceed until errors are resolved.")
        return None

    if dry_run:
        print("\n[DRY RUN] Validation passed. Skipping actual execution.")
        return None

    # Execute with fallback
    metrics = executor.execute_with_fallback()

    return metrics


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='AWS Job Executor with Validation')
    parser.add_argument('--config', '-c', required=True, help='Path to config JSON')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Validate only, do not execute')

    args = parser.parse_args()

    metrics = validate_and_execute(args.config, args.dry_run)

    if metrics:
        print(f"\nFinal Status: {metrics.status}")
        sys.exit(0 if metrics.status == 'SUCCEEDED' else 1)
