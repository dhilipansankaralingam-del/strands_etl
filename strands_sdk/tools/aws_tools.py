"""
AWS Tools for Strands SDK Agents
================================

These tools allow agents to interact with AWS services:
- Glue: Job execution, table metadata
- EMR: Cluster management, step execution
- EKS: Kubernetes job submission
- S3: Data storage
- CloudWatch: Metrics collection
"""

import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from strands import tool

logger = logging.getLogger("strands.tools.aws")

# Global state for dry-run mode and configuration
_dry_run_mode = False
_aws_clients = {}


def set_dry_run_mode(enabled: bool):
    """Enable or disable dry-run mode globally."""
    global _dry_run_mode
    _dry_run_mode = enabled


def _get_glue_client():
    """Get or create Glue client."""
    if 'glue' not in _aws_clients:
        try:
            import boto3
            _aws_clients['glue'] = boto3.client('glue')
        except Exception as e:
            logger.warning(f"Could not create Glue client: {e}")
            return None
    return _aws_clients['glue']


def _get_cloudwatch_client():
    """Get or create CloudWatch client."""
    if 'cloudwatch' not in _aws_clients:
        try:
            import boto3
            _aws_clients['cloudwatch'] = boto3.client('cloudwatch')
        except Exception as e:
            logger.warning(f"Could not create CloudWatch client: {e}")
            return None
    return _aws_clients['cloudwatch']


def _get_emr_client():
    """Get or create EMR client."""
    if 'emr' not in _aws_clients:
        try:
            import boto3
            _aws_clients['emr'] = boto3.client('emr')
        except Exception as e:
            logger.warning(f"Could not create EMR client: {e}")
            return None
    return _aws_clients['emr']


def _get_s3_client():
    """Get or create S3 client."""
    if 's3' not in _aws_clients:
        try:
            import boto3
            _aws_clients['s3'] = boto3.client('s3')
        except Exception as e:
            logger.warning(f"Could not create S3 client: {e}")
            return None
    return _aws_clients['s3']


@tool
def get_table_size(
    database: str,
    table_name: str,
    s3_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get the size of a table from Glue Catalog or S3.

    Args:
        database: Glue database name
        table_name: Table name to get size for
        s3_path: Optional S3 path to scan if Glue metadata unavailable

    Returns:
        Dictionary with size_bytes, size_gb, row_count (if available), source
    """
    if _dry_run_mode:
        # Simulated response for dry-run
        import random
        size_gb = random.uniform(5, 100)
        return {
            "table": table_name,
            "database": database,
            "size_bytes": int(size_gb * 1024 * 1024 * 1024),
            "size_gb": round(size_gb, 2),
            "row_count": int(size_gb * 1_000_000),
            "source": "simulated",
            "detected_at": datetime.utcnow().isoformat()
        }

    glue = _get_glue_client()
    result = {
        "table": table_name,
        "database": database,
        "size_bytes": 0,
        "size_gb": 0,
        "row_count": None,
        "source": "unknown"
    }

    # Try Glue Catalog first
    if glue:
        try:
            response = glue.get_table(DatabaseName=database, Name=table_name)
            params = response.get('Table', {}).get('Parameters', {})

            if 'sizeInBytes' in params:
                result['size_bytes'] = int(params['sizeInBytes'])
                result['size_gb'] = round(result['size_bytes'] / (1024**3), 2)
                result['source'] = 'glue_catalog'

            if 'recordCount' in params:
                result['row_count'] = int(params['recordCount'])

            if result['source'] == 'glue_catalog':
                result['detected_at'] = datetime.utcnow().isoformat()
                return result

        except Exception as e:
            logger.warning(f"Glue catalog lookup failed for {database}.{table_name}: {e}")

    # Fallback to S3 scan
    if s3_path:
        s3 = _get_s3_client()
        if s3:
            try:
                # Parse S3 path
                if s3_path.startswith('s3://'):
                    s3_path = s3_path[5:]
                bucket, prefix = s3_path.split('/', 1)

                total_size = 0
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        total_size += obj['Size']

                result['size_bytes'] = total_size
                result['size_gb'] = round(total_size / (1024**3), 2)
                result['source'] = 's3_scan'
                result['detected_at'] = datetime.utcnow().isoformat()

            except Exception as e:
                logger.warning(f"S3 scan failed for {s3_path}: {e}")

    return result


@tool
def list_glue_tables(database: str) -> List[Dict[str, Any]]:
    """
    List all tables in a Glue database.

    Args:
        database: Glue database name

    Returns:
        List of table metadata dictionaries
    """
    if _dry_run_mode:
        return [
            {"name": "customers", "type": "EXTERNAL_TABLE"},
            {"name": "orders", "type": "EXTERNAL_TABLE"},
            {"name": "products", "type": "EXTERNAL_TABLE"}
        ]

    glue = _get_glue_client()
    if not glue:
        return []

    tables = []
    try:
        paginator = glue.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database):
            for table in page.get('TableList', []):
                tables.append({
                    "name": table['Name'],
                    "type": table.get('TableType', 'UNKNOWN'),
                    "location": table.get('StorageDescriptor', {}).get('Location', ''),
                    "created": table.get('CreateTime', '').isoformat() if table.get('CreateTime') else None
                })
    except Exception as e:
        logger.error(f"Failed to list tables in {database}: {e}")

    return tables


@tool
def start_glue_job(
    job_name: str,
    arguments: Optional[Dict[str, str]] = None,
    worker_type: str = "G.2X",
    number_of_workers: int = 10,
    timeout_minutes: int = 60
) -> Dict[str, Any]:
    """
    Start an AWS Glue ETL job.

    Args:
        job_name: Name of the Glue job to start
        arguments: Job arguments as key-value pairs
        worker_type: Worker type (G.1X, G.2X, G.4X, G.8X)
        number_of_workers: Number of workers
        timeout_minutes: Job timeout in minutes

    Returns:
        Dictionary with job_run_id, status, start_time
    """
    if _dry_run_mode:
        job_run_id = f"jr_simulated_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        logger.info(f"[DRY RUN] Would start Glue job: {job_name}")
        logger.info(f"[DRY RUN] Workers: {number_of_workers} x {worker_type}")
        return {
            "job_name": job_name,
            "job_run_id": job_run_id,
            "status": "SIMULATED",
            "start_time": datetime.utcnow().isoformat(),
            "dry_run": True,
            "config": {
                "worker_type": worker_type,
                "number_of_workers": number_of_workers,
                "timeout_minutes": timeout_minutes
            }
        }

    glue = _get_glue_client()
    if not glue:
        return {"error": "Glue client not available"}

    try:
        args = arguments or {}
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=args,
            WorkerType=worker_type,
            NumberOfWorkers=number_of_workers,
            Timeout=timeout_minutes
        )

        return {
            "job_name": job_name,
            "job_run_id": response['JobRunId'],
            "status": "STARTING",
            "start_time": datetime.utcnow().isoformat(),
            "dry_run": False
        }

    except Exception as e:
        logger.error(f"Failed to start Glue job {job_name}: {e}")
        return {"error": str(e), "job_name": job_name}


@tool
def get_glue_job_status(job_name: str, job_run_id: str) -> Dict[str, Any]:
    """
    Get the status of a Glue job run.

    Args:
        job_name: Name of the Glue job
        job_run_id: Job run ID to check

    Returns:
        Dictionary with status, duration, metrics
    """
    if _dry_run_mode:
        return {
            "job_name": job_name,
            "job_run_id": job_run_id,
            "status": "SUCCEEDED",
            "duration_seconds": 300,
            "execution_time_seconds": 280,
            "dpu_seconds": 2800,
            "metrics": {
                "records_read": 10_000_000,
                "records_written": 9_500_000,
                "bytes_read": 5_000_000_000,
                "bytes_written": 4_500_000_000
            },
            "dry_run": True
        }

    glue = _get_glue_client()
    if not glue:
        return {"error": "Glue client not available"}

    try:
        response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
        run = response['JobRun']

        result = {
            "job_name": job_name,
            "job_run_id": job_run_id,
            "status": run['JobRunState'],
            "started_on": run.get('StartedOn', '').isoformat() if run.get('StartedOn') else None,
            "completed_on": run.get('CompletedOn', '').isoformat() if run.get('CompletedOn') else None,
            "execution_time_seconds": run.get('ExecutionTime', 0),
            "dpu_seconds": run.get('DPUSeconds', 0),
            "error_message": run.get('ErrorMessage'),
            "dry_run": False
        }

        if run.get('CompletedOn') and run.get('StartedOn'):
            result['duration_seconds'] = (run['CompletedOn'] - run['StartedOn']).total_seconds()

        return result

    except Exception as e:
        logger.error(f"Failed to get job status {job_name}/{job_run_id}: {e}")
        return {"error": str(e)}


@tool
def get_cloudwatch_metrics(
    job_name: str,
    job_run_id: str,
    metric_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get CloudWatch metrics for a Glue job run.

    Args:
        job_name: Name of the Glue job
        job_run_id: Job run ID
        metric_names: List of metrics to fetch (default: all common metrics)

    Returns:
        Dictionary with metric values
    """
    if _dry_run_mode:
        return {
            "job_name": job_name,
            "job_run_id": job_run_id,
            "metrics": {
                "glue.driver.aggregate.recordsRead": 10_000_000,
                "glue.driver.aggregate.recordsWritten": 9_500_000,
                "glue.driver.aggregate.bytesRead": 5_000_000_000,
                "glue.driver.aggregate.bytesWritten": 4_500_000_000,
                "glue.driver.aggregate.shuffleBytesWritten": 1_000_000_000,
                "glue.driver.aggregate.elapsedTime": 300_000,
                "glue.ALL.jvm.heap.usage": 0.65,
                "glue.ALL.system.cpuSystemLoad": 0.45
            },
            "dry_run": True
        }

    cloudwatch = _get_cloudwatch_client()
    if not cloudwatch:
        return {"error": "CloudWatch client not available"}

    default_metrics = [
        'glue.driver.aggregate.recordsRead',
        'glue.driver.aggregate.recordsWritten',
        'glue.driver.aggregate.bytesRead',
        'glue.driver.aggregate.bytesWritten',
        'glue.driver.aggregate.shuffleBytesWritten',
        'glue.driver.aggregate.elapsedTime'
    ]

    metrics_to_fetch = metric_names or default_metrics
    result = {
        "job_name": job_name,
        "job_run_id": job_run_id,
        "metrics": {},
        "dry_run": False
    }

    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=2)

        for metric_name in metrics_to_fetch:
            response = cloudwatch.get_metric_statistics(
                Namespace='Glue',
                MetricName=metric_name,
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'JobRunId', 'Value': job_run_id}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Maximum', 'Average']
            )

            datapoints = response.get('Datapoints', [])
            if datapoints:
                latest = max(datapoints, key=lambda x: x['Timestamp'])
                result['metrics'][metric_name] = latest.get('Maximum', latest.get('Average', 0))

    except Exception as e:
        logger.error(f"Failed to get CloudWatch metrics: {e}")
        result['error'] = str(e)

    return result


@tool
def start_emr_step(
    cluster_id: str,
    step_name: str,
    script_path: str,
    arguments: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Submit a step to an EMR cluster.

    Args:
        cluster_id: EMR cluster ID
        step_name: Name for the step
        script_path: S3 path to the Spark script
        arguments: List of script arguments

    Returns:
        Dictionary with step_id, status
    """
    if _dry_run_mode:
        step_id = f"s-simulated-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        logger.info(f"[DRY RUN] Would submit EMR step: {step_name} to cluster {cluster_id}")
        return {
            "cluster_id": cluster_id,
            "step_id": step_id,
            "step_name": step_name,
            "status": "SIMULATED",
            "script_path": script_path,
            "dry_run": True
        }

    emr = _get_emr_client()
    if not emr:
        return {"error": "EMR client not available"}

    try:
        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', script_path] + (arguments or [])
            }
        }

        response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])

        return {
            "cluster_id": cluster_id,
            "step_id": response['StepIds'][0],
            "step_name": step_name,
            "status": "PENDING",
            "dry_run": False
        }

    except Exception as e:
        logger.error(f"Failed to submit EMR step: {e}")
        return {"error": str(e)}


@tool
def submit_eks_job(
    job_name: str,
    namespace: str = "spark",
    image: str = "apache/spark:3.5.0",
    script_path: str = "",
    executor_instances: int = 10,
    executor_memory: str = "16g",
    executor_cores: int = 4
) -> Dict[str, Any]:
    """
    Submit a Spark job to EKS with Karpenter.

    Args:
        job_name: Name for the Spark application
        namespace: Kubernetes namespace
        image: Spark container image
        script_path: Path to the PySpark script
        executor_instances: Number of executors
        executor_memory: Memory per executor
        executor_cores: Cores per executor

    Returns:
        Dictionary with job submission details
    """
    if _dry_run_mode:
        logger.info(f"[DRY RUN] Would submit EKS Spark job: {job_name}")
        logger.info(f"[DRY RUN] Executors: {executor_instances} x {executor_cores} cores x {executor_memory}")
        return {
            "job_name": job_name,
            "namespace": namespace,
            "status": "SIMULATED",
            "config": {
                "executor_instances": executor_instances,
                "executor_memory": executor_memory,
                "executor_cores": executor_cores,
                "image": image
            },
            "dry_run": True,
            "submitted_at": datetime.utcnow().isoformat()
        }

    # In production, this would use kubectl or the Kubernetes Python client
    # to submit a SparkApplication CRD
    logger.warning("EKS job submission not implemented for production mode")
    return {
        "job_name": job_name,
        "status": "NOT_IMPLEMENTED",
        "message": "Production EKS submission requires Kubernetes client setup"
    }
